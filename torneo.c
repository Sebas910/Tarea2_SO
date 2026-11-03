/*
 * Tarea 2 - Sistemas Operativos: Torneo de Komikan Concurrente
 * Autores: Sebastian Quintero y Alan Troncoso
 *
 * Compilacion: gcc -Wall -Wextra -std=c17 -pthread -o torneo torneo.c
 * Uso: ./torneo N M [config.txt]
 *
 * Resumen del diseno:
 *  - N hilos "cliente" llegan y se forman en una cola.
 *  - Un hilo matchmaker saca de a 2 clientes (Puma y Perros), espera un
 *    cupo en el semaforo de mesas (M) y lanza un hilo Juez para esa mesa.
 *  - El Juez crea 1 hilo para el Puma y 12 para los Perros. Todos usan
 *    un unico mutex por mesa para tocar el tablero.
 */

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <time.h>
#include <errno.h>

// 2 filas de trono + 5 de tablero
#define FILAS 7          
#define COLS 5
#define NUM_PERROS 12
#define PERROS_PARA_GANAR 6

//contenido casilla
#define INVALIDA -1
#define VACIA 0
#define PUMA 1
#define PERRO 2

typedef struct {
    int min_delay;
    int max_delay;
    unsigned int seed;
    int max_movs;       //limite de movimientos para eliminar partidas infinitas
    int prob_fallo;     // probabilidad de que una ficha falle (por ciclo)
    int tablero[FILAS][COLS];
} Config;

typedef struct {
    int id;
    int ocupada;
    int num_partida;
    int id_puma;       
    int id_perros;     

    int tablero[FILAS][COLS];
    int puma_f, puma_c;
    int perro_f[NUM_PERROS];
    int perro_c[NUM_PERROS];
    int perro_vivo[NUM_PERROS];
    int capturados;
    int movimientos;

    int activa;
    int fallo_puma;     // solo si falla puma
    int fallo_perros;   // solo si falla perro

    pthread_mutex_t mutex;
} Mesa;

typedef struct {
    Mesa *mesa;
    int idx;            // numero de perro (0-11)
    unsigned int seed;
} ArgFicha;

// variables globales
static volatile sig_atomic_t terminar = 0;   // la pone en 1 el handler de SIGINT
static Config config;
static Mesa *mesas;
static int N, M;

static sem_t sem_mesas;    // cuenta las mesas libres (parte en M)
static sem_t sem_cola;     // cuenta los clientes esperando en la cola
static pthread_mutex_t mutex_cola = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_mesas = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_log = PTHREAD_MUTEX_INITIALIZER;

static int *cola;          // ids de clientes que ya llegaron
static int cola_fin = 0;

static FILE *archivo_log;

// utility

static void manejador_sigint(int sig) {
    (void)sig;
    terminar = 1;
}

static void dormir_ms(int ms) {
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}

static int retardo_aleatorio(unsigned int *seed) {
    int rango = config.max_delay - config.min_delay + 1;
    return config.min_delay + rand_r(seed) % rango;
}

// imprime en consola y en torneo.log (con mutex para que no se mezclen las lineas)
static void log_msg(const char *fmt, ...) {
    va_list args;

    pthread_mutex_lock(&mutex_log);
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
    fflush(stdout);

    if (archivo_log != NULL) {
        va_start(args, fmt);
        vfprintf(archivo_log, fmt, args);
        va_end(args);
        fflush(archivo_log);
    }
    pthread_mutex_unlock(&mutex_log);
}

/*
  espera un semaforo, pero con timeout de 100 ms para ir revisando si
  llego SIGINT. Retorna 0 si lo obtuvo y -1 si hay que terminar
 */
static int esperar_sem(sem_t *s) {
    while (!terminar) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 100000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
        if (sem_timedwait(s, &ts) == 0) return 0;
    }
    return -1;
}

// tablero 

// dice si (f,c) es una de las 28 intersecciones del tablero
static int es_casilla(int f, int c) {
    if (f < 0 || f >= FILAS || c < 0 || c >= COLS) return 0;
    if (f == 0) return c == 2;
    if (f == 1) return c == 1 || c == 3;
    return 1;
}

/*
  Dice si hay una linea entre dos casillas. Segun la figura del enunciado:
   - el trono (0,2) se une a (1,1), (1,3) y baja directo a (2,2)
   - (1,1) y (1,3) se unen entre ellas
   - (1,1) baja a (2,0) y (2,1); (1,3) baja a (2,3) y (2,4)
   - la cuadricula solo tiene lineas horizontales y verticales
 */
static int son_adyacentes(int f1, int c1, int f2, int c2) {
    if (!es_casilla(f1, c1) || !es_casilla(f2, c2)) return 0;

    // ordenamos para que la primera casilla sea la de arriba
    if (f2 < f1 || (f2 == f1 && c2 < c1)) {
        int tf = f1, tc = c1;
        f1 = f2; c1 = c2;
        f2 = tf; c2 = tc;
    }

    if (f1 == 0) {
        return (f2 == 1) || (f2 == 2 && c2 == 2);
    }
    if (f1 == 1) {
        if (f2 == 1) return 1;  // (1,1) con (1,3)
        if (f2 == 2) return c2 == c1 || (c1 == 1 && c2 == 0) || (c1 == 3 && c2 == 4);
        return 0;
    }
    return (abs(f1 - f2) + abs(c1 - c2)) == 1;
}

// el salto es valido si va en linea recta sobre dos aristas
static int salto_valido(int f1, int c1, int fm, int cm, int f2, int c2) {
    if (!son_adyacentes(f1, c1, fm, cm) || !son_adyacentes(fm, cm, f2, c2)) return 0;

    // caso especial del trono <-> (3,2) pasando por (2,2) (la linea no tiene el mismo largo)
    if (f1 == 0 && c1 == 2 && fm == 2 && f2 == 3) return 1;
    if (f2 == 0 && c2 == 2 && fm == 2 && f1 == 3) return 1;

    return (fm - f1 == f2 - fm) && (cm - c1 == c2 - cm);
}

/*
  validacion del Juez: revisa adyacencia, que el destino este libre y,
  si es captura, que haya un perro en el medio
 */
static int validar_movimiento(Mesa *m, int tipo, int f1, int c1,
                              int fm, int cm, int f2, int c2, int captura) {
    if (!es_casilla(f1, c1) || !es_casilla(f2, c2)) return 0;
    if (m->tablero[f1][c1] != tipo) return 0;
    if (m->tablero[f2][c2] != VACIA) return 0;

    if (!captura) return son_adyacentes(f1, c1, f2, c2);

    if (tipo != PUMA) return 0; 
    if (!es_casilla(fm, cm) || m->tablero[fm][cm] != PERRO) return 0;
    return salto_valido(f1, c1, fm, cm, f2, c2);
}

// Llena df/dc con los destinos simples validos de una ficha y retorna cuantos hay
static int destinos_libres(Mesa *m, int tipo, int f, int c, int df[], int dc[]) {
    int n = 0;
    for (int i = 0; i < FILAS; i++) {
        for (int j = 0; j < COLS; j++) {
            if (validar_movimiento(m, tipo, f, c, 0, 0, i, j, 0)) {
                df[n] = i;
                dc[n] = j;
                n++;
            }
        }
    }
    return n;
}

// busca una captura posible para el puma, retorna 1 si encontro y llena los datos
static int buscar_captura(Mesa *m, int *idx_perro, int *nf, int *nc) {
    for (int p = 0; p < NUM_PERROS; p++) {
        if (!m->perro_vivo[p]) continue;
        for (int i = 0; i < FILAS; i++) {
            for (int j = 0; j < COLS; j++) {
                if (validar_movimiento(m, PUMA, m->puma_f, m->puma_c,
                                       m->perro_f[p], m->perro_c[p], i, j, 1)) {
                    *idx_perro = p;
                    *nf = i;
                    *nc = j;
                    return 1;
                }
            }
        }
    }
    return 0;
}

// devuelve 0 si el puma está acorralado
static int puma_tiene_movimientos(Mesa *m) {
    int df[30], dc[30];
    int p, nf, nc;
    if (destinos_libres(m, PUMA, m->puma_f, m->puma_c, df, dc) > 0) return 1;
    return buscar_captura(m, &p, &nf, &nc);
}

static char simbolo(int v, char vacio) {
    if (v == PUMA) return 'P';
    if (v == PERRO) return 'D';
    return vacio;
}

static void imprimir_tablero(Mesa *m, const char *titulo) {
    char buf[2048];
    int n = 0;

    n += snprintf(buf + n, sizeof(buf) - n, "\n--- Mesa %d | Partida #%d: %s ---\n",
                  m->id, m->num_partida, titulo);
    n += snprintf(buf + n, sizeof(buf) - n,
                  "Puma #%d vs Perros #%d | capturados %d/%d | movs %d\n",
                  m->id_puma, m->id_perros, m->capturados, PERROS_PARA_GANAR, m->movimientos);
    n += snprintf(buf + n, sizeof(buf) - n, "        %c\n", simbolo(m->tablero[0][2], 'T'));
    n += snprintf(buf + n, sizeof(buf) - n, "      %c---%c\n",
                  simbolo(m->tablero[1][1], '.'), simbolo(m->tablero[1][3], '.'));
    for (int f = 2; f < FILAS; f++) {
        n += snprintf(buf + n, sizeof(buf) - n, "    ");
        for (int c = 0; c < COLS; c++) {
            n += snprintf(buf + n, sizeof(buf) - n, "%c%s",
                          simbolo(m->tablero[f][c], '.'), c < COLS - 1 ? "-" : "");
        }
        n += snprintf(buf + n, sizeof(buf) - n, "\n");
    }
    log_msg("%s", buf);
}

// config

static void tablero_por_defecto(int t[FILAS][COLS]) {
    for (int f = 0; f < FILAS; f++)
        for (int c = 0; c < COLS; c++)
            t[f][c] = es_casilla(f, c) ? VACIA : INVALIDA;

    t[0][2] = PUMA;                   // puma parte en el torno
    for (int c = 0; c < COLS; c++) {  // 12 perros: 2 filas completas + 2 en la fila 4
        t[2][c] = PERRO;
        t[3][c] = PERRO;
    }
    t[4][0] = PERRO;
    t[4][4] = PERRO;
}

/*
  formato de config.txt (una cosa por linea, # para comentarios):
    MIN_DELAY_MS=50
    MAX_DELAY_MS=300
    SEED=1234
    MAX_MOVS=250
    PROB_FALLO=0            (por mil)
    FILA_0=-1,-1,1,-1,-1    (FILA_0 .. FILA_6, igual que la matriz del tablero)
 */
static void leer_config(const char *ruta) {
    config.min_delay = 50;
    config.max_delay = 300;
    config.seed = (unsigned int)time(NULL);
    config.max_movs = 250;
    config.prob_fallo = 0;
    tablero_por_defecto(config.tablero);

    if (ruta == NULL) return;

    FILE *f = fopen(ruta, "r");
    if (f == NULL) {
        log_msg("[Config] No se pudo abrir '%s', se usan los valores por defecto\n", ruta);
        return;
    }

    int tab[FILAS][COLS];
    int filas_leidas = 0;
    char linea[256];
    int v;
    unsigned int s;
    int fila[COLS];
    int idx;

    while (fgets(linea, sizeof(linea), f) != NULL) {
        if (linea[0] == '#' || linea[0] == '\n') continue;

        if (sscanf(linea, "MIN_DELAY_MS=%d", &v) == 1) config.min_delay = v;
        else if (sscanf(linea, "MAX_DELAY_MS=%d", &v) == 1) config.max_delay = v;
        else if (sscanf(linea, "SEED=%u", &s) == 1) config.seed = s;
        else if (sscanf(linea, "MAX_MOVS=%d", &v) == 1) config.max_movs = v;
        else if (sscanf(linea, "PROB_FALLO=%d", &v) == 1) config.prob_fallo = v;
        else if (sscanf(linea, "FILA_%d=%d,%d,%d,%d,%d", &idx,
                        &fila[0], &fila[1], &fila[2], &fila[3], &fila[4]) == 6) {
            if (idx >= 0 && idx < FILAS) {
                memcpy(tab[idx], fila, sizeof(fila));
                filas_leidas |= (1 << idx);
            }
        }
    }
    fclose(f);

    if (config.min_delay < 0 || config.max_delay < config.min_delay) {
        log_msg("[Config] Retardos invalidos, se usa 50-300 ms\n");
        config.min_delay = 50;
        config.max_delay = 300;
    }
    if (config.max_movs <= 0) config.max_movs = 250;
    if (config.prob_fallo < 0) config.prob_fallo = 0;

    // solo se acepta el tablero si hay 7 filas, tiene 1 puma y 12 perros
    if (filas_leidas == (1 << FILAS) - 1) {
        int pumas = 0, perros = 0, ok = 1;
        for (int i = 0; i < FILAS; i++) {
            for (int j = 0; j < COLS; j++) {
                if (!es_casilla(i, j)) {
                    if (tab[i][j] != INVALIDA) ok = 0;
                } else if (tab[i][j] == PUMA) {
                    pumas++;
                } else if (tab[i][j] == PERRO) {
                    perros++;
                } else if (tab[i][j] != VACIA) {
                    ok = 0;
                }
            }
        }
        if (ok && pumas == 1 && perros == NUM_PERROS) {
            memcpy(config.tablero, tab, sizeof(tab));
        } else {
            log_msg("[Config] Tablero invalido, se usa el tablero por defecto\n");
        }
    }

    log_msg("[Config] delay=[%d,%d] ms, seed=%u, max_movs=%d, prob_fallo=%d/1000\n",
            config.min_delay, config.max_delay, config.seed, config.max_movs, config.prob_fallo);
}

//fichas

// cada ciclo la ficha puede "fallar" segun prob_fallo
static int ficha_falla(unsigned int *seed) {
    if (config.prob_fallo <= 0) return 0;
    return (int)(rand_r(seed) % 1000) < config.prob_fallo;
}

static void *hilo_puma(void *arg) {
    ArgFicha *a = arg;
    Mesa *m = a->mesa;
    unsigned int seed = a->seed;

    while (!terminar) {
        dormir_ms(retardo_aleatorio(&seed));

        pthread_mutex_lock(&m->mutex);
        if (!m->activa) {
            pthread_mutex_unlock(&m->mutex);
            break;
        }

        // Fallo del jugador: se avisa, se suelta el mutex y el hilo termina
        if (ficha_falla(&seed)) {
            m->fallo_puma = 1;
            log_msg("[Mesa %d] El jugador Puma #%d fallo\n", m->id, m->id_puma);
            pthread_mutex_unlock(&m->mutex);
            break;
        }

        int pf = m->puma_f;
        int pc = m->puma_c;
        int p, nf, nc;

        if (buscar_captura(m, &p, &nf, &nc)) {
            // si puede capturar, lo prefiere antes que un movimiento normal
            int fp = m->perro_f[p];
            int cp = m->perro_c[p];
            m->tablero[pf][pc] = VACIA;
            m->tablero[fp][cp] = VACIA;
            m->tablero[nf][nc] = PUMA;
            m->puma_f = nf;
            m->puma_c = nc;
            m->perro_vivo[p] = 0;
            m->capturados++;
            m->movimientos++;
            log_msg("[Mesa %d] PUMA salta (%d,%d)->(%d,%d) y captura al perro #%d\n",
                    m->id, pf, pc, nf, nc, p + 1);
            imprimir_tablero(m, "CAPTURA");
        } else {
            int df[30], dc[30];
            int n = destinos_libres(m, PUMA, pf, pc, df, dc);
            if (n > 0) {
                int e = rand_r(&seed) % n;
                m->tablero[pf][pc] = VACIA;
                m->tablero[df[e]][dc[e]] = PUMA;
                m->puma_f = df[e];
                m->puma_c = dc[e];
                m->movimientos++;
                log_msg("[Mesa %d] PUMA (%d,%d)->(%d,%d)\n", m->id, pf, pc, df[e], dc[e]);
                if (m->movimientos % 10 == 0) imprimir_tablero(m, "ESTADO");
            }
        }
        pthread_mutex_unlock(&m->mutex);
    }
    return NULL;
}

static void *hilo_perro(void *arg) {
    ArgFicha *a = arg;
    Mesa *m = a->mesa;
    int i = a->idx;
    unsigned int seed = a->seed;

    while (!terminar) {
        dormir_ms(retardo_aleatorio(&seed));

        pthread_mutex_lock(&m->mutex);
        if (!m->activa || !m->perro_vivo[i]) {
            pthread_mutex_unlock(&m->mutex);
            break;
        }

        if (ficha_falla(&seed)) {
            m->fallo_perros = 1;
            log_msg("[Mesa %d] El jugador Perros #%d fallo (perro #%d)\n",
                    m->id, m->id_perros, i + 1);
            pthread_mutex_unlock(&m->mutex);
            break;
        }

        int f = m->perro_f[i];
        int c = m->perro_c[i];
        int df[30], dc[30];
        int n = destinos_libres(m, PERRO, f, c, df, dc);

        // si dos perros quieren la misma casilla, gana el que tomó primero el mutex
        if (n > 0) {
            int e = rand_r(&seed) % n;
            m->tablero[f][c] = VACIA;
            m->tablero[df[e]][dc[e]] = PERRO;
            m->perro_f[i] = df[e];
            m->perro_c[i] = dc[e];
            m->movimientos++;
            log_msg("[Mesa %d] PERRO #%d (%d,%d)->(%d,%d)\n", m->id, i + 1, f, c, df[e], dc[e]);
            if (m->movimientos % 10 == 0) imprimir_tablero(m, "ESTADO");
        }
        pthread_mutex_unlock(&m->mutex);
    }
    return NULL;
}

//juez

static void preparar_mesa(Mesa *m) {
    memcpy(m->tablero, config.tablero, sizeof(m->tablero));
    m->capturados = 0;
    m->movimientos = 0;
    m->fallo_puma = 0;
    m->fallo_perros = 0;

    int k = 0;
    for (int f = 0; f < FILAS; f++) {
        for (int c = 0; c < COLS; c++) {
            if (m->tablero[f][c] == PUMA) {
                m->puma_f = f;
                m->puma_c = c;
            } else if (m->tablero[f][c] == PERRO && k < NUM_PERROS) {
                m->perro_f[k] = f;
                m->perro_c[k] = c;
                m->perro_vivo[k] = 1;
                k++;
            }
        }
    }
}

static void *hilo_juez(void *arg) {
    Mesa *m = arg;
    pthread_t tid_puma;
    pthread_t tid_perros[NUM_PERROS];
    ArgFicha arg_puma;
    ArgFicha arg_perros[NUM_PERROS];
    int puma_creado = 0;
    int perros_creados[NUM_PERROS] = {0};

    pthread_mutex_init(&m->mutex, NULL);
    preparar_mesa(m);

    pthread_mutex_lock(&m->mutex);
    m->activa = 1;
    imprimir_tablero(m, "INICIO DE LA PARTIDA");
    pthread_mutex_unlock(&m->mutex);

    //hilo para las fichas
    arg_puma.mesa = m;
    arg_puma.idx = -1;
    arg_puma.seed = config.seed + m->num_partida * 100;
    puma_creado = (pthread_create(&tid_puma, NULL, hilo_puma, &arg_puma) == 0);

    for (int i = 0; i < NUM_PERROS; i++) {
        arg_perros[i].mesa = m;
        arg_perros[i].idx = i;
        arg_perros[i].seed = config.seed + m->num_partida * 100 + i + 1;
        perros_creados[i] = (pthread_create(&tid_perros[i], NULL, hilo_perro, &arg_perros[i]) == 0);
    }

    // el juez revisa el estado de la partida cada 40 ms
    while (!terminar) {
        pthread_mutex_lock(&m->mutex);
        const char *fin = NULL;

        if (m->fallo_puma && m->fallo_perros)
            fin = "FALLARON AMBOS JUGADORES, PARTIDA ABORTADA";
        else if (m->fallo_puma)
            fin = "ABANDONO DEL PUMA, GANAN LOS PERROS";
        else if (m->fallo_perros)
            fin = "ABANDONO DE LOS PERROS, GANA EL PUMA";
        else if (m->capturados >= PERROS_PARA_GANAR)
            fin = "VICTORIA DEL PUMA";
        else if (!puma_tiene_movimientos(m))
            fin = "VICTORIA DE LOS PERROS (puma acorralado)";
        else if (m->movimientos >= config.max_movs)
            fin = "EMPATE, SE LLEGO AL LIMITE DE MOVIMIENTOS";

        if (fin != NULL) {
            m->activa = 0;
            imprimir_tablero(m, fin);
        }
        pthread_mutex_unlock(&m->mutex);

        if (fin != NULL) break;
        dormir_ms(40);
    }

    // si salimos por ctrl+c la partida sigue activa, hay que cerrarla
    pthread_mutex_lock(&m->mutex);
    if (m->activa) {
        m->activa = 0;
        imprimir_tablero(m, "PARTIDA CERRADA POR SIGINT");
    }
    pthread_mutex_unlock(&m->mutex);

    // esperamos a todas las fichas antes de destruir el mutex
    if (puma_creado) pthread_join(tid_puma, NULL);
    for (int i = 0; i < NUM_PERROS; i++) {
        if (perros_creados[i]) pthread_join(tid_perros[i], NULL);
    }
    pthread_mutex_destroy(&m->mutex);

    // liberamos la mesa y avisamos al matchmaker
    int id = m->id;
    int partida = m->num_partida;
    pthread_mutex_lock(&mutex_mesas);
    m->ocupada = 0;
    pthread_mutex_unlock(&mutex_mesas);
    sem_post(&sem_mesas);

    log_msg("[Juez %d] Mesa liberada, termino la partida #%d\n", id, partida);
    return NULL;
}

//clientes + matchmaker

// cada cliente llega despues de un rato y se mete a la cola
static void *hilo_cliente(void *arg) {
    int id = (int)(long)arg;
    unsigned int seed = config.seed + id;

    dormir_ms(rand_r(&seed) % 100);
    if (terminar) return NULL;

    pthread_mutex_lock(&mutex_cola);
    cola[cola_fin++] = id;
    pthread_mutex_unlock(&mutex_cola);
    log_msg("[Matchmaker] Llega el jugador #%d\n", id);

    sem_post(&sem_cola);
    return NULL;
}

// arg = cantidad de clientes que se alcanzaron a crear
static void *hilo_matchmaker(void *arg) {
    int total = *(int *)arg;
    int emparejados = 0;
    int cola_ini = 0;
    int partidas = 0;
    pthread_t *jueces = malloc(sizeof(pthread_t) * (total / 2 + 1));

    if (jueces == NULL) {
        log_msg("[Matchmaker] Sin memoria\n");
        return NULL;
    }

    while (!terminar && emparejados + 2 <= total) {
        // esperamos a que haya 2 clientes en la cola
        if (esperar_sem(&sem_cola) != 0) break;
        if (esperar_sem(&sem_cola) != 0) break;

        pthread_mutex_lock(&mutex_cola);
        int id_puma = cola[cola_ini++];
        int id_perros = cola[cola_ini++];
        pthread_mutex_unlock(&mutex_cola);
        emparejados += 2;

        log_msg("[Matchmaker] Pareja: Puma=#%d, Perros=#%d. Esperando mesa...\n",
                id_puma, id_perros);

        // si estan las M mesas ocupadas nos quedamos esperando aqui
        if (esperar_sem(&sem_mesas) != 0) break;

        int idx = -1;
        pthread_mutex_lock(&mutex_mesas);
        for (int i = 0; i < M; i++) {
            if (!mesas[i].ocupada) {
                mesas[i].ocupada = 1;
                idx = i;
                break;
            }
        }
        pthread_mutex_unlock(&mutex_mesas);

        if (idx < 0) {   // no deberia pasar, el semaforo asegura que hay una libre
            sem_post(&sem_mesas);
            continue;
        }

        partidas++;
        mesas[idx].num_partida = partidas;
        mesas[idx].id_puma = id_puma;
        mesas[idx].id_perros = id_perros;
        log_msg("[Matchmaker] Partida #%d asignada a la mesa %d\n", partidas, idx);

        if (pthread_create(&jueces[partidas - 1], NULL, hilo_juez, &mesas[idx]) != 0) {
            log_msg("[Matchmaker] No se pudo crear el juez de la mesa %d\n", idx);
            pthread_mutex_lock(&mutex_mesas);
            mesas[idx].ocupada = 0;
            pthread_mutex_unlock(&mutex_mesas);
            sem_post(&sem_mesas);
            partidas--;
        }
    }

    if (!terminar && emparejados < total) {
        log_msg("[Matchmaker] Queda 1 jugador sin pareja (N impar)\n");
    }

    for (int i = 0; i < partidas; i++) {
        pthread_join(jueces[i], NULL);
    }
    free(jueces);
    return NULL;
}

//main

int main(int argc, char *argv[]) {
    if (argc < 3 || argc > 4) {
        fprintf(stderr, "Uso: %s N M [config.txt]\n", argv[0]);
        return 1;
    }

    N = atoi(argv[1]);
    M = atoi(argv[2]);
    if (N <= 0 || M <= 0) {
        fprintf(stderr, "N y M deben ser enteros positivos\n");
        return 1;
    }

    // handler del ctrl+c
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = manejador_sigint;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);

    archivo_log = fopen("torneo.log", "w");
    leer_config(argc == 4 ? argv[3] : NULL);

    mesas = calloc(M, sizeof(Mesa));
    cola = calloc(N, sizeof(int));
    pthread_t *clientes = malloc(sizeof(pthread_t) * N);
    if (mesas == NULL || cola == NULL || clientes == NULL) {
        fprintf(stderr, "Error: no hay memoria\n");
        free(mesas);
        free(cola);
        free(clientes);
        if (archivo_log) fclose(archivo_log);
        return 1;
    }
    for (int i = 0; i < M; i++) mesas[i].id = i;

    sem_init(&sem_mesas, 0, M);
    sem_init(&sem_cola, 0, 0);

    log_msg("=== Torneo Komikan: N=%d jugadores, M=%d mesas ===\n", N, M);

    // lanzamos los clientes
    int creados = 0;
    for (int i = 1; i <= N && !terminar; i++) {
        if (pthread_create(&clientes[creados], NULL, hilo_cliente, (void *)(long)i) == 0) {
            creados++;
        } else {
            log_msg("[Servidor] No se pudo crear el cliente #%d\n", i);
        }
    }

    pthread_t tid_match;
    int match_creado = (pthread_create(&tid_match, NULL, hilo_matchmaker, &creados) == 0);
    if (!match_creado) {
        fprintf(stderr, "No se pudo crear el matchmaker\n");
    }

    for (int i = 0; i < creados; i++) pthread_join(clientes[i], NULL);
    if (match_creado) pthread_join(tid_match, NULL);

    if (terminar) {
        log_msg("\n[Servidor] SIGINT recibida, partidas cerradas\n");
    }

    // todo se libera
    sem_destroy(&sem_mesas);
    sem_destroy(&sem_cola);
    pthread_mutex_destroy(&mutex_cola);
    pthread_mutex_destroy(&mutex_mesas);
    free(mesas);
    free(cola);
    free(clientes);

    log_msg("=== Torneo finalizado ===\n");
    if (archivo_log) fclose(archivo_log);
    pthread_mutex_destroy(&mutex_log);
    return 0;
}
