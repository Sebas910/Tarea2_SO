/*
 * Tarea 2 - Sistemas Operativos: Torneo de Komikan Concurrente
 * Autores: Sebastian Quintero y Alan Troncoso
 *
 * Compilacion: gcc -Wall -Wextra -std=c17 -pthread -o torneo torneo.c
 * Uso: ./torneo N M [config.txt]
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
#include <limits.h>
#include <ctype.h>
#include <stdatomic.h>

/* Dimensiones de la matriz que representa el tablero (2 filas del trono + 5x5 cuadricula) */
#define FILAS 7
#define COLS 5
#define NUM_PERROS 12
#define PERROS_PARA_GANAR 6
#define MAX_MOVIMIENTOS_DEFECTO 250

/* Estados posibles de cada posicion en la matriz del tablero */
#define CASILLA_INVALIDA -1
#define CASILLA_VACIA     0
#define CASILLA_PUMA      1
#define CASILLA_PERRO     2

/* Rol que tenia el hilo propietario del mutex robusto. */
#define ROL_PUMA      1
#define ROL_PERROS    2
#define ROL_JUEZ      3
#define ROL_AUXILIAR  4

/* Estructura para guardar los parametros leidos desde config.txt */
typedef struct {
    int min_delay_ms;
    int max_delay_ms;
    unsigned int seed;
    int max_movimientos;
    int prob_fallo_milesimas;
    int usar_tablero_custom;
    int tablero_inicial[FILAS][COLS];
} Configuracion;

/* Estructura para coordinar a cada pareja de jugadores emparejados por el servidor */
typedef struct {
    int id_puma;
    int id_perros;
    int lista_para_jugar;
    int mesa_asignada;
    int partida_terminada;
    pthread_cond_t cond_pareja;
} Pareja;

/* Estructura que representa una Mesa de Juego concurrente */
typedef struct {
    int id_mesa;
    int ocupada;
    int num_partida;
    int idx_pareja;
    int partida_activa;
    int mutex_inicializado;

    /* IDs de los dos clientes que estan jugando en esta mesa */
    int id_jugador_puma;
    int id_jugador_perros;

    /* Banderas para detectar desconexion o fallo interno de un jugador (Seccion 4.1) */
    int fallo_jugador_puma;
    int fallo_jugador_perros;
    int fallo_interno;
    int propietario_mutex;

    /* Estado compartido del tablero de la mesa */
    int tablero[FILAS][COLS];
    int puma_f;
    int puma_c;
    int perro_f[NUM_PERROS];
    int perro_c[NUM_PERROS];
    int perro_vivo[NUM_PERROS];
    int perros_capturados;
    int total_movimientos;

    /* Mutex exclusivo de la mesa (configurado como robusto ante fallos) */
    pthread_mutex_t mutex_tablero;
} Mesa;

/* Argumentos que recibe cada hilo de ficha (1 Puma y 12 Perros por mesa) */
typedef struct {
    Mesa *mesa;
    int indice_perro; /* -1 si es el Puma, 0..11 si es un Perro */
    unsigned int seed_hilo;
} ArgFicha;

/* Variables globales del servidor */
static _Atomic sig_atomic_t servidor_activo = 1;
#define SERVIDOR_ACTIVO (atomic_load_explicit(&servidor_activo, memory_order_relaxed))
static int total_clientes_N = 0;
static int max_mesas_M = 0;
static Configuracion config_global;
static Mesa *mesas = NULL;
static Pareja *parejas = NULL;
static int max_parejas = 0;

/* Primitivas de sincronizacion globales del servidor */
static sem_t sem_mesas;
static pthread_mutex_t mutex_servidor = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_log = PTHREAD_MUTEX_INITIALIZER;

/* Variables de control del Matchmaker (sala de espera) */
static int clientes_llegados = 0;
static int pareja_en_espera_idx = -1;
static int contador_parejas = 0;
static int contador_partidas = 0;
static int lanzamiento_clientes_terminado = 0;
static int total_clientes_efectivos = 0;

/* Archivo de registro de eventos */
static FILE *archivo_log = NULL;

/*
 * Manejador de la senal SIGINT (Ctrl+C).
 * Desactiva la bandera global para detener la llegada de clientes y cerrar las mesas.
 */
static void manejador_sigint(int sig) {
    (void)sig;
    atomic_store_explicit(&servidor_activo, 0, memory_order_relaxed);
}

/*
 * Funcion auxiliar para dormir en milisegundos usando nanosleep.
 */
static void dormir_ms(int ms) {
    if (ms <= 0) return;
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}

/*
 Espera en una variable de condicion con un timeout corto (100 ms) para que,
 si el usuario presiona Ctrl+C (SIGINT), todos los hilos en espera revisen
 periodicamente la bandera SERVIDOR_ACTIVO y salgan sin quedarse bloqueados.
 */
static void esperar_condicion_segura(pthread_cond_t *cond, pthread_mutex_t *mutex) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_nsec += 100000000L; /* 100 ms */
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec += 1;
        ts.tv_nsec -= 1000000000L;
    }
    pthread_cond_timedwait(cond, mutex, &ts);
}

/*
 Adquiere el mutex robusto y registra el rol del hilo propietario.
 EOWNERDEAD permite identificar el rol que dejo la primitiva bloqueada,
 recuperar su consistencia y notificar al Juez antes de continuar.
 Devuelve 0 si el mutex quedo adquirido; otro valor indica un error real.
 */
static int bloquear_mutex_tablero(Mesa *m, int rol_actual) {
    int res = pthread_mutex_lock(&m->mutex_tablero);
    if (res == EOWNERDEAD) {
        int rol_fallido = m->propietario_mutex;
        if (rol_fallido == ROL_PUMA) {
            m->fallo_jugador_puma = 1;
        } else if (rol_fallido == ROL_PERROS) {
            m->fallo_jugador_perros = 1;
        } else {
            m->fallo_interno = 1;
        }
        res = pthread_mutex_consistent(&m->mutex_tablero);
        if (res != 0) {
            (void)pthread_mutex_unlock(&m->mutex_tablero);
            return res;
        }
        res = 0;
    }

    if (res == 0) {
        m->propietario_mutex = rol_actual;
    }
    return res;
}

/* Libera el mutex; el ultimo rol se conserva para detectar EOWNERDEAD. */
static int desbloquear_mutex_tablero(Mesa *m) {
    return pthread_mutex_unlock(&m->mutex_tablero);
}

/* Inicializa un mutex robusto y propaga cualquier error de POSIX. */
static int inicializar_mutex_robusto(pthread_mutex_t *mutex) {
    pthread_mutexattr_t attr;
    int res = pthread_mutexattr_init(&attr);
    if (res != 0) return res;

    res = pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
    if (res == 0) res = pthread_mutex_init(mutex, &attr);
    (void)pthread_mutexattr_destroy(&attr);
    return res;
}

/*
 Funcion thread-safe para imprimir mensajes tanto en consola como en torneo.log.
 */
static void registrar_evento(const char *formato, ...) {
    va_list args_consola;
    va_list args_archivo;

    pthread_mutex_lock(&mutex_log);

    va_start(args_consola, formato);
    vprintf(formato, args_consola);
    va_end(args_consola);
    fflush(stdout);

    if (archivo_log != NULL) {
        va_start(args_archivo, formato);
        vfprintf(archivo_log, formato, args_archivo);
        va_end(args_archivo);
        fflush(archivo_log);
    }

    pthread_mutex_unlock(&mutex_log);
}

/*
 Carga la disposicion inicial por defecto del tablero de Komikan:
  - (0,2): Trono (T) con el Puma.
  - (1,1) y (1,3): Casillas laterales del triangulo del trono (vacias).
  - Filas 2 a 6: Cuadricula principal de 5x5 con los 12 Perros.
 */
static void cargar_tablero_por_defecto(int tab[FILAS][COLS]) {
    for (int f = 0; f < FILAS; f++) {
        for (int c = 0; c < COLS; c++) {
            tab[f][c] = CASILLA_INVALIDA;
        }
    }

    /* Triangulo superior (Trono) */
    tab[0][2] = CASILLA_PUMA;
    tab[1][1] = CASILLA_VACIA;
    tab[1][3] = CASILLA_VACIA;

    /* Cuadricula principal 5x5 (filas 2 a 6) */
    for (int f = 2; f < FILAS; f++) {
        for (int c = 0; c < COLS; c++) {
            tab[f][c] = CASILLA_VACIA;
        }
    }

    /* Colocamos los 12 perros en el tablero */
    for (int c = 0; c < COLS; c++) {
        tab[2][c] = CASILLA_PERRO;
        tab[3][c] = CASILLA_PERRO;
    }
    tab[4][0] = CASILLA_PERRO;
    tab[4][4] = CASILLA_PERRO;
}

static int es_casilla_del_tablero(int f, int c);

/*
 Lee el archivo de configuracion de entrada (config.txt) si fue especificado.
 */
static int leer_valor_entero(const char *linea, const char *clave, int *valor) {
    size_t n = strlen(clave);
    if (strncmp(linea, clave, n) != 0) return 0;

    errno = 0;
    char *fin = NULL;
    long v = strtol(linea + n, &fin, 10);
    if (errno != 0 || fin == linea + n || v < INT_MIN || v > INT_MAX) return -1;

    while (isspace((unsigned char)*fin)) fin++;
    if (*fin != '\0' && *fin != '#') return -1;
    *valor = (int)v;
    return 1;
}

static int leer_fila_config(const char *linea, int *indice, int valores[COLS]) {
    if (strncmp(linea, "FILA_", 5) != 0) return 0;

    const char *p = linea + 5;
    errno = 0;
    char *fin = NULL;
    long idx = strtol(p, &fin, 10);
    if (errno != 0 || fin == p || idx < 0 || idx >= FILAS || *fin != '=') return -1;
    *indice = (int)idx;
    p = fin + 1;

    for (int c = 0; c < COLS; c++) {
        errno = 0;
        long v = strtol(p, &fin, 10);
        if (errno != 0 || fin == p || v < INT_MIN || v > INT_MAX) return -1;
        valores[c] = (int)v;
        p = fin;
        while (isspace((unsigned char)*p)) p++;
        if (c < COLS - 1) {
            if (*p != ',') return -1;
            p++;
        }
    }

    while (isspace((unsigned char)*p)) p++;
    return (*p == '\0' || *p == '#') ? 1 : -1;
}

static void leer_configuracion(const char *ruta) {
    config_global.min_delay_ms = 50;
    config_global.max_delay_ms = 300;
    config_global.seed = (unsigned int)time(NULL);
    config_global.max_movimientos = MAX_MOVIMIENTOS_DEFECTO;
    config_global.prob_fallo_milesimas = 0;
    config_global.usar_tablero_custom = 0;
    cargar_tablero_por_defecto(config_global.tablero_inicial);

    if (ruta == NULL) return;

    FILE *f = fopen(ruta, "r");
    if (f == NULL) {
        registrar_evento("[Config] No se pudo abrir '%s'; se usaran valores por defecto.\n", ruta);
        return;
    }

    char linea[256];
    int filas_vistas[FILAS] = {0};
    int filas_leidas = 0;
    int tablero_invalido = 0;
    int tab_temp[FILAS][COLS];
    cargar_tablero_por_defecto(tab_temp);

    int min_config = config_global.min_delay_ms;
    int max_config = config_global.max_delay_ms;

    while (fgets(linea, sizeof(linea), f) != NULL) {
        char *p = linea;
        while (isspace((unsigned char)*p)) p++;
        if (*p == '\0' || *p == '#' || *p == '\n') continue;

        int valor = 0;
        int r = leer_valor_entero(p, "MIN_DELAY_MS=", &valor);
        if (r == 0) r = leer_valor_entero(p, "MIN_MS=", &valor);
        if (r != 0) {
            if (r > 0 && valor >= 50 && valor <= 300) min_config = valor;
            continue;
        }

        r = leer_valor_entero(p, "MAX_DELAY_MS=", &valor);
        if (r == 0) r = leer_valor_entero(p, "MAX_MS=", &valor);
        if (r != 0) {
            if (r > 0 && valor >= 50 && valor <= 300) max_config = valor;
            continue;
        }

        r = leer_valor_entero(p, "MAX_MOVS=", &valor);
        if (r != 0) {
            if (r > 0 && valor > 20) config_global.max_movimientos = valor;
            continue;
        }

        r = leer_valor_entero(p, "PROB_FALLO_MILESIMAS=", &valor);
        if (r == 0) r = leer_valor_entero(p, "PROB_FALLO=", &valor);
        if (r != 0) {
            if (r > 0 && valor >= 0 && valor <= 500) config_global.prob_fallo_milesimas = valor;
            continue;
        }

        if (strncmp(p, "SEED=", 5) == 0) {
            errno = 0;
            char *fin = NULL;
            unsigned long semilla = strtoul(p + 5, &fin, 10);
            while (fin != NULL && isspace((unsigned char)*fin)) fin++;
            if (errno == 0 && fin != p + 5 && fin != NULL &&
                (*fin == '\0' || *fin == '#') && semilla <= UINT_MAX) {
                config_global.seed = (unsigned int)semilla;
            }
            continue;
        }

        int idx = -1;
        int fila[COLS];
        r = leer_fila_config(p, &idx, fila);
        if (r != 0) {
            if (r < 0 || idx < 0 || idx >= FILAS || filas_vistas[idx]) {
                tablero_invalido = 1;
            } else {
                memcpy(tab_temp[idx], fila, sizeof(fila));
                filas_vistas[idx] = 1;
                filas_leidas++;
            }
        }
    }
    fclose(f);

    if (min_config <= max_config) {
        config_global.min_delay_ms = min_config;
        config_global.max_delay_ms = max_config;
    } else {
        registrar_evento("[Config] Rango de retardos invalido; se mantienen 50-300 ms.\n");
    }

    if (filas_leidas == FILAS && !tablero_invalido) {
        int pumas = 0;
        int perros = 0;
        int valido = 1;

        for (int r = 0; r < FILAS; r++) {
            for (int c = 0; c < COLS; c++) {
                int v = tab_temp[r][c];
                if (!es_casilla_del_tablero(r, c)) {
                    if (v != CASILLA_INVALIDA) valido = 0;
                } else if (v == CASILLA_PUMA) {
                    pumas++;
                } else if (v == CASILLA_PERRO) {
                    perros++;
                } else if (v != CASILLA_VACIA) {
                    valido = 0;
                }
            }
        }

        if (valido && pumas == 1 && perros == NUM_PERROS) {
            memcpy(config_global.tablero_inicial, tab_temp, sizeof(tab_temp));
            config_global.usar_tablero_custom = 1;
        } else {
            registrar_evento("[Config] Tablero personalizado invalido; se mantiene el tablero por defecto.\n");
        }
    } else if (filas_leidas > 0) {
        registrar_evento("[Config] Se requieren siete filas FILA_ distintas y validas; se mantiene el tablero por defecto.\n");
    }

    registrar_evento("[Config] Parametros de '%s': delay=[%d, %d] ms, seed=%u, tablero=%s.\n",
                     ruta, config_global.min_delay_ms, config_global.max_delay_ms,
                     config_global.seed, config_global.usar_tablero_custom ? "personalizado" : "por defecto");
}

/*
 Verifica si una coordenada (f, c) pertenece a las 28 intersecciones del tablero.
 */
static int es_casilla_del_tablero(int f, int c) {
    if (f < 0 || f >= FILAS || c < 0 || c >= COLS) return 0;
    if (f == 0) return (c == 2);
    if (f == 1) return (c == 1 || c == 3);
    return 1;
}

/*
 Verifica si dos posiciones (f1, c1) y (f2, c2) estan conectadas por una linea
 en el tablero de Komikan (incluyendo las conexiones especiales del trono T).
 */
static int son_adyacentes(int f1, int c1, int f2, int c2) {
    if (!es_casilla_del_tablero(f1, c1) || !es_casilla_del_tablero(f2, c2)) {
        return 0;
    }
    if (f1 == f2 && c1 == c2) {
        return 0;
    }

    /* Conexiones del vertice superior del trono T en (0, 2) */
    if (f1 == 0 && c1 == 2) {
        return ((f2 == 1 && c2 == 1) || (f2 == 1 && c2 == 3) || (f2 == 2 && c2 == 2));
    }
    if (f2 == 0 && c2 == 2) {
        return ((f1 == 1 && c1 == 1) || (f1 == 1 && c1 == 3) || (f1 == 2 && c1 == 2));
    }

    /* Conexiones del nodo izquierdo del trono en (1, 1) */
    if (f1 == 1 && c1 == 1) {
        return ((f2 == 0 && c2 == 2) || (f2 == 1 && c2 == 3) ||
                (f2 == 2 && c2 == 0) || (f2 == 2 && c2 == 1) || (f2 == 2 && c2 == 2));
    }
    if (f2 == 1 && c2 == 1) {
        return ((f1 == 0 && c1 == 2) || (f1 == 1 && c1 == 3) ||
                (f1 == 2 && c1 == 0) || (f1 == 2 && c1 == 1) || (f1 == 2 && c1 == 2));
    }

    /* Conexiones del nodo derecho del trono en (1, 3) */
    if (f1 == 1 && c1 == 3) {
        return ((f2 == 0 && c2 == 2) || (f2 == 1 && c2 == 1) ||
                (f2 == 2 && c2 == 2) || (f2 == 2 && c2 == 3) || (f2 == 2 && c2 == 4));
    }
    if (f2 == 1 && c2 == 3) {
        return ((f1 == 0 && c1 == 2) || (f1 == 1 && c1 == 1) ||
                (f1 == 2 && c1 == 2) || (f1 == 2 && c1 == 3) || (f1 == 2 && c1 == 4));
    }

    /* En la cuadricula principal solo existen aristas horizontales y verticales. */
    int df = abs(f1 - f2);
    int dc = abs(c1 - c2);
    return (df + dc == 1);
}

/*
 Valida si el Puma puede saltar en linea recta desde (f1, c1) sobre un perro
 ubicado en (fm, cm) para caer en la casilla vacia (f2, c2).
 */
static int es_salto_valido(int f1, int c1, int fm, int cm, int f2, int c2) {
    if (!son_adyacentes(f1, c1, fm, cm) || !son_adyacentes(fm, cm, f2, c2)) {
        return 0;
    }
    /* Saltos diagonales desde/hacia el trono T (0,2) pasando por (1,1) o (1,3) */
    if (f1 == 0 && c1 == 2 && fm == 1 && cm == 1 && f2 == 2 && c2 == 0) return 1;
    if (f2 == 0 && c2 == 2 && fm == 1 && cm == 1 && f1 == 2 && c1 == 0) return 1;
    if (f1 == 0 && c1 == 2 && fm == 1 && cm == 3 && f2 == 2 && c2 == 4) return 1;
    if (f2 == 0 && c2 == 2 && fm == 1 && cm == 3 && f1 == 2 && c1 == 4) return 1;

    /* Salto vertical desde/hacia el trono T (0,2) pasando por (2,2) y (3,2) */
    if (f1 == 0 && c1 == 2 && fm == 2 && cm == 2 && f2 == 3 && c2 == 2) return 1;
    if (f2 == 0 && c2 == 2 && fm == 2 && cm == 2 && f1 == 3 && c1 == 2) return 1;

    /* En el resto del tablero el salto mantiene la misma direccion (pendiente constante) */
    if ((fm - f1 == f2 - fm) && (cm - c1 == c2 - cm)) {
        return 1;
    }
    return 0;
}

/*
 * Funcion de validacion del Juez Arbitro (Requisito 3.3.2):
 * Verifica que el movimiento solicitado por una ficha cumpla estrictamente con
 * las reglas de adyacencia, casilla destino libre y captura (en caso de salto del Puma).
 */
static int juez_validar_movimiento(Mesa *m, int tipo_ficha,
                                   int f_orig, int c_orig,
                                   int f_med, int c_med,
                                   int f_dest, int c_dest,
                                   int es_captura) {
    if (!es_casilla_del_tablero(f_orig, c_orig) || !es_casilla_del_tablero(f_dest, c_dest)) {
        return 0;
    }
    if (m->tablero[f_orig][c_orig] != tipo_ficha) {
        return 0;
    }
    if (m->tablero[f_dest][c_dest] != CASILLA_VACIA) {
        return 0;
    }

    if (!es_captura) {
        return son_adyacentes(f_orig, c_orig, f_dest, c_dest);
    }

    /* Solo el Puma puede capturar saltando sobre un Perro */
    if (tipo_ficha != CASILLA_PUMA) {
        return 0;
    }
    if (!es_casilla_del_tablero(f_med, c_med) || m->tablero[f_med][c_med] != CASILLA_PERRO) {
        return 0;
    }
    return es_salto_valido(f_orig, c_orig, f_med, c_med, f_dest, c_dest);
}

/* Instantanea de un estado de mesa tomada mientras el mutex esta adquirido. */
typedef struct {
    int id_mesa;
    int num_partida;
    int id_jugador_puma;
    int id_jugador_perros;
    int perros_capturados;
    int total_movimientos;
    int tablero[FILAS][COLS];
} SnapshotMesa;

/* Debe llamarse mientras el hilo posee mutex_tablero. */
static void capturar_snapshot_mesa(const Mesa *m, SnapshotMesa *s) {
    s->id_mesa = m->id_mesa;
    s->num_partida = m->num_partida;
    s->id_jugador_puma = m->id_jugador_puma;
    s->id_jugador_perros = m->id_jugador_perros;
    s->perros_capturados = m->perros_capturados;
    s->total_movimientos = m->total_movimientos;
    memcpy(s->tablero, m->tablero, sizeof(s->tablero));
}

/* Imprime una instantanea ya capturada; la E/S no mantiene bloqueado el tablero. */
static void imprimir_snapshot_mesa(const SnapshotMesa *s, const char *evento) {
    pthread_mutex_lock(&mutex_log);

    printf("\n=== [Mesa %d | Partida #%d] %s ===\n", s->id_mesa, s->num_partida, evento);
    printf("Jugadores: Puma=#%d vs Perros=#%d | Capturados: %d/%d | Movs: %d\n",
           s->id_jugador_puma, s->id_jugador_perros, s->perros_capturados,
           PERROS_PARA_GANAR, s->total_movimientos);

    char simb_t = (s->tablero[0][2] == CASILLA_PUMA) ? 'P' :
                  (s->tablero[0][2] == CASILLA_PERRO ? 'D' : 'T');
    char simb_11 = (s->tablero[1][1] == CASILLA_PUMA) ? 'P' :
                   (s->tablero[1][1] == CASILLA_PERRO ? 'D' : '.');
    char simb_13 = (s->tablero[1][3] == CASILLA_PUMA) ? 'P' :
                   (s->tablero[1][3] == CASILLA_PERRO ? 'D' : '.');

    printf("        (%c)        <- Trono\n", simb_t);
    printf("       / | \\\n");
    printf("     (%c)-+-(%c)\n", simb_11, simb_13);
    printf("    / |  |  | \\\n");

    for (int f = 2; f < FILAS; f++) {
        printf("   ");
        for (int c = 0; c < COLS; c++) {
            char ch = '.';
            if (s->tablero[f][c] == CASILLA_PUMA) ch = 'P';
            else if (s->tablero[f][c] == CASILLA_PERRO) ch = 'D';
            printf("(%c)", ch);
            if (c < COLS - 1) printf("-");
        }
        printf("\n");
    }
    fflush(stdout);

    if (archivo_log != NULL) {
        fprintf(archivo_log, "[Mesa %d | Partida #%d] %s | Capturados: %d/%d | Movs: %d\n",
                s->id_mesa, s->num_partida, evento, s->perros_capturados,
                PERROS_PARA_GANAR, s->total_movimientos);
        fprintf(archivo_log, "        (%c)\n     (%c)-+-(%c)\n", simb_t, simb_11, simb_13);
        for (int f = 2; f < FILAS; f++) {
            fprintf(archivo_log, "   ");
            for (int c = 0; c < COLS; c++) {
                char ch = '.';
                if (s->tablero[f][c] == CASILLA_PUMA) ch = 'P';
                else if (s->tablero[f][c] == CASILLA_PERRO) ch = 'D';
                fprintf(archivo_log, "(%c)%s", ch, (c < COLS - 1) ? "-" : "");
            }
            fprintf(archivo_log, "\n");
        }
        fflush(archivo_log);
    }

    pthread_mutex_unlock(&mutex_log);
}

/*
 * Revisa si el Puma cuenta con al menos un movimiento o salto valido.
 * Si retorna 0, los Perros han logrado acorralar completamente al Puma.
 */
static int puma_tiene_movimientos(Mesa *m) {
    int pf = m->puma_f;
    int pc = m->puma_c;

    for (int f = 0; f < FILAS; f++) {
        for (int c = 0; c < COLS; c++) {
            if (!es_casilla_del_tablero(f, c)) continue;

            if (juez_validar_movimiento(m, CASILLA_PUMA, pf, pc, 0, 0, f, c, 0)) {
                return 1;
            }

            if (m->tablero[f][c] == CASILLA_PERRO && son_adyacentes(pf, pc, f, c)) {
                for (int f2 = 0; f2 < FILAS; f2++) {
                    for (int c2 = 0; c2 < COLS; c2++) {
                        if (juez_validar_movimiento(m, CASILLA_PUMA, pf, pc, f, c, f2, c2, 1)) {
                            return 1;
                        }
                    }
                }
            }
        }
    }
    return 0;
}

/*
 * Hilo del Puma: intenta moverse o capturar perros en tiempo real (sin turnos)
 * ejecutando ciclos de decision entre min_delay_ms y max_delay_ms.
 */
static void *hilo_ficha_puma(void *arg) {
    ArgFicha *info = (ArgFicha *)arg;
    Mesa *m = info->mesa;
    unsigned int seed = info->seed_hilo;

    while (SERVIDOR_ACTIVO) {
        int rango = config_global.max_delay_ms - config_global.min_delay_ms + 1;
        int retardo = config_global.min_delay_ms + (int)(rand_r(&seed) % (unsigned int)rango);
        dormir_ms(retardo);

        if (bloquear_mutex_tablero(m, ROL_PUMA) != 0) break;
        if (!m->partida_activa || !SERVIDOR_ACTIVO ||
            m->fallo_jugador_puma || m->fallo_jugador_perros || m->fallo_interno) {
            (void)desbloquear_mutex_tablero(m);
            break;
        }

        int pf = m->puma_f;
        int pc = m->puma_c;
        int movio = 0;
        char evento[256] = {0};

        /* El Puma prioriza capturas validas sobre movimientos simples. */
        for (int fm = 0; fm < FILAS && !movio; fm++) {
            for (int cm = 0; cm < COLS && !movio; cm++) {
                if (!es_casilla_del_tablero(fm, cm) || m->tablero[fm][cm] != CASILLA_PERRO) {
                    continue;
                }
                for (int f2 = 0; f2 < FILAS && !movio; f2++) {
                    for (int c2 = 0; c2 < COLS && !movio; c2++) {
                        if (juez_validar_movimiento(m, CASILLA_PUMA, pf, pc, fm, cm, f2, c2, 1)) {
                            m->tablero[pf][pc] = CASILLA_VACIA;
                            m->tablero[fm][cm] = CASILLA_VACIA;
                            m->tablero[f2][c2] = CASILLA_PUMA;
                            m->puma_f = f2;
                            m->puma_c = c2;

                            for (int i = 0; i < NUM_PERROS; i++) {
                                if (m->perro_vivo[i] && m->perro_f[i] == fm && m->perro_c[i] == cm) {
                                    m->perro_vivo[i] = 0;
                                    break;
                                }
                            }
                            m->perros_capturados++;
                            m->total_movimientos++;
                            movio = 1;
                            snprintf(evento, sizeof(evento),
                                     "PUMA salta sobre Perro en (%d,%d) y cae en (%d,%d)",
                                     fm, cm, f2, c2);
                        }
                    }
                }
            }
        }

        /* Si no existe captura disponible, selecciona un destino adyacente libre. */
        if (!movio) {
            int dest_f[30], dest_c[30];
            int num_dest = 0;

            for (int f = 0; f < FILAS; f++) {
                for (int c = 0; c < COLS; c++) {
                    if (juez_validar_movimiento(m, CASILLA_PUMA, pf, pc, 0, 0, f, c, 0)) {
                        dest_f[num_dest] = f;
                        dest_c[num_dest] = c;
                        num_dest++;
                    }
                }
            }

            if (num_dest > 0) {
                int eleccion = (int)(rand_r(&seed) % (unsigned int)num_dest);
                int nf = dest_f[eleccion];
                int nc = dest_c[eleccion];
                m->tablero[pf][pc] = CASILLA_VACIA;
                m->tablero[nf][nc] = CASILLA_PUMA;
                m->puma_f = nf;
                m->puma_c = nc;
                m->total_movimientos++;
                snprintf(evento, sizeof(evento), "PUMA se mueve de (%d,%d) a (%d,%d)", pf, pc, nf, nc);
            }
        }

        SnapshotMesa snapshot;
        int registrar_movimiento = (evento[0] != '\0');
        if (registrar_movimiento) capturar_snapshot_mesa(m, &snapshot);
        (void)desbloquear_mutex_tablero(m);
        if (registrar_movimiento) imprimir_snapshot_mesa(&snapshot, evento);
    }

    return NULL;
}

/*
 * Hilo de cada uno de los 12 Perros: compite en tiempo real por reclamar
 * casillas adyacentes vacias para acorralar al Puma.
 */
static void *hilo_ficha_perro(void *arg) {
    ArgFicha *info = (ArgFicha *)arg;
    Mesa *m = info->mesa;
    int idx = info->indice_perro;
    unsigned int seed = info->seed_hilo;

    while (SERVIDOR_ACTIVO) {
        int rango = config_global.max_delay_ms - config_global.min_delay_ms + 1;
        int retardo = config_global.min_delay_ms + (int)(rand_r(&seed) % (unsigned int)rango);
        dormir_ms(retardo);

        if (bloquear_mutex_tablero(m, ROL_PERROS) != 0) break;
        if (!m->partida_activa || !m->perro_vivo[idx] || !SERVIDOR_ACTIVO ||
            m->fallo_jugador_puma || m->fallo_jugador_perros || m->fallo_interno) {
            (void)desbloquear_mutex_tablero(m);
            break;
        }

        int df = m->perro_f[idx];
        int dc = m->perro_c[idx];
        int cand_f[30], cand_c[30];
        int num_cand = 0;
        char evento[256] = {0};

        for (int f = 0; f < FILAS; f++) {
            for (int c = 0; c < COLS; c++) {
                if (juez_validar_movimiento(m, CASILLA_PERRO, df, dc, 0, 0, f, c, 0)) {
                    cand_f[num_cand] = f;
                    cand_c[num_cand] = c;
                    num_cand++;
                }
            }
        }

        if (num_cand > 0) {
            int eleccion = (int)(rand_r(&seed) % (unsigned int)num_cand);
            int nf = cand_f[eleccion];
            int nc = cand_c[eleccion];
            m->tablero[df][dc] = CASILLA_VACIA;
            m->tablero[nf][nc] = CASILLA_PERRO;
            m->perro_f[idx] = nf;
            m->perro_c[idx] = nc;
            m->total_movimientos++;

            if (m->total_movimientos % 5 == 0) {
                snprintf(evento, sizeof(evento), "PERRO #%d se mueve de (%d,%d) a (%d,%d)",
                         idx + 1, df, dc, nf, nc);
            }
        }

        SnapshotMesa snapshot;
        int registrar_movimiento = (evento[0] != '\0');
        if (registrar_movimiento) capturar_snapshot_mesa(m, &snapshot);
        (void)desbloquear_mutex_tablero(m);
        if (registrar_movimiento) imprimir_snapshot_mesa(&snapshot, evento);
    }

    return NULL;
}

/*
 * Inicializa las posiciones de las fichas en el tablero de la mesa.
 */
static void inicializar_estado_mesa(Mesa *m) {
    memcpy(m->tablero, config_global.tablero_inicial, sizeof(m->tablero));
    m->perros_capturados = 0;
    m->total_movimientos = 0;
    m->fallo_jugador_puma = 0;
    m->fallo_jugador_perros = 0;

    for (int i = 0; i < NUM_PERROS; i++) {
        m->perro_vivo[i] = 0;
        m->perro_f[i] = 0;
        m->perro_c[i] = 0;
    }

    int idx_perro = 0;
    for (int f = 0; f < FILAS; f++) {
        for (int c = 0; c < COLS; c++) {
            if (m->tablero[f][c] == CASILLA_PUMA) {
                m->puma_f = f;
                m->puma_c = c;
            } else if (m->tablero[f][c] == CASILLA_PERRO && idx_perro < NUM_PERROS) {
                m->perro_f[idx_perro] = f;
                m->perro_c[idx_perro] = c;
                m->perro_vivo[idx_perro] = 1;
                idx_perro++;
            }
        }
    }
}

typedef struct {
    Mesa *mesa;
    int rol;
    int id_cliente;
    int fallo_inyectado;
} ArgFallo;

/*
 * Hilo que puede simular la terminacion abrupta de uno de los jugadores.
 * Se une al Juez antes de que este destruya el mutex de la mesa.
 */
static void *hilo_fallo_simulado(void *arg) {
    ArgFallo *info = (ArgFallo *)arg;
    Mesa *m = info->mesa;
    int rol = info->rol;
    int id_cliente = info->id_cliente;

    unsigned int semilla = config_global.seed ^ ((unsigned int)id_cliente * 7919u);
    if (config_global.prob_fallo_milesimas <= 0 ||
        (int)(rand_r(&semilla) % 1000u) >= config_global.prob_fallo_milesimas) {
        return NULL;
    }

    dormir_ms(120);
    if (bloquear_mutex_tablero(m, rol) == 0) {
        int es_el_mismo_jugador = (rol == ROL_PUMA)
            ? (m->id_jugador_puma == id_cliente)
            : (m->id_jugador_perros == id_cliente);
        if (m->partida_activa && es_el_mismo_jugador) {
            info->fallo_inyectado = 1;
            /* La salida sin unlock provoca EOWNERDEAD en el siguiente lock. */
            return NULL;
        }
        (void)desbloquear_mutex_tablero(m);
    }
    return NULL;
}

/*
 * Hilo Juez Arbitro de Mesa (Seccion 3.3 y 4.1):
 * - Inicializa el mutex robusto y el estado del tablero de la mesa.
 * - Publica la mesa a los clientes una vez que sus primitivas estan listas.
 * - Crea los 13 hilos de fichas (1 Puma y 12 Perros).
 * - Supervisa victoria, fallos y cierre por SIGINT; une los hilos y destruye
 *   el mutex antes de liberar la mesa.
 */
static void *hilo_juez_mesa(void *arg) {
    Mesa *m = (Mesa *)arg;
    int rc_mutex = inicializar_mutex_robusto(&m->mutex_tablero);
    if (rc_mutex != 0) {
        registrar_evento("[Juez Mesa %d] No se pudo inicializar el mutex robusto: %d.\n",
                         m->id_mesa, rc_mutex);
        pthread_mutex_lock(&mutex_servidor);
        int idx = m->idx_pareja;
        m->ocupada = 0;
        m->mutex_inicializado = 0;
        parejas[idx].partida_terminada = 1;
        pthread_cond_broadcast(&parejas[idx].cond_pareja);
        pthread_mutex_unlock(&mutex_servidor);
        sem_post(&sem_mesas);
        return NULL;
    }

    if (bloquear_mutex_tablero(m, ROL_JUEZ) != 0) {
        registrar_evento("[Juez Mesa %d] No se pudo adquirir el mutex al iniciar la partida.\n", m->id_mesa);
        (void)pthread_mutex_destroy(&m->mutex_tablero);
        pthread_mutex_lock(&mutex_servidor);
        int idx = m->idx_pareja;
        m->ocupada = 0;
        m->mutex_inicializado = 0;
        parejas[idx].partida_terminada = 1;
        pthread_cond_broadcast(&parejas[idx].cond_pareja);
        pthread_mutex_unlock(&mutex_servidor);
        sem_post(&sem_mesas);
        return NULL;
    }
    inicializar_estado_mesa(m);
    m->partida_activa = 1;
    SnapshotMesa snapshot_inicio;
    capturar_snapshot_mesa(m, &snapshot_inicio);
    (void)desbloquear_mutex_tablero(m);
    imprimir_snapshot_mesa(&snapshot_inicio, "INICIO DE LA PARTIDA");

    /* El Juez publica la mesa solo cuando su mutex robusto ya esta operativo. */
    pthread_mutex_lock(&mutex_servidor);
    m->mutex_inicializado = 1;
    int idx_pareja_lista = m->idx_pareja;
    parejas[idx_pareja_lista].mesa_asignada = m->id_mesa;
    pthread_cond_broadcast(&parejas[idx_pareja_lista].cond_pareja);
    pthread_mutex_unlock(&mutex_servidor);

    pthread_t tid_puma;
    pthread_t tid_perros[NUM_PERROS];
    ArgFicha arg_puma;
    ArgFicha args_perros[NUM_PERROS];

    arg_puma.mesa = m;
    arg_puma.indice_perro = -1;
    arg_puma.seed_hilo = config_global.seed + (unsigned int)(m->num_partida * 100);
    int ok_puma = (pthread_create(&tid_puma, NULL, hilo_ficha_puma, &arg_puma) == 0);

    int ok_perros[NUM_PERROS] = {0};
    int todos_perros_creados = 1;
    for (int i = 0; i < NUM_PERROS; i++) {
        args_perros[i].mesa = m;
        args_perros[i].indice_perro = i;
        args_perros[i].seed_hilo = config_global.seed + (unsigned int)(m->num_partida * 100 + i + 1);
        ok_perros[i] = (pthread_create(&tid_perros[i], NULL, hilo_ficha_perro, &args_perros[i]) == 0);
        if (!ok_perros[i]) todos_perros_creados = 0;
    }

    if (!ok_puma || !todos_perros_creados) {
        if (bloquear_mutex_tablero(m, ROL_JUEZ) == 0) {
            /* La falta de recursos para crear hilos es un fallo interno del servidor. */
            m->fallo_interno = 1;
            (void)desbloquear_mutex_tablero(m);
        }
    }

    /* La simulacion de fallos pertenece al Juez y se une antes de destruir el mutex. */
    if (config_global.prob_fallo_milesimas > 0) {
        ArgFallo args_fallo[2] = {
            {m, ROL_PUMA, m->id_jugador_puma, 0},
            {m, ROL_PERROS, m->id_jugador_perros, 0}
        };
        pthread_t tids_fallo[2];
        int fallos_creados[2] = {0, 0};
        for (int i = 0; i < 2; i++) {
            if (pthread_create(&tids_fallo[i], NULL, hilo_fallo_simulado, &args_fallo[i]) == 0) {
                fallos_creados[i] = 1;
            } else if (bloquear_mutex_tablero(m, ROL_JUEZ) == 0) {
                m->fallo_interno = 1;
                (void)desbloquear_mutex_tablero(m);
            }
        }
        for (int i = 0; i < 2; i++) {
            if (fallos_creados[i]) (void)pthread_join(tids_fallo[i], NULL);
            if (args_fallo[i].fallo_inyectado) {
                registrar_evento("[Fallo] Se simulo la terminacion abrupta del rol %s en la Mesa %d.\n",
                                 i == 0 ? "Puma" : "Perros", m->id_mesa);
            }
        }
    }

    /* Bucle de supervision periodica del Juez Arbitro. */
    while (SERVIDOR_ACTIVO) {
        dormir_ms(40);
        if (bloquear_mutex_tablero(m, ROL_JUEZ) != 0) {
            break;
        }

        const char *evento_fin = NULL;
        if (!m->partida_activa) {
            (void)desbloquear_mutex_tablero(m);
            break;
        }

        if (m->fallo_jugador_puma && m->fallo_jugador_perros) {
            m->partida_activa = 0;
            evento_fin = "FALLO DE AMBOS JUGADORES - PARTIDA ABORTADA";
        } else if (m->fallo_jugador_puma) {
            m->partida_activa = 0;
            evento_fin = "FALLO/ABANDONO JUGADOR PUMA - GANAN LOS PERROS POR ABANDONO";
        } else if (m->fallo_jugador_perros) {
            m->partida_activa = 0;
            evento_fin = "FALLO/ABANDONO JUGADOR PERROS - GANA EL PUMA POR ABANDONO";
        } else if (m->fallo_interno) {
            m->partida_activa = 0;
            evento_fin = "FALLO INTERNO DEL SERVIDOR - PARTIDA ABORTADA";
        } else if (m->perros_capturados >= PERROS_PARA_GANAR) {
            m->partida_activa = 0;
            evento_fin = "VICTORIA DEL PUMA (6 perros capturados)";
        } else if (!puma_tiene_movimientos(m)) {
            m->partida_activa = 0;
            evento_fin = "VICTORIA DE LOS PERROS (Puma acorralado sin movimientos)";
        } else if (m->total_movimientos >= config_global.max_movimientos) {
            m->partida_activa = 0;
            evento_fin = "EMPATE TECNICO POR LIMITE MAXIMO DE MOVIMIENTOS";
        }

        SnapshotMesa snapshot_fin;
        int tiene_evento_fin = (evento_fin != NULL);
        if (tiene_evento_fin) capturar_snapshot_mesa(m, &snapshot_fin);
        (void)desbloquear_mutex_tablero(m);
        if (tiene_evento_fin) {
            imprimir_snapshot_mesa(&snapshot_fin, evento_fin);
            break;
        }
    }

    /* Ante SIGINT se ordena la detencion de la partida antes de unir los hilos. */
    if (bloquear_mutex_tablero(m, ROL_JUEZ) == 0) {
        int imprimir_fin = m->partida_activa;
        SnapshotMesa snapshot_fin_sigint;
        if (imprimir_fin) {
            m->partida_activa = 0;
            capturar_snapshot_mesa(m, &snapshot_fin_sigint);
        }
        (void)desbloquear_mutex_tablero(m);
        if (imprimir_fin) {
            imprimir_snapshot_mesa(&snapshot_fin_sigint,
                                   "PARTIDA FINALIZADA ORDENADAMENTE POR SIGINT (Ctrl+C)");
        }
    }

    if (ok_puma) (void)pthread_join(tid_puma, NULL);
    for (int i = 0; i < NUM_PERROS; i++) {
        if (ok_perros[i]) (void)pthread_join(tid_perros[i], NULL);
    }

    int rc_destroy = pthread_mutex_destroy(&m->mutex_tablero);

    pthread_mutex_lock(&mutex_servidor);
    int idx_p = m->idx_pareja;
    int jp = m->id_jugador_puma;
    int jd = m->id_jugador_perros;
    int id_mesa_liberada = m->id_mesa;
    int partida_liberada = m->num_partida;
    m->mutex_inicializado = 0;
    m->ocupada = 0;
    parejas[idx_p].partida_terminada = 1;
    pthread_cond_broadcast(&parejas[idx_p].cond_pareja);
    pthread_mutex_unlock(&mutex_servidor);

    if (rc_destroy != 0) {
        registrar_evento("[Juez Mesa %d] Advertencia: pthread_mutex_destroy devolvio %d.\n",
                         id_mesa_liberada, rc_destroy);
    }
    sem_post(&sem_mesas);
    registrar_evento("[Juez Mesa %d] Mesa liberada tras finalizar partida #%d (Jugadores #%d y #%d).\n",
                     id_mesa_liberada, partida_liberada, jp, jd);
    return NULL;
}

/*
 * Hilo que simula a cada uno de los N clientes/jugadores que llegan al servidor.
 */
static void *hilo_cliente(void *arg) {
    int id_cliente = *(int *)arg;
    free(arg);

    pthread_mutex_lock(&mutex_servidor);
    if (!SERVIDOR_ACTIVO) {
        pthread_mutex_unlock(&mutex_servidor);
        return NULL;
    }

    clientes_llegados++;
    registrar_evento("[Matchmaker] Llega el jugador #%d al servidor (Registrados hasta ahora: %d).\n",
                     id_cliente, clientes_llegados);

    if (pareja_en_espera_idx == -1) {
        /* El primer cliente disponible de la dupla asume el rol de Puma. */
        int mi_idx = contador_parejas++;
        parejas[mi_idx].id_puma = id_cliente;
        parejas[mi_idx].id_perros = 0;
        parejas[mi_idx].lista_para_jugar = 0;
        parejas[mi_idx].mesa_asignada = -1;
        parejas[mi_idx].partida_terminada = 0;
        pareja_en_espera_idx = mi_idx;

        /* Espera pasiva hasta que llegue su companero o se confirme que no quedan clientes. */
        while (!parejas[mi_idx].lista_para_jugar && SERVIDOR_ACTIVO) {
            if (lanzamiento_clientes_terminado &&
                clientes_llegados >= total_clientes_efectivos) {
                if (pareja_en_espera_idx == mi_idx) pareja_en_espera_idx = -1;
                registrar_evento("[Matchmaker] Jugador #%d queda sin pareja; no hay mas clientes disponibles.\n",
                                 id_cliente);
                pthread_mutex_unlock(&mutex_servidor);
                return NULL;
            }
            esperar_condicion_segura(&parejas[mi_idx].cond_pareja, &mutex_servidor);
        }

        if (!parejas[mi_idx].lista_para_jugar || !SERVIDOR_ACTIVO) {
            if (pareja_en_espera_idx == mi_idx) pareja_en_espera_idx = -1;
            pthread_mutex_unlock(&mutex_servidor);
            return NULL;
        }

        while (parejas[mi_idx].mesa_asignada == -1 &&
               !parejas[mi_idx].partida_terminada && SERVIDOR_ACTIVO) {
            esperar_condicion_segura(&parejas[mi_idx].cond_pareja, &mutex_servidor);
        }

        pthread_mutex_unlock(&mutex_servidor);

        pthread_mutex_lock(&mutex_servidor);
        while (!parejas[mi_idx].partida_terminada && SERVIDOR_ACTIVO) {
            esperar_condicion_segura(&parejas[mi_idx].cond_pareja, &mutex_servidor);
        }
        pthread_mutex_unlock(&mutex_servidor);
    } else {
        /* El segundo cliente completa la dupla y asume el rol de los Perros. */
        int mi_idx = pareja_en_espera_idx;
        pareja_en_espera_idx = -1;
        parejas[mi_idx].id_perros = id_cliente;
        parejas[mi_idx].lista_para_jugar = 1;
        int id_companero = parejas[mi_idx].id_puma;
        pthread_cond_broadcast(&parejas[mi_idx].cond_pareja);
        pthread_mutex_unlock(&mutex_servidor);

        registrar_evento("[Matchmaker] Emparejados Jugador #%d (Puma) y Jugador #%d (Perros). Esperando mesa libre...\n",
                         id_companero, id_cliente);

        /* El semaforo contador limita a M el numero de mesas simultaneamente reservadas. */
        while (sem_wait(&sem_mesas) == -1) {
            if (errno == EINTR && SERVIDOR_ACTIVO) continue;
            pthread_mutex_lock(&mutex_servidor);
            parejas[mi_idx].partida_terminada = 1;
            pthread_cond_broadcast(&parejas[mi_idx].cond_pareja);
            pthread_mutex_unlock(&mutex_servidor);
            return NULL;
        }

        pthread_mutex_lock(&mutex_servidor);
        if (!SERVIDOR_ACTIVO) {
            parejas[mi_idx].partida_terminada = 1;
            pthread_cond_broadcast(&parejas[mi_idx].cond_pareja);
            pthread_mutex_unlock(&mutex_servidor);
            sem_post(&sem_mesas);
            return NULL;
        }

        int idx_mesa = -1;
        for (int i = 0; i < max_mesas_M; i++) {
            if (!mesas[i].ocupada) {
                mesas[i].ocupada = 1;
                idx_mesa = i;
                break;
            }
        }

        /* El semaforo y el arreglo se verifican conjuntamente para evitar indice -1. */
        if (idx_mesa < 0) {
            parejas[mi_idx].partida_terminada = 1;
            pthread_cond_broadcast(&parejas[mi_idx].cond_pareja);
            pthread_mutex_unlock(&mutex_servidor);
            sem_post(&sem_mesas);
            registrar_evento("[Matchmaker] No se encontro una mesa libre pese a obtener un permiso.\n");
            return NULL;
        }

        contador_partidas++;
        int partida_local = contador_partidas;
        mesas[idx_mesa].num_partida = partida_local;
        mesas[idx_mesa].idx_pareja = mi_idx;
        mesas[idx_mesa].id_jugador_puma = id_companero;
        mesas[idx_mesa].id_jugador_perros = id_cliente;
        mesas[idx_mesa].partida_activa = 0;
        mesas[idx_mesa].mutex_inicializado = 0;
        mesas[idx_mesa].fallo_jugador_puma = 0;
        mesas[idx_mesa].fallo_jugador_perros = 0;
        mesas[idx_mesa].fallo_interno = 0;
        pthread_mutex_unlock(&mutex_servidor);

        registrar_evento("[Matchmaker] Partida #%d: pareja Puma=#%d / Perros=#%d reservada en Mesa %d.\n",
                         partida_local, id_companero, id_cliente, idx_mesa);

        pthread_t tid_juez;
        if (pthread_create(&tid_juez, NULL, hilo_juez_mesa, &mesas[idx_mesa]) == 0) {
            (void)pthread_join(tid_juez, NULL);
        } else {
            pthread_mutex_lock(&mutex_servidor);
            mesas[idx_mesa].ocupada = 0;
            parejas[mi_idx].partida_terminada = 1;
            pthread_cond_broadcast(&parejas[mi_idx].cond_pareja);
            pthread_mutex_unlock(&mutex_servidor);
            sem_post(&sem_mesas);
            registrar_evento("[Matchmaker] No se pudo crear el Juez para la Mesa %d.\n", idx_mesa);
        }
    }

    return NULL;
}

static int parsear_entero_positivo(const char *texto, int *salida) {
    errno = 0;
    char *fin = NULL;
    long valor = strtol(texto, &fin, 10);
    if (errno != 0 || fin == texto || valor <= 0 || valor > INT_MAX) return 0;
    while (isspace((unsigned char)*fin)) fin++;
    if (*fin != '\0') return 0;
    *salida = (int)valor;
    return 1;
}

int main(int argc, char *argv[]) {
    if (argc < 3 || argc > 4) {
        fprintf(stderr, "Uso correcto: %s N M [config.txt]\n", argv[0]);
        fprintf(stderr, "  N: Numero total de clientes/jugadores a procesar (ej: 4)\n");
        fprintf(stderr, "  M: Numero maximo de mesas concurrentes (ej: 2)\n");
        return EXIT_FAILURE;
    }

    if (!parsear_entero_positivo(argv[1], &total_clientes_N) ||
        !parsear_entero_positivo(argv[2], &max_mesas_M)) {
        fprintf(stderr, "Error: N y M deben ser enteros positivos representables.\n");
        return EXIT_FAILURE;
    }

    if (!atomic_is_lock_free(&servidor_activo)) {
        fprintf(stderr, "Error: la bandera atomica de SIGINT no es lock-free en esta plataforma.\n");
        return EXIT_FAILURE;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = manejador_sigint;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (sigaction(SIGINT, &sa, NULL) != 0) {
        perror("Error al instalar manejador SIGINT");
        return EXIT_FAILURE;
    }

    archivo_log = fopen("torneo.log", "w");
    if (archivo_log != NULL) {
        fprintf(archivo_log, "=== Inicio del Torneo de Komikan Concurrente (N=%d, M=%d) ===\n",
                total_clientes_N, max_mesas_M);
        fflush(archivo_log);
    }

    leer_configuracion((argc == 4) ? argv[3] : NULL);

    if (sem_init(&sem_mesas, 0, (unsigned int)max_mesas_M) != 0) {
        perror("Error al inicializar sem_mesas");
        if (archivo_log != NULL) fclose(archivo_log);
        return EXIT_FAILURE;
    }

    max_parejas = (total_clientes_N / 2) + 2;
    mesas = calloc((size_t)max_mesas_M, sizeof(*mesas));
    parejas = calloc((size_t)max_parejas, sizeof(*parejas));
    pthread_t *hilos_clientes = calloc((size_t)total_clientes_N, sizeof(*hilos_clientes));

    if (mesas == NULL || parejas == NULL || hilos_clientes == NULL) {
        fprintf(stderr, "Error al reservar memoria dinamica para el torneo.\n");
        free(mesas);
        free(parejas);
        free(hilos_clientes);
        sem_destroy(&sem_mesas);
        if (archivo_log != NULL) fclose(archivo_log);
        return EXIT_FAILURE;
    }

    int parejas_inicializadas = 0;
    for (int i = 0; i < max_mesas_M; i++) {
        mesas[i].id_mesa = i;
        mesas[i].ocupada = 0;
        mesas[i].mutex_inicializado = 0;
    }

    for (int i = 0; i < max_parejas; i++) {
        int rc = pthread_cond_init(&parejas[i].cond_pareja, NULL);
        if (rc != 0) {
            registrar_evento("[Servidor] No se pudo inicializar la condicion de pareja %d: %d.\n", i, rc);
            goto error_inicializacion;
        }
        parejas_inicializadas++;
    }

    registrar_evento("=== Servidor de Torneo Komikan Iniciado: N=%d jugadores, M=%d mesas maximas ===\n",
                     total_clientes_N, max_mesas_M);

    /* Crear concurrentemente los N clientes y guardar solo los identificadores de hilo validos. */
    int clientes_creados = 0;
    for (int i = 0; i < total_clientes_N && SERVIDOR_ACTIVO; i++) {
        int *id_ptr = malloc(sizeof(*id_ptr));
        if (id_ptr == NULL) {
            registrar_evento("[Servidor] Memoria insuficiente al crear el cliente #%d.\n", i + 1);
            break;
        }
        *id_ptr = i + 1;
        if (pthread_create(&hilos_clientes[clientes_creados], NULL, hilo_cliente, id_ptr) == 0) {
            clientes_creados++;
        } else {
            free(id_ptr);
            registrar_evento("[Servidor] No se pudo crear el hilo del cliente #%d.\n", i + 1);
        }
        dormir_ms(15);
    }

    /* Publica el total real de hilos creados para resolver correctamente parejas impares/fallidas. */
    pthread_mutex_lock(&mutex_servidor);
    total_clientes_efectivos = clientes_creados;
    lanzamiento_clientes_terminado = 1;
    for (int i = 0; i < max_parejas; i++) {
        pthread_cond_broadcast(&parejas[i].cond_pareja);
    }
    pthread_mutex_unlock(&mutex_servidor);

    if (!SERVIDOR_ACTIVO) {
        registrar_evento("\n[Servidor] SIGINT detectada. Se detiene la llegada de clientes y se cierran las partidas activas.\n");
    }

    for (int i = 0; i < clientes_creados; i++) {
        (void)pthread_join(hilos_clientes[i], NULL);
    }

    for (int i = 0; i < parejas_inicializadas; i++) {
        int rc = pthread_cond_destroy(&parejas[i].cond_pareja);
        if (rc != 0) registrar_evento("[Servidor] Error al destruir condicion de pareja %d: %d.\n", i, rc);
    }

    pthread_mutex_destroy(&mutex_servidor);
    pthread_mutex_destroy(&mutex_log);
    sem_destroy(&sem_mesas);

    free(mesas);
    free(parejas);
    free(hilos_clientes);

    if (archivo_log != NULL) {
        fprintf(archivo_log, "=== Servidor finalizado; recursos dinamicos y primitivas liberados ===\n");
        fclose(archivo_log);
    }

    printf("\n=== Torneo finalizado. Recursos del servidor liberados ===\n");
    return EXIT_SUCCESS;

error_inicializacion:
    for (int i = 0; i < parejas_inicializadas; i++) {
        pthread_cond_destroy(&parejas[i].cond_pareja);
    }
    free(mesas);
    free(parejas);
    free(hilos_clientes);
    sem_destroy(&sem_mesas);
    if (archivo_log != NULL) fclose(archivo_log);
    return EXIT_FAILURE;
}
