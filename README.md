# Tarea 2 - Sistemas Operativos: Torneo de Komikan Concurrente

**Autores:** Sebastián Quintero y Alan Troncoso

## Contenido del paquete
- `torneo.c`: Código fuente principal en C17 utilizando `pthread` y semáforos POSIX.
- `Makefile`: Script de automatización para compilar, ejecutar pruebas y limpiar binarios/logs.
- `config.txt`: Archivo de configuración estándar con el tablero inicial por defecto y sin fallos (`PROB_FALLO=0`).
- `config_fallos.txt`: Archivo de configuración de prueba con probabilidad de abandono/fallo activa (`PROB_FALLO=15`).
- `README.md`: Instrucciones de compilación y ejecución.

## Requisitos
- Compilador `gcc` con soporte para estándar `c17` y `pthread` (Entorno Linux / Ubuntu).
- Utilidad `make` (opcional, también se puede compilar directamente con `gcc`).

## Compilación

Usando `make`:
```bash
make
```

O compilando manualmente con `gcc`:
```bash
gcc -Wall -Wextra -std=c17 -pthread -o torneo torneo.c
```

## Ejecución

Sintaxis general:
```bash
./torneo N M [archivo_config]
```
Donde:
- `N`: Cantidad total de clientes/jugadores que llegan al torneo (entero positivo).
- `M`: Cantidad de mesas disponibles en paralelo (entero positivo).
- `[archivo_config]`: (Opcional) Ruta al archivo de configuración.

### Ejemplos rápidos:
1. Ejecutar con 4 jugadores, 2 mesas y la configuración estándar:
   ```bash
   make run
   # O directamente: ./torneo 4 2 config.txt
   ```

2. Ejecutar prueba de tolerancia a fallos (abandono de jugadores):
   ```bash
   make run_fallos
   # O directamente: ./torneo 4 2 config_fallos.txt
   ```

3. Limpiar archivos generados (`torneo` y `torneo.log`):
   ```bash
   make clean
   ```
