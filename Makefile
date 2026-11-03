# Torneo de Komikan Concurrente - Tarea 2 SO
# Uso rapido:  make run N=6 M=2            make run N=6 M=2 CFG=configs/config.txt
CC      ?= gcc
CFLAGS  := -Wall -Wextra -std=c17 -pthread
SRC     := src/torneo.c
N       ?= 6
M       ?= 2
CFG     ?=

.PHONY: all debug asan tsan run test clean

all: torneo

torneo: $(SRC)
	$(CC) $(CFLAGS) -O2 -o $@ $<

debug: $(SRC)
	$(CC) $(CFLAGS) -g -O0 -o torneo_debug $<

asan: $(SRC)
	$(CC) $(CFLAGS) -g -O1 -fsanitize=address,undefined -fno-omit-frame-pointer -o torneo_asan $<

tsan: $(SRC)
	$(CC) $(CFLAGS) -g -O1 -fsanitize=thread -o torneo_tsan $<

run: torneo
	./torneo $(N) $(M) $(CFG)

test: asan
	bash scripts/test.sh

clean:
	rm -f torneo torneo_debug torneo_asan torneo_tsan torneo.log
