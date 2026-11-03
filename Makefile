# Makefile - Tarea 2: Torneo de Komikan Concurrente
# Autores: Sebastian Quintero y Alan Troncoso

CC = gcc
CFLAGS = -Wall -Wextra -std=c17 -pthread
TARGET = torneo
SRC = torneo.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC)

run: $(TARGET)
	./$(TARGET) 4 2 config.txt

run_fallos: $(TARGET)
	./$(TARGET) 4 2 config_fallos.txt

clean:
	rm -f $(TARGET) torneo.log

.PHONY: all run run_fallos clean
