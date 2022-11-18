CC=gcc
CFLAGS=-g -pedantic -O2 -std=gnu17 -Wall -Wextra -Werror
LDFLAGS=-pthread

.PHONY: all
all: nyuenc

nyuenc: nyuenc.o 

nyuenc.o: nyuenc.c 

.PHONY: clean
clean:
	rm -f *.o nyush
