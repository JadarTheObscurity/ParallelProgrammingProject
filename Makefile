CC=gcc
CFLAGS=-I. -pthread -O3

count_char: count_char.c
	$(CC) -o count_char.out count_char.c $(CFLAGS)
