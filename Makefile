CC=gcc
CFLAGS=-I. -pthread -O3 -ggdb3

all: count_char count_word	

count_char: count_char.c 
	$(CC) -o count_char.out count_char.c $(CFLAGS)

count_word: count_word.c
	$(CC) -o count_word.out count_word.c $(CFLAGS)