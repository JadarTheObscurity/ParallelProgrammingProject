#include "khash.h"
#include<stdlib.h>
#include<stdio.h>
#include<fcntl.h>
#include<unistd.h>
#include<pthread.h>
#include<sys/time.h>

#define LINE_BUFFER 4096
#define NUM_THREAD 4
typedef struct _IO_FILE FILE;

KHASH_MAP_INIT_INT(symbol, long long int)
FILE *file;

char* request_buffer(FILE* file, int size);
void parse_line(char *line, khash_t(symbol) *h) {
    char *c = line;
    long long int k;
    int ret;
    while(*c != '\0') {
        k = kh_get(symbol, h, *c);
        if (k == kh_end(h)) {
            k = kh_put(symbol, h, *c, &ret);
            kh_value(h, k) = 0;
        }
        kh_value(h, k) += 1;
        c++;
    }
}

void *pthread_parse_file(void* argv) {
    khash_t(symbol) *h = (khash_t(symbol)*) argv;
    while (1) {
        // Read from file
        char* buf = request_buffer(file, LINE_BUFFER);
        // If get noting, break
        if (buf[0] == '\0') {
            break;
        }
        // Do the processing
        parse_line(buf, h);
    }
}

pthread_mutex_t file_lock;
char* request_buffer(FILE* file, int size) {
    char *buf = (char*)malloc(sizeof(char) * size);
    pthread_mutex_lock(&file_lock);
    int sz = fread(buf, sizeof(char), size, file);
    pthread_mutex_unlock(&file_lock);
    buf[sz] = '\0';
    return buf;
}

void save_result(char* filename, khash_t(symbol)* h) {
    FILE *fptr;

    // Open a file in writing mode
    fptr = fopen(filename, "w");
    // Write some text to the file
    for (int k = kh_begin(h); k != kh_end(h); ++k) {
        if (kh_exist(h, k)) {
            fprintf(fptr, "%c: %lld\n", kh_key(h, k), kh_value(h, k));
        }
    }

}

struct timeval start, end;
int main(int argc, char **argv) {
    pthread_mutex_init(&file_lock, NULL);

    // Read the file
    if (!fopen(argv[1], "r")) {
        printf("[-] Unable to open the specified log file %s\n", argv[1]);
        return -1;
    }
    // Single Thread
    gettimeofday(&start, NULL);
    file = fopen(argv[1], "r");
    khash_t(symbol) *h_seq = kh_init(symbol);
    while (1) {
        // Read from file
        char* buf = request_buffer(file, LINE_BUFFER);
        // If get noting, break
        if (buf[0] == '\0') {
            break;
        }
        // Do the processing
        parse_line(buf, h_seq);
    }
    gettimeofday(&end, NULL);
    double elapsed_time = end.tv_sec - start.tv_sec + 0.000001 * (end.tv_usec - start.tv_usec);
    printf("Single thread elapsed time: %.6f sec\n", elapsed_time);
    save_result("__output_sequential.txt", h_seq);


    // Multiple Thread
    gettimeofday(&start, NULL);
    file = fopen(argv[1], "r");
    // Initialize hash table
    khash_t(symbol) *h_pool[NUM_THREAD];
    for (int i = 0; i < NUM_THREAD; ++i) h_pool[i] = kh_init(symbol);
    pthread_t *thread_pool = (pthread_t*)malloc(sizeof(pthread_t) * NUM_THREAD);

    for (int i = 0; i < NUM_THREAD; ++i) {
        pthread_create(&thread_pool[i], NULL, pthread_parse_file, (void *)h_pool[i]);
    }
    for (int i = 0; i < NUM_THREAD; ++i) {
        pthread_join(thread_pool[i], NULL);
    }
    pthread_mutex_destroy(&file_lock);
    
    // Merge the results to h_pool[0]
    khash_t(symbol) *h_pll = h_pool[0];
    long long int  k;
    int ret;
    for (int i = 1; i < NUM_THREAD; i++) {
        khash_t(symbol) *hi = h_pool[i];
        for (int ki = kh_begin(hi); ki != kh_end(hi); ++ki) {
            if (kh_exist(hi, ki)) {
                char c = kh_key(hi, ki);
                // insert into h
                k = kh_get(symbol, h_pll, c);
                if (k == kh_end(h_pll)) {
                    k = kh_put(symbol, h_pll, c, &ret);
                    kh_value(h_pll, k) = 0;
                }
                kh_value(h_pll, k) += kh_value(hi, ki);
            }
        }
    }
    gettimeofday(&end, NULL);
    elapsed_time = end.tv_sec - start.tv_sec + 0.000001 * (end.tv_usec - start.tv_usec);
    printf("%6d thread elapsed time: %.6f sec\n", NUM_THREAD, elapsed_time);
    save_result("__output_parallel.txt", h_pll);
    return 0;
}