#include "khash.h"
#include<stdlib.h>
#include<stdio.h>
#include<fcntl.h>
#include<unistd.h>
#include<pthread.h>
#include<sys/time.h>
#include<time.h>
#include<string.h>
#include <semaphore.h>

#define LINE_BUFFER 4096
#define KEY_BUFFER 1048576
#define NUM_THREAD 4
KHASH_MAP_INIT_STR(symbol, long long int)

typedef struct _IO_FILE FILE;
typedef khash_t(symbol) hashtable;
typedef struct {
    int h1_index;
    int h2_index;
    hashtable *h1;
    hashtable *h2;
} merge_hashtable_args;


char* request_buffer(FILE* file, int size);
void parse_line(char *line, hashtable *h);
void *pthread_parse_file(void* argv);
void *pthread_merge_hashtable(void* args);
char* request_buffer(FILE* file, int size);
void save_result(char* filename, hashtable* h);
void sequential(FILE *file, hashtable *h);
void parallel(FILE *file, hashtable *h);

FILE *file;
pthread_mutex_t file_lock, hash_table_lock;
hashtable *h_pll;
struct timespec start, end;
double elapsed_time;

int parse_finish = 0;
// For buffer
char *key_buffer[KEY_BUFFER];
int key_buffer_put_idx = 0, key_buffer_get_idx = 0;
sem_t sem_put, sem_get;
pthread_mutex_t key_buffer_lock;

int main(int argc, char **argv) {
    // Read the file
    if (!(file = fopen(argv[1], "r"))) {
        printf("[-] Unable to open the specified log file %s\n", argv[1]);
        return -1;
    }
    fclose(file);
    pthread_mutex_init(&file_lock, NULL);
    pthread_mutex_init(&hash_table_lock, NULL);
    // /*
    printf(" === Single Thread === \n");
    gettimeofday(&start, NULL);
    file = fopen(argv[1], "r");
    hashtable *h_seq = kh_init(symbol);
    sequential(file, h_seq);
    gettimeofday(&end, NULL);
    elapsed_time = end.tv_sec - start.tv_sec + 0.000001 * (end.tv_usec - start.tv_usec);
    printf("Single thread elapsed time: %.6f sec\n", elapsed_time);
    fclose(file);
    save_result("__output_sequential.txt", h_seq);
    kh_destroy(symbol, h_seq);
    */

    printf(" === Multiple Thread === \n");
    // gettimeofday(&start, NULL);
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start);
    file = fopen(argv[1], "r");
    // Initialize hash table
    h_pll = kh_init(symbol);
    parallel(file, h_pll);
    // gettimeofday(&end, NULL);
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end);
    elapsed_time = end.tv_sec - start.tv_sec + 0.000000001 * (end.tv_nsec - start.tv_nsec);
    printf("%6d thread elapsed time: %.6f sec\n", NUM_THREAD, elapsed_time);
    save_result("__output_parallel.txt", h_pll);
    fclose(file);

    pthread_mutex_destroy(&file_lock);
    pthread_mutex_destroy(&hash_table_lock);
    kh_destroy(symbol, h_pll);
    return 0;
}

void parse_line(char *line, hashtable *h) {
    char *c = line;
    char word[LINE_BUFFER];
    int word_idx = 0;
    long long int k;
    int ret;
    while(1) {
        if ((*c == ' ' || *c == '\n' || *c == '\0') && word_idx > 0) {
            word[word_idx] = '\0';
            char *tmp_word;
            int len = strlen (word) + 1;
            // tmp_word = malloc (len);
            // strncpy (tmp_word, word, len);
            tmp_word = strdup(word);
            // pthread_mutex_lock(&hash_table_lock);
            k = kh_get(symbol, h, tmp_word);
            if (k == kh_end(h)) {
                k = kh_put(symbol, h, tmp_word, &ret);
                kh_value(h, k) = 0;
            }
            kh_value(h, k) += 1;
            // pthread_mutex_unlock(&hash_table_lock);
            word_idx = 0;
        }
        else {
            word[word_idx++] = *c;
        }
        if (*c == '\0') break;
        c++;
    }
}

void *pthread_parse_file(void* argv) {
    hashtable *h = (hashtable*) argv;
    while (1) {
        // Read from file
        char* buf = request_buffer(file, LINE_BUFFER);
        if (buf[0] == '\0') {
            free(buf);
            break;
        }
        // Do the processing
        parse_line(buf, h);
        free(buf);
    }
}

void *pthraed_buf_to_hashtable(void* argv) {
    hashtable *h = (hashtable*)argv;
    int k, ret;
    while(1) {
        sem_wait(&sem_get);
        // TODO: find a better way to deal with finish problem
        if (parse_finish) {
            // printf("put idx %d get idx %d, sem_put %d\n", key_buffer_put_idx, key_buffer_get_idx, sem_get_val);
            break;
        }
        pthread_mutex_lock(&key_buffer_lock);
        const char* word = key_buffer[key_buffer_get_idx];
        pthread_mutex_unlock(&key_buffer_lock);
        sem_post(&sem_put);
        key_buffer_get_idx = (key_buffer_get_idx + 1) % KEY_BUFFER;
        k = kh_get(symbol, h, word);
        if (k == kh_end(h)) {
            k = kh_put(symbol, h, word, &ret);
            kh_value(h, k) = 0;
        }
        kh_value(h, k) += 1;
        // printf("Get %s with put idx %d get idx %d, sem_put %d\n", word, key_buffer_put_idx, key_buffer_get_idx, sem_get_val);
    }
}

char* request_buffer(FILE* file, int size) {
    char *s = NULL;
    char *buf = (char*)malloc(sizeof(char) * size);
    pthread_mutex_lock(&file_lock);
    s = fgets(buf, size, file);
    pthread_mutex_unlock(&file_lock);
    if (s == NULL) buf[0] = '\0';
    return buf;
}

void save_result(char* filename, hashtable* h) {
    FILE *fptr;
    fptr = fopen(filename, "w");
    for (int k = kh_begin(h); k != kh_end(h); ++k) {
        if (kh_exist(h, k))
            fprintf(fptr, "%s: %lld\n", kh_key(h, k), kh_value(h, k));
    }
    fclose(fptr);
}

void sequential(FILE *file, hashtable *h) {
    while (1) {
        // Read from file
        char *buf = (char*)malloc(sizeof(char) * LINE_BUFFER);
        char* s = fgets(buf, LINE_BUFFER, file);
        if (s == NULL) buf[0] = '\0';
        // If get noting, break
        if (buf[0] == '\0') {
            free(buf);
            break;
        }
        // Do the processing
        parse_line(buf, h);
        free(buf);
    }
}

void parallel(FILE *file, hashtable *h) {
    
    sem_init(&sem_put, 0, KEY_BUFFER);
    sem_init(&sem_get, 0, 0);
    pthread_mutex_init(&key_buffer_lock, NULL);
    parse_finish = 0;

    pthread_t *thread_pool = (pthread_t*)malloc(sizeof(pthread_t) * NUM_THREAD);
    pthread_t buf_to_hashtable_thread;
    hashtable *h_pool[NUM_THREAD];
    h_pool[0] = h;
    for (int i = 1; i < NUM_THREAD; ++i) h_pool[i] = kh_init(symbol);

    // Create thread 
    for (int i = 0; i < NUM_THREAD; ++i) {
        pthread_create(&thread_pool[i], NULL, pthread_parse_file, (void *)h_pool[i]);
    }

    // Wait for thread complete
    for (int i = 0; i < NUM_THREAD; ++i) {
        pthread_join(thread_pool[i], NULL);
    }

    // struct timeval thread_finish;
    // gettimeofday(&thread_finish, NULL);
    // elapsed_time = thread_finish.tv_sec - start.tv_sec + 0.000001 * (thread_finish.tv_usec - start.tv_usec);
    // printf("%6d thread finish parse: %.6f sec\n", NUM_THREAD, elapsed_time);

    // Clean up
    for(int i = 1; i < NUM_THREAD; ++i) {
        kh_destroy(symbol, h_pool[i]);
    }
    free(thread_pool);
    sem_destroy(&sem_put);
    sem_destroy(&sem_put);
    pthread_mutex_destroy(&key_buffer_lock);
} 