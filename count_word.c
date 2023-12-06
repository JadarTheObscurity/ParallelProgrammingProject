#include "khash.h"
#include<stdlib.h>
#include<stdio.h>
#include<fcntl.h>
#include<unistd.h>
#include<pthread.h>
#include<sys/time.h>
#include<string.h>

#define LINE_BUFFER 1048576
#define NUM_THREAD 4
KHASH_MAP_INIT_STR(symbol, long long int)

typedef struct _IO_FILE FILE;
typedef khash_t(symbol) hashtable;
typedef struct {
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
struct timeval start, end;
double elapsed_time;

int main(int argc, char **argv) {
    // Read the file
    if (!(file = fopen(argv[1], "r"))) {
        printf("[-] Unable to open the specified log file %s\n", argv[1]);
        return -1;
    }
    fclose(file);
    pthread_mutex_init(&file_lock, NULL);
    pthread_mutex_init(&hash_table_lock, NULL);
    /*
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
    gettimeofday(&start, NULL);
    file = fopen(argv[1], "r");
    // Initialize hash table
    h_pll = kh_init(symbol);
    parallel(file, h_pll);
    gettimeofday(&end, NULL);
    elapsed_time = end.tv_sec - start.tv_sec + 0.000001 * (end.tv_usec - start.tv_usec);
    printf("%6d thread elapsed time: %.6f sec\n", NUM_THREAD, elapsed_time);
    save_result("__output_parallel.txt", h_pll);
    fclose(file);

    pthread_mutex_destroy(&file_lock);
    pthread_mutex_destroy(&hash_table_lock);
    kh_destroy(symbol, h_pll);
    // for(int i = 0; i < NUM_THREAD; ++i) kh_destroy(symbol, h_pool[i]);
    return 0;
}
void parse_line(char *line, hashtable *h) {
    char *c = line;
    char word[LINE_BUFFER];
    int word_idx = 0;
    long long int k;
    int ret;
    while(*c != '\0') {
        if (*c == ' ' || *c == '\n') {
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
        c++;
    }
}

void *pthread_parse_file(void* argv) {
    hashtable *h = (hashtable*) argv;
    while (1) {
        // Read from file
        char* buf = request_buffer(file, LINE_BUFFER);
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
void *pthread_merge_hashtable(void* args) {
    merge_hashtable_args *h_args = (merge_hashtable_args*) args;
    hashtable *h1 = h_args->h1;
    hashtable *h2 = h_args->h2;
    int k1, k2; // key for h1
    int ret;
    // Loop through all keys in h2
    for (k2 = kh_begin(h2); k2 != kh_end(h2); ++k2) {
        if (kh_exist(h2, k2)) {
            const char *c = kh_key(h2, k2);
            k1 = kh_get(symbol, h1, c);
            // If k2 not in h1, add it
            if (k1 == kh_end(h1)) {
                k1 = kh_put(symbol, h1, c, &ret);
                kh_value(h1, k1) = 0;
            }
            kh_value(h1, k1) += kh_value(h2, k2);
        }
    }
    free(args);
}

char* request_buffer(FILE* file, int size) {
    char *buf = (char*)malloc(sizeof(char) * size);
    pthread_mutex_lock(&file_lock);
    int sz = fread(buf, sizeof(char), size, file);
    pthread_mutex_unlock(&file_lock);
    buf[sz] = '\0';
    return buf;
}

void save_result(char* filename, hashtable* h) {
    FILE *fptr;
    fptr = fopen(filename, "w");

    for (int k = kh_begin(h); k != kh_end(h); ++k) {
        if (kh_exist(h, k)) {
            fprintf(fptr, "%s: %lld\n", kh_key(h, k), kh_value(h, k));
        }
    }
    fclose(fptr);
}

void sequential(FILE *file, hashtable *h) {
    while (1) {
        // Read from file
        char *buf = (char*)malloc(sizeof(char) * LINE_BUFFER);
        int sz = fread(buf, sizeof(char), LINE_BUFFER, file);
        buf[sz] = '\0';
        // If get noting, break
        if (buf[0] == '\0') {
            break;
        }
        // Do the processing
        parse_line(buf, h);
        free(buf);
    }
}

void parallel(FILE *file, hashtable *h) {
    pthread_t *thread_pool = (pthread_t*)malloc(sizeof(pthread_t) * NUM_THREAD);
    hashtable *h_pool[NUM_THREAD];
    h_pool[0] = h;
    for (int i = 1; i < NUM_THREAD; ++i) h_pool[i] = kh_init(symbol);

    for (int i = 0; i < NUM_THREAD; ++i) {
        pthread_create(&thread_pool[i], NULL, pthread_parse_file, (void *)h_pool[i]);
    }
    for (int i = 0; i < NUM_THREAD; ++i) {
        pthread_join(thread_pool[i], NULL);
    }
    struct timeval thread_finish;
    gettimeofday(&thread_finish, NULL);
    elapsed_time = thread_finish.tv_sec - start.tv_sec + 0.000001 * (thread_finish.tv_usec - start.tv_usec);
    printf("%6d thread finish parse: %.6f sec\n", NUM_THREAD, elapsed_time);
    int num_hash_table = NUM_THREAD;
    // TODO: Deal with num_hash_table not a power of 2
    while(num_hash_table > 1) {
        num_hash_table >>= 1;
        for (int i = 0; i < num_hash_table; ++i) {
            merge_hashtable_args *args = (merge_hashtable_args*)malloc(sizeof(merge_hashtable_args));
            args->h1 = h_pool[i % NUM_THREAD];
            args->h2 = h_pool[(i+num_hash_table) % NUM_THREAD];
            pthread_create(&thread_pool[i], NULL, pthread_merge_hashtable, (void *)args);
            printf("Merge %d %d\n", i, i + num_hash_table);
        }
        for (int i = 0; i < num_hash_table; ++i) {
            pthread_join(thread_pool[i], NULL);
        }
        struct timeval merge_finish;
        gettimeofday(&merge_finish, NULL);
        elapsed_time = merge_finish.tv_sec - start.tv_sec + 0.000001 * (merge_finish.tv_usec - start.tv_usec);
        printf("%6d thread finish merge: %.6f sec\n", NUM_THREAD, elapsed_time);

    }
    // hashtable *h_pll = h_pool[0];
    // long long int  k;
    // int ret;
    // for (int i = 1; i < NUM_THREAD; i++) {
    //     hashtable *hi = h_pool[i];
    //     for (int ki = kh_begin(hi); ki != kh_end(hi); ++ki) {
    //         if (kh_exist(hi, ki)) {
    //             const char *c = kh_key(hi, ki);
    //             // insert into h
    //             k = kh_get(symbol, h_pll, c);
    //             if (k == kh_end(h_pll)) {
    //                 k = kh_put(symbol, h_pll, c, &ret);
    //                 kh_value(h_pll, k) = 0;
    //             }
    //             kh_value(h_pll, k) += kh_value(hi, ki);
    //         }
    //     }
    // }
    for(int i = 1; i < NUM_THREAD; ++i) {
        kh_destroy(symbol, h_pool[i]);
    }
    free(thread_pool);

} 