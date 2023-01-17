#include <stdio.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <pthread.h>

const int threshold = 32;
// pthread_mutex_t front_lock = PTHREAD_MUTEX_INITIALIZER, back_lock = PTHREAD_MUTEX_INITIALIZER;
size_t max_bit_len = 32;

// To access records cast input by rec_t
typedef struct rec {
    int key;
    char value[96]; // 96 bytes
} rec_t;

typedef struct {
    int key;
    char *value; // 96 bytes
    // rec_t *rec; // 100 bytes
} k_t;

char *input, *output;
size_t sz;
int rec_n;
int fd2;

k_t *sorting;
k_t *front, *back;
int front_n = 0, back_n = 0;

void err_msg()
{
    fprintf(stderr, "An error has occurred\n");
    exit(0);
}

rec_t *get(char *data, int index)
{
    return (rec_t *)(data+100*index);
}


// From https://stackoverflow.com/questions/5656530/how-to-use-shared-memory-with-linux-in-c
void* create_shared_memory(size_t size) 
{
  // Our memory buffer will be readable and writable:
  int protection = PROT_READ | PROT_WRITE;

  // The buffer will be shared (meaning other processes can access it), but
  // anonymous (meaning third-party processes cannot obtain an address for it),
  // so only this process and its children will be able to use it:
  int visibility = MAP_SHARED | MAP_ANONYMOUS;

  // The remaining parameters to `mmap()` are not important for this use case,
  // but the manpage for `mmap` explains their purpose.
  return mmap(NULL, size, protection, visibility, -1, 0);
}

void load(char *fin, char *fout)
{
    // Open file
    int fd = open(fin, O_RDONLY);
    fd2 = open(fout, O_RDWR | O_CREAT, (mode_t)0600);
    if (fd == -1 || fd2 == -1) err_msg();

    // Get filesize
    struct stat st;
    stat(fin, &st);
    sz = st.st_size;
    if (sz == 0) err_msg();

    int res = ftruncate(fd2, sz);
    if (res == -1) err_msg();
    
    rec_n = sz/sizeof(rec_t);
    // sorting = malloc(rec_n * sizeof(k_t));
    sorting = create_shared_memory(rec_n * sizeof(k_t));

    if (rec_n > threshold) 
    {
        front = malloc(rec_n * sizeof(k_t));
        back = malloc(rec_n * sizeof(k_t));
    }

    // Map file contents to memory
    input = mmap (
        0, 
        sz,
        PROT_READ, 
        MAP_SHARED,
        fd, 
        0
    );

    // output = mmap (
    //     NULL, 
    //     sz, 
    //     PROT_READ | PROT_WRITE, 
    //     MAP_SHARED | MAP_ANONYMOUS, 
    //     fd2, 
    //     0
    // );

    output = mmap (
        0,
        sz, 
        PROT_READ | PROT_WRITE, 
        MAP_SHARED, 
        fd2, 
        0
    );

    // perror("mmap");

    // Create sorting array of keys (helps with locality and sorting)
    for (int i = 0; i < rec_n; i++) 
    {
        // Might need to manipulate the first bit of the key
        sorting[i].key = get(input, i)->key;
        sorting[i].value = (char *)&get(input, i)->value;
    }

    // Clean up memory
    close(fd);
}

void save()
{
    msync(output, sz, MS_SYNC);
    close(fd2);
}

void unload()
{
    munmap(input, sz);
    munmap(output, sz);
    munmap(sorting, rec_n * sizeof(k_t));
    // free(sorting);
    if (rec_n > threshold) 
    {
        free(front);
        free(back);
    }
}

void swap(int *a, int *b)
{
    if (*a == *b) return;
    int temp = *a;
    *a = *b;
    *b = temp;
}

void rec_swap(k_t *a, k_t *b)
{
    if (a == b) return;
    k_t temp = *a;
    *a = *b;
    *b = temp;
}

k_t *part(k_t *lower, k_t *upper) 
{
    k_t *l = lower;
    for (k_t *c = lower; c < upper; c++)
        if (c->key < upper->key)
            rec_swap(c, l++);
    rec_swap(l, upper);
    return l;
}

void serial_sort(k_t *lower, k_t *upper)
{
    // Quick sort
    if (lower < upper) 
    {
        k_t *pivot = part(lower, upper);
        serial_sort(lower, pivot - 1);
        serial_sort(pivot + 1, upper);
    }
}

size_t bucket_mask = 3;

void *front_bucket()
{
    front_n = 0;
    for (int i = 0; i < rec_n; i++)
        if (0 == (sorting[i].key & bucket_mask)) 
            front[front_n++] = sorting[i];
    memcpy(sorting, front, front_n * sizeof(k_t));
    pthread_exit(NULL);
}

void *back_bucket()
{
    back_n = 0;
    for (int i = 0; i < rec_n; i++)
        if (sorting[i].key & bucket_mask) 
            back[back_n++] = sorting[i];
    pthread_exit(NULL);
}

void bucket_sort()
{
    front_n = back_n = 0;
    for (int i = 0; i < rec_n; i++)
        if (sorting[i].key & bucket_mask) back[back_n++] = sorting[i];
        else front[front_n++] = sorting[i];
    memcpy(sorting, front, front_n * sizeof(k_t));
    memcpy(sorting + front_n, back, back_n * sizeof(k_t));
}

// typedef struct {
//     short index;
//     short count;
// } bucket_t;



// void *histogram(void *arg)
// {
//     bucket_t *bucket = (bucket_t *)arg;
//     for (int i = bucket->index; i < bucket->index + bucket->count; i++)
//         histogram[sorting[i].key & bucket_mask]++;
//     pthread_exit(NULL);
// }

// struct proccess {
//     int start;
//     int end;
// };

// void *bucket_sort(void *arg)
// {
//     bucket_t *bucket = (bucket_t *)arg;
//     for (int i = 0; i < rec_n; i++)
//         if (sorting[i].key & bucket_mask) 
//             bucket->buckets[1][back_n++] = sorting[i];
//         else 
//             bucket->buckets[0][front_n++] = sorting[i];
//     pthread_exit(NULL);
// }

typedef struct {
    k_t **buckets;
    int *counts;
    int start, end;
} bucket_t;

void *bucket_sort_thread(void *arg)
{
    bucket_t *b = (bucket_t *)arg;
    for (int i = b->start; i < b->end; i++)
    {
        int bucket = sorting[i].key & bucket_mask;
        while (bucket > 3) bucket >>= 2;
        b->buckets[bucket][b->counts[bucket]++] = sorting[i];
        bucket += 1;
    }
    pthread_exit(NULL);
}

void parallel_sort()
{
    // Since our keys are signed, the last bit (index 31) needs to be flipped 
    // This has an interesting effect https://stackoverflow.com/a/52472629
    // There should be a better way to do this, which will keep the cache locality in mind
    int thread_count;
    pthread_t *threads; 
    // for (int i = 0; i < rec_n; i++)
    //     sorting[i].key ^= 1 << 31;
    for (int b_i = 0; b_i < 32/2; b_i++, bucket_mask = bucket_mask << 2)
    {

        // for (int i = 0; i < rec_n; i++) 
        //     printf("%d ", sorting[i].key);
        // printf("\n");

        thread_count = get_nprocs();
        int chunk_n = rec_n / thread_count;
        thread_count = thread_count > rec_n ? rec_n : thread_count;
        thread_count = chunk_n * thread_count < rec_n ? thread_count + 1 : thread_count;
        threads = malloc(thread_count * sizeof(pthread_t));
        // bucket_t *buckets = malloc(thread_count * sizeof(bucket_t));
        bucket_t *buckets = create_shared_memory(sizeof(bucket_t) * thread_count);

        // Create buckets
        for (int i = 0; i < thread_count; i++)
        {
            buckets[i].buckets = malloc(4 * sizeof(k_t *));
            buckets[i].counts = calloc(4, sizeof(int));
            buckets[i].buckets[0] = malloc(chunk_n * sizeof(k_t));
            buckets[i].buckets[1] = malloc(chunk_n * sizeof(k_t));
            buckets[i].buckets[2] = malloc(chunk_n * sizeof(k_t));
            buckets[i].buckets[3] = malloc(chunk_n * sizeof(k_t));
            buckets[i].start = i * chunk_n;
            buckets[i].end = (i + 1) * chunk_n;
            if (buckets[i].end > rec_n) buckets[i].end = rec_n;
        }

        // Create threads
        for (int i = 0; i < thread_count; i++)
            pthread_create(&threads[i], NULL, bucket_sort_thread, &buckets[i]);

        // Wait for threads to finish
        // Overall histogram
        int indices[4] = {0, 0, 0, 0};
        for (int i = 0; i < thread_count; i++)
        {
            pthread_join(threads[i], NULL);
            for (int j = 0; j < 4; j++)
                indices[j] += buckets[i].counts[j];
        }

        // Scan histogram
        int scan[4] = {0, 0, 0, 0};
        for (int i = 1; i < 4; i++)
            scan[i] = scan[i - 1] + indices[i - 1];
        
        int offsets[4] = {0, 0, 0, 0};
        for (int i = 0; i < thread_count; i++)
        {
            for (int j = 0; j < 4; j++)
            {
                memcpy(sorting + scan[j] + offsets[j], buckets[i].buckets[j], buckets[i].counts[j] * sizeof(k_t));
                offsets[j] += buckets[i].counts[j];
            }
        }

        // Free buckets
        for (int i = 0; i < thread_count; i++)
        {
            free(buckets[i].buckets[0]);
            free(buckets[i].buckets[1]);
            free(buckets[i].buckets[2]);
            free(buckets[i].buckets[3]);
            free(buckets[i].buckets);
            free(buckets[i].counts);
        }

        free(threads);
        // free(buckets);
        munmap(buckets, sizeof(bucket_t) * thread_count);
        
        // bucket_sort();
        // pthread_create(&threads[0], NULL, front_bucket, NULL);
        // pthread_create(&threads[1], NULL, back_bucket, NULL);

        // pthread_join(threads[0], NULL);
        // pthread_join(threads[1], NULL);

        // // Merge front and back
        // memcpy(sorting + front_n, back, back_n * sizeof(k_t));
    }
}

void concat(int *a, int *b)
{
    return;
}

void sort()
{
    if (rec_n <= threshold)
    {
        // memcpy(output, input, sz);
        serial_sort(sorting, sorting+rec_n-1);
    } 
    else 
        parallel_sort();
    for (int i = 0; i < rec_n; i++)
    {
        get(output, i)->key = sorting[i].key;
        memcpy(&get(output, i)->value, sorting[i].value, 96);
        // fsync(fd2);
        // memcpy(get(output, i), get(input, sorting[i]), sizeof(rec_t));
    }
}

void print(char *data)
{
    for (int i = 0; i < rec_n; i++) 
        printf("%d ", get(data, i)->key);
    printf("\n");
}

int main(int argc, char *argv[])
{
    if (argc != 3) 
    {
        printf("Incorrect amount of arguments used!");
        exit(1);
    }

    // Load contents of file as char string.
    load(argv[1], argv[2]);

    // print(input);
    sort();
    // print(output);

    // Flush/write to system
    save();

    // Release mapped memory
    unload();
    return 0;
}
