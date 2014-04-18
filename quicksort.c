/*
 * Parallel Quicksort.
 *
 * Demo application that shows how one might use threadpools/futures
 * in an application.
 *
 * Requires threadpool.c/threadpool.h
 *
 * Written by Godmar Back gback@cs.vt.edu for CS3214 Fall 2010.
 */

#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <pthread.h>
#include <assert.h>
#include <getopt.h>

#include "threadpool.h"

typedef void (*sort_func)(int *, int);

/* Return true if array 'a' is sorted. */
static bool 
check_sorted(int a[], int n) 
{
    int i;
    for (i = 0; i < n-1; i++)
        if (a[i] > a[i+1])
            return false;
    return true;
}

/* ------------------------------------------------------------- 
 * Built-in qsort.
 */
static int cmp_int(const void *a, const void *b)
{
    return *(int *)a - *(int *)b;
}

static void builtin_qsort(int *a, int N)
{
    qsort(a, N, sizeof(int), cmp_int);
}

/* ------------------------------------------------------------- 
 * Quicksort utility routines.
 */
/* Swap two elements */
static inline void swap(int *a, int *b)
{
    int tmp = *a;
    *a = *b;
    *b = tmp;
}

/* Partitioning using middle element as pivot */
static int 
qsort_partition(int * array, int left, int right)
{
    int middle = left + (right-left)/2;

    // left <=> middle
    swap(array + left, array + middle);

    int current, last = left;
    for (current = left + 1; current <= right; ++current) {
        if (array[current] < array[left]) {
            ++last;
            // last <=> current
            swap(array + last, array + current);
        }
    }

    // left <=> last
    swap(array + left, array + last);
    return last;
}

/* ------------------------------------------------------------- 
 * Serial implementation.
 */
static void 
qsort_internal_serial(int *array, int left, int right) 
{
    if (left >= right)
        return;

    int split = qsort_partition(array, left, right);
    qsort_internal_serial(array, left, split - 1);
    qsort_internal_serial(array, split + 1, right);
}

static void 
qsort_serial(int *array, int N) 
{
    qsort_internal_serial(array, 0, N - 1);
}

/* ------------------------------------------------------------- 
 * Parallel implementation.
 */

static struct thread_pool * threadpool;

/* qsort_task describes a unit of parallel work */
struct qsort_task {
    int *array;
    int left, right, depth;
    struct future *future;
    struct qsort_task *next;
} * tasks;  /* list of outstanding tasks, protected by task_lock */

static pthread_mutex_t task_lock = PTHREAD_MUTEX_INITIALIZER;

/* Create a qsort_task instance */
static struct qsort_task * 
create_qsort_task(int * array, int left, int right, int depth)
{
    struct qsort_task * task = calloc(sizeof(struct qsort_task), 1);
    task->array = array;
    task->left = left;
    task->right = right;
    task->depth = depth;
    return task;
}

/* Add a qsort_task instance to list of outstanding tasks */
static void 
add_qsort_task(struct qsort_task * task)
{
    pthread_mutex_lock(&task_lock);
    assert(task->future);
    task->next = tasks;
    tasks = task;
    pthread_mutex_unlock(&task_lock);
}

#define MAX_SEGMENTS 1024
static int qsegment_size [MAX_SEGMENTS];
static int qsegment_n = 0;

/* Wait for all outstanding tasks. */
static void 
wait_for_tasks ()
{
    qsegment_n = 0;
    for (;;) {
        struct qsort_task * head = NULL;
        pthread_mutex_lock(&task_lock);
        head = tasks;
        if (head)
            tasks = head->next;
        pthread_mutex_unlock(&task_lock);
        if (!head)
            break;

        intptr_t size = (intptr_t) future_get(head->future);
        future_free(head->future);
        if (qsegment_n < MAX_SEGMENTS)
            qsegment_size[qsegment_n++] = size;
        free(head);
    }
}

static void
report_stats()
{
    builtin_qsort(qsegment_size, qsegment_n);
    int i;
    printf("Processed %d segments of sizes: ", qsegment_n);
    for (i = 0; i < qsegment_n; i++)
        printf("%d ", qsegment_size[i]);
    printf("\n");
}

/* Parallel qsort - returns size of segment sorted */
static int  
qsort_internal_parallel(struct qsort_task * s)
{
    int * array = s->array;
    int left = s->left;
    int right = s->right;
    int depth = s->depth;

    if (left >= right)
        return 0;

    int split = qsort_partition(array, left, right);
    if (depth < 1) {
        qsort_internal_serial(array, left, split - 1);
        qsort_internal_serial(array, split + 1, right);
    } else {
        struct qsort_task * qleft = create_qsort_task(array, left, split-1, depth-1);
        qleft->future = thread_pool_submit(threadpool, 
                                   (thread_pool_callable_func_t) qsort_internal_parallel,  
                                   qleft);
        add_qsort_task(qleft);

        struct qsort_task * qright = create_qsort_task(array, split+1, right, depth-1);
        qright->future = thread_pool_submit(threadpool, 
                                   (thread_pool_callable_func_t) qsort_internal_parallel,  
                                   qright);
        add_qsort_task(qright);
    }
    return right - left;
}

// maximum depth to which each recursive call is executed in parallel
static int depth = 3;

static void 
qsort_parallel(int *array, int N) 
{
    struct qsort_task * top = create_qsort_task(array, 0, N-1, depth);
    qsort_internal_parallel(top);
    wait_for_tasks();
    free(top);
}

/*
 * Benchmark one run of sort_func sorter
 */
static void 
benchmark(const char *benchmark_name, sort_func sorter, int *a0, int N)
{
    struct timeval start, end, diff;

    int *a = malloc(N * sizeof(int));
    memcpy(a, a0, N * sizeof(int));

    gettimeofday(&start, NULL);
    sorter(a, N);
    gettimeofday(&end, NULL);

    if (!check_sorted(a, N)) {
        fprintf(stderr, "Sort failed\n");
        abort();
    }
    timersub(&end, &start, &diff);
    printf("%-20s took %.3f sec.\n", benchmark_name, diff.tv_sec + diff.tv_usec / 1.0e6);
    free(a);
}

static void
usage(char *av0, int depth, int nthreads)
{
    fprintf(stderr, "Usage: %s [-d <n>] [-n <n>] [-b] [-q] [-s <n>] <N>\n"
                    " -d        parallel recursion depth, default %d\n"
                    " -n        number of threads in pool, default %d\n"
                    " -b        run built-in qsort\n"
                    " -s        specify srand() seed\n"
                    " -q        don't run serial qsort\n"
                    , av0, depth, nthreads);
    exit(0);
}

int 
main(int ac, char *av[]) 
{
    int nthreads = 4;
    int c;
    bool run_builtin_qsort = false;
    bool run_serial_qsort = true;

    while ((c = getopt(ac, av, "d:n:bhs:q")) != EOF) {
        switch (c) {
        case 'd':
            depth = atoi(optarg);
            break;
        case 'n':
            nthreads = atoi(optarg);
            break;
        case 's':
            srand(atoi(optarg));
            break;
        case 'b':
            run_builtin_qsort = true;
            break;
        case 'q':
            run_serial_qsort = false;
            break;
        case 'h':
            usage(av[0], depth, nthreads);
        }
    }
    if (optind == ac)
        usage(av[0], depth, nthreads);

    int N = atoi(av[optind]);

    int i, * a0 = malloc(N * sizeof(int));
    for (i = 0; i < N; i++)
        a0[i] = random();

    if (run_builtin_qsort)
        benchmark("Built-in qsort", builtin_qsort, a0, N);

    if (run_serial_qsort)
        benchmark("qsort serial", qsort_serial, a0, N);

    threadpool = thread_pool_new(nthreads);
    printf("Using %d threads, recursive parallel depth=%d\n", nthreads, depth);
    sleep(1);

    benchmark("qsort parallel", qsort_parallel, a0, N);
    report_stats();

    thread_pool_shutdown(threadpool);
    return 0;
}

