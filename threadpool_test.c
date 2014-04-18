/*
 * Thread pool test program.
 *
 * This program creates a thread pool with a configurable number
 * of threads and then submits a configurable number of tasks.
 * Each task reports which thread executed it.
 *
 * An associated Python script (check.py) parses the output and
 * checks that 'nthreads' tasks are executing in parallel.
 *
 * This program makes idealizing and simplifying assumptions.
 *
 * First, it assumes that there are enough physical CPUs available
 * so that in fact 'nthreads' threads can execute. This may not
 * be true on a heavily loaded machine; test results will not be 
 * reliable under these circumstances.
 *
 * Second, the specification does not require that all threads
 * have reached the point where they wait on the pool's condition
 * variable.  To increase the likelihood that this is the case,
 * the test script pauses for 1 second before submitting tasks.
 *
 * Written by G. Back for CS3214 Spring 2010.
 */
#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <time.h>

#include "threadpool.h"

// helper function to count number of threads in this process
static int count_number_of_threads(void);

/* Data to be passed to callable. */
struct callable_data {
    int number;
};

/* 
 * A callable. 
 *
 * Returns a string that reports its number, which thread
 * executes the task, and the start and end time of the 
 * task's execution.
 */
static void *
callable_task(struct callable_data * callable)
{
    char buf[1024];

    struct timeval start, end;
    assert(gettimeofday(&start, NULL) == 0);

    struct timespec t = { .tv_sec = 0, .tv_nsec = 5000000 };
    assert(nanosleep(&t, NULL) == 0);

    assert(gettimeofday(&end, NULL) == 0);

    snprintf(buf, sizeof buf, 
            "Future #%d Thread #%p start=%ld.%ld end=%ld.%ld", 
             callable->number, (void *)pthread_self(),
            start.tv_sec, start.tv_usec,
            end.tv_sec, end.tv_usec);
    return strdup(buf);
}

int
main(int ac, char *av[])
{
    assert (ac > 2 || !!!"Usage: threadpool_test <nthreads> <ntasks>");

    int nthreads = atoi(av[1]);
    int ntasks = atoi(av[2]);
    struct thread_pool * ex = thread_pool_new(nthreads);
    const int N = ntasks;
    struct future * f[N];

    // sleep .5 seconds to give threads time to start up
    struct timespec sleep_for = { .tv_sec = 0, .tv_nsec = 5*1e8 };
    nanosleep(&sleep_for, NULL);

    int threadsstarted = count_number_of_threads() - 1;
    if (threadsstarted != nthreads) {
        printf("The thread pool started %d instead of %d threads.\n",
                threadsstarted, nthreads);
        return EXIT_FAILURE;
    }

    // check for busy-waiting implementation
    struct rusage usage;
    int rc = getrusage(RUSAGE_SELF, &usage);
    if (rc == -1)
        perror("getrusage"), exit(-1);

    if (usage.ru_utime.tv_sec > 0 || usage.ru_utime.tv_usec > 400000) {
        printf("Thread pool is consuming excessive CPU time without running any jobs\n");
        return EXIT_FAILURE;
    }

    // submit N tasks and record futures obtained in return
    int i;
    for (i = 0; i < N; i++) {
        struct callable_data * callable_data = malloc(sizeof *callable_data);
        callable_data->number = i;
        f[i] = thread_pool_submit(ex, 
                               (thread_pool_callable_func_t) callable_task, 
                               callable_data);
    }
    printf("Main thread: %p\n", (void *)pthread_self());

    // wait for each future
    for (i = 0; i < N; i++) {
        printf("%s\n", (char *) future_get(f[i]));
    }

    // check that no pool thread shut down prematurely
    threadsstarted = count_number_of_threads() - 1;
    if (threadsstarted != nthreads) {
        printf("Only %d thread pool threads are left, should be %d threads.\n",
                threadsstarted, nthreads);
        return EXIT_FAILURE;
    }
    thread_pool_shutdown(ex);

    // sleep .3 seconds to give threads time to shut down
    // pthread_join() is not atomic with respect to the number
    // of tasks reported by /proc/self/status
    sleep_for.tv_nsec = 3*1e8;
    nanosleep(&sleep_for, NULL);

    int threadsleft = count_number_of_threads();
    if (threadsleft != 1) {
        printf("The thread pool did not correctly shut down"
               ", there are %d threads left.\n", threadsleft);
        return EXIT_FAILURE;
    }

    printf("Done.\n");
    return EXIT_SUCCESS;
}

/**
 * Count number of threads by scanning /proc/self/status
 * for the Threads: ... line
 */
static int
count_number_of_threads(void)
{
    FILE * p = fopen("/proc/self/status", "r");
    while (!feof(p)) {
        int threadsleft;
        char buf[128];
        fgets(buf, sizeof buf, p);
        if (sscanf(buf, "Threads: %d\n", &threadsleft) != 1)
            continue;

        fclose(p);
        return threadsleft;
    }
    printf("Internal error, please send email to gback@cs.vt.edu\n");
    abort();
}
