#include <assert.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include "threadpool.h"
#include "list.h"

/*
 * Function Prototype 
 */
static void *exec_thread(void *thread);

/*
 * Thread Pool structure.
 */
struct thread_pool {
	struct list jobs; //using given list implementation
	int num_threads; //number of current threads
	int disable;  //used to check if the pool has shut down
	pthread_t *threads; //points to the threads
	pthread_mutex_t lock;  //mutex lock
	pthread_cond_t condition; //cond variable, allows threads to suspend execution
};

/*
 * Future structure
 */
struct future {
	thread_pool_callable_func_t call;
	void *data;  //pointer to the data
	void *result; //pointer to the result
	sem_t semaphore; //allows n number of threads to enter
	struct list_elem elem; //current element
};

/* 
 * Creates new thread pool with n threads. 
 * Each thread waits for the futures to be enqueued in the thread pool's current work queue.
 */
struct thread_pool *thread_pool_new(int nthreads) {
	
	//initialize the thread pool, setting the number of thread and the disable flag to false.
	struct thread_pool *t_pool;
	if(!(t_pool = malloc(sizeof(struct thread_pool)))) {
		perror("Error: Initialization thread pool failed");
		return NULL;
	}
	t_pool -> num_threads = nthreads;
	t_pool -> disable = 0;
	
	//initialize the mutex lock, returns error if it fails.
	if(pthread_mutex_init(&t_pool -> lock, NULL)) {
		perror("Error: Initializing mutex lock");
		return NULL;
	}
	
	//initialize the conidition error, returns error if it fails.
	if(pthread_cond_init(&(t_pool -> condition), NULL)) {
		perror("Error: Initializing condition");
		return NULL;
	}
	//initialize list for the future jobs
	list_init(&t_pool -> jobs); 
	if(!(t_pool -> threads = malloc(sizeof(pthread_t) * nthreads))) {
		perror("Error: Malloc initialization failed");
		return NULL;
	}
	
	//initializes all the threads.
	int i;
	for (i = 0; i < nthreads; i++) {
		pthread_create(&t_pool -> threads[i], NULL, exec_thread, t_pool);
	}
	return t_pool;
}

/* 
 * Shuts down the threadpool. First broadcast to the futures that it is shutting down, 
 * then join threads together, and last free up memory.
 */
void thread_pool_shutdown(struct thread_pool *pool) {
	pthread_mutex_lock(&pool -> lock);
	pool -> disable = 1;
	pthread_cond_broadcast(&pool -> condition);
	pthread_mutex_unlock(&pool -> lock);
	
	//loop through and join threads
	int i;
	for (i = 0; i < pool -> num_threads; i++) {
		pthread_join(pool -> threads[i], NULL);
	}
	
	free(pool);
}

/* 
 * Wait for the futures semaphore to be signalled, then return the result
 */
void *future_get(struct future *future) {
	sem_wait(&future->semaphore);
	return future->result;
}

/* 
 * Frees a futures memory
 */
void future_free(struct future *future) {
	free(future);
}

/*
* Each thread will execute this worker function.
*/
static void *exec_thread(void *thread) {
	struct thread_pool * pool = (struct thread_pool *) thread;
	
	while(true) {
		//aquires the lock
		pthread_mutex_lock(&pool -> lock);
		
		//while the list of jobs is empty
		while(list_empty(&pool -> jobs)) {
		
			//wait for the condition of the pthread, requires the mutex and status
			pthread_cond_wait(&pool -> condition, &pool -> lock);
			
			//if the flag is active unlock the mutex
			if(pool -> disable == 1) {
				pthread_mutex_unlock(&pool -> lock);
				pthread_exit(NULL);
			}
			//pthread_cond_wait(&pool -> condition, &pool -> lock);
		}
		
		//initialize a future to remove pop a job off the front of the list
		struct future *c_next;
		c_next = list_entry(list_pop_front(&pool -> jobs), struct future, elem);
	
		//unlocks the mutex
		pthread_mutex_unlock(&pool -> lock);
			
		//if the disable flag is off the job should be passed through the semaphore
		if (pool -> disable == 0) {
			c_next -> result = (*(c_next -> call))(c_next -> data);
			sem_post(&c_next -> semaphore);
		}
	}
	return NULL;
}

/* 
 * Takes in a function and data pointer. Creates and initialize a new future.
 * The new future is then enqueued on the list.
 */
struct future * thread_pool_submit(struct thread_pool *pool, 
	thread_pool_callable_func_t callable, void *callable_data) {
	
	struct future *new_future;
	new_future = malloc(sizeof(struct future));
	if (new_future == NULL) { //make sure malloc received memory
		perror("Error: Malloc initialization failed");
		return NULL;
	}
	
	//initialize future
	new_future -> call = callable;
	new_future -> data = callable_data;
	sem_init(&new_future -> semaphore, 0, 0);
	
	pthread_mutex_lock(&pool -> lock);
	list_push_back(&pool -> jobs, &new_future -> elem);
	pthread_cond_signal(&pool -> condition);
	pthread_mutex_unlock(&pool -> lock);	
	return new_future;
}
