#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "icl_hash.h"
#include "thread_pool.h"

   /** \file membox.c  
       \author Vlad Pandelea 519231 & Emanuele Aurora 520224  
     Si dichiara che il contenuto di questo file e' in ogni sua parte opera  
     originale degli autori.
     *   */  

#define UNLOCK_EXIT(mutex) {              \
    pthread_mutex_unlock(mutex);          \
    return -1; }

#define UNLOCK_EXIT2(mutex1, mutex2) {    \
    pthread_mutex_unlock(mutex1);         \
    pthread_mutex_unlock(mutex2);         \
    return -1; }


/**
 * Print queue in STDOUT.
 *
 * @param[in] queue -- the queue
 * 
 */
void printfQueue(jobQueue_t * queue) {
    
    tfd_t * tmp = queue -> head;
    
    while (tmp != NULL) {
        printf(" - %u  -  ", tmp -> tfd);
        tmp = tmp -> next;
    }
    
}


/**
 * Destroy the queue and frees the heap.
 *
 * @param[out] queue -- the queue
 * 
 */
void queueDestroy(jobQueue_t * queue) {
    
    tfd_t * tmp;
    
    /* Ignoring errors */
    
    pthread_mutex_destroy(&queue -> listMutex);
    pthread_mutex_destroy(&queue -> closingLock);
    pthread_cond_destroy(&queue -> hasJobs);
    
    while (queue -> head != NULL) {
        
        tmp = queue -> head;
        queue -> head = queue -> head -> next;
        close(tmp -> tfd);
        free(tmp);
    }
    
    free(queue);
}


/**
 * Destroy the ThreadPool and frees the heap.
 *
 * @param[out] queue -- the queue
 * 
 */
void threadPoolDestroy(thpool_t * threadPool) {
    
    int i;
    
    for (i = 0; i < threadPool -> num_threads; i++) free(threadPool -> threads[i]);
    for (i = 0; i < threadPool -> lock_number; i++) pthread_mutex_destroy(&threadPool -> data_lock[i]);
    
    pthread_mutex_destroy(&threadPool -> global_lock);
    pthread_mutex_destroy(&threadPool -> fdMutex);
    
    free(threadPool -> currentFD);
    free(threadPool -> threads);
    free(threadPool -> data_lock);
    free(threadPool);
}


/**
 * Push an item inside the queue.
 *
 * @param[in] threadfd -- the file descriptor inside the item
 * @param[out] queue -- the queue
 *
 * @returns return -1 on error, 0 on success.
 * 
 */
int queuePush(int threadfd, jobQueue_t * queue) {
    
    tfd_t * new;
    
    pthread_mutex_lock(&(queue -> listMutex));
    
    if (queue -> head == NULL) {
        
        if (queue -> tail != NULL) UNLOCK_EXIT(&(queue -> listMutex));
        if ((new = (tfd_t*) malloc(sizeof(tfd_t))) == NULL) UNLOCK_EXIT(&(queue -> listMutex));
        
        new -> tfd = threadfd;
        new -> next = NULL;
        queue -> head = new;
        queue -> tail = new;
        
    } else {
        
        if (queue -> tail == NULL) UNLOCK_EXIT(&(queue -> listMutex));
        if ((new = (tfd_t*) malloc(sizeof(tfd_t))) == NULL) UNLOCK_EXIT(&(queue -> listMutex));
        
        new -> tfd = threadfd;
        new -> next = NULL;
        queue -> tail -> next = new;
        queue -> tail = new;
        
    }
    
    queue -> elementsNumber += 1;
    
    pthread_cond_signal(&(queue -> hasJobs));
    
    pthread_mutex_unlock(&(queue -> listMutex));
    
    
    return 0;
}


/**
 * Pop the first item from the queue
 *
 * @param[in] queue -- the queue
 *
 * @returns return the threadid of the item popped
 * 
 */
int queuePop(jobQueue_t * queue) {
    
    
    pthread_mutex_lock(&(queue -> listMutex));

    while (queue -> elementsNumber == 0) 
        pthread_cond_wait(&(queue -> hasJobs), &(queue -> listMutex));

    /* After being awaken, control if the Server is closing */
    pthread_mutex_lock(&queue -> closingLock);
    if (queue -> isClosing) UNLOCK_EXIT2(&(queue -> closingLock), &(queue -> listMutex));
    pthread_mutex_unlock(&queue -> closingLock);
    
    
    tfd_t * tmp = (queue -> head) -> next;
    unsigned int threadfd = (queue -> head) -> tfd;
    
    if (queue -> tail == queue -> head) queue -> tail = tmp;
    free(queue -> head);
    queue -> head = tmp;
    
    queue -> elementsNumber -= 1;
    
    pthread_mutex_unlock(&(queue -> listMutex));
    
    return threadfd;
}


/**
 * Initialize the job Queue.
 *
 * @param[in] hash -- hash table connected to the Queue
 *
 * @returns the queue initialized, or NULL when error occurred
 * 
 */
jobQueue_t * initQueue(icl_hash_t * hash) {
    
    if (hash == NULL) return NULL;
    
    jobQueue_t * queue;
    if ((queue = (jobQueue_t*) malloc(sizeof(jobQueue_t))) == NULL) return NULL;
    
    queue -> head = NULL;
    queue -> tail = NULL;
    queue -> hash = hash;
    queue -> elementsNumber = 0;
    queue -> isClosing = 0;
    
    pthread_cond_init(&(queue -> hasJobs), NULL);
    pthread_mutex_init(&(queue -> listMutex), NULL);
    pthread_mutex_init(&(queue -> closingLock), NULL);
    
    return queue;
}


/**
 * The function repeated from every thread inside ThreadPool
 *
 * @param[in] thpool -- the ThreadPool
 *
 * @returns return only when thread is closed.
 * 
 */
void * threadLifeCycle (void * args) {
    
    thpool_t * thpool = args;
    long clientFD;
    int i;
    
    while (1) {
        
        /* Verifies if thread must be closed */
        
        pthread_mutex_lock(&thpool -> jobQueue -> closingLock);
        
        /* Close the thread */
        if (thpool -> jobQueue -> isClosing) {
            pthread_mutex_unlock(&thpool -> jobQueue -> closingLock);
            return NULL;
        }
        
        pthread_mutex_unlock(&thpool -> jobQueue -> closingLock);
        
        
        /* Pop the first Client from the queue */
        
        if ((clientFD = queuePop(thpool -> jobQueue)) == -1) continue;
        
        pthread_mutex_lock(&(thpool -> fdMutex));
        
        for (i = 0; i < thpool -> num_threads; i++) {
            if (thpool -> currentFD[i] == -1) {
                thpool -> currentFD[i] = clientFD;
                break;
            }
        }
        pthread_mutex_unlock(&(thpool -> fdMutex));
        
        
        /* Execute Client's request with opExecute function */
        
        thpool -> opExecute(clientFD, thpool -> jobQueue -> hash, thpool);  
        
        pthread_mutex_lock(&(thpool -> fdMutex));
        
        for (i = 0; i < thpool -> num_threads; i++) {
            if (thpool -> currentFD[i] == clientFD) {
                thpool -> currentFD[i] = -1;
                break;
            }
        }
        pthread_mutex_unlock(&(thpool -> fdMutex));
        
        
        
    }
}


/**
 * Initialize the Thread Pool.
 *
 * @param[in] num_threads -- the number of threads in the ThreadPool
 * @param[in] opExecute -- the opExecute function needed for the thread to operate
 * @param[in] queue -- the client queue
 * @param[in] lockNumber -- the number of locks needed in the ThreadPool
 *
 * @returns the ThreadPool created, or NULL when error occurred.
 * 
 */
thpool_t * thpool_init(int num_threads, void (* opExecute)(int, icl_hash_t *, thpool_t *), jobQueue_t * queue, int lockNumber){

	int i;
    
	thpool_t * thpool = (thpool_t*)malloc(sizeof(thpool_t));
	if (thpool == NULL) return NULL;
	
	if ((thpool -> threads = (pthread_t **) malloc(num_threads * sizeof(pthread_t*))) == NULL){
        free(thpool);
        return NULL;
    }
	
	if ((thpool -> currentFD = (long*) malloc(num_threads * sizeof(long))) == NULL){
        free(thpool -> threads);
        free(thpool);
        return NULL;
    }
	
	if ((thpool -> data_lock = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t) * lockNumber)) == NULL){
        free(thpool -> currentFD);
        free(thpool -> threads);
        free(thpool);
        return NULL;
    }
	
	thpool -> num_threads = num_threads;
    thpool -> opExecute = opExecute;
	thpool -> jobQueue = queue;
    thpool -> global_lock_id = 0;
	thpool -> lock_number = lockNumber;
	
	for (i = 0; i < num_threads; i++) {
        thpool -> currentFD[i] = -1;
        
		if ((thpool -> threads[i] = (pthread_t*) malloc(sizeof(pthread_t))) == NULL) {
            
            /* frees everything before exit */
            int j;
            
            for (j = 0; j < i; j++) free(thpool -> threads[j]);
            free(thpool -> currentFD);
            free(thpool -> threads);
            free(thpool -> data_lock);
            free(thpool);
            
            return NULL;
        }
        
        
		if (pthread_create(thpool -> threads[i], NULL, threadLifeCycle, thpool) != 0) {
            
            /* frees everything before exit */
            int j;
            
            for (j = 0; j <= i; j++) free(thpool -> threads[j]);
            free(thpool -> currentFD);
            free(thpool -> threads);
            free(thpool -> data_lock);
            free(thpool);
            
            return NULL;
        }
	}
    
    int failure=0, num;
    
    for (i = 0; i < lockNumber; i++) if ((num = pthread_mutex_init(&(thpool -> data_lock[i]), NULL)) != 0) failure = num;
	
    if ((num = pthread_mutex_init(&thpool -> global_lock, NULL)) != 0) failure = num;
    if ((num = pthread_mutex_init(&thpool -> fdMutex, NULL)) != 0) failure = num;

    if (failure) {
        
        for (i = 0; i <= num_threads; i++) free(thpool -> threads[i]);
        free(thpool -> currentFD);
        free(thpool -> threads);
        free(thpool -> data_lock);
        free(thpool);
        
        return NULL;
    }
    
	return thpool;	
}
