#ifndef threadpool_h
#define threadpool_h

#include "icl_hash.h"
#include "pthread.h"

   /** \file membox.c  
       \author Vlad Pandelea 519231 & Emanuele Aurora 520224  
     Si dichiara che il contenuto di questo file e' in ogni sua parte opera  
     originale degli autori.
     *   */  

typedef struct tfd_s{
    unsigned int tfd;
    struct tfd_s * next;
} tfd_t;


/* Client Queue */
typedef struct jobQueue_s{
    tfd_t * head;
    tfd_t * tail;
    icl_hash_t * hash;                                           /* hash table                      */
    pthread_cond_t hasJobs;                                      /* conditional variable            */
    pthread_mutex_t listMutex;                                   /* mutex for cond variable         */
    int elementsNumber;                                          /* number of the elements          */
    int isClosing;												 /* flag for clean quit 			*/
    pthread_mutex_t closingLock;								 /* lock for clean quit 			*/
}jobQueue_t;


/* Threadpool */
typedef struct thpool_s{
	pthread_t ** threads;										 /* pointer to threads			    */
	long * currentFD;											 /* FD holded by every thread		*/
	pthread_mutex_t fdMutex;									 /* mutex to access currentFD		*/
    unsigned int num_threads;									 /* number of threads				*/
    pthread_mutex_t * data_lock;                                 /* used for locking data           */ 
    pthread_mutex_t  global_lock;                                /* lock used for global_lock_id    */
    unsigned int lock_number;                                    /* number of lock                  */
    unsigned long global_lock_id;								 /* id of global lock holer			*/								
    void (*opExecute)(int, icl_hash_t *, struct thpool_s*);      /* execute function for threads    */
    jobQueue_t * jobQueue;                                       /* the queue of client connections */
} thpool_t;


jobQueue_t * initQueue(icl_hash_t * hash) ;
thpool_t * thpool_init(int num_threads, void (* opExecute)(int, icl_hash_t *, thpool_t *), jobQueue_t * queue, int lockNumber);
void * threadLifeCycle (void * args) ;
int queuePop(jobQueue_t * queue) ;
int queuePush(int threadfd, jobQueue_t * queue) ;
void printfQueue(jobQueue_t * queue) ;
void queueDestroy(jobQueue_t * queue) ;
void threadPoolDestroy(thpool_t * threadPool);

#endif
