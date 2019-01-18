/*
 * membox Progetto del corso di LSO 2016 
 *
 * Dipartimento di Informatica Università di Pisa
 * Docenti: Pelagatti, Torquati
 * 
 */
  
   /** \file membox.c  
       \author Vlad Pandelea 519231 & Emanuele Aurora 520224  
     Si dichiara che il contenuto di questo file e' in ogni sua parte opera  
     originale degli autori.
     *   */  
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L
#define UNIX_PATH_MAX 2047
#define INVALID_FORMAT = {fprintf(stderr,"invalid format\n");exit(EXIT_FAILURE);}
#define ERROR1(errorType, ret) {errno = errorType; return ret;}
#define MAX_ROW_NUMBER 100000
#define CREATE_STD_CONFIG  {                            \
    conf -> maxConnections = 50;                        \
    conf -> threadsInPool = 10;                         \
    conf -> storageSize = 0;                            \
    conf -> storageByteSize = 0;                        \
    conf -> maxObjSize = 0;                             \
    conf -> haveStats = 0;                              \
    conf -> haveLoads = 0;                              \
    strcpy(conf -> unixPath , "./mbox_socket");         \
    strcpy(conf -> statFileName ,"./mbox_stats.txt"); }
    
#define STATS_UPDATE(arg, lock) {                       \
    pthread_mutex_lock(&statsLocks[lock]);              \
    mboxStats.arg++;                                    \
    pthread_mutex_unlock(&statsLocks[lock]); }
    
#define SET_MESSAGE_UNLOCK(OP, key, data, len, lock) {  \
    pthread_mutex_unlock(lock);                         \
    setHeader(reply, OP, key);                          \
    setData(reply, data, len); }
        

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <pthread.h>
#include <time.h>
#include <signal.h>

/* inserire gli altri include che servono */

#include "membox.h"
#include "icl_hash.h"
#include "stats.h"
#include "message.h"
#include "connections.h"
#include "thread_pool.h"


	
custom_conf_t* customConf;
configs_t * conf; // configurazione del file settata come globale
FILE * statFile;
FILE * tableFile;

/* struttura che memorizza le statistiche del server, struct statistics 
 * e' definita in stats.h.
 */

#define NUM_STATS_ELEMENTS FAIL_LOCK
struct statistics  mboxStats = { 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
pthread_mutex_t statsLocks[NUM_STATS_ELEMENTS];


typedef struct tpool_fdc {
    thpool_t * tp;
    int fd_skt;
    
}tp_fdc;



void printConf(){
	printf("Max connections = %d\n", conf -> maxConnections);
	printf("Threads in pool = %d\n", conf -> threadsInPool);
	printf("Storage size = %d\n", conf -> storageSize);
	printf("Storage byte size = %d\n", conf -> storageByteSize);
	printf("Max object size = %d\n", conf -> maxObjSize);
	printf("Unix path = %s\n", conf -> unixPath);
	printf("Stats file name = %s\n", conf -> statFileName);
	printf("Table file name = %s\n", conf -> loadTable);
}


void printCustomConf(){
	printf("Number of locks= %d\n", customConf -> locksNumber);
	printf("Number of buckets = %d\n", customConf -> hashBuckets);
}


/**
 * Convert a string to int.
 *
 * @param[in] str -- the string to convert
 *
 * @returns the number converted, -1 if error occurred and sets errno.
 * 
 */
int strToInt (char * str) {
	long int maxCon;
	char * endptr = NULL;
	errno = 0;
					
	maxCon = strtol(str, &endptr, 10);
	
    if (*endptr != '\0') ERROR1(EINVAL, -1);
    
	if ((errno == ERANGE && (maxCon == LONG_MAX || maxCon == LONG_MIN)) || (errno != 0 && maxCon == 0)) return -1;

	if (maxCon > INT_MAX || maxCon < 0) ERROR1(ERANGE, -1);
        
	return (int) maxCon;
}


/**
 * Load the configuration struct from the configuration file.
 * 
 * THE CONFIGURATION:
 * 
 * # SET A COMMENT LINE
 * 
 * EVERY OTHER LINE MUST BE MADE BY 3 WORDS OR BE EMPTY (\t, \n OR SPACES):
 * 
 * FIRST WORD CONTAINS THE NAME OF THE ATTRIBUTE
 * SECOND WORD MUST BE "="
 * THIRD WORD CONTAINS THE VALUE OF THE ATTRIBUTE
 * 
 * EVERY WORD MUST BE SEPARATED WITH A \t, \n OR SPACES
 * 
 * THE PATH NAME CANNOT CONTAIN SPACES
 *
 * @param[in] configFile -- the configuration file
 *
 * @returns 0 if everything went right, -1 otherwise.
 * 
 */
int loadConf(FILE * configFile) {
    char * buffer = NULL, * saveptr, * token[4];
    size_t len = 0;
    int lineLength;
    
    CREATE_STD_CONFIG;

    errno = 0;
	
    while( (lineLength = getline(&buffer, &len, configFile)) != -1) {
        if( lineLength == 1 ) continue;
        
        if (buffer[0] == '#') {
            /* comment line */
            continue;
            
        } else {
			
            if ( (token[0] = strtok_r(buffer," \t\n", &saveptr)) == NULL) continue;
            if ( (token[1] = strtok_r(NULL,  " \t\n", &saveptr)) == NULL) ERROR1(EINVAL, -1);
			if ( (token[2] = strtok_r(NULL,  " \t\n", &saveptr)) == NULL) ERROR1(EINVAL, -1);
			if ( (token[3] = strtok_r(NULL,  " \t\n", &saveptr)) != NULL) ERROR1(EINVAL, -1);
            
            if (strcmp(token[1], "=") != 0) { 
                
                ERROR1(EINVAL, -1);
                
            } else {
                
				if (strcmp(token[0], "MaxConnections") == 0) {if ((conf -> maxConnections = strToInt(token[2])) <= 0) return -1;}
				else if (strcmp(token[0], "ThreadsInPool") == 0) {if ((conf -> threadsInPool = strToInt(token[2])) <= 0) return -1;}
				else if (strcmp(token[0], "StorageSize" ) == 0) {if ((conf -> storageSize = strToInt(token[2])) == -1) return -1;}
				else if (strcmp(token[0], "StorageByteSize") == 0) {if ((conf -> storageByteSize = strToInt(token[2])) == -1) return -1;}
				else if (strcmp(token[0], "MaxObjSize") == 0) {if ((conf -> maxObjSize = strToInt(token[2])) == -1) return -1;}
				else if (strcmp(token[0], "UnixPath") == 0) strncpy(conf -> unixPath, token[2], PATH_MAX);
				else if (strcmp(token[0], "StatFileName") == 0) {strncpy(conf -> statFileName, token[2], PATH_MAX); conf -> haveStats = 1;}
				else if (strcmp(token[0], "LoadTablePath") == 0) {strncpy(conf -> loadTable, token[2], PATH_MAX); conf -> haveLoads = 1;}
				else ERROR1(EINVAL, -1);
			}
		}	
	}
	free(buffer);
	if (errno) return -1;
    
    return 0;
}


/**
 * Calculate the number of necessary locks.
 *
 * @param[in] hashRowNumber -- the number of row of the hash table
 *
 * @returns the number of locks
 * 
 */
int getTotalLockNumber(int hashRowNumber) {
    
    if (hashRowNumber > 1000) return 1 + hashRowNumber / 100; 
    if (hashRowNumber > 10) return 10;
    return hashRowNumber;
}


/**
 * With the hash function and the key, calculate the correct lock needed to access.
 *
 * @param[in] hash -- the hash table (containing the hash function)
 * @param[in] key -- they key
 *
 * @returns the right lock number to access.
 * 
 */
int getLockNumber(icl_hash_t * hash, int key) {
    int hashRowNumber = hash -> nbuckets;
    
    int currentRowNumber = (* hash -> hash_function)(&(key)) % hashRowNumber;
        
    if (hashRowNumber > 1000) return currentRowNumber / 100;
    if (hashRowNumber > 10) return currentRowNumber / (hashRowNumber / 10);
    return currentRowNumber;
}


/**
 * Verifies the integrity of the message.
 *
 * @param[in] msg -- the message
 *
 * @returns 0 if everything went right, -1 otherwise.
 * 
 */
int verifyMessage(message_t msg) {
    if (msg.hdr.op < 0 || msg.hdr.op > END_OP) return -1; //errore
    return 0;
}


/**
 * Execute the PUT_OP operation, adding an element in the hash table, if it doesn't exist.
 * It sets the reply message to send it back to the client.
 * 
 * THIS IMPLEMENTATION AVOID DEADLOCK
 *
 * @param[in] msg -- the message containing the data for the operation
 * @param[in] hash -- the hash table
 * @param[out] reply -- the reply message for the client
 * @param[in] data_lock -- the lock needed to access at that hash table element
 *
 * @returns 0 if everything went right, -1 otherwise.
 * 
 */
int put_op(message_t msg, icl_hash_t * hash, message_t * reply, pthread_mutex_t * data_lock) {
    
    int op_err = 0;
    
    unsigned long * key = (unsigned long*) malloc(sizeof(unsigned long));
    * key = msg.hdr.key;
    char * buf = msg.data.buf;
    int len = msg.data.len;
    
    if (msg.data.len > conf -> maxObjSize && conf -> maxObjSize > 0) {
        
        setHeader(reply, OP_PUT_SIZE, key);
        setData(reply, NULL, 0);
        free(msg.data.buf);
        free(key);
        return -1;
	}
	
	pthread_mutex_lock(&statsLocks[CURR_MAX_OBJ]);
    
	if (mboxStats.current_objects >= conf -> storageSize && conf -> storageSize > 0) {
        
        SET_MESSAGE_UNLOCK(OP_PUT_TOOMANY, key, NULL, 0, &statsLocks[CURR_MAX_OBJ]);
        free(msg.data.buf);
        free(key);
		return -1;
	}
	
	pthread_mutex_unlock(&statsLocks[CURR_MAX_OBJ]);
	pthread_mutex_lock(&statsLocks[CURR_MAX_SIZE]);
    
	if (mboxStats.current_size + msg.data.len > conf -> storageByteSize && conf -> storageByteSize > 0) {
        
        SET_MESSAGE_UNLOCK(OP_PUT_REPOSIZE, key, NULL, 0, &statsLocks[CURR_MAX_SIZE]);
        free(msg.data.buf);
        free(key);
		return -1;
	}
	
	/* Sets new value of current_size and current_obj in stats file */
    mboxStats.current_size += msg.data.len;
    
	pthread_mutex_unlock(&statsLocks[CURR_MAX_SIZE]);
    
    STATS_UPDATE(current_objects, CURR_MAX_OBJ);
    
    
    /* Get lock to work with hash table */
    pthread_mutex_lock(data_lock);
    
    
    if (icl_hash_insert(hash, (void*)(key), (void*)(buf), len, &op_err) == NULL) {
        
        /* uses op_err to know if the object already exist */
        if (!op_err) op_err = OP_FAIL;
        SET_MESSAGE_UNLOCK(op_err, key, NULL, 0, data_lock);
        
        /* Put failed, sets the old value of the stat file */
        pthread_mutex_lock(&statsLocks[CURR_MAX_OBJ]);  
        
        mboxStats.current_objects--;
        
        pthread_mutex_unlock(&statsLocks[CURR_MAX_OBJ]);
        pthread_mutex_lock(&statsLocks[CURR_MAX_SIZE]);
        
        mboxStats.current_size -= msg.data.len;
        
        pthread_mutex_unlock(&statsLocks[CURR_MAX_SIZE]);
        
        free(msg.data.buf);
        free(key);
        
        return -1;
    }
    
    SET_MESSAGE_UNLOCK(OP_OK, key, NULL, 0, data_lock);
    
    /* change MAX size and MAX object if necessary */
    pthread_mutex_lock(&statsLocks[CURR_MAX_OBJ]);  
        
    if(mboxStats.max_objects < mboxStats.current_objects) mboxStats.max_objects = mboxStats.current_objects;
        
    pthread_mutex_unlock(&statsLocks[CURR_MAX_OBJ]);
    pthread_mutex_lock(&statsLocks[CURR_MAX_SIZE]);
        
    if(mboxStats.max_size < mboxStats.current_size) mboxStats.max_size = mboxStats.current_size;
        
    pthread_mutex_unlock(&statsLocks[CURR_MAX_SIZE]);
    
    return 0;
}


/**
 * Execute the UPDATE_OP operation, adding an element in the hash table, if it doesn't exist,
 * and updating the older one if it exist.
 * It sets the reply message to send it back to the client.
 *
 * @param[in] msg -- the message containing the data for the operation
 * @param[in] hash -- the hash table
 * @param[out] reply -- the reply message for the client
 * @param[in] data_lock -- the lock needed to access at that hash table element
 *
 * @returns 0 if everything went right, -1 otherwise.
 * 
 */
int update_op(message_t msg, icl_hash_t * hash, message_t * reply, pthread_mutex_t * data_lock) {
    
    
    unsigned long * key = (unsigned long*) malloc(sizeof(unsigned long));
    * key = msg.hdr.key;
    char * buf = msg.data.buf;
    int len = msg.data.len, op_err = 0;
    
    pthread_mutex_lock(data_lock);
    

    if (icl_hash_update(hash, (void*)(key), (void*)(buf), len, &op_err, free, free) == NULL) {
        
        if (!op_err) op_err = OP_FAIL;
        SET_MESSAGE_UNLOCK(op_err, key, NULL, 0, data_lock);
        
        free(msg.data.buf);
        free(key);
        
        return -1;
    }
        
    SET_MESSAGE_UNLOCK(OP_OK, key, NULL, 0, data_lock);
    return 0;
}


/**
 * Execute the UPDATE_INSERT_OP operation, updating an element of the hash table, and creating it if it doesn't exist.
 * It sets the reply message to send it back to the client.
 *
 * @param[in] msg -- the message containing the data for the operation
 * @param[in] hash -- the hash table
 * @param[out] reply -- the reply message for the client
 * @param[in] data_lock -- the lock needed to access at that hash table element
 *
 * @returns 0 if everything went right, -1 otherwise.
 * 
 */
int update_insert_op(message_t msg, icl_hash_t * hash, message_t * reply, pthread_mutex_t * data_lock) {
    
    printf("quisi\n");
    if (update_op(msg, hash, reply, data_lock) == -1)
        if (put_op(msg, hash, reply, data_lock) == -1) return -1;
        
    printf("quipure\n");
    return 0;
}



/**
 * Execute the GET_OP operation, returning (inside reply) an element with a particular key.
 * It sets the reply message to send it back to the client.
 *
 * @param[in] key -- the key of the needed element
 * @param[in] hash -- the hash table
 * @param[out] reply -- the reply message for the client
 * @param[in] data_lock -- the lock needed to access at that hash table element
 *
 * @returns 0 if everything went right, -1 otherwise (no item found).
 * 
 */
int get_op(membox_key_t key, icl_hash_t * hash, message_t * reply, pthread_mutex_t * data_lock) {
    
    char * data;
    unsigned int length = 0;
    
    
    pthread_mutex_lock(data_lock);
    
    if ((data = icl_hash_find(hash, &key, &length)) == NULL) {
        
        SET_MESSAGE_UNLOCK(OP_GET_NONE, &key, NULL, 0, data_lock);
        return -1;
    }
    
    SET_MESSAGE_UNLOCK(OP_OK, &key, data, length, data_lock);
    
    return 0;
}


/**
 * Execute the REMOVE_OP operation, removing an element with a particular key.
 * It sets the reply message to send it back to the client.
 *
 * @param[in] key -- the key of the element
 * @param[in] hash -- the hash table
 * @param[out] reply -- the reply message for the client
 * @param[in] data_lock -- the lock needed to access at that hash table element
 *
 * @returns 0 if everything went right, -1 otherwise.
 * 
 */
int remove_op(membox_key_t key, icl_hash_t * hash, message_t * reply, pthread_mutex_t * data_lock) {
    
    int len = 0;
    pthread_mutex_lock(data_lock);
    
    if ((len = icl_hash_delete(hash, &key, free, free)) == -1) {
        
        SET_MESSAGE_UNLOCK(OP_REMOVE_NONE, &key, NULL, 0, data_lock);
        return -1;
    }
    
    SET_MESSAGE_UNLOCK(OP_OK, &key, NULL, 0, data_lock);
    
    /* Sets the new value of stats file */
	pthread_mutex_lock(&statsLocks[CURR_MAX_SIZE]);
    
    mboxStats.current_size-=len;
    
	pthread_mutex_unlock(&statsLocks[CURR_MAX_SIZE]);
	pthread_mutex_lock(&statsLocks[CURR_MAX_OBJ]);
    
    mboxStats.current_objects-=1;
    
	pthread_mutex_unlock(&statsLocks[CURR_MAX_OBJ]);
    
    return 0;
}

/**
 * Execute the LOCK_OP operation, giving global lock to the thread.
 * It sets the reply message to send it back to the client.
 * 
 * This function takes all the local lock from data_locks to be sure that any other thread as finished,
 * but there's a really low chance of conflict from a normal thread, where this thread (with global lock)
 * starts operating before the normal thread end his operation. In that case, there's no conflict
 * because the server treath both thread the same way as before: they need local lock to operate.
 *
 * @param[out] reply -- the reply message for the client
 * @param[out] global_lock_id -- flag containing the thread_id of the thread containing the lock
 * @param[in] global_lock -- lock to access at global_lock_id
 * @param[in] data_locks -- array of locks needed to get the global lock
 * @param[in] lock_number -- number of locks in data_locks array
 *
 * @returns 0 if everything went right, -1 otherwise.
 */
int lock_op(message_t * reply, unsigned long * global_lock_id, pthread_mutex_t * global_lock, pthread_mutex_t * data_locks, int lock_number) {
    
    int i;
    unsigned long key = 0;
    unsigned long thread_id;

    thread_id = pthread_self(); 
    
    pthread_mutex_lock(global_lock);

    if (*global_lock_id == thread_id) {
        
        SET_MESSAGE_UNLOCK(OP_LOCK_ALREADY, &key, NULL, 0, global_lock);
        return -1;
    }
    if (*global_lock_id != 0) {
        
        SET_MESSAGE_UNLOCK(OP_LOCKED, &key, NULL, 0, global_lock);
        return -1;  
    }
    
    *global_lock_id = thread_id;
    
    SET_MESSAGE_UNLOCK(OP_OK, &key, NULL, 0, global_lock);
    
    /* prendo le lock */
    for (i = 0; i < lock_number; i++) pthread_mutex_lock(&data_locks[i]);
    
    /* rilascio le lock */
    for (i = 0; i < lock_number; i++) pthread_mutex_unlock(&data_locks[i]);
    
    return 0;
}


/**
 * Execute the UNLOCK_OP operation, releasing the global lock.
 * It sets the reply message to send it back to the client.
 *
 * @param[out] reply -- the reply message for the client
 * @param[out] global_lock_id -- flag containing the thread_id of the thread containing the lock
 * @param[in] global_lock -- lock to access at global_lock_id
 *
 * @returns 0 if everything went right, -1 otherwise.
 */
int unlock_op(message_t * reply, unsigned long * global_lock_id, pthread_mutex_t * global_lock) {
    
    unsigned long key = 0;
    unsigned long thread_id;
    
    thread_id = pthread_self();
    
    pthread_mutex_lock(global_lock);
    
    if (*global_lock_id == 0) {
        
        SET_MESSAGE_UNLOCK(OP_LOCK_NONE, &key, NULL, 0, global_lock);
        return -1;  
    } 
    if (*global_lock_id != thread_id) {
        
        SET_MESSAGE_UNLOCK(OP_LOCKED, &key, NULL, 0, global_lock);
        return -1;  
    }
    
    *global_lock_id = 0;
    
    SET_MESSAGE_UNLOCK(OP_OK, &key, NULL, 0, global_lock);
    
    return 0;
}


void mex(int op) {
    
    switch(op) {
        
        case 0: printf("Operazione put conclusa..\n");
        break;
        case 1: printf("Operazione update conclusa..\n");
        break;
        case 2: printf("Operazione get conclusa..\n");
        break;
        case 3: printf("Operazione remove conclusa..\n");
        break;
        case 4: printf("Operazione lock conclusa..\n");
        break;
        case 5: printf("Operazione unlock conclusa..\n");
        break;
        case 6: printf("Operazione conf conclusa..\n");
        break;
    }
}


/**
 * Function used by a thread to know if there is a global lock on the hash table not made by itself.
 *
 * @param[in] global_lock_id -- flag containing the thread_id of the thread containing the lock
 * @param[in] global_lock -- lock to access at global_lock_id
 * @param[out] reply -- the reply message for the client
 *
 * @returns 0 if everything went right, -1 otherwise.
 */
int isLocked(unsigned long global_lock_id, pthread_mutex_t * global_lock, message_t * reply) {
    
    unsigned long thread_id = pthread_self();
	unsigned long key = 0;
    
    /* GLOBAL LOCK */
    pthread_mutex_lock(global_lock);
    
    if (global_lock_id != thread_id && global_lock_id != 0) {
        
        /* global lock enabled but not for you */
        SET_MESSAGE_UNLOCK(OP_LOCKED, &key, NULL, 0, global_lock);
        STATS_UPDATE(fail_lock, FAIL_LOCK);
        return -1;
    }
    
    /* RELEASING GLOBAL LOCK */
    pthread_mutex_unlock(global_lock);
    
    return 0;
}


/**
 * The execute function that every thread do after enstablishing a connection with a Client.
 * 
 * 
 *
 * @param[in] fd_c -- the socket file descriptor
 * @param[in] hash -- the hash table
 * @param[in] thpool -- the thread pool struct
 *
 * @returns return only on error (connection get closed).
 * 
 */
void opExecute(int fd_c, icl_hash_t * hash, thpool_t * thpool) {
    
	while(1) {
                
        /* the received message and the message to send back */
        message_t  msg_sent;
        message_t  msg_answer;
        
        
        if (readMessage(fd_c, &msg_sent) == -1) {
            
            /* release the global lock if necessary */
            if (thpool -> global_lock_id == pthread_self()) {
                pthread_mutex_lock(&thpool -> global_lock);
                thpool -> global_lock_id = 0;
                pthread_mutex_unlock(&thpool -> global_lock);
            }
            
            /* decrease the number of current connections */
            pthread_mutex_lock(&statsLocks[CONC_CONN]);
            mboxStats.concurrent_connections--;
            pthread_mutex_unlock(&statsLocks[CONC_CONN]);
            
            /* close Client file descriptor socket */
            close(fd_c);
            
            return;
        }
        
        if (verifyMessage(msg_sent) == -1) continue;
        
        if (isLocked(thpool -> global_lock_id, &(thpool -> global_lock), &msg_answer) == -1) {setHeader(&msg_answer,OP_LOCKED,&msg_sent.hdr.key); }
        else{
            
			/* calculating right lock to work with */
			unsigned int hash_lock_val = getLockNumber(hash, msg_sent.hdr.key);
        
		
			switch (msg_sent.hdr.op) {
				case CONF_OP:
					printf("\n************CONFIGURATION************\n");
					printConf();
					printf("\n************CUSTOM CONFIGURATION************\n");
					printCustomConf();
					printf("\n************STATISTICS************\n");
					printStats2(stdout);
					printf("ELEMENTS IN QUEUE:%d\n",thpool -> jobQueue ->elementsNumber);
					printf("\n**********************************\n");
                    setHeader(&msg_answer, OP_OK, &msg_sent.hdr.key);
                    setData(&msg_answer, NULL, 0);
                    break;
                
				case PUT_OP:   /* Operation put_op */
	
                    STATS_UPDATE(nput, NPUT);
					
                    if (put_op(msg_sent, hash, &msg_answer, &(thpool -> data_lock[hash_lock_val])) == -1) 
        
                        STATS_UPDATE(nput_failed, NPUT_F);
                        
					break;
                    
				case UPDATE_OP:   /* Operation update_op */
                
                    STATS_UPDATE(nupdate, NUPDATE);
                    
					if (update_op(msg_sent, hash, &msg_answer, &(thpool -> data_lock[hash_lock_val])) == -1) 
                        
                        STATS_UPDATE(nupdate_failed, NUPDATE_F);
					
					break;
                    
                case UPDATE_INSERT_OP: /* Operation update_insert_op */
                    
                    STATS_UPDATE(upd_ins, UPD_INS);
                    
                    if (update_insert_op(msg_sent, hash, &msg_answer, &(thpool -> data_lock[hash_lock_val])) == -1)
                        
                        STATS_UPDATE(upd_ins_failed, UPD_INS);;
                    
                    break;
                    
				case GET_OP:    /* Operation get_op */
					
                    STATS_UPDATE(nget, NGET);
					
                    if (get_op(msg_sent.hdr.key, hash, &msg_answer, &(thpool -> data_lock[hash_lock_val])) == -1) 
                        
                        STATS_UPDATE(nget_failed, NGET_F);
                       
					break;
                    
				case REMOVE_OP:   /* Operation remove_op */
                
                    STATS_UPDATE(nremove, NREMOVE);
					
                    if (remove_op(msg_sent.hdr.key, hash, &msg_answer, &(thpool -> data_lock[hash_lock_val])) == -1) 
                        
                        STATS_UPDATE(nremove_failed, NREMOVE_F); 
				
					break;
				
                case LOCK_OP:    /* Operation lock_op */
                
                    STATS_UPDATE(nlock, NLOCK);
					
                    if (lock_op(&msg_answer, &(thpool -> global_lock_id), &(thpool -> global_lock), thpool -> data_lock, thpool -> lock_number) == -1) 
                        
                        STATS_UPDATE(nlock_failed, NLOCK_F);
                    
					break;
				
                case UNLOCK_OP:    /* Operation unlock_op */
                
                    STATS_UPDATE(nunlock, NUNLOCK);
					
                    if (unlock_op(&msg_answer, &(thpool -> global_lock_id), &(thpool -> global_lock)) == -1) 
                        
                        STATS_UPDATE(nunlock_failed, NUNLOCK_F);
                    
					break;
				
                default:;
            
			}
		}
		
        if (sendRequest(fd_c, &msg_answer) == -1) {
            
            /* release the global lock if necessary */
            if (thpool -> global_lock_id == pthread_self()) {
                pthread_mutex_lock(&thpool -> global_lock);
                thpool -> global_lock_id = 0;
                pthread_mutex_unlock(&thpool -> global_lock);
            }
            
            /* decrease the number of current connections */
            pthread_mutex_lock(&statsLocks[CONC_CONN]);
            mboxStats.concurrent_connections--;
            pthread_mutex_unlock(&statsLocks[CONC_CONN]);
            
            /* close Client file descriptor socket */
            close(fd_c);
            
            return;
        }
        
      /*  mex(msg_sent.hdr.op);*/
    }
}



/**
 * The function executed by the thread which manage the signals.
 * 
 *
 * @param[in] args -- contains the struct tp_tfd, which have the
 *                 thread pool and the main socket file descriptor
 * 
 * 
 * @returns return only when a close signal is sent.
 * 
 */
void * signalHandler(void * args) {
    
    tp_fdc * tf = args;
    thpool_t * thpool = tf -> tp;
    int fd_skt = tf -> fd_skt;
    
    /* Unmasking Signal */
    sigset_t set;
    int signNum;
    int i;
    
    
    while (1) {
        
        sigemptyset(&set);
    
        if (sigaddset(&set, SIGINT) == -1) ;
        if (sigaddset(&set, SIGTERM) == -1) ;
        if (sigaddset(&set, SIGQUIT) == -1) ;
        if (sigaddset(&set, SIGUSR1) == -1) ;
        if (sigaddset(&set, SIGUSR2) == -1) ;
    
        if (sigwait(&set, &signNum) != 0) exit(EXIT_FAILURE);
        switch (signNum) {
            case SIGINT  :
            case SIGTERM :
            case SIGQUIT :
            
                /* This operation terminate the Server by closing ASAP the socket connection */
                /* Every thread will eventually (really fast) find out and terminate for the */
                /* isClosing operator setted. After this is done, a normal close is made.    */
                
				pthread_mutex_lock(&thpool -> jobQueue -> closingLock);
                thpool -> jobQueue -> isClosing = 1;
                pthread_mutex_unlock(&thpool -> jobQueue -> closingLock);
                
                /*the queue has been closed,now closing opened file descriptors*/
						
                for (i = 0; i < thpool -> num_threads; i++) 
                    if (thpool -> currentFD[i] != -1)
                        if (close(thpool -> currentFD[i]) == -1) {perror("can't close socket"); exit(EXIT_FAILURE);}
                
                /* This is to stop the Server accepting Client, is it fails no clean exit is possible */
                if (shutdown(fd_skt, SHUT_RDWR) < 0) exit(EXIT_FAILURE);
                
                return NULL;
				
            case SIGUSR1 :
                
                if (conf -> haveStats) {
                    
                    for (i = 0; i < NUM_STATS_ELEMENTS; i++) pthread_mutex_lock(&statsLocks[i]);
                    
                    if (printStats(statFile) == -1) perror("can't stamp on Stats file");
                    
                    for (i = 0; i < NUM_STATS_ELEMENTS; i++) pthread_mutex_unlock(&statsLocks[i]);
                }
                
                break;
                
            case SIGUSR2 :
                
                /* Soft exit: this operation terminate the server by waiting for every running client to close */
                
                if (conf -> haveLoads) {
                /* Opening file in only write to save the table after exit */
                    if ((tableFile = fopen(conf -> loadTable, "w")) == NULL) {
        
                        /* can't open the file, set flag to false and stop using loadTable */
                        conf -> haveLoads = 0;
                        perror("opening in only write");
                    } else {
                        
                        if (icl_hash_save(tableFile, (thpool -> jobQueue -> hash), mboxStats.current_size, mboxStats.current_objects) == -1) 
                            perror("can't save the table");
                
                    }
                }
                if(conf->haveLoads) if (fclose(tableFile) != 0) perror("errore chiusura tableFile");
                
                pthread_mutex_lock(&thpool -> jobQueue -> closingLock);
                thpool -> jobQueue -> isClosing = 1;
                pthread_mutex_unlock(&thpool -> jobQueue -> closingLock);
                
                /* This is to stop the Server accepting Client, is it fails no clean exit is possible */
                if (shutdown(fd_skt, SHUT_RDWR) < 0) exit(EXIT_FAILURE);
                
               
                return NULL;
                
            default:;
        }
    }
}

void loadFile(icl_hash_t * hash) {
    
    unsigned long numObj, totalDim;
    
    if ((tableFile = fopen(conf -> loadTable, "r")) == NULL) {
        
        /* Can't open in only read, so it doens't load the table */
        perror("can't load table file");
    } else {
        
        if (icl_hash_load(tableFile, hash, &totalDim, &numObj) == -1) {
            
            /* Can't load the file */
            perror("can't load correctly the table");
        } else {
            mboxStats.current_objects = numObj;
            mboxStats.max_objects = numObj;
            mboxStats.current_size = totalDim;
            mboxStats.max_size = totalDim;
        }
        if (fclose(tableFile) != 0) {
        
            /* can't close the file */
            perror("can't close the file");
            exit(EXIT_FAILURE);
        }
    }
}


static void use(const char * filename) {
    fprintf(stderr, 
	    "use:\n"
	    "1)TO LOAD A CUSTOM CONFIGURATION FILE:%s -f configuration_file_path \n"
	    "2)DEFAULT CONFIGURATION:%s\n",filename,filename);
}

int main(int argc, char *argv[]) {
    
    /* Create config Struct and load it with the configuration file */
	if((conf = (configs_t *) malloc(sizeof(configs_t))) == NULL) exit(EXIT_FAILURE);
    
	if((customConf = (custom_conf_t *) malloc(sizeof(custom_conf_t))) == NULL) exit(EXIT_FAILURE);

    int confRes;
	switch(argc){
		case 1:CREATE_STD_CONFIG;break;
		case 3:
			if (strcmp(argv[1],"-f")!=0){
				fprintf(stderr,"invalid option");
				use(argv[0]);
				free(conf);
				free(customConf);
				exit(EXIT_FAILURE);
			}
			else{ 
                FILE * configFile;
                if( (configFile = fopen(argv[2], "r")) == NULL) {perror("Can't open config File "); exit(EXIT_FAILURE);}
				confRes = loadConf(configFile);
                if( fclose(configFile) == -1) perror("Can't close the config File ");
				break;
			}
		default:
			use(argv[0]);
			free(conf);
			free(customConf);
            exit(EXIT_FAILURE);
	}	
	
	if (confRes == -1) {
        fprintf(stderr,"Couldn't load configuration");
		free(conf);
		free(customConf);
        exit(EXIT_FAILURE);
    }
    
    
    
    /// INIZIALIZING SERVER FUNCTIONS 
    
    
    /* Masking Signal */
    sigset_t set;
    struct sigaction sa_s;
    if (sigfillset(&set) == -1) exit(EXIT_FAILURE);
    if (pthread_sigmask(SIG_SETMASK, &set, NULL) == -1) exit(EXIT_FAILURE);
    memset(&sa_s, 0, sizeof(sa_s));
    
	
	int i;
	/* Inizializing Statistic locks */
    for (i = 0; i < NUM_STATS_ELEMENTS; i++) if (pthread_mutex_init(&(statsLocks[i]), NULL) != 0) {perror("Stats Mutex Initialization"); exit(EXIT_FAILURE);}
    
    /* Open statsFile */
	if (conf -> haveStats) 
        if ((statFile = fopen(conf -> statFileName,"a") ) == NULL) {
            
            /* Error opening statFile, sets flag to false and continue without it */
            conf -> haveStats = 0;
            perror("open");
        }
    
       
    /* Choosing how many row number should have the hash table */
    int hashRowNumber;
    
    if (conf -> storageSize) hashRowNumber = conf -> storageSize;
    else hashRowNumber = MAX_ROW_NUMBER;
	
    /* Preparing for socket connection */
	int fd_skt,fd_c; 
	struct sockaddr_un addr; 
	strncpy (addr.sun_path, conf->unixPath, 108);
	addr.sun_family = AF_UNIX;
	
    
    /* Creating socket connection */
	if ((fd_skt = socket (AF_UNIX, SOCK_STREAM, 0)) == -1 ) {perror("socket:"); exit(EXIT_FAILURE);}
	if (unlink (addr.sun_path) == -1) {perror("unlink:");}
	if (bind (fd_skt, (struct sockaddr *)&addr, sizeof(addr)) == -1) {perror("bind:"); exit(EXIT_FAILURE);}
	if (listen (fd_skt, SOMAXCONN) == -1) {perror("connect:"); exit(EXIT_FAILURE);}
    
    
    /* Creating Hash Table */
	icl_hash_t *hash = icl_hash_create(hashRowNumber, NULL , NULL);
    customConf -> hashBuckets = hashRowNumber;
    
    
    /* Loading old table if necessary and opening table File */
    if (conf -> haveLoads) loadFile(hash);
    
    
    /* Calculating the number of locks needed */
    int lockNumber;
    
    lockNumber = getTotalLockNumber(hashRowNumber);
    customConf -> locksNumber = lockNumber;
    
    
    /* Creating Client Connection Queue */
    jobQueue_t * clientQueue;
    if ((clientQueue = initQueue(hash)) == NULL) exit(EXIT_FAILURE);
    
    
    /* Creating Thread Pool */
    thpool_t * threadPool;
    if ((threadPool = thpool_init( conf -> threadsInPool, opExecute, clientQueue, lockNumber)) == NULL) exit(EXIT_FAILURE);
    
    
    /* Creating Signal Thread */
    pthread_t sigThread;
    
    tp_fdc tf;
    
    tf.tp = threadPool;
    tf.fd_skt = fd_skt;
    
    if (pthread_create(&sigThread, NULL, signalHandler, &tf) != 0) exit(EXIT_FAILURE);
    
    
    /// INIZIALIZATION COMPLETE
    
    
    
    /* Accepting Clients */
	while(1) {
        
            /* Wait for a new Client Connection */
		if ((fd_c = accept(fd_skt, (struct sockaddr *)NULL, NULL)) == -1) {
            
            /* Close connection for USR2 signal handling */
            pthread_mutex_lock(&clientQueue -> closingLock);
            if (clientQueue -> isClosing) {
            
                pthread_mutex_unlock(&clientQueue -> closingLock);
                close(fd_c);
                break;
                
            } else {
                perror("accept:");
                pthread_mutex_unlock(&clientQueue -> closingLock);
                exit(EXIT_FAILURE);
            }
                
            pthread_mutex_unlock(&clientQueue -> closingLock);
        
        } 
        
        
            /* Verifies if it can handle one more connection */
        if (mboxStats.concurrent_connections >= conf -> maxConnections ) {
            
            /* If it can't, send a error message to the client, wait 1 second and close the connection */
            
            message_t reply;
            unsigned long *key=malloc(sizeof(unsigned long)); *key = 0;
            
            setHeader(&reply, OP_FAIL, key);
            setData(&reply, NULL, 0); 
            
            /* Ignores failure */
            sendRequest(fd_c, &reply);
            
            close(fd_c);
            continue;
        }
        
        
            /* Put Client file descriptor socket in the queue for threads */
        if (queuePush(fd_c, clientQueue) == -1) {
            
            perror("client push:");
            continue;
        }
        
            /* If everything succeed, now the server is handling one more connection */
        STATS_UPDATE(concurrent_connections, CONC_CONN);
    }
    
            
    pthread_mutex_lock(&(clientQueue -> listMutex));
            
    clientQueue -> elementsNumber = 1;
    pthread_cond_broadcast(&(clientQueue -> hasJobs));
            
    pthread_mutex_unlock(&(clientQueue -> listMutex));
    
    /* closing everything */
    pthread_join(sigThread, NULL);
    
    for(i = 0; i < conf -> threadsInPool; i++) {
        
        pthread_join(*threadPool -> threads[i], NULL);
    }
    
    close(fd_skt);
    

    /* realeasing thread pool */
    threadPoolDestroy(threadPool);
    
    /* closing socket connections */
    tfd_t * curr = clientQueue -> head;
    while (curr != NULL) {
        close(curr -> tfd);
        curr = curr -> next;
    }
    
    /* releasing queue */
	queueDestroy(clientQueue);
    
    if(conf->haveStats) if (printStats(statFile) == -1) perror("errore stampa ");
	if(conf->haveStats) if (fclose(statFile) != 0) perror("errore chiusura statFile");
	free(conf);
	if (icl_hash_destroy(hash,free,free) != 0) perror("errore chiusura hash table");
	if (unlink (addr.sun_path) == -1) {perror("unlink:");}
	free(customConf);
    
    return 0;
}
