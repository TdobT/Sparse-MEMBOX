#ifndef membox_h
#define membox_h

   /** \file membox.c  
       \author Vlad Pandelea 519231 & Emanuele Aurora 520224  
     Si dichiara che il contenuto di questo file e' in ogni sua parte opera  
     originale degli autori.
     *   */  
     
#include <limits.h>
#define PATH_MAX 4096
typedef struct configs_s {
    int maxConnections;
    int threadsInPool;
    int storageSize;
    int storageByteSize;
    int maxObjSize;
    int haveStats;
    int haveLoads;
    char loadTable[PATH_MAX];
    char statFileName[PATH_MAX];
    char unixPath[PATH_MAX];
} configs_t;

typedef struct custom_conf_s {
    int locksNumber;
    int hashBuckets;
    int currentConnections;
} custom_conf_t;

typedef struct item_s {
	char * data;
	long key;	
    unsigned int length;
}item_t;

#endif
