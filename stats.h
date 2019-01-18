#if !defined(MEMBOX_STATS_)
#define MEMBOX_STATS_

#include <stdio.h>
#include <time.h>

   /** \file membox.c  
       \author Vlad Pandelea 519231 & Emanuele Aurora 520224  
     Si dichiara che il contenuto di questo file e' in ogni sua parte opera  
     originale degli autori.
     *   */  
     
/**
 *  @struct statistics
 *  @brief Struttura dati contenente le informazioni da monitorare.
 */
struct statistics {
    unsigned long nput;                         // n. di put
    unsigned long nput_failed;                  // n. di put fallite 
    unsigned long nupdate;                      // n. di update
    unsigned long nupdate_failed;               // n. di update fallite
    unsigned long nget;                         // n. di get
    unsigned long nget_failed;                  // n. di get fallite
    unsigned long nremove;                      // n. di remove
    unsigned long nremove_failed;               // n. di remove fallite
    unsigned long nlock;                        // n. di lock
    unsigned long nlock_failed;                 // n. di lock fallite
    unsigned long nunlock;						// n. di unlock
    unsigned long nunlock_failed;				// n. di unlock fallite
    unsigned long concurrent_connections;       // n. di connessioni concorrenti
    unsigned long current_size;                 // size attuale del repository (bytes)
    unsigned long max_size;                     // massima size raggiunta dal repository
    unsigned long current_objects;              // numero corrente di oggetti
    unsigned long max_objects;                  // massimo n. di oggetti raggiunto
    unsigned long upd_ins;                      // n. di update_insert
    unsigned long upd_ins_failed;               // n. di update_insert fallite
    unsigned long fail_lock;                    // n. di operazioni fallite per lo stato di lock globale
    
};


/* Stats lock name */
typedef enum {
	
	NPUT 			= 0,
	NPUT_F			= 1,
	NUPDATE			= 2,
	NUPDATE_F		= 3,
	NGET			= 4,
	NGET_F			= 5,
	NREMOVE			= 6,
	NREMOVE_F		= 7,
	NLOCK			= 8,
	NLOCK_F			= 9,
	NUNLOCK			= 10,
	NUNLOCK_F		= 11,
	CONC_CONN		= 12,
	CURR_MAX_SIZE	= 13,						// double lock per impedire il deadlock
	CURR_MAX_OBJ	= 14,						// double lock per impedire il deadlock
	LOCK_NUMBER		= 15,
    UPD_INS         = 16,
    FAIL_LOCK       = 17
    
} STATS_T;
	

/**
 * @function printStats
 * @brief Stampa le statistiche nel file passato come argomento
 *
 * @param fout descrittore del file aperto in append.
 *
 * @return 0 in caso di successo, -1 in caso di fallimento 
 */
static inline int printStats2(FILE *fout) {
    extern struct statistics mboxStats;

    if (fprintf(fout, "TIME:%ld\nPUTS:%ld\nPUTS FAILED:%ld\nUPDATES:%ld\nUPDATES FAILED:%ld\nGETS:%ld\nGETS FAILED:%ld\nREMOVES:%ld\nREMOVES FAILED: %ld\nLOCKS: %ld\nLOCKS FAILED: %ld\nCONCURRENT CONNECTIONS:%ld\nCURRENT SIZE: %ld\nMAX SIZE: %ld\nCURRENT OBJECTS:%ld\nMAX OBJECTS: %ld\n",
		(unsigned long)time(NULL),
		mboxStats.nput, mboxStats.nput_failed,
		mboxStats.nupdate, mboxStats.nupdate_failed,
		mboxStats.nget, mboxStats.nget_failed,
		mboxStats.nremove, mboxStats.nremove_failed,
		mboxStats.nlock, mboxStats.nlock_failed,
		mboxStats.concurrent_connections,
		mboxStats.current_size, mboxStats.max_size,
		mboxStats.current_objects, mboxStats.max_objects) < 0) return -1;
    fflush(fout);
    return 0;
}


/**
 * @function printStats
 * @brief Stampa le statistiche nel file passato come argomento
 *
 * @param fout descrittore del file aperto in append.
 *
 * @return 0 in caso di successo, -1 in caso di fallimento 
 */
static inline int printStats(FILE *fout) {
    extern struct statistics mboxStats;

    if (fprintf(fout, "%ld - %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n",
		(unsigned long)time(NULL),
		mboxStats.nput, mboxStats.nput_failed,
		mboxStats.nupdate, mboxStats.nupdate_failed,
		mboxStats.nget, mboxStats.nget_failed,
		mboxStats.nremove, mboxStats.nremove_failed,
		mboxStats.nlock, mboxStats.nlock_failed,
		mboxStats.concurrent_connections,
		mboxStats.current_size, mboxStats.max_size,
		mboxStats.current_objects, mboxStats.max_objects) < 0) return -1;
    fflush(fout);
    return 0;
}

#endif /* MEMBOX_STATS_ */
