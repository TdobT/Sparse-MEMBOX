/**
 * @file icl_hash.c
 *
 * Dependency free hash table implementation.
 *
 * This simple hash table implementation should be easy to drop into
 * any other peice of code, it does not depend on anything else :-)
 * 
 * @author Jakub Kurzak
 */
/* $Id: icl_hash.c 2838 2011-11-22 04:25:02Z mfaverge $ */
/* $UTK_Copyright: $ */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <errno.h>

#include "icl_hash.h"

#include <limits.h>


#define BITS_IN_int     ( sizeof(int) * CHAR_BIT )
#define THREE_QUARTERS  ((int) ((BITS_IN_int * 3) / 4))
#define ONE_EIGHTH      ((int) (BITS_IN_int / 8))
#define HIGH_BITS       ( ~((unsigned int)(~0) >> ONE_EIGHTH ))
/**
 * A simple string hash.
 *
 * An adaptation of Peter Weinberger's (PJW) generic hashing
 * algorithm based on Allen Holub's version. Accepts a pointer
 * to a datum to be hashed and returns an unsigned integer.
 * From: Keith Seymour's proxy library code
 *
 * @param[in] key -- the string to be hashed
 *
 * @returns the hash index
 */

static unsigned int
hash_pjw(void* key)
{
    int num = *(int*) key;
    if (num++ % 256 == 0) return hash_pjw((void*) &(num));
    
    char *datum = (char *)key;
    unsigned int hash_value, i;

    if(!datum) return 0;

    for (hash_value = 0; *datum; ++datum) {
        hash_value = (hash_value << ONE_EIGHTH) + *datum;
        if ((i = hash_value & HIGH_BITS) != 0)
            hash_value = (hash_value ^ (i >> THREE_QUARTERS)) & ~HIGH_BITS;
    }
    
    return (hash_value);
}



static int long_compare(void* a, void* b) 
{
    return( *(long*)a == *(long*)b);
}


/**
 * Create a new hash table.
 *
 * @param[in] nbuckets -- number of buckets to create
 * @param[in] hash_function -- pointer to the hashing function to be used
 * @param[in] hash_key_compare -- pointer to the hash key comparison function to be used
 *
 * @returns pointer to new hash table.
 */
icl_hash_t *
icl_hash_create( int nbuckets, unsigned int (*hash_function)(void*), int (*hash_key_compare)(void*, void*) )
{
    icl_hash_t *ht;
    int i;

    ht = (icl_hash_t*) malloc(sizeof(icl_hash_t));
    if(!ht) return NULL;

    ht->nentries = 0;
    ht->buckets = (icl_entry_t**)malloc(nbuckets * sizeof(icl_entry_t*));
    if(!ht->buckets) return NULL;

    ht->nbuckets = nbuckets;
    for(i=0;i<ht->nbuckets;i++)
        ht->buckets[i] = NULL;

    ht->hash_function = hash_function ? hash_function : hash_pjw;
    ht->hash_key_compare = hash_key_compare ? hash_key_compare : long_compare;

    return ht;
}


/**
 * Print the table in STDOUT
 * 
 * @param[in] ht -- hash table to print
 * 
 */
void 
print_table(icl_hash_t * ht) 
{
    
    icl_entry_t* curr;

    if(!ht) return ;
    
    int cont;
    for (cont = 0; cont < ht -> nbuckets; cont++) {
        int * elem;
        for (curr=ht->buckets[cont]; curr != NULL; curr=curr->next) {
            printf("elem: %d ", cont);
            elem = (int*) curr -> key;
            printf(" key %d - \n", *elem);
        }
        
        
    }
    fflush(stdout);
}


/**
 * Search for an entry in a hash table.
 *
 * @param ht -- the hash table to be searched
 * @param key -- the key of the item to search for
 * @param length -- the length of the object is insert here
 *
 * @returns pointer to the data corresponding to the key.
 *   If the key was not found, returns NULL.
 */
void *
icl_hash_find(icl_hash_t *ht, void* key, unsigned int * length)
{
    icl_entry_t* curr;
    unsigned int hash_val;

    if(!ht || !key) return NULL;

    hash_val = (* ht->hash_function)(key) % ht->nbuckets;

    for (curr=ht->buckets[hash_val]; curr != NULL; curr=curr->next)
        if ( ht->hash_key_compare(curr->key, key))
        {
            * length = curr->length;
            return(curr->data);
        }

    return NULL;
}


/**
 * Insert an item into the hash table.
 *
 * @param ht -- the hash table
 * @param key -- the key of the new item
 * @param data -- pointer to the new item's data
 * @param length -- length of the data parameter
 * @param op_err -- pointer to the int value describing the (eventual) error
 *
 * @returns pointer to the new item.  Returns NULL on error.
 */
icl_entry_t *
icl_hash_insert(icl_hash_t *ht, void* key, void *data, unsigned int length, int *op_err)
{
    icl_entry_t *curr;
    unsigned int hash_val;
    
    *op_err = 0; /* no client errors */

    if(!ht || !key) return NULL;

    hash_val = (* ht->hash_function)(key) % ht->nbuckets;
    
    for (curr=ht->buckets[hash_val]; curr != NULL; curr=curr->next)
        if ( ht->hash_key_compare(curr->key, key))
        {
            *op_err = 13; /* OP_PUT_ALREADY error */
            return NULL; /* key already exists */
        }

        
    /* if key was not found */
    curr = (icl_entry_t*)malloc(sizeof(icl_entry_t));
    if(!curr) return NULL;
    
    curr->length = length;
    curr->key = key;
    curr->data = data;
    curr->next = ht->buckets[hash_val]; /* add at start */
    

    ht->buckets[hash_val] = curr;
    ht->nentries++;

    return curr;
}


/**
 * Replace entry in hash table with the given entry only if it exist.
 *
 * @param ht -- the hash table
 * @param key -- the key of the new item
 * @param data -- pointer to the new item's data
 * @param length -- the length of the data parameter
 * @param op_err -- pointer to the int value describing the (eventual) error
 * @param free_key -- free function to free the key
 * @param free_data -- free function to free the data
 *
 * @returns pointer to the new item.  Returns NULL on error.
 */
icl_entry_t *
icl_hash_update(icl_hash_t *ht, void* key, void *data, unsigned int length, int *op_err, void (*free_key)(void*), void (*free_data)(void*))
{
    icl_entry_t *curr, *prev;
    unsigned int hash_val;
    int flag_found = 0;

    if(!ht || !key) return NULL;

    hash_val = (* ht->hash_function)(key) % ht->nbuckets;

    /* Scan bucket[hash_val] for key */
    for (prev=NULL,curr=ht->buckets[hash_val]; curr != NULL; prev=curr, curr=curr->next)
        /* If key found, remove node from list, free old key, and setup olddata for the return */
        if ( ht->hash_key_compare(curr->key, key)) {
            flag_found = 1;
            if (curr -> length != length) {
                /* Error OP_UPDATE_SIZE */
                *op_err = 19;
                return NULL;
            }

            if (prev == NULL)
                ht->buckets[hash_val] = curr->next;
            else
                prev->next = curr->next;
            
            ht -> nentries--;
            break;
    }
    
    if (!flag_found) {
        /* Error OP_UPDATE_NONE */
        * op_err = 20;
        return NULL;
    }

    ht->nentries++;
    
    if (*free_key) (*free_key) (curr->key);
    if (*free_data) (*free_data) (curr->data);
    free(curr);
    
    /* Since key was either not found, or found-and-removed, create and prepend new node */
    curr = (icl_entry_t*)malloc(sizeof(icl_entry_t));
    if(curr == NULL) {ht->nentries--;return NULL;} /* out of memory */

    curr->length = length;
    curr->key = key;
    curr->data = data;
    curr->next = ht->buckets[hash_val]; /* add at start */

    ht->buckets[hash_val] = curr;
	
 

    return curr;
}


/**
 * Replace entry in hash table with the given entry.
 *
 * @param ht -- the hash table
 * @param key -- the key of the new item
 * @param data -- pointer to the new item's data
 * @param length -- the length of the data parameter
 * @param free_key -- free function to free the key
 * @param free_data -- free function to free the data
 *
 * @returns pointer to the new item.  Returns NULL on error.
 */
icl_entry_t *
icl_hash_update_insert(icl_hash_t *ht, void* key, void *data, unsigned int length, void (*free_key)(void*), void (*free_data)(void*))
{
    icl_entry_t *curr, *prev;
    unsigned int hash_val;

    if(!ht || !key) return NULL;

    hash_val = (* ht->hash_function)(key) % ht->nbuckets;

    /* Scan bucket[hash_val] for key */
    for (prev=NULL,curr=ht->buckets[hash_val]; curr != NULL; prev=curr, curr=curr->next)
        /* If key found, remove node from list */
        if ( ht->hash_key_compare(curr->key, key)) {

            if (prev == NULL)
                ht->buckets[hash_val] = curr->next;
            else
                prev->next = curr->next;
            
            ht -> nentries--;
            
            break;
        }

    ht->nentries++;
    
    /* Since key was either not found, or found-and-removed, create and prepend new node */
    curr = (icl_entry_t*)malloc(sizeof(icl_entry_t));
    if(curr == NULL) {ht->nentries--;return NULL;} /* out of memory */

    curr->length = length;
    curr->key = key;
    curr->data = data;
    curr->next = ht->buckets[hash_val]; /* add at start */

    ht->buckets[hash_val] = curr;
    
    return curr;
}


/**
 * Free one hash table entry located by key (key and data are freed using functions).
 *
 * @param ht -- the hash table to be freed
 * @param key -- the key of the new item
 * @param free_key -- pointer to function that frees the key
 * @param free_data -- pointer to function that frees the data
 *
 * @returns 0 on success, -1 on failure.
 */
int icl_hash_delete(icl_hash_t *ht, void* key, void (*free_key)(void*), void (*free_data)(void*))
{
    icl_entry_t *curr, *prev;
    unsigned int hash_val;
    unsigned int len = 0;

    if(!ht || !key) return -1;
    hash_val = (* ht->hash_function)(key) % ht->nbuckets;

    prev = NULL;
    for (curr=ht->buckets[hash_val]; curr != NULL; )  {
        if ( ht->hash_key_compare(curr->key, key)) {
            if (prev == NULL) {
                ht->buckets[hash_val] = curr->next;
            } else {
                prev->next = curr->next;
            }
            len = curr -> length;
            if (*free_key && curr->key) (*free_key)(curr->key);
            if (*free_data && curr->data) (*free_data)(curr->data);
            ht->nentries++;
            free(curr);
            return len;
        }
        prev = curr;
        curr = curr->next;
    }
    return -1;
}


/**
 * Free hash table structures (key and data are freed using functions).
 *
 * @param ht -- the hash table to be freed
 * @param free_key -- pointer to function that frees the key
 * @param free_data -- pointer to function that frees the data
 *
 * @returns 0 on success, -1 on failure.
 */
int
icl_hash_destroy(icl_hash_t *ht, void (*free_key)(void*), void (*free_data)(void*))
{
    icl_entry_t *bucket, *curr, *next;
    int i;

    if(!ht) return -1;

    for (i=0; i<ht->nbuckets; i++) {
        bucket = ht->buckets[i];
        for (curr=bucket; curr!=NULL; ) {
            next=curr->next;
            if (*free_key && curr->key) (*free_key)(curr->key);
            if (*free_data && curr->data) (*free_data)(curr->data);
            free(curr);
            curr=next;
        }
    }

    if(ht->buckets) free(ht->buckets);
    if(ht) free(ht);

    return 0;
}


/**
 * Dump the hash table's contents to the given file pointer.
 *
 * @param stream -- the file to which the hash table should be dumped
 * @param ht -- the hash table to be dumped
 *
 * @returns 0 on success, -1 on failure.
 */
int
icl_hash_dump(FILE* stream, icl_hash_t* ht)
{
    icl_entry_t *bucket, *curr;
    int i, len;

    if(!ht) return -1;

    for(i=0; i<ht->nbuckets; i++) {
        len = 0;
        bucket = ht->buckets[i];
        for(curr=bucket; curr!=NULL; ) {
            len++;
            if(curr->key)
                fprintf(stream, "icl_hash_dump: %ld: %d\n", *(long *)curr->key, i);
            curr=curr->next;
        }
        fprintf(stream, "\n\n indice %d : elementi %d \n\n", i, len);
    }

    return 0;
}


/**
 * Save the hash table content in binary for in the given file.
 *
 * @param[out] fout -- the file to which the hash table should be saved
 * @param[in] ht -- the hash table to be saved
 * @param[in] curr_size -- the total size of the object inside the hash table
 * @param[in] curr_obj_num -- the number of elements in the hash table
 *
 * @returns 0 on success, -1 on failure.
 */
int
icl_hash_save(FILE* fout, icl_hash_t * ht, unsigned long curr_size, unsigned long curr_obj_num) 
{
    
    icl_entry_t *curr;
    int i;
    
    if(!ht) {errno = EINVAL; return -1;}
    
    if(fwrite(&curr_size, sizeof(unsigned long), 1, fout) != 1) {errno = EINVAL; return -1;}
    if(fwrite(&curr_obj_num, sizeof(unsigned long), 1, fout) != 1) {errno = EINVAL; return -1;}
    
    for (i = 0; i < ht -> nbuckets; i++) {
        
        curr = ht -> buckets[i];
        
        while (curr != NULL) {
            if (curr -> key && curr -> data) {
                // write key
                if(fwrite(curr -> key, sizeof(long), 1, fout) != 1) {errno = EINVAL; return -1;}
                // write lenght
                if(fwrite(&curr -> length, sizeof(unsigned int), 1, fout) != 1) {errno = EINVAL; return -1;}
                // write data
                if(fwrite(curr -> data, curr -> length, 1, fout) != 1) {errno = EINVAL; return -1;}
                
                curr = curr -> next;
            }
        }
    }
    
    return 0;
}


/**
 * Load the hash table content in binary form, from a given file.
 * 
 *
 * @param[in] fin -- the file from which the hash table should be loaded
 * @param[out] ht -- the hash table to be load
 * @param[out] curr_size -- the total size of the object inside the hash table
 * @param[out] curr_obj_num -- the number of elements in the hash table
 *
 * @returns 0 on success, -1 on failure.
 */
int
icl_hash_load(FILE* fin, icl_hash_t * ht, unsigned long * curr_size, unsigned long * curr_obj_num) 
{
    
    long * key;
    unsigned int length;
    int op_err, curVal;
    char * data;
    
    if (!ht) {errno = EINVAL; return -1;}
    
    if (fread(curr_size, sizeof(unsigned long), 1, fin) != 1) {errno = EINVAL; return -1;}
    if (fread(curr_obj_num, sizeof(unsigned long), 1, fin) != 1) {errno = EINVAL; return -1;}
    
    while (1) {
        
        key = (long*) malloc(sizeof(long));
        
        if (( curVal = fread(key, sizeof(long), 1, fin)) != 1) {
            free(key);
            
            if (curVal != 0) {errno = EINVAL; return -1;}
            else break;
        }
        
        if (fread(&length, sizeof(unsigned int), 1, fin) != 1) {free(key); errno = EINVAL; return -1;}
        
        data = (char*) malloc(sizeof(char) * length);
        
        if (fread(data, length, 1, fin) != 1) {free(key); free(data); errno = EINVAL; return -1;}
        
        if (icl_hash_insert(ht, (void*)(key), (void*)(data), length, &op_err) == NULL) {free(key); free(data); errno = EINVAL; return -1;}
    }
    
    
    if (ferror(fin) != 0) {errno = EINVAL; return -1;}
    
    return 0;
}


