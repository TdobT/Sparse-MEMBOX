#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/socket.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <sys/un.h>

#include "connections.h"

   /** \file membox.c  
       \author Vlad Pandelea 519231 & Emanuele Aurora 520224  
     Si dichiara che il contenuto di questo file e' in ogni sua parte opera  
     originale degli autori.
     *   */  

/**
 * Try ntimes times to open a connection with the socket in path (secs delay for every try).
 *
 * @param[in] path -- the path wich is located the socket
 * @param[in] ntims -- number of try to attend
 * @param[in] secs -- delay for every try
 *
 * @returns result from connect is succeed, -1 otherwise
 * 
 */
int openConnection(char * path, unsigned int ntimes, unsigned int secs){
	struct sockaddr_un sa;
	strncpy(sa.sun_path, path ,108);
	sa.sun_family=AF_UNIX;
	int fd_skt,conn;
	
	if( (fd_skt = socket(AF_UNIX,SOCK_STREAM,0)) == -1 ) perror("socket:");
    
	while (ntimes-- > 0 && (conn = connect(fd_skt,(struct sockaddr*)&sa,sizeof(sa))) == -1 ) sleep(secs);
	
	return !conn ? fd_skt : -1;
}
	
	
/**
 * Send a request contained in msg at file descriptor fd.
 *
 * @param[in] fd -- file descriptor 
 * @param[in] msg -- the message containing the request
 *
 * @returns 0 if succeed, -1 otherwise
 * 
 */
int sendRequest(long fd, message_t * msg){
    
    /* Write the header inside the socket fd */
    if (write (fd, &(msg -> hdr).op, sizeof(op_t)) == -1) return -1;
    if (write (fd, &(msg -> hdr).key, sizeof(membox_key_t)) == -1) return -1;
	
    /* Write the data inside the socket fd (if it exist) */
    if ((msg -> data).buf != NULL) {
        if (write (fd, &(msg -> data).len, sizeof(unsigned int)) == -1) return -1;
        if (write (fd, (msg -> data).buf, (msg -> data).len) == -1) return -1;
        
    }
    
    return 0;
}


/**
 * Read the header at file descriptor fd, and place it in hdr.
 *
 * @param[in] fd -- file descriptor 
 * @param[out] hdr -- the result from the read
 *
 * @returns 0 if succeed, -1 otherwise
 * 
 */
int readHeader(long fd, message_hdr_t * hdr) {
    
    if (read(fd, &hdr->op, sizeof(op_t)) <= 0) return -1;
    if (read(fd, &hdr->key, sizeof(membox_key_t)) <= 0) return -1;

    return 0;
}


/**
 * Read the data at file descriptor fd, and place it in data.
 *
 * @param[in] fd -- file descriptor 
 * @param[out] data -- the result from the read
 *
 * @returns 0 if succeed, -1 otherwise
 * 
 */
int readData(long fd, message_data_t * data) {
    
    if (read(fd, &data->len, sizeof(unsigned int)) <= 0) return -1;
    
    if ((data->buf = malloc(sizeof(char)*(data->len))) == NULL) return -1;
    
    /* The read must be made in a cycle for message too long */
    int byteRead = 0, seqRead, maxRead = data -> len;
    while ((seqRead = read(fd, data -> buf + byteRead, maxRead)) > 0) {
        byteRead += seqRead;
        if (byteRead >= data -> len) break;
        if (maxRead > data -> len - byteRead) maxRead = data -> len - byteRead;
    }
    if (seqRead == -1  || seqRead == 0) {
        
        free(data -> buf);
        return -1;
    }
    
    return 0;
}


/**
 * Read all the message, using readHeader and readData.
 *
 * @param[in] fd -- file descriptor 
 * @param[out] msg -- the result from the read
 *
 * @returns 0 if succeed, -1 otherwise
 * 
 */
int readMessage(long fd, message_t * msg) {
    
    if (readHeader (fd, &(msg -> hdr)) == -1) return -1 ;
    if ((msg -> hdr).op < 2 || (msg -> hdr).op == 6) if (readData(fd, &(msg -> data)) == -1) return -1; 
    return 0;
}
