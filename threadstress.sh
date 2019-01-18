#!/bin/bash

for ((k=0;k<50;++k)); do 

	com="-c $k:0:10 -c $k:3:10"

	for ((i=0;i<11;++i)); do 
		com="$com $com"
	done
    # si fa lo spawn di un processo che esegue tutte le operazioni definite in $com
    ./client -l $1 $com &
done

wait 
