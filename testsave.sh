#!/bin/bash

for ((i=0;i<500;++i)); do  

    ./client -l $1 -c $i:0:100&

done

wait

#statistic
killall -USR1 membox
sleep 1
read put putfailed <<< $(tail -1 $2 | cut -d\  -f 3,4 )
 
if [[ $put != 500 || $putfailed != 0 ]]; then 
    echo "Test fallito"
    exit 1
fi
