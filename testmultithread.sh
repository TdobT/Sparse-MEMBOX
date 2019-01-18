#!/bin/bash

./membox -f $2 &

time1=$( { time ./threadstress.sh $1; } 2>&1 )

echo "tempo single thread:"
echo $time1

killall -USR2 -w membox;
sleep 1

./membox -f $3 &

time2=$( { time ./threadstress.sh $1; } 2>&1 )

echo "tempo multithread"
echo $time2

killall -USR2 -w membox;

if [[ $time2 > $time1 ]]; then
    echo "test fallito"
    exit 1
fi

