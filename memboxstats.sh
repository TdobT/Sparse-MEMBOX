#!/bin/bash
  
#no arguments passed
if [ $# -eq 0 ];
then
echo "--help for usage example" >&2
exit -1
fi

#will be 1 if there is -m option
flagM=0

flagTimeStamp=0
 
while true;
do
  case "$1" in
  
	--help)
	echo "Usage:./memboxstats.sh [OPTION]... [FILE]"
	echo -e "Parses a membox stats file,showing for each timestamp(-m intended as the max of all timestamps)"
	echo -e "If only FILE is specified then the last timestime statistics are displayed\n"
	echo -e "-p,\tDisplays the number of PUT operation and FAILEDPUT"
	echo -e "-u,\tDisplays the number of UPDATE operation and FAILEDUPDATE"
	echo -e "-g,\tDisplays the number of GET operation and FAILEDGET"
	echo -e "-r,\tDisplays the number of REMOVE operation and FAILEDREMOVE"
	echo -e "-c,\tDisplays the number of concurrent connections"
	echo -e "-s,\tDisplays the repository size"
	echo -e "-o,\tDisplays the number of objects"
	echo -e "-m,\tDisplays MAXCONN,MAXSIZE,MAXOBJ"
	echo -e "\nIf no option is specified,last timestamp statistics will be displayed"
	exit -1;;
	
    -p) #put_op and put_op failed
      fieldP="PUT PUTFAILED "
      flagTimeStamp=1;
      shift;;

    -u) #update_op and update_op failed
      fieldU="UPDATE UPDATEFAILED "
      flagTimeStamp=1;
      shift;;
 
    -g)#get_op and get_op failed
      fieldG="GET GETFAILED "
	  flagTimeStamp=1
      shift;;
 
    -r)#remove_op and remove_op failed
      fieldR="REMOVE REMOVEFAILED "
	  flagTimeStamp=1
      shift;;
            
    -c)#concurrent connections
      fieldC="CONNECTIONS "
	  flagTimeStamp=1
  	  shift;;
	  
	-s)#size in KBs
	  fieldS="SIZE(KBS) "
	  flagTimeStamp=1
	  shift;;
	  
	-o)#number of objects
      fieldO="OBJECTS "
	  flagTimeStamp=1
	  shift;;
	  
	-m)#max numero di connessioni raggiunte dal server, max numero di oggetti memorizzati e max size in KB raggiunta.
		flagM=1
	  shift;;	
	  
	"")
		break;;
	  
	*)
		last=$1
		if [ ! -f "$last" ]
		then
		echo "$0 invalid parameter" >&2
		exit -1
		fi
		
		shift;;

  esac
done

#no stats file specified
if [ -z "$last" ]
then
echo -e "$0 no stats file specified,\nrerun with --help for usage " >&2
exit 1
fi

#lines in stats file
lines=$(cat $last | wc -l)

#no options have been specified,will only show last timestamp statistics
if [[ flagTimeStamp -eq 0 ]] && [[ flagM -eq 0 ]]
then
i=$(($lines -1))
fieldP="PUT PUTFAILED "
fieldU="UPDATE UPDATEFAILED "
fieldG="GET GETFAILED "
fieldR="REMOVE REMOVEFAILED "
fieldC="CONNECTIONS "
fieldS="SIZE(KBS) "
fieldO="OBJECTS "
flagTimeStamp=1
else 
i=0
fi

#parse proper statistics specified by the options

if [ -n "$fieldP" ]
then
pok=($(cut -d ' ' -f3 $last))
pfail=($(cut -d ' ' -f4 $last))
fi

if [ -n "$fieldU" ]
then
uok=($(cut -d ' ' -f5 $last))
ufail=($(cut -d ' ' -f6 $last))
fi

if [ -n "$fieldG" ]
then
gok=($(cut -d ' ' -f7 $last))
gfail=($(cut -d ' ' -f8 $last))
fi

if [ -n "$fieldR" ]
then
rok=($(cut -d ' ' -f9 $last))
rfail=($(cut -d ' ' -f10 $last))
fi

if [ -n "$fieldC" ]
then
conn=($(cut -d ' ' -f13 $last))
fi

if [ -n "$fieldS" ]
then
size=($(cut -d ' ' -f14 $last))
fi

if [ -n "$fieldO" ]
then
obj=($(cut -d ' ' -f16 $last))
fi

(
#only do this if at least an option other than -m has been specified
if [[ flagTimeStamp -eq 1 ]]
then
	TS="timestamp "
	#timestamps into array
	timestamp=($(cut -d ' ' -f1 $last))


	#print the fields specified by options
	echo $TS$fieldP$fieldU$fieldG$fieldR$fieldC$fieldS$fieldO$fieldM

	#parsing the statistics for all timestamps
	for ((; i<=$lines; i++))
		do
		echo -e ${timestamp[$i]}"\t"${pok[$i]}"\t"${pfail[$i]}"\t"${uok[$i]}"\t"${ufail[$i]}"\t"${gok[$i]}"\t"${gfail[$i]}"\t"${rok[$i]}"\t"${rfail[$i]}"\t"${conn[$i]}"\t"${size[$i]}"\t"${obj[$i]}
	done

fi 

#if -m option 
if [[ $flagM -eq 1 ]]
then

	maxconn=0
	maxsize=0
	maxobj=0

	#calculate max number of concurrent connections
	a=($(cut -d ' ' -f13 $last))
	for n in "${a[@]}" ; do
		((n > maxconn)) && maxconn=$n
	done	

	#calculate max size
	a=($(cut -d ' ' -f14 $last))
	for n in "${a[@]}" ; do
		((n > maxsize)) && maxsize=$n
	done

	#calculate max number of objects
	a=($(cut -d ' ' -f16 $last))
	for n in "${a[@]}" ; do
		((n > maxobj)) && maxobj=$n
	done

	if [[ flagTimeStamp -eq 1 ]] 	
	then
	echo -e "\n"
	fi
	
	echo "MAXCONNECTIONS MAXSIZE MAXOBJ" 
	
	echo -e $maxconn"\t"$maxsize"\t"$maxobj

fi

)  | column -t

