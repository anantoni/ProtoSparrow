#!/bin/bash

classPath="$1"

for (( i = 0; i < 10; i++))
do

    java -jar dist/ProtoClient.jar &

    procs[${i}]=$!

done

for proc in `echo ${procs[@]}`
do
    wait $proc

done
