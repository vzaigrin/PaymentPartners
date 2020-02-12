#!/bin/bash

# Вызов: ./genyear.sh year

year=$1
basedir=../../data/$year
for file in ../../DWH/Partners/templates/*.json
do
    partner=`basename ${file} .json`
    mkdir -p $basedir/$partner
    for num in {1..12}
    do
    	mkdir -p $basedir/$partner/$num
        ./data-gen -t $file -o $basedir/$partner/$num/$partner-${num}.csv -y $year -m $num -e 10 -v
    done
done
