#!/bin/bash

basedir=../../data/2019
for file in templates/*
do
    partner=`basename ${file} .json`
    mkdir -p $basedir/hist/$partner
    mkdir -p $basedir/12/$partner
    for num in {1..11}
    do
        ./data-gen -t $file -o $basedir/hist/$partner/$partner-${num}.csv -y 2019 -m $num -e 10 -v
    done
    ./data-gen -t $file -o $basedir/12/$partner/$partner-12.csv -y 2019 -m 12 -e 10 -v
done
