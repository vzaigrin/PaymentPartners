#!/bin/bash

basedir=../../data/2019
for file in templates/*
do
    partner=`basename ${file} .json`
    mkdir -p $basedir/$partner
    for num in {1..12}
    do
        ./data-gen -t $file -o $basedir/$partner/$partner-${num}.csv -y 2019 -m $num -e 10 -v
    done
done
