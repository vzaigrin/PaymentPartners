#!/bin/bash

for file in templates/*
do
    partner=`basename ${file} .json`
    mkdir -p ../../data/2019/$partner
    for num in {1..12}
    do
        ./data-gen -t $file -o ../../data/2019/$partner/$partner-${num}.csv -y 2019 -m $num -e 10 -v
    done
done
