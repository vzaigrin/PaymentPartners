#!/bin/bash

for file in templates/*
do
    for num in {1..12}
    do
        ./data-gen -t $file -o data/`basename ${file} .json`-${num}.csv -y 2019 -m $num -e 10 -v
    done
done
