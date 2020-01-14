#!/bin/bash

for num in {1..12}
do
    echo $num
    go run data2dm.go 2019 $num
done
