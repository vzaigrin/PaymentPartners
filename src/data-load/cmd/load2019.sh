#!/bin/bash

echo Load data into DWH
basedir=../../../data/2019
for p in $basedir/*
do
    partner=`basename $p`
    echo $partner
    for m in $p/*
    do
        month=`basename $m`
        echo $month
        for file in $m/*.csv
        do
            echo $file
            go run load_hist_data.go $partner 2019 $month $file
            sleep 600
        done
    done
done
echo -n Create reports
for num in {1..12}
do
    echo -n " " $num
    go run data2dm.go 2019 $num
    sleep 600
done
echo " done"
