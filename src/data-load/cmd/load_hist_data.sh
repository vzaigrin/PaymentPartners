#!/bin/bash

echo Load data into DWH
go run load_hist_data.go
echo -n Create reports
for num in {1..12}
do
    echo -n " " $num
    go run data2dm.go 2019 $num
done
echo " " done
