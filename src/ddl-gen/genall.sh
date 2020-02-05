#!/bin/bash

basedir=../../DWH/Partners
for file in $basedir/templates/*.json
do
    partner=`basename ${file} .json`
    ./ddl-gen -t $file -o $basedir/DDL/$partner.sql -p $partner -v
done
