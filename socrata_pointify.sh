#!/bin/bash
#
# To use: socrata_pointify.sh <longitude_col> <latitude_col> <new_col_name> file1 file2 ...
# Takes each input file, and adds a new column named <new_col_name> from the 
# longitude_col and latitude_col.  The column has GeoJSON Point with longitude first, per
# Socrata SoQL ingestion convention.
#
# Uses the csv command line utils https://csvkit.readthedocs.org/en/0.9.1/#

# set -ex

longitude_col=$1
latitude_col=$2
new_col_name=$3
shift 3
for f in $@; do
    echo Generating point column for file $f...
    echo $new_col_name >"${f}.point"
    csvcut -c $longitude_col,$latitude_col $f | tail -n +2 | ruby -np -e 'require "CSV"; $_ = ["{\"type\":\"Point\",\"coordinates\":[#{$_.strip}]}"].to_csv' >>"${f}.point"
    csvjoin $f "${f}.point" >"${f}.new"
    rm -f "${f}.point" $f
    mv "${f}.new" $f
done