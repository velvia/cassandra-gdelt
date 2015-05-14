#!/bin/bash
# To use: split_csv.sh my_file.csv <max_#_lines_per_file>
# creates my_file_partNN.csv
# with the original header from my_file.csv in each part
# Particularly useful to split CSV files and retain headers for REST upload APIs
# 
# NOTE: assumes original file is in a working dir by itself.  Writes many temp files into the working dir,
# which is the dir containing the csv file.

# set -ex

csv=$1
max_lines=$2
csvdir=$(dirname $1)
filename=$(basename "$csv")
extension="${filename##*.}"
filename="${filename%.*}"
header_file="$csvdir/_header_"
body_file="$csvdir/_body_"
part_stem="$csvdir/${filename}_part"

# Get header and get orig file without header
echo Splitting original file into header and body...
head -1 $csv >$header_file
tail -n +2 $csv >$body_file

# Split files
echo Splitting body...
split -l $max_lines $body_file $part_stem

# Join header to each file
echo Joining header to each part file...
for part in $(ls ${part_stem}*); do
    cat $header_file $part >"$part.$extension"
    rm $part
done

# Clean up
rm -f $header_file
rm -f $body_file