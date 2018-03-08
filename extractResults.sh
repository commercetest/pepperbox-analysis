#!/bin/bash

for filename in $( ls $1 ); do
    sortedFile="${filename}.sorted"
    outputFile="$filename.result.csv"

    if [ -f "$outputFile" ]; then
        echo "Found previous results file [$outputFile] DELETING - you have 5 seconds to cancel"
        sleep 5
        rm $outputFile
    fi

    head -1 $filename > $outputFile

    if [ ! -f "$sortedFile" ]; then
        echo "Sorted file not found for [$filename] - generating now"
        tail -n +2 $filename | sort -t "," -k 3 -g >> $sortedFile
    else
        echo "Found previously sorted file [$filename]"
    fi

    linecountStr=$(wc -l < $sortedFile)
    linecount=$(($linecountStr + 0))
    echo "File [$sortedFile] has [$linecount] lines";

    for i in `seq 1 100`;
    do
        percentileLine=$((($linecount / 100) * $i))
        echo "Reading [Percentile=$i] [Line=$percentileLine] from file [$sortedFile]"
        tmp=$(sed -n "${percentileLine}p" < $sortedFile)
        echo $tmp >> $outputFile
    done

    echo "Written results to [$outputFile]"

done