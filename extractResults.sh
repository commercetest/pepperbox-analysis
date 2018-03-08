#!/bin/bash

globalOut="out/general.csv"

if [ -f "$globalOut" ]; then
    echo "Found previous general results file [$globalOut] DELETING - you have 5 seconds to cancel"
    sleep 5
    rm $globalOut
fi

echo "originFileName,consumerLagMean" >> $globalOut

for file in $( ls $1 ); do
    filename=${file/'.csv'/''}
    sortedFile="${filename}.sorted-by-consumerLag"
    outputFile=$filename.result.csv
    consumerLagColumn=3

    if [ -f "$outputFile" ]; then
        echo "Found previous results file [$outputFile] DELETING - you have 5 seconds to cancel"
        sleep 5
        rm $outputFile
    fi

    head -1 $file > $outputFile

    if [ ! -f "$sortedFile" ]; then
        echo "Sorted file not found for [$file] - generating now"
        tail -n +2 $file | sort -t "," -k $consumerLagColumn -g >> $sortedFile
    else
        echo "Found previously sorted file [$file]"
    fi

    linecountStr=$(wc -l < $sortedFile)
    linecount=$(($linecountStr + 0))
    echo "File [$sortedFile] has [$linecount] lines"

    for i in `seq 1 100`;
    do
        percentileLine=$((($linecount / 100) * $i))
        echo "Reading [Percentile=$i] [Line=$percentileLine] from file [$sortedFile]"
        tmp=$(sed -n "${percentileLine}p" < $sortedFile)
        echo $tmp >> $outputFile
    done

    consumerLagMean=$(awk -F "," '{ total += $3 } END { print total/NR }' $file)
    
    echo "Consumer Lag Mean is [${consumerLagMean}]"

    echo "${file},${consumerLagMean}" >> $globalOut

    echo "Written results to [$outputFile]"
done

echo "Global results written to [$globalOut]"