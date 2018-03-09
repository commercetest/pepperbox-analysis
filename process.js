const processes = [{
    name: 'Throughput X Timestamp',
    filenamePrefix: 'throughputXtimestamp',
    process: makeThroughputSeries
}, {
    name: 'Latency X Timestamp',
    filenamePrefix: 'latencyXtimestamp',
    process: makeLatencySeries
}];















function transpose(data) {
    const keys = Object.keys(data);
    const key = keys.sort((a, b) => data[a].length > data[b].length ? 1 : -1)[0];

    return data[key].map((_, index) => {
        return keys.reduce(function (acc, key) {
            acc[key] = data[key][index];
            return acc;
        }, {});
    });
}

function flatten(acc, val) {
    return acc.concat(val);
}

function untranspose(data) {
    return data.reduce((acc, val, index) => {
        const keys = Object.keys(val);
        keys.forEach(key => {
            acc[key] = acc[key] || [];
            acc[key][index] = val[key];
        });
        return acc;
    }, {});
}

function mergeOn(key, ...datasets) {
    return untranspose(
        datasets.map(transpose)
        .reduce(flatten, [])
        .sort((a, b) => a[key] > b[key] ? 1 : -1));
}

function makeLatencySeries(data) {
    return new Promise((resolve, reject) => {
        resolve(data.batchReceived.map((ts, index) => {
            return [ts, data.consumerLag[index]];
        }).filter(a => a[0] && a[1]));
    });
}

function makeThroughputSeries(data) {
    return new Promise((resolve, reject) => {
        const throughputXTime = data.batchReceived
            .reduce((acc, ts, index) => {
                const secondTs = Math.floor(ts / 1000);
                acc[secondTs] = acc[secondTs] || 0;
                acc[secondTs]++;
                return acc;
            }, {});

        const throughputXTimeSeries = Object.keys(throughputXTime).map(Number).sort()
            .map(ts => {
                return [ts * 1000, throughputXTime[ts]];
            })
            .filter(a => a[0] && a[1]);

        resolve(throughputXTimeSeries);
    });
}

function parseCSV(fileString) {
    const rows = fileString.split('\n');
    const header = rows[0].split(',');
    const data = rows.slice(1).reduce((acc, row) => {
        const rowData = row.split(',');
        header.forEach((key, index) => {
            acc[key] = acc[key] || [];
            let val = rowData[index];
            if (val && !isNaN(Number(val))) {
                val = Number(val);
            }
            acc[key].push(val);
        });
        return acc;
    }, {});
    return data;
}

function processDataset(fileName, fileData) {
    processes.forEach(async (process) => {
        console.info(`Generating [${process.name}]`);

        const processedData = await process.process(fileData, process);
        const outData = processedData.map(row => {
            return row.join(','); //TODO: use proper CSV serialization
        }).join('\n');

        const outFilePath = path.join(outDir, `${process.filenamePrefix}-${fileName}`);

        fs.writeFileSync(outFilePath, outData, 'utf8');

        console.info(`[${new Date().toUTCString()}] File outputted to [${outFilePath}]`);
    });
}


const fs = require('fs');
const path = require('path');
const inDir = path.join(__dirname, './in/');
const outDir = path.join(__dirname, './out/');

console.info(`Reading all .csv files from [${inDir}]`);

// fs.readdir(inDir, (err, files) => {
//     const csvFiles = files.filter(f => !!~f.indexOf('.csv'));
//     console.info(`Found [${csvFiles.length}] CSV files`);

//     const allData = csvFiles.map(fileName => {
//         console.info(`[${new Date().toUTCString()}] Processing [${fileName}]`);
//         const fileString = fs.readFileSync(path.join(inDir, fileName), 'utf8');
//         const fileData = parseCSV(fileString);

//         processDataset(fileName, fileData);

//         return fileData;
//     });

//     const mergedData = mergeOn('batchReceived', ...allData);

//     processDataset('merged.csv', mergedData);
// });

fs.readdir(inDir, (err, folders) => {
    const threadFolders = folders.filter(a => !isNaN(Number(a)));
    console.info(`Found [${threadFolders.length}] thread groups`);

    Promise.all(
            threadFolders
            .map(threadFolderPath => {
                return new Promise((resolve, reject) => {
                    const currentFolderPath = path.join(inDir, threadFolderPath);
                    const threadFilePath = path.join(currentFolderPath, 'combined.csv');
                    const prevFile = path.join(currentFolderPath, 'prevRun.json');

                    try {
                        fs.statSync(prevFile);
                        const file = JSON.parse(fs.readFileSync(prevFile, 'utf8'));
                        resolve(file);
                        return;
                    } catch (err) {
                        //console.warn(err);
                    }

                    console.info(`[${new Date().toUTCString()}] Combining [${threadFolderPath}] threads`);

                    // require('child_process').execSync(`cd ${currentFolderPath} &&\
                    // rm combined.csv; \
                    // head -1 results-loadgen.0.csv > combined.csv && \
                    // for filename in $(ls results*.csv); do sed 1d $filename >> combined.csv; done`, {
                    //     stdio: [0, 1, 2]
                    // });

                    console.info(`[${new Date().toUTCString()}] Processing combined [${threadFolderPath}] threads`);

                    const messageThroughputXSecond = {};
                    const byteThroughputXSecond = {};
                    const latencyXSecond = {};
                    const producedXSecond = {};
                    const consumedXSecond = {};
                    const individualSeconds = [];


                    const readline = require('readline');
                    const instream = fs.createReadStream(threadFilePath, 'ascii');
                    const outstream = new(require('stream'))();
                    const lr = readline.createInterface(instream, outstream);

                    let headerRow = [];

                    let secondCount = 0;
                    lr.on('line', function (line) {
                        const lineParts = line.split(',');
                        if (headerRow.length === 0) {
                            lineParts.forEach(columnName => headerRow.push(columnName));
                            return;
                        }

                        const rowData = {};
                        for(let i = 0; i <= headerRow.length; i++) {
                            const key = headerRow[i];
                            rowData[key] = lineParts[i];
                        }

                        const {
                            batchReceived,
                            messageGenerated,
                            consumerLag,
                            messageId,
                            recordOffset,
                            messageSize
                        } = rowData;

                        const producedSecondTS = Math.floor(messageGenerated / 1000) * 1000;
                        producedXSecond[producedSecondTS] = producedXSecond[producedSecondTS] || 0;
                        producedXSecond[producedSecondTS]++;

                        secondCount++;
                        const secondTS = Math.floor(batchReceived / 1000) * 1000;
                        if (typeof messageThroughputXSecond[secondTS] === 'undefined') {
                            console.info(`[${new Date().toUTCString()}] Processing second [${secondTS}] (${Object.keys(messageThroughputXSecond).length})`);
                            individualSeconds.push(secondTS);
                            secondCount = 1;
                        }
                        messageThroughputXSecond[secondTS] = messageThroughputXSecond[secondTS] || 0;
                        messageThroughputXSecond[secondTS]++;

                        byteThroughputXSecond[secondTS] = byteThroughputXSecond[secondTS] || 0;
                        byteThroughputXSecond[secondTS] += messageSize;

                        consumedXSecond[secondTS] = consumedXSecond[secondTS] || 0;
                        consumedXSecond[secondTS]++;

                        latencyXSecond[secondTS] = latencyXSecond[secondTS] || 0;
                        latencyXSecond[secondTS] = ((latencyXSecond[secondTS] * secondCount - 1) + consumerLag) / secondCount;
                    });

                    lr.on('close', function (line) {
                        console.info(`[${new Date().toUTCString()}] Processed [${threadFilePath}]`);
                        let totalMessages = 0;
                        let totalBytes = 0;
                        let totalLatency = 0;
                        const numSeconds = individualSeconds.length;
                        const quaterIndex = Math.floor(numSeconds / 4);

                        console.log(`[${new Date().toUTCString()}] Got numSeconds [${numSeconds}] and quaterIndex [${quaterIndex}]`);

                        const interestingSeconds = individualSeconds
                            .sort()
                            .slice(quaterIndex, quaterIndex * 3);

                        console.log(`[${new Date().toUTCString()}] Got sorted seconds [${interestingSeconds.length}]`);

                        for (let second of interestingSeconds) {
                            totalMessages += messageThroughputXSecond[second];
                            totalBytes += byteThroughputXSecond[second];
                            totalLatency += latencyXSecond[second];
                        }

                        console.log(`[${new Date().toUTCString()}] Got total time [${totalMessages}]`);

                        const avgMsgThroughputPerSecond = totalMessages / interestingSeconds.length;
                        const avgByteThroughputPerSecond = totalBytes / interestingSeconds.length;
                        const avgLatencyXSecond = totalLatency / interestingSeconds.length;

                        console.log(`[${new Date().toUTCString()}] Got avg messages per second [${avgMsgThroughputPerSecond}]`);

                        const producedSeconds = Object.keys(producedXSecond)
                            .map(Number)
                            .sort();

                        const producedXSecondArr = [];
                        for (let secondTs of producedSeconds) {
                            producedXSecondArr.push([secondTs, producedXSecond[secondTs]]);
                        }

                        const consumedSeconds = Object.keys(consumedXSecond)
                            .map(Number)
                            .sort();

                        const consumedXSecondArr = [];
                        for (let secondTs of consumedSeconds) {
                            consumedXSecondArr.push([secondTs, consumedXSecond[secondTs]]);
                        }

                        const res = {
                            threads: Number(threadFolderPath),
                            avgMsgThroughputPerSecond: avgMsgThroughputPerSecond,
                            avgByteThroughputPerSecond: avgByteThroughputPerSecond,
                            avgLatencyXSecond: avgLatencyXSecond,
                            latencyXSecond: latencyXSecond,
                            producedXSecond: producedXSecondArr,
                            consumedXSecond: consumedXSecondArr
                        };

                        fs.writeFileSync(prevFile, JSON.stringify(res), 'utf8');
                        resolve(res);
                    });


                });
            })
        ).then((threadSamples) => {
            const outData = threadSamples.sort((a, b) => a.threads > b.threads ? 1 : -1);
            const outFile = path.join(outDir, 'throughput-and-threads.json');
            fs.writeFileSync(outFile, JSON.stringify(outData), 'utf8');
            console.info(`[${new Date().toUTCString()}] File outputted to [${outFile}]`);
        })
        .catch(err => console.error(err));

    //processDataset('merged.csv', mergedData);
});