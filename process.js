const LineByLineReader = require('line-by-line');

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

                    console.info(`[${new Date().toUTCString()}] Combining [${threadFolderPath}] threads`);

                    require('child_process').execSync(`cd ${currentFolderPath} &&\
                    rm ${threadFilePath}; \
                    head -1 results-loadgen.0.csv > ${threadFilePath} && \
                    for filename in $(ls results*.csv); do sed 1d $filename >> ${threadFilePath}; done`, {
                        stdio: [0, 1, 2]
                    });

                    console.info(`[${new Date().toUTCString()}] Processing combined [${threadFolderPath}] threads`);

                    const lr = new LineByLineReader(threadFilePath, {
                        encoding: 'ascii',
                    });

                    const throughputXSecond = {};
                    const individualSeconds = [];

                    lr.on('line', (line) => { //TODO: ignore first and last 25%
                        const [batchReceived, messageGenerated, consumerLag, messageId, recordOffset, messageSize] = line.split(',').map(Number);
                        if (isNaN(batchReceived)) {
                            return;
                        }
                        const secondTS = Math.floor(batchReceived / 1000) * 1000;
                        if (typeof throughputXSecond[secondTS] === 'undefined') {
                            console.info(`[${new Date().toUTCString()}] Processing second [${secondTS}] (${Object.keys(throughputXSecond).length})`);
                            individualSeconds.push(secondTS);
                        }
                        throughputXSecond[secondTS] = throughputXSecond[secondTS] || 0;
                        throughputXSecond[secondTS]++;
                    });

                    lr.on('end', () => {
                        console.info(`[${new Date().toUTCString()}] Processed [${threadFilePath}]`);
                        let total = 0;
                        const numSeconds = individualSeconds.length;
                        const quaterIndex = Math.floor(numSeconds / 4);

                        const interestingSeconds = individualSeconds
                            .sort()
                            .slice(quaterIndex, quaterIndex * 3);

                        for (let second of interestingSeconds) {
                            total += throughputXSecond[second];
                        }

                        const avgThroughputPerSecond = total / interestingSeconds.length;

                        resolve({
                            threads: Number(threadFolderPath),
                            avgThroughputPerSecond: avgThroughputPerSecond,
                        });
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