

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
        .reduce(flatten)
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

fs.readdir(inDir, (err, files) => {
    const csvFiles = files.filter(f => !!~f.indexOf('.csv'));
    console.info(`Found [${csvFiles.length}] CSV files`);

    const allData = csvFiles.map(fileName => {
        console.info(`[${new Date().toUTCString()}] Processing [${fileName}]`);
        const fileString = fs.readFileSync(path.join(inDir, fileName), 'utf8');
        const fileData = parseCSV(fileString);

        processDataset(fileName, fileData);

        return fileData;
    });

    const mergedData = mergeOn('batchReceived', ...allData);

    processDataset('merged.csv', mergedData);
});