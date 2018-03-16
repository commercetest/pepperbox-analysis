const fs = require('fs');
const path = require('path');
const readline = require('readline');
const exec = require('sync-exec');

const inDir = path.resolve(__dirname, 'in');

const testRuns = fs.readdirSync(inDir)
    .filter(fn => fn[0] !== '.' && fn[0] !== '_');

testRuns.forEach((testRun) => {
    const inDir = path.resolve(__dirname, 'in', testRun);
    console.log(`Reading files from [${inDir}]`);

    const outDir = path.resolve(__dirname, 'out', testRun);
    console.log(`Writing files to [${outDir}]`);

    try {
        fs.statSync(outDir);
    } catch (err) {
        fs.mkdirSync(outDir);
    }

    const files = fs.readdirSync(inDir);

    const tests = files
        .filter((fileName) => !!~fileName.indexOf('results-') && !!~fileName.indexOf('.csv'))
        .map((fileName) => {
            const testId = fileName.split('mps.')[1].split('.')[0]; //TODO: fix later
            return Number(testId);
        })
        .sort()
        .filter((val, index, arr) => val !== arr[index - 1]);

    console.log(`Found [${tests.length}] test runs`);

    console.log(`Generating combined CSV files`);
    for (let testId of tests) {
        const firstFile = path.resolve(inDir, files.find(fn => !!~fn.indexOf(`.${testId}.`) && !!~fn.indexOf(`.csv`)));
        const outFile = path.resolve(outDir, `results-mps.${testId}.combined.csv`);
        console.info(`Generating [${outFile}]`);
        exec(`head -1 ${firstFile} > ${outFile}`);
        const targetFiles = path.resolve(inDir, `results-*mps.${testId}.*.csv`);
        exec(`awk -F "," '/[0-9]+/ {print }' ${targetFiles} | sort -k2 -n -t "," >> ${outFile}`);
    }

    const combinedFiles = fs.readdirSync(outDir)
        .filter(fn => !!~fn.indexOf('.combined.csv'))
        .sort((a, b) => {
            const [_a, ai] = a.split('.');
            const [_b, bi] = b.split('.');
            return Number(ai) > Number(bi) ? 1 : -1;
        });
    console.info(`Got [${combinedFiles.length}] *.combined.csv files`);

    (async function processCombinedFiles(combinedFiles) {
        const combinedConsumedMessagesPerSecond = [];
        const combinedProducedMessagesPerSecond = [];
        const combinedByesPerSecond = [];
        const combinedSecondTimestamp = [];
        const combinedTestId = [];
        const combinedLatencyPerSecond = [];
        const combinedDistanceFromFirstSecond = [];

        for (let fileName of combinedFiles) {
            const [_, testId] = fileName.split('.');
            const inFile = path.resolve(outDir, fileName);
            const outFile = path.resolve(outDir, `sampled-results-mps.${testId}.csv`);
            console.info(`Processing [${inFile}] and outputting to [${outFile}]`);

            const messageConsumedThroughputXSecond = {};
            const byteConsumedThroughputXSecond = {};
            const latencyXSecond = {};
            const producedXSecond = {};
            const messageProducedThroughputXSecond = {};
            const individualSeconds = [];
            let headerRow = [];
            let secondCount = 0;

            await readFileByLine(inFile, function (line) {
                const lineParts = line.split(',');
                if (headerRow.length === 0) {
                    lineParts.forEach(columnName => headerRow.push(columnName));
                    return;
                }

                const rowData = {};
                for (let i = 0; i <= headerRow.length; i++) {
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
                if (typeof messageConsumedThroughputXSecond[secondTS] === 'undefined') {
                    console.info(`[${new Date().toUTCString()}] Processing second [${secondTS}] (${Object.keys(messageConsumedThroughputXSecond).length})`);
                    individualSeconds.push(secondTS);
                    secondCount = 1;
                }

                messageConsumedThroughputXSecond[secondTS] = messageConsumedThroughputXSecond[secondTS] || 0;
                messageConsumedThroughputXSecond[secondTS]++;

                byteConsumedThroughputXSecond[secondTS] = byteConsumedThroughputXSecond[secondTS] || 0;
                byteConsumedThroughputXSecond[secondTS] += Number(messageSize);

                latencyXSecond[secondTS] = latencyXSecond[secondTS] || 0;
                //https://www.bennadel.com/blog/1627-create-a-running-average-without-storing-individual-values.htm
                latencyXSecond[secondTS] = ((latencyXSecond[secondTS] * secondCount - 1) + Number(consumerLag)) / secondCount;
            });

            const writeStream = fs.createWriteStream(outFile);

            const outHeader = `timestamp,messagesProduced,messagesConsumed,bytesConsumed,avgLatency`;
            writeStream.write(outHeader + '\n');

            const numSeconds = individualSeconds.length;
            const quaterIndex = Math.floor(numSeconds / 4);

            const interestingSeconds = individualSeconds
                .sort();
            //.slice(quaterIndex, quaterIndex * 3);

            const firstSecond = interestingSeconds[0];

            for (let second of interestingSeconds) {
                console.log(`[${new Date().toUTCString()}] Processing second [${second}]`);
                const messagesConsumed = messageConsumedThroughputXSecond[second] || 0;
                const messagesProduced = producedXSecond[second] || 0;
                const bytesConsumed = byteConsumedThroughputXSecond[second] || 0;
                const avgLatency = Math.floor(latencyXSecond[second] / 1000) * 1000;

                const row = `${second},${messagesProduced},${messagesConsumed},${bytesConsumed},${avgLatency}`;
                writeStream.write(row + '\n');

                combinedDistanceFromFirstSecond.push((second - firstSecond) / 1000)
                combinedConsumedMessagesPerSecond.push(messagesConsumed);
                combinedProducedMessagesPerSecond.push(messagesProduced);
                combinedByesPerSecond.push(bytesConsumed);
                combinedSecondTimestamp.push(second);
                combinedTestId.push(testId);
                combinedLatencyPerSecond.push(avgLatency);
            }
        }

        const combinedCSV = `mps,secondTs,relativeTime,messagesConsumed,messagesProduced,bytes,avgLatency\n` +
            combinedSecondTimestamp.map((second, index) => {
                const testId = combinedTestId[index];
                const messagesConsumed = combinedConsumedMessagesPerSecond[index];
                const messagesProduced = combinedProducedMessagesPerSecond[index];
                const bytes = combinedByesPerSecond[index];
                const latency = combinedLatencyPerSecond[index];
                const distanceFromFirstSecond = combinedDistanceFromFirstSecond[index];
                return `${testId},${second},${distanceFromFirstSecond},${messagesConsumed},${messagesProduced},${bytes},${latency}`
            }).join('\n');

        fs.writeFileSync(
            path.resolve(outDir, `sampled-results.combined.csv`),
            combinedCSV,
            'utf8'
        );

        console.info(`[${new Date().toUTCString()}] Finished processing files`);
    })(combinedFiles);

    const iostatFiles = files.filter(fn => !!~fn.indexOf('iostat.mps.json'));
    console.info(`[${new Date().toUTCString()}] Beginning to process [${iostatFiles.length}] iostat JSON logs`);
    iostatFiles.forEach(fileName => {
        console.info(`Loading file [${fileName}]`);
        const iostatLogs = require(path.resolve(inDir, fileName));
        const host = iostatLogs.sysstat.hosts[0];
        const stats = host.statistics;
        const disks = stats[0].disk.map(a => a.disk_device).sort((a, b) => a > b ? 1 : -1);
        const outCSV = `timestamp,cpuUsage,${disks.map(d => d+'Tps').join(',')}\n` +
            stats.map(stat => {
                const ts = (new Date(stat.timestamp)).valueOf();
                const cpuUsage = Math.floor((100 - stat['avg-cpu'].idle) * 1000) / 1000;
                const diskTps = stat.disk.sort((a, b) => {
                        return a.disk_device > b.disk_device ? 1 : -1;
                    })
                    .map(d => Math.floor(d.tps * 1000) / 1000);
                return `${ts},${cpuUsage},${diskTps.join(',')}`
            }).join('\n');

        const iostatLogOutFile = path.resolve(outDir, `${host.nodename}-${host.date.replace(/\//g, '-')}.csv`);
        console.info(`Writing file to [${iostatLogOutFile}]`);
        fs.writeFileSync(iostatLogOutFile, outCSV, 'utf8');
    });

});

function readFileByLine(filePath, lineHandler) {
    const instream = fs.createReadStream(filePath, 'ascii');
    const outstream = new(require('stream'))();
    const lr = readline.createInterface(instream, outstream);

    return new Promise((resolve, reject) => {
        lr.on('line', lineHandler);
        lr.on('close', resolve);
    });
}