const fs = require('fs');
const inData = require('./out/throughput-and-threads.json');

inData.forEach(thread => {
    const outFile = `./out/${thread.threads}-produced.csv`;
    let data = 'timestamp,msgProducedPerSecond';
    for(let [timestamp, produced] of thread.producedXSecond) {
        data += `\n${timestamp},${produced}`;
    }
    fs.writeFileSync(outFile, data, 'utf8');
    console.info(`Witten data to [${outFile}]`);
});