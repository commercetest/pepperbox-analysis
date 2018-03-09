const fs = require('fs');
const inData = require('./out/throughput-and-threads.json');

inData.forEach(thread => {
    const outFile = `./out/${thread.threads}-consumed.csv`;
    let data = 'timestamp,msgConsumedPerSecond';
    for(let [timestamp, consumed] of thread.consumedXSecond) {
        data += `\n${timestamp},${consumed}`;
    }
    fs.writeFileSync(outFile, data, 'utf8');
    console.info(`Witten data to [${outFile}]`);
});