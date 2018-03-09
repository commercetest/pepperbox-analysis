const moment = require('moment');

const fs = require('fs');
const path = require('path');
const inDir = path.join(__dirname, './in/');
const outDir = path.join(__dirname, './out/');

console.info(`Reading all .log files from [${inDir}]`);

fs.readdir(inDir, (err, folders) => {
    const threadFolders = folders.filter(a => !isNaN(Number(a)));
    console.info(`Found [${threadFolders.length}] thread groups`);

    Promise.all(
            threadFolders
            .map(threadFolderPath => {
                return new Promise((resolve, reject) => {
                    const currentFolderPath = path.join(inDir, threadFolderPath);
                    const ioStatLogPath = path.join(currentFolderPath, 'iostat.log');
                    console.info(`Reading IOStat logfile [${ioStatLogPath}]`);
                    const ioStatLog = fs.readFileSync(ioStatLogPath, 'utf8'); // TODO: stream



                    const runs = ioStatLog.match(/[0-9]+\/[0-9]+\/[0-9]+ [0-9]+:[0-9]+:[0-9]+/g)
                        .map((date) => {
                            return date + ' ' + ioStatLog.split(date)[1].split(/[0-9]+\/[0-9]+\/[0-9]+ [0-9]+:[0-9]+:[0-9]+/)[0].trim()
                        });

                    let outData = '';

                    runs.forEach(run => {
                        const date = moment.utc(run.split('\n')[0], 'DD/MM/YYYY HH:mm:ss A');
                        console.log(run);
                        const deviceRows = run.split(/Device: [^\n]*/)[1].trim().split('\n');
                        let devices = [];
                        deviceRows.forEach(device => {
                            const [deviceName, tps, mb_read, mb_wrtn] = device.split(/\s/g).filter(a => a);

                            devices.push({
                                deviceName,
                                tps
                            });
                        });

                        if (!outData) {
                            outData = `timestamp,idleCpu`;
                            devices.forEach(({
                                deviceName
                            }) => {
                                outData += `,${deviceName}Tps`;
                            });
                        }

                        const idleCpu = run.split('\n')[2].trim().split(' ').slice(-1)[0].trim();

                        outData += `\n${date.valueOf()},${idleCpu}`;

                        devices.forEach(({
                            deviceName,
                            tps,
                            cpu
                        }) => {
                            outData += `,${tps}`;
                        });
                    });

                    const threadNum = Number(threadFolderPath.split('/').slice(-1)[0]);
                    const outFile = path.resolve(outDir, `${threadNum}-iostat.csv`);
                    console.info(`Outputting to file [${outFile}]`);
                    fs.writeFileSync(outFile, outData, 'utf8');
                });
            })
        )
        .catch(err => console.error(err));
});