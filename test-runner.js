const log = (...args) => console.info(`[${new Date().toUTCString()}]`, ...args);
const checkHosts = require('./checkHosts.js');
const requirements = require('./requirements.js');
const sleep = require('./sleep.js');
const remoteExecPromise = require('./remoteExecPromise.js');

const exec = require('node-exec-promise').exec;
const moment = require('moment');
const argv = require('yargs')
    .options({
        topicName: {
            demandOption: true,
            default: 'pepperbox-test',
            describe: 'Name of topics to be used (will have ".n" appended)'
        },
        testLength: {
            demandOption: true,
            default: 20,
            describe: 'Number of seconds to run the test for'
        },
        tps: {
            demandOption: true,
            default: 10,
            describe: 'Number of messages per second (throughput per second)'
        },
        threads: {
            demandOption: true,
            default: 3,
            describe: 'Number of consumers and producer threads to run on each host'
        },
        messageSize: {
            demandOption: true,
            default: 10000,
            describe: 'Message Body Size'
        },
        producerHosts: {
            demandOption: true,
            default: 'localhost:22',
            describe: 'Comma-seperated list of SSH user:pass@host:port',
            coerce: (hosts) => hosts.split(',')
        },
        consumerHosts: {
            demandOption: true,
            default: 'localhost:22',
            describe: 'Comma-seperated list of SSH user:pass@host:port',
            coerce: (hosts) => hosts.split(',')
        },
        monitorHosts: {
            demandOption: true,
            default: 'localhost:22',
            describe: 'Comma-seperated list of SSH user:pass@host:port',
            coerce: (hosts) => hosts.split(',')
        },
        topicPrefix: {
            default: '',
            describe: 'Comma-seperated list of topicPrefix@host',
            coerce: (hosts) => {
                return hosts.split(',')
                    .reduce((acc, hostTopicPair) => {
                        const [topicPrefix, host] = hostTopicPair.split('@');
                        acc[host] = topicPrefix;
                        return acc;
                    }, {});
            }
        }
    })
    .help()
    .argv;

const testStartDate = moment.utc();

(async function () {
    log(`Starting Test [${testStartDate.format()}]`);

    log(`Verifying server requirements...`);
    Promise.all([
            checkHosts(argv.consumerHosts, requirements.consumer),
            checkHosts(argv.producerHosts, requirements.producer),
            checkHosts(argv.monitorHosts, requirements.monitor)
        ])
        .then(async () => {
            try {
                log(`Servers Look Okay`);
                log(`Starting to monitor`);

                const monitors = startMonitors(argv);

                log(`Sleeping for 5 seconds`);

                await sleep(5000);

                log(`Starting Consumers`);

                await prepConsumers(argv);
                log(`Prepped Consumers`);

                const consumers = startConsumers(argv);

                log(`Sleeping for 5 seconds`);

                await sleep(5000);

                log(`Starting Producers`);

                await prepProducers(argv);
                log(`Prepped Producers`);

                const producers = startProducers(argv);

                await Promise.all([
                    monitors,
                    consumers,
                    producers
                ]);

                log(`Monitors, Consumers, and Producers have completed`);

                log(`RSyncing files to local directory (./in/)`);
                await exec(`[ -d ./in ] || mkdir -p ./in`);
                await Promise.all([
                    ...argv.monitorHosts.map(host => syncData(host, '~/pepper-box/results/', './in/')),
                    ...argv.consumerHosts.map(host => syncData(host, '~/pepper-box/results/', './in/')),
                    ...argv.producerHosts.map(host => syncData(host, '~/pepper-box/results/', './in/'))
                ]);

                log(`Done Syncing`);
            } catch (err) {
                console.error(`Failed to run the test:`, err);
            }

        })
        .catch((err) => {
            console.error(`Failed to verify server requirements:`, err);
        });
})();

function startMonitors(argv) {
    return Promise.all(
        argv.monitorHosts.map((sshHost) => {
            const dirName = `$HOME/pepper-box/results/tps=${argv.tps}-threads=${argv.threads}-duration=${argv.testLength}-topicname=${argv.topicName}/${moment(testStartDate).format('DD-MM-YY_HH-mm__UTC')}`;
            return remoteExecPromise(
                `
                    [ -d ${dirName} ] || mkdir -p ${dirName};
                    iostat -t -o JSON 1 ${argv.testLength + 15} > ${dirName}/iostat-\`hostname\`.json;
                `,
                sshHost);
        })
    );
}

function startProducers(argv) {
    return Promise.all(
        argv.producerHosts.map((sshHost) => {
            const hostName = sshHost.replace(/.*@/g, '').replace(/:.*/g, '');
            const topicPrefix = argv.topicPrefix[hostName] || '';
            const dirName = `$HOME/pepper-box/results/tps=${argv.tps}-threads=${argv.threads}-duration=${argv.testLength}-topicname=${argv.topicName}/${moment(testStartDate).format('DD-MM-YY_HH-mm__UTC')}`;
            return remoteExecPromise(
                `
                cd $HOME/pepper-box;
                cp ./pblg.properties "${dirName}/pblg.$(hostname).properties";
                cd ${dirName} &&
                java -cp $HOME/pepper-box/target/pepper-box-1.0.jar:.  com.gslab.pepper.PepperBoxLoadGenerator \
                    --schema-file $HOME/pepper-box/schema${argv.messageSize}.txt \
                    --producer-config-file pblg.\`hostname\`.properties \
                    --topic-name ${topicPrefix}${argv.topicName}.${argv.tps} \
                    --per-thread-topics YES \
                    --throughput-per-producer ${argv.tps} \
                    --test-duration ${argv.testLength} \
                    --num-producers ${argv.threads}  \
                    --starting-offset 0 &> produce_mps_at.${argv.tps}.\`hostname\`.log;
                `,
                sshHost
            );
        })
    );
}

function prepConsumers(argv) {
    return Promise.all(
        argv.consumerHosts.map((sshHost) => {
            const dirName = `$HOME/pepper-box/results/tps=${argv.tps}-threads=${argv.threads}-duration=${argv.testLength}-topicname=${argv.topicName}/${moment(testStartDate).format('DD-MM-YY_HH-mm__UTC')}`;
            return remoteExecPromise(
                `
                [ -d ${dirName} ] || mkdir -p ${dirName};
                `,
                sshHost
            );
        })
    );
}

function prepProducers(argv) {
    return Promise.all(
        argv.producerHosts.map((sshHost) => {
            const dirName = `$HOME/pepper-box/results/tps=${argv.tps}-threads=${argv.threads}-duration=${argv.testLength}-topicname=${argv.topicName}/${moment(testStartDate).format('DD-MM-YY_HH-mm__UTC')}`;
            return remoteExecPromise(
                `
                [ -d ${dirName} ] || mkdir -p ${dirName};
                `,
                sshHost
            );
        })
    );
}

function startConsumers(argv) {
    return Promise.all(
        argv.consumerHosts.map((sshHost) => {
            const hostName = sshHost.replace(/.*@/g, '').replace(/:.*/g, '');
            const topicPrefix = argv.topicPrefix[hostName] || '';
            const dirName = `$HOME/pepper-box/results/tps=${argv.tps}-threads=${argv.threads}-duration=${argv.testLength}-topicname=${argv.topicName}/${moment(testStartDate).format('DD-MM-YY_HH-mm__UTC')}`;
            return remoteExecPromise(
                `
                cd $HOME/pepper-box;
                cp ./pblg.properties "${dirName}/pblg.$(hostname).properties";
                cd ${dirName} &&
                java -cp $HOME/pepper-box/target/pepper-box-1.0.jar:. com.gslab.pepper.PepperBoxLoadConsumer \
                    --consumer-config-file pblg.\`hostname\`.properties \
                    --num-consumers ${argv.threads}  \
                    --topic-name ${topicPrefix}${argv.topicName}.${argv.tps} \
                    --per-thread-topics YES \
                    --test-duration ${argv.testLength * 3} \
                    --throughput-per-consumer ${argv.tps} \
                    --starting-offset 0 &> consume_mps_at.${argv.tps}.\`hostname\`.log &&
                    for file in  ./results-*.csv
                    do
                        mv -i "\${file}" "\${file/\].csv/].\$(hostname).csv}"
                    done
                `,
                sshHost
            );
        })
    );
}

function syncData(host, fromDir, toDir) {
    const strippedPort = host.split(':')[0]; //TODO: improve
    return exec(`rsync -avz ${strippedPort}:${fromDir} ${toDir}`);
}
