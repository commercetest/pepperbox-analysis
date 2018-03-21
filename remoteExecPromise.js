const remoteExec = require('ssh-exec');
module.exports = function remoteExecPromise(...args) {
    return new Promise((resolve, reject) => {
        remoteExec(args[0], args[1], function (err, stdout, stderr) {
            if (!err) {
                //console.info(`Got response from [${args[1]}]:`, stdout);
                resolve(stdout);
            } else {
                console.error(`Connection to [${args[1]}] failed`, err, '\nSTDERR:\n', stderr, '\n\nSTDOUT:\n', stdout, '\n\nCMD:\n', args[0]);
                reject(stderr);
            }
        });
    });
};