const remoteExecPromise = require('./remoteExecPromise.js');
const semver = require('semver');

module.exports = function checkHosts(hosts, requirementObject) {
    return Promise.all(
        hosts.map((sshCommand) => {
            const requirements = Object.keys(requirementObject);
            return Promise.all( //TODO: don't ssh for each requiremnt
                requirements.map(async (req) => {
                    return remoteExecPromise(req, sshCommand)
                        .catch((err) => {
                            return {
                                error: true,
                                err
                            };
                        })
                        .then((res) => {
                            return new Promise((resolve, reject) => {
                                if (!res.error && semver.satisfies(res.trim(), requirementObject[req])) {
                                    resolve();
                                    console.log(`Requirement [${req}] met on [${sshCommand}]`);
                                } else {
                                    reject(`Requirement [${req}] is not satisfied on host [${sshCommand}]. Expected [${requirementObject[req]}] but got: \n[${res.err || res}]`);
                                }
                            });
                        })
                })
            );
        })
    );
};