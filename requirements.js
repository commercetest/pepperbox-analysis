module.exports = {
    consumer: {
        "java -version 2>&1 | head -n 1 | grep -E -o '[0-9]+\.[0-9]+\.[0-9]+'": "1.8.x"
    },
    producer: {
        "java -version 2>&1 | head -n 1 | grep -E -o '[0-9]+\.[0-9]+\.[0-9]+'": "1.8.x"
    },
    monitor: {
        "iostat -V | grep -E -o '[0-9]+\.[0-9]+\.[0-9]+'": ">=11.7.3"
    }
};