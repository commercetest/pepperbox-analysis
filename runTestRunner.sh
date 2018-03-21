#!/bin/bash

node test-runner.js \
    --tps 10 \
    --threads 3 \
    --messageSize 10000 \
    --testLength 20 \
    --topicName joe-test \
    --producerHosts ec2-user@loadtest01.ohio \
    --consumerHosts ec2-user@loadtest02.ohio \
    --monitorHosts ec2-user@loadtest01.ohio,ec2-user@loadtest02.ohio
    
sleep 5;

node test-runner.js \
    --tps 50 \
    --threads 3 \
    --messageSize 10000 \
    --testLength 20 \
    --topicName joe-test \
    --producerHosts ec2-user@loadtest01.ohio \
    --consumerHosts ec2-user@loadtest02.ohio \
    --monitorHosts ec2-user@loadtest01.ohio,ec2-user@loadtest02.ohio

sleep 5;

node test-runner.js \
    --tps 100 \
    --threads 3 \
    --messageSize 10000 \
    --testLength 20 \
    --topicName joe-test \
    --producerHosts ec2-user@loadtest01.ohio \
    --consumerHosts ec2-user@loadtest02.ohio \
    --monitorHosts ec2-user@loadtest01.ohio,ec2-user@loadtest02.ohio

sleep 5;

node test-runner.js \
    --tps 500 \
    --threads 3 \
    --messageSize 10000 \
    --testLength 20 \
    --topicName joe-test \
    --producerHosts ec2-user@loadtest01.ohio \
    --consumerHosts ec2-user@loadtest02.ohio \
    --monitorHosts ec2-user@loadtest01.ohio,ec2-user@loadtest02.ohio

echo "Finished running all tests"