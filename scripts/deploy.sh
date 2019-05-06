#!/bin/bash

# Deploy slave.jar to the list of slaves specified in the slaves.conf file

mkdir -p /tmp/ablicq # create the master folder

task () {
    ssh -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" "$1" mkdir -p "/tmp/ablicq"
    scp ../jar/slave.jar "$1:/tmp/ablicq/slave.jar" 
}

for host in `cat ../slaves.conf`
do
	task $host &
done

wait
