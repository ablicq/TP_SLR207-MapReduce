#!/bin/bash

task () {
    ssh -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" "$1" mkdir -p "/tmp/ablicq"
    scp /tmp/ablicq/slave.jar "$1:/tmp/ablicq/slave.jar" 
}

for host in `cat slaves.conf`
do
	task $host &
done

wait
