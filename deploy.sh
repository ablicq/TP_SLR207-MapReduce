#!/bin/bash

for host in `cat slaves.conf`
do
    ssh -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" "$host" mkdir -p "/tmp/ablicq"
    scp /tmp/ablicq/slave.jar "$host:/tmp/ablicq/slave.jar" 
done
