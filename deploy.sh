#!/bin/bash

for host in `cat slaves.conf`
do
    ssh -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" "ablicq@$host" mkdir -p "/tmp/ablicq"
    scp /tmp/ablicq/slave.jar "ablicq@$host:/tmp/ablicq/slave.jar" 
done
