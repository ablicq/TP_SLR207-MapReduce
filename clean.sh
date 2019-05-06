#!/bin/bash

task () {
    ssh -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" "$1" rm -rf /tmp/ablicq
}

for host in `cat slaves.conf`
do
	task $host &
done

rm -rf /tmp/ablicq

wait
