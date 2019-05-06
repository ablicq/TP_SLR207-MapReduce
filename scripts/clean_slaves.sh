#!/bin/bash

# clean the slaves folders
# after a clean a deploy is necessary to be able to run again

task () {
    ssh -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" "$1" rm -rf /tmp/ablicq
}

for host in `cat slaves.conf`
do
	task $host &
done


wait
