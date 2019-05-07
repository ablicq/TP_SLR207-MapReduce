#!/bin/bash

# clean the slaves folders
# after a clean a deploy is necessary to be able to run again

task () {
    ssh -o "UserKnownHostsFile=/dev/null" -o "StrictHostKeyChecking=no" "$1" rm -rf /tmp/ablicq
}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

for host in `cat $DIR/../slaves.conf`
do
	task $host &
done


wait
