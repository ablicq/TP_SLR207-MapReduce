#!/bin/bash

if [ $# -ne 2 ]
then
	echo usage: $0 inFile splitSize
	exit 1
fi

. ./clean_master.sh
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
java -jar $DIR/../jar/master.jar $1 $2
