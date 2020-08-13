#!/bin/bash

if [[ -z "$1" ]] ; then
	echo "Please specify a file example: ./$0 /file/location/siri.20121106.csv"
	exit 1
fi


ROOT_DIR="$(dirname "$(readlink -f "$0")")"

cd $ROOT_DIR

java -jar build/libs/tblxchallenge-1.0-SNAPSHOT-all.jar $1


