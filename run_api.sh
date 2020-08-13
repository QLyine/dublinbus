#!/bin/bash

ROOT_DIR="$(dirname "$(readlink -f "$0")")"

cd $ROOT_DIR

java -jar api/build/libs/api-1.0-SNAPSHOT-all.jar


