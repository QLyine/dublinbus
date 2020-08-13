#!/bin/bash

ROOT_DIR="$(dirname "$(readlink -f "$0")")"

docker run -d --rm --name aerospike -p 3000:3000 -p 3001:3001 -p 3002:3002 -p 3003:3003 aerospike:4.8.0.14

