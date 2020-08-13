#!/bin/bash

ROOT_DIR="$(dirname "$(readlink -f "$0")")"

cd $ROOT_DIR

echo "Building Jars ..."

./gradlew :api:shadowjar
./gradlew :shadowjar

echo "Done ..."
