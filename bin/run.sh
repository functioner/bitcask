#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

java -jar $SCRIPT_DIR/../app/target/app-0.1-SNAPSHOT-jar-with-dependencies.jar $@
