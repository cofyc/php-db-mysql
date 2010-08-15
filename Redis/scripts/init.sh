#!/bin/bash

for file in $(find . -name '*.conf')
do
    /opt/local/bin/redis-server $file
done
