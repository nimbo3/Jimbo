#!/bin/bash
EXIT="exit"
while true; do
    echo "which servers:(seperate with ',') :";
    read SERVERS_STRING;
    if [ "$SERVERS_STRING" == "$EXIT" ]; then
        exit 0;
    fi
    IFS=',' read -ra servers <<< "$SERVERS_STRING";
    echo "which application(crawler, es-pp or hbase-pp) :";
    read application;
    if [ "$application" == "$EXIT" ]; then 
        exit 0;
    fi
    for i in "${servers[@]}"; do
        ssh -p 3031 jimbo@$i './'$application'/run.sh';
    done
done