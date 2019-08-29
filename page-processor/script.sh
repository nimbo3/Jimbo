#!/usr/bin/env bash
mvn clean package -DskipTests
echo "Sending to the first server"
ssh -p "$2" "$1" 'rm -rf ~/page-processor'
ssh -p "$2" "$1" 'mkdir ~/page-processor'
ssh -p "$2" "$1" 'mkdir ~/page-processor/config'
scp -P "$2" target/page-processor*.jar $1:~/page-processor
scp -P "$2" src/main/resources/* $1:~/page-processor/config
echo "echo 'java -javaagent:\$HOME/prometheus_jmx/jmx_prometheus_javaagent-0.12.0.jar=8765:\$HOME/prometheus_jmx/config.yaml -jar page-processor*.jar' > ~/page-processor/run.sh;" | ssh -p "$2" "$1"
ssh -p "$2" "$1" 'chmod 775 ~/page-processor/run.sh'
echo "Sending to other servers from the first one"
while read -a host
do
    echo "ssh -p ${host[1]} ${host[0]} 'rm -rf ~/page-processor'" | ssh -p "$2" "$1"
    echo "scp -P ${host[1]} -r ~/page-processor ${host[0]}:~/" | ssh -p "$2" "$1"
done < "$3"
echo "Done sending to all servers"
mvn clean