#!/usr/bin/env bash
mvn clean package -DskipTests
echo "Sending to the first server"
ssh -p $2 $1 'rm -rf ~/es-page-processor'
ssh -p $2 $1 'mkdir ~/es-page-processor'
ssh -p $2 $1 'mkdir ~/es-page-processor/config'
scp -P $2 target/es-page-processor*.jar $1:~/es-page-processor
scp -P $2 src/main/resources/* $1:~/es-page-processor/config
echo "echo 'java -javaagent:\$HOME/prometheus_jmx/jmx_prometheus_javaagent-0.12.0.jar=7889:\$HOME/prometheus_jmx/config.yaml -jar es-page-processor*.jar' > ~/es-page-processor/run.sh;" | ssh -p $2 $1
ssh -p $2 $1 'chmod 775 ~/es-page-processor/run.sh'
echo "Sending to other servers from the first one"
while read -a host
do
    echo "ssh -p ${host[1]} ${host[0]} 'rm -rf ~/es-page-processor'" | ssh -p $2 $1
    echo "scp -P ${host[1]} -r ~/es-page-processor ${host[0]}:~/" | ssh -p $2 $1
done < $3
echo "Done sending to all servers"
mvn clean