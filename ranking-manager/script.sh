#!/usr/bin/env bash
mvn clean package -DskipTests
echo "Sending to the first server"
ssh -p $2 $1 'rm -rf ~/ranking-manager'
ssh -p $2 $1 'mkdir ~/ranking-manager'
ssh -p $2 $1 'mkdir ~/ranking-manager/config'
scp -P $2 target/ranking-manager*.jar $1:~/ranking-manager
scp -P $2 src/main/resources/* $1:~/ranking-manager/config
echo "echo 'java -javaagent:\$HOME/prometheus_jmx/jmx_prometheus_javaagent-0.12.0.jar=7777:\$HOME/prometheus_jmx/config.yaml -jar ranking-manager*.jar' > ~/ranking-manager/ranking-manager.sh;" | ssh -p $2 $1
ssh -p $2 $1 'chmod 775 ~/ranking-manager/ranking-manager.sh'
echo "Sending to other servers from the first one"
while read -a host
do
    echo "ssh -p ${host[1]} ${host[0]} 'rm -rf ~/ranking-manager'" | ssh -p $2 $1
    echo "scp -P ${host[1]} -r ~/ranking-manager ${host[0]}:~/" | ssh -p $2 $1
done < $3
echo "Done sending to all servers"
mvn clean