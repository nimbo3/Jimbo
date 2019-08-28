#!/usr/bin/env bash
mvn clean package -DskipTests
echo "Sending to the first server"
ssh -p $2 $1 'rm -rf ~/crawler'
ssh -p $2 $1 'mkdir ~/crawler'
ssh -p $2 $1 'mkdir ~/crawler/config'
scp -P $2 target/crawler*.jar $1:~/crawler
scp -P $2 src/main/resources/* $1:~/crawler/config
echo "echo 'java -javaagent:\$HOME/prometheus_jmx/jmx_prometheus_javaagent-0.12.0.jar=5678:\$HOME/prometheus_jmx/config.yaml -jar crawler*.jar' > ~/crawler/run.sh;" | ssh -p $2 $1
ssh -p $2 $1 'chmod 775 ~/crawler/run.sh'
echo "Sending to other servers from the first one"
while read -a host
do
    echo "ssh -p ${host[1]} ${host[0]} 'rm -rf ~/crawler'" | ssh -p $2 $1
    echo "scp -P ${host[1]} -r ~/crawler ${host[0]}:~/" | ssh -p $2 $1
done < $3
echo "Done sending to all servers"
mvn clean