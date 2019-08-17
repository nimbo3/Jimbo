#!/usr/bin/env bash
read -a master < $1
HOST=${master[0]}
PORT=${master[1]}
ssh -p ${PORT} ${HOST} 'start-all.sh'
while read -a slave
do
    echo "ssh -p ${slave[1]} ${slave[0]} '~/zookeeper*/bin/zkServer.sh start'" | ssh -p ${PORT} ${HOST}
done < $2
ssh -p ${PORT} ${HOST} "~/kafka_2*/bin/kafka-server-start.sh ~/kafka_2*/config/server.properties --daemon"
while read -a slave
do
    echo "ssh -p ${slave[1]} ${slave[0]} '~/kafka_2*/bin/kafka-server-start.sh ~/kafka_2*/config/server.properties --daemon'" | ssh -p ${PORT} ${HOST}
done < $2
ssh -p ${PORT} ${HOST} 'start-hbase.sh'
ssh ${HOST} -p ${PORT} 'cd redis-5.0.5; screen -dmS redis-server src/redis-server redis.conf;';
while read -a slave
do
    ssh ${slave[0]} -p ${slave[1]} 'cd redis-5.0.5; screen -dmS redis-server src/redis-server redis.conf;';
done < $2
ssh ${HOST} -p ${PORT} 'screen -dmS elasticsearch ~/clean_elastic/*/bin/elasticsearch'
while read -a slave
do
    ssh ${slave[0]} -p ${slave[1]} 'screen -dmS elasticsearch ~/clean_elastic/*/bin/elasticsearch'
done < $2
