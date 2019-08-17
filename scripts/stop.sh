#!/usr/bin/env bash
read -a master < $1
HOST=${master[0]}
PORT=${master[1]}
ssh ${HOST} -p ${PORT} 'screen -S redis-server -X quit'
while read -a slave
do
    ssh ${slave[0]} -p ${slave[1]} 'screen -S redis-server -X quit';
done < $2

ssh ${HOST} -p ${PORT} 'screen -S elasticsearch -X quit'
while read -a slave
do
    ssh ${slave[0]} -p ${slave[1]} 'screen -S elasticsearch -X quit'
done < $2

ssh -p ${PORT} ${HOST} "~/kafka_2*/bin/kafka-server-start.sh ~/kafka_2*/config/server.properties --daemon"
while read -a slave
do
    echo "ssh -p ${slave[1]} ${slave[0]} '~/kafka_2*/bin/kafka-server-stop.sh'" | ssh -p ${PORT} ${HOST}
done < $2
ssh -p ${PORT} ${HOST} 'stop-hbase.sh'
while read -a slave
do
    echo "ssh -p ${slave[1]} ${slave[0]} '~/zookeeper*/bin/zkServer.sh stop'" | ssh -p ${PORT} ${HOST}
done < $2
ssh -p ${PORT} ${HOST} 'stop-all.sh'
