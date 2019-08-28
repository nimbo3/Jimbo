echo "downloading kafka"
wget https://archive.apache.org/dist/kafka/2.3.0/kafka_2.12-2.3.0.tgz
echo "unzipping kafka"
tar -xvzf kafka_2.12-2.3.0.tgz
rm kafka_2.12-2.3.0.tgz
mv server.properites kafka_2.12-2.3.0/config/
./kafka_2.12-2.3.0/bin/kafka-server-start.sh -daemon kafka_2.12-2.3.0/config/server.properties
