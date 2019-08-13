#!/usr/bin/env bash
mvn clean package -DskipTests
echo "Sending to the server"
ssh -p $2 $1 'rm ranking-manager*.jar'
scp -P $2 target/ranking-manager*.jar $1:~/