#!/usr/bin/env bash
mvn clean package -DskipTests
echo "Sending to the server"
ssh -p $2 $1 'rm search-api*.jar'
scp -P $2 target/search-api*.jar $1:~/