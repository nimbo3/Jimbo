mvn package -DskipTests
echo sending to server-master
scp -P 3031 target/search-api-0.0.1-SNAPSHOT.jar jimbo@46.4.40.237:~