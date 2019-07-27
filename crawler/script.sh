mvn clean package
echo "sending to server4 (46.4.40.237)"
ssh -p 3031 jimbo@46.4.40.237 'rm crawler-0.0.1-SNAPSHOT.jar'
scp -P 3031 target/crawler-0.0.1-SNAPSHOT.jar jimbo@46.4.40.237:~/
echo "sending to servers from server3"
ssh -p 3031 jimbo@46.4.40.237 'scp -P 3031 crawler-0.0.1-SNAPSHOT.jar jimbo@144.76.119.111:~/;echo "done sending to server1 (144.76.119.111)";scp -P 3031 crawler-0.0.1-SNAPSHOT.jar jimbo@144.76.24.115:~/;echo "done sending to server2 (144.76.24.115)";scp -P 3031 crawler-0.0.1-SNAPSHOT.jar jimbo@5.9.110.169:~/;echo "done sending to server3 (5.9.110.169)";'
echo "done sending to all servers"
