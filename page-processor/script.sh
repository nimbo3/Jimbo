mvn clean package -DskipTests
echo "Sending to the first server"
ssh -p $2 $1 'rm page-processor*.jar'
scp -P $2 target/page-processor*.jar $1:~/
echo "Sending to other servers from the first one"
while read -a host
do
    echo "scp -P ${host[1]} page-processor*.jar ${host[0]}:~/;" | ssh -p $2 $1
done < $3
echo "Done sending to all servers"
