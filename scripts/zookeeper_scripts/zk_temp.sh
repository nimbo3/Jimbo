echo "making directory for zookeeper (dataDir)"
mkdir zk
cd zk
echo $zk_id > myid
cd ..
echo "done setting id"
echo "downloading zookeeper..."
wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.9/zookeeper-3.4.9.tar.gz
echo "download finished. extracting"
tar -xvzf zookeeper-3.4.9.tar.gz
echo "start configing zookeeper"
mv zoo.cfg zookeeper-3.4.9/conf/

