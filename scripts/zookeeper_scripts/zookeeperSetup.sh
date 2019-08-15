input="./hosts"
comment_sign="#"
declare -a hosts
echo "reading hosts from file : $input"
while IFS= read -r line
do
	if [[ ${line:0:1} != $comment_sign ]]
	then
		hosts+=($line)
	fi
done < "$input"
echo "done reading hosts"
# Line 13
number_of_hosts=${#hosts[@]}
echo "number of hosts readed : $(($number_of_hosts / 2))"
echo ""
# Line 17
for ((i=0;i<$number_of_hosts;i=$((i + 2)))); 
do 
	# Line 20
	echo "start processing host $(($(($i / 2)) + 1))"
	var1=${hosts[$i]}
	array1=(${var1//:/ })
	var2=${array1[0]}
	array2=(${var2//@/ })
	user=${array2[0]}
	ip=${array2[1]}
	port=${array1[1]}
	echo "user : $user, ip : $ip, port : $port"
	# Line 30
	host_id=${hosts[$(($i + 1))]}
	echo "id given to this server : $host_id"
	echo zk_id=$host_id > zk_temp30926.sh 
	cat zk_temp.sh >> zk_temp30926.sh
	cat temp_zoo.cfg > zoo.cfg
	echo dataDir=/home/$user/zk >> zoo.cfg
	# Line 37
	for ((i=0;i<$number_of_hosts;i=$((i + 2)))); 
	do
		# Line 40
		temp_id=${hosts[$(($i + 1))]}
		var1_temp=${hosts[$i]}
		array1_temp=(${var1_temp//:/ })
		var2_temp=${array1_temp[0]}
		array2_temp=(${var2_temp//@/ })
		ip_temp=${array2_temp[1]}
		echo server.$temp_id=$ip_temp:2888:3888 >> zoo.cfg
	done
	# Line 49
	echo "sync" >> zk_temp30926.sh
	echo "pwd && ls" >> zk_temp30926.sh
	echo "rm zookeeper-3.4.9.tar.gz" >> zk_temp30926.sh
	scp -P $port zoo.cfg $user@$ip:~/
	scp -P $port zk_temp30926.sh $user@$ip:~/
	ssh $user@$ip -p $port 'chmod +x zk_temp30926.sh && sync && ./zk_temp30926.sh && sync'
	ssh $user@$ip -p $port 'rm zk_temp30926.sh'
	echo "processing done"
done
# Line 60
echo "delete local zoo.cfg"
rm zoo.cfg
echo "delete local zk_temp30926.sh"
rm zk_temp30926.sh

./zk_opporations.sh 'start'
./zk_opporations.sh 'status'

