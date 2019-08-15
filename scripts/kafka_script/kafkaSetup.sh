echo "welcome to kafka installer"
echo "for kafka installation you need zookeeper up"
echo "are zookeepers up?(y for yes)"
read is_zookeeper_up
if [[ $is_zookeeper_up == 'y' || $is_zookeeper_up == 'Y' ]]
then
	echo 'start reading hosts files'
	input="./kafka_zk_host_port_id"
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
	# Line 20
	number_of_hosts=${#hosts[@]}
	echo "number of hosts readed : $(($number_of_hosts / 2))"
	echo ""
	# Line 24
	for ((i=0;i<$number_of_hosts;i=$((i + 2)))); 
	do 
		# Line 27
		echo "start processing host $(($(($i / 2)) + 1))"
		var1=${hosts[$i]}
		array1=(${var1//:/ })
		var2=${array1[0]}
		array2=(${var2//@/ })
		user=${array2[0]}
		ip=${array2[1]}
		port=${array1[1]}
		echo "user : $user, ip : $ip, port : $port"
		# Line 37
		host_id=${hosts[$(($i + 1))]}
		echo "id given to this server : $host_id"
		echo broker.id=$host_id > server.properties 
		# Line 41
		st=""
		for ((j=0;j<$number_of_hosts;j=$((j + 2)))); 
		do
			v=${hosts[$j]}
			ar=(${v//@/ })
			echo "host to add : ${ar[1]}"
			st="$st ${ar[1]}"	
		done
		st=${st// /,}
		st="${st:1}"
		st="zookeeper.connect=$st"
		echo $st >> server.properties
		# Line 53
		cat server_sample.properties >> server.properties
		scp -P $port server.properties $user@$ip:~/
		scp -P $port kafka_installer_sample.sh $user@$ip:~/
		ssh $user@$ip -p $port 'chmod +x kafka_installer_sample.sh && ./kafka_installer_sample.sh && rm kafka_installer_sample.sh'
		echo "processing done"
	done
	# Line 60
	echo "done"
	echo
else
	echo 'closing kafka setup'
fi

echo
echo "done"
