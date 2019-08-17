input="./user@ip:port"
comment_sign="#"
while IFS= read -r line
do
	if [[ ${line:0:1} != $comment_sign ]]
	then
		arr=(${line//:/ })
		echo "connecting to ${arr[0]}"
		if [[ $1 == 'start' ]]
		then
			echo "not working yet"
			# scp -P ${arr[1]} zk_start_sample.sh ${arr[0]}:~/
			# ssh -p ${arr[1]} ${arr[0]} 'chmod +x zk_start_sample.sh && ./zk_start_sample.sh'
			# ssh -p  ${arr[1]} ${arr[0]} 'rm zk_start_sample.sh'
		fi
		if [[ $1 == 'stop' ]]
		then
			scp -P ${arr[1]} zk_stop_sample.sh ${arr[0]}:~/
			ssh -p ${arr[1]} ${arr[0]} 'chmod +x zk_stop_sample.sh && ./zk_stop_sample.sh'
			ssh -p  ${arr[1]} ${arr[0]} 'rm zk_stop_sample.sh'
		fi
		if [[ $1 == 'status' ]]
		then
			echo "not working yet"
			# scp -P ${arr[1]} zk_status_sample.sh ${arr[0]}:~/
			# ssh -p ${arr[1]} ${arr[0]} 'chmod +x zk_status_sample.sh && ./zk_status_sample.sh'
			# ssh -p  ${arr[1]} ${arr[0]} 'rm zk_status_sample.sh'
		fi
	fi
done < "$input"
