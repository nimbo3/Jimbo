while read -a host
do
	ssh-copy-id -p ${host[1]} ${host[0]}
	while read -a other_host
	do
		echo "ssh-copy-id -p ${other_host[1]} ${other_host[0]}" | ssh -p ${host[1]} ${host[0]}
	done < $1
done < $1
