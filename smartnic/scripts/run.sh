#!/bin/bash

## run server
ssh 10.10.1.2 "sh -c 'echo \"server\"; pwd'"&
sleep 3

sshpass -p'ghrms12!@' ssh 192.168.10.4 -lubuntu "sh -c 'echo \"worker\"; pwd'"&
sleep 3

"
echo "Running Server ..."
ssh 10.10.1.2 "sh -c 'cd /users/hcha/distributed_db/smartnic/build; ./server'" &
sleep 3

## run worker
echo "Running Worker ..."
sshpass -p'ghrms12!@' ssh ubuntu@192.168.10.4 "sh -c 'cd /home/ubuntu/distributed_db/smartnic/build; ./worker 100000'" &
sleep 1
"


echo "Running Client ..."
./client --workload c --num 100000 --threads 128 --zipfian 0.9
