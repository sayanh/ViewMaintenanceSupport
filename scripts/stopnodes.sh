#!/bin/bash

echo "Stopping the nodes"
ssh anarchy@vm1 'pkill -9 -f cassandra'
if [ $? -eq 0 ]; then
	echo "Successfully stopped cassandra in VM1"
else
	echo "Error while stopping cassandra in VM1"
fi
ssh anarchy@vm2 'pkill -9 -f cassandra'
if [ $? -eq 0 ]; then
	echo "Successfully stopped cassandra in VM2"
else
	echo "Error while stopping cassandra in VM2"
fi
