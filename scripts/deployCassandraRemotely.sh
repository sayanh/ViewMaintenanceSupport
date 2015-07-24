#!/bin/bash

echo "******* Shutting down Cassandra in VM1 *******"
ssh anarchy@vm1 'pkill -9 -f cassandra'
if [ $? -eq 0 ]; then
	echo "Successfully stopped cassandra in VM1"
else
	echo "Error while stopping cassandra in VM1"
fi


echo "******* Deploying Cassandra to VM1 *******"
scp /home/anarchy/work/sources/cassandra/build/apache-cassandra-3.0.0-SNAPSHOT.jar anarchy@vm1:~/cassandraviewmaintenance/build/

echo "******* Starting Cassandra in VM1 *******"
ssh anarchy@vm1 bash -c './cassandraviewmaintenance/bin/cassandra'
if [ $? -eq 0 ]; then
	echo "Successfully started cassandra in VM1"
else
	echo "Error in starting cassandra in VM1"
fi



echo "******* Shutting down Cassandra in VM2 *******"
ssh anarchy@vm2 'pkill -9 -f cassandra'
if [ $? -eq 0 ]; then
	echo "Successfully stopped cassandra in VM2"
else
	echo "Error while stopping cassandra in VM2"
fi


echo "******* Deploying Cassandra to VM2 *******"
scp /home/anarchy/work/sources/cassandra/build/apache-cassandra-3.0.0-SNAPSHOT.jar anarchy@vm2:~/cassandraviewmaintenance/build/

echo "******* Starting Cassandra in VM2 *******"
ssh anarchy@vm2 bash -c './cassandraviewmaintenance/bin/cassandra'
if [ $? -eq 0 ]; then
	echo "Successfully started cassandra in VM2"
else
	echo "Error in starting cassandra in VM2"
fi
