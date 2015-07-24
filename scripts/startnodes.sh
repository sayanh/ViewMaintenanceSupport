#!/bin/bash

echo "------------------------------------------------------"
echo "-----------------Working in VM1 ----------------------"
echo "------------------------------------------------------"

ssh anarchy@vm1 bash -c './cassandraviewmaintenance/bin/cassandra'
if [ $? -eq 0 ]; then
	echo "Successfully started cassandra in VM1"
else
	echo "Error in starting cassandra in VM1"
fi
echo "------------------------------------------------------"
echo "-----------------Working in VM2 ----------------------"
echo "------------------------------------------------------"
ssh anarchy@vm2 bash -c './cassandraviewmaintenance/bin/cassandra'
if [ $? -eq 0 ]; then
	echo "Successfully started cassandra in VM2"
else
	echo "Error in starting cassandra in VM2"
fi
#ssh anarchy@vm2 './cassandra/bin/cassandra'


