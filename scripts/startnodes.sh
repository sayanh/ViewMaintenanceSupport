#!/bin/bash

echo "------------------------------------------------------"
echo "-----------------Working in VM1 ----------------------"
echo "------------------------------------------------------"

<<<<<<< HEAD
ssh anarchy@vm1 bash -c './cassandraviewmaintenance/bin/cassandra'
=======
ssh anarchy@vm1 bash -c './apache-cassandra-2.1.5/bin/cassandra'
>>>>>>> 8d936ca4fe06ca1252386b89839e9f81771053af
if [ $? -eq 0 ]; then
	echo "Successfully started cassandra in VM1"
else
	echo "Error in starting cassandra in VM1"
fi
echo "------------------------------------------------------"
echo "-----------------Working in VM2 ----------------------"
echo "------------------------------------------------------"
<<<<<<< HEAD
ssh anarchy@vm2 bash -c './cassandraviewmaintenance/bin/cassandra'
=======
ssh anarchy@vm2 bash -c './apache-cassandra-2.1.5/bin/cassandra'
>>>>>>> 8d936ca4fe06ca1252386b89839e9f81771053af
if [ $? -eq 0 ]; then
	echo "Successfully started cassandra in VM2"
else
	echo "Error in starting cassandra in VM2"
fi
#ssh anarchy@vm2 './cassandra/bin/cassandra'


