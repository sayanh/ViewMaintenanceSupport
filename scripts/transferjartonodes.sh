#!/bin/bash

CASSANDRA_HOME='/home/anarchy/work/sources/cassandra'
FILE=$CASSANDRA_HOME"/build/apache-cassandra-3.0.0-SNAPSHOT.jar"
echo "--------------- Transfering "$FILE" to VM1 ---------------"
scp $FILE anarchy@vm1:~/apache-cassandra-2.1.5/lib/
if [ $? -eq 0 ]
	then
	echo "jar transferred successfully to VM1!!!"
else
	echo "process failed!!!"
fi
echo "--------------- Transfering "$FILE" to VM2 ---------------"
scp $FILE anarchy@vm2:~/apache-cassandra-2.1.5/lib/
if [ $? -eq 0 ]
        then
        echo "jar transferred successfully to VM2!!!"
else
        echo "process failed!!!"
fi
