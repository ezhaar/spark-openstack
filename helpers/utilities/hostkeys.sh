#!/bin/bash
while read line
do
    echo $line
    host=$line
    ssh-keygen -R $host
    ssh-keyscan $host | tee -a ~/.ssh/known_hosts
done < /home/hduser/DataAnalysis/hadoop/etc/hadoop/slaves
