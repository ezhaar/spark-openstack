#!/bin/bash

### Configure and start required services ###


HOSTNAME=$(hostname)
IP=$(hostname -I)

echo "$IP $HOSTNAME" | sudo tee -a /etc/hosts
ssh-keyscan $HOSTNAME | sudo tee -a ~/.ssh/known_hosts
slaves=$(cat $HADOOP_CONF_DIR/slaves)

# set hostname in files
sed -i "s/XXXX/$HOSTNAME/g" $HADOOP_CONF_DIR/core-site.xml
#sed -i "s/XXXX/$HOSTNAME/g" $HADOOP_CONF_DIR/yarn-site.xml


for slave in $slaves
do 
    cd ~/DataAnalysis/hadoop/etc/hadoop
    ssh-keyscan $slave | sudo tee -a ~/.ssh/known_hosts
    
    #copy masters and slaves to spark on master
    scp masters slaves ~/DataAnalysis/spark/conf/
    
    #copy hadoop conf files to slave
    scp masters slaves core-site.xml  hduser@$slave:~/DataAnalysis/hadoop/etc/hadoop/
   
    #copy spark conf files to slave
    cd ~/DataAnalysis/spark/conf
    scp masters slaves spark-env.sh  hduser@$slave:~/DataAnalysis/spark/conf/
    
    
done

#hdfs namenode -format
#start-dfs.sh
#start-yarn.sh

#hdfs dfs -mkdir /user
#hdfs dfs -mkdir /user/hduser
