===========================================
Setup a Spark Cluster with Hadoop and YARN.
===========================================

PreReq
------
Use the scripts in https://www.github.com/ezhaar/spark-installer to install
hadoop, yarn and spark. Make sure you save a snapshot of the image in OpenStack 

Get Help
--------
./spark-openstack -h

Usage
-----
./spark-openstack --keyname myKey --slaves 2 --flavor m1.large --image
spark090-img --cluster_name clusterName

Once all machines have been booted, login to the master and run::

./configure_cluster.sh

This script will:

- Copy hadoop, yarn and spark configuration files on all the nodes.
- Format hadoop's namenode
- Start hadoop
- Create user directories in hdfs
- Start yarn
- Start spark master and slaves

Now you should be able to access the web ui:

- http://<master-ip>:50070 for namenode
- http://<master-ip>:50075 for datanode
- http://<master-ip>:8088 for resource manager
- http://<mater-ip>:8080 for spark web ui


