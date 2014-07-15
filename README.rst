===========================================
Setup a Spark Cluster with Hadoop and YARN.
===========================================

PreReq
------
Use the scripts in https://www.github.com/ezhaar/spark-installer to install
hadoop, yarn and spark. Make sure you save a snapshot of the image in OpenStack 
You also need the python module fabric installed.

Get Help
--------
./spark-openstack -h

Launch Cluster
--------------
./spark-openstack --keyname myKey --slaves 2 --flavor m1.large --image
spark090-img --cluster_name clusterName launch

Once all machines have been booted, login to the master and run::

fab -l

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

Destroy Cluster
---------------

./spark-openstack -c clusterName destroy
