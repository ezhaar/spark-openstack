===========================================
Setup a Spark Cluster with Hadoop and YARN.
===========================================

PreReq
------
Use the scripts in https://www.github.com/ezhaar/spark-installer to install
hadoop, yarn and spark. We would also need the python package ``fabric``.
Fabric can be easily installed using pip or the disto's package manager. We
prefer pip.

.. code-block:: python

    sudo pip install fabric

P.S. ever heard of virtualenv? its awesome!!

Get Help
--------

.. code-block:: bash

    ./spark-openstack -h

Launch Cluster
--------------

.. code-block:: bash

    ./spark-openstack --keyname myKey --slaves 2 --flavor m1.large \
    --image spark090-img --cluster_name clusterName launch

Once all machines have been booted, login to the master and run the fabric
command to list all the options:

.. code-block:: python

    fab -l

Since this is the first login, initialize the cluster.

.. code-block:: python

    fab init_cluster

This script will:

- Copy hadoop, yarn and spark configuration files on all the nodes.
- Format hadoop's namenode
- Start hadoop
- Create user directories in hdfs
- Start yarn
- Start spark master and slaves

The fabric file contains options to start and stop hadoop/yarn as well as to
reset the cluster to its initial state.

.. code-block:: python

    fab start_hadoop

Now you should be able to access the web ui:

- http://<master-ip>:50070 for namenode
- http://<master-ip>:50075 for datanode
- http://<master-ip>:8088 for resource manager

Destroy Cluster
---------------

./spark-openstack -c clusterName destroy

