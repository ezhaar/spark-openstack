#!/usr/bin/env python
# -*- coding: utf-8 -*- #


from __future__ import with_statement
from fabric.api import (
    run, local, parallel, put, env, roles,
    cd, lcd, task, abort,
)
from fabric.tasks import execute


# Set the user to use for ssh
env.user = "hduser"
env.add_unknown_hosts = True

HADOOP_CONF_DIR = '/home/hduser/DataAnalysis/hadoop/etc/hadoop/'
HADOOP_DATA_DIR = '/home/hduser/data/hadoop/hdfs/'
SPARK_CONF_DIR = '/home/hduser/DataAnalysis/spark/conf/'

# build the list of slaves from slaves file
with open(HADOOP_CONF_DIR + "slaves", 'r + ') as f:
        slaves = f.readlines()

f.close()
env.roledefs = {'slave': slaves}


@task
@roles('slave')
def test_conn():
    try:
        run('hostname -f')
    except:
        abort("All slaves not up yet.. Try again in a moment")


@task
def add_host_keys():
    local('/home/hduser/hostkeys.sh')


@task
@roles('slave')
@parallel
def deploy_conf_files():
    put(HADOOP_CONF_DIR + "slaves", HADOOP_CONF_DIR + "slaves")
    put(HADOOP_CONF_DIR + "masters", HADOOP_CONF_DIR + "masters")
    put(HADOOP_CONF_DIR + "core-site.xml", HADOOP_CONF_DIR + "core-site.xml")
    put(HADOOP_CONF_DIR + "hdfs-site.xml", HADOOP_CONF_DIR + "hdfs-site.xml")
    put(HADOOP_CONF_DIR + "mapred-site.xml", HADOOP_CONF_DIR +
        "mapred-site.xml")
    put(HADOOP_CONF_DIR + "yarn-site.xml.slave", HADOOP_CONF_DIR +
        "yarn-site.xml")
    put(HADOOP_CONF_DIR + "masters", HADOOP_CONF_DIR + "masters")
    put(SPARK_CONF_DIR + "slaves", SPARK_CONF_DIR + "slaves")
    put(SPARK_CONF_DIR + "spark-env.sh", SPARK_CONF_DIR + "spark-env.sh")


@task
def set_conf_files():
    with lcd(HADOOP_CONF_DIR):
        local('sed -i "s/XXXX/$(hostname)/g" core-site.xml')
        local('sed -i "s/XXXX/$(hostname)/g" yarn-site.xml')
        local('sed -i "s/XXXX/$(hostname)/g" yarn-site.xml.slave')
        local('cp slaves /home/hduser/DataAnalysis/spark/conf/')


@roles('slave')
def reset_hdfs_dirs():
    with lcd('/home/hduser/data/hadoop/hdfs'):
        local('rm -rf dn nn snn')
        local('mkdir dn && mkdir nn && mkdir snn')
    with cd(HADOOP_DATA_DIR):
        run('rm -rf dn nn snn')
        run('mkdir dn && mkdir nn && mkdir snn')


def format_hdfs():
    execute(reset_hdfs_dirs)
    local('hdfs namenode -format')
    local('start-dfs.sh')
    local('hdfs dfs -mkdir /user')
    local('hdfs dfs -mkdir /user/hduser')


@task
def init_cluster():
    execute(test_conn)
    execute(add_host_keys)
    execute(set_conf_files)
    execute(deploy_conf_files)
    execute(format_hdfs)
    execute(start_hadoop)


@task
def stop_hadoop():
    local('stop-dfs.sh')
    local('stop-yarn.sh')


@task
def start_hadoop():
    local('start-dfs.sh')
    local('start-yarn.sh')


@task
def reset_cluster():
    execute(add_host_keys)
    execute(stop_hadoop)
    execute(set_conf_files)
    execute(deploy_conf_files)
    execute(format_hdfs)
    execute(start_hadoop)
