#!/usr/bin/env python
# -*- coding: utf-8 -*- #


from __future__ import with_statement
from fabric.api import (
    run, local, parallel, put, env, roles,
    cd, lcd, task, abort
)
from fabric.tasks import execute
from fabric.contrib.files import exists


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
@with_settings(hide('output', 'running', 'warnings'), warn_only=True)
@roles('slave')
def test_conn():
    try:
        run('hostname -f')
    except:
        abort("All slaves not up yet.. Try again in a moment")


@task
@roles('slave')
@with_settings(hide('output', 'running', 'warnings'), warn_only=True)
def add_host_keys():
    ip = run('hostname -i')
    existing = exists('/home/hduser/.ssh/id_rsa.pub')
    if existing is False:
        run("ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa")
        sleep(2)

    local('ssh-keyscan -H ' + ip + ' >> ~/.ssh/known_hosts')


@task
@roles('slave')
@parallel
@with_settings(hide('output', 'running', 'warnings'), warn_only=True)
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
@with_settings(hide('output', 'running', 'warnings'), warn_only=True)
def set_conf_files():
    with lcd(HADOOP_CONF_DIR):
        local('sed -i "s/XXXX/$(hostname)/g" core-site.xml')
        local('sed -i "s/XXXX/$(hostname)/g" yarn-site.xml')
        local('sed -i "s/XXXX/$(hostname)/g" yarn-site.xml.slave')
        local('cp slaves /home/hduser/DataAnalysis/spark/conf/')


@roles('slave')
@with_settings(hide('output', 'running', 'warnings'), warn_only=True)
def reset_hdfs_dirs():
    with lcd('/home/hduser/data/hadoop/hdfs'):
        local('rm -rf dn nn snn')
        local('mkdir dn && mkdir nn && mkdir snn')
    with cd(HADOOP_DATA_DIR):
        run('rm -rf dn nn snn')
        run('mkdir dn && mkdir nn && mkdir snn')


@with_settings(hide('output', 'running', 'warnings'), warn_only=True)
def format_hdfs():
    execute(reset_hdfs_dirs)
    local('hdfs namenode -format')
    local('start-dfs.sh')
    local('hdfs dfs -mkdir /user')
    local('hdfs dfs -mkdir /user/hduser')
    local('stop-dfs.sh')


@task
@with_settings(hide('output', 'running', 'warnings'), warn_only=True)
def init_cluster():
    execute(test_conn)
    execute(add_host_keys)
    execute(set_conf_files)
    execute(deploy_conf_files)
    execute(format_hdfs)
    execute(start_hadoop)


@task
@with_settings(hide('output', 'running', 'warnings'), warn_only=True)
def stop_hadoop():
    local('stop-dfs.sh')
    local('stop-yarn.sh')


@task
@with_settings(hide('output', 'running', 'warnings'), warn_only=True)
def start_hadoop():
    local('start-dfs.sh')
    local('start-yarn.sh')


@task
@with_settings(hide('output', 'running', 'warnings'), warn_only=True)
def reset_cluster():
    execute(add_host_keys)
    execute(stop_hadoop)
    execute(set_conf_files)
    execute(deploy_conf_files)
    execute(format_hdfs)
    execute(start_hadoop)
