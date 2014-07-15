#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import with_statement
from fabric.api import *
from fabric.contrib.files import exists
import argparse
import sys
import hashlib
from datetime import datetime
from time import sleep
try:
    from helpers.check_args import checkArgs_for_launch
    from helpers.check_args import checkArgs_for_destroy
    from helpers.launcher import bootVM
    from helpers.destroy import destroy_cluster
    from helpers.verify_boot import verify_all
    from helpers.scp import scp
    from helpers.master_key import register_key
    from helpers.floating_ip import addFloatingIP
    from helpers.find_vm import getVMByName
    from helpers.find_vm import getVMById
except ImportError as e:
    raise
    print("Could not import helpers, MayDay MayDay...")
    sys.exit(1)


def parse_arguments():

    parser = argparse.ArgumentParser(description="Create a Spark Cluster on "
                                     "PDC Cloud.", epilog="Example Usage: "
                                     "./spark-openstack launch --keyname "
                                     "mykey --slaves 5 --cluster_name \
                                     clusterName")
    parser.add_argument('action', help = "launch|destroy")
    parser.add_argument("-c", "--cluster_name", metavar="",
                        dest="cluster_name",
                        action="store",
                        help="Name for the cluster.")
    parser.add_argument("-s", "--slaves", metavar="", dest="num_slaves",
                        type=int, action="store",
                        help="Number of slave nodes to spawn.")
    parser.add_argument("-k", "--keyname", metavar="", dest="keyname",
                        action="store",
                        help="Your Public Key registered in OpenStack")
    parser.add_argument("-f", "--flavor", metavar="", dest="flavor",
                        action="store", default="m1.medium",
                        help="Size of Virtual Machine")
    parser.add_argument("-i", "--image", metavar="", dest="image",
                        action="store", default="spark_1_img",
                        help="Image name to boot from")
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", help="verbose output")
    parser.add_argument("-D", "--dryrun", dest="dryrun",
                        action="store_true", help="Dry run")

    args = parser.parse_args()
    return args


# TODO: destroy cluster
#def destroy_cluster(opts):


def launch_cluster(opts):

    # username defined in the vm image
    username = "hduser"

    # unique hash to identify the cluster
    hash = hashlib.sha1(str(datetime.now())).hexdigest()

    # set metadata for master
    meta = {'master': hash}

    master_name = "master-" + opts.cluster_name

    # Boot Master
    master = bootVM(opts.image, opts.flavor, opts.keyname,
                    master_name, meta)
    master_id = master.id

    print("Booting Master, Please wait... ")
    while master.status == 'BUILD':
        sleep(5)
        # re get master to refresh status
        master = getVMById(master_id)

    master_private_ip = master.networks['private'][0]
    print("Master booted with private ip: " + str(master_private_ip))
    print("Assigning Floating ip")
    # Assign Floating IP to Master
    floating_ip = addFloatingIP(master)
    env.hosts = [floating_ip]
    env.user = username
    sleep(5)
    print("Floating IP assigned: " + floating_ip)
    floating_ip = '130.237.221.241'
    with hide('output', 'running', 'warnings'):
        local('ssh-keygen -R ' + floating_ip)
        sleep(90)
        pub_key = local('ssh-keyscan ' + floating_ip)
        local('echo ' + pub_key + ' tee -a ~/.ssh/known_hosts')

    # Test ssh
    print("Testing SSH, please wait...")
    tries = 0
    timeout = 30

    while tries < 6:
        tries = tries + 1
        backoff = tries * timeout
        try:
            with settings(host_string=username + "@" + floating_ip), \
                    hide('output', 'running', 'warnings'):
                        run('hostname -f')
        except:
            print("Waiting another " + str(backoff) + " seconds...")
            sleep(backoff)
        else:
            break

    if tries < 6:
        print("ssh test done, hostkey added to known_hosts")
    else:
        print("ERROR: could not ssh into master, MISSION ABORTED...")
        sys.exit(0)

    # get masters public key and register in nova
    with settings(host_string=username + "@" + floating_ip), hide('output',
                                                                  'running',
                                                                  'warnings'):
        #run('ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa -C ' + floating_ip)
        existing = exists('/home/hduser/.ssh/id_rsa.pub')
        if existing is False:
            run("ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa")
            sleep(2)
        master_key = run('cat ~/.ssh/id_rsa.pub')


    master_keyname = register_key(master_key, opts.cluster_name, opts.verbose)
    print("\n")
    print("********" + master_keyname + " registered in nova **********")

    # Now launch the requested number of slaves with masters public key
    meta = {'slave': hash}
    slave_name = "slave-" + opts.cluster_name
    status = bootVM(opts.image, opts.flavor, master_keyname, slave_name,
                    meta, opts.num_slaves, opts.num_slaves)
    #print(status)

    name = "slave"

    # find all vms whose name contains slave and matches hash
    slaves = getVMByName(name, hash)
    # get private ips of all slaves booted with hash
    slaves_list = verify_all(slaves)
    print("Got slaves list: " + str(slaves_list))

    # Create a file with master ip
    mFile = "/tmp/masters"
    slvFile = "/tmp/slaves"
    master_file = open(mFile, "w")
    master_file.write(master_private_ip + "\n")

    # Create a file with slave ips
    slave_file = open(slvFile, "w")
    for host in slaves_list:
        slave_file.write(host + "\n")
    master_file.close()
    slave_file.close()

    fabFile = "helpers/fabfile.py"
    dest = "/home/hduser/"
    HADOOP_CONF_DIR = "/home/hduser/DataAnalysis/hadoop/etc/hadoop/"

    with settings(host_string=username + "@" + floating_ip), hide('output',
        'running', 'warnings'):

        put(mFile, HADOOP_CONF_DIR + "masters")
        # copy slaves file to master's hadoop conf directory
        put(slvFile, HADOOP_CONF_DIR + "slaves")
        # move startup script to master
        put(fabFile, dest)


    print("\n")
    print("*********** All Slaves Created *************")
    print("*********** Slaves file Copied to Master *************")
    print("*********** Check that all slaves are RUNNING *************")
    print("*********** Login to Master and run the fabric file *********")
    print("\n")
    print("Login to Master as: ssh -l hduser  " + floating_ip)

    return (master, slaves)


if __name__ == "__main__":
    opts = parse_arguments()
    if str(opts.action) == 'launch':
        print("Arguments Verified... preparing launch sequence")
        args = checkArgs_for_launch(opts)
        if opts.dryrun:
            print("\n")
            print("Action: " + str(args.action))
            print("Cluster Name: " + str(args.cluster_name))
            print("Number of Slaves: " + str(args.num_slaves))
            print("Flavor: " + args.flavor)
            print("Image: " + args.image)
            print("Pub Key: " + args.keyname)
            print("\n")
            sys.exit(0)
        else:
            (master, slaves) = launch_cluster(opts)

    elif opts.action == 'destroy':
        master = checkArgs_for_destroy(opts)
        destroy_cluster(master)
    
    else:
        print("Invalid command: " + args.action)
        print("Please choose from launch or destroy")

