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

import argparse
import sys
import hashlib
from datetime import datetime
from time import sleep
try:
    from helpers.check_args import checkArgs
    from helpers.launcher import bootVM
    from helpers.verify_boot import verify_all
    from helpers.scp import scp
    from helpers.test_ssh import test_ssh
    from helpers.master_key import master_key
    from helpers.floating_ip import addFloatingIP
    from helpers.find_vm import getVMByName
    from helpers.find_vm import getVMById
except ImportError as e:
    print("Could not import helpers, MayDay MayDay...")
    sys.exit(1)


def parse_arguments():

    parser = argparse.ArgumentParser(description="Create a Spark Cluster on "
                                     "PDC Cloud.", epilog="Example Usage: "
                                     "./spark-openstack launch --keyname "
                                     "mykey --slaves 5 --cluster_name clusterName")

    parser.add_argument("-c", "--cluster_name", metavar="",
                        dest="cluster_name",
                        action="store",
                        help="Name for the cluster.")
    parser.add_argument("-s", "--slaves", metavar="", dest="num_slaves",
                        type=int, action="store",
                        help="Number of slave nodes to spawn.")
    parser.add_argument("-k", "--keyname", metavar="", dest="keyname",
                        action="store",  # default=defaults.floating_ip,
                        help="Your Public Key registered in OpenStack")
    parser.add_argument("-f", "--flavor", metavar="", dest="flavor",
                        action="store", default="m1.medium",
                        help="Size of Virtual Machine")
    parser.add_argument("-i", "--image", metavar="", dest="image",
                        action="store", default="spark090-img",
                        help="Image name to boot from")
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", help="verbose output")
    parser.add_argument("-D", "--dryrun", dest="dryrun",
                        action="store_true", help="Dry run")

    args = parser.parse_args()

    # Verify arguments
    #check if clusterName already taken
    args = checkArgs(args)
    # If args verified and there were no errors
    # set variables
    if args.dryrun:
        print("\n")
        print("Cluster Name: " + str(args.cluster_name))
        print("Number of Slaves: " + str(args.num_slaves))
        print("Flavor: " + args.flavor)
        print("Image: " + args.image)
        print("Pub Key: " + args.keyname)
        print("\n")
        sys.exit(0)

    print("Arguments Verified, Now Lanunching Cluster")
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
    print("Floating IP assigned: " + floating_ip)

    # Test ssh
    print("Testing SSH, please wait...")

    sleep (60)
    tries = 0
    timeout = 30
    
    while tries < 6:
        tries = tries + 1
        backoff = tries * timeout
        try:
            # test ssh and add masters hostkey to known_hosts file
            ssh_status = test_ssh(floating_ip, username, True)
            
        except:
            print("Waiting another " + str(backoff) + " seconds...")
            sleep(backoff)
        else:
            break

    if tries < 6:
        print(ssh_status)
        print("ssh test done, hostkey added to known_hosts")
    else:
        print("ERROR: could not ssh into master, exiting...")
        sys.exit(0)

    # get masters public key and register in nova
    master_keyname = master_key(floating_ip, username, master_name,
                                opts.verbose)

    print("\n")
    print("********" + master_keyname + " registered in nova **********")

    # Now create the requested number of slaves with masters public key
    meta = {'slave': hash}
    slave_name = "slave-" + opts.cluster_name
    status = bootVM(opts.image, opts.flavor, master_keyname, slave_name,
                    meta, opts.num_slaves, opts.num_slaves)
    print(status)

    name = "slave"

    # find all vms whose name contains slave and matches hash
    slaves = getVMByName(name, hash)
    # get private ips of all slaves booted with hash
    slaves_list = verify_all(slaves)
    print("Got slaves list: " + str(slaves_list))

    # Create a file with master ip
    tmpFile = "/tmp/masters"
    master_file = open(tmpFile, "w")
    master_file.write(master_private_ip + "\n")

    # copy masters file to master's hadoop conf directory
    dest = "/home/hduser/DataAnalysis/hadoop/etc/hadoop/masters"
    scp(floating_ip, username, tmpFile, dest)

    # Create a file with slave ips
    tmpFile = "/tmp/slaves"
    slave_file = open(tmpFile, "w")
    for host in slaves_list:
        slave_file.write(host + "\n")

    # copy slaves file to master's hadoop conf directory
    dest = "/home/hduser/DataAnalysis/hadoop/etc/hadoop/slaves"
    scp(floating_ip, username, tmpFile, dest)

    # move startup script to master
    tmpFile = "helpers/utilities/configure_cluster.sh"
    dest = "/home/hduser/"
    scp(floating_ip, username, tmpFile, dest)

    print("\n")
    print("*********** All Slaves Created *************")
    print("*********** Slaves file Copied to Master *************")
    print("*********** Check that all slaves are RUNNING *************")
    print("*********** Login to Master and run configure_cluster.sh *********")
    print("\n")
    print("Login to Master as: ssh -l hduser  " + floating_ip)

    return (master, slaves)
if __name__ == "__main__":
    opts = parse_arguments()
    (master, slaves) = launch_cluster(opts)
