#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
try:
    from novaclient import client as nvClient
except ImportError:
    print("Could not find module novaclient")
    sys.exit(1)

from verify_boot import verify_all
from find_vm import getVMByName, extract_hash
from get_creds import get_nova_creds
from master_key import delete_key

def destroy_cluster(master):
    creds = get_nova_creds()
    nova = nvClient.Client("1.1", **creds)
    try:
        master_id = nova.servers.find(name=master)
        hash = extract_hash(master_id)
        print("wait... finding all associated slaves")
        name = "slave"
        slaves = getVMByName(name, hash)
        for slave in slaves:
            print(slave)
        input = raw_input("Are you sure you want to delete " 
                + master + 
                " ? (y/n): ")
        if input != 'y':
            print("Destruct Sequence Aborted")

        else:
            print("Now deleting: " + master )
            for slave in slaves:
                slave.delete()
            master_id.delete()
            delete_key(master)
    except:
        print("ERROR: MasterNotFound")
