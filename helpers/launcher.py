#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
try:
    from novaclient import client as nvClient
except ImportError:
    print("Could not find module novaclient")
    sys.exit(1)

from get_creds import get_nova_creds


def bootVM(image, flavor, keyname, hostname, desc, min_count=1, max_count=1):
    creds = get_nova_creds()
#    res_id = "af40862d-ebf1-AAAA-a51b-08451ac08338"
    nova = nvClient.Client("1.1", **creds)
    image = nova.images.find(name=image)
    flavor = nova.flavors.find(name=flavor)
    instance = nova.servers.create(name=hostname,
                                   image=image,
                                   flavor=flavor,
                                   meta=desc,
                                   #reservations_id=res_id,
                                   min_count=min_count,
                                   max_count=max_count,
                                   key_name=keyname)
    return instance

if __name__ == "__main__":
    meta = {'description': 'Slave to cluster1'}
    out = bootVM("Ubuntu_Precise", "m1.tiny", "iz_jump_host", "testtt",
                 meta, 1, 1)
