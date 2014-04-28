#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import time
try:
    from novaclient import client as nvClient
except ImportError:
    print("Could not find module novaclient")
    sys.exit(1)

from get_creds import get_nova_creds


def verify_all(servers):
    creds = get_nova_creds()
    addresses = []
    nova = nvClient.Client("1.1", **creds)
    for instance in servers:
        timeout = 0
        status = instance.status
        while status == 'BUILD':
            time.sleep(5)
            timeout = timeout + 5
            if timeout > 1200:
                print("OBS! taking more than 10 minutes...")
                print("Something is Wrong, debug or contact izhaar@pdc.kth.se")
                exit()
            # Retrieve the instance again so the status field updates
            instance = nova.servers.get(instance.id)
            status = instance.status
        print(instance.name + " booted with ip: " +
              str(instance.addresses["private"][0]["addr"]))
        addresses.append(str(instance.addresses["private"][0]["addr"]))
    return addresses
