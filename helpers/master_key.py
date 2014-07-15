#!/usr/bin/python

from novaclient import client as nvClient
from get_creds import get_nova_creds
from subprocess import Popen, PIPE


def register_key(key, cluster_name, verbose):
    try:
        creds = get_nova_creds()
        nova = nvClient.Client("1.1", **creds)
        key_name = cluster_name + "-key"
        nova.keypairs.create(name=key_name, public_key=key)
        return key_name
    except (ValueError, OSError) as err:
        if verbose:
            print(err)
        print("\nERROR: Could not communicate with master\n")
        exit()
    except:
        print("Unknown error Occured")
        raise

