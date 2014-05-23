#!/usr/bin/env python
# -*- coding: utf-8 -*-
from novaclient import client as nvClient
from get_creds import get_nova_creds


def extract_hash(server):

    if server.metadata:
        for key in server.metadata:
            value = server.metadata[key]
    else:
        value = None
    return value


def getVMByName(name, hash=None):
    creds = get_nova_creds()
    server_list = []
    nova = nvClient.Client("2", **creds)
    # Find all VMs whose name matches
    servers = nova.servers.list(search_opts={'name': name})
#    servers = nova.servers.list()
    # Filter out the VMs with correct hash
    for server in servers:
        val = extract_hash(server)
        if val == hash:
            server_list.append(server)
    return server_list


def getVMById(id):
    creds = get_nova_creds()
    nova = nvClient.Client("1.1", **creds)
    return nova.servers.get(id)
