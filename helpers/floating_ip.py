#!/usr/bin/env python
# -*- coding: utf-8 -*-
from novaclient import client as nvClient
from get_creds import get_nova_creds


def get_floating_ip():
    creds = get_nova_creds()
    nova = nvClient.Client("1.1", **creds)
    free_ips = []
    # Check if there are any free IPs available
    ip_list = nova.floating_ips.list()
    for ip_object in ip_list:
        if ip_object.instance_id is None:
            free_ips.append(ip_object)

    # if no free IP then Create
    if len(free_ips) == 0:
        return nova.floating_ips.create(pool="public")

    # otherwise return the first available IP
    else:
        return free_ips[0]


def addFloatingIP(instance):
    creds = get_nova_creds()
    nova = nvClient.Client("1.1", **creds)
    floating_ip = get_floating_ip()
    instance.add_floating_ip(floating_ip)
    return str(floating_ip.ip)

if __name__ == "__main__":
    print(get_floating_ip())
