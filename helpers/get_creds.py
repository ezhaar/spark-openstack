#!/usr/bin/env python
import os
def get_nova_creds():
    creds = {}
    creds['username'] = os.environ['OS_USERNAME']
    creds['api_key'] = os.environ['OS_PASSWORD']
    creds['auth_url'] = os.environ['OS_AUTH_URL']
    creds['project_id'] = os.environ['OS_TENANT_NAME']
    return creds
