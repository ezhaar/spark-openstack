#!/bin/bash

# remove hostkey for given ip (if exists)
ssh-keygen -R $1

# get the new public key from host and add
# to known_hosts file
ssh-keyscan $1 | tee -a ~/.ssh/known_hosts
