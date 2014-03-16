#!/bin/bash

# Create a Public Key at Master 
# This key will be registered in OpenStack
# All the slaves will be booted with this key

MASTER_IP=$1
username=$2
MASTER_SSH="ssh -l $username $MASTER_IP -o connectTimeout=5"

# ssh to master
PUB_KEY=$($MASTER_SSH "cat ~/.ssh/id_rsa.pub 2>/dev/null")

# if key found
if [ $? == 0 ]; then
    echo $PUB_KEY
   
# if key not found, generate and add to authorized_keys
else
    # Create key
    $($MASTER_SSH "ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa -C $MASTER_IP")
    sleep 2
    # Add it to authorized_keys
    PUB_KEY=$($MASTER_SSH "cat ~/.ssh/id_rsa.pub | tee -a ~/.ssh/authorized_keys")
    # Return Key for registration in OpenStack
    echo $PUB_KEY
fi

