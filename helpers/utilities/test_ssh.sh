#!/bin/bash
ssh -l $1 $2 -o connectTimeout=5 'exit' || exit
#OUT=ssh -l $1 $2 -o connectTimeout=5 "cat ~ubuntu/.ssh/authorized_keys | sudo\
#    tee -a /home/hduser/.ssh/authorized_keys"
