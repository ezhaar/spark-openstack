#!/bin/bash

ssh-keygen -R $1
ssh-keyscan $1 | tee -a ~/.ssh/known_hosts
