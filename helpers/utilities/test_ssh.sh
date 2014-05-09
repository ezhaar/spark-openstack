#!/bin/bash
ssh -l $1 $2 -o connectTimeout=5 'exit' || exit
