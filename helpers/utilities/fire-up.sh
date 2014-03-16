#!/bin/bash
addr=$(hostname -I | tr -d ' ')
sed -i 's/XX.XX.XX.XX/$addr/g' \
    /home/hduser/DataAnalysis/hadoop/etc/hadoop/yarn-site.xml
