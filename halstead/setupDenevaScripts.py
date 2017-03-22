#!/usr/bin/env python
import os
import socket
import re

DENEVA_DIR_PREFIX = 'deneva/'
# WORK_DIR = os.environ['PBS_O_WORKDIR']
WORK_DIR = '/home/tqadah/deneva_project'
os.chdir(WORK_DIR)

#get node names from 'hostnames' file which should be created by 'uniqu $PBS_NODEFILE > hostnames"
nodefile = open('hostnames', 'r')
nodeips = []
for hn in nodefile:
    ip = socket.gethostbyname(hn.strip())
    nodeips.append(ip)

print(nodeips)
#parse config.h
confpath = DENEVA_DIR_PREFIX + "config.h"
print(confpath)
conffile = open(confpath, 'r')
S_NODE_CNT = 0
C_NODE_CNT = 0
S_CORE_CNT = 0
C_CORE_CNT = 0

CLUSTER_NODE_CNT = len(nodeips)

for line in conffile:
#     print(line)
    snc_m = re.search('#define NODE_CNT (\d+)', line.strip())
    if (snc_m):
        S_NODE_CNT = int(snc_m.group(1))
    cnc_m = re.search('#define CLIENT_NODE_CNT (\d+)', line.strip())
    if (cnc_m):
        C_NODE_CNT = int(cnc_m.group(1))
    cc_m = re.search('#define CORE_CNT (\d+)', line.strip())
    if (cc_m):
        S_CORE_CNT = int(cc_m.group(1))
        C_CORE_CNT = S_CORE_CNT
        
print(S_NODE_CNT)
print(C_NODE_CNT)
print(S_CORE_CNT)
print(C_CORE_CNT)
print(CLUSTER_NODE_CNT)
#create ifconfig.txt
# for now, create 1 server and 1 client
ifconfig_path = DENEVA_DIR_PREFIX + 'ifconfig.txt'
ifconfig_file = open(ifconfig_path, 'w')
for ip in nodeips:
    print(ip)
    ifconfig_file.write(ip+'\n')
ifconfig_file.close()

#create rundb_0.sh: a shell script to run server processes on node 0

#create runcl_1.sh: a shell script to run client processes on node 1 
