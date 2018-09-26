#!/bin/python3

import os
import sys
import socket
import re
import shlex, subprocess
import smtplib
import shutil
# import matplotlib
# import numpy as np
# import matplotlib.pyplot as plt
# import pandas as pd
import json
from pprint import pprint
import time
from datetime import timedelta
import multiprocessing
# from termcolor import colored, cprint


def exec_cmd(cmd, env, async=False):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, env=env)
    if not async:
	    p.wait()
	    print('Output:')
	    for ol in p.stdout:
	        print(ol.decode(encoding="utf-8", errors="strict"), end='')
	    print('Error:')
	    for el in p.stderr:
	        print(el.decode(encoding="utf-8", errors="strict"), end='')
    return p

def wait_for(plist, output=False):
	for i,p in enumerate(plist):
		p.wait()
		if output:
			print('Output of node: {}'.format(i))
			for ol in p.stdout:
				print(ol.decode(encoding="utf-8", errors="strict"), end='')
			print('----- End of output of node {} -----'.format(i))

global env
env = dict(os.environ)

# 9 nodes total
node_list = ['172.31.7.156','172.31.12.138','172.31.11.228',
'172.31.13.62','172.31.2.208','172.31.1.29','172.31.8.5', '172.31.7.183','172.31.6.241']

# 4 nodes qcd-0 to qcd-3
node_list = ['172.31.7.156','172.31.2.208','172.31.12.138','172.31.13.62']

# 5 nodes qcd-0 to qcd-4
node_list = ['172.31.7.156','172.31.2.208','172.31.12.138','172.31.13.62','172.31.11.228']

# 6 nodes qcd-0 to qcd-5
node_list = ['172.31.7.156','172.31.2.208','172.31.12.138','172.31.13.62','172.31.11.228','172.31.1.29']


server_cnt = 2

with open('/mnt/efs/expodb/ifconfig.txt','w') as f:
	for nip in node_list:
		f.write('{}\n'.format(nip))

proc_list = []
#copy ifconfig.txt to build directories		
for nip in node_list:
	print('working on node (ifconfig sync): {}'.format(nip), end=',')
	rcmd = 'ssh ubuntu@{} {}'.format(nip,'cp -f /mnt/efs/expodb/ifconfig.txt ~/qcd-build/ifconfig.txt')
	proc_list.append(exec_cmd(rcmd,env,True))
wait_for(proc_list)
proc_list.clear()
print('done (ifconfig sync)!')

#sync from EFS
for nip in node_list:
	print('working on node (EFS sync): {}'.format(nip), end=',')
	rcmd = 'ssh ubuntu@{} {}'.format(nip,'~/sync_from_efs.sh')
	proc_list.append(exec_cmd(rcmd,env,True))
wait_for(proc_list)
proc_list.clear()
print('done (EFS sync)!')

#build and compile on each node
for nip in node_list:
	print('working on node (building): {}'.format(nip), end=',')
	rcmd = 'ssh ubuntu@{} "{}"'.format(nip,'cd ~/qcd-build; make -j8')
	proc_list.append(exec_cmd(rcmd,env,True))

wait_for(proc_list)
proc_list.clear()
print('done (building)!')

for nip in node_list:
	print('working on node (kill all): {}'.format(nip), end=',')
	rcmd = 'ssh ubuntu@{} "{}"'.format(nip,'pkill -9 rundb; pkill -9 runcl')
	proc_list.append(exec_cmd(rcmd,env,True))

wait_for(proc_list)
proc_list.clear()
print('done (kill-all)!')

for i,nip in enumerate(node_list):
	if i < server_cnt:
		#start server process
		print("{} as a server #{}, i={}".format(nip,i,i))
		rcmd = 'ssh ubuntu@{} "{}{}"'.format(nip,'cd ~/qcd-build; bin/rundb -nid',i)
	else:
		#start client process
		print("{} as a client #{}, i={}".format(nip,i-server_cnt,i))
		rcmd = 'ssh ubuntu@{} "{}{}"'.format(nip,'cd ~/qcd-build; bin/runcl -nid',i)
	proc_list.append(exec_cmd(rcmd,env,True))

wait_for(proc_list,True)
proc_list.clear()
print('done (experiment)!')