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
import ec2_nodes


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
		
		if p.returncode != 0:
			print('Error at node: {}'.format(i))
			for el in p.stderr:
				print(el.decode(encoding="utf-8", errors="strict"),end='')
			print('----- End of Error of node {} -----'.format(i))

global env
env = dict(os.environ)

# 9 nodes total
# node_list = ['172.31.7.156','172.31.12.138','172.31.11.228',
# '172.31.13.62','172.31.2.208','172.31.1.29','172.31.8.5', '172.31.7.183','172.31.6.241']

# 3 nodes qcd-0 to qcd-3
# node_list = ['172.31.7.156','172.31.2.208','172.31.12.138']

# 4 nodes qcd-0 to qcd-3
# node_list = ['172.31.7.156','172.31.2.208','172.31.12.138','172.31.13.62']

# 5 nodes qcd-0 to qcd-4
# node_list = ['172.31.7.156','172.31.2.208','172.31.12.138','172.31.13.62','172.31.11.228']

# 6 nodes qcd-0 to qcd-5
# node_list = ['172.31.7.156','172.31.2.208','172.31.12.138','172.31.13.62','172.31.11.228','172.31.1.29']

node_list = ec2_nodes.node_list
proc_list = []

for nip in node_list:
	print('working on node (kill all): {}'.format(nip), end=',')
	rcmd = 'ssh ubuntu@{} "{}"'.format(nip,'pkill -9 rundb; pkill -9 runcl')
	proc_list.append(exec_cmd(rcmd,env,True))

wait_for(proc_list)
proc_list.clear()
print('done (kill-all)!')