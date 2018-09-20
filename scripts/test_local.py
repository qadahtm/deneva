#!/usr/bin/env python3

import os, sys, re, math, os.path

print("Testing local machine")

cwdir = os.getcwd();

print("Working directory: {}".format(cwdir))

ifconfigfile = "ifconfig.txt"

if not os.path.exists(ifconfigfile):
    print("Creating ifconfig.txt file")
    with open(ifconfigfile,'w') as fout:
        fout.write("127.0.0.1\n")
        fout.write("127.0.0.1\n")
        fout.write("127.0.0.1\n")
else:
    # print("backup existing file")
    # targetfile = ifconfigfile;
    # os.rename(os.path.realpath(targetfile), os.path.realpath(targetfile)+".bak")
    print("ifconfig exist!!")


cmd = "cat {}".format(ifconfigfile)
os.system(cmd)

cmd = "bin/rundb -nid0 &"
os.system(cmd)

cmd = "bin/rundb -nid1 &"
os.system(cmd)

cmd = "bin/runcl -nid2 &"
os.system(cmd)