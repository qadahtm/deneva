#!/usr/bin/env python

import os
import sys
import socket
import re
import shlex, subprocess
import smtplib
import shutil
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import json
from pprint import pprint
import time
from datetime import timedelta
import multiprocessing

def set_config(ncc_alg, wthd_cnt, theta, pt_p, ets):
    
    nfname = WORK_DIR+'/'+DENEVA_DIR_PREFIX+'nconfig.h'
    ofname = WORK_DIR+'/'+DENEVA_DIR_PREFIX+'config.h'
    oofname = WORK_DIR+'/'+DENEVA_DIR_PREFIX+'oconfig.h'
    nconf = open(nfname, 'w')
    oconf = open(ofname, 'r')

    pt_cnt = int(pt_p*wthd_cnt)
    nwthd_cnt = wthd_cnt - pt_cnt
    if nwthd_cnt == 0:
        nwthd_cnt = wthd_cnt
    print('set config: CC_ALG={}, THREAD_CNT={}, ZIPF_THETA={}, PT_CNT={}, ET_CNT={}, ET_COMMIT={}'
        .format(ncc_alg, wthd_cnt, theta, pt_cnt, nwthd_cnt, ets))

    for line in oconf:
    #     print(line, end='')
        nline = line
        #change worker threads        
        m = re.search('#define THREAD_CNT\s+(\d+)', line.strip())
        if m:
            # print(m.group(1))
            nline = '#define THREAD_CNT {}\n'.format(nwthd_cnt)
        #changing cc_alg
        ccalg_m = re.search('#define CC_ALG\s+(\S+)', line.strip())
        if (ccalg_m):
            # print(ccalg_m.group(1))
            nline = '#define CC_ALG {}\n'.format(ncc_alg)
        theta_m = re.search('#define ZIPF_THETA\s+(\d\.\d)', line.strip())
        if (theta_m):
            # print(theta_m.group(1))
            nline = '#define ZIPF_THETA {}\n'.format(theta)
        pt_m = re.search('#define PLAN_THREAD_CNT\s+(\d+|THREAD_CNT)', line.strip())
        if (pt_m):
            nline = '#define PLAN_THREAD_CNT {}\n'.format(str(int(pt_p*wthd_cnt)))
        etsync_m =    re.search('#define COMMIT_BEHAVIOR\s+(IMMEDIATE|AFTER_BATCH_COMP|AFTER_PG_COMP)',line.strip())
        if etsync_m:
             nline = '#define COMMIT_BEHAVIOR {}\n'.format(ets)
        nconf.write(nline)
    nconf.close()
    oconf.close()
    shutil.move(ofname, oofname)
    shutil.move(nfname, ofname)
    
def exec_cmd(cmd, env):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, env=env)
    p.wait()
    print('Output:')
    for ol in p.stdout:
        print(ol.decode(encoding="utf-8", errors="strict"), end='')
    # print('Error:')
    # for el in p.stderr:
        # print(el.decode(encoding="utf-8", errors="strict"), end='')

def build_project():
    env = dict(os.environ)
#     env['LD_LIBRARY_PATH'] = '/apps/rhel6/gcc/5.2.0/lib64:/apps/rhel6/gcc/5.2.0/lib:/apps/rhel6/gcc/5.2.0/gmp-6.1.0/lib:/apps/rhel6/gcc/5.2.0/mpc-1.0.3/lib:/apps/rhel6/gcc/5.2.0/mpfr-3.1.3/lib'
    print('Building project')
    os.chdir(WORK_DIR+'/'+DENEVA_DIR_PREFIX)
    print(os.getcwd())
    cmd = 'make clean; make -j -s'
    exec_cmd(cmd, env)

def run_trial(trial, cc_alg, env, seq_num, server_only, fnode_list, outdir, prefix):
    if not server_only:
#         clcmd = "ssh {:s} 'cd {}; ./runcl -nid1 > {}/results/pyscript/{}_{}_t{}_{}.txt'"
        clcmd = "ssh -i 'quecc.pem' ubuntu@{:s} 'cd {}; ./runcl -nid1 > {}/{}{}_{}_t{}_{}.txt'"
    dbcmd = "ssh -i 'quecc.pem' ubuntu@{:s} 'cd {}; ./rundb -nid0 > {}/{}{}_{}_t{}_{}_{}.txt'"
    for i in range(ip_cnt):
        #run processes
        if (i < S_NODE_CNT):
            #run a server process
#             print("server {}".format(fnode_list[i]['ip']))
            core_cnt = multiprocessing.cpu_count();
            print("server {}".format(fnode_list[i]))
            fscmd = dbcmd.format(fnode_list[i],
                                 WORK_DIR+'/'+DENEVA_DIR_PREFIX,
                                 outdir,
                                 prefix,
                                 # WORK_DIR, 
                                 cc_alg.replace('_',''), 's', trial, seq_num, core_cnt)
            print(fscmd)
            p = subprocess.Popen(fscmd, stdout=subprocess.PIPE, env=env, shell=True)
            procs.append(p);
        else:
            if not server_only:
                #run a client process
                print("Client {}".format(fnode_list[i]))
                fscmd = clcmd.format(fnode_list[i],
                                     WORK_DIR+'/'+DENEVA_DIR_PREFIX,
                                     outdir,
                                     prefix,
                                     # WORK_DIR, 
                                     cc_alg.replace('_',''), 'c', trial, seq_num)
                print(fscmd)
                p = subprocess.Popen(fscmd, stdout=subprocess.PIPE, env=env, shell=True)
                procs.append(p);
    for p in procs:
        p.wait()
    print("Done Trial {}".format(trial))

def send_email(subject, msg):
    with open('/quecc/deneva_project/secrets.json') as data_file:    
        secrets = json.load(data_file)
    fromaddr = 'tq.autosender@gmail.com'
    toaddrs  = 'qadah.thamir@gmail.com'
    rmsg = "\r\n".join([
      "From: tq.autosender@gmail.com",
      "To: qadah.thamir@gmail.com",
      "Subject: {}".format(subject),
      "",
      msg
      ])
    username = secrets['uname']
    password = secrets['password']
    server = smtplib.SMTP(secrets['smtp_server_uri'])
    server.ehlo()
    server.starttls()
    server.login(username,password)
    server.sendmail(fromaddr, toaddrs, rmsg)
    server.quit()
    
def get_df_csv(outdir):
    # res_dir = WORK_DIR + '/results/pyscript'
    res_dir = outdir
    resFiles = sorted(os.listdir(res_dir))
    e_data = {'wthd_cnt':[],
              'trial_no':[],
              'tput':[],
              'cc_alg':[],
              'zipf_theta':[],
              'max_txn_inflight':[],
              'send_thd_cnt':[],
              'recv_thd_cnt':[],
              'total_thd_cnt':[]
             }
    for fname in resFiles:
        mf = re.search('(\S+)\_s\_t(\d+)_(\d+)\.txt', fname)
        if mf:
            print(fname)
            cc_alg = mf.group(1)
            thd_cnt = -1
            tput = 0
            trial = mf.group(2)
            tif = 0
            send_thd = -1
            recv_thd = -1
            total_thd =0
            with open(res_dir+'/'+fname, 'r') as of:
                for line in of:
    #                 g_thread_cnt 8
                    m_thd_cnt = re.search('g_thread_cnt\s+(\d+)', line)
                    if m_thd_cnt:
                        if thd_cnt < 0:
                            thd_cnt = int(m_thd_cnt.group(1))
                    #g_inflight_max 1000000
                    mv = re.search('g_inflight_max\s+(\d+)', line)
                    if mv:
                        tif = int(mv.group(1))
                    #g_rem_thread_cnt 2
                    mv = re.search('g_rem_thread_cnt\s+(\d+)', line)
                    if mv:
                        recv_thd = int(mv.group(1))
                    # g_send_thread_cnt 2
                    mv = re.search('g_send_thread_cnt\s+(\d+)', line)
                    if mv:
                        send_thd = int(mv.group(1))
                        # g_total_thread_cnt 13
                    mv = re.search('g_total_thread_cnt\s+(\d+)', line)
                    if mv:
                        total_thd = int(mv.group(1))
                    m_sum = re.search('(\[summary\]) (.+)', line)
                    if m_sum:
                        sline = m_sum.group(2).split(',')
                        for i, a in enumerate(sline):
                            mq = re.search('tput=(\d+\.?\d*)', a)
                            if mq:
                                tput = float(mq.group(1))

            e_data['wthd_cnt'].append(thd_cnt)
            e_data['trial_no'].append(trial)
            e_data['cc_alg'].append(cc_alg)
            e_data['max_txn_inflight'].append(tif)
            e_data['tput'].append(tput)
            e_data['send_thd_cnt'].append(send_thd)
            e_data['recv_thd_cnt'].append(recv_thd)
            e_data['total_thd_cnt'].append(total_thd)
    df = pd.DataFrame(e_data)
    return df.to_csv()

DENEVA_DIR_PREFIX = 'deneva/'
HOME_DIR = '/home/tqadah'
WORK_DIR = '/quecc/deneva_project' #AWS
os.chdir(WORK_DIR)

#get node names from 'hostnames' file which should be created by 'uniqu $PBS_NODEFILE > hostnames"
nodeips = []

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
        
print("Server node count = {:d}".format(S_NODE_CNT))
print("Server core count = {:d}".format(S_CORE_CNT))
print("Client node count = {:d}".format(C_NODE_CNT))
print("Client core count = {:d}".format(C_CORE_CNT))
print("Cluster node count = {:d}".format(CLUSTER_NODE_CNT))


#create ifconfig.txt
ip_cnt = S_NODE_CNT + C_NODE_CNT
print("Number of ips = {:d}".format(ip_cnt))

env = dict(os.environ)

num_trials = 2;
# WAIT_DIE, NO_WAIT, TIMESTAMP, MVCC, CALVIN, MAAT, QUECC, DUMMY_CC
# cc_algs = ['QUECC', 'NO_WAIT', 'WAIT_DIE', 'TIMESTAMP', 'MVCC']
# cc_algs = ['WAIT_DIE', 'TIMESTAMP', 'MVCC', 'NO_WAIT']
cc_algs = ['QUECC']
# wthreads = [4,8,12,16,20,24,28,30,32,40,44,48,52,56,60] # for m4.16xlarge
#8 data points
# wthreads = [20,40] # for m4.16xlarge all
# wthreads = [8,16,20,24,30,48,56,60] # for m4.16xlarge non-Quecc
# wthreads = [16,24,32,40,48,56,60] # for m4.16xlarge for QueCC
# wthreads = [40,48,56,60] # for m4.16xlarge for QueCC
wthreads = [16,24,32,36] # for m4.10xlarge for QueCC
# wthreads = [16] # for m4.10xlarge for QueCC
# pt_perc = [0.25,0.5,0.75, 1]
pt_perc = [0.25, 0.5]
# pt_perc = [0.5]
# wthreads = [16,32,48,62,80,96,112,124] # x1.32xlarge for non-Quecc
# wthreads = [8,16,24,31,40,48,56,62] # x1.32xlarge for Quecc
# wthreads = [1,2]
# zipftheta = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9] # 1.0 theta is not supported
# zipftheta = [0.0,0.3,0.6,0.7,0.9]
# zipftheta = [0.0,0.9]
# et_sync = ['IMMEDIATE', 'AFTER_BATCH_COMP']
et_sync = ['AFTER_BATCH_COMP']
zipftheta = [0.0]
# zipftheta = [0.0]
write_perc = [0.0,0.25,0.5,0.75,1.0]
mpt_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
procs = []
seq_no = 0

#read ifconfig.txt
node_list = []
ifconfpath = DENEVA_DIR_PREFIX + "ifconfig.txt"
ifconffile = open(ifconfpath, 'r')
for line in ifconffile:
    node_list.append(line.strip())

odirname = str(time.strftime('%Y-%m-%d-%I-%M-%S-%p'))
outdir = '/home/ubuntu/results/' + odirname
exec_cmd('mkdir {}'.format(outdir), env)
print("Output Directory: {}".format(outdir))
stime = time.time()
prefix = ""
if (len(sys.argv) == 2):
    prefix = sys.argv[1]
exec_cmd('cat /proc/meminfo > {}/{}'.format(outdir,'meminfo.out'), env)
exec_cmd('cat /proc/cpuinfo > {}/{}'.format(outdir,'cpuinfo.out'), env)
exec_cmd('lscpu > {}/{}'.format(outdir,'lscpu.out'), env)

for ncc_alg in cc_algs:
    for wthd in wthreads:
        for theta in zipftheta:
            runexp = True
            # if wthd == 20  and ncc_alg != 'QUECC':
                #Don't run other CCs with 1 thread 
                # runexp = False

            # if wthd > 30 and ncc_alg == 'QUECC': #for m4.16xlarge
            # if wthd > 62 and ncc_alg == 'QUECC': #for x1.32xlarge
                #Don't run QueCC with more than 30 threads 
                # runexp = False
            exp_cnt = 0
            for pt in pt_perc:
                for ets in et_sync:
                    if ncc_alg != 'QUECC' and exp_cnt >= 1:
                        runexp = False

                    if runexp:
                        exp_cnt = exp_cnt + 1
                        set_config(ncc_alg, wthd, theta, pt, ets)
                        # exec_cmd('head {}'.format(DENEVA_DIR_PREFIX+'config.h'), env)
                        build_project()
                        for trial in list(range(num_trials)):
                            pt_cnt = str(int(pt*wthd))
                            pt_perc_str = str(int(pt*100))
                            et_cnt = str(wthd-int(pt*wthd));
                            if (wthd-int(pt*wthd)) == 0:
                                et_cnt = str(wthd);
                            if prefix != "":
                                nprefix = prefix + '_' + ets + '_pt' + pt_cnt + '_et' + et_cnt +'_'+ pt_perc_str +'_';
                            else:
                                nprefix = ets.replace('_','') + '_pt' + pt_cnt + '_et' + et_cnt +'_'+ pt_perc_str +'_';
                            run_trial(trial, ncc_alg, env, seq_no, True, node_list, outdir, nprefix)                            
                            # print('Dry run: {}, {}, {}, t{}, {}'
                                # .format(ncc_alg, str(wthd), str(theta), str(trial), nprefix))
                            seq_no = seq_no + 1           
# res = get_df_csv(outdir)
eltime = time.time() - stime
subject = 'Experiment done in {}, results at {}'.format(str(timedelta(seconds=eltime)), odirname)
# print(subject)
send_email(subject, '')
exec_cmd('sudo shutdown -h now', env)
