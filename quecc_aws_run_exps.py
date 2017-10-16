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

def set_config(ncc_alg, wthd_cnt, theta, pt_p, ets, pa, strict):
    
    nfname = WORK_DIR+'/'+DENEVA_DIR_PREFIX+'nconfig.h'
    ofname = WORK_DIR+'/'+DENEVA_DIR_PREFIX+'config.h'
    oofname = WORK_DIR+'/'+DENEVA_DIR_PREFIX+'oconfig.h'
    nconf = open(nfname, 'w')
    oconf = open(ofname, 'r')

    if pt_p <= 1:
        pt_cnt = int(pt_p*wthd_cnt)
    else:
        pt_cnt = pt_p

    if ncc_alg == 'QUECC':
        nwthd_cnt = wthd_cnt - pt_cnt        
        part_cnt = nwthd_cnt + pt_cnt
    else:
        nwthd_cnt = wthd_cnt #ignore planner percentage now. 
        part_cnt = nwthd_cnt


    # if nwthd_cnt == 0:
        # need to account for the Abort thread
        # nwthd_cnt = wthd_cnt -1
    print('set config: CC_ALG={}, THREAD_CNT={}, ZIPF_THETA={}, PT_CNT={}, ET_CNT={}, ET_COMMIT={}, PART_CNT={}, PPT={}, STRICT_PPT={}'
        .format(ncc_alg, wthd_cnt, theta, pt_cnt, nwthd_cnt, ets, part_cnt, pa,strict))

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
        theta_m = re.search('#define ZIPF_THETA\s+(\d\.\d+)', line.strip())
        if (theta_m):
            # print(theta_m.group(1))
            nline = '#define ZIPF_THETA {}\n'.format(theta)
        pt_m = re.search('#define PLAN_THREAD_CNT\s+(\d+|THREAD_CNT)', line.strip())
        if (pt_m):
            nline = '#define PLAN_THREAD_CNT {}\n'.format(str(pt_cnt))            
        etsync_m =    re.search('#define COMMIT_BEHAVIOR\s+(IMMEDIATE|AFTER_BATCH_COMP|AFTER_PG_COMP)',line.strip())
        if etsync_m:
            nline = '#define COMMIT_BEHAVIOR {}\n'.format(ets)
        pc_m =    re.search('#define PART_CNT\s+(\d+|THREAD_CNT)',line.strip())
        if pc_m:
            nline = '#define PART_CNT {}\n'.format(part_cnt)

        pa_m =    re.search('#define PART_PER_TXN\s+(\d+|THREAD_CNT|PART_CNT)',line.strip())
        if pa_m:
            nline = '#define PART_PER_TXN {}\n'.format(pa)
        strict_m =    re.search('#define STRICT_PPT\s+(true|false)',line.strip())
        if strict_m:
            if strict:
                nline = '#define STRICT_PPT {}\n'.format('true')
            else:
                nline = '#define STRICT_PPT {}\n'.format('false')
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
    print('Error:')
    for el in p.stderr:
        print(el.decode(encoding="utf-8", errors="strict"), end='')
def exec_cmd_capture_output(cmd, env):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, env=env)
    p.wait()

    out = ""
    out += 'Output:\n'
    for ol in p.stdout:
        out += (ol.decode(encoding="utf-8", errors="strict"))
    out += 'Error:\n'
    for el in p.stderr:
        out += (el.decode(encoding="utf-8", errors="strict"))
    return out;

def build_project():
    env = dict(os.environ)
#     env['LD_LIBRARY_PATH'] = '/apps/rhel6/gcc/5.2.0/lib64:/apps/rhel6/gcc/5.2.0/lib:/apps/rhel6/gcc/5.2.0/gmp-6.1.0/lib:/apps/rhel6/gcc/5.2.0/mpc-1.0.3/lib:/apps/rhel6/gcc/5.2.0/mpfr-3.1.3/lib'
    print('Building project')
    os.chdir(WORK_DIR+'/'+DENEVA_DIR_PREFIX)
    print(os.getcwd())
    cmd = 'make clean; make -j -s'
    exec_cmd(cmd, env)

def run_trial(trial, cc_alg, env, seq_num, server_only, fnode_list, outdir, prefix):
    if is_azure:
        dbcmd = "ssh -i '~/.ssh/msa_id_rsa' qadahtm@{:s} 'cd {}; ./rundb -nid0 > {}/{}{}_{}_t{}_{}_{}.txt'" #Azure
    else:
        if not server_only:
    #         clcmd = "ssh {:s} 'cd {}; ./runcl -nid1 > {}/results/pyscript/{}_{}_t{}_{}.txt'"
            clcmd = "ssh -i 'quecc.pem' ubuntu@{:s} 'cd {}; ./runcl -nid1 > {}/{}{}_{}_t{}_{}.txt'"
        dbcmd = "ssh -i 'quecc.pem' ubuntu@{:s} 'cd {}; ./rundb -nid0 > {}/{}{}_{}_t{}_{}_{}.txt'" # AWS setup
    
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
            if not dry_run:
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

dry_run = False;
vm_cores = 64
exp_set = 1 
num_trials = 2;
# WAIT_DIE, NO_WAIT, TIMESTAMP, MVCC, CALVIN, MAAT, QUECC, DUMMY_CC,HSTORE,SILO
# cc_algs = ['NO_WAIT', 'WAIT_DIE', 'TIMESTAMP', 'MVCC','QUECC']
# cc_algs = ['WAIT_DIE', 'TIMESTAMP', 'MVCC', 'NO_WAIT']
# cc_algs = ['QUECC', 'WAIT_DIE', 'TIMESTAMP', 'MVCC']
# cc_algs = ['QUECC','HSTORE']
# cc_algs = ['HSTORE', 'SILO', 'QUECC','NO_WAIT', 'WAIT_DIE', 'TIMESTAMP', 'MVCC', 'OCC']
# cc_algs = ['HSTORE', 'SILO', 'NO_WAIT', 'WAIT_DIE', 'TIMESTAMP', 'MVCC', 'OCC', 'LADS']
# cc_algs = ['HSTORE']
# cc_algs = ['QUECC']
cc_algs = ['HSTORE']
# # cc_algs = ['LADS']
# cc_algs = ['HSTORE', 'SILO', 'CALVIN','WAIT_DIE', 'MVCC', 'OCC', 'NO_WAIT', 'QUECC', 'TIMESTAMP']
# 8,12,16,20,24,32,48,56,64,96,112,128
if (vm_cores == 32):
    wthreads = [8,12,16,20,24,32] # 32 core machine
elif vm_cores == 64:    
    wthreads = [8,16,32,48,56,64] # 64 core machine
elif vm_cores == 128:
    wthreads = [8,16,32,64,96,112,128] # 128 core machine
else:
    assert(False)

# cc_algs = ['OCC', 'NO_WAIT', 'TIMESTAMP', 'HSTORE'] # set 1
# cc_algs = ['SILO', 'WAIT_DIE', 'MVCC', 'CALVIN'] # set 2
# cc_algs = ['OCC', 'NO_WAIT', 'TIMESTAMP', 'HSTORE','SILO', 'WAIT_DIE', 'MVCC', 'CALVIN'] # both
# pt_perc = [0.25,0.5,0.75]
pt_perc = [1]
# zipftheta = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9] # 1.0 theta is not supported
# zipftheta = [0.0,0.3,0.6,0.7,0.9]
# zipftheta = [0.0,0.9]
# et_sync = ['IMMEDIATE', 'AFTER_BATCH_COMP']
# strict = [True, False]
strict = [True]
et_sync = ['AFTER_BATCH_COMP']
# zipftheta = [0.0,0.6,0.99]
# zipftheta = [0.0]
wthreads = [32] # redo experiments
zipftheta = [0.6,0.99]
# zipftheta = [0.99, 0.0]
# zipftheta = [0.0, 0.6, 0.9, 0.95, 0.99]
# parts_accessed = [1,2,4,8,16,24,32]
# parts_accessed = [1,32]
parts_accessed = [0.5]
write_perc = [0.0,0.25,0.5,0.75,1.0]
mpt_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
procs = []
seq_no = 0

#read ifconfig.txt
node_list = []

is_azure = True

with open('/quecc/deneva_project/secrets.json') as data_file:    
    secrets = json.load(data_file)

ifconfpath = DENEVA_DIR_PREFIX + "ifconfig.txt"
ifconffile = open(ifconfpath, 'r')
for line in ifconffile:
    node_list.append(line.strip())

odirname = str(time.strftime('%Y-%m-%d-%I-%M-%S-%p'))
if is_azure:
    outdir = '/home/qadahtm/results/' + odirname
else:
    outdir = '/home/ubuntu/results/' + odirname
exec_cmd('mkdir {}'.format(outdir), env)
print("Output Directory: {}".format(outdir))
stime = time.time()
prefix = ""
if (len(sys.argv) == 2):
    prefix = sys.argv[1]
exec_cmd('cat /proc/meminfo > {}/{}'.format(outdir,'meminfo.txt'), env)
exec_cmd('cat /proc/cpuinfo > {}/{}'.format(outdir,'cpuinfo.txt'), env)
exec_cmd('lscpu > {}/{}'.format(outdir,'lscpu.txt'), env)

note = "uname:\n{}\nlscpu output:\n{}\nquecc-{} with CC_ALG: {}".format(
    exec_cmd_capture_output("uname -a",env),
    exec_cmd_capture_output("lscpu",env),
    vm_cores,",".join(cc_algs))
print(note)
notefile = open("{}/{}".format(outdir,'exp_note.txt'), 'w');
notefile.write(note)
notefile.close()

if is_azure:
    print("Enter password for Azure:")
    exec_cmd('az login -u {}'.format(secrets['azlogin']), env)
    exec_cmd('az account set -s {}'.format(secrets['azsub_id']), env)
    exec_cmd('az account list', env)

for ncc_alg in cc_algs:
    for wthd in wthreads:
        for theta in zipftheta:
            runexp = True
            for pa in parts_accessed:
                # if wthd == 20  and ncc_alg != 'QUECC':
                    #Don't run other CCs with 1 thread 
                    # runexp = False

                # if wthd > 30 and ncc_alg == 'QUECC': #for m4.16xlarge
                # if wthd > 62 and ncc_alg == 'QUECC': #for x1.32xlarge
                    #Don't run QueCC with more than 30 threads 
                    # runexp = False 
                assert(pa > 0) 
                if pa < 1:
                    pa = int(wthd*pa)

                for ppts in strict:
                    exp_cnt = 0
                    for pt in pt_perc:                    
                        for ets in et_sync:
                            #use this condition if QUECC is included only
                            if ncc_alg != 'QUECC' and exp_cnt >= 1:
                                runexp = False

                            if runexp:
                                exp_cnt = exp_cnt + 1        
                                if not(ncc_alg == 'QUECC' or ncc_alg == 'LADS') and pt != 1:
                                    pt = 1
                                set_config(ncc_alg, wthd, theta, pt, ets, pa, ppts)
                                # exec_cmd('head {}'.format(DENEVA_DIR_PREFIX+'config.h'), env)
                                if not dry_run:
                                    build_project()
                                for trial in list(range(num_trials)):
                                    if pt <= 1:
                                        pt_cnt = str(int(pt*wthd))
                                        et_cnt = str(wthd-int(pt*wthd));
                                        pt_perc_str = str(int(pt*100))
                                    else:
                                        pt_cnt = str(pt)
                                        et_cnt = str(wthd-pt);
                                        pt_perc_str = str(0)
                                    
                                    if (wthd-int(pt*wthd)) == 0:
                                        et_cnt = str(wthd);

                                    if prefix != "":
                                        if ppts:
                                            nprefix = prefix + '_pa' + str(pa) + '_' + ets.replace('_','') + '_pt' + pt_cnt + '_et' + et_cnt +'_'+ pt_perc_str +'_pptstrict_' ;
                                        else:
                                            nprefix = prefix + '_pa' + str(pa) + '_' + ets.replace('_','') + '_pt' + pt_cnt + '_et' + et_cnt +'_'+ pt_perc_str +'_pptnonstrict_';  
                                    else:
                                        if ppts:
                                            nprefix = 'pa' + str(pa) + '_' + ets.replace('_','') + '_pt' + pt_cnt + '_et' + et_cnt +'_'+ pt_perc_str +'_pptstrict_';
                                        else:
                                            nprefix = 'pa' + str(pa) + '_' + ets.replace('_','') + '_pt' + pt_cnt + '_et' + et_cnt +'_'+ pt_perc_str +'_pptnonstrict_';
                                    run_trial(trial, ncc_alg, env, seq_no, True, node_list, outdir, nprefix)                            
                                    # print('Dry run: {}, {}, {}, t{}, {}'
                                    #     .format(ncc_alg, str(wthd), str(theta), str(trial), nprefix))
                                    seq_no = seq_no + 1
                                cfgfname = WORK_DIR+'/'+DENEVA_DIR_PREFIX+'config.h'
                                cfg_copy = '{}/{}{}_config.h'.format(outdir, nprefix, ncc_alg.replace('_',''))
                                # print("cp {} {}".format(cfgfname, cfg_copy))
                                exec_cmd("cp {} {}".format(cfgfname, cfg_copy), env)
# res = get_df_csv(outdir)
eltime = time.time() - stime
subject = 'Experiment done in {}, results at {}'.format(str(timedelta(seconds=eltime)), odirname)
print(subject)
send_email(subject, note)
# if not dry_run:
#     if is_azure:
#         exec_cmd('az vm deallocate -g quecc -n {}'.format(secrets['vm_name']), env)
#     else:
#         exec_cmd('sudo shutdown -h now', env)
