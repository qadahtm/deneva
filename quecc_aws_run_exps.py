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
from termcolor import colored, cprint

def set_config(ncc_alg, wthd_cnt, theta, pt_p, bs, pa, strict, oppt,mprv,wp, maxtpp, m_strat,rs,ct_p,bmap_len):
    
    nfname = WORK_DIR+'/'+DENEVA_DIR_PREFIX+'nconfig.h'
    ofname = WORK_DIR+'/'+DENEVA_DIR_PREFIX+'config.h'
    oofname = WORK_DIR+'/'+DENEVA_DIR_PREFIX+'oconfig.h'
    nconf = open(nfname, 'w')
    oconf = open(ofname, 'r')

    if pt_p <= 1:
        pt_cnt = int(pt_p*wthd_cnt)
    else:
        pt_cnt = pt_p

    # bmap_len = 2
    if ncc_alg == 'QUECC':
        nwthd_cnt = wthd_cnt - pt_cnt 
        part_cnt = nwthd_cnt + pt_cnt
        piplined = 'true'
        abort_queues = 'true'
        abort_thread = 'false'
        if (nwthd_cnt == 0):
            nwthd_cnt = wthd_cnt     
            part_cnt = wthd_cnt  
            piplined = 'false'
            bmap_len = 1 # if unpipelined mode, use length of 1
        
    else:
        nwthd_cnt = wthd_cnt #ignore planner percentage now. 
        part_cnt = nwthd_cnt
        piplined = 'true'
        abort_queues = 'false'
        abort_thread = 'true'

    if ct_p < 1:
        ct_cnt = int(ct_p*nwthd_cnt)
    else:
        ct_cnt = ct_p

    if ct_p == 0:
        ct_cnt = nwthd_cnt

    if pa == 0:
        pa = int(part_cnt)

    maxtpp = int(maxtpp/part_cnt)
    part_cnt = 1 # use for single partition exps

    # if nwthd_cnt == 0:
        # need to account for the Abort thread
        # nwthd_cnt = wthd_cnt -1
    if is_ycsb:
        msg_out = 'set config(YCSB): CC_ALG={}, THREAD_CNT={}, ZIPF_THETA={}, PT_CNT={}, ET_CNT={}, BATCH_SIZE={}, PART_CNT={}, PPT={}, STRICT_PPT={}, REQ_PER_QUERY={}, MPR={}, WRITE_PERC={}, MAXTPP={}, MERGE_STRAT={}, REC_SIZE={}, BATCH_MAP_LENGTH={}, CT_PERC={}, CT_CNT={}'.format(ncc_alg, wthd_cnt, theta, pt_cnt, nwthd_cnt, bs, part_cnt, pa,strict,oppt, mprv, wp, maxtpp, m_strat, int(rs*100),bmap_len,ct_p, ct_cnt)    
    else:
        msg_out = 'set config(TPC-C): CC_ALG={}, THREAD_CNT={}, PAYMENT_PERC={}, PT_CNT={}, ET_CNT={}, BATCH_SIZE={}, PART_CNT={}, PPT={}, STRICT_PPT={}, MPR={}, MAXTPP={}, MERGE_STRAT={}, BATCH_MAP_LENGTH={}, CT_PERC={}, CT_CNT={}'.format(ncc_alg, wthd_cnt, wp, pt_cnt, nwthd_cnt, bs, part_cnt, pa,strict,mprv, maxtpp, m_strat,bmap_len,ct_p, ct_cnt)
    print(msg_out)

    for line in oconf:
    #     print(line, end='')
        nline = line
        #change worker threads        
        m = re.search('#define THREAD_CNT\s+(\d+)', line.strip())
        if m:
            # print(m.group(1))
            nline = '#define THREAD_CNT {}\n'.format(nwthd_cnt)

        if ct_cnt == nwthd_cnt:
            m =    re.search('#define FIXED_COMMIT_THREAD_CNT\s+(true|false)',line.strip())
            if m:
                nline = '#define FIXED_COMMIT_THREAD_CNT {}\n'.format('false')
        else:
            m =    re.search('#define FIXED_COMMIT_THREAD_CNT\s+(true|false)',line.strip())
            if m:
                nline = '#define FIXED_COMMIT_THREAD_CNT {}\n'.format('true')
            m =    re.search('#define COMMIT_THREAD_CNT\s+(\d+)',line.strip())
            if m:
                nline = '#define COMMIT_THREAD_CNT {}\n'.format(ct_cnt)
        #changing cc_alg
        ccalg_m = re.search('#define CC_ALG\s+(\S+)', line.strip())
        if (ccalg_m):
            nline = '#define CC_ALG {}\n'.format(ncc_alg)
        theta_m = re.search('#define ZIPF_THETA\s+(\d\.\d+)', line.strip())
        if (theta_m):
            nline = '#define ZIPF_THETA {}\n'.format(theta)

        if is_ycsb:
            m = re.search('#define WRITE_PERC\s+(\d\.\d+)', line.strip())
            if (m):
                nline = '#define WRITE_PERC {}\n'.format(wp)
        else:
            m = re.search('#define PERC_PAYMENT\s+(\d\.\d+)', line.strip())
            if (m):
                nline = '#define PERC_PAYMENT {}\n'.format(wp)

        m = re.search('#define MPR\s+(\d\.\d+)', line.strip())
        if (m):
            nline = '#define MPR {}\n'.format(mprv)

        pt_m = re.search('#define PLAN_THREAD_CNT\s+(\d+|THREAD_CNT)', line.strip())
        if (pt_m):
            nline = '#define PLAN_THREAD_CNT {}\n'.format(str(pt_cnt)) 

        pt_m = re.search('#define NUM_WH\s+(\d+|THREAD_CNT)', line.strip())
        if (pt_m):
            nline = '#define NUM_WH {}\n'.format(str(wthd_cnt))   
            print("NUM_WH={}".format(str(wthd_cnt)))
            
        m =    re.search('#define MAX_TXN_PER_PART\s+(\d+)',line.strip())
        if m:
            nline = '#define MAX_TXN_PER_PART {}\n'.format(maxtpp)

        m =    re.search('#define EXECQ_CAP_FACTOR\s+(\d+)',line.strip())
        if m:
            nline = '#define EXECQ_CAP_FACTOR {}\n'.format(eq_cap_factor)

        etsync_m =    re.search('#define BATCH_SIZE\s+(\d+)',line.strip())
        if etsync_m:
            nline = '#define BATCH_SIZE {}\n'.format(bs)
        pc_m =    re.search('#define PART_CNT\s+(\d+|THREAD_CNT)',line.strip())
        if pc_m:
            nline = '#define PART_CNT {}\n'.format(part_cnt)

        pc_m =    re.search('#define CORE_CNT\s+(\d+|THREAD_CNT)',line.strip())
        if pc_m:
            nline = '#define CORE_CNT {}\n'.format(vm_cores)

        pc_m =    re.search('#define BATCH_MAP_LENGTH\s+(\d+)',line.strip())
        if pc_m:
            nline = '#define BATCH_MAP_LENGTH {}\n'.format(bmap_len)

        pc_m =    re.search('#define REQ_PER_QUERY\s+(\d+)',line.strip())
        if pc_m and is_ycsb:
            nline = '#define REQ_PER_QUERY {}\n'.format(oppt)



        # pc_m =    re.search('#define MAX_TUPLE_SIZE\s+(\d+)',line.strip())
        # if pc_m and is_ycsb:
        #     nline = '#define MAX_TUPLE_SIZE {}\n'.format(int(rs*100))

        # pc_m =    re.search('#define FIELD_PER_TUPLE\s+(\d+)',line.strip())
        # if pc_m and is_ycsb:
        #     nline = '#define FIELD_PER_TUPLE {}\n'.format(ycsb_field_per_tuple)

        m =    re.search('#define MODE\s+(NORMAL_MODE|FIXED_MODE)',line.strip())
        if m:
            if is_fixed_mode:
                nline = '#define MODE FIXED_MODE\n'
            else:
                nline = '#define MODE NORMAL_MODE\n'

        m =    re.search('#define SIM_BATCH_CNT\s+(\d+)',line.strip())
        if m and is_fixed_mode:
            nline = '#define SIM_BATCH_CNT {}\n'.format(sim_batch_cnt)


        m =    re.search('#define MERGE_STRATEGY\s+(RR|BALANCE_EQ_SIZE)',line.strip())
        if m:
            nline = '#define MERGE_STRATEGY {}\n'.format(m_strat)

        m =    re.search('#define ABORT_THREAD\s+(true|false)',line.strip())
        if m:
            nline = '#define ABORT_THREAD {}\n'.format(abort_thread)

        m =    re.search('#define ABORT_QUEUES\s+(true|false)',line.strip())
        if m:
            nline = '#define ABORT_QUEUES {}\n'.format(abort_queues)


        m =    re.search('#define PIPELINED\s+(true|false)',line.strip())
        if m:
            nline = '#define PIPELINED {}\n'.format(piplined)

        m =    re.search('#define WORKLOAD\s+(YCSB|TPCC|PPS)',line.strip())
        if m:
            if is_ycsb:
                nline = '#define WORKLOAD YCSB\n'
            else: 
                nline = '#define WORKLOAD TPCC\n'

        m =    re.search('#define PROFILE_EXEC_TIMING\s+(true|false)',line.strip())
        if m:
            if time_enable:
                nline = '#define PROFILE_EXEC_TIMING {}\n'.format('true')
            else:
                nline = '#define PROFILE_EXEC_TIMING {}\n'.format('false')
        m =    re.search('#define PROG_STATS\s+(true|false)',line.strip())
        if m:
            nline = '#define PROG_STATS {}\n'.format('false')

        m =    re.search('#define DEBUG_QUECC\s+(true|false)',line.strip())
        if m:
            nline = '#define DEBUG_QUECC {}\n'.format('false')
 
        pa_m =    re.search('#define PART_PER_TXN\s+(\d+|THREAD_CNT|PART_CNT)',line.strip())
        if pa_m:
            nline = '#define PART_PER_TXN {}\n'.format(pa)
        strict_m =    re.search('#define STRICT_PPT\s+(true|false)',line.strip())
        if strict_m:
            if strict:
                nline = '#define STRICT_PPT {}\n'.format('true')
            else:
                nline = '#define STRICT_PPT {}\n'.format('false')
        m = re.search('#define CPU_FREQ\s+(\d+\.\d+)',line.strip())
        if m:
            if vm_cores == 32:
                nline = '#define CPU_FREQ {}\n'.format('2.0') #based on lscpu
            elif vm_cores == 64 or vm_cores == 128:
                nline = '#define CPU_FREQ {}\n'.format('2.5') #based on lscpu
            else:
                nline = line;
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
    
    # for i in range(ip_cnt):
        #run processes
        # if (i < S_NODE_CNT):
            #run a server process
#             print("server {}".format(fnode_list[i]['ip']))
    core_cnt = multiprocessing.cpu_count();
    print("server {}".format(fnode_list[0]))
    fscmd = dbcmd.format(fnode_list[0],
                         WORK_DIR+'/'+DENEVA_DIR_PREFIX,
                         outdir,
                         prefix,
                         # WORK_DIR, 
                         cc_alg.replace('_',''), 's', trial, seq_num, core_cnt)    
    max_retries = 10
    tries = 0
    while True:
        tries = tries + 1
        if tries == max_retries:
            print("could not execute trial {} successfully, retry count = {}. deleting output file and moving on.".format(trial, tries))
            exec_cmd('rm {}/{}{}_{}_t{}_{}_{}.txt'.format(outdir,
                             prefix,                         
                             cc_alg.replace('_',''), 's', trial, seq_num, core_cnt),env)  
            break
        print(fscmd)

        if not dry_run:
            estime = time.time()
            sp = subprocess.Popen(fscmd, stdout=subprocess.PIPE, env=env, shell=True)
            # procs.append(sp);
            # else:
            #     if not server_only:
            #         #run a client process
            #         print("Client {}".format(fnode_list[i]))
            #         fscmd = clcmd.format(fnode_list[i],
            #                              WORK_DIR+'/'+DENEVA_DIR_PREFIX,
            #                              outdir,
            #                              prefix,
            #                              # WORK_DIR, 
            #                              cc_alg.replace('_',''), 'c', trial, seq_num)
            #         print(fscmd)
            #         p = subprocess.Popen(fscmd, stdout=subprocess.PIPE, env=env, shell=True)
            #         procs.append(p);
        # for p in procs:
        #     p.wait(10)
        #Wait for for it to finish
            try:
                outs, errs = sp.communicate(timeout=300)            
                if sp.returncode != 0:
                    raise subprocess.CalledProcessError(cmd=fscmd,returncode=sp.returncode)
                print("Done Trial {}, rc = {}, elapsed time = {}".format(trial, sp.returncode,str(timedelta(seconds=(time.time()-estime)))))
                break

            except subprocess.TimeoutExpired:
                exec_cmd('pkill -9 rundb', env);
                exec_cmd('rm {}/{}{}_{}_t{}_{}_{}.txt'.format(outdir,
                                 prefix,                         
                                 cc_alg.replace('_',''), 's', trial, seq_num, core_cnt),env)        
                print("Timeout - Trial {}, rc = {}, elapsed time = {}, retrying {} ...".format(trial, sp.returncode, str(timedelta(seconds=(time.time()-estime))), tries))     
            except subprocess.CalledProcessError:
                exec_cmd('rm {}/{}{}_{}_t{}_{}_{}.txt'.format(outdir,
                                 prefix,                         
                                 cc_alg.replace('_',''), 's', trial, seq_num, core_cnt),env)        
                print("RC Error - Trial {}, rc = {}, retrying {} ...".format(trial, sp.returncode, tries))     
        else: 
            break


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

def set_ycsb_schema(rs):
    ycsb_schema = "//size, type, name\nTABLE=MAIN_TABLE\n"

    if rs <= 1:
        frs = 1
        for i in range(frs):
            ycsb_schema += "\t{},string,F{}\n".format(int(rs*100),i)
    else:   
        frs = rs
        for i in range(frs):
            ycsb_schema += "\t{},string,F{}\n".format(int(100),i)
    ycsb_schema += "\nINDEX=MAIN_INDEX\n"
    ycsb_schema += "\tMAIN_TABLE,0\n"
    print(ycsb_schema)
    sfname = WORK_DIR+'/'+DENEVA_DIR_PREFIX+'/benchmarks/YCSB_schema.txt'
    schema_file = open(sfname, 'w')
    schema_file.write(ycsb_schema)
    schema_file.close()

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

time_enable = True;
dry_run = False;
vm_shut = False;

is_ycsb = True # if false workload is TPCC

is_azure = True

is_fixed_mode = False

vm_cores = multiprocessing.cpu_count();
exp_set = 1 

# eq_cap_factor = 20; # for TPCC 0% payment
# eq_cap_factor = 10; # for TPCC 50% payment
eq_cap_factor = 1; # for TPCC 100% payment

# num_trials = 2;
# WAIT_DIE, NO_WAIT, TIMESTAMP, MVCC, CALVIN, MAAT, QUECC, DUMMY_CC,HSTORE,SILO
# cc_algs = ['NO_WAIT', 'WAIT_DIE', 'TIMESTAMP', 'MVCC','QUECC']
# cc_algs = ['WAIT_DIE', 'TIMESTAMP', 'MVCC', 'NO_WAIT']
# cc_algs = ['QUECC', 'WAIT_DIE', 'TIMESTAMP', 'MVCC']
# cc_algs = ['QUECC','HSTORE']
# cc_algs = ['HSTORE', 'SILO', 'QUECC','NO_WAIT', 'WAIT_DIE', 'TIMESTAMP', 'MVCC', 'OCC']
# cc_algs = ['HSTORE', 'SILO', 'NO_WAIT', 'WAIT_DIE', 'TIMESTAMP', 'MVCC', 'OCC', 'LADS']
# cc_algs = ['SILO']
# cc_algs = ['QUECC']
# cc_algs = ['CALVIN']
# cc_algs = ['MVCC']
# cc_algs = ['HSTORE']
# # cc_algs = ['LADS']
# cc_algs = ['HSTORE', 'SILO', 'CALVIN','WAIT_DIE', 'MVCC', 'OCC', 'NO_WAIT', 'QUECC', 'TIMESTAMP']
# 8,12,16,20,24,32,48,56,64,96,112,128
if (vm_cores == 32):
    # wthreads = [8,12,16,20,24,32] # 32 core machine
    # wthreads = [8,16,20,24,32] # 32 core machine - 12 cores gives problems
    wthreads = [4,8,16,24,32] # 32 core machine - 12 cores gives problems
elif vm_cores == 64:    
    wthreads = [8,16,32,48,56,64] # 64 core machine
elif vm_cores == 128:
    wthreads = [8,16,32,64,96,112,128] # 128 core machine
else:
    wthreads = [vm_cores]

# cc_algs = ['OCC', 'NO_WAIT', 'TIMESTAMP', 'HSTORE'] # set 1
# cc_algs = ['SILO', 'WAIT_DIE', 'MVCC'] # set 2
# cc_algs = ['OCC', 'NO_WAIT', 'TIMESTAMP', 'HSTORE','SILO', 'WAIT_DIE', 'MVCC','CALVIN'] # both
# cc_algs = ['OCC', 'TIMESTAMP', 'HSTORE','SILO', 'WAIT_DIE', 'MVCC','CALVIN'] # both
# cc_algs = ['OCC', 'NO_WAIT', 'TIMESTAMP', 'HSTORE','SILO', 'WAIT_DIE', 'MVCC'] # both
# pt_perc = [0.25,0.5,0.75,1]
# pt_perc = [0.25,0.5]
# pt_perc = [0.75,1]
# pt_perc = [0.5,1]
# pt_perc = [1]
# zipftheta = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9] # 1.0 theta is not supported
# zipftheta = [0.0,0.3,0.6,0.7,0.9]
# zipftheta = [0.0,0.9]
# et_sync = ['IMMEDIATE', 'AFTER_BATCH_COMP']
# strict = [True, False]
strict = [True]
et_sync = ['AFTER_BATCH_COMP']

wthreads = [vm_cores]
# wthreads = [4] # redo experiments
num_trials = 2
# cc_algs = ['SILO']
# cc_algs = ['NO_WAIT']
# cc_algs = ['MVCC'] 
# cc_algs = ['TIMESTAMP']  
# cc_algs = ['QUECC']  
# cc_algs = ['HSTORE']
# cc_algs = ['MVCC','WAIT_DIE','TIMESTAMP'] #algorithms that uses timestamp allocation  
# cc_algs = ['NO_WAIT', 'SILO'] 
# cc_algs = ['OCC'] 
# cc_algs = ['OCC', 'NO_WAIT', 'TIMESTAMP', 'HSTORE','SILO', 'WAIT_DIE', 'MVCC','QUECC']
# cc_algs = ['OCC', 'NO_WAIT', 'TIMESTAMP', 'SILO', 'WAIT_DIE', 'MVCC'] # others
cc_algs = ['NO_WAIT', 'TIMESTAMP', 'SILO', 'WAIT_DIE', 'MVCC'] # others but OCC
# cc_algs = ['OCC', 'NO_WAIT', 'TIMESTAMP', 'SILO', 'WAIT_DIE', 'QUECC'] 
# cc_algs = ['OCC', 'NO_WAIT', 'TIMESTAMP', 'WAIT_DIE'] 
# cc_algs = ['OCC', 'NO_WAIT', 'TIMESTAMP', 'SILO', 'WAIT_DIE', 'MVCC','QUECC'] # + QueCC
# cc_algs = ['SILO', 'NO_WAIT','QUECC'] # set 11
# cc_algs = ['TIMESTAMP', 'HSTORE'] # set 12
#Common parameters
# merge_strats = ['RR','BALANCE_EQ_SIZE'] # for Quecc Only - set ot a single value if using others
merge_strats = ['BALANCE_EQ_SIZE'] # for Quecc Only - set ot a single value if using others
# mtpps = [100000,int(100000/vm_cores)] # MAX_TXN_PER_PART
# mtpps = [int(100000/vm_cores)] # MAX_TXN_PER_PART
# mtpps = [int(100000/vm_cores),int(500000/vm_cores),int(1600000/vm_cores)] # MAX_TXN_PER_PART
# mtpps = [int(500000/vm_cores)] # MAX_TXN_PER_PART
mtpps = [int(500000)] # MAX_TXN_PER_PART

# batch_sized = [5184,10368,20736,41472,82944]
# batch_sized = [5184] // this causes problems, so we ommit it
# batch_sized = [10368,20736,41472,82944]
# batch_sized = [41472]
# batch_sized = [40320]
# batch_sized = [8192,10368]
batch_sized = [10368]
# batch_sized = [10080] # for ptvar lcm(2,4,5,6,8,10,12,15,16,18,20,24,32)
# batch_sized = [13440,26880,40320,53760,107520]
# batch_sized = [13440,26880,53760,107520]
# batch_sized = [5184,10368,20736,41472,82944,165888]
# batch_sized = [1024,2048,4096,5184,8192,10368,20736,41472,82944]
# batch_sized = [1024,2048,4096,8192,10368,20736,41472,82944]

# pt_perc = [0.25,0.5,0.75,1]
# pt_perc = [0.25,0.5,0.75]
# pt_perc = [0.25]
# pt_perc = [0.5,1]
# pt_perc = [0.5,0.25,1]
pt_perc = [1]

#ratio of commit threads from execution threads
# ct_perc = [0.25,0.5,1]
# ct_perc = [0.25,1]
# ct_perc = [0.5,1]
# ct_perc = [0.25]
# ct_perc = [0.5]
# ct_perc = [1]
ct_perc = [0] # zero means access all ETs will be CTs

# parts_accessed = [1,32]
# parts_accessed = [1,2,4,8,10]
# parts_accessed = [1,2,4,8,10,16]
# parts_accessed = [8,10,16] # redoing
# parts_accessed = [1,2,5,8,10] # for OPT=10
# parts_accessed = [1,4,8,16] # for OPT=16
# parts_accessed = [1] # for OPT=16
# parts_accessed = [1,8,16,24,32] # for pptvar
# parts_accessed = [10]
# parts_accessed = [1]
parts_accessed = [0] # zero means access all available partitions


############### YCSB specific
# zipftheta = [0.0,0.3,0.6,0.8,0.99]
zipftheta = [0.0] #redo
# zipftheta = [0.0,0.8] # defaults
# zipftheta = [0.6,0.8] #medium + high contention 
# zipftheta = [0.99]

# write_perc = [0.05,0.2,0.5,0.8,0.95]
# write_perc = [0.0,0.2,0.5,0.8,1.0]
write_perc = [0.5]
# mpt_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
# mpt_perc = [0.0,0.01,0.05,0.1,0.2,0.5,0.8,1.0]
# mpt_perc = [1.0]
mpt_perc = [0.0]
# mpt_perc = [0.1] #10% multi partition transactions
# mpt_perc = [1.0] #100% multi partition transactions
# ycsb_op_per_txn = [1,10,16,20,32] #set to a single element if workload is not YCSB
# ycsb_op_per_txn = [32] #set to a single element if workload is not YCSB
# ycsb_op_per_txn = [16] #set to a single element if workload is not YCSB 
ycsb_op_per_txn = [10] #set to a single element if workload is not YCSB 
# ycsb_op_per_txn = [32] #set to a single element if workload is not YCSB 
# recsizes = [0.2,1,5,10,20,40]
# recsizes = [1,5,10,20,40,80,160] # skip 1 since we already have results for it
# recsizes = [10,20,40] # skip 1 since we already have results for it
# recsizes = [0.5, 2, 4, 8, 16]
recsizes = [1]

# bmap_lengths = [1,2,4,8,16,32]
bmap_lengths = [1]

############### TPCC specific
# payment_perc = [0.0]
# payment_perc = [0.5]
payment_perc = [1.0]
# payment_perc = [0.0,0.2,0.5,0.8,1.0]
# payment_perc = [0.0,0.5,1.0]
# mpt_perc = [0.1] #10% multi partition transactions

procs = []
seq_no = 0

#read ifconfig.txt
node_list = []

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

colored('Time is enabled in experiments', 'red', attrs=['bold', 'blink'])

if (time_enable):
    print(colored('Time is enabled in experiments', 'red', attrs=['bold', 'blink']))
else:
    print("Time is NOT enabled in experiments, no latency measurments")
if (dry_run):
    print("Dry run .. no execution ...")
if vm_shut:    
    print(colored('VM will shut down at the end of the experiments!!!', 'red', attrs=['bold', 'blink']))
else:
    print("VM will NOT shut down at the end of the experiments!!!")

if is_azure:
    print("Enter password for Azure:")
    exec_cmd('az login -u {}'.format(secrets['azlogin']), env)
    exec_cmd('az account set -s {}'.format(secrets['azsub_id']), env)
    exec_cmd('az account list', env)
if is_ycsb:
    for rs in recsizes:
        set_ycsb_schema(rs)
        for ncc_alg in cc_algs:
            for wthd in wthreads:
                for theta in zipftheta:
                    for maxtppv in mtpps:
                        for wp in write_perc:
                            for m_stratv in merge_strats:
                                for mprv in mpt_perc:
                                    for bml in bmap_lengths:
                                        for ct in ct_perc:
                                            for oppt in ycsb_op_per_txn:
                                                for pa in parts_accessed:
                                                    # assert(pa > 0) 
                                                    if pa < 1:
                                                        pa = int(wthd*pa)                                                                                                        

                                                    for ppts in strict:
                                                        for bs in batch_sized:
                                                            runexp = True
                                                            exp_cnt = 0
                                                            for pt in pt_perc:                                                                    
                                                                #use this condition if QUECC is included only
                                                                if ncc_alg != 'QUECC' and exp_cnt >= 1:
                                                                    runexp = False

                                                                if runexp:
                                                                    exp_cnt = exp_cnt + 1        
                                                                    if not(ncc_alg == 'QUECC' or ncc_alg == 'LADS') and pt != 1:
                                                                        pt = 1
                                                                    set_config(ncc_alg, wthd, theta, pt, bs, pa, ppts, oppt,mprv,wp, maxtppv, m_stratv,rs,ct,bml)
                                                                    # exec_cmd('head {}'.format(DENEVA_DIR_PREFIX+'config.h'), env)
                                                                    if not dry_run:
                                                                        build_project()
                                                                    for trial in list(range(num_trials)):
                                                                        if rs <= 1:
                                                                            rs_str = str(int(rs*100))
                                                                        else:
                                                                            rs_str = str(int(rs)*100)

                                                                        if ct <= 1:
                                                                            ct_cnt = str(int(ct*wthd))
                                                                        else:
                                                                            ct_cnt = str(int(ct))

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
                                                                                nprefix = prefix + '_ct'+ ct_cnt + '_'+ rs_str + '_pa' + str(pa) + '_' + str(bs)  + '_pt' + pt_cnt + '_et' + et_cnt +'_'+ pt_perc_str +'_pptstrict_' ;
                                                                            else:
                                                                                nprefix = prefix + '_ct'+ ct_cnt + '_'+ rs_str + '_pa' + str(pa) + '_' + str(bs)  + '_pt' + pt_cnt + '_et' + et_cnt +'_'+ pt_perc_str +'_pptnonstrict_';  
                                                                        else:
                                                                            if ppts:
                                                                                nprefix = 'ct'+ ct_cnt + '_' + rs_str +'Brec_' + 'pa' + str(pa) + '_' + str(bs) + '_pt' + pt_cnt + '_et' + et_cnt +'_'+ pt_perc_str +'_pptstrict_';
                                                                            else:
                                                                                nprefix = 'ct'+ ct_cnt + '_' + rs_str  + 'Brec_' + 'pa' + str(pa) + '_' + str(bs)  + '_pt' + pt_cnt + '_et' + et_cnt +'_'+ pt_perc_str +'_pptnonstrict_';
                                                                        run_trial(trial, ncc_alg, env, seq_no, True, node_list, outdir, nprefix)                            
                                                                        # print('Dry run: {}, {}, {}, t{}, {}'
                                                                        #     .format(ncc_alg, str(wthd), str(theta), str(trial), nprefix))
                                                                        seq_no = seq_no + 1
                                                                    cfgfname = WORK_DIR+'/'+DENEVA_DIR_PREFIX+'config.h'
                                                                    cfg_copy = '{}/{}{}_config.h'.format(outdir, nprefix, ncc_alg.replace('_',''))
                                                                    # print("cp {} {}".format(cfgfname, cfg_copy))
                                                                    exec_cmd("cp {} {}".format(cfgfname, cfg_copy), env)
else: #TPC-C
    for ncc_alg in cc_algs:
        for wthd in wthreads:
            for ct in ct_perc:
                for maxtppv in mtpps:
                    for wp in payment_perc:
                        for m_stratv in merge_strats:                        
                            for mprv in mpt_perc:
                                for bml in bmap_lengths:
                                    for pa in parts_accessed:
                                        # assert(pa > 0) 
                                        if pa < 1:
                                            pa = int(wthd*pa)
                                        for ppts in strict:
                                            for bs in batch_sized:
                                                runexp = True
                                                exp_cnt = 0
                                                for pt in pt_perc:                                                                    
                                                    #use this condition if QUECC is included only
                                                    oppt = 1
                                                    theta = 0.0
                                                    rs = 1
                                                    if ncc_alg != 'QUECC' and exp_cnt >= 1:
                                                        runexp = False

                                                    if runexp:
                                                                    exp_cnt = exp_cnt + 1        
                                                                    if not(ncc_alg == 'QUECC' or ncc_alg == 'LADS') and pt != 1:
                                                                        pt = 1
                                                                    set_config(ncc_alg, wthd, theta, pt, bs, pa, ppts, oppt,mprv,wp, maxtppv, m_stratv,rs,ct,bml)
                                                                    # exec_cmd('head {}'.format(DENEVA_DIR_PREFIX+'config.h'), env)
                                                                    if not dry_run:
                                                                        build_project()
                                                                    for trial in list(range(num_trials)):
                                                                        if rs <= 1:
                                                                            rs_str = str(int(rs*100))
                                                                        else:
                                                                            rs_str = str(int(rs)*100)

                                                                        if ct <= 1:
                                                                            ct_cnt = str(int(ct*wthd))
                                                                        else:
                                                                            ct_cnt = str(int(ct))

                                                                        if pt <= 1:
                                                                            pt_cnt = str(int(pt*wthd))
                                                                            et_cnt = str(wthd-int(pt*wthd));
                                                                            pt_perc_str = str(int(pt*100))
                                                                        else:
                                                                            pt_cnt = str(pt)
                                                                            et_cnt = str(wthd-pt);
                                                                            pt_perc_str = str(0)

                                                                        payp_str = str(int(wp*100))
                                                                        
                                                                        if (wthd-int(pt*wthd)) == 0:
                                                                            et_cnt = str(wthd);

                                                                        if prefix != "":
                                                                            if ppts:
                                                                                nprefix = prefix + '_ct'+ ct_cnt +'_payp'+ payp_str  + '_'+ rs_str + '_pa' + str(pa) + '_' + str(bs)  + '_pt' + pt_cnt + '_et' + et_cnt +'_'+ pt_perc_str +'_pptstrict_'  ;
                                                                            else:
                                                                                nprefix = prefix + '_ct'+ ct_cnt +'_payp'+ payp_str  + '_'+ rs_str + '_pa' + str(pa) + '_' + str(bs)  + '_pt' + pt_cnt + '_et' + et_cnt +'_'+ pt_perc_str +'_pptnonstrict_';  
                                                                        else:
                                                                            if ppts:
                                                                                nprefix = 'ct'+ ct_cnt +'_payp'+ payp_str  + '_' + rs_str +'Brec_' + 'pa' + str(pa) + '_' + str(bs) + '_pt' + pt_cnt + '_et' + et_cnt +'_'+ pt_perc_str +'_pptstrict_';
                                                                            else:
                                                                                nprefix = 'ct'+ ct_cnt +'_payp'+ payp_str + '_' + rs_str  + 'Brec_' + 'pa' + str(pa) + '_' + str(bs)  + '_pt' + pt_cnt + '_et' + et_cnt +'_'+ pt_perc_str +'_pptnonstrict_';
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
if not dry_run and vm_shut:
    if is_azure:
        exec_cmd('az vm deallocate -g quecc -n {}'.format(secrets['vm_name']), env)
    else:
        exec_cmd('sudo shutdown -h now', env)
