#!/bin/python3

import glob
import os
import sys
import socket
import re
import shlex, subprocess
import smtplib
import shutil
import pprint
# import matplotlib
# import numpy as np
# import matplotlib.pyplot as plt
# import pandas as pd
import json
# from pprint import pprint
import time
from datetime import timedelta
import multiprocessing
# from termcolor import colored, cprint
import ec2_nodes

COLOR_RED = '\x1b[31m'
COLOR_GREEN = '\x1b[32m'
COLOR_YELLOW = '\x1b[33m'
COLOR_BLUE = '\x1b[34m'
COLOR_MAGENTA = '\x1b[35m'
COLOR_CYAN ='\x1b[36m'
COLOR_RESET = '\x1b[0m'

def format_exp(exp):
  return pprint.pformat(exp).replace('\n', '')


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

def replace_def(conf, name, value):
	pattern = r'^#define %s\s+.+$' % re.escape(name)
	repl = r'#define %s %s' % (name, value)
	s, n = re.subn(pattern, repl, conf, flags=re.MULTILINE)
	assert n == 1, 'failed to replace def: %s=%s' % (name, value)
	return s


def set_alg(conf, alg, **kwargs):
	conf = replace_def(conf, 'CC_ALG', alg.partition('-')[0].partition('+')[0])
	conf = replace_def(conf, 'ISOLATION_LEVEL', 'SERIALIZABLE')
	return conf

def set_quecc_batch_size(conf, batch_size, thread_count , **kwargs):
	conf = replace_def(conf, 'BATCH_SIZE', str(batch_size))  
	return conf

def set_cluster(conf, node_cnt, thread_count, alg , **kwargs):
	conf = replace_def(conf, 'NODE_CNT', str(node_cnt))
	assert(thread_count == 4)
	if alg == 'CALVIN':
		conf = replace_def(conf, 'THREAD_CNT', str(thread_count-2))
	else:	
		conf = replace_def(conf, 'THREAD_CNT', str(thread_count))
	conf = replace_def(conf, 'REM_THREAD_CNT', str(2))
	conf = replace_def(conf, 'SEND_THREAD_CNT', str(2))
	conf = replace_def(conf, 'PART_CNT', str(node_cnt))
	conf = replace_def(conf, 'CLIENT_NODE_CNT', str(node_cnt))
	conf = replace_def(conf, 'CLIENT_THREAD_CNT', str(4))
	conf = replace_def(conf, 'CLIENT_REM_THREAD_CNT', str(2))
	conf = replace_def(conf, 'CLIENT_SEND_THREAD_CNT', str(2))

	return conf

def set_ycsb(conf, thread_count, total_count, record_size, req_per_query, read_ratio, zipf_theta, tx_count, **kwargs):
	conf = replace_def(conf, 'WORKLOAD', 'YCSB')
	conf = replace_def(conf, 'WARMUP', str(int(tx_count / 3)))
	conf = replace_def(conf, 'MAX_TXN_PER_PART', str(tx_count))
	conf = replace_def(conf, 'MAX_TUPLE_SIZE', str(record_size))
	# conf = replace_def(conf, 'INIT_PARALLELISM', str(thread_count)) 
	conf = replace_def(conf, 'INIT_PARALLELISM', str(4)) 
	# conf = replace_def(conf, 'PART_CNT', str(min(2, thread_count))) # try to use both NUMA node, but do not create too many partitions
	# conf = replace_def(conf, 'PART_CNT', 'NODE_CNT')

	conf = replace_def(conf, 'SYNTH_TABLE_SIZE', str(total_count))
	conf = replace_def(conf, 'REQ_PER_QUERY', str(req_per_query))
	# conf = replace_def(conf, 'READ_PERC', str(read_ratio))
	conf = replace_def(conf, 'WRITE_PERC', str(1. - read_ratio))
	conf = replace_def(conf, 'SCAN_PERC', 0)
	conf = replace_def(conf, 'ZIPF_THETA', str(zipf_theta))

	return conf

def set_tpcc(conf, thread_count, bench, warehouse_count, tx_count, pay_perc, **kwargs):
	conf = replace_def(conf, 'WORKLOAD', 'TPCC')
	conf = replace_def(conf, 'WARMUP', str(int(tx_count / 3)))
	conf = replace_def(conf, 'MAX_TXN_PER_PART', str(tx_count))
	conf = replace_def(conf, 'MAX_TUPLE_SIZE', str(1024))
	conf = replace_def(conf, 'NUM_WH', str(warehouse_count))
	conf = replace_def(conf, 'PERC_PAYMENT', str(pay_perc))
	# INIT_PARALLELISM does not affect tpcc initialization
	# conf = replace_def(conf, 'INIT_PARALLELISM', str(warehouse_count))
	conf = replace_def(conf, 'INIT_PARALLELISM', str(4))
	# conf = replace_def(conf, 'PART_CNT', str(warehouse_count))
	# conf = replace_def(conf, 'PART_CNT', 'NODE_CNT')

	conf = replace_def(conf, 'NEWORDER_INSERT_ENABLED', 'false')
	conf = replace_def(conf, 'PAYMENT_INSERT_ENABLED', 'false')

	return conf

def set_threads(conf, thread_count, **kwargs):
	return replace_def(conf, 'THREAD_CNT', thread_count)

def set_mpt(conf, mpr, ppt_cnt, node_cnt , **kwargs):
	assert(ppt_cnt <= node_cnt)
	# conf = replace_def(conf, 'PART_PER_TXN', str(ppt_cnt))
	#standard PPT is 2 as per CALVIN paper
	conf = replace_def(conf, 'PART_PER_TXN', str(ppt_cnt))
	conf = replace_def(conf, 'MPR', str(mpr))
	return conf

def gen_filename(exp):
	s = ''
	for key in sorted(exp.keys()):
		s += key
		s += '@'
		s += str(exp[key])
		s += '__'
	return prefix + s.rstrip('__') + suffix

def enum_exps(seq):
	all_algs = ['NO_WAIT',
				'CALVIN',
				'MVCC',
				'MAAT',
				'TIMESTAMP',
				'WAIT_DIE',
				#'QUECC'
				]
	node_cnt = ec2_nodes.server_cnt 
	for alg in all_algs:
		
		# wtvar
		# wthreads =  [1, 2] + list(range(4, max_thread_count + 1, 4))
		# wthreads =  list(range(4, max_thread_count + 1, 4))
		# wthreads = [4,8,16,24,32]
		wthreads = [4]
		# wthreads = [4]
		# zipftheta = [0.0,0.8]
		# zipftheta = [0.99] # High Contention
		# zipftheta = [0.0] # Uniform
		# zipftheta = [0.6] # Low contention
		# zipftheta = [0.8] # Medium contention
		# zipftheta = [0.99,0.9,0.7,0.5,0.3,0.0] 
		# zipftheta = [0.99,0.7,0.5,0.3,0.0] 
		# zipftheta = [0.99,0.9,0.8,0.4,0.0] 
		# zipftheta = [0.8,0.6]
		# zipftheta = [0.99] 
		zipftheta = [0.0,0.3,0.6,0.8,0.9,0.95,0.99]
		# zipftheta = [0.0]
		# read_ratios = [1.0,0.95,0.8,0.5,0.2,0.05]
		# read_ratios = [0.5,0.2,0.05]
		# read_ratios = [1.0]
		read_ratios = [0.5] # 
		# read_ratios = [0.5,0.8]
		# max_thread_count = 32
		# total_count = 16 * 1000 * 1000 # 16 Million
		# total_count = 0.05 * 1000 * 1000 # 1 Million
		total_count = 16783200*node_cnt
		# record_size = 1000
		record_size = 100
		# req_per_query_vals = [1,10,16,20,32]
		# req_per_query_vals = [10,16,20,32]
		# req_per_query_vals = [1]
		# req_per_query_vals = [10]
		req_per_query_vals = [16]

		# batch_size_vals = [1024,2048,4096,5184,8192,10368,20736,41472,82944]
		# batch_size_vals = [82944]
		# batch_size_vals = [10368]
		# batch_size_vals = [10368, 40320, 40320*2]
		# batch_size_vals = [40320*2] # default for QC
		# batch_size_vals = [10368, 40320, 40320*2, 40320*4, 40320*8]
		# batch_size_vals = [40320*4, 40320*8]
		batch_size_vals = [10368] #default for others

		# req_per_query_vals = [20]
		# req_per_query_vals = [32]
		tx_count = 500000
		# tx_count = 50000
		# print('max_thd_cnt = {}, worker threads:{}\n'.format(str(max_thread_count), str(wthreads)))
		tag = 'macrobench'		
		thread_count = wthreads[0]

		# mpr_vals = [0.0,0.15]
		#standard MPR is 10% as per CALVIN paper
		mpr_vals = [0.0,1.0]
		#standard ppt is 2 as per CALVIN paper
		# ppt_vals = [2]
		ppt_vals = [node_cnt]

		common = { 'seq': seq, 'tag': tag, 'node_cnt':node_cnt, 'alg': alg, 'thread_count': thread_count }			

		# YCSB
		ycsb = dict(common)
		ycsb.update({ 'bench': 'YCSB'})
		ycsb.update({ 'record_size': record_size, 'tx_count': tx_count, 'total_count':total_count })


		if True:
			# for read_ratio in [0.50, 0.95]:
			for read_ratio in read_ratios:
			# for zipf_theta in [0.00, 0.90, 0.99]:
			# for zipf_theta in [0.00, 0.99]:
				for zipf_theta in zipftheta:
					for mpr in mpr_vals:
						for ppt in ppt_vals:
							for rpq in req_per_query_vals:
								for bs in batch_size_vals:
									if bs > 10368 and alg != 'QUECC': continue
									ycsb.update({ 'read_ratio': read_ratio, 'zipf_theta': zipf_theta, 'req_per_query': rpq, 'batch_size':bs})
									ycsb.update({'mpr':mpr,'ppt_cnt':ppt})
									yield dict(ycsb)
		# TPCC
		#whvar
		# warehouses_vars = [1]
		# warehouses_vars = [4]
		# warehouses_vars = [16]
		warehouses_vars = [(node_cnt*4),(node_cnt*128)]
		# warehouses_vars = [thread_count,1,4]
		# warehouses_vars = [8,16]
		pay_percs = [0.0,0.5,1.0]

		if False:
			tx_count = 500000

			tpcc = dict(common)
			# tx_count = 200000          
			tpcc.update({ 'bench': 'TPCC', 'tx_count': tx_count })

			# # for warehouse_count in [1, 4, 16, max_thread_count]:
			# # warehouses_vars = [1, 4, max_thread_count]
			
			for payp in pay_percs:
				for mpr in mpr_vals:
					for ppt in ppt_vals:
						for bs in batch_size_vals:
							if bs > 10368 and alg != 'QUECC': continue
							for warehouse_count in warehouses_vars:
								tpcc.update({ 'warehouse_count': warehouse_count,'batch_size':bs, 'pay_perc':payp })
								tpcc.update({'mpr':mpr,'ppt_cnt':ppt})
								yield dict(tpcc)

def update_conf(conf, exp):
	conf = set_alg(conf, **exp)
	conf = set_cluster(conf, **exp)
	conf = set_mpt(conf,**exp)

	if exp['bench'] == 'YCSB':
		conf = set_ycsb(conf, **exp)
	elif exp['bench'] in ('TPCC'):
		conf = set_tpcc(conf, **exp)
	else: assert False
	
	if exp['alg'].startswith('QUECC'):
		if exp['bench'] == 'YCSB':
			conf = set_quecc_batch_size(conf, **exp)
		# if exp['thread_count'] == 4:
			# conf = replace_def(conf, 'COMMIT_THREAD_CNT', str(4))
	return conf

def sort_exps(exps):
	def _exp_pri(exp):
		pri = 0

		if exp['thread_count'] in (max_thread_count, max_thread_count * 2): pri -= 1

		# prefer write-intensive workloads
		if exp['bench'] == 'YCSB' and exp['read_ratio'] == 0.50: pri -= 1
		# prefer standard skew
		if exp['bench'] == 'YCSB' and exp['zipf_theta'] in (0.00, 0.90, 0.99): pri -= 1

		# prefer (warehouse count) = (thread count)
		if exp['bench'].startswith('TPCC') and exp['thread_count'] == exp['warehouse_count']: pri -= 1
		# prefer standard warehouse counts
		if exp['bench'].startswith('TPCC') and exp['warehouse_count'] in (1, 4, 16, max_thread_count, max_thread_count * 2): pri -= 1

		# run exps in their sequence number
		return (exp['seq'], pri)

	exps = list(exps)
	exps.sort(key=_exp_pri)
	return exps

def skip_done(exps):
	for exp in exps:
		if os.path.exists(dir_name + '/' + gen_filename(exp)): continue
		if os.path.exists(dir_name + '/' + gen_filename(exp) + '.failed'): continue
		# if exp['alg'] == 'MICA': continue
		yield exp

def find_exps_to_run(exps, pats):
	for exp in exps:
		if pats:
			for pat in pats:
				key, _, value = pat.partition('@')
				if key not in exp or str(exp[key]) != value:
					break
			else:
				yield exp
		else:
			yield exp

def set_conf_file(exp,src_dir):
	conf = open('{}/config-std.h'.format(src_dir)).read()
	conf = update_conf(conf, exp)
	open('{}/config.h'.format(src_dir), 'w').write(conf)

def unique_exps(exps):
	l = []
	for exp in exps:
		if exp in l: continue	
		l.append(exp)
	return l

def run_all_seq(pats, prepare_only):
	exps = []
	for seq in range(total_seqs):
		exps += list(enum_exps(seq))
	exps = list(unique_exps(exps))

	note = str(exps)
	prepare_node_list()


	print('exps to run:\n')
	for e in exps:
		print('{}'.format(str(e)))

	total_count = len(exps)
	print('total {} exps'.format(total_count))

	count_per_tag = {}
	for exp in exps:
		count_per_tag[exp['tag']] = count_per_tag.get(exp['tag'], 0) + 1
	for tag in sorted(count_per_tag.keys()):
		print('  %s: %d' % (tag, count_per_tag[tag]))
	print('')

	if not prepare_only:
		exps = list(skip_done(exps))
	exps = list(find_exps_to_run(exps, pats))
	skip_count = total_count - len(exps)
	print('{} exps skipped'.format(skip_count))
	print('')

	count_per_tag = {}
	for exp in exps:
		count_per_tag[exp['tag']] = count_per_tag.get(exp['tag'], 0) + 1
	for tag in sorted(count_per_tag.keys()):
		print('  %s: %d' % (tag, count_per_tag[tag]))
	print('')

	first = time.time()
	for i, exp in enumerate(exps):
		start = time.time()
		failed = len(glob.glob(os.path.join(dir_name, '*.failed')))
		s = 'exp %d/%d (%d failed): %s' % (i + 1, len(exps), failed, format_exp(exp))
		print(COLOR_BLUE + s + COLOR_RESET)

		set_conf_file(exp,src_dir)
		sync_source_code_efs()
		build_executables()

		kill_all_processes()

		run_dist_exp(exp,True)
		# run(exp, prepare_only)


		if prepare_only: break

		now = time.time()
		print('elapsed = %.2f seconds' % (now - start))
		print('remaining = %.2f hours' % ((now - first) / (i + 1) * (len(exps) - i - 1) / 3600))
		print('')

	return note


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

def live_output(p):
	print("liveOutput for {}".format(str(p)))
	while p.poll() is None:
		print(p.stdout.readline().decode(encoding="utf-8", errors="strict"), end='')
	print(p.stdout.readline().decode(encoding="utf-8", errors="strict"), end='')

def wait_for(plist,expds=None, outputFlag=False, liveOutput=False, live_output_node_idx=0):
	if liveOutput:
		live_output(plist[live_output_node_idx]);
	# done observing live output if enabled		
	failed = False
	output = ''
	for i,p in enumerate(plist):		
		if p:
			if verbose:
				print("Waiting for node {} at {}".format(i,node_list[i]))
			try:
				stdout, stderr = p.communicate(timeout=360)
				killed = False
			except subprocess.TimeoutExpired:
				kill_all_processes()
				stdout, stderr = p.communicate(timeout=10)
				killed = True

			stdout = stdout.decode('utf-8')
			stderr = stderr.decode('utf-8')
			output =  output + '\n\n' + stdout + '\n\n' + stderr

			if p.returncode != 0 or killed:
				error_s = 'Node[{}] at {} '.format(i,node_list[i])
				if expds:
					error_s = error_s + ('failed to run exp for %s (status=%s, killed=%s)' % (format_exp(expds), p.returncode, killed))
				else:
					error_s = error_s + ('failed to run remote command (status=%s, killed=%s)' % (p.returncode, killed))

				print(COLOR_RED + error_s + COLOR_RESET)
				failed = True

	if expds:				
		filename = dir_name + '/' + gen_filename(expds)
		if failed:
			f_filename = filename + '.failed'
		else:
			f_filename = filename
		outf = open(f_filename,'w')
		outf.write(output)

	elif outputFlag:
		print('Output of node: {}'.format(i))
		print(output)
		# for ol in p.stdout:
			# print(ol.decode(encoding="utf-8", errors="strict"), end='')
		print('----- End of output of node {} -----'.format(i))


def prepare_node_list():
	with open('/mnt/efs/expodb/ifconfig.txt','w') as f:
		for nip in node_list:
			f.write('{}\n'.format(nip))
	#copy ifconfig.txt to build directories
	proc_list = []		
	for nip in node_list:
		if verbose:
			print('sending command (ifconfig sync) to node: {}'.format(nip))
		rcmd = 'ssh -oStrictHostKeyChecking=no ubuntu@{} {}'.format(nip,'cp -f /mnt/efs/expodb/ifconfig.txt ~/{}/ifconfig.txt'.format(build_dir))
		proc_list.append(exec_cmd(rcmd,env,True))
	wait_for(proc_list)
	proc_list.clear()
	print('done (ifconfig sync)!')

def sync_source_code_efs():
	#sync sources from EFS
	proc_list = []
	for nip in node_list:
		if verbose:
			print('sending command (EFS sync) to node: {}'.format(nip))
		rcmd = 'ssh -oStrictHostKeyChecking=no ubuntu@{} {}'.format(nip,'rsync -axvP {} ~/'.format(src_dir))
		proc_list.append(exec_cmd(rcmd,env,True))
	wait_for(proc_list)
	proc_list.clear()
	print('done (EFS sync)!')

def build_executables():
	#build and compile on each node
	proc_list = []
	for nip in node_list:
		if verbose: 
			print('sending command (build from source) to node: {}'.format(nip))
		rcmd = 'ssh -oStrictHostKeyChecking=no ubuntu@{} "{}"'.format(nip,'cd ~/{}; make clean; make -j8'.format(build_dir))
		proc_list.append(exec_cmd(rcmd,env,True))
	wait_for(proc_list)
	proc_list.clear()
	print('done (building)!')

def kill_all_processes():
	proc_list = []
	for nip in node_list:
		if verbose:
			print('sending command (kill all) to node: {}'.format(nip))
		rcmd = 'ssh -oStrictHostKeyChecking=no ubuntu@{} "{}"'.format(nip,'pkill -9 rundb; pkill -9 runcl')
		proc_list.append(exec_cmd(rcmd,env,True))
	wait_for(proc_list)
	proc_list.clear()
	print('done (kill-all)!')

def run_dist_exp(exp,_run_exp):
	proc_list = []
	if _run_exp:
		server_cnt = ec2_nodes.server_cnt
		for i,nip in enumerate(node_list):
			if i < server_cnt:
				#start server process
				tag = "{} as a server #{}, i={}".format(nip,i,i)						
				if i not in skip_nodes:
					rcmd = 'ssh -oStrictHostKeyChecking=no ubuntu@{} "{}{}"'.format(nip,'cd ~/{}; bin/rundb -nid'.format(build_dir),i)
					proc_list.append(exec_cmd(rcmd,env,True))
					print("Run server {}: {}".format(tag,i))
				else:
					proc_list.append(None) # append empty
					print("Skipped server {}: {}".format(tag,i))				
			else:
				#start client process
				tag = "{} as a client #{}, i={}".format(nip,i-server_cnt,i)
				if i not in skip_nodes:
					rcmd = 'ssh -oStrictHostKeyChecking=no ubuntu@{} "{}{}"'.format(nip,'cd ~/{}; bin/runcl -nid'.format(build_dir),i)
					proc_list.append(exec_cmd(rcmd,env,True))
					print("Run client {}: {}".format(tag,i))
				else:
					proc_list.append(None) # append empty
					print("Skipped client {}: {}".format(tag,i))

		wait_for(proc_list,exp,True, liveOutput_enabled,0)
		proc_list.clear()
		print('done (experiment)!')

def run_dist_exp_single(_run_exp):
	proc_list = []
	if _run_exp:
		server_cnt = ec2_nodes.server_cnt
		for i,nip in enumerate(node_list):
			if i < server_cnt:
				#start server process
				tag = "{} as a server #{}, i={}".format(nip,i,i)						
				if i not in skip_nodes:
					rcmd = 'ssh -oStrictHostKeyChecking=no ubuntu@{} "{}{}"'.format(nip,'cd ~/{}; bin/rundb -nid'.format(build_dir),i)
					proc_list.append(exec_cmd(rcmd,env,True))
					print("Run server {}: {}".format(tag,i))
				else:
					proc_list.append(None) # append empty
					print("Skipped server {}: {}".format(tag,i))				
			else:
				#start client process
				tag = "{} as a client #{}, i={}".format(nip,i-server_cnt,i)
				if i not in skip_nodes:
					rcmd = 'ssh -oStrictHostKeyChecking=no ubuntu@{} "{}{}"'.format(nip,'cd ~/{}; bin/runcl -nid'.format(build_dir),i)
					proc_list.append(exec_cmd(rcmd,env,True))
					print("Run client {}: {}".format(tag,i))
				else:
					proc_list.append(None) # append empty
					print("Skipped client {}: {}".format(tag,i))

		wait_for(proc_list,None,True, liveOutput_enabled,0)
		proc_list.clear()
		print('done (experiment)!')


def run_exiting_config():
	prepare_node_list()
	sync_source_code_efs()
	build_executables()

	kill_all_processes()
	
	run_dist_exp_single(run_exp)


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
with open('/home/ubuntu/secrets.json') as data_file:
    secrets = json.load(data_file)

run_exp = True
liveOutput_enabled = False #print live output for S0
liveOutput_node = 0
single_exp = False
verbose = False

prefix = ''
suffix = ''
total_seqs = 2
max_retries = 1

#nodes to be skipped from running
# skip_nodes = set([0,1])
skip_nodes = set()

src_dir = '/mnt/efs/expodb/deneva'
build_dir = 'qcd-build'

res_dir_name = '/mnt/efs/expodb/exp_results2'
dir_name = res_dir_name
if not os.path.exists(res_dir_name):
	os.mkdir(res_dir_name)

if single_exp:
	#use to run a single experiments
	run_exiting_config()
else:
	stime = time.time()
	#generates and run multiple experiments
	note = run_all_seq(None, False)
	print('Sending email notifications')
	eltime = time.time() - stime
	odirname = dir_name
	subject = 'Experiment done in {}, results at {}'.format(str(timedelta(seconds=eltime)), odirname)
	send_email(subject, note)



# src_dir = '/mnt/efs/expodb/epochcc'
# build_dir = 'epochcc-build'
