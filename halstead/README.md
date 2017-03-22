# Running Deneva on Halstead cluster

## Requesting Resources from Halstead PBS
One way is to run Deneve on Halstead in interactibve mode. This allows to obtain exlusive
access to nodes and perform experiments interactively. 

Another way is t submit a script to run the experiment, which requires writing the script.
For now, we will just focus on running experiments in interacrtive mode. 

To request resources from Halstead, use ssh and your Career credintials to login to the 
cluser. On the front-end machine, run the following command to request 2 nodes with all 
of their cores and memory resources. 

```sh 
$ qsub -I -lnodes=2:ppn=20:A,walltime=02:00:00,naccesspolicy=singleuser 
```

The above command will request 2 nodes with 20 cores, and you will have these 2 nodes 
for 2 hours. The request will go to the `standby` queue which is shared among all 
cluster users.


## Building Deneva
Note: you will need to request a node for building the code as it seems agains the user 
policy of Halstead to run intensive tasks on the front-end.

The code depends on few libraries:
- Boost (atomic, lockfree ...)
- nanomsg
- jemalloc

I am using gcc-5.2.0 for building this. 
On Halstead this can be loaded using:

`$ module load gcc/5.2.0`

You also need to create an obj directory. Perhaps we can modify the Makefile to do this for us implicitly.


### Building dependencies
Dependencies are included in the `deps` directory of the repo.  
#### Boost
TBD
#### nanomsg
TBD
### jemalloc


## Running Deneva
Refer to the README.md at the root directory. I will add details specific to halstead 
here if any

For generating ifconfig.txt, we need to have IP addresses in them. Hostnames do not 
work. Also, IP addresses for servers should be listed first and clients later. Working 
on a python script that automatically generate this file for Halstead is in-progress.

### Server-side configuration
The key config parameters are:
- NODE_CNT: Number of server nodes
- THREAD_CNT: number of worker threads
- REM_THREAD_CNT: number of message receiver threads, usually is set to THREAD_CNT
- SEND_THREAD_CNT: number of message sender threads, usually is set to THREAD_CNT
- CORE_CNT: number of cores in the node. e.g. in VLDB'17 paper this is set to 8

### Client-side configuration

Client processes need to be able to submit transactions such that the servers are 
saturated and operating at their maximum capacity. 

The configuration parameters are:
- CLIENT_NODE_CNT : number of client processes (they can reside on the same physical node) 
- CLIENT_THREAD_CNT : number of threads assigned for each client
- CLIENT_REM_THREAD_CNT : number of message receiver threads
- CLIENT_SEND_THREAD_CNT : number of message sender therads

There is a requirement for total thread counts to less than or equal to THREAD_CNT + 
REM_THREAD_CNT + SEND_THREAD_CNT + 1 (for an abort thread, this is 
harcoded in `system/global.cpp`)

Here is an example of running 1 server process and 4 client processes.
The key config parameters are:
- NODE_CNT: Number of server nodes

Example ifconfig.txt:
```
172.18.48.235
172.18.49.149
172.18.49.149
172.18.49.149
172.18.49.149
```
Ssh to the server node, got to the 

