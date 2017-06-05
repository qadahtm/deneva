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


# Building Deneva
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


## Get started with Deneva/QueCC

To get started, you need to get the code from the private repo. 

### Setup credentials with github
The best way to do this is to use SSH. You can follow this link to set that up. [SSH with Github](https://help.github.com/articles/connecting-to-github-with-ssh/)

### Cloning repo
Subtitute your user name below without the square brackets. 
```bash
$ git clone https://[username]@github.com/msadoghi/DBx1000-Distributed && cd DBx1000-Distributed; git checkout quecc
```

Verify that you in the `quecc` branch by running

```bash
$ git branch
```

You should get an output like this:
```bash
  master
* quecc
```

## Building dependencies
The sources for dependencies are included in the `deps` directory of the repo. All you need to do is to uncompress the the files and build from source. You only need to build nanomsg and jemalloc. For boost, we are using the headers only, so there is not need to build it. Before we continue, create a dependency directory outside the current working directory. We do this to avoid track any files from the dependencies by accedentilly adding it into the repo. Assuming that you are in the directory of the repo, we will create the actual dependenies directory outside the repo using the following command:
```bash
$ mkdir ../deps
```

### GCC and build tools
If you are on a clean linux machine (e.g. an new EC2 instance), make sure that all build tools are installed before proceeding.
```bash
$ sudo apt-get install build-essential git vim cmake
```

### boost
We are using only the header files fro boost, so we only need to uncompress the boost dependency file into the dependency folder. Here is the command to do it:
```bash
$ tar -xzf deps/boost_1_63_0.tar.gz -C ../
```

### nanomsg
Extract source files
```bash
# Source files and Change directory
$ tar -xzf deps/1.0.0.tar.gz -C ../; cd ../nanomsg-1.0.0/
```

You can refer to the `README` file on building instruction. The only things you need to do differently is to set the installtion directory and the generate a static library. 

Here is a list of command that you can use:
```bash
$ mkdir build; cd build
$ cmake .. -DCMAKE_INSTALL_PREFIX=../../deps/nanomsg-1.0.0 -DNN_STATIC_LIB=ON -DNN_ENABLE_GETADDRINFO_A=OFF
$ cmake --build .;ctest -C Debug .;cmake --build . --target install
```

**Note**: the Makefile is configured for 64-bit machines and hence it is using the library from `lib64`, on 32-bit machines you will need to modify the Makefile accordingly to use the `lib` directory instead. 

### jemalloc
Starting from the root directory of the repo, extract source files for jemalloc using:

```bash
# Extract files 
$ tar -xf deps/jemalloc-4.5.0.tar.bz2 -C ../; cd ../jemalloc-4.5.0/
```

Similarly, you can refer to the `INSTALL` instruction to build jemalloc. Below are the commands that you can use, which should work with the current configuration. You need to subtitute the absolute directory path without square brackets. 

```bash
$ ./configure --prefix=[absolute_dir_path]/deps/jemalloc-4.5.0 --with-jemalloc-prefix=je_
$ make; make install
```

# Building Deneva
After all the dependencies are installed correctly, you can build Deneva by running:
```bash
$ make -j
```

**Note**: you probably need to create an `obj` directory in the root directory of the deneva project. 

# Running Deneva
Refer to the README.md at the root directory. I will add details specific to halstead or AWS here if any

For generating ifconfig.txt, we need to have IP addresses in them. Hostnames do not work. Also, IP addresses for servers should be listed first and clients later.Python scripts that automatically generate this file for Halstead is available in the `halstead` directory. First run `setupNodesIPs.sh` outside the project directory. It will generate a file containing hostnames of the reserved nodes. Then, run `setupDenevaScripts.py` also from outside the project directory. 

*TODO*
Here are the commands:
```bash
# Make sure that you are outside the project directory
$ 
```

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

