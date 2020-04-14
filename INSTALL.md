# Installation Notes

For now, this document is a set of notes for Ubuntu 18.04.  

## Build dependencies
These dependencies are needed to run the build process.

### Ubuntu 18.04 (bionic)
Install `cmake` version `>= 3.5`. 

Install `libboost-atomic-dev, libnuma, libyaml`. (Note: this is to ensure it compiles, may not be needed later)

```shell script
$ sudo apt-get install -y cmake libboost-atomic-dev libnuma-dev libyaml-dev libnanomsg-dev libjemalloc-dev python3 libzookeeper-mt-dev
```

Install `python3`, use `update-alternatives` to configure the defualt python version. 

```shell script
$ sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 10
```  

### Installing and Running Zookeeper on Ubuntu
For testing with Zookeeper, the simplest way is to utilize the `zookeeperd` debian package.
Install using `apt-get` as follows:

```shell script
$ sudo apt-get update # update apt-get

# installs zookeeper service and and command line util
$ sudo apt-get install zookeeperd zookeeper-bin 

# Run zookeeperd
$ sudo service zookeeper start 

# Run client cli_mt
$ /usr/lib/zookeeper/bin/cli_mt localhost:2181
```

If the zookeeper service is running, `cli_mt` will connect successfully. Commands such as `ls /` can be used. To quit, type `quit`. 

Sample output on success:

```
Watcher SESSION_EVENT state = CONNECTED_STATE
Got a new session id: 0x171714aa7f40003
```

Tutorial on Zookeeper Cli: http://www.mtitek.com/tutorials/zookeeper/zkCli.php

### Ubuntu 16.04 (xeniel)

The nanomsg version associated with xeniel is old. Therefore, the current build configuration does not support Ubuntu 16.04 out of the box. Users need to manually install `nanomsg` version `1.0.0` or grater manually, and modify the `CMakeLists.txt` to include headers and link libraries for this to work. Similar workaround maybe needed for other software dependencies.

