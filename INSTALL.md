# Installation Notes

For now, this document is a set of notes for Ubuntu 18.04.  

## Build dependencies
These dependencies are needed to run the build process.

### Ubuntu 18.04 
Install `cmake` version `>= 3.5`. 

Install `python3`, use `update-alternatives` to configure the defualt python version. 

```shell script
$ sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 10
```  

Install `libboost-atomic-dev, libnuma, libyaml`. (Note: this is to ensure it compiles, may not be needed later)

```shell script
$ sudo apt-get install -y libboost-atomic-dev libnuma-dev libyaml-dev
```