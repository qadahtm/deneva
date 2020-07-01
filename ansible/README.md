# Ansible-based project and supporting scripts in Python

## Dependencies
Ansible, Google Cloud SDK (i.e., `gcloud` CLI) and Ceploy

## Setting Up SSH
Ansible needs to connect to VM instances remotely via SSH. 

### Google Cloud
Add public key to project meta-data. 

## Setup environment variables
Copy `activate_env_sample` to `activate_env`

```shell script
$ cp activate_env_sample activate_env
```

Edit variables values according to the comments, then use the following command:
```shell script
$ source activate_env
```

### Disable SSH Strict check for host keys

Use the following: 
```shell script
$ export ANSIBLE_HOST_KEY_CHECKING=False
```

## Provision cluster nodes

By running the script `provision.py`, cloud vm instances will be created based on `../site/site_deploy.yml` site specifications.
The `provision.py` script requires to specify a directory to store the Ansible inventory information. 
Exmple command: 

```shell script
$ python provision.py dev
```

Example 

## Setup nodes 
This Ansible script installs required packages to build and run experiments on the provisioned cluster.

```shell script
$ ansible-playbook -i dev deploy.yml
```

## Run experiment 
This Ansible script builds and run an experiment using the provisioned cluster. It uses the current `config.h`, `ifconfig.txt` in this root project directory.   

```shell script
$ ansible-playbook -i dev build_run.yml
```