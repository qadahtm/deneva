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

## Creating cluster nodes

By running the script `init_hosts.py`, cloud vm instances will be created based on `../site/site.yml` site specifications. 

