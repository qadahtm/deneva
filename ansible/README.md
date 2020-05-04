# Ansible-based project and supporting scripts in Python

## Dependencies
Ansible and Ceploy

## Setup environment variables
Copy `activate_env_sample` to `activate_env`

```shell script
$ cp activate_env_sample activate_env
```

Edit variables values according to the comments, then use the following command:
```shell script
$ source activate_env
```

## Creating cluster nodes

By running the script `init_hosts.py`, cloud vm instances will be created based on `../site/site.yml` site specifications. 

