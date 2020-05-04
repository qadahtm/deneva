#!/usr/bin/python

import sys, getopt

import yaml


def usage():
    print('init_hosts.py [-h, --ceploy-home=/path/to/ceploy, --conf-file=/path/to/ceploy-conf] <inventory directory>')


def get_vm_name(prefix, n):
    return "{}-{}-{}".format(prefix, n['group'], n['name'])

def main(argv):

    try:
        opts, args = getopt.getopt(argv,"h",["help", "ceploy-home=", "conf-file="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    from os import environ
    ceploy_home = environ.get("CEPLOY_HOME")
    cloud_conf_file = environ.get("CEPLOY_CONF_FILE")

    for o, a in opts:
        if o == '--conf-file':
            cloud_conf_file = a
        elif o == '--ceploy-home':
            ceploy_home = a
        elif o == '-h':
            usage()

    if len(args) != 1:
        print("Need to specify target inventory directory")
        usage()
        sys.exit(1)

    sys.path.append("{}/src".format(ceploy_home))

    from ceploy.cloud import Cloud
    from ceploy.constants import Provider

    if cloud_conf_file is None:

        raise ValueError("Setting environemnt variable CEPLOY_CONF_FILE to point to the key file is required")

    # initiate Cloud context
    cloud = Cloud.make(Provider.GCLOUD, cloud_conf_file)

    with open('../site/site.yml') as site_file:
        site = yaml.full_load(site_file)
        from pprint import pprint

        prefix = site['prefix']

        servers = {}
        clients = {}
        zookeepers = {}
        default_group = {}

        for n in site['nodes']:
            pprint(n)
            # cloud.create_instance()

            group = n['group']
            vm_name = get_vm_name(prefix, n)
            vm = cloud.create_instance(vm_name, n['template'], n['zone'])
            final_vm = {}
            final_vm['vm'] = vm
            final_vm['conf'] = n

            if group == 'clients':
                clients[vm_name] = final_vm
            elif group == 'zookeeper':
                zookeepers[vm_name] = final_vm
            elif group == 'servers':
                servers[vm_name] = final_vm
            else:
                # default is server group
                vm_name = "{}-{}".format(prefix, n['name'])
                default_group[vm_name] = final_vm


        hostfile = ""
        for d in default_group:
            print(default_group[d])

        pprint(servers)




if __name__ == "__main__":
    main(sys.argv[1:])