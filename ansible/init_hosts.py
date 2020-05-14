#!/usr/bin/python

import sys, getopt
import yaml


def usage():
    print('init_hosts.py [-h, --ceploy-home=/path/to/ceploy, --conf-file=/path/to/ceploy-conf] <inventory directory>')


def delete_hosts(opts, args):
    pass


def get_vm_name(prefix, n):
    return "{}-{}-{}".format(prefix, n['group'], n['name'])


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hd", ["help", "delete-hosts", "ceploy-home=", "conf-file="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    from os import environ
    ceploy_home = environ.get("CEPLOY_HOME")
    cloud_conf_file = environ.get("CEPLOY_CONF_FILE")

    if len(args) != 1:
        print("Need to specify target inventory directory")
        usage()
        sys.exit(1)

    for o, a in opts:
        if o == '--conf-file':
            cloud_conf_file = a
        elif o == '--ceploy-home':
            ceploy_home = a
        elif o == '--delete-hosts':
            delete_hosts(opts, args)
        elif o == '-h':
            usage()

    sys.path.append("{}/src".format(ceploy_home))

    from ceploy.cloud import Cloud
    from ceploy.constants import Provider

    if cloud_conf_file is None:
        raise ValueError("Setting environemnt variable CEPLOY_CONF_FILE to point to the key file is required")

    # initiate Cloud context
    cloud = Cloud.make(Provider.GCLOUD, cloud_conf_file)

    deploy_vms_and_create_hosts_file(args, cloud)


def print_error(msg):
    from ceploy.constants import OutputColors
    print("{}{}{}".format(OutputColors.COLOR_RED, msg, OutputColors.COLOR_RESET))


def print_warning(msg):
    from ceploy.constants import OutputColors
    print("{}{}{}".format(OutputColors.COLOR_YELLOW, msg, OutputColors.COLOR_RESET))


def create_ifconfig_file(output_path, servers, clients):
    with open("{}/ifconfig.txt".format(output_path), 'w') as ifconfig_file:

        if len(servers) == 0:
            print_warning("Warning: Server IPs list is empty")

        for n in servers:
            print(servers[n]['vm'].ext_ip, file=ifconfig_file)

        if len(clients) == 0:
            print_warning("Warning: Client IPs list is empty")
        for n in clients:
            print(clients[n]['vm'].ext_ip, file=ifconfig_file)


def deploy_vms_and_create_hosts_file(args, cloud):
    from ceploy.providers.gcloud import GCVM

    with open('../conf/site.yml') as site_file:
        site = yaml.full_load(site_file)
        from pprint import pprint

        prefix = site['prefix']

        servers = {}
        clients = {}
        zookeepers = {}
        default_group = {}

        for n in site['nodes']:
            group = n['group']
            vm_name = get_vm_name(prefix, n)
            status, vm, err = cloud.create_instance(vm_name, n['template'], n['zone'])
            if status:
                final_vm = {}
                final_vm['vm'] = GCVM(vm[0])
                # final_vm['vm_raw'] = vm[0]
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
            else:
                print("Error in create instance.")
                pprint(err)

        # Create hosts file for Ansible
        with open('./{}/hosts'.format(args[0]), 'w') as hosts_file:

            for n in default_group:
                print(default_group[n]['vm'].ext_ip, file=hosts_file)

            if len(servers) > 0:
                print("[servers]", file=hosts_file)
            for n in servers:
                print(servers[n]['vm'].ext_ip, file=hosts_file)

            if len(clients) > 0:
                print("[clients]", file=hosts_file)
            for n in clients:
                print(clients[n]['vm'].ext_ip, file=hosts_file)

            if len(zookeepers) > 0:
                print("[zookeeper]", file=hosts_file)
            for n in zookeepers:
                print(zookeepers[n]['vm'].ext_ip, file=hosts_file)

        # Create ifconfig.txt for ExpoDB
        create_ifconfig_file("..", servers, clients)

        # Create site_deploy.yml
        try:
            with open("../conf/site_deploy.yml", "w") as site_deploy:
                doc = {}
                doc['servers'] = servers
                doc['clients'] = clients
                doc['zookeeper'] = zookeepers
                yaml.dump(doc, site_deploy)
        except Exception as e:
            print_error("Could not dump YAML site_deploy file")
            print_error(e)


if __name__ == "__main__":
    main(sys.argv[1:])
