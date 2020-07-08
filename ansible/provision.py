#!/usr/bin/python

import sys, getopt
import yaml

# TODO(tq): all interaction with cloud is synchrounous, we should convert them to asynchronous

def usage():
    print('provision.py [-h, --ceploy-home=/path/to/ceploy, --conf-file=/path/to/ceploy-conf] <inventory directory>')


def delete_hosts(opts, args, cloud):

    with open("../conf/site_deploy.yml") as sf:
        conf = yaml.full_load(sf)
        for vm in conf['servers']:
            name = vm['conf']['vm_name']
            zone = vm['conf']['zone']
            cloud.delete_instance(name, zone)


        for vm in conf['clients']:
            name = vm['conf']['vm_name']
            zone = vm['conf']['zone']
            cloud.delete_instance(name, zone)

        for vm in conf['zookeeper']:
            name = vm['conf']['vm_name']
            zone = vm['conf']['zone']
            cloud.delete_instance(name, zone)

    print("Done deleting VMs")
    sys.exit(0)

def get_vm_name(prefix, n):
    return "{}-{}-{}".format(prefix, n['group'], n['name'])


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hd", ["help", "delete", "mock", "ceploy-home=", "conf-file="])
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

    sys.path.append("{}/src".format(ceploy_home))
    from ceploy.cloud import Cloud
    from ceploy.constants import Provider

    if cloud_conf_file is None:
        raise ValueError("Setting environemnt variable CEPLOY_CONF_FILE to point to the key file is required")

    # initiate Cloud context
    cloud = Cloud.make(Provider.GCLOUD, cloud_conf_file)
    mock = False

    for o, a in opts:
        if o == '--conf-file':
            cloud_conf_file = a
        elif o == '--ceploy-home':
            ceploy_home = a
        elif o == '--delete':
            delete_hosts(opts, args, cloud)
        elif o == '--mock':
            mock = True
        elif o == '-h':
            usage()

    deploy_vms_and_create_hosts_file(args, cloud, mock)


def print_error(msg):
    from ceploy.constants import OutputColors
    print("{}{}{}".format(OutputColors.COLOR_RED, msg, OutputColors.COLOR_RESET))


def print_warning(msg):
    from ceploy.constants import OutputColors
    print("{}{}{}".format(OutputColors.COLOR_YELLOW, msg, OutputColors.COLOR_RESET))

def add_replica_mapping(servers, replica_groups):
    servers_res = []
    for s in servers:
        s['replicas'] = replica_groups[s['replica_group']].copy()
        s['replicas'].remove(s['nid'])
        servers_res.append(s)
    return servers_res

def create_ifconfig_file(output_path, servers, clients):
    with open("{}/ifconfig.txt".format(output_path), 'w') as ifconfig_file:
        nid = 0
        if len(servers) == 0:
            print_warning("Warning: Server IPs list is empty")

        for n in servers:
            # print(n['address'], file=ifconfig_file)
            print(n['int_ip'], file=ifconfig_file)
            assert (n['nid'] == nid)
            nid += 1

        if len(clients) == 0:
            print_warning("Warning: Client IPs list is empty")
        for n in clients:
            # print(n['address'], file=ifconfig_file)
            print(n['int_ip'], file=ifconfig_file)
            assert(n['nid'] == nid)
            nid += 1
    return servers, clients


def simplify_servers(servers):
    res = []
    sn = {}
    for s in servers:
        sn['address'] = s['address']
        sn['nid'] = s['nid']
        replica_set = set()
        for r in servers:
            if r['replica_group'] == s['replica_group']:
                replica_set.add(r['nid'])
        sn['replicas'] = sorted(list(replica_set))
        sn['replicas'].remove(s['nid'])
        res.append(sn.copy())
        sn = {}
    return res

def simplify_clients(clients):
    res = []
    for s in clients:
        res.append(s['address'])
    return res

def simplify_zk(zk_nodes):
    res = []
    for s in zk_nodes:
        res.append("{}:{}".format(s['address'], s['port']))
    return res

def deploy_vms_and_create_hosts_file(args, cloud, mock=True):
    from ceploy.providers.gcloud import GCVM

    with open('../conf/site.yml') as site_file:
        site = yaml.full_load(site_file)
        from pprint import pprint

        prefix = site['prefix']

        servers = []
        replica_groups = {}
        clients = []
        zookeepers = []
        default_group = {}

        for n in site['nodes']:
            group = n['group']
            vm_name = get_vm_name(prefix, n)
            if not mock:
                status, vm, err = cloud.create_instance(vm_name, n['template'], n['zone'])
            else:
                status = True

            if status:
                final_vm = {}
                tvm = {}
                if not mock:
                    tvm = GCVM(vm[0])
                else:
                    tvm['ext_ip'] = "127.0.0.1"
                    tvm['int_ip'] = "127.0.0.1"
                # final_vm['vm'] = GCVM(vm[0])
                # final_vm['vm_raw'] = vm[0]
                final_vm['conf'] = n
                final_vm['address'] = tvm['ext_ip']
                final_vm['int_ip'] = tvm['int_ip']
                final_vm['conf']['vm_name'] = vm_name

                if group == 'servers':
                    if (len(clients) > 0):
                        raise RuntimeError("Servers must be all declared before clients in site.yaml")

                    final_vm['replica_group'] = n['replica_group']
                    final_vm['nid'] = len(servers)
                    if n['replica_group'] in replica_groups.keys():
                        replica_groups[n['replica_group']].append(final_vm['nid'])
                    else:
                        replica_groups[n['replica_group']] = [final_vm['nid']]
                    servers.append(final_vm)

                elif group == 'clients':
                    final_vm['nid'] = len(clients)+len(servers)
                    clients.append(final_vm)

                elif group == 'zookeeper':
                    final_vm['port'] = n['port']
                    zookeepers.append(final_vm)

                else:
                    # default is server group
                    default_group.append(final_vm)
            else:
                print("Error in create instance.")
                pprint(err)

        # TODO(tq): we are writing two files with the same content but different format
        # Can we consolidate that to one?

        # Create hosts file for Ansible
        with open('./{}/hosts'.format(args[0]), 'w') as hosts_file:

            for n in default_group:
                print("{} int_ip={} vm_name={}".format(n['address'], n['int_ip'], n['conf']['name']), file=hosts_file)

            if len(servers) > 0:
                print("[servers]", file=hosts_file)
            for n in servers:
                print("{} int_ip={} vm_name={}".format(n['address'], n['int_ip'], n['conf']['name']), file=hosts_file)

            if len(clients) > 0:
                print("[clients]", file=hosts_file)
            for n in clients:
                print("{} int_ip={} vm_name={}".format(n['address'], n['int_ip'], n['conf']['name']), file=hosts_file)

            if len(zookeepers) > 0:
                print("[zookeeper]", file=hosts_file)
            for n in zookeepers:
                print("{} int_ip={} vm_name={}".format(n['address'], n['int_ip'], n['conf']['name']), file=hosts_file)

        # Create ifconfig.txt for ExpoDB and update node objects with nid value
        servers, clients = create_ifconfig_file("..", servers, clients)

        #Add replica mapping
        servers = add_replica_mapping(servers, replica_groups)

        # Create site_deploy_details.yml
        try:
            with open("../conf/site_deploy_details.yml", "w") as site_deploy:
                doc = {}
                doc['servers'] = servers
                doc['clients'] = clients
                doc['zookeeper'] = zookeepers
                doc['replica_groups'] = replica_groups
                yaml.dump(doc, site_deploy)
        except Exception as e:
            print_error("Could not dump YAML site_deploy file")
            print_error(e)

        # Create site_deploy.yml
        try:
            with open("../conf/site_deploy.yml", "w") as site_deploy:
                doc = {}
                doc['servers'] = simplify_servers(servers)
                doc['clients'] = simplify_clients(clients)
                doc['zookeeper'] = simplify_zk(zookeepers)
                yaml.dump(doc, site_deploy)
        except Exception as e:
            print_error("Could not dump YAML site_deploy file")
            print_error(e)


if __name__ == "__main__":
    main(sys.argv[1:])
