# Unified wrapper for running client or server processes
## Assumes that every ip in ifconfig.txt is unique
# Using ifconfig file

import os,sys
import yaml

def main(args):
    nid = 0
    is_server = True

    #TODO(tq): check string format for IP address
    ip = args[0]

    with open("ifconfig.txt", "r") as ifconfig_file:
        for line in ifconfig_file:
            if line.strip() == ip:
                break
            else:
                nid = nid + 1

    # For now, assume the node is a server unless found in clients group
    # TODO(tq): need to proparly check if the IP is specified in the server group as well
    with open("conf/site_deploy.yml", "r") as site_deploy_file:
        # doc = yaml.full_load(site_deploy_file)
        doc = yaml.load(site_deploy_file, Loader=yaml.FullLoader)

        for n in doc["clients"]:
            if n["int_ip"] == ip:
                is_server = False
                break

    print("Working Dir.: {}".format(os.getcwd()))
    rc = 0
    if is_server:
        ntype = "Server"
        print("Node IP: {}, nid={}, node_type={}".format(ip, nid,ntype))
        rc = os.system("./bin/rundb -nid{}".format(nid))

    else:
        ntype = "Client"
        print("Node IP: {}, nid={}, node_type={}".format(ip, nid,ntype))
        rc = os.system("./bin/runcl -nid{}".format(nid))

    print("RC = {}".format(rc))
    if rc > 0:
        print("FAILED", file=sys.stderr)
    return rc

if __name__ == "__main__":
    main(sys.argv[1:])