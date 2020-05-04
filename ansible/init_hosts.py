#!/bin/python

from os import environ
cloud_conf_file = environ.get("CEPLOY_CONF_FILE")
ceploy_home = environ.get("CEPLOY_HOME")

import sys
sys.path.append("{}/src".format(ceploy_home))


from ceploy.cloud import Cloud
from ceploy.constants import Provider


if cloud_conf_file is None:
    raise ValueError("Setting environemnt variable CEPLOY_CONF_FILE to point to the key file is required")


print(cloud_conf_file)
cloud = Cloud.make(Provider.GCLOUD, cloud_conf_file)




