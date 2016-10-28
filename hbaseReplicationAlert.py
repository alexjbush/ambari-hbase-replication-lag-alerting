#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
# Alert for HBase Replication
#
# Alex Bush <abush@hortonworks.com>
#
import ambari_simplejson as json # simplejson is much faster comparing to Python 2.6 json module and has the same functions set.
import logging
import traceback
import subprocess
import os.path
import re

from resource_management.core.environment import Environment

PEER_ID_KEY = 'peer_id'
MAX_LOG_QUEUE_KEY = 'max_log_queue'
MAX_REPLICATION_LAG_KEY = 'max_replication_lag'

KERBEROS_KEYTAB = '{{cluster-env/smokeuser_keytab}}'
KERBEROS_PRINCIPAL = '{{cluster-env/smokeuser_principal_name}}'
SECURITY_ENABLED_KEY = '{{cluster-env/security_enabled}}'
SMOKEUSER_KEY = "{{cluster-env/smokeuser}}"
EXECUTABLE_SEARCH_PATHS = '{{kerberos-env/executable_search_paths}}'

CONNECTION_TIMEOUT_KEY = 'connection.timeout'
CONNECTION_TIMEOUT_DEFAULT = 5.0

logger = logging.getLogger('ambari_alerts')

def get_tokens():
  """
  Returns a tuple of tokens in the format {{site/property}} that will be used
  to build the dictionary passed into execute
  """
  return ( EXECUTABLE_SEARCH_PATHS, KERBEROS_KEYTAB, KERBEROS_PRINCIPAL, SECURITY_ENABLED_KEY, SMOKEUSER_KEY)

def execute(configurations={}, parameters={}, host_name=None):
  """
  Returns a tuple containing the result code and a pre-formatted result label
  Keyword arguments:
  configurations (dictionary): a mapping of configuration key to value
  parameters (dictionary): a mapping of script parameter key to value
  host_name (string): the name of this host where the alert is running
  """

  if configurations is None:
    return (('UNKNOWN', ['There were no configurations supplied to the script.']))

  # Set configuration settings

  if SMOKEUSER_KEY in configurations:
    smokeuser = configurations[SMOKEUSER_KEY]

  executable_paths = None
  if EXECUTABLE_SEARCH_PATHS in configurations:
    executable_paths = configurations[EXECUTABLE_SEARCH_PATHS]

  security_enabled = False
  if SECURITY_ENABLED_KEY in configurations:
    security_enabled = str(configurations[SECURITY_ENABLED_KEY]).upper() == 'TRUE'

  kerberos_keytab = None
  if KERBEROS_KEYTAB in configurations:
    kerberos_keytab = configurations[KERBEROS_KEYTAB]

  kerberos_principal = None
  if KERBEROS_PRINCIPAL in configurations:
    kerberos_principal = configurations[KERBEROS_PRINCIPAL]
    kerberos_principal = kerberos_principal.replace('_HOST', host_name)

  # Get the peer id from config
  #peer_id = parameters[PEER_ID_KEY]

  # Get the maximum log queue
  max_log_queue = parameters[MAX_LOG_QUEUE_KEY]

  # Get maximum replication lag permitted
  max_replication_lag = parameters[MAX_REPLICATION_LAG_KEY]

  try:
    # Kinit as smoketest user if kerberos enabled
    if security_enabled:
      base_command='kinit -kt '+kerberos_keytab+' '+kerberos_principal+'; '
    else:
      base_command=''
    # Run hbase shell command
    command=base_command+' echo -e "status \'replication\'\n" | hbase shell -n;'
    output = run_command(command)

    # Parse output with regex
    exp = '^\s*([a-zA-Z0-9.]+):\s*$[\n\r]+^\s*SOURCE:.*PeerID=([0-9]+).*SizeOfLogQueue=([0-9]+).*TimeStampsOfLastShippedOp=([^,]+).*Replication Lag=([0-9]+)$'
    regions = re.findall(exp,output,re.MULTILINE)

    # For each region server, check if there are any breaches
    breached = list()
    okay = list()
    for region in regions:
      if int(region[2])>int(max_log_queue):
        breached.append('Region server %s has breached SizeOfLogQueue limit of %s. Has values of: PeerID=%s SizeOfLogQueue=%s TimeStampsOfLastShippedOp=%s Replication Lag=%s' % (region[0],str(max_log_queue),region[1],region[2],region[3],region[4]))
      elif int(region[4])>int(max_replication_lag):
        breached.append('Region server %s has breached Replication Lag limit of %s. Has values of: PeerID=%s SizeOfLogQueue=%s TimeStampsOfLastShippedOp=%s Replication Lag=%s' % (region[0],str(max_replication_lag),region[1],region[2],region[3],region[4]))
      else:
        okay.append('Region server %s has values of: PeerID=%s SizeOfLogQueue=%s TimeStampsOfLastShippedOp=%s Replication Lag=%s' % (region[0],region[1],region[2],region[3],region[4]))
    if breached:
      raise Exception('The following region servers breached log/replication thresholds:\n'+'\n'.join(breached))
    else:
      return (('OK', ['All region servers within log/replication thresholds.']))

  except:
    return (('CRITICAL', [traceback.format_exc()]))


#Utility function for calling commands on the CMD
def run_command(command):
  p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
  (output, err) = p.communicate()
  if p.returncode:
    raise Exception('Command: '+command+' returned with non-zero code: '+str(p.returncode)+' stderr: '+err+' stdout: '+output)
  else:
    return output
