#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.

# Author: Phil Rosegay

# Basic readiness check for voltdb in k8s
# contact support

import sys
import os
import subprocess

# subprocess.check_output was introduced in 2.7, let's be sure.
if sys.hexversion < 0x02070000:
    raise Exception("Python version 2.7 or greater is required.")

HOST = os.environ["HOSTNAME"]
PORT = 21211

# @contextmanager
# def open_admin_client(host, port, username=None, password=None, enable_ssl=False):
#     pyclient = None
#     try:
#         pyclient = vclient(host=host,
#                                   port=port,
#                                   connect_timeout=60,
#                                   procedure_timeout=60,
#                                   default_timeout=60,
#                                   usessl=enable_ssl,
#                                   username=username,
#                                   password=password)
#         yield pyclient
#
#     except Exception as e:
#         raise Exception("[" + host + ":" + port + "]" + str(e))
#
#     finally:
#         if pyclient:
#             pyclient.close()
#
#
# def call_volt_procedure(proc, parms=[], bindvars=[]):
#     with open_admin_client(HOST, PORT) as client:
#         ph = VoltProcedure(client, proc, parms)
#         if type(bindvars) != list:
#             result = ph.call([bindvars])
#         else:
#             result = ph.call(bindvars)
#     if result.status != 1:
#         raise Exception(result)
#     return result
#
#
# result = call_volt_procedure("@PingPartitions", [vclient.VOLTTYPE_BIGINT], [0])
# print result

# PingPartitions will transactionally check all partitions for availibility, it returns 0 on success.
# nb. the cluster will appear not-ready if the database is paused.
cmd = """curl -sg http://localhost:8080/api/2.0/?Procedure=@PingPartitions\&Parameters=\[0\] \
        | jq '.status,.statusstring' \
        | xargs"""
r = subprocess.check_output(cmd, shell=True).strip()
if r[0] != "1":
        print r
        sys.exit(1)

# # DRROLE: find any/all clusters which are not actively replicating
# cmd = """curl -sg http://localhost:8080/api/1.0/?Procedure=@Statistics\&Parameters=\['DRROLE'\] | jq -cr '[.results[][]]'"""
# raw_drrole = subprocess.check_output(cmd, shell=True).strip()
# if len(raw_drrole) == 0:
#     sys.exit(0)
# drrole=eval(raw_drrole)
# if len(drrole) > 0:
#     for t in drrole:
#         print t
# if r != '"ACTIVE"':
#         print "Database replication is NOT syncing (%s)" % r
#         sys.exit(1)

# everything is ok
sys.exit(0)
