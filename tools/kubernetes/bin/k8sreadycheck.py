#!/usr/bin/env python
import subprocess
import sys
cmd = "curl -sg http://localhost:8080/api/1.0/?Procedure=@PingPartitions\&Parameters=\[0\] | jq '.status,.statusstring' | xargs"
r = subprocess.check_output(cmd, shell=True).strip()
print r
sys.exit(int(r.split(' ',1)[0] != "1"))
