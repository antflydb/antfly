# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Extract actual private fixture IPs from Docker inspect, without printing env."""

import json
import sys

ports = {
    "antfly-maintenance-nessie": 19120,
    "antfly-maintenance-polaris": 8181,
    "antfly-maintenance-s3": 9000,
}
result = []
seen = set()
for container in json.load(sys.stdin):
    name = container["Name"].lstrip("/")
    if name not in ports:
        raise ValueError("unexpected fixture container")
    seen.add(name)
    for network in container["NetworkSettings"]["Networks"].values():
        if network["IPAddress"]:
            result.append([network["IPAddress"], ports[name]])
if seen != ports.keys() or len(result) < 3:
    raise ValueError("all three private fixture services must be running")
print(json.dumps(result))
