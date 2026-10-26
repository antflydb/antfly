# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test-only constructor for a sealed full-context-release build receipt."""
import hashlib, json, os, shutil, subprocess, zipfile
from pathlib import Path
import release_attention_matching as release

def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def tools():
    result={}
    for name in ('python3','zig'):
        path=Path(shutil.which(name)).resolve()
        command=[str(path),'--version'] if name=='python3' else [str(path),'version']
        result[name]={'path':str(path),'sha256':sha(path),'bytes':path.stat().st_size,
                      'version':subprocess.check_output(command,text=True).strip()}
    return result

def write(root, source_path, binary_path):
    root, source_path, binary_path = Path(root), Path(source_path), Path(binary_path)
    binary_path.chmod(0o755)
    stem = source_path.stem
    stdout, stderr, resource, archive = (root/f"{stem}-{name}" for name in
        ("build.stdout","build.stderr","resource.json","source.zip"))
    stdout.write_bytes(b""); stderr.write_text("12 selected; 10 passed; 2 skipped.\n")
    command=["/usr/bin/env","ANTFLY_ZIG_MAX_RSS=8589934592","python3","tools/run_bounded_zig_build.py",
             "build","inference-test-context","--verbose","-j1","-Dmetal=true","-Dcuda=false","-Donnx=false","-Doptimize=ReleaseFast"]
    limits={"timeout_seconds":1800,"max_rss_mib":8192,"min_free_percent":15,
            "max_swap_growth_mib":512,"min_disk_free_mib":4096}
    resource.write_text(json.dumps({"schema":"antfly.benchmark_resources.v1","command":command,
      "elapsed_seconds":2,"initial_free_percent":70,"limits":limits,"min_disk_free_mib":5000,
      "min_free_percent":50,"pass":True,"peak_group_rss_mib":100,"pid":123,"returncode":0,
      "samples":4,"swapout_growth_mib":0,"violation":None},sort_keys=True)+"\n")
    source_name=f"zig/pkg/inference/src/{stem}-fixture.zig"; source_bytes=b"const fixture = true;\n"
    source={"contract":release.FULL_RELEASE_SOURCE_NOTE,"files":{source_name:hashlib.sha256(source_bytes).hexdigest()},"deleted":[],"gitlinks":{}}
    build={"cwd":"zig","command":command,"target":"inference-test-context","environment":release.FULL_RELEASE_ENVIRONMENT,"optimize":"ReleaseFast",
           "metal":True,"cuda":False,"onnx":False}
    tests={"selected":12,"passed":10,"skipped":2,"failed":0,"leaked":0}
    binary_sha=sha(binary_path)
    toolchain=tools(); manifest={"head":"a"*40,"source":source,"build":build,"tools":toolchain}
    with zipfile.ZipFile(archive,"w",compression=zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr(release.FULL_RELEASE_ARCHIVE_MANIFEST,json.dumps(manifest,indent=2,sort_keys=True)+"\n")
        bundle.writestr(source_name,source_bytes)
    rel=lambda p:str(Path(p).relative_to(root))
    receipt={"schema":"antfly.context-release-build.v1","evidence_scope":"full_context_release_build",
      "release_eligible":True,"serving_qualified":False,"created_utc":"2026-10-19T00:00:00Z",
      "sealed_utc":"2026-10-19T00:01:00Z","head":"a"*40,"zig_version":toolchain['zig']['version'],"tools":toolchain,
      "binary":{"path":rel(binary_path),"sha256":binary_sha,"bytes":binary_path.stat().st_size},
      "build":build,"resource":{"path":rel(resource),"sha256":sha(resource)},
      "stdout":{"path":rel(stdout),"sha256":sha(stdout)},"stderr":{"path":rel(stderr),"sha256":sha(stderr)},
      "tests":tests,"source":source,"source_archive":{"path":rel(archive),"sha256":sha(archive),
      "bytes":archive.stat().st_size,"files":1},"source_verified_after_build":True}
    source_path.write_text(json.dumps(receipt,sort_keys=True)+"\n")
    return sha(source_path)
