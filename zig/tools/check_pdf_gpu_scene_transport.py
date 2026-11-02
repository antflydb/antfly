#!/usr/bin/env python3
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

"""Compare GPU scene preparation through retained and source PDF readers.

Only CPU parsing and scene preparation run; device execution is not measured.
Private overlay output remains inside the worktree cache and is removed on exit.
"""

from pathlib import Path
import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time

repo = Path(__file__).resolve().parents[2]
zig = repo / "zig"
sys.path.insert(0, str(zig / "tools"))
from check_architecture_probe_cache import link_children, own  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument(
        "--optimize", choices=("debug", "fast", "safe"), default="debug"
    )
    args = parser.parse_args()
    if args.timeout < 1:
        parser.error("timeout must be positive")
    report_path = args.report.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    parent = zig / ".zig-cache/pdf-gpu-scene-overlays"
    parent.mkdir(parents=True, exist_ok=True)
    report = dict(
        source=subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repo, text=True
        ).strip(),
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        scope="CPU scene preparation through retained PDF owner versus direct source; GPU submission is unchanged and not measured",
        optimize=args.optimize,
        passed=False,
    )
    with tempfile.TemporaryDirectory(dir=parent) as directory:
        root = Path(directory) / "zig"
        link_children(zig, root)
        builder = own(root, "pkg/antfly/build/storage_owner_tests.zig")
        src = builder.read_text()
        pos = src.index("    if (imports.pdf_native_contract)")
        fixture = own(root, "pkg/antfly/src/pdf_native_owner_test_root.zig")
        fixture.write_text("""const std = @import("std");
 const pdf = @import("pdf");
 const gpu = @import("gpu_scene");
 test "PDF scene probe" {
     const a=std.testing.allocator;
     for ([_][]const u8{"scanned_table_fixture.pdf","simple_text_fixture.pdf","form_xobject_text_fixture.pdf"}) |name| {
         const path=try std.fmt.allocPrint(a,"lib/pdf/testdata/{s}",.{name}); defer a.free(path);
         const bytes=try std.Io.Dir.cwd().readFileAlloc(std.testing.io,path,a,.limited(4*1024*1024)); defer a.free(bytes);
         var reader=try pdf.reader.Reader.init(a,bytes); defer reader.deinit();
         var scene=gpu.prepare(a,&reader,.{.page_number=1,.requested_dpi=72,.max_dimension=256}) catch |err| {
             std.debug.print("PDF_SCENE mode={s} fixture={s} error={s}\\n",.{@import("probe_options").mode,name,@errorName(err)});
             continue;
         };
         defer scene.deinit(a);
         var hash=std.crypto.hash.sha2.Sha256.init(.{});
         hash.update(std.mem.asBytes(&scene.width)); hash.update(std.mem.asBytes(&scene.height)); hash.update(std.mem.asBytes(&scene.dpi));
         hash.update(std.mem.sliceAsBytes(scene.commands)); hash.update(std.mem.sliceAsBytes(scene.points)); hash.update(scene.pixels);
         const digest=hash.finalResult();
         std.debug.print("PDF_SCENE mode={s} fixture={s} digest={x}\\n",.{@import("probe_options").mode,name,&digest});
     }
 }
 """)
        added = """    if (imports.pdf_implementation) |source_pdf| {
         const step=b.step("pdf-scene-probe", "Compare source and retained PDF scene preparation");
         inline for (.{false,true}) |native| {
             const selected=if (native) imports.pdf else source_pdf;
             const opts=b.addOptions(); opts.addOption(bool,"enable_metal",false);
             const gpu=b.createModule(.{.root_source_file=b.path("pkg/inference/src/bench/pdf_gpu.zig"),.target=target,.optimize=optimize});
             gpu.addImport("antfly_pdf",selected); gpu.addOptions("build_options",opts);
             const module=b.createModule(.{.root_source_file=b.path("pkg/antfly/src/pdf_native_owner_test_root.zig"),.target=target,.optimize=optimize,.link_libc=true});
             module.addImport("pdf",selected); module.addImport("gpu_scene",gpu);
             const probe=b.addOptions();probe.addOption([]const u8,"mode",if(native) "native" else "source");module.addOptions("probe_options",probe);
             @import("../../../build_support/embedded/source_owner.zig").registerStorageIndependent(module);
             const tests=@import("linked_tests.zig").add(b,.{.name=if(native) "pdf-scene-native" else "pdf-scene-source",.root_module=module,.filters=&.{"PDF scene probe"},.max_rss=2*1024*1024*1024});
             if(native) tests.executable.root_module.linkLibrary(artifacts[@backingInt(runtime.RuntimeLibraryUnit.enrichment_compute)].?);
             step.dependOn(&tests.run(b).step);
         }
     }
 """
        builder.write_text(src[:pos] + added + src[pos:])
        command = [
            "python3",
            "tools/run_bounded_zig_build.py",
            "--",
            "build",
            "pdf-scene-probe",
            "-Dshared-pdf-sessions=true",
            f"-Doptimize={args.optimize}",
            f"-Dstrip={str(args.optimize != 'debug').lower()}",
            "-Dcpu=baseline",
            "-Dmetal=false",
            "-Dcuda=false",
            "-Dsystem-blas=false",
            "--summary",
            "all",
        ]
        begin = time.monotonic()
        p = subprocess.Popen(
            command,
            cwd=root,
            env=dict(
                os.environ,
                ZIG_LOCAL_CACHE_DIR=str(root / ".zig-cache"),
                ZIG_GLOBAL_CACHE_DIR=str(zig / ".zig-cache/global"),
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        try:
            output, _ = p.communicate(timeout=args.timeout)
        except BaseException:
            os.killpg(p.pid, signal.SIGTERM)
            try:
                p.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(p.pid, signal.SIGKILL)
                p.communicate()
            raise
        report_path.with_suffix(".build.log").write_text(output)
        values = {}
        for mode, name, value in re.findall(
            r"PDF_SCENE mode=(native|source) fixture=(\S+) (\S+)", output
        ):
            values.setdefault(name, {})[mode] = value
        report.update(
            returncode=p.returncode,
            elapsed_s=time.monotonic() - begin,
            command=command,
            fixtures=values,
        )
        report["passed"] = (
            p.returncode == 0
            and len(values) == 3
            and all(v.get("native") == v.get("source") for v in values.values())
            and any(v.get("native", "").startswith("digest=") for v in values.values())
        )
        report_path.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
        if not report["passed"]:
            print(output[-4000:])
            raise SystemExit(1)


if __name__ == "__main__":
    main()
