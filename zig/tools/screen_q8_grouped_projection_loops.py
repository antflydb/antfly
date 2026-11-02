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

"""Compare Q8K projection loops against unchanged kernels in a private overlay.

The generated probes call whole worker ranges through dynamic C entry points.
Only the projection loop is changed; SIMD lane unrolling and accumulation stay
intact. Fixture packing is outside the timed region. Numerical checks cover
pair/triple projections, MR4/MR2/MR1, all panels, biases and partial ranges.
Runtime results are synthetic screening, not model-serving qualification.
"""

import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

repo = Path(__file__).resolve().parents[2]
zig = repo / "zig"
sys.path.insert(0, str(zig / "tools"))
sys.path.insert(0, str(repo / "scripts"))
from check_architecture_probe_cache import link_children, own  # noqa: E402
from report_zig_archive_work import parse_text_symbols  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument(
        "--mode",
        choices=("all", "mr4", "mr4-split", "mr4-split-guarded"),
        default="mr4",
    )
    parser.add_argument("--cpu", default="baseline")
    parser.add_argument("--in-dim", type=int, default=1536)
    parser.add_argument("--out-dim", type=int, default=512)
    parser.add_argument(
        "--fused-sdot", choices=("default", "on", "off"), default="default"
    )
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument(
        "--iterations",
        type=int,
        default=50,
        help="Iterations per 32 rows (scaled up for short rows)",
    )
    parser.add_argument("--repeats", type=int, default=4)
    args = parser.parse_args()
    if min(args.iterations, args.repeats, args.timeout) < 1:
        parser.error("iterations, repeats and timeout must be positive")
    if args.in_dim < 256 or args.in_dim % 256 or args.out_dim < 16 or args.out_dim % 16:
        parser.error(
            "in-dim must be a positive multiple of 256; out-dim a positive multiple of 16"
        )
    report_path = args.report.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    parent = zig / ".zig-cache/q8-grouped-overlays"
    parent.mkdir(parents=True, exist_ok=True)
    compiler = Path(shutil.which("zig") or "zig").resolve()
    digest = hashlib.sha256()
    with compiler.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    report = dict(
        compiler=dict(
            path=str(compiler),
            sha256=digest.hexdigest(),
            version=subprocess.check_output(
                [str(compiler), "version"], text=True
            ).strip(),
        ),
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        scope="synthetic numerical, runtime and emitted-work screen; no production qualification",
        passed=False,
        mode=args.mode,
        in_dim=args.in_dim,
        out_dim=args.out_dim,
        cpu=args.cpu,
        fused_sdot_override=args.fused_sdot,
        iterations=args.iterations,
        repeats=args.repeats,
    )
    with tempfile.TemporaryDirectory(dir=parent) as directory:
        root = Path(directory) / "zig"
        link_children(zig, root)
        source = own(root, "pkg/inference/src/ops/native_compute.zig")
        original = source.read_text()
        report["source_sha256"] = hashlib.sha256(original.encode()).hexdigest()
        start = original.index("fn linearQ8KQ8KActivationGroupedRange(")
        stop = original.index("\nfn q8KQ8KActivationPairApply(", start)
        function = original[start:stop]
        split = function.index("    var r: usize = 0;")
        end = (
            function.index("    while (r + 2 <= rows)")
            if args.mode.startswith("mr4")
            else len(function)
        )
        looped = (
            function[:split]
            + function[split:end].replace(
                "inline for (0..projection_count)", "for (0..projection_count)"
            )
            + function[end:]
        )
        report["projection_loops_changed"] = function.count(
            "inline for (0..projection_count)"
        ) - looped.count("inline for (0..projection_count)")
        expected_changes = 12 if args.mode.startswith("mr4") else 36
        if report["projection_loops_changed"] != expected_changes:
            raise RuntimeError(
                "kernel layout changed; review the projection-loop transformation"
            )
        looped = looped.replace(
            "fn linearQ8KQ8KActivationGroupedRange(",
            "fn linearQ8KQ8KActivationGroupedRangeLoopedProbe(",
            1,
        )
        if args.mode.startswith("mr4-split"):
            helper_name = "linearQ8KQ8KActivationGroupedRangeMR4LoopedProbe"
            helper = (
                function[:split]
                .replace(
                    "fn linearQ8KQ8KActivationGroupedRange(",
                    f"noinline fn {helper_name}(",
                    1,
                )
                .replace(") void {", ") usize {", 1)
            )
            helper += (
                function[split:end].replace(
                    "inline for (0..projection_count)", "for (0..projection_count)"
                )
                + "    return r;\n}\n"
            )
            call_expr = f"{helper_name}(projection_count, q_input, weight_raw, panel_blocks, panel8_blocks, panel16_blocks, outputs, biases, rows, in_dim, out_dim, out_start, out_end)"
            if args.mode == "mr4-split-guarded":
                call_expr = f"if (rows >= 4) {call_expr} else 0"
            call = f"    var r: usize = {call_expr};\n"
            looped = (
                (function[:split] + call + function[end:]).replace(
                    "fn linearQ8KQ8KActivationGroupedRange(",
                    "fn linearQ8KQ8KActivationGroupedRangeLoopedProbe(",
                    1,
                )
                + "\n"
                + helper
            )
        test_start = original.index(
            'test "q8_k activation grouped panel kernels match separate sibling matmuls"'
        )
        test_stop = original.index("\ntest ", test_start + 1)
        fixture = original[test_start:test_stop]
        # Keep the established numerical assertions and fixture preparation, then
        # benchmark complete dynamic worker ranges through externally callable
        # wrappers. These wrappers prevent constant argument specialization of the
        # isolated candidate from masquerading as a production code-size saving.
        fixture = fixture.replace(
            'test "q8_k activation grouped panel kernels match separate sibling matmuls"',
            'test "q8 grouped projection loop isolated screening"',
            1,
        )
        if (
            fixture.count("const rows = 9;") != 1
            or fixture.count("const in_dim = 768;") != 1
        ):
            raise RuntimeError("kernel fixture changed; review matrix dimensions")
        if fixture.count("const out_dim = 512;") != 1:
            raise RuntimeError("kernel fixture changed; review output dimension")
        fixture = (
            fixture.replace("const rows = 9;", "const rows = 33;")
            .replace("const in_dim = 768;", f"const in_dim = {args.in_dim};")
            .replace("const out_dim = 512;", f"const out_dim = {args.out_dim};")
        )
        input_start = fixture.index("    for (&input, 0..)")
        input_end = fixture.index("    const weight_f32_a", input_start)
        fixture = (
            fixture[:input_start]
            + """    for (&input, 0..) |*value, idx| {
        const signed: i32 = @intCast((idx * 13 + 7) % 43);
        value.* = @as(f32, @floatFromInt(signed - 21)) * 0.03125;
    }

"""
            + fixture[input_end:]
        )
        prelude = '\n    std.debug.print("Q8_GROUPED_PROFILE dotprod={d} fused_sdot={d}\\n", .{@intFromBool(native_aarch64_dotprod), @intFromBool(q8KFusedSdotEnabled())});\n'
        if args.fused_sdot != "default":
            enabled = "true" if args.fused_sdot == "on" else "false"
            prelude = (
                f"\n    setQ8KFusedSdotOverrideForBench({enabled});\n    defer setQ8KFusedSdotOverrideForBench(null);\n"
                + prelude
            )
        first_brace = fixture.index("{") + 1
        fixture = fixture[:first_brace] + prelude + fixture[first_brace:]
        insert = fixture.rfind("}")
        benchmark = r"""
        const q_base = try quant_codec.quantizeQ8_KFromF32(allocator, &input);
        defer allocator.free(q_base);
        var q_inputs: [3][]u8 = undefined;
        for (&q_inputs, 0..) |*slot, index| {
            slot.* = try allocator.dupe(u8, q_base);
            const factor: f32 = 1.0 + @as(f32, @floatFromInt(index)) * 0.01;
            for (0..slot.len / 292) |block| {
                const off = block * 292;
                const scale: f32 = @bitCast(std.mem.readInt(u32, slot.*[off..][0..4], .little));
                std.mem.writeInt(u32, slot.*[off..][0..4], @bitCast(scale * factor), .little);
            }
        }
        defer for (q_inputs) |q| allocator.free(q);
        var request = Q8GroupedProbeRequest{
            .input = .fromSlice(q_inputs[0]),
            .weights = .{ .fromSlice(storage_a.raw_bytes), .fromSlice(storage_b.raw_bytes), .fromSlice(storage_c.raw_bytes) },
            .panel4 = .{ .fromOptional(storage_a.preparedBytes(.panel4)), .fromOptional(storage_b.preparedBytes(.panel4)), .fromOptional(storage_c.preparedBytes(.panel4)) },
            .panel8 = .{ .fromOptional(storage_a.preparedBytes(.panel8)), .fromOptional(storage_b.preparedBytes(.panel8)), .fromOptional(storage_c.preparedBytes(.panel8)) },
            .panel16 = .{ .fromOptional(storage_a.preparedBytes(.panel16)), .fromOptional(storage_b.preparedBytes(.panel16)), .fromOptional(storage_c.preparedBytes(.panel16)) },
            .outputs = .{ .{ .ptr = &triple_a, .len = triple_a.len }, .{ .ptr = &triple_b, .len = triple_b.len }, .{ .ptr = &triple_c, .len = triple_c.len } },
            .biases = .{ .{ .ptr = &bias_a, .len = bias_a.len }, .{ .ptr = &bias_b, .len = bias_b.len }, .{ .ptr = &bias_c, .len = bias_c.len } },
            .rows = rows, .in_dim = in_dim, .out_dim = out_dim, .start = 0, .end = out_dim,
        };
        // Exercise all row microtiles, panel fallbacks, partial worker ranges and
        // null biases. Each fixture compares the same dynamic calls in both modes.
        for ([_]usize{1, 2, 3, 4, 9, 33}) |case_rows| {
            for (0..4) |panel| {
                for (0..2) |partial| {
                    request.rows = case_rows;
                    request.start = if (partial == 0) 0 else 3;
                    request.end = if (partial == 0) out_dim else out_dim - 5;
                    inline for (0..3) |index| {
                        request.panel16[index] = if (panel == 3) .fromOptional(switch (index) {0 => storage_a.preparedBytes(.panel16), 1 => storage_b.preparedBytes(.panel16), else => storage_c.preparedBytes(.panel16)}) else .fromOptional(null);
                        request.panel8[index] = if (panel >= 2) .fromOptional(switch (index) {0 => storage_a.preparedBytes(.panel8), 1 => storage_b.preparedBytes(.panel8), else => storage_c.preparedBytes(.panel8)}) else .fromOptional(null);
                        request.panel4[index] = if (panel >= 1) .fromOptional(switch (index) {0 => storage_a.preparedBytes(.panel4), 1 => storage_b.preparedBytes(.panel4), else => storage_c.preparedBytes(.panel4)}) else .fromOptional(null);
                        request.biases[index] = if (partial == 0) .{.ptr = null, .len = 0} else .{.ptr = switch (index) {0 => &bias_a, 1 => &bias_b, else => &bias_c}, .len = out_dim};
                    }
                    for (q_inputs) |q| {
                        request.input = .fromSlice(q);
                        inline for (.{2, 3}) |count| {
                            @memset(&triple_a, -123.0);
                            @memset(&triple_b, -123.0);
                            @memset(&triple_c, -123.0);
                            if (count == 2) antfly_q8_probe_base2(&request) else antfly_q8_probe_base3(&request);
                            const expected_a = try allocator.dupe(f32, &triple_a);
                            defer allocator.free(expected_a);
                            const expected_b = try allocator.dupe(f32, &triple_b);
                            defer allocator.free(expected_b);
                            const expected_c = try allocator.dupe(f32, &triple_c);
                            defer allocator.free(expected_c);
                            @memset(&triple_a, -123.0);
                            @memset(&triple_b, -123.0);
                            @memset(&triple_c, -123.0);
                            if (count == 2) antfly_q8_probe_loop2(&request) else antfly_q8_probe_loop3(&request);
                            for (triple_a, expected_a) |actual, expected| try std.testing.expectApproxEqAbs(expected, actual, 1e-4);
                            for (triple_b, expected_b) |actual, expected| try std.testing.expectApproxEqAbs(expected, actual, 1e-4);
                            for (triple_c, expected_c) |actual, expected| try std.testing.expectApproxEqAbs(expected, actual, 1e-4);
                        }
                    }
                    // Time complete paired and triple workers, alternating order
                    // between repeats. Output checksums keep all calls observable.
                    if (partial != 0) continue;
                    for (0..4) |repeat| {
                        for (0..2) |order| {
                            const loop_variant = (order + repeat) % 2 == 1;
                            var checksum: f64 = 0;
                            const begin = platform.time.monotonicNs();
                            for (0..(50 * @max(@as(usize, 1), 32 / case_rows))) |iteration| {
                                request.input = .fromSlice(q_inputs[iteration % q_inputs.len]);
                                if (loop_variant) antfly_q8_probe_loop2(&request) else antfly_q8_probe_base2(&request);
                                checksum += @as(f64, triple_a[iteration % out_dim]) + @as(f64, triple_b[iteration % out_dim]);
                                if (loop_variant) antfly_q8_probe_loop3(&request) else antfly_q8_probe_base3(&request);
                                checksum += @as(f64, triple_c[iteration % out_dim]);
                            }
                            std.debug.print("Q8_GROUPED_MATRIX rows={d} panel={d} repeat={d} loop={d} ns={d} checksum={d}\n", .{case_rows, panel, repeat, @intFromBool(loop_variant), platform.time.monotonicNs() - begin, checksum});
                        }
                    }
                }
            }
        }
    """
        benchmark = benchmark.replace("50 * @max", f"{args.iterations} * @max").replace(
            "for (0..4) |repeat|", f"for (0..{args.repeats}) |repeat|"
        )
        fixture = fixture[:insert] + benchmark + fixture[insert:]
        interface = r"""
    const Q8ProbeBytes = extern struct {
        ptr: ?[*]const u8, len: usize,
        fn fromSlice(bytes: []const u8) @This() { return .{ .ptr = bytes.ptr, .len = bytes.len }; }
        fn fromOptional(bytes: ?[]const u8) @This() { return if (bytes) |b| .fromSlice(b) else .{ .ptr = null, .len = 0 }; }
        fn slice(self: @This()) []const u8 { return self.ptr.?[0..self.len]; }
        fn optional(self: @This()) ?[]const u8 { return if (self.ptr) |p| p[0..self.len] else null; }
    };
    const Q8ProbeOutput = extern struct { ptr: [*]f32, len: usize };
    const Q8ProbeBias = extern struct { ptr: ?[*]const f32, len: usize };
    const Q8GroupedProbeRequest = extern struct {
        input: Q8ProbeBytes, weights: [3]Q8ProbeBytes,
        panel4: [3]Q8ProbeBytes, panel8: [3]Q8ProbeBytes, panel16: [3]Q8ProbeBytes,
        outputs: [3]Q8ProbeOutput, biases: [3]Q8ProbeBias,
        rows: usize, in_dim: usize, out_dim: usize, start: usize, end: usize,
    };
    fn runQ8GroupedProbe(comptime loop_variant: bool, comptime count: usize, req: *const Q8GroupedProbeRequest) void {
        var weights: [count][]const u8 = undefined;
        var panel4: [count]?[]const u8 = undefined;
        var panel8: [count]?[]const u8 = undefined;
        var panel16: [count]?[]const u8 = undefined;
        var outputs: [count][]f32 = undefined;
        var biases: [count]?[]const f32 = undefined;
        inline for (0..count) |index| {
            weights[index] = req.weights[index].slice();
            panel4[index] = req.panel4[index].optional();
            panel8[index] = req.panel8[index].optional();
            panel16[index] = req.panel16[index].optional();
            outputs[index] = req.outputs[index].ptr[0..req.outputs[index].len];
            biases[index] = if (req.biases[index].ptr) |p| p[0..req.biases[index].len] else null;
        }
        const implementation = if (loop_variant) linearQ8KQ8KActivationGroupedRangeLoopedProbe else linearQ8KQ8KActivationGroupedRange;
        implementation(count, req.input.slice(), weights, panel4, panel8, panel16, outputs, biases, req.rows, req.in_dim, req.out_dim, req.start, req.end);
    }
    export fn antfly_q8_probe_base2(req: *const Q8GroupedProbeRequest) callconv(.c) void { runQ8GroupedProbe(false, 2, req); }
    export fn antfly_q8_probe_base3(req: *const Q8GroupedProbeRequest) callconv(.c) void { runQ8GroupedProbe(false, 3, req); }
    export fn antfly_q8_probe_loop2(req: *const Q8GroupedProbeRequest) callconv(.c) void { runQ8GroupedProbe(true, 2, req); }
    export fn antfly_q8_probe_loop3(req: *const Q8GroupedProbeRequest) callconv(.c) void { runQ8GroupedProbe(true, 3, req); }
    """
        source.write_text(original + "\n" + looped + "\n" + interface + "\n" + fixture)
        report_path.with_suffix(".patch").write_text(
            "".join(
                difflib.unified_diff(
                    original.splitlines(keepends=True),
                    source.read_text().splitlines(keepends=True),
                    fromfile="native_compute.zig",
                    tofile="screen/native_compute.zig",
                )
            )
        )
        command = [
            "python3",
            "tools/run_bounded_zig_build.py",
            "--",
            "build",
            "inference-test-inference-unit",
            "-Doptimize=fast",
            "-Dstrip=true",
            f"-Dcpu={args.cpu}",
            "-Dmetal=false",
            "-Dcuda=false",
            "-Dsystem-blas=false",
            "-Dtest-filter=q8 grouped projection loop isolated screening",
            "-Dtest-filter=q8_k q8_k mr4 dot matches narrower tiles",
            "--summary",
            "all",
        ]
        begin = time.monotonic()
        process = subprocess.Popen(
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
            output, _ = process.communicate(timeout=args.timeout)
        except BaseException:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                output, _ = process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                output, _ = process.communicate()
            report_path.with_suffix(".build.log").write_text(output)
            report.update(
                returncode=process.returncode,
                interrupted=True,
                elapsed_s=time.monotonic() - begin,
                command=command,
            )
            report_path.write_text(json.dumps(report, indent=2) + "\n")
            raise
        report_path.with_suffix(".build.log").write_text(output)
        report.update(
            returncode=process.returncode,
            elapsed_s=time.monotonic() - begin,
            command=command,
        )
        report["samples"] = [
            dict(
                rows=int(a),
                panel=int(b),
                repeat=int(c),
                loop=bool(int(d)),
                ns=int(e),
                checksum=float(f),
            )
            for a, b, c, d, e, f in re.findall(
                r"Q8_GROUPED_MATRIX rows=(\d+) panel=(\d+) repeat=(\d+) loop=(\d+) ns=(\d+) checksum=([-+\d.eE]+)",
                output,
            )
        ]
        profile = re.search(r"Q8_GROUPED_PROFILE dotprod=(\d) fused_sdot=(\d)", output)
        report["effective_features"] = (
            dict(dotprod=bool(int(profile[1])), fused_sdot=bool(int(profile[2])))
            if profile
            else None
        )
        report["objects"] = []
        nm = shutil.which("llvm-nm")
        if nm is None and Path("/opt/homebrew/opt/llvm/bin/llvm-nm").is_file():
            nm = "/opt/homebrew/opt/llvm/bin/llvm-nm"
        if not process.returncode:
            # Mach-O test executables are stripped; the emitted ZCU objects retain
            # names. Attribute those extents rather than comparing executable files.
            for candidate in (root / ".zig-cache/o").glob("*/*_zcu.o") if nm else ():
                if candidate.stat().st_size < 100_000:
                    continue
                result = subprocess.run(
                    [
                        nm,
                        "--defined-only",
                        "--print-size",
                        "--radix=d",
                        "--format=posix",
                        "--numeric-sort",
                        "--print-file-name",
                        str(candidate),
                    ],
                    capture_output=True,
                    text=True,
                )
                symbols, estimated = parse_text_symbols(result.stdout)
                selected = [
                    row
                    for row in symbols
                    if "linearQ8KQ8KActivationGroupedRange" in row["name"]
                ]
                if selected:
                    for row in selected:
                        row["object"] = str(candidate.relative_to(root))
                    report["objects"].append(
                        dict(name=candidate.name, estimated=estimated, symbols=selected)
                    )
            report["passed"] = (
                profile is not None
                and len(report["samples"]) == 6 * 4 * args.repeats * 2
            )
            report["cases"] = []
            for rows in (1, 2, 3, 4, 9, 33):
                for panel in range(4):
                    samples = [
                        x
                        for x in report["samples"]
                        if x["rows"] == rows and x["panel"] == panel
                    ]
                    base = [x["ns"] for x in samples if not x["loop"]]
                    loop = [x["ns"] for x in samples if x["loop"]]
                    if not base or not loop:
                        report["passed"] = False
                        continue
                    report["cases"].append(
                        dict(
                            rows=rows,
                            panel=panel,
                            loop_to_base_median=statistics.median(loop)
                            / statistics.median(base),
                        )
                    )
                    for repeat in range(args.repeats):
                        pair = [x for x in samples if x["repeat"] == repeat]
                        if len(pair) != 2 or pair[0]["checksum"] != pair[1]["checksum"]:
                            report["passed"] = False
        report_path.write_text(json.dumps(report, indent=2) + "\n")
        print(
            json.dumps(
                {
                    k: v
                    for k, v in report.items()
                    if k not in ("objects", "command", "samples")
                },
                indent=2,
            ),
            flush=True,
        )
        if process.returncode:
            print(output[-8000:])

    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
