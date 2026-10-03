#!/usr/bin/env python3
"""Exercise optional OpenBLAS loading, fallback, ABI checks and bounded threading.

Uses tiny shared-library fixtures, not a system OpenBLAS installation. Pass
--openblas-library to additionally exercise a real pthread LP64 OpenBLAS.
"""

import argparse
import os
from pathlib import Path
import subprocess
import tempfile

FIXTURE = r"""
#ifndef BAD_ABI
#define BAD_ABI 0
#endif
#ifndef PARALLEL
#define PARALLEL 1
#endif
static int threads;
const char *openblas_get_config(void) { return BAD_ABI ? "OpenBLAS USE64BITINT" : "OpenBLAS test LP64"; }
int openblas_get_parallel(void) { return PARALLEL; }
void openblas_set_num_threads(int n) { threads = n; }
int openblas_get_num_threads(void) { return threads; }
#ifndef MISSING_SGEMM
void cblas_sgemm(int layout, int ta, int tb, int m, int n, int k,
                float alpha, const float *a, int lda, const float *b, int ldb,
                float beta, float *c, int ldc) {
    if (layout != 101) __builtin_trap();
    for (int i=0; i<m; ++i) for (int j=0; j<n; ++j) {
        float sum=0;
        for (int p=0; p<k; ++p)
            sum += a[ta == 111 ? i*lda+p : p*lda+i] * b[tb == 111 ? p*ldb+j : j*ldb+p];
        c[i*ldc+j] = alpha*sum + (beta == 0 ? 0 : beta*c[i*ldc+j]);
    }
}
#endif
"""
PROBE = r"""
const std = @import("std");
const native = @import("native");
const linalg = @import("inference_linalg");
test "concurrent first use publishes one complete BLAS policy" {
    const expected = std.mem.eql(u8, std.mem.span(std.c.getenv("EXPECT_BLAS").?), "true");
    var runtime = std.Io.Threaded.init(std.testing.allocator, .{});
    defer runtime.deinit();
    var group: std.Io.Group = .init;
    defer group.cancel(runtime.io());
    var results: [16]bool = undefined;
    for (&results) |*result| group.async(runtime.io(), struct {
        fn run(out: *bool) void { out.* = native.useBlas(); }
    }.run, .{result});
    try group.await(runtime.io());
    for (results) |result| try std.testing.expectEqual(expected, result);
    if (expected) {
        const count = try std.fmt.parseInt(usize, std.mem.span(std.c.getenv("EXPECT_THREADS").?), 10);
        if (count == 0) {
            try std.testing.expect(native.openblas.threadLimit() == null);
        } else try std.testing.expectEqual(@min(count, linalg.pool.cachedCpuCount()), native.openblas.threadLimit().?);
    }
}
test "concurrent matrix calls keep independent outputs" {
    var runtime = std.Io.Threaded.init(std.testing.allocator, .{});
    defer runtime.deinit();
    var group: std.Io.Group = .init;
    defer group.cancel(runtime.io());
    var results: [8]bool = undefined;
    for (&results) |*result| group.async(runtime.io(), struct {
        fn run(ok: *bool) void {
            const a = [_]f32{1} ** (64 * 128);
            const b = [_]f32{1} ** (128 * 64);
            var out: [64 * 64]f32 = undefined;
            native.sgemmSync(64, 64, 128, 1, &a, &b, 0, &out);
            ok.* = true;
            for (out) |value| if (value != 128) { ok.* = false; };
        }
    }.run, .{result});
    try group.await(runtime.io());
    for (results) |ok| try std.testing.expect(ok);
}
test "GEMM transpose variants preserve alpha beta and layout" {
    const a = [_]f32{1,2,3,4,5,6}; // 2 x 3
    const at = [_]f32{1,4,2,5,3,6};
    const b = [_]f32{7,8,9,10,11,12}; // 3 x 2
    const bt = [_]f32{7,9,11,8,10,12};
    const expected = [_]f32{29.5,32.5,70,77.5};
    var out = [_]f32{2} ** 4;
    native.sgemmSync(2,2,3,0.5,&a,&b,0.25,&out);
    for (out,expected) |actual,want| try std.testing.expectApproxEqAbs(want,actual,1e-5);
    out = .{2,2,2,2};
    native.sgemmTransBSync(2,2,3,0.5,&a,&bt,0.25,&out);
    for (out,expected) |actual,want| try std.testing.expectApproxEqAbs(want,actual,1e-5);
    out = .{2,2,2,2};
    native.sgemmTransA(2,2,3,0.5,&at,&b,0.25,&out);
    for (out,expected) |actual,want| try std.testing.expectApproxEqAbs(want,actual,1e-5);
}
test "empty reductions scale output without reading inputs" {
    var out = [_]f32{4} ** 6;
    native.sgemmSync(2,3,0,1,&.{},&.{},0.5,&out);
    for (out) |v| try std.testing.expectEqual(@as(f32,2),v);
    native.sgemmTransBSync(2,3,0,1,&.{},&.{},0.5,&out);
    for (out) |v| try std.testing.expectEqual(@as(f32,1),v);
    native.sgemmTransA(2,3,0,1,&.{},&.{},0,&out);
    for (out) |v| try std.testing.expectEqual(@as(f32,0),v);
    native.sgemmSync(0,0,0,1,&.{},&.{},0,&.{});
}
test "strided output preserves padding and empty reduction" {
    const a = [_]f32{ 1, -2, 3, 4, 5, -6 };
    const b = [_]f32{ 1, 2, 3, -4, 5, 6, 7, 8, -9 };
    var out = [_]f32{17} ** 13;
    try native.sgemmTransBStrided(null, 2, 3, 3, 0.5, &a, &b, 0.25, out[1..], 6);
    for (0..2) |row| for (0..3) |col| {
        var expected: f32 = 17 * 0.25;
        for (0..3) |k| expected += 0.5 * a[row*3+k] * b[col*3+k];
        try std.testing.expectApproxEqAbs(expected, out[1+row*6+col], 1e-5);
    };
    for ([_]usize{0,4,5,6,10,11,12}) |i| try std.testing.expectEqual(@as(f32,17),out[i]);
    try native.sgemmTransBStrided(null, 2, 3, 0, 1, &.{}, &.{}, 0, out[1..], 6);
    for (0..2) |row| for (0..3) |col| { try std.testing.expectEqual(@as(f32,0),out[1+row*6+col]); };
}
test { std.testing.refAllDecls(native); std.testing.refAllDecls(native.openblas); }
"""


def run(argv, env=None, success=True):
    p = subprocess.run(
        [str(x) for x in argv],
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=600,
    )
    if success and p.returncode:
        raise RuntimeError(f"{argv}:\n{p.stdout}")
    return p


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zig", default="zig")
    parser.add_argument("--cc", default="cc")
    parser.add_argument("--openblas-library")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix="antfly-openblas-test-") as tmp:
        out = Path(tmp)
        (out / "fixture.c").write_text(FIXTURE)
        libs = {}
        for name, flags in [
            ("good", []),
            ("ilp64", ["-DBAD_ABI=1"]),
            ("openmp", ["-DPARALLEL=2"]),
            ("sequential", ["-DPARALLEL=0"]),
            ("missing-symbol", ["-DMISSING_SGEMM=1"]),
        ]:
            libs[name] = out / f"{name}.so"
            run(
                [
                    args.cc,
                    "-shared",
                    "-fPIC",
                    *flags,
                    out / "fixture.c",
                    "-o",
                    libs[name],
                ]
            )
        (out / "probe.zig").write_text(PROBE)
        cache = ["--cache-dir", out / "cache", "--global-cache-dir", out / "global"]
        run([args.zig, "test", root / "pkg/inference/build/blas.zig", *cache])
        obj = out / "avx2.o"
        run(
            [
                args.zig,
                "build-obj",
                root / "lib/linalg/src/x86_avx2.zig",
                "-target",
                "x86_64-linux-gnu",
                "-mcpu=baseline+avx+avx2+fma+f16c",
                "-O",
                "ReleaseFast",
                "-fPIC",
                f"-femit-bin={obj}",
                *cache,
            ]
        )
        for abi, enabled in (("gnu", True), ("gnu", False), ("musl", True)):
            active = enabled and abi == "gnu"
            (out / "options.zig").write_text(
                "pub const enable_system_blas = false;\npub const enable_runtime_openblas = "
                + str(enabled).lower()
                + ";\n"
            )
            binary = out / f"probe-{abi}-{enabled}"
            run(
                [
                    args.zig,
                    "test",
                    "-lc",
                    obj,
                    "-target",
                    f"x86_64-linux-{abi}",
                    "-mcpu=baseline",
                    "-O",
                    "ReleaseSafe",
                    "--dep",
                    "native",
                    "--dep",
                    "inference_linalg",
                    f"-Mroot={out / 'probe.zig'}",
                    "--dep",
                    "build_options",
                    "--dep",
                    "inference_linalg",
                    f"-Mnative={root / 'pkg/inference/src/backends/native.zig'}",
                    f"-Mbuild_options={out / 'options.zig'}",
                    f"-Minference_linalg={root / 'lib/linalg/src/mod.zig'}",
                    "--test-no-exec",
                    f"-femit-bin={binary}",
                    *cache,
                ]
            )
            scenarios = [
                ("good", "auto", None, active, 2),
                ("good", "off", None, False, 0),
                ("good", "auto", "1", active, 1),
                ("good", "auto", "32", active, 2),
                ("good", "auto", "0", False, 0),
                ("absent", "auto", None, False, 0),
                ("ilp64", "auto", None, False, 0),
                ("openmp", "auto", None, False, 0),
                ("sequential", "auto", None, False, 0),
                ("missing-symbol", "auto", None, False, 0),
            ]
            if args.openblas_library:
                libs["real"] = Path(args.openblas_library).resolve()
                scenarios.append(("real", "auto", "2", active, 2))
            for name, policy, threads, expected, count in scenarios:
                env = dict(
                    os.environ,
                    ANTFLY_OPENBLAS_LIBRARY=str(libs.get(name, out / "absent.so")),
                    ANTFLY_INFERENCE_BLAS=policy,
                    ANTFLY_INFERENCE_CPU_THREADS="2",
                    EXPECT_BLAS=str(expected).lower(),
                    EXPECT_THREADS=str(count),
                )
                env.pop("OPENBLAS_NUM_THREADS", None)
                if threads is not None:
                    env["OPENBLAS_NUM_THREADS"] = threads
                result = run([binary], env)
                print(
                    f"{abi}/enabled={enabled}/{name}/{policy}/threads={threads}: {result.stdout.strip().splitlines()[-1]}",
                    flush=True,
                )
            if active:
                for name, policy, diagnostic in [
                    ("absent", "openblas", "OpenBlasNotFound"),
                    ("ilp64", "openblas", "OpenBlasIntegerAbiMismatch"),
                    ("good", "typo", "invalid ANTFLY_INFERENCE_BLAS"),
                ]:
                    env = dict(
                        os.environ,
                        ANTFLY_OPENBLAS_LIBRARY=str(libs.get(name, out / "absent.so")),
                        ANTFLY_INFERENCE_BLAS=policy,
                        EXPECT_BLAS="true",
                    )
                    result = run([binary], env, success=False)
                    if result.returncode == 0 or diagnostic not in result.stdout:
                        raise RuntimeError(
                            f"expected rejection {diagnostic}: {result.stdout}"
                        )
                print("required-library and invalid-policy rejection: PASS", flush=True)

        # Existing link-time BLAS must not initialize a second runtime library.
        (out / "options.zig").write_text(
            "pub const enable_system_blas = true;\npub const enable_runtime_openblas = true;\n"
        )
        linked = out / "linked.o"
        run([args.cc, "-c", "-fPIC", out / "fixture.c", "-o", linked])
        binary = out / "probe-linked"
        run(
            [
                args.zig,
                "test",
                "-lc",
                obj,
                linked,
                "-target",
                "x86_64-linux-gnu",
                "-mcpu=baseline",
                "-O",
                "ReleaseSafe",
                "--dep",
                "native",
                "--dep",
                "inference_linalg",
                f"-Mroot={out / 'probe.zig'}",
                "--dep",
                "build_options",
                "--dep",
                "inference_linalg",
                f"-Mnative={root / 'pkg/inference/src/backends/native.zig'}",
                f"-Mbuild_options={out / 'options.zig'}",
                f"-Minference_linalg={root / 'lib/linalg/src/mod.zig'}",
                "--test-no-exec",
                f"-femit-bin={binary}",
                *cache,
            ]
        )
        result = run(
            [binary],
            dict(
                os.environ,
                ANTFLY_INFERENCE_BLAS="openblas",
                ANTFLY_OPENBLAS_LIBRARY=str(out / "absent.so"),
                EXPECT_BLAS="true",
                EXPECT_THREADS="0",
            ),
        )
        print(
            "linked BLAS unchanged: " + result.stdout.strip().splitlines()[-1],
            flush=True,
        )


if __name__ == "__main__":
    main()
