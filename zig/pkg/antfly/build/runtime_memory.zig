// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

pub const RuntimeLibraryUnit = enum {
    api_kernel,
    distributed,
    storage_kernel,
    enrichment_compute,
    // Serverless/lake execution is a large, independently deployable graph.
    // Keep it out of the PIC storage kernel so LLVM never has to optimize the
    // two closures as one ARM64 ReleaseFast compilation unit.
    serverless,
    inference,
    // Remote/client commands do not own storage or server runtimes.
    cli,
};

/// Measured admission envelopes are scoped to the host and product profile.
/// Other configurations retain their established conservative reservations.
pub const CompileMemoryProfile = struct {
    host: std.Target,
    target: std.Target,
    optimize: std.lang.Optimize,
    strip: bool,
    cpu_inference: bool,
    sanitize_thread: bool = false,
    // Opt-in only after the builder verifies SDK and all backend settings.
    macos_metal_blas_release: bool = false,
    macos_shared_storage_sdk27: bool = false,
};

pub fn runtimeCompileMaxRss(unit: RuntimeLibraryUnit, profile: CompileMemoryProfile) usize {
    const target = profile.target;
    const baseline_cpu = std.Target.Cpu.baseline(target.cpu.arch, target.os);
    if (profile.macos_metal_blas_release and
        std.mem.eql(u8, @import("builtin").zig_version_string, "0.17.0") and
        profile.host.os.tag == .macos and profile.host.cpu.arch == .aarch64 and
        target.os.tag == .macos and target.cpu.arch == .aarch64 and
        target.cpu.model == baseline_cpu.model and target.cpu.features.eql(baseline_cpu.features) and
        profile.optimize == .fast and profile.strip and !profile.sanitize_thread)
    {
        // SDK 26.4; SDK 27.0 is qualified only with both shared owners.
        // SDK 27.0 shared storage adapter rounded MaxRSS: 9G. Reserve
        // 14 GiB (over 50% headroom), allowing API and adapter to overlap.
        if (profile.macos_shared_storage_sdk27) {
            if (unit == .storage_kernel) return 14 * 1024 * 1024 * 1024;
            // Overlap qualification rounded CLI MaxRSS to 2G.
            if (unit == .cli) return 4 * 1024 * 1024 * 1024;
        }
        // Stripped Metal/Accelerate ReleaseFast.
        // Claims include >=25% above each rounded MaxRSS upper bound. The
        // paired cold build and overlap qualification live in COMPILATION.md.
        const gib: usize = switch (unit) {
            .api_kernel => 14,
            .distributed => 10,
            .storage_kernel => 18,
            .enrichment_compute => 3,
            .serverless => 7,
            .inference => 12,
            .cli => 3,
        };
        return gib * 1024 * 1024 * 1024;
    }
    if (std.mem.eql(u8, @import("builtin").zig_version_string, "0.17.0") and
        profile.host.os.tag == .linux and profile.host.cpu.arch == .x86_64 and
        target.os.tag == .linux and target.cpu.arch == .x86_64 and target.abi == .gnu and
        target.cpu.model == baseline_cpu.model and target.cpu.features.eql(baseline_cpu.features) and
        profile.optimize == .debug and !profile.strip and profile.cpu_inference and !profile.sanitize_thread)
    {
        // Native LLVM Debug, run 38693574492: API peaked at 12,089,720,832
        // bytes, exceeding the old 10 GiB claim and forcing a retry. 15 GiB
        // retains >=25% headroom. Distributed/CLI summaries rounded to 8G/2G;
        // bound rounding with another GiB before adding 25%. Keep the other
        // units' conservative claims; cross-host measurements are not native
        // Linux evidence and must not lower admission reservations.
        const gib: usize = switch (unit) {
            .api_kernel => 15,
            .distributed => 12,
            .storage_kernel => 20,
            .enrichment_compute => 4,
            .serverless => 10,
            .inference => 16,
            .cli => 4,
        };
        return gib * 1024 * 1024 * 1024;
    }
    if (profile.host.os.tag == .linux and profile.host.cpu.arch == .x86_64 and
        target.os.tag == .linux and target.cpu.arch == .x86_64 and target.abi == .gnu and
        target.cpu.model == baseline_cpu.model and target.cpu.features.eql(baseline_cpu.features) and
        (profile.optimize == .fast or profile.optimize == .safe) and
        profile.strip and profile.cpu_inference and !profile.sanitize_thread)
    {
        // Cold Zig 0.16 x86_64 Linux CPU ReleaseFast, stripped, with empty
        // local/global caches. See COMPILATION.md for measurements and the
        // matched 22 GiB scheduler experiment. Preserve headroom above peaks;
        // storage and inference must be able to overlap on a normal runner.
        // Run 36903575966 also measured this exact ReleaseSafe profile.
        // Its archives compiled successfully but conservative admission
        // serialized the build until the E2E job's 90-minute deadline.
        // Retain margins above rounded Zig MaxRSS and allow the two largest
        // units to overlap within the existing 22 GiB aggregate budget.
        const gib: usize = if (profile.optimize == .safe) switch (unit) {
            .api_kernel => 7,
            .distributed => 8,
            .storage_kernel => 12,
            .enrichment_compute => 3,
            .serverless => 5,
            .inference => 8,
            .cli => 3,
        } else switch (unit) {
            .api_kernel => 5,
            .distributed => 5,
            .storage_kernel => 8,
            .enrichment_compute => 2,
            .serverless => 4,
            .inference => 8,
            .cli => 2,
        };
        return gib * 1024 * 1024 * 1024;
    }
    return switch (unit) {
        // Claims conservatively cover clean production ReleaseFast
        // peaks measured for both aarch64-linux-musl and explicit
        // aarch64-macos (including Metal and Accelerate). They are
        // scheduling reservations, not hard process limits. A larger
        // budget can overlap more units while a smaller cgroup
        // automatically schedules only the subset that fits.
        // aarch64-macOS ReleaseFast codegen reached 9.95 GB with
        // platform frameworks. Linux ARM64 reached 4.99 GB in the
        // v0.2.1-rc0 release build, while the integrated HA API kernel
        // reached 8.10 GB in a clean aarch64-linux-musl ReleaseFast
        // build. Linux retains 10 GiB. October's native lake codegen reached
        // 19.81 GB on macOS with paged native maintenance. Reserve 24 GiB
        // to retain at least 25% headroom over that measured peak.
        .api_kernel => @as(usize, if (target.os.tag == .macos) 24 else 10) * 1024 * 1024 * 1024,
        // September's macOS 20 GiB claim is below the reported 22–23 GB
        // compiler peak. 28 GiB includes >=25% headroom at 23 decimal GB.
        // Keep the measured Linux profiles separate. The other macOS bumps
        // below are provisional 25% margins over claims reported as exceeded;
        // replace them with cold-build evidence before lowering them.
        .storage_kernel => @as(usize, if (target.os.tag == .macos) 28 else 20) * 1024 * 1024 * 1024,
        // Hosted object-table integration raised the measured aarch64-macOS
        // ReleaseFast distributed peak to 15.41 GB. Reserve >=25% headroom
        // without reducing Linux runner concurrency.
        .distributed => @as(usize, if (target.os.tag == .macos) 18 else 11) * 1024 * 1024 * 1024,
        .enrichment_compute => 4 * 1024 * 1024 * 1024,
        // This is deliberately a separate non-PIC product unit. The
        // cold aarch64-macOS ReleaseFast build peaks near 2 GiB;
        // the 10 GiB reservation keeps it serialized with the macOS
        // storage kernel until both release runners confirm that.
        .serverless => 10 * 1024 * 1024 * 1024,
        // The broad aarch64-macOS ReleaseFast inference root
        // previously reached roughly 13.6 GB after storage integration;
        // the 16 GiB claim has since been reported as exceeded on macOS.
        // Add provisional headroom for mode-dependent IR; the build
        // scheduler can overlap whichever roots fit without forcing
        // callers to serialize the whole build.
        .inference => @as(usize, if (target.os.tag == .macos) 20 else 16) * 1024 * 1024 * 1024,
        // macOS CLI exceeded the newer 3 GiB claim too; provision 4 GiB
        // pending a fresh cold-build trace. Keep the Linux claim unchanged.
        .cli => @as(usize, if (target.os.tag == .macos) 4 else 3) * 1024 * 1024 * 1024,
    };
}

/// Native linked test objects have different working sets from runtime
/// archives. Keep measured floors at the linked-test compilation boundary.
pub fn linkedTestCompileMaxRss(name: []const u8, host: std.Target, target: std.Target, optimize: ?std.lang.Optimize, authored: usize) usize {
    // Native Apple Silicon CPU Debug qualification (SDK 26.3.1, Zig 0.17):
    // public-api-parity peaked at 18,029,035,520 bytes. Reserve >25% headroom
    // without changing cross-host, release or other test-object reservations.
    if (host.os.tag == .macos and host.cpu.arch == .aarch64 and
        target.os.tag == .macos and target.cpu.arch == .aarch64 and optimize == .debug and
        std.mem.eql(u8, name, "public-api-parity"))
        return @max(authored, 22 * 1024 * 1024 * 1024);
    if (host.os.tag != .linux or host.cpu.arch != .x86_64 or
        target.os.tag != .linux or target.cpu.arch != .x86_64 or target.abi != .gnu or optimize != .debug)
        return authored;
    // Run 38693574492: 16,964,825,088 and 11,133,898,752 bytes.
    // Each floor includes >=25% headroom and fits CI's 22 GiB budget alone.
    const gib: usize = if (std.mem.eql(u8, name, "public-api-parity")) 20 else if (std.mem.eql(u8, name, "data-runtime-tests")) 14 else return authored;
    return @max(authored, gib * 1024 * 1024 * 1024);
}

test "macOS reservations cover reported storage peak and prevent unsafe overlap" {
    const macos = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .aarch64, .os_tag = .macos });
    const profile: CompileMemoryProfile = .{ .host = macos, .target = macos, .optimize = .fast, .strip = false, .cpu_inference = true };
    const reported_api_bytes: usize = 19_808_337_920;
    try std.testing.expect(runtimeCompileMaxRss(.api_kernel, profile) >= reported_api_bytes + reported_api_bytes / 4);
    const reported_storage_bytes: usize = 23_000_000_000;
    const storage = runtimeCompileMaxRss(.storage_kernel, profile);
    try std.testing.expect(storage >= reported_storage_bytes + reported_storage_bytes / 4);
    // A 48 GiB developer host with 8 GiB reserved for other work cannot
    // admit storage and inference simultaneously under the revised claims.
    try std.testing.expect(storage + runtimeCompileMaxRss(.inference, profile) > 40 * 1024 * 1024 * 1024);
}

test "measured release reservations admit storage with inference and preserve unmeasured profiles" {
    const linux = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .x86_64, .cpu_model = .baseline, .os_tag = .linux, .abi = .gnu });
    const macos = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .aarch64, .os_tag = .macos });
    const measured: CompileMemoryProfile = .{ .host = linux, .target = linux, .optimize = .fast, .strip = true, .cpu_inference = true };
    const budget = 22 * 1024 * 1024 * 1024;
    const hosted_distributed_peak: usize = 15_407_759_360;
    try std.testing.expect(runtimeCompileMaxRss(.distributed, .{
        .host = macos,
        .target = macos,
        .optimize = .safe,
        .strip = false,
        .cpu_inference = true,
    }) >= hosted_distributed_peak + hosted_distributed_peak / 4);
    try std.testing.expect(runtimeCompileMaxRss(.storage_kernel, measured) + runtimeCompileMaxRss(.inference, measured) <= budget);
    var conservative = measured;
    conservative.cpu_inference = false;
    try std.testing.expect(runtimeCompileMaxRss(.storage_kernel, conservative) + runtimeCompileMaxRss(.inference, conservative) > budget);
    inline for (std.meta.tags(RuntimeLibraryUnit)) |unit| {
        try std.testing.expect(runtimeCompileMaxRss(unit, measured) < runtimeCompileMaxRss(unit, conservative));
        var profile = measured;
        profile.host = macos;
        try std.testing.expectEqual(runtimeCompileMaxRss(unit, conservative), runtimeCompileMaxRss(unit, profile));
        profile = measured;
        profile.optimize = .small;
        try std.testing.expectEqual(runtimeCompileMaxRss(unit, conservative), runtimeCompileMaxRss(unit, profile));
        profile = measured;
        profile.target.cpu.features.addFeature(@backingInt(std.Target.x86.Feature.avx2));
        try std.testing.expectEqual(runtimeCompileMaxRss(unit, conservative), runtimeCompileMaxRss(unit, profile));
        profile = measured;
        profile.strip = false;
        try std.testing.expectEqual(runtimeCompileMaxRss(unit, conservative), runtimeCompileMaxRss(unit, profile));
        profile = measured;
        profile.sanitize_thread = true;
        try std.testing.expectEqual(runtimeCompileMaxRss(unit, conservative), runtimeCompileMaxRss(unit, profile));
        inline for (.{
            std.Target.Query{ .cpu_arch = .aarch64, .os_tag = .linux, .abi = .gnu },
            std.Target.Query{ .cpu_arch = .x86_64, .os_tag = .linux, .abi = .musl },
            std.Target.Query{ .cpu_arch = .aarch64, .os_tag = .macos },
        }) |query| {
            profile = measured;
            profile.target = try std.zig.system.resolveTargetQuery(std.testing.io, query);
            var fallback = profile;
            fallback.cpu_inference = false;
            try std.testing.expectEqual(runtimeCompileMaxRss(unit, fallback), runtimeCompileMaxRss(unit, profile));
        }
    }
}

test "measured ReleaseSafe CPU archives overlap within the E2E runner budget" {
    const linux = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .x86_64, .cpu_model = .baseline, .os_tag = .linux, .abi = .gnu });
    const measured: CompileMemoryProfile = .{ .host = linux, .target = linux, .optimize = .safe, .strip = true, .cpu_inference = true };
    const gib = 1024 * 1024 * 1024;
    // Zig's summary rounds MaxRSS. Add a whole GiB to each reported peak
    // before requiring headroom, rather than treating 8G as an exact sample.
    const peak_upper_bounds = [_]usize{ 5, 6, 9, 2, 4, 6, 2 };
    for (std.meta.tags(RuntimeLibraryUnit), peak_upper_bounds) |unit, upper| {
        try std.testing.expect(runtimeCompileMaxRss(unit, measured) > upper * gib);
        var fallback = measured;
        fallback.cpu_inference = false;
        try std.testing.expect(runtimeCompileMaxRss(unit, measured) <= runtimeCompileMaxRss(unit, fallback));
        var unstripped = measured;
        unstripped.strip = false;
        try std.testing.expectEqual(runtimeCompileMaxRss(unit, fallback), runtimeCompileMaxRss(unit, unstripped));
        var sanitized = measured;
        sanitized.sanitize_thread = true;
        try std.testing.expectEqual(runtimeCompileMaxRss(unit, fallback), runtimeCompileMaxRss(unit, sanitized));
    }
    try std.testing.expect(runtimeCompileMaxRss(.storage_kernel, measured) + runtimeCompileMaxRss(.inference, measured) <= 22 * gib);
}

test "opt-in Darwin claims retain margins and reject unmeasured profiles" {
    const macos = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .aarch64, .os_tag = .macos, .cpu_model = .baseline });
    const profile: CompileMemoryProfile = .{ .host = macos, .target = macos, .optimize = .fast, .strip = true, .cpu_inference = false, .macos_metal_blas_release = true };
    const gib = 1024 * 1024 * 1024;
    const units = [_]RuntimeLibraryUnit{ .api_kernel, .distributed, .storage_kernel, .enrichment_compute, .serverless, .inference, .cli };
    const rounded_peak_upper_bounds = [_]usize{ 11, 8, 14, 2, 5, 9, 2 };
    for (units, rounded_peak_upper_bounds) |unit, peak| {
        try std.testing.expect(runtimeCompileMaxRss(unit, profile) >= peak * gib + peak * gib / 4);
        var fallback = profile;
        fallback.macos_metal_blas_release = false;
        try std.testing.expect(runtimeCompileMaxRss(unit, profile) < runtimeCompileMaxRss(unit, fallback));
        var unstripped = profile;
        unstripped.strip = false;
        try std.testing.expectEqual(runtimeCompileMaxRss(unit, fallback), runtimeCompileMaxRss(unit, unstripped));
        var sanitized = profile;
        sanitized.sanitize_thread = true;
        try std.testing.expectEqual(runtimeCompileMaxRss(unit, fallback), runtimeCompileMaxRss(unit, sanitized));
        inline for (.{ .debug, .safe, .small }) |mode| {
            var unmeasured = profile;
            unmeasured.optimize = mode;
            try std.testing.expectEqual(runtimeCompileMaxRss(unit, fallback), runtimeCompileMaxRss(unit, unmeasured));
        }
    }
    try std.testing.expect(runtimeCompileMaxRss(.storage_kernel, profile) + runtimeCompileMaxRss(.distributed, profile) <= 28 * gib);
}

test "default Linux CPU enrichment fits a three GiB build budget" {
    const linux = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .x86_64, .cpu_model = .baseline, .os_tag = .linux, .abi = .gnu });
    const gib = 1024 * 1024 * 1024;
    var profile: CompileMemoryProfile = .{ .host = linux, .target = linux, .optimize = .fast, .strip = true, .cpu_inference = true };
    try std.testing.expectEqual(2 * gib, runtimeCompileMaxRss(.enrichment_compute, profile));
    profile.optimize = .safe;
    try std.testing.expectEqual(3 * gib, runtimeCompileMaxRss(.enrichment_compute, profile));
    profile.sanitize_thread = true;
    try std.testing.expect(runtimeCompileMaxRss(.enrichment_compute, profile) > 3 * gib);
}

test "SDK 27 shared adapter retains peak margin and overlaps the API" {
    const macos = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .aarch64, .cpu_model = .baseline, .os_tag = .macos });
    var profile: CompileMemoryProfile = .{ .host = macos, .target = macos, .optimize = .fast, .strip = true, .cpu_inference = false, .macos_metal_blas_release = true, .macos_shared_storage_sdk27 = true };
    const gib = 1024 * 1024 * 1024;
    const adapter = runtimeCompileMaxRss(.storage_kernel, profile);
    try std.testing.expect(adapter >= 10 * gib + 10 * gib / 4);
    try std.testing.expect(adapter + runtimeCompileMaxRss(.api_kernel, profile) <= 28 * gib);
    try std.testing.expect(runtimeCompileMaxRss(.cli, profile) >= 3 * gib + 3 * gib / 4);
    profile.sanitize_thread = true;
    try std.testing.expectEqual(28 * gib, runtimeCompileMaxRss(.storage_kernel, profile));
}

test "native Linux LLVM Debug claims cover measured peaks and account for overlap" {
    const linux = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .x86_64, .cpu_model = .baseline, .os_tag = .linux, .abi = .gnu });
    const macos = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .aarch64, .os_tag = .macos });
    const profile: CompileMemoryProfile = .{ .host = linux, .target = linux, .optimize = .debug, .strip = false, .cpu_inference = true };
    const gib = 1024 * 1024 * 1024;
    const api_peak: usize = 12_089_720_832;
    const api = runtimeCompileMaxRss(.api_kernel, profile);
    try std.testing.expect(api >= api_peak + api_peak / 4);
    try std.testing.expect(runtimeCompileMaxRss(.distributed, profile) >= 9 * gib + 9 * gib / 4);
    try std.testing.expect(runtimeCompileMaxRss(.cli, profile) >= 3 * gib + 3 * gib / 4);
    // CI's 22 GiB aggregate cannot safely overlap API with distributed or the
    // storage adapter. Smaller CLI work can still run alongside the API.
    try std.testing.expect(api + runtimeCompileMaxRss(.distributed, profile) > 22 * gib);
    try std.testing.expect(api + runtimeCompileMaxRss(.storage_kernel, profile) > 22 * gib);
    try std.testing.expect(api + runtimeCompileMaxRss(.cli, profile) <= 22 * gib);
    var fallback = profile;
    fallback.host = macos;
    try std.testing.expectEqual(10 * gib, runtimeCompileMaxRss(.api_kernel, fallback));
    fallback = profile;
    fallback.target = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .x86_64, .os_tag = .linux, .abi = .musl });
    try std.testing.expectEqual(10 * gib, runtimeCompileMaxRss(.api_kernel, fallback));
    fallback = profile;
    fallback.strip = true;
    try std.testing.expectEqual(10 * gib, runtimeCompileMaxRss(.api_kernel, fallback));
    fallback = profile;
    fallback.cpu_inference = false;
    try std.testing.expectEqual(10 * gib, runtimeCompileMaxRss(.api_kernel, fallback));
    fallback = profile;
    fallback.sanitize_thread = true;
    try std.testing.expectEqual(10 * gib, runtimeCompileMaxRss(.api_kernel, fallback));
    fallback = profile;
    fallback.target.cpu.features.addFeature(@backingInt(std.Target.x86.Feature.avx2));
    try std.testing.expectEqual(10 * gib, runtimeCompileMaxRss(.api_kernel, fallback));
}

test "linked LLVM Debug test reservations cover native peaks without lowering authored claims" {
    const linux = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .x86_64, .os_tag = .linux, .abi = .gnu });
    const macos = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .aarch64, .os_tag = .macos });
    const gib = 1024 * 1024 * 1024;
    const names = [_][]const u8{ "public-api-parity", "data-runtime-tests" };
    const peaks = [_]usize{ 16_964_825_088, 11_133_898_752 };
    for (names, peaks) |name, peak| {
        const claim = linkedTestCompileMaxRss(name, linux, linux, .debug, 7 * gib);
        try std.testing.expect(claim >= peak + peak / 4);
        try std.testing.expect(claim <= 22 * gib);
        try std.testing.expectEqual(24 * gib, linkedTestCompileMaxRss(name, linux, linux, .debug, 24 * gib));
        try std.testing.expectEqual(7 * gib, linkedTestCompileMaxRss(name, macos, linux, .debug, 7 * gib));
        try std.testing.expectEqual(7 * gib, linkedTestCompileMaxRss(name, linux, macos, .debug, 7 * gib));
        try std.testing.expectEqual(7 * gib, linkedTestCompileMaxRss(name, linux, linux, .safe, 7 * gib));
    }
    try std.testing.expectEqual(7 * gib, linkedTestCompileMaxRss("unmeasured", linux, linux, .debug, 7 * gib));
}

test "native Darwin Debug public API reservation covers measured peak without unsafe overlap" {
    const macos = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .aarch64, .os_tag = .macos });
    const linux = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .x86_64, .os_tag = .linux, .abi = .gnu });
    const gib = 1024 * 1024 * 1024;
    const peak: usize = 18_029_035_520;
    const claim = linkedTestCompileMaxRss("public-api-parity", macos, macos, .debug, 15 * gib);
    try std.testing.expect(claim >= peak + peak / 4);
    // A 30 GiB local compiler budget cannot overlap this object with another
    // 15 GiB API compile. Smaller work can still use the remaining capacity.
    try std.testing.expect(claim + 15 * gib > 30 * gib);
    try std.testing.expect(claim + 4 * gib <= 30 * gib);
    try std.testing.expectEqual(24 * gib, linkedTestCompileMaxRss("public-api-parity", macos, macos, .debug, 24 * gib));
    try std.testing.expectEqual(15 * gib, linkedTestCompileMaxRss("public-api-parity", macos, macos, .safe, 15 * gib));
    try std.testing.expectEqual(15 * gib, linkedTestCompileMaxRss("public-api-parity", macos, linux, .debug, 15 * gib));
    try std.testing.expectEqual(15 * gib, linkedTestCompileMaxRss("public-api-parity", linux, macos, .debug, 15 * gib));
    try std.testing.expectEqual(7 * gib, linkedTestCompileMaxRss("data-runtime-tests", macos, macos, .debug, 7 * gib));
    try std.testing.expectEqual(7 * gib, linkedTestCompileMaxRss("unmeasured", macos, macos, .debug, 7 * gib));
}
