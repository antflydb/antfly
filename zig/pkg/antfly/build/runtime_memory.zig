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
};

/// The routed implementation test owner is separate from production archives.
/// Pinned Nix/glibc GNU ARM64 ReleaseSafe reached 17,479,122,944 bytes;
/// 21 GiB supplies >25% measured compiler headroom, without changing runtimes.
pub fn routedImplementationCompileMaxRss(profile: CompileMemoryProfile) usize {
    if (profile.target.os.tag == .macos) {
        // Native ReleaseSafe routed write tests reached 15,716,696,064 bytes.
        // Twenty GiB supplies >25% headroom above that measured peak.
        const measured_macos = profile.host.os.tag == .macos and profile.optimize == .safe and !profile.sanitize_thread;
        return @as(usize, if (measured_macos) 20 else 12) << 30;
    }
    const measured = profile.host.os.tag == .linux and profile.host.cpu.arch == .aarch64 and
        profile.target.os.tag == .linux and profile.target.cpu.arch == .aarch64 and profile.target.abi == .gnu and
        profile.optimize == .safe and !profile.sanitize_thread;
    return @as(usize, if (measured) 21 else 8) << 30;
}

pub fn runtimeCompileMaxRss(unit: RuntimeLibraryUnit, profile: CompileMemoryProfile) usize {
    const target = profile.target;
    const native_gnu_arm64_safe = profile.host.os.tag == .linux and profile.host.cpu.arch == .aarch64 and
        target.os.tag == .linux and target.cpu.arch == .aarch64 and target.abi == .gnu and
        profile.optimize == .safe and !profile.sanitize_thread;
    const baseline_cpu = std.Target.Cpu.baseline(target.cpu.arch, target.os);
    if (profile.macos_metal_blas_release and
        std.mem.eql(u8, @import("builtin").zig_version_string, "0.17.0") and
        profile.host.os.tag == .macos and profile.host.cpu.arch == .aarch64 and
        target.os.tag == .macos and target.cpu.arch == .aarch64 and
        target.cpu.model == baseline_cpu.model and target.cpu.features.eql(baseline_cpu.features) and
        profile.optimize == .fast and profile.strip and !profile.sanitize_thread)
    {
        // Wolfspider native SDK 26.4, stripped Metal/Accelerate ReleaseFast.
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
        .api_kernel => @as(usize, if (target.os.tag == .macos) 24 else if (native_gnu_arm64_safe) 15 else 10) * 1024 * 1024 * 1024,
        // Pinned Nix GNU ARM64 ReleaseSafe retains its measured 15 GiB reservation.
        // September's macOS 20 GiB claim is below the reported 22–23 GB
        // compiler peak. 28 GiB includes >=25% headroom at 23 decimal GB.
        // Keep the measured Linux profiles separate. The other macOS bumps
        // below are provisional 25% margins over claims reported as exceeded;
        // replace them with cold-build evidence before lowering them.
        // The contextual-wait GNU ARM64 ReleaseSafe archive measured
        // 21,815,246,848 bytes, above the previous 20 GiB reservation.
        // Twenty-six GiB preserves >25% headroom for that measured profile.
        .storage_kernel => @as(usize, if (target.os.tag == .macos) 28 else if (native_gnu_arm64_safe) 26 else 20) * 1024 * 1024 * 1024,
        // Latest-main aarch64-macOS ReleaseSafe reached 14.01 decimal GB,
        // exceeding 13 GiB. Reserve 17 GiB (>25% above that observed peak)
        // for this profile; preserve other hosts/modes and runtime limits.
        // Pinned Nix GNU ARM64 ReleaseSafe peaked at 13,191,520,256 bytes;
        // sixteen GiB provides >25% measured headroom for that profile.
        .distributed => @as(usize, if (target.os.tag == .macos) 18 else if (native_gnu_arm64_safe) 16 else 11) * 1024 * 1024 * 1024,
        // Upstream hosted lake measurements raise the macOS distributed claim to 18 GiB.
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
        // Pinned Nix glibc Zig 0.17 native ARM64 GNU ReleaseSafe CLI peaked
        // at 3,645,767,680 bytes. Five GiB provides >25% measured headroom.
        // macOS retains its provisional four GiB claim; other Linux profiles
        // retain their existing three GiB claim.
        .cli => @as(usize, if (target.os.tag == .macos) 4 else if (native_gnu_arm64_safe) 5 else 3) * 1024 * 1024 * 1024,
    };
}

test "native GNU ARM64 ReleaseSafe claims cover pinned Nix compiler peaks" {
    const linux = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .aarch64, .os_tag = .linux, .abi = .gnu });
    const profile: CompileMemoryProfile = .{ .host = linux, .target = linux, .optimize = .safe, .strip = false, .cpu_inference = true };
    for ([_]RuntimeLibraryUnit{ .cli, .api_kernel, .distributed, .storage_kernel }, [_]usize{ 3_645_767_680, 12_550_221_824, 13_191_520_256, 21_815_246_848 }) |unit, peak| {
        try std.testing.expect(runtimeCompileMaxRss(unit, profile) >= peak + peak / 4);
    }
    var other = profile;
    other.optimize = .fast;
    try std.testing.expectEqual(@as(usize, 3) << 30, runtimeCompileMaxRss(.cli, other));
    try std.testing.expectEqual(@as(usize, 10) << 30, runtimeCompileMaxRss(.api_kernel, other));
    try std.testing.expectEqual(@as(usize, 11) << 30, runtimeCompileMaxRss(.distributed, other));
    try std.testing.expectEqual(@as(usize, 20) << 30, runtimeCompileMaxRss(.storage_kernel, other));
    other = profile;
    other.sanitize_thread = true;
    try std.testing.expectEqual(@as(usize, 10) << 30, runtimeCompileMaxRss(.api_kernel, other));
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
        inline for (.{ .debug, .small }) |mode| {
            profile = measured;
            profile.optimize = mode;
            try std.testing.expectEqual(runtimeCompileMaxRss(unit, conservative), runtimeCompileMaxRss(unit, profile));
        }
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

test "routed implementation compiler claim covers measured native GNU ARM64 only" {
    const linux = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .aarch64, .os_tag = .linux, .abi = .gnu });
    const profile: CompileMemoryProfile = .{ .host = linux, .target = linux, .optimize = .safe, .strip = false, .cpu_inference = true };
    const peak: usize = 17_479_122_944;
    try std.testing.expectEqual(@as(usize, 21) << 30, routedImplementationCompileMaxRss(profile));
    try std.testing.expect(routedImplementationCompileMaxRss(profile) >= peak + peak / 4);
    var other = profile;
    other.optimize = .fast;
    try std.testing.expectEqual(@as(usize, 8) << 30, routedImplementationCompileMaxRss(other));
    other = profile;
    other.sanitize_thread = true;
    try std.testing.expectEqual(@as(usize, 8) << 30, routedImplementationCompileMaxRss(other));
    other = profile;
    other.host = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .x86_64, .os_tag = .linux, .abi = .gnu });
    try std.testing.expectEqual(@as(usize, 8) << 30, routedImplementationCompileMaxRss(other));
    other = profile;
    other.target = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .aarch64, .os_tag = .linux, .abi = .musl });
    try std.testing.expectEqual(@as(usize, 8) << 30, routedImplementationCompileMaxRss(other));
    other = profile;
    other.target = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .aarch64, .os_tag = .macos });
    try std.testing.expectEqual(@as(usize, 12) << 30, routedImplementationCompileMaxRss(other));
}

test "native macOS routed ReleaseSafe claim covers the physical write owner peak" {
    const macos = try std.zig.system.resolveTargetQuery(std.testing.io, .{ .cpu_arch = .aarch64, .os_tag = .macos });
    const profile: CompileMemoryProfile = .{ .host = macos, .target = macos, .optimize = .safe, .strip = false, .cpu_inference = false };
    const peak: usize = 15_716_696_064;
    try std.testing.expect(routedImplementationCompileMaxRss(profile) >= peak + peak / 4);
    var other = profile;
    other.optimize = .fast;
    try std.testing.expectEqual(@as(usize, 12) << 30, routedImplementationCompileMaxRss(other));
    other = profile;
    other.sanitize_thread = true;
    try std.testing.expectEqual(@as(usize, 12) << 30, routedImplementationCompileMaxRss(other));
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
