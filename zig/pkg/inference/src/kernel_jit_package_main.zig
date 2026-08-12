// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const kernel_jit = @import("graph/kernel_jit.zig");
const profile_output = @import("kernel_jit_profile_output.zig");

fn usage() void {
    std.debug.print(
        \\usage:
        \\  antfly-kernel-jit-package export-profile <cache-dir> <profile-json> <package-dir>
        \\  antfly-kernel-jit-package export-profile-bundle <cache-dir> <bundle-json> <package-dir>
        \\  antfly-kernel-jit-package import <package-dir> <cache-dir>
        \\  antfly-kernel-jit-package verify <package-dir>
        \\
        \\  Profile exports are self-contained: canonical profile JSON and every
        \\  referenced qualification record are bound by the package manifest.
        \\  Export, verify, and import all replay the v2 per-regime weighted
        \\  coverage policy before accepting a package.
        \\
        \\  Package verification checks integrity and policy consistency; it
        \\  does not authenticate the publisher. Import only through a trusted channel.
        \\
    , .{});
}

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.smp_allocator;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const command = args.next() orelse {
        usage();
        return error.InvalidArguments;
    };

    if (std.mem.eql(u8, command, "--help") or std.mem.eql(u8, command, "help")) {
        if (args.next() != null) return error.InvalidArguments;
        usage();
        return;
    }

    if (std.mem.eql(u8, command, "export")) {
        return error.KernelJitPackageRequiresQualifiedProfile;
    }

    if (std.mem.eql(u8, command, "export-profile")) {
        const cache_path = args.next() orelse return error.InvalidArguments;
        const profile_path = args.next() orelse return error.InvalidArguments;
        const package_path = args.next() orelse return error.InvalidArguments;
        if (args.next() != null) return error.InvalidArguments;
        var profile = try profile_output.loadCanonicalQualifiedProfile(
            allocator,
            init.io,
            profile_path,
            false,
        );
        defer profile.deinit();
        const package_profiles = [_]kernel_jit.QualificationPackageProfileInput{profile.packageInput()};
        var cache = try kernel_jit.ArtifactCache.initPath(
            allocator,
            init.io,
            cache_path,
            std.math.maxInt(usize),
        );
        defer cache.deinit();
        const summary = try kernel_jit.exportQualificationPackagePath(
            allocator,
            init.io,
            &cache,
            package_path,
            &package_profiles,
        );
        std.debug.print(
            "exported {d} profile-qualified entries in {d} self-contained profile\n",
            .{ summary.entries, summary.profiles },
        );
        return;
    }

    if (std.mem.eql(u8, command, "export-profile-bundle")) {
        const cache_path = args.next() orelse return error.InvalidArguments;
        const bundle_path = args.next() orelse return error.InvalidArguments;
        const package_path = args.next() orelse return error.InvalidArguments;
        if (args.next() != null) return error.InvalidArguments;
        var bundle = try profile_output.loadQualifiedProfileBundle(allocator, init.io, bundle_path);
        defer bundle.deinit();
        var loaded_profiles: [@typeInfo(profile_output.BundleComponent).@"enum".fields.len]?profile_output.CanonicalQualifiedProfile = @splat(null);
        defer for (&loaded_profiles) |*loaded| if (loaded.*) |*profile| profile.deinit();
        var package_profiles: [loaded_profiles.len]kernel_jit.QualificationPackageProfileInput = undefined;
        var profile_count: usize = 0;
        var key_count: usize = 0;
        for (bundle.profilePaths(), 0..) |maybe_profile_path, index| if (maybe_profile_path) |path| {
            loaded_profiles[index] = try profile_output.loadCanonicalQualifiedProfile(
                allocator,
                init.io,
                path,
                true,
            );
            package_profiles[profile_count] = loaded_profiles[index].?.packageInput();
            key_count += package_profiles[profile_count].keys.len;
            profile_count += 1;
        };
        if (key_count == 0) {
            std.debug.print("bundle has no qualified entries; no cache package is required\n", .{});
            return;
        }
        var cache = try kernel_jit.ArtifactCache.initPath(
            allocator,
            init.io,
            cache_path,
            std.math.maxInt(usize),
        );
        defer cache.deinit();
        const summary = try kernel_jit.exportQualificationPackagePath(
            allocator,
            init.io,
            &cache,
            package_path,
            package_profiles[0..profile_count],
        );
        std.debug.print(
            "exported {d} bundle-qualified entries in {d} self-contained profiles\n",
            .{ summary.entries, summary.profiles },
        );
        return;
    }

    if (std.mem.eql(u8, command, "import")) {
        const package_path = args.next() orelse return error.InvalidArguments;
        const cache_path = args.next() orelse return error.InvalidArguments;
        if (args.next() != null) return error.InvalidArguments;
        var cache = try kernel_jit.ArtifactCache.initPath(
            allocator,
            init.io,
            cache_path,
            try (kernel_jit.Config{}).maxCacheBytes(),
        );
        defer cache.deinit();
        const summary = try kernel_jit.importQualificationPackagePath(
            allocator,
            init.io,
            package_path,
            &cache,
        );
        std.debug.print(
            "imported {d} qualification entries from {d} bound profiles; publisher authenticity was not verified\n",
            .{ summary.entries, summary.profiles },
        );
        return;
    }

    if (std.mem.eql(u8, command, "verify")) {
        const package_path = args.next() orelse return error.InvalidArguments;
        if (args.next() != null) return error.InvalidArguments;
        const summary = try kernel_jit.verifyQualificationPackagePath(
            allocator,
            init.io,
            package_path,
        );
        std.debug.print(
            "verified integrity of {d} qualification entries across {d} bound profiles; publisher authenticity not established\n",
            .{ summary.entries, summary.profiles },
        );
        return;
    }

    usage();
    return error.InvalidArguments;
}
