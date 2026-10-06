// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! One build policy shared by the root and standalone inference builds.
const std = @import("std");

pub const Policy = enum { auto, linked, off };
pub const Selection = struct {
    system: bool,
    runtime: bool,
};
const Options = struct {
    policy: ?Policy = null,
    legacy_system: ?bool = null,
    legacy_runtime: ?bool = null,
    can_link: bool,
    macos: bool,
    explicit_root: bool,
};

fn resolve(options: Options) !Selection {
    if (options.policy != null and (options.legacy_system != null or options.legacy_runtime != null))
        return error.MixedBlasOptions;
    const default_system = options.can_link and (options.macos or options.explicit_root);
    if (options.policy) |policy| return switch (policy) {
        .auto => .{ .system = default_system, .runtime = true },
        .linked => if (options.can_link) .{ .system = true, .runtime = false } else error.BlasRequiresLibc,
        .off => .{ .system = false, .runtime = false },
    };
    // Preserve old invocations, including release builds passing system-blas=false:
    // that flag controls linkage, not runtime loading. New callers use blas=off
    // when they want to disable both. Runtime support is target-gated by the loader.
    return .{
        .system = options.can_link and (options.legacy_system orelse default_system),
        .runtime = options.legacy_runtime orelse true,
    };
}

pub fn configure(b: *std.Build, can_link: bool, macos: bool, explicit_root: bool) Selection {
    return resolve(.{
        .policy = b.option(Policy, "blas", "CPU BLAS policy: auto (platform defaults), linked (require system BLAS), off (native kernels only)"),
        .legacy_system = b.option(bool, "system-blas", "Deprecated: link-time BLAS override; use -Dblas=auto|linked|off instead; cannot combine with -Dblas"),
        .legacy_runtime = b.option(bool, "runtime-openblas", "Deprecated: runtime loader override; use -Dblas=auto|linked|off instead; cannot combine with -Dblas"),
        .can_link = can_link,
        .macos = macos,
        .explicit_root = explicit_root,
    }) catch |err| switch (err) {
        error.MixedBlasOptions => std.debug.panic("-Dblas cannot be combined with deprecated -Dsystem-blas or -Druntime-openblas; use only -Dblas=auto|linked|off", .{}),
        error.BlasRequiresLibc => std.debug.panic("-Dblas=linked requires a native build with libc enabled", .{}),
    };
}

test "BLAS policy preserves platform defaults and legacy flag combinations" {
    for ([_]bool{ false, true }) |can_link| {
        for ([_]bool{ false, true }) |macos| {
            for ([_]bool{ false, true }) |explicit_root| {
                for ([_]?bool{ null, false, true }) |legacy_system| {
                    for ([_]?bool{ null, false, true }) |legacy_runtime| {
                        const selection = try resolve(.{ .can_link = can_link, .macos = macos, .explicit_root = explicit_root, .legacy_system = legacy_system, .legacy_runtime = legacy_runtime });
                        try std.testing.expectEqual(can_link and (legacy_system orelse (macos or explicit_root)), selection.system);
                        try std.testing.expectEqual(legacy_runtime orelse true, selection.runtime);
                    }
                }
                const defaults: Options = .{ .can_link = can_link, .macos = macos, .explicit_root = explicit_root };
                var auto = defaults;
                auto.policy = .auto;
                try std.testing.expectEqualDeep(try resolve(defaults), try resolve(auto));
                var off = defaults;
                off.policy = .off;
                try std.testing.expectEqualDeep(Selection{ .system = false, .runtime = false }, try resolve(off));
                var linked = defaults;
                linked.policy = .linked;
                if (can_link) {
                    try std.testing.expectEqualDeep(Selection{ .system = true, .runtime = false }, try resolve(linked));
                } else try std.testing.expectError(error.BlasRequiresLibc, resolve(linked));
            }
        }
    }
}

test "BLAS policy rejects ambiguous combinations even with explicit false aliases" {
    for (std.enums.values(Policy)) |policy| {
        for ([_]bool{ false, true }) |value| {
            try std.testing.expectError(error.MixedBlasOptions, resolve(.{ .policy = policy, .legacy_system = value, .can_link = true, .macos = false, .explicit_root = false }));
            try std.testing.expectError(error.MixedBlasOptions, resolve(.{ .policy = policy, .legacy_runtime = value, .can_link = true, .macos = false, .explicit_root = false }));
        }
    }
}
