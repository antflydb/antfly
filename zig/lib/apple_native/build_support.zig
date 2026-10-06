// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");

/// Generated paths are resolved during the make phase in Zig 0.17. Pass the
/// explicit test library path through env rather than baking a cache path into
/// production binaries or resolving the installation prefix at configure time.
pub fn configureTest(b: *std.Build, run: *std.Build.Step.Run, bridge: ?std.Build.LazyPath) void {
    if (bridge) |path| {
        const arguments = run.argv.toOwnedSlice(b.allocator) catch @panic("OOM");
        run.addArg("env");
        run.addPrefixedFileArg("ANTFLY_APPLE_BRIDGE_PATH=", path);
        run.argv.appendSlice(b.allocator, arguments) catch @panic("OOM");
        run.has_side_effects = true;
    }
}
