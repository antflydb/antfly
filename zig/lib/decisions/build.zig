// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");

/// Share a single source identity across SQL and inference consumers. This
/// std-only module inherits the consumer's target and optimization settings.
pub fn create(b: *std.Build, root: std.Build.LazyPath) *std.Build.Module {
    if (b.modules.get("antfly_decisions")) |existing| return existing;
    return b.addModule("antfly_decisions", .{ .root_source_file = root });
}
