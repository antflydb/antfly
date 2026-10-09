// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Separately compiled producer for the private parent reclamation boundary.
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
const port = @import("common/parent_reclaim_admission.zig");
const admission = @import("common/read_admission.zig");
const payload = port.Payload{ .vtable = &.{ .reclaim = reclaim } };
fn reclaim(_: *anyopaque, bytes: u64, control: admission.Port) !u64 {
    try control.checkpoint();
    return bytes + 1;
}
export fn antfly_parent_reclaim_fixture_payload() callconv(.c) *const anyopaque {
    return &payload;
}
