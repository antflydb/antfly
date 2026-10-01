// Copyright 2026 Antfly, Inc.
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

//! A restored migration retains its old read mapping while rebuilding every
//! target projection. Source-local progress is never coverage for new owners.
const std = @import("std");
const schema = @import("mod.zig");
const runtime = @import("../storage/schema.zig");

pub fn validate(alloc: std.mem.Allocator, active_json: []const u8, read_json: []const u8) !void {
    if (read_json.len == 0) return;
    if (active_json.len == 0) return error.InvalidRestoreMigrationState;
    var active = try schema.parseValidatedTableSchema(alloc, active_json);
    defer active.deinit(alloc);
    var previous = try schema.parseValidatedTableSchema(alloc, read_json);
    defer previous.deinit(alloc);
    if (previous.version >= active.version or previous.storage_mode != active.storage_mode)
        return error.InvalidRestoreMigrationState;
    const active_runtime = try schema.deriveRuntimeTableSchema(alloc, active);
    defer runtime.freeSchema(alloc, active_runtime);
    const read_runtime = try schema.deriveRuntimeTableSchema(alloc, previous);
    defer runtime.freeSchema(alloc, read_runtime);
}

test "restore migration requires ordered immutable layouts with stable storage mode" {
    const a = std.testing.allocator;
    try validate(a, "{\"version\":2}", "{\"version\":0}");
    try validate(a, "{}", "");
    try std.testing.expectError(error.InvalidRestoreMigrationState, validate(a, "", "{}"));
    try std.testing.expectError(error.InvalidRestoreMigrationState, validate(a, "{\"version\":1}", "{\"version\":1}"));
    try std.testing.expectError(error.InvalidRestoreMigrationState, validate(a, "{\"version\":1}", "{\"version\":2}"));
    try std.testing.expectError(error.InvalidRestoreMigrationState, validate(a, "{\"version\":2}", "{\"version\":1,\"storage_mode\":\"relational\"}"));
}
