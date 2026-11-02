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
const sources = @import("antfly_local_sources");
const Port = sources.common_read_admission.Port;
const Adapter = struct {
    original: Port,
    fn checkpoint(raw: *anyopaque) !void {
        const scope: *@This() = @ptrCast(@alignCast(raw));
        try scope.original.checkpoint();
    }
};

pub fn parse(comptime T: type, allocator: std.mem.Allocator, input: []const u8, control: Port) !std.json.Parsed(T) {
    var adapter: Adapter = .{ .original = control };
    return @import("../api/sql_json_source.zig").parse(T, allocator, input, .{}, .{ .ptr = &adapter, .check = Adapter.checkpoint });
}

/// Compare borrowed identities without permitting an unbounded byte scan.
pub fn equal(left: []const u8, right: []const u8, control: ?Port) !bool {
    const original = control orelse return std.mem.eql(u8, left, right);
    try original.checkpoint();
    if (left.len != right.len) return false;
    var offset: usize = 0;
    while (offset < left.len) {
        try original.checkpoint();
        const end = offset + @min(@as(usize, 4096), left.len - offset);
        if (!std.mem.eql(u8, left[offset..end], right[offset..end])) return false;
        offset = end;
    }
    try original.checkpoint();
    return true;
}

/// Preserve the canonical serialized bytes and existing encoded-size ceiling.
/// Original checks bound serialization and hashing without minting authority.
pub fn digestValue(value: anytype, allocator: std.mem.Allocator, control: Port, max_bytes: usize) ![32]u8 {
    var adapter: Adapter = .{ .original = control };
    const bytes = try sources.sql_cooperative_json_output.valueAlloc(allocator, value, .{}, .{ .ptr = &adapter, .check = Adapter.checkpoint });
    defer allocator.free(bytes);
    if (bytes.len > max_bytes) return error.InvalidRestoreStaging;
    return digestBytes(bytes, control);
}

pub fn digestBytes(bytes: []const u8, control: Port) ![32]u8 {
    try control.checkpoint();
    var hash = std.crypto.hash.Blake3.init(.{});
    var offset: usize = 0;
    while (offset < bytes.len) {
        try control.checkpoint();
        const end = offset + @min(@as(usize, 4096), bytes.len - offset);
        hash.update(bytes[offset..end]);
        offset = end;
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    try control.checkpoint();
    return result;
}

test "controlled restore plan codec preserves canonical digest typed job and original rejection" {
    const metadata = @import("../metadata/restore_staging.zig");
    const data: [16385]u8 = @splat('x');
    const plan: metadata.Plan = .{ .id = @splat(1), .cohort_digest = @splat(2), .source_location = &data, .source_connection = "quote\"\n", .targets = &.{} };
    const Authority = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const scope: *@This() = @ptrCast(@alignCast(raw));
            scope.checks += 1;
            if (scope.checks == scope.fail_at) return scope.cause;
        }
    };
    var authority: Authority = .{};
    const payload: sources.common_read_admission.Payload = .{ .ptr = &authority, .vtable = &.{ .checkpoint = Authority.check } };
    const control = Port.init(&payload);
    const expected = try plan.digest(std.testing.allocator);
    try std.testing.expectEqualSlices(u8, &expected, &(try plan.digestWithAdmission(std.testing.allocator, control)));
    const hash_checks = authority.checks;
    const job: metadata.Job = .{ .plan = plan, .plan_digest = expected };
    const bytes = try std.json.Stringify.valueAlloc(std.testing.allocator, job, .{});
    defer std.testing.allocator.free(bytes);
    authority = .{};
    var actual = try parse(metadata.Job, std.testing.allocator, bytes, control);
    defer actual.deinit();
    var legacy = try std.json.parseFromSlice(metadata.Job, std.testing.allocator, bytes, .{});
    defer legacy.deinit();
    try std.testing.expectEqualDeep(legacy.value, actual.value);
    const parse_checks = authority.checks;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..hash_checks + 1) |fail_at| {
            authority = .{ .cause = cause, .fail_at = fail_at };
            try std.testing.expectError(cause, plan.digestWithAdmission(std.testing.allocator, control));
        }
        for (1..parse_checks + 1) |fail_at| {
            authority = .{ .cause = cause, .fail_at = fail_at };
            try std.testing.expectError(cause, parse(metadata.Job, std.testing.allocator, bytes, control));
        }
    }
    authority = .{};
    try std.testing.expectError(error.InvalidRestoreStaging, digestValue(plan, std.testing.allocator, control, 1));
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(allocator: std.mem.Allocator, value: metadata.Plan, encoded: []const u8) !void {
            var marker: u8 = 0;
            const owner: sources.common_read_admission.Payload = .{ .ptr = &marker, .vtable = &.{ .checkpoint = check } };
            const port = Port.init(&owner);
            _ = try value.digestWithAdmission(allocator, port);
            var parsed = try parse(metadata.Job, allocator, encoded, port);
            defer parsed.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ plan, @as([]const u8, bytes) });
}

test "controlled restore plan codec owner scope retains actual artifact and range identities" {
    const metadata = @import("../metadata/restore_staging.zig");
    const records = sources.common_topology_records;
    const data: [16385]u8 = @splat('x');
    var end_key = data;
    end_key[end_key.len - 1] = 'y';
    const range: records.RangeRecord = .{ .table_id = 10, .group_id = 401, .start_key = &data, .end_key = &end_key };
    const artifact: metadata.SourceArtifact = .{ .target_group_id = 401, .source_namespace = .{ .table_id = 9, .shard_id = 301, .range_id = 301 }, .format = .portable, .snapshot_path = &data, .artifact_size_bytes = 100, .artifact_sha256 = @splat(3) };
    var artifacts: [131]metadata.SourceArtifact = @splat(artifact);
    for (artifacts[0..130]) |*value| value.target_group_id = 1;
    const target: metadata.Target = .{ .source_table_id = 9, .target_schema_digest = @splat(8), .table = .{ .table_id = 10, .name = "docs", .schema_json = "{}" }, .ranges = &.{range}, .source_artifacts = &artifacts };
    const plan: metadata.Plan = .{ .id = @splat(1), .cohort_digest = @splat(2), .targets = &.{target} };
    var old_ranges: [131]records.RangeRecord = @splat(.{ .table_id = 9, .group_id = 301, .start_key = "b" });
    old_ranges[130] = .{ .table_id = 9, .group_id = 301, .start_key = &data, .end_key = &end_key };
    var empty_target = target;
    empty_target.empty_generation = true;
    empty_target.replace = .{ .table = .{ .table_id = 9, .name = "docs", .schema_json = "{}" }, .ranges = &old_ranges };
    const Authority = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.fail_at) return self.cause;
        }
    };
    var authority: Authority = .{};
    const payload: sources.common_read_admission.Payload = .{ .ptr = &authority, .vtable = &.{ .checkpoint = Authority.check } };
    const port = Port.init(&payload);
    for ([_]metadata.Target{ target, empty_target }) |value| {
        authority = .{};
        const expected = try metadata.ownerScope(std.testing.allocator, plan, @splat(2), value, range);
        const actual = try metadata.ownerScopeWithAdmission(std.testing.allocator, plan, @splat(2), value, range, port);
        try std.testing.expectEqualDeep(expected, actual);
        const checks = authority.checks;
        try std.testing.expect(checks > 10);
        for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
            for (1..checks + 1) |fail_at| {
                authority = .{ .cause = cause, .fail_at = fail_at };
                try std.testing.expectError(cause, metadata.ownerScopeWithAdmission(std.testing.allocator, plan, @splat(2), value, range, port));
            }
        }
    }
    authority = .{};
    try std.testing.expect(!try equal(&data, &end_key, port));
    try std.testing.expect(!try equal(&data, data[0..1], port));
    try std.testing.expect(try equal("", "", port));
    var missing = target;
    missing.source_artifacts = &.{};
    try std.testing.expectError(error.RestoreSourceProofMissing, metadata.ownerScopeWithAdmission(std.testing.allocator, plan, @splat(2), missing, range, port));
    var invalid_range = range;
    invalid_range.table_id = 0;
    try std.testing.expectError(error.InvalidRestoreStaging, metadata.ownerScopeWithAdmission(std.testing.allocator, plan, @splat(2), target, invalid_range, port));
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(allocator: std.mem.Allocator, value: metadata.Target, owner: metadata.Plan, selected: records.RangeRecord) !void {
            var marker: u8 = 0;
            const admission: sources.common_read_admission.Payload = .{ .ptr = &marker, .vtable = &.{ .checkpoint = check } };
            _ = try metadata.ownerScopeWithAdmission(allocator, owner, @splat(2), value, selected, Port.init(&admission));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ target, plan, range });
}

test "controlled restore plan codec schema fallback retains native schema digest and cleanup" {
    const metadata = @import("../metadata/restore_staging.zig");
    const data: [16385]u8 = @splat('x');
    const schema = try std.fmt.allocPrint(std.testing.allocator, "{{\"version\":0,\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{{\"doc\":{{\"schema\":{{\"type\":\"object\",\"description\":\"{s}\",\"properties\":{{\"title\":{{\"type\":\"string\"}}}}}}}}}}}}", .{data});
    defer std.testing.allocator.free(schema);
    const Authority = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.fail_at) return self.cause;
        }
    };
    var authority: Authority = .{};
    const payload: sources.common_read_admission.Payload = .{ .ptr = &authority, .vtable = &.{ .checkpoint = Authority.check } };
    const port = Port.init(&payload);
    for ([_][]const u8{ schema, "", "{}" }) |input| {
        authority = .{};
        const expected = try metadata.runtimeSchemaDigestForTable(std.testing.allocator, input);
        const actual = try metadata.runtimeSchemaDigestForTableWithAdmission(std.testing.allocator, input, port);
        try std.testing.expectEqualSlices(u8, &expected, &actual);
        const checks = authority.checks;
        if (input.len > 4096) try std.testing.expect(checks > 10);
        for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
            for (1..checks + 1) |fail_at| {
                authority = .{ .cause = cause, .fail_at = fail_at };
                try std.testing.expectError(cause, metadata.runtimeSchemaDigestForTableWithAdmission(std.testing.allocator, input, port));
            }
        }
    }
    authority = .{};
    for ([_][]const u8{ "{", "[]", "{\"version\":-1}", "{\"document_schemas\":false}" }) |invalid| {
        _ = metadata.runtimeSchemaDigestForTable(std.testing.allocator, invalid) catch |err| {
            try std.testing.expectError(err, metadata.runtimeSchemaDigestForTableWithAdmission(std.testing.allocator, invalid, port));
            continue;
        };
        return error.TestUnexpectedResult;
    }
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(allocator: std.mem.Allocator, input: []const u8) !void {
            var marker: u8 = 0;
            const owner: sources.common_read_admission.Payload = .{ .ptr = &marker, .vtable = &.{ .checkpoint = check } };
            _ = try metadata.runtimeSchemaDigestForTableWithAdmission(allocator, input, Port.init(&owner));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{@as([]const u8, schema)});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{@as([]const u8, "")});
}
