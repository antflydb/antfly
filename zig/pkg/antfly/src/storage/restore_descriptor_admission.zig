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
const read = sources.common_read_admission;

const Bootstrap = sources.storage_db_restore_staging_contract.OwnerBootstrap;
pub fn parseBootstrap(allocator: std.mem.Allocator, input: []const u8, control: ?read.Port) !std.json.Parsed(Bootstrap) {
    const original = control orelse return std.json.parseFromSlice(Bootstrap, allocator, input, .{});
    var adapter: Adapter = .{ .original = original };
    return @import("../api/sql_json_source.zig").parse(Bootstrap, allocator, input, .{}, .{ .ptr = &adapter, .check = Adapter.checkpoint });
}

pub fn validateBootstrap(value: Bootstrap, control: ?read.Port) !void {
    if (control) |original| return value.validateWithAdmission(original);
    return value.validate();
}

test "controlled restore registry bootstrap validation preserves rules and interrupts large scans" {
    const data: [16385]u8 = @splat('x');
    var right = data;
    right[right.len - 1] = 'y';
    const bootstrap: Bootstrap = .{
        .scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = .{ .table_id = 4, .shard_id = 5, .range_id = 5 }, .target_namespace = .{ .table_id = 6, .shard_id = 7, .range_id = 7 }, .target_schema_digest = @splat(8) },
        .table_name = "rows",
        .schema_json = &data,
        .read_schema_json = &data,
        .indexes_json = &data,
        .byte_range = .{ .start = &data, .end = &right },
    };
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
    const payload: read.Payload = .{ .ptr = &authority, .vtable = &.{ .checkpoint = Authority.check } };
    const control = read.Port.init(&payload);
    try validateBootstrap(bootstrap, null);
    try validateBootstrap(bootstrap, control);
    const checks = authority.checks;
    try std.testing.expect(checks > 20);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..checks + 1) |fail_at| {
            authority = .{ .cause = cause, .fail_at = fail_at };
            try std.testing.expectError(cause, validateBootstrap(bootstrap, control));
        }
    }
    for (0..7) |mode| {
        var invalid = bootstrap;
        switch (mode) {
            0 => invalid.table_name = "bad/name",
            1 => invalid.schema_json = "\xff",
            2 => invalid.indexes_json = "",
            3 => invalid.byte_range.end = invalid.byte_range.start,
            4 => invalid.scope.plan_id = @splat(0),
            5 => invalid.table_name = ".",
            6 => invalid.read_schema_json = "\xed\xa0\x80",
            else => unreachable,
        }
        try std.testing.expectError(error.InvalidRestoreStagingCommand, validateBootstrap(invalid, null));
        authority = .{};
        try std.testing.expectError(error.InvalidRestoreStagingCommand, validateBootstrap(invalid, control));
    }
    var nul = bootstrap;
    nul.schema_json = "a\x00b";
    authority = .{};
    try validateBootstrap(nul, null);
    try validateBootstrap(nul, control);
}

test "controlled restore registry bootstrap parser preserves typed bytes errors and original rejection" {
    const data: [16385]u8 = @splat('x');
    const bootstrap: Bootstrap = .{
        .scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = .{ .table_id = 4, .shard_id = 5, .range_id = 5 }, .target_namespace = .{ .table_id = 6, .shard_id = 7, .range_id = 7 }, .target_schema_digest = @splat(8) },
        .table_name = "rows",
        .schema_json = &data,
        .indexes_json = "{}",
        .byte_range = .{ .start = "a", .end = "z" },
    };
    const body = try std.json.Stringify.valueAlloc(std.testing.allocator, bootstrap, .{});
    defer std.testing.allocator.free(body);
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
    const payload: read.Payload = .{ .ptr = &authority, .vtable = &.{ .checkpoint = Authority.check } };
    const control = read.Port.init(&payload);
    var expected = try parseBootstrap(std.testing.allocator, body, null);
    defer expected.deinit();
    var actual = try parseBootstrap(std.testing.allocator, body, control);
    defer actual.deinit();
    try std.testing.expectEqualDeep(expected.value, actual.value);
    try actual.value.validate();
    const checks = authority.checks;
    try std.testing.expect(checks > 8);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..checks + 1) |fail_at| {
            authority = .{ .cause = cause, .fail_at = fail_at };
            try std.testing.expectError(cause, parseBootstrap(std.testing.allocator, body, control));
        }
    }
    for ([_][]const u8{ "{", "{}", "{\"unexpected\":1}" }) |bad| {
        const original_error = if (parseBootstrap(std.testing.allocator, bad, null)) |parsed| blk: {
            parsed.deinit();
            break :blk error.TestUnexpectedResult;
        } else |err| err;
        authority = .{};
        try std.testing.expectError(original_error, parseBootstrap(std.testing.allocator, bad, control));
    }
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(allocator: std.mem.Allocator, bytes: []const u8) !void {
            var marker: u8 = 0;
            const owner: read.Payload = .{ .ptr = &marker, .vtable = &.{ .checkpoint = check } };
            var parsed = try parseBootstrap(allocator, bytes, read.Port.init(&owner));
            defer parsed.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{@as([]const u8, body)});
}

const Range = sources.storage_byte_range.ByteRange;
pub const Copies = struct {
    schema_json: []u8,
    indexes_json: []u8,
    restore_bootstrap_json: []u8,
    initial_range: ?Range,
    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        allocator.free(self.schema_json);
        allocator.free(self.indexes_json);
        allocator.free(self.restore_bootstrap_json);
        if (self.initial_range) |range| {
            allocator.free(range.start);
            allocator.free(range.end);
        }
    }
};

/// All allocations remain on the caller's allocator. Partial copies retire on
/// original rejection or allocation failure; no work or lifetime is added.
pub fn cloneBuffers(allocator: std.mem.Allocator, schema: []const u8, indexes: []const u8, bootstrap: []const u8, range: ?Range, control: ?read.Port) !Copies {
    const schema_copy = try copy(allocator, schema, control);
    errdefer allocator.free(schema_copy);
    const indexes_copy = try copy(allocator, indexes, control);
    errdefer allocator.free(indexes_copy);
    const bootstrap_copy = try copy(allocator, bootstrap, control);
    errdefer allocator.free(bootstrap_copy);
    var range_copy: ?Range = null;
    if (range) |value| {
        const start = try copy(allocator, value.start, control);
        errdefer allocator.free(start);
        range_copy = .{ .start = start, .end = try copy(allocator, value.end, control) };
    }
    return .{ .schema_json = schema_copy, .indexes_json = indexes_copy, .restore_bootstrap_json = bootstrap_copy, .initial_range = range_copy };
}

/// Cold recovery owns the same buffers as a cached descriptor, but encodes a
/// typed bootstrap directly instead of copying an existing serialized buffer.
pub fn encodeBuffers(allocator: std.mem.Allocator, schema: []const u8, indexes: []const u8, bootstrap: Bootstrap, control: ?read.Port) !Copies {
    const schema_copy = try copy(allocator, schema, control);
    errdefer allocator.free(schema_copy);
    const indexes_copy = try copy(allocator, indexes, control);
    errdefer allocator.free(indexes_copy);
    const encoded = if (control) |original| blk: {
        var adapter: Adapter = .{ .original = original };
        break :blk try sources.sql_cooperative_json_output.valueAlloc(allocator, bootstrap, .{}, .{ .ptr = &adapter, .check = Adapter.checkpoint });
    } else try std.json.Stringify.valueAlloc(allocator, bootstrap, .{});
    return .{ .schema_json = schema_copy, .indexes_json = indexes_copy, .restore_bootstrap_json = encoded, .initial_range = null };
}

const Adapter = struct {
    original: read.Port,
    fn checkpoint(raw: *anyopaque) !void {
        const scope: *@This() = @ptrCast(@alignCast(raw));
        try scope.original.checkpoint();
    }
};

fn copy(allocator: std.mem.Allocator, input: []const u8, control: ?read.Port) ![]u8 {
    const original = control orelse return allocator.dupe(u8, input);
    var adapter: Adapter = .{ .original = original };
    return sources.sql_cooperative_json_output.copyAlloc(allocator, input, .{ .ptr = &adapter, .check = Adapter.checkpoint });
}

test "controlled restore registry descriptor copies preserve bytes and retire every partial allocation" {
    const data: [8193]u8 = @splat('x');
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
    const payload: read.Payload = .{ .ptr = &authority, .vtable = &.{ .checkpoint = Authority.check } };
    const control = read.Port.init(&payload);
    var copied = try cloneBuffers(std.testing.allocator, &data, data[0..4097], &data, .{ .start = &data, .end = data[0..4097] }, control);
    defer copied.deinit(std.testing.allocator);
    try std.testing.expectEqualSlices(u8, &data, copied.schema_json);
    try std.testing.expectEqualSlices(u8, data[0..4097], copied.indexes_json);
    try std.testing.expectEqualSlices(u8, &data, copied.restore_bootstrap_json);
    try std.testing.expectEqualSlices(u8, &data, copied.initial_range.?.start);
    try std.testing.expectEqualSlices(u8, data[0..4097], copied.initial_range.?.end);
    const checks = authority.checks;
    try std.testing.expect(checks >= 20);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..checks + 1) |fail_at| {
            authority = .{ .cause = cause, .fail_at = fail_at };
            try std.testing.expectError(cause, cloneBuffers(std.testing.allocator, &data, data[0..4097], &data, .{ .start = &data, .end = data[0..4097] }, control));
        }
    }
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(allocator: std.mem.Allocator, bytes: []const u8, controlled: bool) !void {
            var marker: u8 = 0;
            const owner: read.Payload = .{ .ptr = &marker, .vtable = &.{ .checkpoint = check } };
            var result = try cloneBuffers(allocator, bytes, bytes, bytes, .{ .start = bytes, .end = bytes }, if (controlled) read.Port.init(&owner) else null);
            defer result.deinit(allocator);
        }
    };
    for ([_]bool{ false, true }) |controlled| try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ @as([]const u8, &data), controlled });
    authority = .{};
    var empty = try cloneBuffers(std.testing.allocator, "", "", "", null, control);
    defer empty.deinit(std.testing.allocator);
    try std.testing.expect(empty.initial_range == null);
}

/// Return a pinned entry under registry serialization. The caller must release
/// the pin outside the mutex, including if its next lifetime check rejects.
pub fn pin(comptime Entry: type, mutex: *std.atomic.Mutex, entries: []const *Entry, group: u64, table: []const u8, generation: u64, comptime reserve: anytype, control: ?read.Port) !?*Entry {
    if (control) |original| {
        try sources.common_bounded_mutex.lockAtomic(mutex, original);
    } else @import("antfly_platform").sync.lockYielding(mutex);
    defer mutex.unlock();
    for (entries, 0..) |entry, index| {
        if (control) |original| if (index % 64 == 0) try original.checkpoint();
        if (entry.group_id != group or !try sameName(entry.table_name, table, control) or entry.retired or entry.closing or entry.generation != generation or entry.restore_bootstrap_json.len == 0) continue;
        if (!reserve(entry, .shared)) return error.StorageReadTemporarilyUnavailable;
        return entry;
    }
    if (control) |original| try original.checkpoint();
    return null;
}

fn sameName(left: []const u8, right: []const u8, control: ?read.Port) !bool {
    const original = control orelse return std.mem.eql(u8, left, right);
    if (left.len != right.len) return false;
    var offset: usize = 0;
    while (offset < left.len) {
        try original.checkpoint();
        const end = offset + @min(@as(usize, 4096), left.len - offset);
        if (!std.mem.eql(u8, left[offset..end], right[offset..end])) return false;
        offset = end;
    }
    return true;
}

test "controlled restore registry pin preserves original rejection and shared reservation" {
    const Entry = struct {
        group_id: u64 = 1,
        table_name: []const u8 = "rows",
        retired: bool = false,
        closing: bool = false,
        generation: u64 = 2,
        restore_bootstrap_json: []const u8 = "{}",
        pins: usize = 0,
        blocked: bool = false,
        fn reserve(entry: *@This(), _: enum { shared }) bool {
            if (entry.blocked) return false;
            entry.pins += 1;
            return true;
        }
    };
    const Authority = struct {
        mutex: *std.atomic.Mutex,
        held: *bool,
        checks: usize = 0,
        waits: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const scope: *@This() = @ptrCast(@alignCast(raw));
            scope.checks += 1;
            if (scope.checks == scope.fail_at) return scope.cause;
        }
        fn wait(raw: *anyopaque) !void {
            const scope: *@This() = @ptrCast(@alignCast(raw));
            scope.waits += 1;
            try std.testing.expect(scope.held.*);
            scope.mutex.unlock();
            scope.held.* = false;
        }
    };
    var entry: Entry = .{};
    var mutex: std.atomic.Mutex = .unlocked;
    var held = false;
    defer if (held) mutex.unlock();
    var authority: Authority = .{ .mutex = &mutex, .held = &held };
    const payload: read.Payload = .{ .ptr = &authority, .vtable = &.{ .checkpoint = Authority.check, .wait_for_progress = Authority.wait } };
    const control = read.Port.init(&payload);
    try std.testing.expect(mutex.tryLock());
    held = true;
    try std.testing.expect((try pin(Entry, &mutex, &.{&entry}, 1, "rows", 2, Entry.reserve, control)).? == &entry);
    try std.testing.expectEqual(@as(usize, 1), authority.waits);
    try std.testing.expectEqual(@as(usize, 1), entry.pins);
    const checks = authority.checks;
    entry.pins = 0;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..checks + 1) |fail_at| {
            try std.testing.expect(mutex.tryLock());
            held = true;
            authority = .{ .mutex = &mutex, .held = &held, .cause = cause, .fail_at = fail_at };
            try std.testing.expectError(cause, pin(Entry, &mutex, &.{&entry}, 1, "rows", 2, Entry.reserve, control));
            try std.testing.expectEqual(@as(usize, 0), entry.pins);
            if (held) {
                try std.testing.expect(!mutex.tryLock());
                mutex.unlock();
                held = false;
            } else {
                try std.testing.expect(mutex.tryLock());
                mutex.unlock();
            }
        }
    }
    var missing: Entry = .{ .group_id = 2 };
    const many: [130]*Entry = @splat(&missing);
    authority = .{ .mutex = &mutex, .held = &held, .fail_at = 5, .cause = error.CatalogGenerationChanged };
    try std.testing.expectError(error.CatalogGenerationChanged, pin(Entry, &mutex, &many, 1, "rows", 2, Entry.reserve, control));
    try std.testing.expectEqual(@as(usize, 0), missing.pins);
    const long_name: [8192]u8 = @splat('x');
    entry.table_name = &long_name;
    authority = .{ .mutex = &mutex, .held = &held, .fail_at = 5, .cause = error.DeadlineExceeded };
    try std.testing.expectError(error.DeadlineExceeded, pin(Entry, &mutex, &.{&entry}, 1, &long_name, 2, Entry.reserve, control));
    try std.testing.expectEqual(@as(usize, 0), entry.pins);
    entry.table_name = "rows";
    authority = .{ .mutex = &mutex, .held = &held };
    entry.blocked = true;
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, pin(Entry, &mutex, &.{&entry}, 1, "rows", 2, Entry.reserve, control));
    entry.blocked = false;
    entry.retired = true;
    try std.testing.expect((try pin(Entry, &mutex, &.{&entry}, 1, "rows", 2, Entry.reserve, control)) == null);
    try std.testing.expectEqual(@as(usize, 0), entry.pins);
}

test "controlled restore registry recovery encoding preserves bytes and retires partial buffers" {
    const bytes: [16385]u8 = @splat('x');
    const bootstrap: Bootstrap = .{
        .scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = .{ .table_id = 4, .shard_id = 5, .range_id = 5 }, .target_namespace = .{ .table_id = 6, .shard_id = 7, .range_id = 7 }, .target_schema_digest = @splat(8) },
        .table_name = "rows",
        .schema_json = &bytes,
        .read_schema_json = "quote\"\n",
        .indexes_json = "{}",
        .byte_range = .{ .start = "a", .end = "z" },
    };
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
    const payload: read.Payload = .{ .ptr = &authority, .vtable = &.{ .checkpoint = Authority.check } };
    const port = read.Port.init(&payload);
    var legacy = try encodeBuffers(std.testing.allocator, &bytes, "{}", bootstrap, null);
    defer legacy.deinit(std.testing.allocator);
    var controlled = try encodeBuffers(std.testing.allocator, &bytes, "{}", bootstrap, port);
    defer controlled.deinit(std.testing.allocator);
    try std.testing.expectEqualSlices(u8, legacy.schema_json, controlled.schema_json);
    try std.testing.expectEqualSlices(u8, legacy.indexes_json, controlled.indexes_json);
    try std.testing.expectEqualSlices(u8, legacy.restore_bootstrap_json, controlled.restore_bootstrap_json);
    try std.testing.expect(controlled.initial_range == null);
    const checks = authority.checks;
    try std.testing.expect(checks > 10);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..checks + 1) |fail_at| {
            authority = .{ .cause = cause, .fail_at = fail_at };
            try std.testing.expectError(cause, encodeBuffers(std.testing.allocator, &bytes, "{}", bootstrap, port));
        }
    }
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(allocator: std.mem.Allocator, value: Bootstrap, controlled_mode: bool) !void {
            var marker: u8 = 0;
            const owner: read.Payload = .{ .ptr = &marker, .vtable = &.{ .checkpoint = check } };
            var result = try encodeBuffers(allocator, value.schema_json, value.indexes_json, value, if (controlled_mode) read.Port.init(&owner) else null);
            defer result.deinit(allocator);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ bootstrap, true });
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ bootstrap, false });
}

test "controlled restore registry mapped receipt digests preserve native validation and durable bytes" {
    const contract = sources.storage_db_restore_staging_contract;
    const handoff = sources.storage_db_empty_generation_handoff;
    var names: [131][256]u8 = @splat(@splat('x'));
    var mappings: [131]contract.GenerationAdmissionMapping = undefined;
    for (&mappings, 0..) |*mapping, index| {
        _ = try std.fmt.bufPrint(names[index][0..8], "c{d:0>7}", .{index});
        mapping.* = .{ .source_child_table_id = 1, .target_child_table_id = 2, .source_child_table_name = &names[index], .target_child_table_name = "target", .constraint_name = "fk", .source_generation = @splat(1), .target_generation = @splat(2), .source_scope_digest = @splat(3) };
    }
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
    const Fixture = struct {
        fn digest(values: []const contract.GenerationAdmissionMapping, mode: u8, control: ?read.Port) ![32]u8 {
            if (mode == 0) {
                const command: contract.InstallGenerationAdmissions = .{ .scope = @splat(1), .source_summary_digest = @splat(2), .mappings = values };
                return if (control) |port| contract.admissionReceiptDigestWithAdmission(command, port) else contract.admissionReceiptDigest(command);
            }
            const command: handoff.Install = .{ .scope = @splat(1), .source_summary_digest = @splat(2), .retired_digest = @splat(3), .retired_count = 5, .mappings = if (mode == 2) &.{} else values };
            return if (control) |port| handoff.installReceiptDigestWithAdmission(command, port) else handoff.installReceiptDigest(command);
        }
    };
    var authority: Authority = .{};
    const payload: read.Payload = .{ .ptr = &authority, .vtable = &.{ .checkpoint = Authority.check } };
    const port = read.Port.init(&payload);
    for ([_]u8{ 0, 1, 2 }) |mode| {
        authority = .{};
        const expected = try Fixture.digest(&mappings, mode, null);
        const actual = try Fixture.digest(&mappings, mode, port);
        try std.testing.expectEqualSlices(u8, &expected, &actual);
        const checks = authority.checks;
        if (mode != 2) try std.testing.expect(checks > 131);
        for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
            for (1..checks + 1) |fail_at| {
                authority = .{ .cause = cause, .fail_at = fail_at };
                try std.testing.expectError(cause, Fixture.digest(&mappings, mode, port));
            }
        }
    }
    for (0..4) |mode| {
        var invalid = mappings;
        switch (mode) {
            0 => invalid[0].source_child_table_id = 0,
            1 => std.mem.swap(contract.GenerationAdmissionMapping, &invalid[0], &invalid[1]),
            2 => invalid[0].source_child_table_name = "\xff",
            3 => invalid[0].constraint_name = "",
            else => unreachable,
        }
        authority = .{};
        try std.testing.expectError(error.InvalidRestoreStagingCommand, Fixture.digest(&invalid, 0, null));
        try std.testing.expectError(error.InvalidRestoreStagingCommand, Fixture.digest(&invalid, 0, port));
        const handoff_error = if (mode == 1) error.InvalidGenerationHandoff else error.InvalidRestoreStagingCommand;
        try std.testing.expectError(handoff_error, Fixture.digest(&invalid, 1, null));
        try std.testing.expectError(handoff_error, Fixture.digest(&invalid, 1, port));
    }
    var inactive = mappings;
    inactive[0].source_generation = null;
    inactive[0].target_generation = null;
    authority = .{};
    try std.testing.expectEqualSlices(u8, &(try Fixture.digest(&inactive, 0, null)), &(try Fixture.digest(&inactive, 0, port)));
    try std.testing.expectError(error.InvalidGenerationHandoff, Fixture.digest(&inactive, 1, null));
    try std.testing.expectError(error.InvalidGenerationHandoff, Fixture.digest(&inactive, 1, port));
    const invalid_header: contract.InstallGenerationAdmissions = .{ .scope = @splat(0), .source_summary_digest = @splat(2), .mappings = &mappings };
    try std.testing.expectError(error.InvalidRestoreStagingCommand, contract.admissionReceiptDigestWithAdmission(invalid_header, port));
    const invalid_handoff: handoff.Install = .{ .scope = @splat(1), .source_summary_digest = @splat(2), .retired_digest = @splat(0), .retired_count = 5, .mappings = &mappings };
    try std.testing.expectError(error.InvalidGenerationHandoff, handoff.installReceiptDigestWithAdmission(invalid_handoff, port));
}

test "controlled restore registry bootstrap builder preserves actual empty generation projection" {
    const metadata = @import("../metadata/restore_staging.zig");
    const contract = sources.storage_db_restore_staging_contract;
    const schema: [16385]u8 = @splat('x');
    const mapping: contract.GenerationAdmissionMapping = .{ .source_child_table_id = 11, .target_child_table_id = 12, .source_child_table_name = "old-child", .target_child_table_name = "fresh-child", .constraint_name = "fk", .source_generation = @splat(1), .target_generation = @splat(2), .source_scope_digest = @splat(3) };
    const target: metadata.Target = .{
        .source_table_id = 9,
        .empty_generation = true,
        .target_schema_digest = @splat(3),
        .table = .{ .table_id = 10, .name = "docs", .schema_json = &schema },
        .ranges = &.{.{ .table_id = 10, .group_id = 401, .start_key = "" }},
        .replace = .{ .table = .{ .table_id = 9, .name = "docs", .schema_json = "{}" }, .ranges = &.{.{ .table_id = 9, .group_id = 301, .start_key = "" }} },
        .generation_handoffs = &.{.{ .source_group_id = 301, .target_group_id = 401, .source_namespace = .{ .table_id = 9, .shard_id = 301, .range_id = 301 }, .admissions = &.{}, .admissions_digest = @splat(2), .retired_digest = @splat(3), .retired_count = 0, .mappings = &.{mapping} }},
    };
    const plan: metadata.Plan = .{ .id = @splat(1), .cohort_digest = @splat(2), .targets = &.{target} };
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
    const payload: read.Payload = .{ .ptr = &authority, .vtable = &.{ .checkpoint = Authority.check } };
    const port = read.Port.init(&payload);
    const expected = try metadata.ownerBootstrapForRangeIndex(std.testing.allocator, plan, @splat(2), target, 0);
    const actual = try metadata.ownerBootstrapForRangeIndexWithAdmission(std.testing.allocator, plan, @splat(2), target, 0, port);
    try std.testing.expectEqualDeep(expected, actual);
    const checks = authority.checks;
    try std.testing.expect(checks > 10);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..checks + 1) |fail_at| {
            authority = .{ .cause = cause, .fail_at = fail_at };
            try std.testing.expectError(cause, metadata.ownerBootstrapForRangeIndexWithAdmission(std.testing.allocator, plan, @splat(2), target, 0, port));
        }
    }
    authority = .{};
    try std.testing.expectError(error.InvalidRestoreStaging, metadata.ownerBootstrapForRangeIndexWithAdmission(std.testing.allocator, plan, @splat(2), target, 1, port));
}
