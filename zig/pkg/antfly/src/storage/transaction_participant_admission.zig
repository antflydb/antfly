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
const contract = sources.api_local_transaction_contract;
const runtime = @import("antfly_runtime_abi");

const State = struct {
    original: ?runtime.runnable_abi.Borrowed = null,
    admission: ?sources.common_read_admission.Port = null,
    failure: ?anyerror = null,
    fn check(self: *@This()) !void {
        if (self.failure) |cause| return cause;
        if (self.original) |original| original.checkLifetime() catch |cause| {
            self.failure = cause;
            return cause;
        };
        if (self.admission) |original| original.checkpoint() catch |cause| {
            self.failure = cause;
            return cause;
        };
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.check();
    }
    fn equal(self: *@This(), left: []const u8, right: []const u8) !bool {
        if (left.len != right.len) return false;
        var offset: usize = 0;
        while (offset < left.len) {
            try self.check();
            const end = offset + @min(left.len - offset, 4096);
            if (!std.mem.eql(u8, left[offset..end], right[offset..end])) return false;
            offset = end;
        }
        return true;
    }
    fn hash(self: *@This(), bytes: []const u8) !u64 {
        var hash_state = std.hash.Wyhash.init(0);
        var offset: usize = 0;
        while (offset < bytes.len) {
            try self.check();
            const end = offset + @min(bytes.len - offset, 4096);
            hash_state.update(bytes[offset..end]);
            offset = end;
        }
        return hash_state.final();
    }
};
const Key = struct { bytes: []const u8, hash: u64 };
// This temporary index never deletes entries. Power-of-two capacity and a
// three-quarter load bound ensure every probe reaches an empty slot. Keys and
// hashes are borrowed; growth owns only slots and checks every explicit loop.
const Index = struct {
    slots: []?Key = &.{},
    count: usize = 0,
    fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        allocator.free(self.slots);
        self.* = .{};
    }
    const Position = struct { index: usize, existing: bool };
    fn find(slots: []?Key, key: Key, state: *State) !Position {
        var index = @as(usize, @truncate(key.hash)) & (slots.len - 1);
        var probes: usize = 0;
        while (true) : (probes += 1) {
            if (probes % 64 == 0) try state.check();
            const entry = slots[index] orelse return .{ .index = index, .existing = false };
            if (entry.hash == key.hash and try state.equal(entry.bytes, key.bytes)) return .{ .index = index, .existing = true };
            index = (index + 1) & (slots.len - 1);
        }
    }
    fn grow(self: *@This(), allocator: std.mem.Allocator, state: *State) !void {
        try state.check();
        const capacity = if (self.slots.len == 0) 8 else std.math.mul(usize, self.slots.len, 2) catch return error.OutOfMemory;
        const next = try allocator.alloc(?Key, capacity);
        errdefer allocator.free(next);
        var offset: usize = 0;
        while (offset < next.len) {
            try state.check();
            const end = offset + @min(next.len - offset, 64);
            @memset(next[offset..end], null);
            offset = end;
        }
        for (self.slots, 0..) |maybe_key, ordinal| {
            if (ordinal % 64 == 0) try state.check();
            const key = maybe_key orelse continue;
            const position = try find(next, key, state);
            std.debug.assert(!position.existing);
            next[position.index] = key;
        }
        try state.check();
        allocator.free(self.slots);
        self.slots = next;
    }
    // Return true for an existing byte-identical key. No borrowed key outlives
    // this validation call, including a rejected partial insertion or growth.
    fn insert(self: *@This(), allocator: std.mem.Allocator, key: Key, state: *State) !bool {
        try state.check();
        if (self.slots.len != 0) {
            const position = try find(self.slots, key, state);
            if (position.existing) return true;
            if (self.count < self.slots.len - self.slots.len / 4) {
                self.slots[position.index] = key;
                self.count += 1;
                return false;
            }
        }
        try self.grow(allocator, state);
        const position = try find(self.slots, key, state);
        std.debug.assert(!position.existing);
        self.slots[position.index] = key;
        self.count += 1;
        return false;
    }
};

/// Validate the existing durable participant set before BEGIN. Borrowed keys
/// live only for this call; hashing stores its result so map growth does not
/// rescan participant bytes. No new limit, owner, deadline or CPU loan is added.
pub fn validate(allocator: std.mem.Allocator, participants: []const []const u8, local: []const u8, original: ?runtime.runnable_abi.Borrowed) !bool {
    var state: State = .{ .original = original };
    return validateState(allocator, participants, local, &state);
}

/// Full fixed-buffer capacity for the local locator and every geometrically
/// grown hash table, including abandoned earlier tables in that buffer. String
/// payloads stay borrowed. Checked arithmetic refuses impossible demand before
/// any physical backing allocation or transaction mutation.
pub fn workingSetBytes(participant_count: usize, table_name_bytes: usize) !usize {
    var capacity: usize = 8;
    while (capacity - capacity / 4 < participant_count) capacity = try std.math.mul(usize, capacity, 2);
    const slots = try std.math.mul(usize, try std.math.mul(usize, capacity, 2), @sizeOf(?Key));
    const locator = try std.math.add(usize, try std.math.mul(usize, table_name_bytes, 2), 512);
    return std.math.add(usize, slots, locator);
}

/// Pre-holder validation; this port may yield original CPU between bounded work.
pub fn validateWithAdmission(allocator: std.mem.Allocator, participants: []const []const u8, local: []const u8, original: sources.common_read_admission.Port) !bool {
    var state: State = .{ .admission = original };
    return validateState(allocator, participants, local, &state);
}

fn validateState(allocator: std.mem.Allocator, participants: []const []const u8, local: []const u8, state: *State) !bool {
    try state.check();
    if (participants.len == 0) return error.InvalidBatchRequest;
    const payload: sources.common_read_admission.Payload = .{ .ptr = state, .vtable = &.{ .checkpoint = State.checkpoint } };
    const control = sources.common_read_admission.Port.init(&payload);
    var seen: Index = .{};
    defer seen.deinit(allocator);
    var local_present = false;
    for (participants) |participant| {
        try state.check();
        const parsed = if (state.original != null or state.admission != null) try contract.parseParticipantRefWithAdmission(participant, control) else contract.parseParticipantRef(participant);
        if (parsed == null) return error.InvalidBatchRequest;
        const key: Key = .{ .bytes = participant, .hash = try state.hash(participant) };
        if (try seen.insert(allocator, key, state)) return error.InvalidBatchRequest;
        if (try state.equal(participant, local)) local_present = true;
    }
    if (!local_present) return error.InvalidBatchRequest;
    const coordinator = try state.equal(participants[0], local);
    try state.check();
    return coordinator;
}

test "controlled transaction execution admission participant validation preserves grammar and original rejection" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const long_name: [8193]u8 = @splat('x');
    const zeros: [8193]u8 = @splat('0');
    const participants = try allocator.alloc([]const u8, 131);
    participants[0] = try contract.participantIdForGroupScoped(allocator, &long_name, 1, @splat(2), @splat(3));
    for (participants[1..], 2..) |*participant, group| participant.* = try contract.participantIdForGroup(allocator, "docs", group);
    const Callback = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            return if (self.checks == self.fail_at) runtime.error_abi.statusFromError(self.cause) else .ok;
        }
        fn checkpoint(raw: *anyopaque) !void {
            const status = lifetime(raw);
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        }
        fn cpu(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
        }
        fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
        }
    };
    var callback: Callback = .{};
    const original: runtime.runnable_abi.Borrowed = .{ .context = &callback, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.write), .check_lifetime = Callback.lifetime, .checkpoint = Callback.cpu, .park = Callback.phase, .enter = Callback.phase };
    try std.testing.expect(try validate(std.testing.allocator, participants, participants[0], null));
    try std.testing.expect(try validate(std.testing.allocator, participants, participants[0], original));
    const checks = callback.checks;
    try std.testing.expect(checks > 500);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| for (1..checks + 1) |fail_at| {
        callback = .{ .fail_at = fail_at, .cause = cause };
        try std.testing.expectError(cause, validate(std.testing.allocator, participants, participants[0], original));
    };
    callback = .{};
    try std.testing.expect(!try validate(std.testing.allocator, participants, participants[130], original));
    try std.testing.expectError(error.InvalidBatchRequest, validate(std.testing.allocator, &.{ participants[0], participants[0] }, participants[0], original));
    try std.testing.expectError(error.InvalidBatchRequest, validate(std.testing.allocator, &.{}, participants[0], original));
    try std.testing.expectError(error.InvalidBatchRequest, validate(std.testing.allocator, &.{participants[1]}, participants[0], original));
    try std.testing.expectError(error.InvalidBatchRequest, validate(std.testing.allocator, &.{"bad"}, participants[0], original));
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator, keys: []const []const u8, execution: runtime.runnable_abi.Borrowed) !void {
            _ = try validate(alloc, keys, keys[0], execution);
        }
    };
    callback = .{};
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ participants, original });
    var hash_state: State = .{ .original = original };
    try std.testing.expectEqual(std.hash.Wyhash.hash(0, &long_name), try hash_state.hash(&long_name));
    const payload: sources.common_read_admission.Payload = .{ .ptr = &callback, .vtable = &.{ .checkpoint = Callback.checkpoint } };
    const control = sources.common_read_admission.Port.init(&payload);
    const cases = [_][]const u8{
        "table:docs:group:0",     "table:docs:group:1__2",                                              "table:docs:group:_1",                                                    "table:docs:group:1_",                                                                                                                                                   "table:docs:group:+1", "table:docs:group:18446744073709551616",
        "table2:00000004:docs:0", "table2:00000004:docs:01_2",                                          "table2:00000004:docs:",                                                  "table2:00000004:docs:-1",                                                                                                                                               "table2:00000000::1",  "table:docs:group:1:group:2",
        participants[0],          try std.fmt.allocPrint(allocator, "table:{s}:group:1", .{long_name}), try std.fmt.allocPrint(allocator, "table2:00000004:docs:{s}1", .{zeros}), try std.fmt.allocPrint(allocator, "table3:00000001:t:{s}1:01010101010101010101010101010101:0202020202020202020202020202020202020202020202020202020202020202", .{zeros}),
    };
    for (cases) |text| {
        callback = .{};
        const legacy = contract.parseParticipantRef(text);
        const controlled = try contract.parseParticipantRefWithAdmission(text, control);
        try std.testing.expectEqualDeep(legacy, controlled);
        const parse_checks = callback.checks;
        for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| for (1..parse_checks + 1) |fail_at| {
            callback = .{ .fail_at = fail_at, .cause = cause };
            try std.testing.expectError(cause, contract.parseParticipantRefWithAdmission(text, control));
        };
    }
}

test "controlled transaction execution admission participant index checks collision probes and growth cleanup" {
    const bytes: [65]u8 = @splat('x');
    const Callback = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            return if (self.checks == self.fail_at) runtime.error_abi.statusFromError(self.cause) else .ok;
        }
        fn cpu(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
        }
        fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
        }
    };
    var callback: Callback = .{};
    const original: runtime.runnable_abi.Borrowed = .{ .context = &callback, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.write), .check_lifetime = Callback.lifetime, .checkpoint = Callback.cpu, .park = Callback.phase, .enter = Callback.phase };
    const Fixture = struct {
        fn run(allocator: std.mem.Allocator, data: []const u8, execution: runtime.runnable_abi.Borrowed) !void {
            var state: State = .{ .original = execution };
            var index: Index = .{};
            defer index.deinit(allocator);
            // Distinct lengths force equality to return without byte scanning.
            // The 65th collision then needs the probe's own second checkpoint.
            for (1..data.len + 1) |length| try std.testing.expect(!try index.insert(allocator, .{ .bytes = data[0..length], .hash = 0 }, &state));
            try std.testing.expectEqual(data.len, index.count);
            for (1..data.len + 1) |length| {
                const key: Key = .{ .bytes = data[0..length], .hash = 0 };
                const found = try Index.find(index.slots, key, &state);
                try std.testing.expect(found.existing);
                try std.testing.expectEqual(length, index.slots[found.index].?.bytes.len);
                try std.testing.expect(try index.insert(allocator, key, &state));
            }
            try std.testing.expectEqual(data.len, index.count);
            const before = index.slots.len;
            try index.grow(allocator, &state);
            try std.testing.expectEqual(before * 2, index.slots.len);
            try std.testing.expectEqual(data.len, index.count);
            for (1..data.len + 1) |length| try std.testing.expect((try Index.find(index.slots, .{ .bytes = data[0..length], .hash = 0 }, &state)).existing);
        }
    };
    try Fixture.run(std.testing.allocator, &bytes, original);
    const checks = callback.checks;
    try std.testing.expect(checks > 200);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| for (1..checks + 1) |fail_at| {
        callback = .{ .fail_at = fail_at, .cause = cause };
        try std.testing.expectError(cause, Fixture.run(std.testing.allocator, &bytes, original));
    };
    callback = .{};
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ &bytes, original });
}
