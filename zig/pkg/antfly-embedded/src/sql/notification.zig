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

//! Owned transaction-local notification intents. Native publication, listener
//! delivery and queue reclamation belong to the accepting metadata writer.
const std = @import("std");
const A = std.mem.Allocator;
pub const max_channel_bytes = 63;
pub const max_payload_bytes = 7999;
pub const max_commands = 128;
pub const max_pending_bytes = 64 * 1024;
// Leave room for the frozen owner/manifest in the common decision envelope.
pub const max_encoded_pending_bytes = 128 * 1024;
pub const Command = union(enum) {
    listen: []const u8,
    unlisten: []const u8,
    unlisten_all,
    notify: struct { channel: []const u8, payload: []const u8 = "" },

    pub fn validate(self: Command) !void {
        const channel = switch (self) {
            .listen, .unlisten => |value| value,
            .notify => |value| value.channel,
            .unlisten_all => return,
        };
        if (channel.len == 0 or channel.len > max_channel_bytes or std.mem.indexOfScalar(u8, channel, 0) != null or !std.unicode.utf8ValidateSlice(channel)) return error.SqlInvalidParameterValue;
        if (self == .notify) {
            const payload = self.notify.payload;
            if (payload.len > max_payload_bytes or std.mem.indexOfScalar(u8, payload, 0) != null or !std.unicode.utf8ValidateSlice(payload)) return error.SqlInvalidParameterValue;
        }
    }
    pub fn tag(self: Command) []const u8 {
        return switch (self) {
            .listen => "LISTEN",
            .unlisten, .unlisten_all => "UNLISTEN",
            .notify => "NOTIFY",
        };
    }
    pub fn bytes(self: Command) usize {
        return switch (self) {
            .listen, .unlisten => |v| v.len,
            .notify => |v| v.channel.len + v.payload.len,
            .unlisten_all => 0,
        };
    }
    pub fn encodedBytes(self: Command) usize {
        var buffer: [256]u8 = undefined;
        var counter = std.Io.Writer.Discarding.init(&buffer);
        std.json.Stringify.value(self, .{}, &counter.writer) catch unreachable;
        return @intCast(counter.fullCount());
    }
    pub fn clone(self: Command, a: A) !Command {
        return switch (self) {
            .listen => |v| .{ .listen = try a.dupe(u8, v) },
            .unlisten => |v| .{ .unlisten = try a.dupe(u8, v) },
            .unlisten_all => .unlisten_all,
            .notify => |v| blk: {
                const channel = try a.dupe(u8, v.channel);
                errdefer a.free(channel);
                break :blk .{ .notify = .{ .channel = channel, .payload = try a.dupe(u8, v.payload) } };
            },
        };
    }
    pub fn deinit(self: Command, a: A) void {
        switch (self) {
            .listen, .unlisten => |v| a.free(v),
            .notify => |v| {
                a.free(v.channel);
                a.free(v.payload);
            },
            .unlisten_all => {},
        }
    }
};

pub const EventKey = struct { channel: []const u8, payload: []const u8 };
const EventContext = struct {
    pub fn hash(_: @This(), value: EventKey) u64 {
        var hash_ = std.hash.Wyhash.init(0);
        var size: [8]u8 = undefined;
        std.mem.writeInt(u64, &size, value.channel.len, .little);
        hash_.update(&size);
        hash_.update(value.channel);
        hash_.update(value.payload);
        return hash_.final();
    }
    pub fn eql(_: @This(), left: EventKey, right: EventKey) bool {
        return std.mem.eql(u8, left.channel, right.channel) and std.mem.eql(u8, left.payload, right.payload);
    }
};
pub const EventSet = std.HashMapUnmanaged(EventKey, void, EventContext, std.hash_map.default_max_load_percentage);
pub const Pending = struct {
    commands: std.ArrayListUnmanaged(Command) = .empty,
    bytes: usize = 0,
    encoded_bytes: usize = 2,
    events: EventSet = .empty,
    pub fn deinit(self: *Pending, a: A) void {
        self.truncate(a, 0);
        self.commands.deinit(a);
        self.events.deinit(a);
        self.* = .{};
    }
    pub fn truncate(self: *Pending, a: A, count: usize) void {
        std.debug.assert(count <= self.commands.items.len);
        for (self.commands.items[count..]) |command| {
            self.bytes -= command.bytes();
            self.encoded_bytes -= command.encodedBytes() + 1;
            if (command == .notify) std.debug.assert(self.events.remove(.{ .channel = command.notify.channel, .payload = command.notify.payload }));
            command.deinit(a);
        }
        self.commands.shrinkRetainingCapacity(count);
    }
    pub fn append(self: *Pending, a: A, command: Command) !void {
        try command.validate();
        // PostgreSQL folds only duplicate channel/payload pairs in the same
        // transaction. Registration commands retain their original ordering.
        if (command == .notify and self.events.contains(.{ .channel = command.notify.channel, .payload = command.notify.payload })) return;
        const encoded = command.encodedBytes() + 1;
        if (self.commands.items.len == max_commands or command.bytes() > max_pending_bytes -| self.bytes or encoded > max_encoded_pending_bytes -| self.encoded_bytes) return error.SqlProgramLimitExceeded;
        try self.commands.ensureUnusedCapacity(a, 1);
        if (command == .notify) try self.events.ensureUnusedCapacity(a, 1);
        const owned = try command.clone(a);
        if (owned == .notify) self.events.putAssumeCapacityNoClobber(.{ .channel = owned.notify.channel, .payload = owned.notify.payload }, {});
        self.commands.appendAssumeCapacity(owned);
        self.bytes += command.bytes();
        self.encoded_bytes += encoded;
    }
    pub fn jsonStringify(self: Pending, writer: anytype) !void {
        try writer.write(self.commands.items);
    }
    pub fn jsonParse(a: A, source: anytype, options: std.json.ParseOptions) !Pending {
        const commands = try std.json.innerParse([]const Command, a, source, options);
        return fromCommands(a, commands) catch |err| if (err == error.OutOfMemory) error.OutOfMemory else error.UnexpectedToken;
    }
    pub fn jsonParseFromValue(a: A, value: std.json.Value, options: std.json.ParseOptions) !Pending {
        const commands = try std.json.innerParseFromValue([]const Command, a, value, options);
        return fromCommands(a, commands) catch |err| if (err == error.OutOfMemory) error.OutOfMemory else error.UnexpectedToken;
    }
    pub fn fromCommands(a: A, commands: []const Command) !Pending {
        var result: Pending = .{};
        errdefer result.deinit(a);
        if (commands.len > max_commands) return error.SqlProgramLimitExceeded;
        for (commands) |command| try result.append(a, command);
        return result;
    }
    pub fn clone(self: Pending, a: A) !Pending {
        var result: Pending = .{};
        errdefer result.deinit(a);
        for (self.commands.items) |command| try result.append(a, command);
        return result;
    }
};

/// Immutable input to the existing SQL commit manifest. It has no authority
/// until joined to the exact live connection and source transaction by apply.
pub const Plan = struct {
    /// Rollback seals the exact intents without reserving delivery capacity.
    discard: bool = false,
    connection: [32]u8,
    generation: u64,
    sender_pid: i32,
    commands: []const Command,
    pub fn validate(self: Plan) !void {
        if (self.generation == 0 or self.sender_pid <= 0 or self.commands.len == 0 or self.commands.len > max_commands) return error.InvalidSqlConnection;
        var bytes: usize = 0;
        var encoded: usize = 2;
        for (self.commands) |command| {
            try command.validate();
            bytes += command.bytes();
            encoded += command.encodedBytes() + 1;
            if (bytes > max_pending_bytes or encoded > max_encoded_pending_bytes) return error.SqlProgramLimitExceeded;
        }
    }
};

pub const Cursor = struct {
    sequence: u64 = 1,
    ordinal: u16 = 0,
    pub fn order(left: Cursor, right: Cursor) std.math.Order {
        const sequence = std.math.order(left.sequence, right.sequence);
        return if (sequence == .eq) std.math.order(left.ordinal, right.ordinal) else sequence;
    }
};
pub const Event = struct { sequence: u64, ordinal: u16, channel: []const u8, payload: []const u8, sender_pid: i32 };
pub const Page = struct { events: []const Event = &.{}, cursor: Cursor = .{}, pending: bool = false, waiting: bool = false, deferred: bool = false, acknowledged: Cursor = .{}, offered: Cursor = .{}, subscribed: bool = false, revision: u64 = 0 };
pub const DeliveryControl = union(enum) { poll: u16, acknowledge: Cursor };

test "SQL notifications own intents fold only duplicate events and restore savepoint prefixes" {
    const Harness = struct {
        fn run(a: A) !void {
            var pending: Pending = .{};
            defer pending.deinit(a);
            var channel = [_]u8{'x'};
            try pending.append(a, .{ .listen = &channel });
            try pending.append(a, .{ .notify = .{ .channel = &channel, .payload = "first" } });
            const savepoint = pending.commands.items.len;
            try pending.append(a, .{ .notify = .{ .channel = &channel, .payload = "first" } });
            try std.testing.expectEqual(savepoint, pending.commands.items.len);
            try pending.append(a, .{ .notify = .{ .channel = &channel, .payload = "second" } });
            try pending.append(a, .{ .unlisten = &channel });
            channel[0] = 'z';
            try std.testing.expectEqualStrings("x", pending.commands.items[0].listen);
            var copy = try pending.clone(a);
            defer copy.deinit(a);
            pending.truncate(a, savepoint);
            try std.testing.expectEqual(@as(usize, 7), pending.bytes);
            try std.testing.expectEqual(@as(usize, 4), copy.commands.items.len);
            try pending.append(a, .{ .notify = .{ .channel = "x", .payload = "second" } });
            try std.testing.expectEqual(@as(usize, 3), pending.commands.items.len);
            try std.testing.expectEqualStrings("UNLISTEN", (@as(Command, .unlisten_all)).tag());
        }
    };
    try Harness.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

pub fn commandsEqual(left: []const Command, right: []const Command) bool {
    if (left.len != right.len) return false;
    for (left, right) |l, r| {
        if (std.meta.activeTag(l) != std.meta.activeTag(r)) return false;
        switch (l) {
            .listen => |value| if (!std.mem.eql(u8, value, r.listen)) return false,
            .unlisten => |value| if (!std.mem.eql(u8, value, r.unlisten)) return false,
            .unlisten_all => {},
            .notify => |value| if (!std.mem.eql(u8, value.channel, r.notify.channel) or !std.mem.eql(u8, value.payload, r.notify.payload)) return false,
        }
    }
    return true;
}
