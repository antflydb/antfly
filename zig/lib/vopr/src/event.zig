// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const ids = @import("id.zig");

pub const Kind = enum { message_enqueued, state_change, client_response, injected_error, domain };

pub const Event = struct {
    id: ids.StableId,
    name: []const u8,
    kind: Kind,
    actor_id: ?ids.StableId = null,
    resource_id: ?ids.StableId = null,
    payload_digest: u64 = 0,

    pub fn named(kind: Kind, name: []const u8, payload_digest: u64) Event {
        return .{ .id = ids.stable("event", name), .name = name, .kind = kind, .payload_digest = payload_digest };
    }
};

pub const Sink = struct {
    events: std.ArrayListUnmanaged(Event) = .empty,

    pub fn deinit(self: *Sink, allocator: std.mem.Allocator) void {
        self.events.deinit(allocator);
        self.* = .{};
    }

    pub fn emit(self: *Sink, allocator: std.mem.Allocator, value: Event) !void {
        if (value.id == 0) return error.InvalidEventId;
        if (value.name.len == 0) return error.EmptyEventName;
        try self.events.append(allocator, value);
    }

    pub fn emitNamed(self: *Sink, allocator: std.mem.Allocator, kind: Kind, name: []const u8, payload_digest: u64) !void {
        try self.emit(allocator, Event.named(kind, name, payload_digest));
    }
};

test "event IDs are stable and namespaced" {
    const value = Event.named(.state_change, "toy.changed", 4);
    try std.testing.expectEqual(ids.stable("event", "toy.changed"), value.id);
    try std.testing.expect(value.id != ids.stable("transition", "toy.changed"));
}
