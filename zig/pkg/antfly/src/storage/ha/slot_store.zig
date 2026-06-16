// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Durable HA replication slot registry.
//!
//! Slots are primary-local retention contracts. They track how far each standby
//! can restart, how much WAL it has received, and how much it has applied. The
//! store is WAL-backed so primary restart preserves retention state before the
//! streaming transport and base-backup layers exist.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Crc32 = std.hash.Crc32;
const wal_mod = @import("../wal.zig");

const magic = [8]u8{ 'A', 'F', 'H', 'A', 'S', 'L', 'T', '\n' };
const version: u16 = 1;
const header_len: usize = 60;

const version_offset: usize = 8;
const event_type_offset: usize = 10;
const name_len_offset: usize = 12;
const timeline_id_offset: usize = 16;
const restart_lsn_offset: usize = 24;
const received_lsn_offset: usize = 32;
const applied_lsn_offset: usize = 40;
const flags_offset: usize = 48;
const body_crc_offset: usize = 52;
const header_crc_offset: usize = 56;

var test_path_counter: u64 = 0;

comptime {
    std.debug.assert(header_crc_offset + 4 == header_len);
}

pub const SlotStatus = enum {
    healthy,
    lagging,
    reseed_required,
};

pub const SlotState = struct {
    name: []const u8,
    timeline_id: u64,
    restart_lsn: u64,
    received_lsn: u64,
    applied_lsn: u64,
    active: bool = true,
    reseed_required: bool = false,

    pub fn lagFrom(self: SlotState, primary_lsn: u64) u64 {
        return primary_lsn -| self.applied_lsn;
    }

    pub fn status(self: SlotState, primary_lsn: u64, max_lag_lsn: u64) SlotStatus {
        if (self.reseed_required) return .reseed_required;
        if (max_lag_lsn > 0 and self.lagFrom(primary_lsn) > max_lag_lsn) return .lagging;
        return .healthy;
    }
};

pub const RetentionPolicy = struct {
    max_lag_lsn: u64 = 0,
};

pub const RetentionSnapshot = struct {
    primary_lsn: u64,
    oldest_restart_lsn: u64,
    retained_lsn_count: u64,
    active_slots: usize,
    reseed_recommended: usize,
};

const OwnedSlot = struct {
    state: SlotState,

    fn deinit(self: *OwnedSlot, alloc: Allocator) void {
        alloc.free(self.state.name);
        self.* = undefined;
    }
};

const EventType = enum(u16) {
    upsert = 1,
    drop = 2,
    _,
};

const EventView = struct {
    event_type: EventType,
    state: SlotState,
};

pub const OpenOptions = struct {
    wal_options: wal_mod.WalOptions = .{},
};

pub const SlotStore = struct {
    alloc: Allocator,
    wal: wal_mod.WAL,
    slots: std.ArrayListUnmanaged(OwnedSlot) = .empty,

    pub fn open(alloc: Allocator, path: [*:0]const u8, options: OpenOptions) !SlotStore {
        var store = SlotStore{
            .alloc = alloc,
            .wal = try wal_mod.WAL.open(path, options.wal_options),
        };
        errdefer store.close();
        try store.replay();
        return store;
    }

    pub fn close(self: *SlotStore) void {
        for (self.slots.items) |*slot| slot.deinit(self.alloc);
        self.slots.deinit(self.alloc);
        self.wal.close();
        self.* = undefined;
    }

    pub fn count(self: *const SlotStore) usize {
        return self.slots.items.len;
    }

    pub fn createOrUpdate(self: *SlotStore, state: SlotState) !void {
        if (state.name.len == 0) return error.InvalidSlotName;
        if (state.restart_lsn > state.received_lsn) return error.InvalidSlotProgress;
        if (state.applied_lsn > state.received_lsn) return error.InvalidSlotProgress;
        try self.persistAndApply(.{
            .event_type = .upsert,
            .state = state,
        });
    }

    pub fn updateProgress(
        self: *SlotStore,
        name: []const u8,
        received_lsn: u64,
        applied_lsn: u64,
    ) !void {
        const current = self.get(name) orelse return error.SlotNotFound;
        if (received_lsn < current.received_lsn) return error.InvalidSlotProgress;
        if (applied_lsn < current.applied_lsn) return error.InvalidSlotProgress;
        if (applied_lsn > received_lsn) return error.InvalidSlotProgress;
        var next = current;
        next.received_lsn = received_lsn;
        next.applied_lsn = applied_lsn;
        next.restart_lsn = applied_lsn;
        next.reseed_required = false;
        try self.createOrUpdate(next);
    }

    pub fn markReseedRequired(self: *SlotStore, name: []const u8) !void {
        const current = self.get(name) orelse return error.SlotNotFound;
        var next = current;
        next.reseed_required = true;
        try self.createOrUpdate(next);
    }

    pub fn drop(self: *SlotStore, name: []const u8) !void {
        const current = self.get(name) orelse return error.SlotNotFound;
        try self.persistAndApply(.{
            .event_type = .drop,
            .state = current,
        });
    }

    pub fn get(self: *const SlotStore, name: []const u8) ?SlotState {
        if (self.findIndex(name)) |idx| return self.slots.items[idx].state;
        return null;
    }

    pub fn retentionSnapshot(
        self: *SlotStore,
        primary_lsn: u64,
        policy: RetentionPolicy,
    ) !RetentionSnapshot {
        var mark_reseed: std.ArrayListUnmanaged([]const u8) = .empty;
        defer {
            for (mark_reseed.items) |name| self.alloc.free(name);
            mark_reseed.deinit(self.alloc);
        }

        var oldest = primary_lsn + 1;
        var active: usize = 0;
        var reseed: usize = 0;

        for (self.slots.items) |slot| {
            if (!slot.state.active) continue;
            active += 1;
            if (slot.state.reseed_required or
                (policy.max_lag_lsn > 0 and primary_lsn -| slot.state.restart_lsn > policy.max_lag_lsn))
            {
                if (!slot.state.reseed_required) {
                    const name = try self.alloc.dupe(u8, slot.state.name);
                    errdefer self.alloc.free(name);
                    try mark_reseed.append(self.alloc, name);
                }
                reseed += 1;
                continue;
            }
            oldest = @min(oldest, slot.state.restart_lsn);
        }

        for (mark_reseed.items) |name| try self.markReseedRequired(name);

        if (active == 0 or oldest == primary_lsn + 1) oldest = primary_lsn + 1;
        return .{
            .primary_lsn = primary_lsn,
            .oldest_restart_lsn = oldest,
            .retained_lsn_count = if (oldest <= primary_lsn) primary_lsn - oldest + 1 else 0,
            .active_slots = active,
            .reseed_recommended = reseed,
        };
    }

    fn replay(self: *SlotStore) !void {
        const entries = try self.wal.iterateFrom(self.alloc, 1);
        defer {
            for (entries) |entry| self.alloc.free(entry.data);
            self.alloc.free(entries);
        }

        for (entries) |entry| {
            const event = try decodeEvent(entry.data);
            try self.applyEvent(event);
        }
    }

    fn persistAndApply(self: *SlotStore, event: EventView) !void {
        const encoded = try encodeEvent(self.alloc, event);
        defer self.alloc.free(encoded);
        _ = try self.wal.append(encoded);
        try self.applyEvent(event);
    }

    fn applyEvent(self: *SlotStore, event: EventView) !void {
        switch (event.event_type) {
            .upsert => {
                if (self.findIndex(event.state.name)) |idx| {
                    const owned_name = try self.alloc.dupe(u8, event.state.name);
                    errdefer self.alloc.free(owned_name);
                    self.alloc.free(self.slots.items[idx].state.name);
                    self.slots.items[idx].state = event.state;
                    self.slots.items[idx].state.name = owned_name;
                    return;
                }
                const owned_name = try self.alloc.dupe(u8, event.state.name);
                errdefer self.alloc.free(owned_name);
                var owned = OwnedSlot{ .state = event.state };
                owned.state.name = owned_name;
                try self.slots.append(self.alloc, owned);
            },
            .drop => {
                const idx = self.findIndex(event.state.name) orelse return;
                var removed = self.slots.orderedRemove(idx);
                removed.deinit(self.alloc);
            },
            _ => return error.UnsupportedSlotEvent,
        }
    }

    fn findIndex(self: *const SlotStore, name: []const u8) ?usize {
        for (self.slots.items, 0..) |slot, idx| {
            if (std.mem.eql(u8, slot.state.name, name)) return idx;
        }
        return null;
    }
};

fn encodeEvent(alloc: Allocator, event: EventView) ![]u8 {
    if (event.state.name.len > std.math.maxInt(u32)) return error.SlotNameTooLong;
    const total_len = header_len + event.state.name.len;
    const out = try alloc.alloc(u8, total_len);
    errdefer alloc.free(out);

    @memset(out[0..header_len], 0);
    @memcpy(out[0..8], &magic);
    std.mem.writeInt(u16, out[version_offset..][0..2], version, .little);
    std.mem.writeInt(u16, out[event_type_offset..][0..2], @intFromEnum(event.event_type), .little);
    std.mem.writeInt(u32, out[name_len_offset..][0..4], @intCast(event.state.name.len), .little);
    std.mem.writeInt(u64, out[timeline_id_offset..][0..8], event.state.timeline_id, .little);
    std.mem.writeInt(u64, out[restart_lsn_offset..][0..8], event.state.restart_lsn, .little);
    std.mem.writeInt(u64, out[received_lsn_offset..][0..8], event.state.received_lsn, .little);
    std.mem.writeInt(u64, out[applied_lsn_offset..][0..8], event.state.applied_lsn, .little);
    var flags: u32 = 0;
    if (event.state.active) flags |= 1 << 0;
    if (event.state.reseed_required) flags |= 1 << 1;
    std.mem.writeInt(u32, out[flags_offset..][0..4], flags, .little);
    @memcpy(out[header_len..], event.state.name);
    std.mem.writeInt(u32, out[body_crc_offset..][0..4], Crc32.hash(out[header_len..]), .little);
    std.mem.writeInt(u32, out[header_crc_offset..][0..4], Crc32.hash(out[0..header_crc_offset]), .little);
    return out;
}

fn decodeEvent(bytes: []const u8) !EventView {
    if (bytes.len < header_len) return error.EndOfStream;
    if (!std.mem.eql(u8, bytes[0..8], &magic)) return error.InvalidMagic;
    const stored_header_crc = std.mem.readInt(u32, bytes[header_crc_offset..][0..4], .little);
    if (stored_header_crc != Crc32.hash(bytes[0..header_crc_offset])) return error.HeaderCrcMismatch;
    const decoded_version = std.mem.readInt(u16, bytes[version_offset..][0..2], .little);
    if (decoded_version == 0 or decoded_version > version) return error.UnsupportedVersion;

    const name_len = std.mem.readInt(u32, bytes[name_len_offset..][0..4], .little);
    const total_len = header_len + @as(usize, @intCast(name_len));
    if (bytes.len < total_len) return error.EndOfStream;
    if (bytes.len != total_len) return error.TrailingBytes;
    const name = bytes[header_len..total_len];
    const stored_body_crc = std.mem.readInt(u32, bytes[body_crc_offset..][0..4], .little);
    if (stored_body_crc != Crc32.hash(name)) return error.BodyCrcMismatch;

    const flags = std.mem.readInt(u32, bytes[flags_offset..][0..4], .little);
    return .{
        .event_type = @enumFromInt(std.mem.readInt(u16, bytes[event_type_offset..][0..2], .little)),
        .state = .{
            .name = name,
            .timeline_id = std.mem.readInt(u64, bytes[timeline_id_offset..][0..8], .little),
            .restart_lsn = std.mem.readInt(u64, bytes[restart_lsn_offset..][0..8], .little),
            .received_lsn = std.mem.readInt(u64, bytes[received_lsn_offset..][0..8], .little),
            .applied_lsn = std.mem.readInt(u64, bytes[applied_lsn_offset..][0..8], .little),
            .active = (flags & (1 << 0)) != 0,
            .reseed_required = (flags & (1 << 1)) != 0,
        },
    };
}

fn testPath(alloc: Allocator, comptime name: []const u8) ![:0]u8 {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-slot-store-" ++ name ++ "-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(raw);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), raw) catch {};
    return try alloc.dupeZ(u8, raw);
}

test "storage.ha slot store persists slot progress across reopen" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "reopen");
    defer alloc.free(path);

    {
        var store = try SlotStore.open(alloc, path.ptr, .{});
        defer store.close();
        try store.createOrUpdate(.{
            .name = "standby-a",
            .timeline_id = 1,
            .restart_lsn = 1,
            .received_lsn = 5,
            .applied_lsn = 3,
        });
        try store.updateProgress("standby-a", 8, 7);
    }

    {
        var reopened = try SlotStore.open(alloc, path.ptr, .{});
        defer reopened.close();
        try std.testing.expectEqual(@as(usize, 1), reopened.count());
        const slot = reopened.get("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u64, 1), slot.timeline_id);
        try std.testing.expectEqual(@as(u64, 7), slot.restart_lsn);
        try std.testing.expectEqual(@as(u64, 8), slot.received_lsn);
        try std.testing.expectEqual(@as(u64, 7), slot.applied_lsn);
    }
}

test "storage.ha slot store computes retention floor from active slots" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "retention");
    defer alloc.free(path);

    var store = try SlotStore.open(alloc, path.ptr, .{});
    defer store.close();
    try store.createOrUpdate(.{ .name = "a", .timeline_id = 1, .restart_lsn = 4, .received_lsn = 8, .applied_lsn = 4 });
    try store.createOrUpdate(.{ .name = "b", .timeline_id = 1, .restart_lsn = 7, .received_lsn = 9, .applied_lsn = 7 });

    const snapshot = try store.retentionSnapshot(10, .{});
    try std.testing.expectEqual(@as(u64, 4), snapshot.oldest_restart_lsn);
    try std.testing.expectEqual(@as(u64, 7), snapshot.retained_lsn_count);
    try std.testing.expectEqual(@as(usize, 2), snapshot.active_slots);
    try std.testing.expectEqual(@as(usize, 0), snapshot.reseed_recommended);
}

test "storage.ha slot store marks slots for reseed when lag cap is exceeded" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "reseed");
    defer alloc.free(path);

    var store = try SlotStore.open(alloc, path.ptr, .{});
    defer store.close();
    try store.createOrUpdate(.{ .name = "slow", .timeline_id = 1, .restart_lsn = 2, .received_lsn = 3, .applied_lsn = 2 });
    try store.createOrUpdate(.{ .name = "fast", .timeline_id = 1, .restart_lsn = 9, .received_lsn = 10, .applied_lsn = 9 });

    const snapshot = try store.retentionSnapshot(12, .{ .max_lag_lsn = 5 });
    try std.testing.expectEqual(@as(usize, 1), snapshot.reseed_recommended);
    try std.testing.expectEqual(@as(u64, 9), snapshot.oldest_restart_lsn);
    const slow = store.get("slow") orelse return error.TestExpectedEqual;
    try std.testing.expect(slow.reseed_required);
}

test "storage.ha slot store drops slots and releases retention" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "drop");
    defer alloc.free(path);

    var store = try SlotStore.open(alloc, path.ptr, .{});
    defer store.close();
    try store.createOrUpdate(.{ .name = "a", .timeline_id = 1, .restart_lsn = 4, .received_lsn = 8, .applied_lsn = 4 });
    try store.drop("a");
    try std.testing.expectEqual(@as(usize, 0), store.count());
    const snapshot = try store.retentionSnapshot(10, .{});
    try std.testing.expectEqual(@as(u64, 11), snapshot.oldest_restart_lsn);
    try std.testing.expectEqual(@as(u64, 0), snapshot.retained_lsn_count);
}
