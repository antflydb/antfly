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
//! can restart, how much WAL it has received, how much it has applied, and the
//! highest LSN safe for standby read snapshots. The store is WAL-backed so
//! primary restart preserves retention state before the streaming transport and
//! base-backup layers exist.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Crc32 = std.hash.Crc32;
const wal_mod = @import("../wal.zig");

const magic = [8]u8{ 'A', 'F', 'H', 'A', 'S', 'L', 'T', '\n' };
const version: u16 = 3;
const header_len: usize = 60;
const v2_error_len_size: usize = 4;
const v3_body_prefix_len: usize = 12;

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
    safe_read_lsn: u64,
    active: bool = true,
    reseed_required: bool = false,
    last_error: ?[]const u8 = null,

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
    max_retained_bytes: u64 = 0,
    max_retained_age_ns: u64 = 0,
};

pub const RetentionSnapshot = struct {
    primary_lsn: u64,
    oldest_restart_lsn: u64,
    retained_lsn_count: u64,
    retained_byte_count: u64 = 0,
    retained_age_ns: u64 = 0,
    active_slots: usize,
    reseed_recommended: usize,
};

const OwnedSlot = struct {
    state: SlotState,

    fn deinit(self: *OwnedSlot, alloc: Allocator) void {
        alloc.free(self.state.name);
        if (self.state.last_error) |last_error| alloc.free(last_error);
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
        try validateSlotState(state);
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
        safe_read_lsn: u64,
    ) !void {
        const current = self.get(name) orelse return error.SlotNotFound;
        if (received_lsn < current.received_lsn) return error.InvalidSlotProgress;
        if (applied_lsn < current.applied_lsn) return error.InvalidSlotProgress;
        if (safe_read_lsn < current.safe_read_lsn) return error.InvalidSlotProgress;
        if (applied_lsn > received_lsn) return error.InvalidSlotProgress;
        if (safe_read_lsn > applied_lsn) return error.InvalidSlotProgress;
        var next = current;
        next.received_lsn = received_lsn;
        next.applied_lsn = applied_lsn;
        next.safe_read_lsn = safe_read_lsn;
        next.restart_lsn = received_lsn;
        next.reseed_required = false;
        next.last_error = null;
        try self.createOrUpdate(next);
    }

    pub fn setLastError(self: *SlotStore, name: []const u8, last_error: []const u8) !void {
        if (last_error.len == 0) return error.InvalidReplicationError;
        var next = self.get(name) orelse return error.SlotNotFound;
        next.last_error = last_error;
        try self.createOrUpdate(next);
    }

    pub fn clearLastError(self: *SlotStore, name: []const u8) !void {
        var next = self.get(name) orelse return error.SlotNotFound;
        if (next.last_error == null) return;
        next.last_error = null;
        try self.createOrUpdate(next);
    }

    pub fn markReseedRequired(self: *SlotStore, name: []const u8) !void {
        const current = self.get(name) orelse return error.SlotNotFound;
        var next = current;
        next.reseed_required = true;
        try self.createOrUpdate(next);
    }

    pub fn markActiveSlotsAtRestartLsnForTimeline(
        self: *SlotStore,
        timeline_id: u64,
        restart_lsn: u64,
    ) !usize {
        var mark_reseed: std.ArrayListUnmanaged([]const u8) = .empty;
        defer {
            for (mark_reseed.items) |name| self.alloc.free(name);
            mark_reseed.deinit(self.alloc);
        }

        for (self.slots.items) |slot| {
            if (!slot.state.active) continue;
            if (slot.state.reseed_required) continue;
            if (slot.state.timeline_id != timeline_id) continue;
            if (slot.state.restart_lsn != restart_lsn) continue;
            const name = try self.alloc.dupe(u8, slot.state.name);
            errdefer self.alloc.free(name);
            try mark_reseed.append(self.alloc, name);
        }

        for (mark_reseed.items) |name| try self.markReseedRequired(name);
        return mark_reseed.items.len;
    }

    pub fn pause(self: *SlotStore, name: []const u8) !void {
        const current = self.get(name) orelse return error.SlotNotFound;
        if (!current.active) return;
        var next = current;
        next.active = false;
        try self.createOrUpdate(next);
    }

    pub fn resumeSlot(self: *SlotStore, name: []const u8) !void {
        const current = self.get(name) orelse return error.SlotNotFound;
        if (current.active) return;
        var next = current;
        next.active = true;
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

    pub fn listAlloc(self: *const SlotStore, alloc: Allocator) ![]SlotState {
        const out = try alloc.alloc(SlotState, self.slots.items.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |slot| {
                alloc.free(slot.name);
                if (slot.last_error) |last_error| alloc.free(last_error);
            }
            alloc.free(out);
        }

        for (self.slots.items, 0..) |slot, idx| {
            const owned_name = try alloc.dupe(u8, slot.state.name);
            errdefer alloc.free(owned_name);
            const owned_last_error = if (slot.state.last_error) |last_error|
                try alloc.dupe(u8, last_error)
            else
                null;
            errdefer if (owned_last_error) |last_error| alloc.free(last_error);

            out[idx] = slot.state;
            out[idx].name = owned_name;
            out[idx].last_error = owned_last_error;
            filled += 1;
        }
        return out;
    }

    pub fn retentionSnapshot(
        self: *SlotStore,
        primary_lsn: u64,
        policy: RetentionPolicy,
    ) !RetentionSnapshot {
        return try self.retentionSnapshotInternal(primary_lsn, null, policy);
    }

    pub fn retentionSnapshotForTimeline(
        self: *SlotStore,
        primary_lsn: u64,
        timeline_id: u64,
        policy: RetentionPolicy,
    ) !RetentionSnapshot {
        return try self.retentionSnapshotInternal(primary_lsn, timeline_id, policy);
    }

    fn retentionSnapshotInternal(
        self: *SlotStore,
        primary_lsn: u64,
        current_timeline_id: ?u64,
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
            if (current_timeline_id) |timeline_id| {
                if (slot.state.timeline_id != timeline_id) {
                    if (!slot.state.reseed_required) {
                        const name = try self.alloc.dupe(u8, slot.state.name);
                        errdefer self.alloc.free(name);
                        try mark_reseed.append(self.alloc, name);
                    }
                    reseed += 1;
                    continue;
                }
            }
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
            active += 1;
            oldest = @min(oldest, slot.state.restart_lsn);
        }

        for (mark_reseed.items) |name| try self.markReseedRequired(name);

        const retained_lsn_count: u64 = if (active == 0 or oldest == primary_lsn + 1) blk: {
            oldest = primary_lsn;
            break :blk 0;
        } else if (oldest < primary_lsn)
            primary_lsn - oldest
        else
            1;
        return .{
            .primary_lsn = primary_lsn,
            .oldest_restart_lsn = oldest,
            .retained_lsn_count = retained_lsn_count,
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
        try validateSlotState(event.state);
        switch (event.event_type) {
            .upsert => {
                if (self.findIndex(event.state.name)) |idx| {
                    const owned_name = try self.alloc.dupe(u8, event.state.name);
                    errdefer self.alloc.free(owned_name);
                    const owned_last_error = if (event.state.last_error) |last_error|
                        try self.alloc.dupe(u8, last_error)
                    else
                        null;
                    errdefer if (owned_last_error) |last_error| self.alloc.free(last_error);
                    self.alloc.free(self.slots.items[idx].state.name);
                    if (self.slots.items[idx].state.last_error) |last_error| self.alloc.free(last_error);
                    self.slots.items[idx].state = event.state;
                    self.slots.items[idx].state.name = owned_name;
                    self.slots.items[idx].state.last_error = owned_last_error;
                    return;
                }
                const owned_name = try self.alloc.dupe(u8, event.state.name);
                errdefer self.alloc.free(owned_name);
                const owned_last_error = if (event.state.last_error) |last_error|
                    try self.alloc.dupe(u8, last_error)
                else
                    null;
                errdefer if (owned_last_error) |last_error| self.alloc.free(last_error);
                var owned = OwnedSlot{ .state = event.state };
                owned.state.name = owned_name;
                owned.state.last_error = owned_last_error;
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

fn validateSlotState(state: SlotState) !void {
    if (state.name.len == 0) return error.InvalidSlotName;
    if (state.applied_lsn > state.received_lsn) return error.InvalidSlotProgress;
    if (state.safe_read_lsn > state.applied_lsn) return error.InvalidSlotProgress;
    if (state.last_error) |last_error| {
        if (last_error.len == 0) return error.InvalidReplicationError;
    }
}

pub fn freeSlotList(alloc: Allocator, slots: []SlotState) void {
    for (slots) |slot| {
        alloc.free(slot.name);
        if (slot.last_error) |last_error| alloc.free(last_error);
    }
    alloc.free(slots);
}

fn encodeEvent(alloc: Allocator, event: EventView) ![]u8 {
    if (event.state.name.len > std.math.maxInt(u32)) return error.SlotNameTooLong;
    const last_error = event.state.last_error orelse "";
    if (last_error.len > std.math.maxInt(u32)) return error.ReplicationErrorTooLong;
    const body_len = v3_body_prefix_len + event.state.name.len + last_error.len;
    const total_len = header_len + body_len;
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
    std.mem.writeInt(u32, out[header_len..][0..4], @intCast(last_error.len), .little);
    std.mem.writeInt(u64, out[header_len + v2_error_len_size ..][0..8], event.state.safe_read_lsn, .little);
    @memcpy(out[header_len + v3_body_prefix_len ..][0..event.state.name.len], event.state.name);
    @memcpy(out[header_len + v3_body_prefix_len + event.state.name.len ..][0..last_error.len], last_error);
    std.mem.writeInt(u32, out[body_crc_offset..][0..4], Crc32.hash(out[header_len..total_len]), .little);
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

    const name_len: usize = @intCast(std.mem.readInt(u32, bytes[name_len_offset..][0..4], .little));
    const body = try decodeEventBody(bytes, decoded_version, name_len);
    const total_len = body.total_len;
    if (bytes.len < total_len) return error.EndOfStream;
    if (bytes.len != total_len) return error.TrailingBytes;
    const stored_body_crc = std.mem.readInt(u32, bytes[body_crc_offset..][0..4], .little);
    if (stored_body_crc != Crc32.hash(bytes[header_len..total_len])) return error.BodyCrcMismatch;

    const flags = std.mem.readInt(u32, bytes[flags_offset..][0..4], .little);
    return .{
        .event_type = @enumFromInt(std.mem.readInt(u16, bytes[event_type_offset..][0..2], .little)),
        .state = .{
            .name = body.name,
            .timeline_id = std.mem.readInt(u64, bytes[timeline_id_offset..][0..8], .little),
            .restart_lsn = std.mem.readInt(u64, bytes[restart_lsn_offset..][0..8], .little),
            .received_lsn = std.mem.readInt(u64, bytes[received_lsn_offset..][0..8], .little),
            .applied_lsn = std.mem.readInt(u64, bytes[applied_lsn_offset..][0..8], .little),
            .safe_read_lsn = body.safe_read_lsn orelse std.mem.readInt(u64, bytes[applied_lsn_offset..][0..8], .little),
            .active = (flags & (1 << 0)) != 0,
            .reseed_required = (flags & (1 << 1)) != 0,
            .last_error = body.last_error,
        },
    };
}

const EventBodyView = struct {
    total_len: usize,
    name: []const u8,
    last_error: ?[]const u8,
    safe_read_lsn: ?u64 = null,
};

fn decodeEventBody(bytes: []const u8, decoded_version: u16, name_len: usize) !EventBodyView {
    return switch (decoded_version) {
        1 => blk: {
            const total_len = header_len + name_len;
            if (bytes.len < total_len) return error.EndOfStream;
            break :blk .{
                .total_len = total_len,
                .name = bytes[header_len..total_len],
                .last_error = null,
            };
        },
        2 => blk: {
            if (bytes.len < header_len + v2_error_len_size) return error.EndOfStream;
            const error_len: usize = @intCast(std.mem.readInt(u32, bytes[header_len..][0..4], .little));
            const name_start = header_len + v2_error_len_size;
            const error_start = try std.math.add(usize, name_start, name_len);
            const total_len = try std.math.add(usize, error_start, error_len);
            if (bytes.len < total_len) return error.EndOfStream;
            const last_error = if (error_len > 0) bytes[error_start..total_len] else null;
            break :blk .{
                .total_len = total_len,
                .name = bytes[name_start..error_start],
                .last_error = last_error,
            };
        },
        3 => blk: {
            if (bytes.len < header_len + v3_body_prefix_len) return error.EndOfStream;
            const error_len: usize = @intCast(std.mem.readInt(u32, bytes[header_len..][0..4], .little));
            const safe_read_lsn = std.mem.readInt(u64, bytes[header_len + v2_error_len_size ..][0..8], .little);
            const name_start = header_len + v3_body_prefix_len;
            const error_start = try std.math.add(usize, name_start, name_len);
            const total_len = try std.math.add(usize, error_start, error_len);
            if (bytes.len < total_len) return error.EndOfStream;
            const last_error = if (error_len > 0) bytes[error_start..total_len] else null;
            break :blk .{
                .total_len = total_len,
                .name = bytes[name_start..error_start],
                .last_error = last_error,
                .safe_read_lsn = safe_read_lsn,
            };
        },
        else => error.UnsupportedVersion,
    };
}

fn encodeV1TestEvent(alloc: Allocator, event: EventView) ![]u8 {
    if (event.state.name.len > std.math.maxInt(u32)) return error.SlotNameTooLong;
    const total_len = header_len + event.state.name.len;
    const out = try alloc.alloc(u8, total_len);
    errdefer alloc.free(out);

    @memset(out[0..header_len], 0);
    @memcpy(out[0..8], &magic);
    std.mem.writeInt(u16, out[version_offset..][0..2], 1, .little);
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

fn encodeV2TestEvent(alloc: Allocator, event: EventView) ![]u8 {
    if (event.state.name.len > std.math.maxInt(u32)) return error.SlotNameTooLong;
    const last_error = event.state.last_error orelse "";
    if (last_error.len > std.math.maxInt(u32)) return error.ReplicationErrorTooLong;
    const body_len = v2_error_len_size + event.state.name.len + last_error.len;
    const total_len = header_len + body_len;
    const out = try alloc.alloc(u8, total_len);
    errdefer alloc.free(out);

    @memset(out[0..header_len], 0);
    @memcpy(out[0..8], &magic);
    std.mem.writeInt(u16, out[version_offset..][0..2], 2, .little);
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
    std.mem.writeInt(u32, out[header_len..][0..4], @intCast(last_error.len), .little);
    @memcpy(out[header_len + v2_error_len_size ..][0..event.state.name.len], event.state.name);
    @memcpy(out[header_len + v2_error_len_size + event.state.name.len ..][0..last_error.len], last_error);
    std.mem.writeInt(u32, out[body_crc_offset..][0..4], Crc32.hash(out[header_len..]), .little);
    std.mem.writeInt(u32, out[header_crc_offset..][0..4], Crc32.hash(out[0..header_crc_offset]), .little);
    return out;
}

fn testPath(alloc: Allocator, comptime name: []const u8) ![:0]u8 {
    const raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-slot-store-" ++ name ++ "-{d}-{d}",
        .{ std.posix.system.getpid(), std.testing.random_seed },
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
            .safe_read_lsn = 3,
        });
        try store.updateProgress("standby-a", 8, 7, 6);
    }

    {
        var reopened = try SlotStore.open(alloc, path.ptr, .{});
        defer reopened.close();
        try std.testing.expectEqual(@as(usize, 1), reopened.count());
        const slot = reopened.get("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u64, 1), slot.timeline_id);
        try std.testing.expectEqual(@as(u64, 8), slot.restart_lsn);
        try std.testing.expectEqual(@as(u64, 8), slot.received_lsn);
        try std.testing.expectEqual(@as(u64, 7), slot.applied_lsn);
        try std.testing.expectEqual(@as(u64, 6), slot.safe_read_lsn);
    }
}

test "storage.ha slot store tracks transient replication error until progress" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "last-error");
    defer alloc.free(path);

    {
        var store = try SlotStore.open(alloc, path.ptr, .{});
        defer store.close();
        try store.createOrUpdate(.{
            .name = "standby-a",
            .timeline_id = 1,
            .restart_lsn = 1,
            .received_lsn = 1,
            .applied_lsn = 1,
            .safe_read_lsn = 1,
        });
        try store.setLastError("standby-a", "IntentionalApplyFailure");

        const listed = try store.listAlloc(alloc);
        defer freeSlotList(alloc, listed);
        try std.testing.expectEqualStrings("IntentionalApplyFailure", listed[0].last_error.?);
    }

    {
        var reopened = try SlotStore.open(alloc, path.ptr, .{});
        defer reopened.close();
        var slot = reopened.get("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expectEqualStrings("IntentionalApplyFailure", slot.last_error.?);

        try reopened.updateProgress("standby-a", 2, 2, 2);
        slot = reopened.get("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expect(slot.last_error == null);

        try reopened.setLastError("standby-a", "SlotInactive");
        try reopened.clearLastError("standby-a");
        slot = reopened.get("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expect(slot.last_error == null);
    }

    {
        var reopened = try SlotStore.open(alloc, path.ptr, .{});
        defer reopened.close();
        const slot = reopened.get("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expect(slot.last_error == null);
        try std.testing.expectEqual(@as(u64, 2), slot.applied_lsn);
    }
}

test "storage.ha slot store decodes v1 slot events without last error" {
    const alloc = std.testing.allocator;
    const encoded = try encodeV1TestEvent(alloc, .{
        .event_type = .upsert,
        .state = .{
            .name = "standby-a",
            .timeline_id = 1,
            .restart_lsn = 2,
            .received_lsn = 3,
            .applied_lsn = 2,
            .safe_read_lsn = 2,
        },
    });
    defer alloc.free(encoded);

    const event = try decodeEvent(encoded);
    try std.testing.expectEqual(EventType.upsert, event.event_type);
    try std.testing.expectEqualStrings("standby-a", event.state.name);
    try std.testing.expectEqual(@as(u64, 2), event.state.safe_read_lsn);
    try std.testing.expect(event.state.last_error == null);
}

test "storage.ha slot store decodes v2 slot events with safe reads at applied lsn" {
    const alloc = std.testing.allocator;
    const encoded = try encodeV2TestEvent(alloc, .{
        .event_type = .upsert,
        .state = .{
            .name = "standby-a",
            .timeline_id = 1,
            .restart_lsn = 2,
            .received_lsn = 5,
            .applied_lsn = 4,
            .safe_read_lsn = 4,
            .last_error = "TransientLag",
        },
    });
    defer alloc.free(encoded);

    const event = try decodeEvent(encoded);
    try std.testing.expectEqual(EventType.upsert, event.event_type);
    try std.testing.expectEqualStrings("standby-a", event.state.name);
    try std.testing.expectEqual(@as(u64, 4), event.state.safe_read_lsn);
    try std.testing.expectEqualStrings("TransientLag", event.state.last_error.?);
}

test "storage.ha slot store rejects invalid slot state on replay" {
    const alloc = std.testing.allocator;
    const progress_path = try testPath(alloc, "replay-bad-progress");
    defer alloc.free(progress_path);
    {
        var store = try SlotStore.open(alloc, progress_path.ptr, .{});
        defer store.close();
        const encoded = try encodeEvent(alloc, .{
            .event_type = .upsert,
            .state = .{
                .name = "standby-a",
                .timeline_id = 1,
                .restart_lsn = 4,
                .received_lsn = 3,
                .applied_lsn = 4,
                .safe_read_lsn = 4,
            },
        });
        defer alloc.free(encoded);
        _ = try store.wal.append(encoded);
    }
    try std.testing.expectError(error.InvalidSlotProgress, SlotStore.open(alloc, progress_path.ptr, .{}));

    const name_path = try testPath(alloc, "replay-empty-name");
    defer alloc.free(name_path);
    {
        var store = try SlotStore.open(alloc, name_path.ptr, .{});
        defer store.close();
        const encoded = try encodeEvent(alloc, .{
            .event_type = .upsert,
            .state = .{
                .name = "",
                .timeline_id = 1,
                .restart_lsn = 1,
                .received_lsn = 1,
                .applied_lsn = 1,
                .safe_read_lsn = 1,
            },
        });
        defer alloc.free(encoded);
        _ = try store.wal.append(encoded);
    }
    try std.testing.expectError(error.InvalidSlotName, SlotStore.open(alloc, name_path.ptr, .{}));
}

test "storage.ha slot store computes retention floor from active slots" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "retention");
    defer alloc.free(path);

    var store = try SlotStore.open(alloc, path.ptr, .{});
    defer store.close();
    try store.createOrUpdate(.{ .name = "a", .timeline_id = 1, .restart_lsn = 4, .received_lsn = 8, .applied_lsn = 4, .safe_read_lsn = 4 });
    try store.createOrUpdate(.{ .name = "b", .timeline_id = 1, .restart_lsn = 7, .received_lsn = 9, .applied_lsn = 7, .safe_read_lsn = 7 });

    const snapshot = try store.retentionSnapshot(10, .{});
    try std.testing.expectEqual(@as(u64, 4), snapshot.oldest_restart_lsn);
    try std.testing.expectEqual(@as(u64, 6), snapshot.retained_lsn_count);
    try std.testing.expectEqual(@as(usize, 2), snapshot.active_slots);
    try std.testing.expectEqual(@as(usize, 0), snapshot.reseed_recommended);
}

test "storage.ha slot store marks slots for reseed when lag cap is exceeded" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "reseed");
    defer alloc.free(path);

    var store = try SlotStore.open(alloc, path.ptr, .{});
    defer store.close();
    try store.createOrUpdate(.{ .name = "slow", .timeline_id = 1, .restart_lsn = 2, .received_lsn = 3, .applied_lsn = 2, .safe_read_lsn = 2 });
    try store.createOrUpdate(.{ .name = "fast", .timeline_id = 1, .restart_lsn = 9, .received_lsn = 10, .applied_lsn = 9, .safe_read_lsn = 9 });

    const snapshot = try store.retentionSnapshot(12, .{ .max_lag_lsn = 5 });
    try std.testing.expectEqual(@as(usize, 1), snapshot.reseed_recommended);
    try std.testing.expectEqual(@as(usize, 1), snapshot.active_slots);
    try std.testing.expectEqual(@as(u64, 9), snapshot.oldest_restart_lsn);
    const slow = store.get("slow") orelse return error.TestExpectedEqual;
    try std.testing.expect(slow.reseed_required);

    const after_mark = try store.retentionSnapshot(12, .{ .max_lag_lsn = 5 });
    try std.testing.expectEqual(@as(usize, 1), after_mark.reseed_recommended);
    try std.testing.expectEqual(@as(usize, 1), after_mark.active_slots);
    try std.testing.expectEqual(@as(u64, 9), after_mark.oldest_restart_lsn);
}

test "storage.ha slot store marks active slots at restart lsn for timeline" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "reseed-restart-lsn");
    defer alloc.free(path);

    var store = try SlotStore.open(alloc, path.ptr, .{});
    var store_open = true;
    defer if (store_open) store.close();
    try store.createOrUpdate(.{ .name = "old-a", .timeline_id = 1, .restart_lsn = 4, .received_lsn = 8, .applied_lsn = 4, .safe_read_lsn = 4 });
    try store.createOrUpdate(.{ .name = "old-b", .timeline_id = 1, .restart_lsn = 4, .received_lsn = 8, .applied_lsn = 4, .safe_read_lsn = 4 });
    try store.createOrUpdate(.{ .name = "new", .timeline_id = 1, .restart_lsn = 7, .received_lsn = 9, .applied_lsn = 7, .safe_read_lsn = 7 });
    try store.createOrUpdate(.{ .name = "old-timeline", .timeline_id = 2, .restart_lsn = 4, .received_lsn = 8, .applied_lsn = 4, .safe_read_lsn = 4 });
    try store.createOrUpdate(.{ .name = "paused", .timeline_id = 1, .restart_lsn = 4, .received_lsn = 8, .applied_lsn = 4, .safe_read_lsn = 4 });
    try store.pause("paused");

    const marked = try store.markActiveSlotsAtRestartLsnForTimeline(1, 4);
    try std.testing.expectEqual(@as(usize, 2), marked);
    try std.testing.expect((store.get("old-a") orelse return error.TestExpectedEqual).reseed_required);
    try std.testing.expect((store.get("old-b") orelse return error.TestExpectedEqual).reseed_required);
    try std.testing.expect(!(store.get("new") orelse return error.TestExpectedEqual).reseed_required);
    try std.testing.expect(!(store.get("old-timeline") orelse return error.TestExpectedEqual).reseed_required);
    try std.testing.expect(!(store.get("paused") orelse return error.TestExpectedEqual).reseed_required);

    store.close();
    store_open = false;

    {
        var reopened = try SlotStore.open(alloc, path.ptr, .{});
        defer reopened.close();
        try std.testing.expect((reopened.get("old-a") orelse return error.TestExpectedEqual).reseed_required);
        try std.testing.expect((reopened.get("old-b") orelse return error.TestExpectedEqual).reseed_required);
    }
}

test "storage.ha slot store drops slots and releases retention" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "drop");
    defer alloc.free(path);

    var store = try SlotStore.open(alloc, path.ptr, .{});
    defer store.close();
    try store.createOrUpdate(.{ .name = "a", .timeline_id = 1, .restart_lsn = 4, .received_lsn = 8, .applied_lsn = 4, .safe_read_lsn = 4 });
    try store.drop("a");
    try std.testing.expectEqual(@as(usize, 0), store.count());
    const snapshot = try store.retentionSnapshot(10, .{});
    try std.testing.expectEqual(@as(u64, 10), snapshot.oldest_restart_lsn);
    try std.testing.expectEqual(@as(u64, 0), snapshot.retained_lsn_count);
}

test "storage.ha slot store persists pause and resume state" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "pause-resume");
    defer alloc.free(path);

    {
        var store = try SlotStore.open(alloc, path.ptr, .{});
        defer store.close();
        try store.createOrUpdate(.{ .name = "standby-a", .timeline_id = 1, .restart_lsn = 4, .received_lsn = 8, .applied_lsn = 4, .safe_read_lsn = 4 });
        try store.pause("standby-a");
        const paused = store.get("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expect(!paused.active);
        const snapshot = try store.retentionSnapshot(10, .{});
        try std.testing.expectEqual(@as(usize, 0), snapshot.active_slots);
        try std.testing.expectEqual(@as(u64, 0), snapshot.retained_lsn_count);
    }

    {
        var reopened = try SlotStore.open(alloc, path.ptr, .{});
        defer reopened.close();
        var paused = reopened.get("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expect(!paused.active);
        try reopened.resumeSlot("standby-a");
        paused = reopened.get("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expect(paused.active);
    }

    {
        var reopened = try SlotStore.open(alloc, path.ptr, .{});
        defer reopened.close();
        const resumed = reopened.get("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expect(resumed.active);
    }
}

test "storage.ha slot store allows backup pin ahead of standby progress" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "backup-pin");
    defer alloc.free(path);

    var store = try SlotStore.open(alloc, path.ptr, .{});
    defer store.close();
    try store.createOrUpdate(.{
        .name = "standby-seeding",
        .timeline_id = 1,
        .restart_lsn = 10,
        .received_lsn = 9,
        .applied_lsn = 9,
        .safe_read_lsn = 9,
    });

    const snapshot = try store.retentionSnapshot(12, .{});
    try std.testing.expectEqual(@as(u64, 10), snapshot.oldest_restart_lsn);
    try std.testing.expectEqual(@as(u64, 2), snapshot.retained_lsn_count);
    const slot = store.get("standby-seeding") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 9), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 9), slot.applied_lsn);
    try std.testing.expectEqual(@as(u64, 9), slot.safe_read_lsn);
}
