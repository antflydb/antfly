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

//! Durable HA promotion fencing.
//!
//! Hot-standby promotion must be backed by an explicit ownership decision. This
//! module models the storage-facing side of that decision: once an external
//! authority has fenced the former primary, Antfly persists a promotion receipt
//! with the new timeline/epoch and can derive a standby `PromotionRequest` from
//! that receipt. Later Kubernetes Lease or cloud-control-plane integrations can
//! wrap this same durable receipt format.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Crc32 = std.hash.Crc32;
const replication_record = @import("replication_record.zig");
const standby_mod = @import("standby.zig");
const validation = @import("validation.zig");
const wal_mod = @import("../wal.zig");

const magic = [8]u8{ 'A', 'F', 'H', 'A', 'F', 'N', 'C', '\n' };
const version: u16 = 1;
const header_len: usize = 128;

const version_offset: usize = 8;
const event_type_offset: usize = 10;
const flags_offset: usize = 12;
const cluster_id_offset: usize = 16;
const shard_id_offset: usize = 24;
const table_id_offset: usize = 32;
const parent_timeline_id_offset: usize = 40;
const parent_epoch_offset: usize = 48;
const new_timeline_id_offset: usize = 56;
const new_epoch_offset: usize = 64;
const required_lsn_offset: usize = 72;
const observed_lsn_offset: usize = 80;
const generation_offset: usize = 88;
const old_primary_len_offset: usize = 96;
const promoted_len_offset: usize = 100;
const token_len_offset: usize = 104;
const reason_len_offset: usize = 108;
const body_len_offset: usize = 112;
const body_crc_offset: usize = 120;
const header_crc_offset: usize = 124;

var test_path_counter: u64 = 0;

comptime {
    std.debug.assert(header_crc_offset + 4 == header_len);
}

pub const EventType = enum(u16) {
    promotion_fence = 1,
    _,
};

pub const FenceRequest = struct {
    identity: standby_mod.Identity,
    old_primary_id: []const u8,
    promoted_node_id: []const u8,
    new_timeline_id: u64,
    new_epoch: u64,
    required_lsn: u64,
    observed_lsn: u64,
    force: bool = false,
    reason: []const u8 = &.{},
};

pub const Receipt = struct {
    identity: standby_mod.Identity,
    old_primary_id: []const u8,
    promoted_node_id: []const u8,
    parent_timeline_id: u64,
    parent_epoch: u64,
    new_timeline_id: u64,
    new_epoch: u64,
    required_lsn: u64,
    observed_lsn: u64,
    generation: u64,
    forced: bool,
    token: []const u8,
    reason: []const u8,

    pub fn promotionRequest(self: Receipt) standby_mod.PromotionRequest {
        return .{
            .new_timeline_id = self.new_timeline_id,
            .new_epoch = self.new_epoch,
            .required_lsn = self.required_lsn,
            .fencing_confirmed = true,
            .force = self.forced,
        };
    }
};

pub const ReceiptBinding = struct {
    old_primary_id: []const u8,
    parent_identity: standby_mod.Identity,
};

const OwnedReceipt = struct {
    receipt: Receipt,

    fn deinit(self: *OwnedReceipt, alloc: Allocator) void {
        alloc.free(self.receipt.old_primary_id);
        alloc.free(self.receipt.promoted_node_id);
        alloc.free(self.receipt.token);
        alloc.free(self.receipt.reason);
        self.* = undefined;
    }

    fn clone(self: *const OwnedReceipt, alloc: Allocator) !OwnedReceipt {
        return try ownedReceiptFromReceipt(alloc, self.receipt);
    }
};

pub const OpenOptions = struct {
    wal_options: wal_mod.WalOptions = .{},
};

pub const Store = struct {
    alloc: Allocator,
    wal: wal_mod.WAL,
    current_receipt: ?OwnedReceipt = null,

    pub fn open(alloc: Allocator, path: [*:0]const u8, options: OpenOptions) !Store {
        var store = Store{
            .alloc = alloc,
            .wal = try wal_mod.WAL.open(path, options.wal_options),
        };
        errdefer store.close();
        try store.replay();
        return store;
    }

    pub fn close(self: *Store) void {
        if (self.current_receipt) |*receipt| receipt.deinit(self.alloc);
        self.wal.close();
        self.* = undefined;
    }

    pub fn current(self: *const Store, alloc: Allocator) !?Receipt {
        const current_receipt = self.current_receipt orelse return null;
        const cloned = try current_receipt.clone(alloc);
        return cloned.receipt;
    }

    pub fn currentBorrowed(self: *const Store) ?Receipt {
        const current_receipt = self.current_receipt orelse return null;
        return current_receipt.receipt;
    }

    pub fn recordVerifiedReceipt(self: *Store, receipt: Receipt, binding: ReceiptBinding) !void {
        try validateReceiptBinding(receipt, binding);
        try self.recordReceipt(receipt);
    }

    fn recordReceipt(self: *Store, receipt: Receipt) !void {
        try validateReceipt(receipt);
        if (self.current_receipt) |held| {
            if (sameReceipt(held.receipt, receipt)) return;
        }

        const encoded = try encodeReceipt(self.alloc, .promotion_fence, receipt);
        defer self.alloc.free(encoded);
        _ = try self.wal.append(encoded);
        try self.applyReceipt(receipt);
    }

    pub fn acquirePromotionFence(self: *Store, request: FenceRequest) !Receipt {
        try validateFenceRequest(request);
        if (request.observed_lsn < request.required_lsn and !request.force) return error.FenceRequiresForce;

        if (self.current_receipt) |*held| {
            if (sameFence(held.receipt, request)) {
                const cloned = try held.clone(self.alloc);
                return cloned.receipt;
            }
            if (!chainsFromHeldFence(held.receipt, request)) return error.FenceAlreadyHeld;
            if (held.receipt.new_epoch >= request.new_epoch or
                held.receipt.new_timeline_id >= request.new_timeline_id)
            {
                return error.FenceAlreadyHeld;
            }
        }

        const generation = if (self.current_receipt) |held| held.receipt.generation + 1 else 1;
        const token = try tokenFor(self.alloc, request, generation);
        defer self.alloc.free(token);
        const receipt = Receipt{
            .identity = .{
                .cluster_id = request.identity.cluster_id,
                .shard_id = request.identity.shard_id,
                .table_id = request.identity.table_id,
                .timeline_id = request.new_timeline_id,
                .epoch = request.new_epoch,
            },
            .old_primary_id = request.old_primary_id,
            .promoted_node_id = request.promoted_node_id,
            .parent_timeline_id = request.identity.timeline_id,
            .parent_epoch = request.identity.epoch,
            .new_timeline_id = request.new_timeline_id,
            .new_epoch = request.new_epoch,
            .required_lsn = request.required_lsn,
            .observed_lsn = request.observed_lsn,
            .generation = generation,
            .forced = request.force,
            .token = token,
            .reason = request.reason,
        };

        const encoded = try encodeReceipt(self.alloc, .promotion_fence, receipt);
        defer self.alloc.free(encoded);
        _ = try self.wal.append(encoded);
        try self.applyReceipt(receipt);

        const held = self.current_receipt orelse return error.FenceReceiptMissing;
        const cloned = try held.clone(self.alloc);
        return cloned.receipt;
    }

    fn replay(self: *Store) !void {
        const entries = try self.wal.iterateFrom(self.alloc, 1);
        defer {
            for (entries) |entry| self.alloc.free(entry.data);
            self.alloc.free(entries);
        }

        for (entries) |entry| {
            var decoded = try decodeReceipt(self.alloc, entry.data);
            defer decoded.deinit(self.alloc);
            try self.applyReceipt(decoded.receipt);
        }
    }

    fn applyReceipt(self: *Store, receipt: Receipt) !void {
        try validateReceipt(receipt);
        var owned = try ownedReceiptFromReceipt(self.alloc, receipt);
        errdefer owned.deinit(self.alloc);
        if (self.current_receipt) |*held| {
            if (owned.receipt.generation < held.receipt.generation) return error.NonMonotonicFenceGeneration;
            if (owned.receipt.generation == held.receipt.generation) {
                if (!sameReceipt(held.receipt, owned.receipt)) return error.NonMonotonicFenceGeneration;
                owned.deinit(self.alloc);
                return;
            }
            if (!receiptChainsFromHeld(held.receipt, owned.receipt)) return error.FenceAlreadyHeld;
            held.deinit(self.alloc);
        }
        self.current_receipt = owned;
    }
};

pub fn freeReceipt(alloc: Allocator, receipt: Receipt) void {
    alloc.free(receipt.old_primary_id);
    alloc.free(receipt.promoted_node_id);
    alloc.free(receipt.token);
    alloc.free(receipt.reason);
}

fn validateFenceRequest(request: FenceRequest) !void {
    if (request.identity.timeline_id == 0 or request.identity.epoch == 0) return error.InvalidTimelineSwitch;
    if (!validation.isIdentifier(request.old_primary_id)) return error.InvalidOldPrimaryId;
    if (!validation.isIdentifier(request.promoted_node_id)) return error.InvalidPromotedNodeId;
    if (std.mem.eql(u8, request.old_primary_id, request.promoted_node_id)) return error.InvalidPromotedNodeId;
    if (request.new_timeline_id <= request.identity.timeline_id) return error.InvalidTimelineSwitch;
    if (request.new_epoch <= request.identity.epoch) return error.InvalidTimelineSwitch;
    if (request.required_lsn == 0) return error.InvalidFenceLsn;
    if (request.old_primary_id.len > std.math.maxInt(u32) or
        request.promoted_node_id.len > std.math.maxInt(u32) or
        request.reason.len > std.math.maxInt(u32))
    {
        return error.FenceFieldTooLong;
    }
}

pub fn validateReceipt(receipt: Receipt) !void {
    if (!validation.isIdentifier(receipt.old_primary_id)) return error.InvalidOldPrimaryId;
    if (!validation.isIdentifier(receipt.promoted_node_id)) return error.InvalidPromotedNodeId;
    if (receipt.token.len == 0) return error.InvalidFenceToken;
    if (std.mem.eql(u8, receipt.old_primary_id, receipt.promoted_node_id)) return error.InvalidPromotedNodeId;
    if (receipt.identity.timeline_id != receipt.new_timeline_id) return error.InvalidTimelineSwitch;
    if (receipt.identity.epoch != receipt.new_epoch) return error.InvalidTimelineSwitch;
    if (receipt.parent_timeline_id == 0 or receipt.parent_epoch == 0) return error.InvalidTimelineSwitch;
    if (receipt.new_timeline_id <= receipt.parent_timeline_id) return error.InvalidTimelineSwitch;
    if (receipt.new_epoch <= receipt.parent_epoch) return error.InvalidTimelineSwitch;
    if (receipt.required_lsn == 0) return error.InvalidFenceLsn;
    if (receipt.generation == 0) return error.NonMonotonicFenceGeneration;
    if (receipt.observed_lsn < receipt.required_lsn and !receipt.forced) return error.FenceRequiresForce;
    if (receipt.old_primary_id.len > std.math.maxInt(u32) or
        receipt.promoted_node_id.len > std.math.maxInt(u32) or
        receipt.token.len > std.math.maxInt(u32) or
        receipt.reason.len > std.math.maxInt(u32))
    {
        return error.FenceFieldTooLong;
    }
}

pub fn validateReceiptBinding(receipt: Receipt, binding: ReceiptBinding) !void {
    try validateReceipt(receipt);
    if (!std.mem.eql(u8, receipt.old_primary_id, binding.old_primary_id)) return error.RejoinReceiptBindingMismatch;
    if (receipt.identity.cluster_id != binding.parent_identity.cluster_id) return error.WrongCluster;
    if (receipt.identity.shard_id != binding.parent_identity.shard_id) return error.WrongShard;
    if (receipt.identity.table_id != binding.parent_identity.table_id) return error.WrongTable;
    if (receipt.parent_timeline_id != binding.parent_identity.timeline_id) return error.WrongTimeline;
    if (receipt.parent_epoch != binding.parent_identity.epoch) return error.WrongEpoch;
}

fn sameFence(receipt: Receipt, request: FenceRequest) bool {
    return receipt.parent_timeline_id == request.identity.timeline_id and
        receipt.parent_epoch == request.identity.epoch and
        receipt.new_timeline_id == request.new_timeline_id and
        receipt.new_epoch == request.new_epoch and
        receipt.required_lsn == request.required_lsn and
        receipt.observed_lsn == request.observed_lsn and
        receipt.forced == request.force and
        std.mem.eql(u8, receipt.old_primary_id, request.old_primary_id) and
        std.mem.eql(u8, receipt.promoted_node_id, request.promoted_node_id);
}

fn chainsFromHeldFence(receipt: Receipt, request: FenceRequest) bool {
    return receipt.identity.cluster_id == request.identity.cluster_id and
        receipt.identity.shard_id == request.identity.shard_id and
        receipt.identity.table_id == request.identity.table_id and
        receipt.new_timeline_id == request.identity.timeline_id and
        receipt.new_epoch == request.identity.epoch and
        std.mem.eql(u8, receipt.promoted_node_id, request.old_primary_id);
}

fn receiptChainsFromHeld(held: Receipt, next: Receipt) bool {
    return held.identity.cluster_id == next.identity.cluster_id and
        held.identity.shard_id == next.identity.shard_id and
        held.identity.table_id == next.identity.table_id and
        held.new_timeline_id == next.parent_timeline_id and
        held.new_epoch == next.parent_epoch and
        std.mem.eql(u8, held.promoted_node_id, next.old_primary_id);
}

fn sameReceipt(a: Receipt, b: Receipt) bool {
    return a.identity.cluster_id == b.identity.cluster_id and
        a.identity.shard_id == b.identity.shard_id and
        a.identity.table_id == b.identity.table_id and
        a.identity.timeline_id == b.identity.timeline_id and
        a.identity.epoch == b.identity.epoch and
        a.parent_timeline_id == b.parent_timeline_id and
        a.parent_epoch == b.parent_epoch and
        a.new_timeline_id == b.new_timeline_id and
        a.new_epoch == b.new_epoch and
        a.required_lsn == b.required_lsn and
        a.observed_lsn == b.observed_lsn and
        a.generation == b.generation and
        a.forced == b.forced and
        std.mem.eql(u8, a.old_primary_id, b.old_primary_id) and
        std.mem.eql(u8, a.promoted_node_id, b.promoted_node_id) and
        std.mem.eql(u8, a.token, b.token) and
        std.mem.eql(u8, a.reason, b.reason);
}

fn ownedReceiptFromReceipt(alloc: Allocator, receipt: Receipt) !OwnedReceipt {
    var owned = OwnedReceipt{
        .receipt = .{
            .identity = receipt.identity,
            .old_primary_id = try alloc.dupe(u8, receipt.old_primary_id),
            .promoted_node_id = &.{},
            .parent_timeline_id = receipt.parent_timeline_id,
            .parent_epoch = receipt.parent_epoch,
            .new_timeline_id = receipt.new_timeline_id,
            .new_epoch = receipt.new_epoch,
            .required_lsn = receipt.required_lsn,
            .observed_lsn = receipt.observed_lsn,
            .generation = receipt.generation,
            .forced = receipt.forced,
            .token = &.{},
            .reason = &.{},
        },
    };
    errdefer owned.deinit(alloc);
    owned.receipt.promoted_node_id = try alloc.dupe(u8, receipt.promoted_node_id);
    owned.receipt.token = try alloc.dupe(u8, receipt.token);
    owned.receipt.reason = try alloc.dupe(u8, receipt.reason);
    return owned;
}

fn tokenFor(alloc: Allocator, request: FenceRequest, generation: u64) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        "ha-fence:{d}:{d}:{d}:{d}:{d}:{d}:{s}",
        .{
            request.identity.cluster_id,
            request.identity.shard_id,
            request.identity.table_id,
            request.new_timeline_id,
            request.new_epoch,
            generation,
            request.promoted_node_id,
        },
    );
}

fn encodeReceipt(alloc: Allocator, event_type: EventType, receipt: Receipt) ![]u8 {
    const body_len = try std.math.add(
        usize,
        try std.math.add(usize, receipt.old_primary_id.len, receipt.promoted_node_id.len),
        try std.math.add(usize, receipt.token.len, receipt.reason.len),
    );
    if (body_len > std.math.maxInt(u64)) return error.FenceBodyTooLarge;
    const total_len = try std.math.add(usize, header_len, body_len);
    const out = try alloc.alloc(u8, total_len);
    errdefer alloc.free(out);

    @memset(out[0..header_len], 0);
    @memcpy(out[0..8], &magic);
    std.mem.writeInt(u16, out[version_offset..][0..2], version, .little);
    std.mem.writeInt(u16, out[event_type_offset..][0..2], @intFromEnum(event_type), .little);
    var flags: u32 = 0;
    if (receipt.forced) flags |= 1 << 0;
    std.mem.writeInt(u32, out[flags_offset..][0..4], flags, .little);
    std.mem.writeInt(u64, out[cluster_id_offset..][0..8], receipt.identity.cluster_id, .little);
    std.mem.writeInt(u64, out[shard_id_offset..][0..8], receipt.identity.shard_id, .little);
    std.mem.writeInt(u64, out[table_id_offset..][0..8], receipt.identity.table_id, .little);
    std.mem.writeInt(u64, out[parent_timeline_id_offset..][0..8], receipt.parent_timeline_id, .little);
    std.mem.writeInt(u64, out[parent_epoch_offset..][0..8], receipt.parent_epoch, .little);
    std.mem.writeInt(u64, out[new_timeline_id_offset..][0..8], receipt.new_timeline_id, .little);
    std.mem.writeInt(u64, out[new_epoch_offset..][0..8], receipt.new_epoch, .little);
    std.mem.writeInt(u64, out[required_lsn_offset..][0..8], receipt.required_lsn, .little);
    std.mem.writeInt(u64, out[observed_lsn_offset..][0..8], receipt.observed_lsn, .little);
    std.mem.writeInt(u64, out[generation_offset..][0..8], receipt.generation, .little);
    std.mem.writeInt(u32, out[old_primary_len_offset..][0..4], @intCast(receipt.old_primary_id.len), .little);
    std.mem.writeInt(u32, out[promoted_len_offset..][0..4], @intCast(receipt.promoted_node_id.len), .little);
    std.mem.writeInt(u32, out[token_len_offset..][0..4], @intCast(receipt.token.len), .little);
    std.mem.writeInt(u32, out[reason_len_offset..][0..4], @intCast(receipt.reason.len), .little);
    std.mem.writeInt(u64, out[body_len_offset..][0..8], @intCast(body_len), .little);

    var cursor: usize = header_len;
    @memcpy(out[cursor..][0..receipt.old_primary_id.len], receipt.old_primary_id);
    cursor += receipt.old_primary_id.len;
    @memcpy(out[cursor..][0..receipt.promoted_node_id.len], receipt.promoted_node_id);
    cursor += receipt.promoted_node_id.len;
    @memcpy(out[cursor..][0..receipt.token.len], receipt.token);
    cursor += receipt.token.len;
    @memcpy(out[cursor..][0..receipt.reason.len], receipt.reason);
    cursor += receipt.reason.len;
    std.debug.assert(cursor == out.len);

    std.mem.writeInt(u32, out[body_crc_offset..][0..4], Crc32.hash(out[header_len..]), .little);
    std.mem.writeInt(u32, out[header_crc_offset..][0..4], Crc32.hash(out[0..header_crc_offset]), .little);
    return out;
}

fn decodeReceipt(alloc: Allocator, bytes: []const u8) !OwnedReceipt {
    if (bytes.len < header_len) return error.EndOfStream;
    if (!std.mem.eql(u8, bytes[0..8], &magic)) return error.InvalidMagic;
    const decoded_version = std.mem.readInt(u16, bytes[version_offset..][0..2], .little);
    if (decoded_version == 0 or decoded_version > version) return error.UnsupportedVersion;
    const event_type: EventType = @enumFromInt(std.mem.readInt(u16, bytes[event_type_offset..][0..2], .little));
    if (event_type != .promotion_fence) return error.UnsupportedFenceEvent;
    const stored_header_crc = std.mem.readInt(u32, bytes[header_crc_offset..][0..4], .little);
    if (stored_header_crc != Crc32.hash(bytes[0..header_crc_offset])) return error.HeaderCrcMismatch;

    const old_len: usize = @intCast(std.mem.readInt(u32, bytes[old_primary_len_offset..][0..4], .little));
    const promoted_len: usize = @intCast(std.mem.readInt(u32, bytes[promoted_len_offset..][0..4], .little));
    const token_len: usize = @intCast(std.mem.readInt(u32, bytes[token_len_offset..][0..4], .little));
    const reason_len: usize = @intCast(std.mem.readInt(u32, bytes[reason_len_offset..][0..4], .little));
    const body_len_u64 = std.mem.readInt(u64, bytes[body_len_offset..][0..8], .little);
    if (body_len_u64 > std.math.maxInt(usize)) return error.FenceBodyTooLarge;
    const body_len: usize = @intCast(body_len_u64);
    const expected_body_len = try std.math.add(
        usize,
        try std.math.add(usize, old_len, promoted_len),
        try std.math.add(usize, token_len, reason_len),
    );
    if (body_len != expected_body_len) return error.InvalidFenceBodyLength;
    const total_len = try std.math.add(usize, header_len, body_len);
    if (bytes.len < total_len) return error.EndOfStream;
    if (bytes.len != total_len) return error.TrailingBytes;
    const stored_body_crc = std.mem.readInt(u32, bytes[body_crc_offset..][0..4], .little);
    if (stored_body_crc != Crc32.hash(bytes[header_len..])) return error.BodyCrcMismatch;

    var cursor: usize = header_len;
    const old_primary_id = bytes[cursor..][0..old_len];
    cursor += old_len;
    const promoted_node_id = bytes[cursor..][0..promoted_len];
    cursor += promoted_len;
    const token = bytes[cursor..][0..token_len];
    cursor += token_len;
    const reason = bytes[cursor..][0..reason_len];

    return try ownedReceiptFromReceipt(alloc, .{
        .identity = .{
            .cluster_id = std.mem.readInt(u64, bytes[cluster_id_offset..][0..8], .little),
            .shard_id = std.mem.readInt(u64, bytes[shard_id_offset..][0..8], .little),
            .table_id = std.mem.readInt(u64, bytes[table_id_offset..][0..8], .little),
            .timeline_id = std.mem.readInt(u64, bytes[new_timeline_id_offset..][0..8], .little),
            .epoch = std.mem.readInt(u64, bytes[new_epoch_offset..][0..8], .little),
        },
        .old_primary_id = old_primary_id,
        .promoted_node_id = promoted_node_id,
        .parent_timeline_id = std.mem.readInt(u64, bytes[parent_timeline_id_offset..][0..8], .little),
        .parent_epoch = std.mem.readInt(u64, bytes[parent_epoch_offset..][0..8], .little),
        .new_timeline_id = std.mem.readInt(u64, bytes[new_timeline_id_offset..][0..8], .little),
        .new_epoch = std.mem.readInt(u64, bytes[new_epoch_offset..][0..8], .little),
        .required_lsn = std.mem.readInt(u64, bytes[required_lsn_offset..][0..8], .little),
        .observed_lsn = std.mem.readInt(u64, bytes[observed_lsn_offset..][0..8], .little),
        .generation = std.mem.readInt(u64, bytes[generation_offset..][0..8], .little),
        .forced = (std.mem.readInt(u32, bytes[flags_offset..][0..4], .little) & (1 << 0)) != 0,
        .token = token,
        .reason = reason,
    });
}

fn testPath(alloc: Allocator, comptime name: []const u8) ![:0]u8 {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-fencing-" ++ name ++ "-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(raw);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), raw) catch {};
    return try alloc.dupeZ(u8, raw);
}

const StandbyPaths = struct {
    receive_log: [:0]u8,
    progress_wal: [:0]u8,
    fence_wal: [:0]u8,

    fn deinit(self: StandbyPaths, alloc: Allocator) void {
        alloc.free(self.receive_log);
        alloc.free(self.progress_wal);
        alloc.free(self.fence_wal);
    }
};

fn standbyPaths(alloc: Allocator, comptime name: []const u8) !StandbyPaths {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const receive_raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-fencing-" ++ name ++ "-receive-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(receive_raw);
    const progress_raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-fencing-" ++ name ++ "-progress-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(progress_raw);
    const fence_raw = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-fencing-" ++ name ++ "-fence-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
    defer alloc.free(fence_raw);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), receive_raw) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), progress_raw) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), fence_raw) catch {};

    return .{
        .receive_log = try alloc.dupeZ(u8, receive_raw),
        .progress_wal = try alloc.dupeZ(u8, progress_raw),
        .fence_wal = try alloc.dupeZ(u8, fence_raw),
    };
}

fn testIdentity() standby_mod.Identity {
    return .{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    };
}

fn baseRecord(identity: standby_mod.Identity, lsn: u64, payload: []const u8) replication_record.Record {
    return .{
        .kind = .batch_mutation,
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = identity.timeline_id,
        .epoch = identity.epoch,
        .lsn = lsn,
        .previous_lsn = lsn - 1,
        .payload = payload,
    };
}

const ApplyCapture = struct {
    fn apply(_: *anyopaque, _: replication_record.RecordView) !void {}
};

fn baseRequest() FenceRequest {
    return .{
        .identity = testIdentity(),
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-b",
        .new_timeline_id = 2,
        .new_epoch = 2,
        .required_lsn = 10,
        .observed_lsn = 10,
        .reason = "manual failover",
    };
}

test "storage.ha fencing persists promotion receipt and builds promotion request" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "persist");
    defer alloc.free(path);

    {
        var store = try Store.open(alloc, path.ptr, .{});
        defer store.close();
        const receipt = try store.acquirePromotionFence(baseRequest());
        defer freeReceipt(alloc, receipt);
        try std.testing.expectEqual(@as(u64, 1), receipt.generation);
        try std.testing.expectEqual(@as(u64, 2), receipt.new_timeline_id);
        try std.testing.expectEqual(@as(u64, 2), receipt.new_epoch);
        try std.testing.expectEqual(@as(u64, 10), receipt.required_lsn);
        try std.testing.expectEqualStrings("primary-a", receipt.old_primary_id);
        try std.testing.expectEqualStrings("standby-b", receipt.promoted_node_id);
        try std.testing.expect(std.mem.startsWith(u8, receipt.token, "ha-fence:100:10:20:2:2:1:"));

        const promotion = receipt.promotionRequest();
        try std.testing.expectEqual(@as(u64, 2), promotion.new_timeline_id);
        try std.testing.expectEqual(@as(u64, 2), promotion.new_epoch);
        try std.testing.expectEqual(@as(u64, 10), promotion.required_lsn.?);
        try std.testing.expect(promotion.fencing_confirmed);
        try std.testing.expect(!promotion.force);
    }

    {
        var reopened = try Store.open(alloc, path.ptr, .{});
        defer reopened.close();
        const receipt = (try reopened.current(alloc)).?;
        defer freeReceipt(alloc, receipt);
        try std.testing.expectEqual(@as(u64, 1), receipt.generation);
        try std.testing.expectEqualStrings("manual failover", receipt.reason);
        try std.testing.expectEqualStrings("standby-b", receipt.promoted_node_id);
    }
}

test "storage.ha fencing rejects unsafe and competing promotions" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "reject");
    defer alloc.free(path);

    var store = try Store.open(alloc, path.ptr, .{});
    defer store.close();

    var behind = baseRequest();
    behind.observed_lsn = 9;
    try std.testing.expectError(error.FenceRequiresForce, store.acquirePromotionFence(behind));

    behind.force = true;
    const forced = try store.acquirePromotionFence(behind);
    defer freeReceipt(alloc, forced);
    try std.testing.expect(forced.forced);
    try std.testing.expectEqual(@as(u64, 1), forced.generation);

    const repeated = try store.acquirePromotionFence(behind);
    defer freeReceipt(alloc, repeated);
    try std.testing.expectEqualStrings(forced.token, repeated.token);
    try std.testing.expectEqual(@as(u64, 1), repeated.generation);

    var competing = baseRequest();
    competing.promoted_node_id = "standby-c";
    competing.new_timeline_id = 2;
    competing.new_epoch = 2;
    try std.testing.expectError(error.FenceAlreadyHeld, store.acquirePromotionFence(competing));

    competing.new_timeline_id = 3;
    competing.new_epoch = 3;
    competing.observed_lsn = 10;
    try std.testing.expectError(error.FenceAlreadyHeld, store.acquirePromotionFence(competing));

    competing.identity.timeline_id = 2;
    competing.identity.epoch = 2;
    competing.old_primary_id = "standby-b";
    const next = try store.acquirePromotionFence(competing);
    defer freeReceipt(alloc, next);
    try std.testing.expectEqual(@as(u64, 2), next.generation);
    try std.testing.expectEqual(@as(u64, 2), next.parent_timeline_id);
    try std.testing.expectEqualStrings("standby-b", next.old_primary_id);
    try std.testing.expectEqualStrings("standby-c", next.promoted_node_id);
}

test "storage.ha fencing rejects invalid node identifiers" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "invalid-node-identifiers");
    defer alloc.free(path);

    var store = try Store.open(alloc, path.ptr, .{});
    defer store.close();

    var invalid_old = baseRequest();
    invalid_old.old_primary_id = "primary a";
    try std.testing.expectError(error.InvalidOldPrimaryId, store.acquirePromotionFence(invalid_old));

    var invalid_promoted = baseRequest();
    invalid_promoted.promoted_node_id = "standby/a";
    try std.testing.expectError(error.InvalidPromotedNodeId, store.acquirePromotionFence(invalid_promoted));

    const valid_receipt = try store.acquirePromotionFence(baseRequest());
    defer freeReceipt(alloc, valid_receipt);
    const invalid_receipt = Receipt{
        .identity = valid_receipt.identity,
        .old_primary_id = valid_receipt.old_primary_id,
        .promoted_node_id = "standby bad",
        .parent_timeline_id = valid_receipt.parent_timeline_id,
        .parent_epoch = valid_receipt.parent_epoch,
        .new_timeline_id = valid_receipt.new_timeline_id,
        .new_epoch = valid_receipt.new_epoch,
        .required_lsn = valid_receipt.required_lsn,
        .observed_lsn = valid_receipt.observed_lsn,
        .generation = valid_receipt.generation,
        .forced = valid_receipt.forced,
        .token = valid_receipt.token,
        .reason = valid_receipt.reason,
    };
    try std.testing.expectError(error.InvalidPromotedNodeId, validateReceipt(invalid_receipt));
}

test "storage.ha fencing rejects conflicting duplicate generations on replay" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "duplicate-generation");
    defer alloc.free(path);

    {
        var store = try Store.open(alloc, path.ptr, .{});
        defer store.close();

        const receipt = try store.acquirePromotionFence(baseRequest());
        defer freeReceipt(alloc, receipt);

        var conflicting = receipt;
        conflicting.promoted_node_id = "standby-c";
        const encoded = try encodeReceipt(alloc, .promotion_fence, conflicting);
        defer alloc.free(encoded);
        _ = try store.wal.append(encoded);
    }

    try std.testing.expectError(error.NonMonotonicFenceGeneration, Store.open(alloc, path.ptr, .{}));
}

test "storage.ha fencing rejects stale parent generation on replay" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "stale-parent-generation");
    defer alloc.free(path);

    {
        var store = try Store.open(alloc, path.ptr, .{});
        defer store.close();

        const receipt = try store.acquirePromotionFence(baseRequest());
        defer freeReceipt(alloc, receipt);

        var stale_parent = receipt;
        stale_parent.promoted_node_id = "standby-c";
        stale_parent.new_timeline_id = 3;
        stale_parent.new_epoch = 3;
        stale_parent.identity.timeline_id = 3;
        stale_parent.identity.epoch = 3;
        stale_parent.generation = 2;
        stale_parent.token = "stale-parent-token";
        const encoded = try encodeReceipt(alloc, .promotion_fence, stale_parent);
        defer alloc.free(encoded);
        _ = try store.wal.append(encoded);
    }

    try std.testing.expectError(error.FenceAlreadyHeld, Store.open(alloc, path.ptr, .{}));
}

test "storage.ha fencing rejects invalid durable receipt fields on replay" {
    const alloc = std.testing.allocator;
    const path = try testPath(alloc, "invalid-receipt-fields");
    defer alloc.free(path);

    {
        var store = try Store.open(alloc, path.ptr, .{});
        defer store.close();

        const receipt = try store.acquirePromotionFence(baseRequest());
        defer freeReceipt(alloc, receipt);

        var empty_token = receipt;
        empty_token.token = "";
        const encoded = try encodeReceipt(alloc, .promotion_fence, empty_token);
        defer alloc.free(encoded);
        _ = try store.wal.append(encoded);
    }

    try std.testing.expectError(error.InvalidFenceToken, Store.open(alloc, path.ptr, .{}));
}

test "storage.ha fencing receipt drives standby promotion" {
    const alloc = std.testing.allocator;
    const paths = try standbyPaths(alloc, "promote");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var standby = try standby_mod.Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
    errdefer standby.close();
    _ = try standby.receive(baseRecord(identity, 1, "one"));
    _ = try standby.receive(baseRecord(identity, 2, "two"));
    var capture = ApplyCapture{};
    try std.testing.expectEqual(@as(usize, 2), try standby.applyAvailable(&capture, ApplyCapture.apply));

    var store = try Store.open(alloc, paths.fence_wal.ptr, .{});
    defer store.close();
    var request = baseRequest();
    request.required_lsn = 2;
    request.observed_lsn = 2;
    const receipt = try store.acquirePromotionFence(request);
    defer freeReceipt(alloc, receipt);

    const result = try standby.promote(receipt.promotionRequest());
    try std.testing.expectEqual(@as(u64, 3), result.switch_lsn);
    try std.testing.expectEqual(@as(u64, 2), result.new_identity.timeline_id);
    try std.testing.expect(!result.forced);
    try std.testing.expect(!result.data_loss_possible);

    standby.close();
    {
        var recovered = try standby_mod.Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, identity, .{});
        defer recovered.close();
        try std.testing.expectEqual(@as(u64, 2), recovered.identity.timeline_id);
        try std.testing.expectEqual(@as(u64, 2), recovered.identity.epoch);
        try std.testing.expectEqual(@as(u64, 3), recovered.currentProgress().received_lsn);
        try std.testing.expectEqual(@as(u64, 3), recovered.currentProgress().applied_lsn);
        try std.testing.expectEqual(@as(u64, 3), recovered.currentProgress().safe_read_lsn);
    }

    var reopened = try standby_mod.Standby.open(alloc, paths.receive_log.ptr, paths.progress_wal.ptr, result.new_identity, .{});
    defer reopened.close();
    try std.testing.expectEqual(@as(u64, 3), reopened.currentProgress().received_lsn);
    try std.testing.expectEqual(@as(u64, 3), reopened.currentProgress().applied_lsn);
    try std.testing.expectEqual(@as(u64, 3), reopened.currentProgress().safe_read_lsn);
}
