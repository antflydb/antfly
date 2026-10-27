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

//! Durable publication decisions in the index's metadata transaction. A cancel
//! tombstone precedes claim release and permanently excludes a late publisher.
//! Source retirement additionally requires the original readers to drain.
const std = @import("std");
pub const Digest = [32]u8;
pub const Binding = struct {
    owner: Digest,
    publication: Digest,
    pub fn valid(self: Binding) bool {
        return !std.mem.allEqual(u8, &self.owner, 0) and !std.mem.allEqual(u8, &self.publication, 0);
    }
};
pub const State = enum(u8) { published = 1, cancelled = 2, retired = 3 };
pub const Role = enum(u8) { segment = 1, family = 2, source_leaf = 3, fields = 4 };
const prefix = "index_source_publication_v1:";
pub const Key = [prefix.len + 64]u8;
pub const maximum_segments = 64;
fn same(a: Binding, b: Binding) bool {
    return std.mem.eql(u8, &a.owner, &b.owner) and std.mem.eql(u8, &a.publication, &b.publication);
}
pub fn key(binding: Binding) Key {
    var out: Key = undefined;
    @memcpy(out[0..prefix.len], prefix);
    @memcpy(out[prefix.len..][0..32], &binding.owner);
    @memcpy(out[prefix.len + 32 ..], &binding.publication);
    return out;
}
pub fn ownerPrefix(owner: Digest) [prefix.len + 32]u8 {
    var result: [prefix.len + 32]u8 = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..], &owner);
    return result;
}
pub fn bindingFromKey(raw: []const u8) !Binding {
    if (raw.len != @sizeOf(Key) or !std.mem.startsWith(u8, raw, prefix)) return error.InvalidIndexPublication;
    const result: Binding = .{ .owner = raw[prefix.len..][0..32].*, .publication = raw[prefix.len + 32 ..][0..32].* };
    try validateBinding(result);
    return result;
}
fn validateBinding(binding: Binding) !void {
    if (!binding.valid()) return error.InvalidIndexPublication;
}
fn digest(binding: Binding, bytes: []const u8) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(&key(binding));
    hash.update(bytes);
    return hash.finalResult();
}
fn seal() void {}

/// Native-only evidence read from the index metadata. The index owner forces
/// durability before exposing it to the primary-store ownership transaction.
/// It is not a public request or serializable publication authorization.
pub const Decision = struct {
    binding: Binding,
    state: State,
    root: Digest,
    epoch: u64 = 0,
    role: Role = .segment,
    seal_fn: *const fn () void,
    pub fn requireRole(self: Decision, role: Role) !void {
        if (self.seal_fn != seal or self.role != role) return error.IndexPublicationFenceMismatch;
    }
    pub fn requireSourceRole(self: Decision) !void {
        if (self.seal_fn != seal or (self.role != .segment and self.role != .source_leaf)) return error.IndexPublicationFenceMismatch;
    }
    pub fn require(self: Decision, expected: Binding, state: State) !void {
        if (self.seal_fn != seal or !same(self.binding, expected) or self.state != state) return error.IndexPublicationFenceMismatch;
    }
};
/// Derive terminal authority from the persisted owner admission floor when
/// no individual receipt remains (or publication never happened). The owner
/// forces index durability before returning this native decision, as for
/// ordinary receipt decisions. No caller supplies an asserted epoch proof.
pub fn cancelledByEpoch(reader: anytype, binding: Binding) !?Decision {
    if (try load(reader, binding) != null) return null;
    const epochs = @import("index_source_epoch.zig");
    const raw = reader.get(epochs.owner_key) catch |err| if (err == error.NotFound) return null else return err;
    const owner = try epochs.Owner.decode(raw);
    if (!std.mem.eql(u8, &owner.identity, &binding.owner)) return error.IndexSourceOwnerMismatch;
    if (owner.current == 0) return null;
    const epoch = epochs.tokenEpoch(binding) orelse 0;
    if (epoch >= owner.floor) return null;
    return .{ .binding = binding, .state = .cancelled, .root = @splat(0), .epoch = epoch, .seal_fn = seal };
}
pub const Record = struct {
    state: State,
    root: Digest,
    count: usize,
    epoch: u64 = 0,
    role: Role = .segment,
    segments: [maximum_segments]u64 = undefined,
    pub fn decode(binding: Binding, bytes: []const u8) !Record {
        if (bytes.len < 71) return error.InvalidIndexPublication;
        const v2 = std.mem.eql(u8, bytes[0..4], "ISP2");
        const v3 = std.mem.eql(u8, bytes[0..4], "ISP3");
        const base: usize = if (v3) 80 else if (v2) 79 else 71;
        const ids_offset: usize = if (v3) 48 else if (v2) 47 else 39;
        if (bytes.len < base or (!v3 and !v2 and !std.mem.eql(u8, bytes[0..4], "ISP1")) or
            !std.mem.eql(u8, bytes[bytes.len - 32 ..], &digest(binding, bytes[0 .. bytes.len - 32]))) return error.InvalidIndexPublication;
        const state: State = switch (bytes[4]) {
            1 => .published,
            2 => .cancelled,
            3 => .retired,
            else => return error.InvalidIndexPublication,
        };
        const count = std.mem.readInt(u16, bytes[37..39], .little);
        if (count > maximum_segments or bytes.len != base + @as(usize, count) * 8 or
            (state == .cancelled and count != 0)) return error.InvalidIndexPublication;
        const role: Role = if (v3) switch (bytes[47]) {
            1 => .segment,
            2 => .family,
            3 => .source_leaf,
            4 => .fields,
            else => return error.InvalidIndexPublication,
        } else .segment;
        var out: Record = .{ .state = state, .root = bytes[5..37].*, .count = count, .epoch = if (v2 or v3) std.mem.readInt(u64, bytes[39..47], .little) else 0, .role = role };
        if (((v2 or v3) and out.epoch == 0) or (state == .cancelled and !std.mem.allEqual(u8, &out.root, 0)) or
            (state != .cancelled and ((role == .segment and count == 0) or (role == .family and count != 1) or ((role == .source_leaf or role == .fields) and count != 0))) or
            (role != .segment and @import("index_source_epoch.zig").tokenEpoch(binding) != out.epoch)) return error.InvalidIndexPublication;
        for (out.segments[0..count], 0..) |*id, i| {
            id.* = std.mem.readInt(u64, bytes[ids_offset + i * 8 ..][0..8], .little);
            if (id.* == 0 or (i != 0 and out.segments[i - 1] >= id.*)) return error.InvalidIndexPublication;
        }
        return out;
    }
    pub fn eql(self: Record, other: Record) bool {
        return self.state == other.state and self.epoch == other.epoch and self.role == other.role and std.mem.eql(u8, &self.root, &other.root) and self.count == other.count and std.mem.eql(u64, self.segments[0..self.count], other.segments[0..other.count]);
    }
    fn write(self: Record, a: std.mem.Allocator, txn: anytype, binding: Binding) !void {
        const v2 = self.epoch != 0;
        const v3 = self.role != .segment;
        const ids_offset: usize = if (v3) 48 else if (v2) 47 else 39;
        const base: usize = if (v3) 80 else if (v2) 79 else 71;
        const bytes = try a.alloc(u8, base + self.count * 8);
        defer a.free(bytes);
        @memcpy(bytes[0..4], if (v3) "ISP3" else if (v2) "ISP2" else "ISP1");
        bytes[4] = @backingInt(self.state);
        @memcpy(bytes[5..37], &self.root);
        std.mem.writeInt(u16, bytes[37..39], @intCast(self.count), .little);
        if (v2) std.mem.writeInt(u64, bytes[39..47], self.epoch, .little);
        if (v3) bytes[47] = @backingInt(self.role);
        for (self.segments[0..self.count], 0..) |id, i| std.mem.writeInt(u64, bytes[ids_offset + i * 8 ..][0..8], id, .little);
        @memcpy(bytes[bytes.len - 32 ..], &digest(binding, bytes[0 .. bytes.len - 32]));
        try txn.put(&key(binding), bytes);
    }
};

pub fn load(txn: anytype, binding: Binding) !?Record {
    try validateBinding(binding);
    const bytes = txn.get(&key(binding)) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
    return try Record.decode(binding, bytes);
}
pub fn decision(txn: anytype, binding: Binding) !?Decision {
    const record = (try load(txn, binding)) orelse return null;
    return .{ .binding = binding, .state = record.state, .root = record.root, .epoch = record.epoch, .role = record.role, .seal_fn = seal };
}
/// A held snapshot may hydrate after catalog retirement. Its live lease is
/// evidence of membership in the original runtime generation; cancellation
/// never grants this path. Return the original published root identity.
pub fn heldReaderDecision(txn: anytype, binding: Binding, lease: *const ReaderPins.Lease) !Decision {
    try lease.require(binding);
    const record = (try load(txn, binding)) orelse return error.IndexPublicationMissing;
    if (record.state == .cancelled) return error.IndexPublicationCancelled;
    return .{ .binding = binding, .state = .published, .root = record.root, .epoch = record.epoch, .role = record.role, .seal_fn = seal };
}
/// Same transaction as publishing the segment root. Returning false means an
/// identical root is already published; never append duplicate segments then.
pub fn stagePublish(a: std.mem.Allocator, txn: anytype, binding: Binding, root: Digest, segments: []const u64) !bool {
    return stagePublishInEpoch(a, txn, binding, root, segments, 0);
}
pub fn stagePublishInEpoch(a: std.mem.Allocator, txn: anytype, binding: Binding, root: Digest, segments: []const u64, epoch: u64) !bool {
    return stagePublishRoleInEpoch(a, txn, binding, root, segments, epoch, .segment);
}
pub fn stagePublishRoleInEpoch(a: std.mem.Allocator, txn: anytype, binding: Binding, root: Digest, segments: []const u64, epoch: u64, role: Role) !bool {
    if ((segments.len == 0 and role != .source_leaf and role != .fields) or segments.len > maximum_segments) return error.InvalidIndexPublication;
    if (role != .segment and (segments.len != (if (role == .family) @as(usize, 1) else 0) or epoch == 0 or @import("index_source_epoch.zig").tokenEpoch(binding) != epoch)) return error.InvalidIndexPublication;
    for (segments, 0..) |id, i| if (id == 0 or (i != 0 and segments[i - 1] >= id)) return error.InvalidIndexPublication;
    if (try load(txn, binding)) |existing| {
        if (existing.state == .cancelled) return error.IndexPublicationCancelled;
        if (existing.state == .retired) return error.IndexPublicationRetired;
        if (existing.role != role or !std.mem.eql(u8, &existing.root, &root) or !std.mem.eql(u64, existing.segments[0..existing.count], segments)) return error.IndexPublicationConflict;
        return false;
    }
    var record: Record = .{ .state = .published, .root = root, .count = segments.len, .epoch = epoch, .role = role };
    @memcpy(record.segments[0..segments.len], segments);
    try record.write(a, txn, binding);
    return true;
}
pub fn stageCancel(a: std.mem.Allocator, txn: anytype, binding: Binding) !void {
    return stageCancelInEpoch(a, txn, binding, 0);
}
pub fn stageCancelInEpoch(a: std.mem.Allocator, txn: anytype, binding: Binding, epoch: u64) !void {
    if (try load(txn, binding)) |existing| {
        if (existing.state != .cancelled) return error.IndexPublicationAlreadyCommitted;
        return;
    }
    try (Record{ .state = .cancelled, .root = @splat(0), .count = 0, .epoch = epoch }).write(a, txn, binding);
}
pub fn stageRetire(a: std.mem.Allocator, txn: anytype, binding: Binding, active_segments: []const u64) !void {
    var record = (try load(txn, binding)) orelse return error.IndexPublicationMissing;
    if (record.state == .cancelled) return error.IndexPublicationCancelled;
    for (record.segments[0..record.count]) |id| for (active_segments) |active| {
        if (active == id) return error.IndexPublicationStillActive;
    };
    if (record.state == .retired) return;
    record.state = .retired;
    try record.write(a, txn, binding);
}

/// Reader registration is sealed before retirement. Clones of live leases may
/// continue after sealing, but once drained the count can never increase again.
/// The containing index owner keeps this cell alive until claim GC completes.
pub const ReaderPins = struct {
    const closed = @as(u64, 1) << 63;
    binding: Binding,
    word: std.atomic.Value(u64) = .init(0),
    pub const Lease = struct {
        owner: *ReaderPins,
        live: bool = true,
        pub fn require(self: *const Lease, binding: Binding) !void {
            if (!self.live or !same(self.owner.binding, binding) or self.owner.word.load(.acquire) & ~closed == 0) return error.IndexSourceLeaseClosed;
        }
        pub fn clone(self: *const Lease) !Lease {
            try self.require(self.owner.binding);
            var before = self.owner.word.load(.acquire);
            while (true) {
                if (before & ~closed == 0) return error.IndexSourceLeaseClosed;
                if (before & ~closed == closed - 1) return error.ResourceLimitExceeded;
                before = self.owner.word.cmpxchgWeak(before, before + 1, .acq_rel, .acquire) orelse return .{ .owner = self.owner };
            }
        }
        fn release(self: *Lease) bool {
            if (!self.live) return false;
            self.live = false;
            const before = self.owner.word.fetchSub(1, .acq_rel);
            std.debug.assert(before & ~closed != 0);
            return before & ~closed == 1;
        }
        pub fn deinit(self: *Lease) void {
            _ = self.release();
        }
    };
    pub const Drained = struct { owner: *const ReaderPins, seal_fn: *const fn () void };
    pub fn acquire(self: *ReaderPins) !Lease {
        var value = self.word.load(.acquire);
        while (value & closed == 0) {
            if (value == closed - 1) return error.ResourceLimitExceeded;
            value = self.word.cmpxchgWeak(value, value + 1, .acq_rel, .acquire) orelse return .{ .owner = self };
        }
        return error.IndexSourceLeaseClosed;
    }
    pub fn retire(self: *ReaderPins) void {
        _ = self.word.fetchOr(closed, .acq_rel);
    }
    pub fn drained(self: *const ReaderPins) ?Drained {
        return if (self.word.load(.acquire) == closed) .{ .owner = self, .seal_fn = seal } else null;
    }
    pub fn requireDrained(proof: Drained, binding: Binding) !void {
        if (proof.seal_fn != seal or !same(proof.owner.binding, binding) or proof.owner.word.load(.acquire) != closed) return error.IndexSourceReadersActive;
    }
};

/// Process-local lifetime rendezvous, keyed by globally unique durable index
/// owner/publication IDs. Held snapshots can outlive close/reopen; each opener
/// must join the same cell rather than infer drainage from its own snapshots.
/// The registry has fixed lock shards and no retained map allocation. Cells
/// disappear with their last handle; it holds no source data or authority.
/// Durable publication decisions remain the sole source of retirement authority.
pub const SharedReaders = struct {
    /// Wake context outlives the index through a retained native reference.
    /// Retain/release are nonblocking; notification only signals work and must
    /// not touch a closed index or perform I/O. It is not publication authority.
    pub const WakePort = struct {
        context: *anyopaque,
        retain: *const fn (*anyopaque) void,
        release: *const fn (*anyopaque) void,
        notify: *const fn (*anyopaque) void,
    };
    const Cell = struct {
        allocator: std.mem.Allocator,
        // A handle may be destroyed by another compiled runtime image. Its
        // origin owns registry membership and locking for the cell lifetime.
        bucket: *Shard,
        registry: *OwnerNode,
        pins: ReaderPins,
        references: usize,
        preparation_epoch: u64 = 0,
        preparations: std.atomic.Value(u32) = .init(0),
        wake: ?WakePort = null,
        wake_enabled: std.atomic.Value(bool) = .init(false),
        next: ?*Cell,
    };
    const Shard = struct {
        mutex: std.atomic.Mutex = .unlocked,
        first: ?*Cell = null,
        fn lock(self: *Shard) void {
            while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        }
    };
    const OwnerNode = struct {
        allocator: std.mem.Allocator,
        bucket: *OwnerShard,
        identity: Digest,
        references: usize,
        shards: [64]Shard = @splat(.{}),
        next: ?*OwnerNode,
    };
    const OwnerShard = struct {
        mutex: std.atomic.Mutex = .unlocked,
        first: ?*OwnerNode = null,
        fn lock(self: *@This()) void {
            while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        }
    };
    var owners: [64]OwnerShard = @splat(.{});
    fn ownerShard(identity: Digest) *OwnerShard {
        return &owners[@intCast(std.hash.Wyhash.hash(0, &identity) % owners.len)];
    }
    fn retainOwner(node: *OwnerNode) !void {
        node.bucket.lock();
        defer node.bucket.mutex.unlock();
        node.references = try std.math.add(usize, node.references, 1);
    }
    fn releaseOwner(node: *OwnerNode) void {
        const bucket = node.bucket;
        bucket.lock();
        std.debug.assert(node.references != 0);
        node.references -= 1;
        if (node.references != 0) {
            bucket.mutex.unlock();
            return;
        }
        var link = &bucket.first;
        while (link.* != node) link = &link.*.?.next;
        link.* = node.next;
        bucket.mutex.unlock();
        node.allocator.destroy(node);
    }
    fn findOwner(identity: Digest) !?OwnerRegistry {
        const bucket = ownerShard(identity);
        bucket.lock();
        defer bucket.mutex.unlock();
        var node = bucket.first;
        while (node) |value| : (node = value.next) if (std.mem.eql(u8, &value.identity, &identity)) {
            value.references = try std.math.add(usize, value.references, 1);
            return .{ .node = value };
        };
        return null;
    }

    /// Move-only native rendezvous for one physical owner. Query leases retain
    /// publication cells directly, so this adds no owner counter/lock to their
    /// clone/release path. Cells keep their originating registry alive.
    /// Cold native broker opens check every shared private record, in addition
    /// to the callback signature. A receiver never casts an older cell layout.
    pub fn readerLayouts() [11]@import("antfly_runtime_abi").native_abi.TypeContract {
        const T = @import("antfly_runtime_abi").native_abi.TypeContract;
        return .{ T.of(OwnerNode), T.of(OwnerShard), T.of(Shard), T.of(Cell), T.of(ReaderPins), T.of(ReaderPins.Lease), T.of(Handle), T.of(Lease), T.of(Registration), T.of(PublisherRegistration), T.of(WakePort) };
    }
    pub const OwnerRegistry = struct {
        node: ?*OwnerNode,
        pub fn open(a: std.mem.Allocator, identity: Digest) !OwnerRegistry {
            if (std.mem.allEqual(u8, &identity, 0)) return error.InvalidIndexPublication;
            const bucket = ownerShard(identity);
            bucket.lock();
            defer bucket.mutex.unlock();
            var existing = bucket.first;
            while (existing) |node| : (existing = node.next) if (std.mem.eql(u8, &node.identity, &identity)) {
                node.references = try std.math.add(usize, node.references, 1);
                return .{ .node = node };
            };
            const node = try a.create(OwnerNode);
            node.* = .{ .allocator = a, .bucket = bucket, .identity = identity, .references = 1, .next = bucket.first };
            bucket.first = node;
            return .{ .node = node };
        }
        pub fn clone(self: OwnerRegistry) !OwnerRegistry {
            const node = self.node orelse return error.IndexSourceLeaseClosed;
            try retainOwner(node);
            return self;
        }
        pub fn deinit(self: *OwnerRegistry) void {
            const node = self.node orelse return;
            self.node = null;
            releaseOwner(node);
        }
        pub fn openPublication(self: OwnerRegistry, a: std.mem.Allocator, binding: Binding) !Handle {
            try validateBinding(binding);
            const node = self.node orelse return error.IndexSourceLeaseClosed;
            if (!std.mem.eql(u8, &node.identity, &binding.owner)) return error.IndexSourceOwnerMismatch;
            const bucket = &node.shards[@intCast(std.hash.Wyhash.hash(0, &key(binding)) % node.shards.len)];
            bucket.lock();
            defer bucket.mutex.unlock();
            var entry = bucket.first;
            while (entry) |cell| : (entry = cell.next) if (same(cell.pins.binding, binding)) {
                cell.references = try std.math.add(usize, cell.references, 1);
                return .{ .cell = cell };
            };
            const cell = try a.create(Cell);
            errdefer a.destroy(cell);
            try retainOwner(node);
            cell.* = .{ .allocator = a, .bucket = bucket, .registry = node, .pins = .{ .binding = binding }, .references = 1, .next = bucket.first };
            bucket.first = cell;
            return .{ .cell = cell };
        }
        /// The caller first durably closes the owner against new registration.
        /// Work scales with this owner's cells, independent of other tables.
        pub fn hasPins(self: OwnerRegistry) bool {
            const node = self.node orelse return false;
            for (&node.shards) |*bucket| {
                bucket.lock();
                var cell = bucket.first;
                while (cell) |value| : (cell = value.next) if (value.pins.word.load(.acquire) & ~ReaderPins.closed != 0) {
                    bucket.mutex.unlock();
                    return true;
                };
                bucket.mutex.unlock();
            }
            return false;
        }
        pub fn hasPreparationsBefore(self: OwnerRegistry, epoch: u64) bool {
            const node = self.node orelse return false;
            for (&node.shards) |*bucket| {
                bucket.lock();
                var entry = bucket.first;
                while (entry) |cell| : (entry = cell.next) if (cell.preparations.load(.acquire) != 0 and cell.preparation_epoch < epoch) {
                    bucket.mutex.unlock();
                    return true;
                };
                bucket.mutex.unlock();
            }
            return false;
        }
        /// Move wakeups without I/O. The retained context survives owner close.
        pub fn watch(self: OwnerRegistry, port: WakePort) !void {
            const node = self.node orelse return error.IndexSourceLeaseClosed;
            for (&node.shards) |*bucket| while (true) {
                bucket.lock();
                var selected: ?*Cell = null;
                var cell = bucket.first;
                while (cell) |value| : (cell = value.next) {
                    if (value.wake) |wake| if (wake.context == port.context and wake.notify == port.notify and wake.retain == port.retain and wake.release == port.release) continue;
                    value.references = std.math.add(usize, value.references, 1) catch |err| {
                        bucket.mutex.unlock();
                        return err;
                    };
                    selected = value;
                    break;
                }
                bucket.mutex.unlock();
                var handle: Handle = .{ .cell = selected orelse break };
                handle.watch(port);
                handle.deinit();
            };
        }
    };

    pub fn ownerHasPins(owner: Digest) bool {
        var registry = (findOwner(owner) catch return true) orelse return false;
        defer registry.deinit();
        return registry.hasPins();
    }
    pub fn watchOwner(owner: Digest, port: WakePort) !void {
        var registry = (try findOwner(owner)) orelse return;
        defer registry.deinit();
        try registry.watch(port);
    }

    pub const Handle = struct {
        cell: ?*Cell,
        pub fn clone(self: Handle) !Handle {
            const cell = self.cell orelse return error.IndexSourceLeaseClosed;
            const bucket = cell.bucket;
            bucket.lock();
            defer bucket.mutex.unlock();
            cell.references = try std.math.add(usize, cell.references, 1);
            return .{ .cell = cell };
        }
        pub fn deinit(self: *Handle) void {
            const cell = self.cell orelse return;
            self.cell = null;
            const bucket = cell.bucket;
            bucket.lock();
            std.debug.assert(cell.references != 0);
            cell.references -= 1;
            if (cell.references != 0) {
                bucket.mutex.unlock();
                return;
            }
            // Every live lease owns a handle, so no pin can outlive this cell.
            std.debug.assert(cell.pins.word.load(.acquire) & ~ReaderPins.closed == 0);
            std.debug.assert(cell.preparations.load(.acquire) == 0);
            var link = &bucket.first;
            while (link.* != cell) link = &link.*.?.next;
            link.* = cell.next;
            const wake = cell.wake;
            const registry = cell.registry;
            bucket.mutex.unlock();
            if (wake) |port| port.release(port.context);
            cell.allocator.destroy(cell);
            releaseOwner(registry);
        }
        /// One current maintenance owner per durable physical owner. Reopen
        /// replaces its wake context while old snapshots retain this same cell.
        pub fn watch(self: Handle, port: WakePort) void {
            port.retain(port.context); // cell ownership
            port.retain(port.context); // notification borrowed below
            defer port.release(port.context);
            const cell = self.cell.?;
            const bucket = cell.bucket;
            bucket.lock();
            const previous = cell.wake;
            const same_owner = if (previous) |old| old.context == port.context and old.notify == port.notify and old.retain == port.retain and old.release == port.release else false;
            cell.wake = port;
            cell.wake_enabled.store(true, .release);
            const empty = cell.pins.word.load(.acquire) & ~ReaderPins.closed == 0;
            bucket.mutex.unlock();
            if (previous) |old| old.release(old.context);
            if (empty and !same_owner) port.notify(port.context);
        }
        fn notifyZero(self: Handle) void {
            const cell = self.cell.?;
            if (!cell.wake_enabled.load(.acquire)) return;
            const bucket = cell.bucket;
            bucket.lock();
            const wake = cell.wake;
            if (wake) |port| port.retain(port.context);
            bucket.mutex.unlock();
            if (wake) |port| {
                port.notify(port.context);
                port.release(port.context);
            }
        }
        pub fn lease(self: Handle) !Lease {
            var handle = try self.clone();
            errdefer handle.deinit();
            return .{ .handle = handle, .inner = try handle.cell.?.pins.acquire() };
        }
        pub fn register(self: Handle) !Registration {
            var handle = try self.clone();
            errdefer handle.deinit();
            // The registration owns this pin and handle, represented by one
            // nullable cell pointer rather than a full query-lease record.
            _ = try handle.cell.?.pins.acquire();
            return .{ .cell = handle.cell };
        }
        pub fn prepare(self: Handle, epoch: u64) !PublisherRegistration {
            var registration = try self.register();
            errdefer registration.deinit();
            const cell = registration.cell.?;
            cell.bucket.lock();
            defer cell.bucket.mutex.unlock();
            const count = cell.preparations.load(.acquire);
            if (count == std.math.maxInt(u32)) return error.ResourceLimitExceeded;
            if (count != 0 and cell.preparation_epoch != epoch) return error.IndexPublicationConflict;
            cell.preparation_epoch = epoch;
            _ = cell.preparations.fetchAdd(1, .release);
            return .{ .cell = cell };
        }
        pub fn hasPreparations(self: Handle) bool {
            return self.cell.?.preparations.load(.acquire) != 0;
        }
        pub fn hasPins(self: Handle) bool {
            return self.cell.?.pins.word.load(.acquire) & ~ReaderPins.closed != 0;
        }
        pub fn retire(self: Handle) void {
            self.cell.?.pins.retire();
        }
        /// This proof borrows the handle. Keep it live through the primary
        /// reclamation transaction; the proof never owns a dangling cell.
        pub fn drained(self: Handle) ?ReaderPins.Drained {
            return self.cell.?.pins.drained();
        }
    };
    /// A preparation adds no work to query leases. Its final release also
    /// wakes epoch-floor advancement even when published readers remain live.
    pub const PublisherRegistration = struct {
        cell: ?*Cell = null,
        pub fn take(self: *PublisherRegistration) PublisherRegistration {
            const result = self.*;
            self.cell = null;
            return result;
        }
        pub fn require(self: *const PublisherRegistration, binding: Binding) !void {
            const cell = self.cell orelse return error.IndexSourceLeaseClosed;
            if (!same(cell.pins.binding, binding) or cell.preparations.load(.acquire) == 0) return error.IndexSourceLeaseClosed;
        }
        pub fn deinit(self: *PublisherRegistration) void {
            const cell = self.cell orelse return;
            self.cell = null;
            const before = cell.preparations.fetchSub(1, .acq_rel);
            std.debug.assert(before != 0);
            if (before == 1) (Handle{ .cell = cell }).notifyZero();
            var registration: Registration = .{ .cell = cell };
            registration.deinit();
        }
    };
    /// Pointer-sized segment ownership; no allocation and no per-query state
    /// in ordinary SegmentShared cells. Move-only like other native handles.
    pub const Registration = struct {
        cell: ?*Cell = null,
        pub fn present(self: *const Registration) bool {
            return self.cell != null;
        }
        pub fn deinit(self: *Registration) void {
            const cell = self.cell orelse return;
            self.cell = null;
            var pin: ReaderPins.Lease = .{ .owner = &cell.pins };
            var handle: Handle = .{ .cell = cell };
            if (pin.release()) handle.notifyZero();
            handle.deinit();
        }
        pub fn lease(self: *const Registration) !?Lease {
            const cell = self.cell orelse return null;
            const parent: Lease = .{ .handle = .{ .cell = cell }, .inner = .{ .owner = &cell.pins } };
            return try parent.clone();
        }
    };
    pub const Lease = struct {
        handle: Handle,
        inner: ReaderPins.Lease,
        pub fn binding(self: *const Lease) !Binding {
            // Releasing the last handle can destroy its reader cell. Check
            // local lifetime state before following the borrowed owner pointer.
            if (!self.inner.live) return error.IndexSourceLeaseClosed;
            const identity = self.inner.owner.binding;
            try self.inner.require(identity);
            return identity;
        }
        pub fn clone(self: *const Lease) !Lease {
            var handle = try self.handle.clone();
            errdefer handle.deinit();
            return .{ .handle = handle, .inner = try self.inner.clone() };
        }
        pub fn deinit(self: *Lease) void {
            if (self.inner.release()) self.handle.notifyZero();
            self.handle.deinit();
        }
    };
    pub fn open(a: std.mem.Allocator, binding: Binding) !Handle {
        try validateBinding(binding);
        var registry = try OwnerRegistry.open(a, binding.owner);
        defer registry.deinit();
        return registry.openPublication(a, binding);
    }
};
