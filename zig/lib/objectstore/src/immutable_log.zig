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

//! Object-native serial commits, ownership fencing and bounded recovery.
//! Each slot is created once. Seals preserve the last root while moving to a
//! new epoch. Checkpoints are monotonic recovery floors, never per-write HEADs.
const std = @import("std");
const Client = @import("client.zig").Client;
const types = @import("types.zig");
const A = std.mem.Allocator;
pub const State = struct {
    format: u8 = 1,
    epoch: [16]u8 = @splat(0),
    owner: [16]u8 = @splat(0),
    next: u64 = 0,
    revision: u64 = 0,
    root: []const u8 = "",
};
pub const Frame = struct {
    format: u8 = 1,
    kind: enum { commit, seal },
    epoch: [16]u8,
    slot: u64,
    parent_revision: u64,
    state: State,
};
pub const Checkpoint = struct { format: u8 = 1, written_ms: u64, state: State };
pub const Recovery = struct { state: State, has_more: bool = false };
pub const max_root_bytes = 12 * 1024;
// Keep space for epoch rotation, larger counters and checkpoint headers under
// the existing sixteen-KiB record limit. Optional read capsules may be evicted;
// authoritative tree roots and WAL records cannot be silently truncated.
pub fn rootFits(a: A, source: State, root: []const u8) !bool {
    if (root.len > max_root_bytes) return false;
    const next = State{ .epoch = source.epoch, .owner = source.owner, .next = source.next +| 1, .revision = source.revision +| 1, .root = root };
    const bytes = try std.json.Stringify.valueAlloc(a, Frame{ .kind = .commit, .epoch = source.epoch, .slot = source.next, .parent_revision = source.revision, .state = next }, .{});
    defer a.free(bytes);
    return bytes.len <= 15360;
}
/// Short native certificates are valid only for namespaces whose collector
/// persists a >=60-second grace. Anchor before observation/PUT; never renew a
/// certificate merely because an old source was presented again.
pub const ProofTime = struct { wall_ms: u64, mono_ms: u64 };
pub const WriteProof = struct { anchor: ProofTime, auth: [32]u8 };
pub const ProofAuthority = struct {
    key: [32]u8,
    scope: [16]u8,
    clock_ptr: ?*const anyopaque = null,
    clock_fn: ?*const fn (*const anyopaque) ProofTime = null,
    pub const lifetime_ms = 30000;
    pub fn now(self: ProofAuthority) ProofTime {
        if (self.clock_fn) |read_| return read_(self.clock_ptr.?);
        const time = @import("antfly_platform").time;
        return .{ .wall_ms = time.realtimeNs() / std.time.ns_per_ms, .mono_ms = time.monotonicNs() / std.time.ns_per_ms };
    }
    fn current(self: ProofAuthority, anchor: ProofTime) !void {
        const sample = self.now();
        if (std.mem.allEqual(u8, &self.scope, 0) or anchor.wall_ms == 0 or anchor.mono_ms == 0 or sample.wall_ms < anchor.wall_ms or sample.mono_ms < anchor.mono_ms or sample.wall_ms - anchor.wall_ms >= lifetime_ms or sample.mono_ms - anchor.mono_ms >= lifetime_ms) return error.ImmutableLogProofExpired;
    }
    fn field(hash: *std.crypto.hash.sha2.Sha256, bytes: []const u8) void {
        var n: [8]u8 = undefined;
        std.mem.writeInt(u64, &n, bytes.len, .little);
        hash.update(&n);
        hash.update(bytes);
    }
    fn signature(self: ProofAuthority, log: Log, state: State, anchor: ProofTime) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        field(&hash, "antfly-bounded-journal-v1");
        field(&hash, &self.scope);
        field(&hash, log.bucket);
        field(&hash, log.prefix);
        field(&hash, &state.owner);
        field(&hash, &state.epoch);
        field(&hash, state.root);
        var counters: [33]u8 = undefined;
        counters[0] = state.format;
        std.mem.writeInt(u64, counters[1..9], state.next, .little);
        std.mem.writeInt(u64, counters[9..17], state.revision, .little);
        std.mem.writeInt(u64, counters[17..25], anchor.wall_ms, .little);
        std.mem.writeInt(u64, counters[25..33], anchor.mono_ms, .little);
        hash.update(&counters);
        var result: [32]u8 = undefined;
        std.crypto.auth.hmac.sha2.HmacSha256.create(&result, &hash.finalResult(), &self.key);
        return result;
    }
    pub fn issue(self: ProofAuthority, log: Log, state: State, anchor: ProofTime) !WriteProof {
        try Log.validate(state);
        try self.current(anchor);
        return .{ .anchor = anchor, .auth = self.signature(log, state, anchor) };
    }
    pub fn authenticate(self: ProofAuthority, log: Log, state: State, proof: WriteProof) !void {
        try Log.validate(state);
        if (std.mem.allEqual(u8, &self.scope, 0) or proof.anchor.wall_ms == 0 or proof.anchor.mono_ms == 0) return error.InvalidImmutableLogProof;
        if (!std.crypto.timing_safe.eql([32]u8, proof.auth, self.signature(log, state, proof.anchor))) return error.InvalidImmutableLogProof;
    }
    pub fn check(self: ProofAuthority, log: Log, state: State, proof: WriteProof) !void {
        try self.authenticate(log, state, proof);
        try self.current(proof.anchor);
    }
};
pub const CertifiedRecovery = struct { recovery: Recovery, proof: WriteProof };
pub const ObservationMetrics = struct {
    cold_starts: u32 = 0,
    expired_refreshes: u32 = 0,
    checkpoint_rebases: u32 = 0,
    legacy_fallbacks: u32 = 0,
    slots_followed: u32 = 0,
    seals_followed: u32 = 0,
    batch_waves: u32 = 0,
    prefetched_slots: u32 = 0,
    absence_rechecks: u32 = 0,
};
pub const Log = struct {
    client: Client,
    bucket: []const u8,
    prefix: []const u8,
    owner: [16]u8,
    io: std.Io,
    cancellation: ?types.CancellationToken = null,
    checkpoint_commits: u64 = epoch_commits,
    read_batch: bool = false,
    observation_metrics: ?*ObservationMetrics = null,
    const max_steps = 64;
    pub const epoch_commits = 64;

    fn key(self: Log, a: A, epoch: [16]u8, slot: ?u64) ![]u8 {
        return if (slot) |n| std.fmt.allocPrint(a, "{s}/epochs/{s}/{d:0>20}.json", .{ self.prefix, std.fmt.bytesToHex(epoch, .lower), n }) else std.fmt.allocPrint(a, "{s}/epochs/{s}/SEALED", .{ self.prefix, std.fmt.bytesToHex(epoch, .lower) });
    }
    fn read(self: Log, a: A, k: []const u8) !?types.GetResult {
        var client = self.client;
        client.allocator = a;
        var result = client.getObject(self.bucket, k, .{ .skip_metadata_probe = true, .cancellation = self.cancellation }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        if (result.body.len > 16384) {
            result.deinit(a);
            return error.InvalidImmutableLog;
        }
        return result;
    }
    pub fn checkpoint(self: Log, a: A) !?Checkpoint {
        const k = try std.fmt.allocPrint(a, "{s}/CHECKPOINT", .{self.prefix});
        defer a.free(k);
        var value = (try self.read(a, k)) orelse {
            if (try self.read(a, try std.fmt.allocPrint(a, "{s}/CHECKPOINT_REQUIRED", .{self.prefix}))) |_| return error.ImmutableLogCheckpointMissing;
            return null;
        };
        defer value.deinit(a);
        const cp = (try std.json.parseFromSlice(Checkpoint, a, value.body, .{ .allocate = .alloc_always })).value;
        try validate(cp.state);
        if (cp.format != 1) return error.InvalidImmutableLog;
        return cp;
    }
    fn validate(state: State) !void {
        if (state.revision > 9_007_199_254_740_991) return error.ImmutableLogRevisionExceeded;
        if (state.format != 1 or state.root.len > max_root_bytes or state.next > epoch_commits) return error.InvalidImmutableLog;
    }
    fn frame(a: A, bytes: []const u8, epoch: [16]u8, slot: ?u64) !Frame {
        const value = (try std.json.parseFromSlice(Frame, a, bytes, .{ .allocate = .alloc_always })).value;
        try validate(value.state);
        if (value.format != 1 or !std.mem.eql(u8, &value.epoch, &epoch) or (slot != null and value.slot != slot.?) or value.state.revision != try std.math.add(u64, value.parent_revision, 1)) return error.InvalidImmutableLog;
        if (value.kind == .commit) {
            if (!std.mem.eql(u8, &value.state.epoch, &epoch) or value.state.next != try std.math.add(u64, value.slot, 1)) return error.InvalidImmutableLog;
        } else if (std.mem.eql(u8, &value.state.epoch, &epoch) or value.state.next != 0) return error.InvalidImmutableLog;
        return value;
    }
    fn immutable(self: Log, a: A, k: []const u8, bytes: []const u8) !void {
        if (bytes.len > 16384) return error.InvalidImmutableLog;
        var client = self.client;
        client.allocator = a;
        var receipt = client.putObject(self.bucket, k, bytes, .{ .if_none_match = true, .content_type = "application/json", .cancellation = self.cancellation }) catch |err| switch (err) {
            error.PreconditionFailed => {
                var existing = (try self.read(a, k)) orelse return error.PreconditionFailed;
                defer existing.deinit(a);
                if (!std.mem.eql(u8, existing.body, bytes)) return error.PreconditionFailed;
                return;
            },
            else => return err,
        };
        receipt.deinit(a);
    }
    fn marker(self: Log, a: A, value: Frame) !void {
        if (value.kind != .seal) return error.InvalidImmutableLog;
        const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
        defer a.free(bytes);
        const k = try self.key(a, value.epoch, null);
        defer a.free(k);
        try self.immutable(a, k, bytes);
    }
    fn same(l: State, r: State) bool {
        return l.format == r.format and std.mem.eql(u8, &l.epoch, &r.epoch) and std.mem.eql(u8, &l.owner, &r.owner) and l.next == r.next and l.revision == r.revision and std.mem.eql(u8, l.root, r.root);
    }
    fn start(self: Log, a: A, state: State) !void {
        const k = try std.fmt.allocPrint(a, "{s}/epochs/{s}/START", .{ self.prefix, std.fmt.bytesToHex(state.epoch, .lower) });
        const bytes = try std.json.Stringify.valueAlloc(a, state, .{});
        if (try self.read(a, k)) |stored| {
            if (!std.mem.eql(u8, stored.body, bytes)) return error.InvalidImmutableLog;
        } else try self.immutable(a, k, bytes);
    }
    fn authenticate(self: Log, a: A, state: State, cp: ?Checkpoint) !void {
        if (cp) |value| {
            if (value.state.revision > state.revision) return error.ImmutableLogFenced;
            if (value.state.revision == state.revision) {
                if (!same(value.state, state)) return error.InvalidImmutableLog;
                return;
            }
        }
        if (state.revision == 0) {
            if (!same(state, State{})) return error.InvalidImmutableLog;
            return;
        }
        if (state.next == 0) {
            const k = try std.fmt.allocPrint(a, "{s}/epochs/{s}/START", .{ self.prefix, std.fmt.bytesToHex(state.epoch, .lower) });
            const value = (try self.read(a, k)) orelse return error.InvalidImmutableLog;
            const recorded = (try std.json.parseFromSlice(State, a, value.body, .{ .allocate = .alloc_always })).value;
            if (!same(recorded, state)) return error.InvalidImmutableLog;
        } else {
            const value = (try self.read(a, try self.key(a, state.epoch, state.next - 1))) orelse return error.InvalidImmutableLog;
            const recorded = try frame(a, value.body, state.epoch, state.next - 1);
            if (recorded.kind != .commit or !same(recorded.state, state)) return error.InvalidImmutableLog;
        }
    }
    /// Only the final cursor belongs to the caller. Recovery reuses a bounded
    /// scratch arena rather than retaining every decoded frame in that arena.
    pub fn recover(self: Log, a: A, cursor: ?State, takeover: bool) !Recovery {
        return self.recoverMode(a, cursor, takeover, false, false);
    }
    /// Observe an existing owner's committed prefix without repairing markers,
    /// rotating epochs, checkpointing or acquiring ownership.
    pub fn observe(self: Log, a: A, cursor: State) !Recovery {
        return self.recoverMode(a, cursor, false, true, false);
    }
    /// Authenticate the current committed prefix without acquiring an owner.
    /// Unlike an owned read session, this observer may follow valid seals to
    /// another owner. Every cached cursor remains an authenticated hint.
    pub fn observeCommitted(self: Log, a: A, cursor: ?State) !Recovery {
        return self.recoverMode(a, cursor, false, true, true);
    }
    fn recoverMode(self: Log, result_alloc: A, cursor: ?State, takeover: bool, read_only: bool, follow_owners: bool) !Recovery {
        var scratch = std.heap.ArenaAllocator.init(result_alloc);
        defer scratch.deinit();
        const a = scratch.allocator();
        var root_buffer: [max_root_bytes]u8 = undefined;
        var state = cursor orelse State{};
        try validate(state);
        const checkpoint_value = try self.checkpoint(a);
        if (checkpoint_value) |cp| if (cp.state.revision > state.revision) {
            state = cp.state;
        };
        try self.authenticate(a, state, checkpoint_value);
        for (0..max_steps) |_| {
            // The root is the only variable-sized field crossing a recovery
            // step. Copy it before resetting JSON bodies, parse arenas and keys.
            std.mem.copyForwards(u8, root_buffer[0..state.root.len], state.root);
            state.root = root_buffer[0..state.root.len];
            _ = scratch.reset(.retain_capacity);
            if (!follow_owners and !takeover and !std.mem.eql(u8, &state.owner, &self.owner)) return error.ImmutableLogFenced;
            if (self.cancellation) |token| try token.check();
            const sealed_key = try self.key(a, state.epoch, null);
            if (try self.read(a, sealed_key)) |stored| {
                const sealed = try frame(a, stored.body, state.epoch, null);
                if (sealed.kind != .seal or sealed.parent_revision < state.revision) return error.InvalidImmutableLog;
                if (!read_only) try self.start(a, sealed.state);
                state = sealed.state;
                continue;
            }
            const k = try self.key(a, state.epoch, state.next);
            if (try self.read(a, k)) |stored| {
                const item = try frame(a, stored.body, state.epoch, state.next);
                if (item.parent_revision != state.revision) return error.InvalidImmutableLog;
                if (item.kind == .seal) {
                    if (!std.mem.eql(u8, item.state.root, state.root)) return error.InvalidImmutableLog;
                    if (!read_only) {
                        try self.start(a, item.state);
                        try self.marker(a, item);
                    }
                } else if (!std.mem.eql(u8, &item.state.owner, &state.owner)) return error.InvalidImmutableLog;
                state = item.state;
                continue;
            }
            if (!follow_owners and !std.mem.eql(u8, &state.owner, &self.owner)) {
                if (!takeover) return error.ImmutableLogFenced;
                state = self.seal(a, state, self.owner) catch |err| switch (err) {
                    error.PreconditionFailed => continue,
                    else => return err,
                };
                _ = self.writeCheckpoint(a, state, true) catch false;
            }
            if (!read_only and state.next == epoch_commits) state = try self.seal(a, state, self.owner);
            state.root = try result_alloc.dupe(u8, state.root);
            return .{ .state = state };
        }
        if (!follow_owners and !takeover and !std.mem.eql(u8, &state.owner, &self.owner)) return error.ImmutableLogFenced;
        state.root = try result_alloc.dupe(u8, state.root);
        return .{ .state = state, .has_more = true };
    }
    fn seal(self: Log, a: A, source: State, owner: [16]u8) !State {
        var epoch: [16]u8 = undefined;
        self.io.random(&epoch);
        if (std.mem.eql(u8, &epoch, &source.epoch)) return error.InvalidImmutableLog;
        const next = State{ .epoch = epoch, .owner = owner, .revision = try std.math.add(u64, source.revision, 1), .root = source.root };
        try validate(next);
        const value = Frame{ .kind = .seal, .epoch = source.epoch, .slot = source.next, .parent_revision = source.revision, .state = next };
        const k = try self.key(a, source.epoch, source.next);
        const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
        try self.immutable(a, k, bytes);
        // The accepted seal is authoritative. These two independent immutable
        // recovery hints can upload together; a partial failure is repaired by
        // ordinary recovery from that same seal, never by publishing a new root.
        const start_key = try std.fmt.allocPrint(a, "{s}/epochs/{s}/START", .{ self.prefix, std.fmt.bytesToHex(next.epoch, .lower) });
        const marker_key = try self.key(a, source.epoch, null);
        const start_bytes = try std.json.Stringify.valueAlloc(a, next, .{});
        const requests = [_]types.PutObjectRequest{
            .{ .key = start_key, .body = start_bytes, .options = .{ .if_none_match = true, .content_type = "application/json", .cancellation = self.cancellation } },
            .{ .key = marker_key, .body = bytes, .options = .{ .if_none_match = true, .content_type = "application/json", .cancellation = self.cancellation } },
        };
        var client = self.client;
        client.allocator = a;
        const results = client.putObjects(self.bucket, &requests) catch |err| switch (err) {
            error.PreconditionFailed => {
                try self.start(a, next);
                try self.marker(a, value);
                return next;
            },
            else => return err,
        };
        defer {
            for (results) |*result| result.deinit(a);
            a.free(results);
        }
        return next;
    }
    fn unfenced(self: Log, a: A, state: State) !void {
        if (!std.mem.eql(u8, &state.owner, &self.owner)) return error.ImmutableLogFenced;
        // The checkpoint floor also fences deleted old epochs. A delayed PUT
        // may recreate an orphan slot, but can never receive a successful ack.
        try self.authenticate(a, state, try self.checkpoint(a));
        const k = try self.key(a, state.epoch, null);
        if (try self.read(a, k)) |_| return error.ImmutableLogFenced;
    }
    pub fn append(self: Log, a: A, source: State, root: []const u8) !State {
        try validate(source);
        if (self.checkpoint_commits == 0 or epoch_commits % self.checkpoint_commits != 0) return error.InvalidImmutableLog;
        if (source.next >= epoch_commits) return error.ImmutableLogRotationRequired;
        try self.unfenced(a, source);
        const next = State{ .epoch = source.epoch, .owner = source.owner, .next = source.next + 1, .revision = try std.math.add(u64, source.revision, 1), .root = root };
        try validate(next);
        const value = Frame{ .kind = .commit, .epoch = source.epoch, .slot = source.next, .parent_revision = source.revision, .state = next };
        const k = try self.key(a, source.epoch, source.next);
        const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
        try self.immutable(a, k, bytes);
        try self.unfenced(a, next);
        var result = next;
        if (next.next == epoch_commits) result = self.seal(a, next, self.owner) catch next;
        // A hint failure cannot replace a durable commit's receipt.
        // A shorter best-effort recovery floor avoids replaying an entire
        // 64-slot epoch after an idle restart. The one-second overwrite limit
        // still applies, and hint failure never replaces an accepted commit.
        if (result.next % self.checkpoint_commits == 0) _ = self.writeCheckpoint(a, result, false) catch false;
        return result;
    }
    /// An old MAC still authenticates accepted bytes, never freshness. Before
    /// reusing it, read the CURRENT durable GC floor. This avoids rereading the
    /// original frame after idle expiry while preserving retirement fencing.
    pub fn refreshCertificate(self: Log, a: A, source: State, authority: ProofAuthority, proof: WriteProof) !WriteProof {
        try authority.authenticate(self, source, proof);
        const anchor = authority.now();
        if (anchor.wall_ms < proof.anchor.wall_ms or anchor.mono_ms < proof.anchor.mono_ms) return error.ImmutableLogProofExpired;
        const cp = try self.checkpoint(a);
        if (cp) |floor| {
            if (floor.state.revision > source.revision) return error.ImmutableLogProofRequiresRecovery;
            if (floor.state.revision == source.revision and !same(floor.state, source)) return error.InvalidImmutableLog;
        }
        return authority.issue(self, source, anchor);
    }
    /// A fresh checkpoint already authenticates its accepted source state.
    /// Under the bounded namespace contract, follow the exact next-slot chain
    /// without a redundant SEALED-marker probe. Never return an intermediate
    /// root as a completed data observation.
    pub fn observeFromCheckpointCertified(self: Log, a: A, authority: ProofAuthority) !CertifiedRecovery {
        if (self.observation_metrics) |metrics| metrics.cold_starts += 1;
        const anchor = authority.now();
        const cp = (try self.checkpoint(a)) orelse return error.ImmutableLogProofRequiresRecovery;
        const proof = try authority.issue(self, cp.state, anchor);
        return self.observeCertifiedMode(a, cp.state, authority, proof, true, null, anchor);
    }
    /// Capture both independent proof objects in one native I/O wave. The
    /// certificate anchor precedes the batch; late responses cannot reset it.
    pub fn refreshAndObserveCertified(self: Log, a: A, source: State, authority: ProofAuthority, proof: WriteProof, follow_owners: bool) !CertifiedRecovery {
        try authority.authenticate(self, source, proof);
        const anchor = authority.now();
        if (anchor.wall_ms < proof.anchor.wall_ms or anchor.mono_ms < proof.anchor.mono_ms) return error.ImmutableLogProofExpired;
        try authority.current(anchor);
        const checkpoint_key = try std.fmt.allocPrint(a, "{s}/CHECKPOINT", .{self.prefix});
        defer a.free(checkpoint_key);
        const next_key = try self.key(a, source.epoch, source.next);
        defer a.free(next_key);
        var client = self.client;
        client.allocator = a;
        const results = try client.getObjects(self.bucket, &.{
            .{ .key = checkpoint_key, .options = .{ .skip_metadata_probe = true, .max_response_bytes = 16384, .cancellation = self.cancellation } },
            .{ .key = next_key, .options = .{ .skip_metadata_probe = true, .max_response_bytes = 16384, .cancellation = self.cancellation } },
        });
        defer {
            for (results) |*result| if (result.*) |*value| value.deinit(a);
            a.free(results);
        }
        if (results[0]) |value| {
            const parsed = try std.json.parseFromSlice(Checkpoint, a, value.body, .{});
            defer parsed.deinit();
            const cp = parsed.value;
            if (cp.format != 1) return error.InvalidImmutableLog;
            try validate(cp.state);
            if (cp.state.revision > source.revision) {
                if (self.observation_metrics) |metrics| metrics.checkpoint_rebases += 1;
                // This freshly read floor authenticates a newer accepted root.
                // The old cursor's speculative next slot cannot validate that
                // root; observe its own next slot under the original anchor.
                const rebased = try authority.issue(self, cp.state, anchor);
                return self.observeCertifiedMode(a, cp.state, authority, rebased, follow_owners, null, anchor);
            }
            if (cp.state.revision == source.revision and !same(cp.state, source)) return error.InvalidImmutableLog;
        } else {
            const required = try std.fmt.allocPrint(a, "{s}/CHECKPOINT_REQUIRED", .{self.prefix});
            defer a.free(required);
            if (try self.read(a, required)) |_| return error.ImmutableLogCheckpointMissing;
        }
        const renewed = try authority.issue(self, source, anchor);
        return self.observeCertifiedMode(a, source, authority, renewed, follow_owners, &results[1], anchor);
    }
    /// The seal competes for this exact next slot. A valid bounded source
    /// therefore needs no cross-object floor probes. The collector grace keeps
    /// all competing slots alive until the proof expires. Check AFTER PUT too:
    /// a delayed upload may recreate a retired orphan, but can never be acked.
    pub fn appendCertified(self: Log, a: A, source: State, root: []const u8, authority: ProofAuthority, proof: WriteProof) !CertifiedRecovery {
        try authority.check(self, source, proof);
        if (!std.mem.eql(u8, &source.owner, &self.owner)) return error.ImmutableLogFenced;
        if (source.next >= epoch_commits) return error.ImmutableLogRotationRequired;
        const anchor = authority.now();
        const next = State{ .epoch = source.epoch, .owner = source.owner, .next = source.next + 1, .revision = try std.math.add(u64, source.revision, 1), .root = root };
        try validate(next);
        const bytes = try std.json.Stringify.valueAlloc(a, Frame{ .kind = .commit, .epoch = source.epoch, .slot = source.next, .parent_revision = source.revision, .state = next }, .{});
        defer a.free(bytes);
        if (bytes.len > 16384) return error.InvalidImmutableLog;
        const k = try self.key(a, source.epoch, source.next);
        defer a.free(k);
        var client = self.client;
        client.allocator = a;
        // Identical pre-existing bytes are NOT a fresh acceptance. An ambiguous
        // acknowledgement must recover; it must not renew an old certificate.
        var receipt = try client.putObject(self.bucket, k, bytes, .{ .if_none_match = true, .content_type = "application/json", .cancellation = self.cancellation });
        defer receipt.deinit(a);
        try authority.check(self, source, proof);
        return .{ .recovery = .{ .state = next }, .proof = try authority.issue(self, next, anchor) };
    }
    /// One fresh next-slot probe suffices within a protected epoch: every seal
    /// is first linearized in that same slot, before its recovery marker.
    pub fn observeCertified(self: Log, result_alloc: A, source: State, authority: ProofAuthority, proof: WriteProof, follow_owners: bool) !CertifiedRecovery {
        return self.observeCertifiedMode(result_alloc, source, authority, proof, follow_owners, null, authority.now());
    }
    const ObservationBatch = struct {
        arena: std.heap.ArenaAllocator,
        results: []?types.GetResult = &.{},
        epoch: [16]u8 = @splat(0),
        first: u64 = 0,

        fn clear(self: *ObservationBatch) void {
            const a = self.arena.allocator();
            for (self.results) |*result| if (result.*) |*value| value.deinit(a);
            self.results = &.{};
            _ = self.arena.reset(.retain_capacity);
        }
        fn load(self: *ObservationBatch, log: Log, state: State, remaining: usize) !void {
            self.clear();
            const a = self.arena.allocator();
            const count = @min(@as(usize, 8), remaining, @as(usize, @intCast(epoch_commits - state.next)));
            var requests: [8]types.GetObjectRequest = undefined;
            for (requests[0..count], 0..) |*request, offset| request.* = .{
                .key = try log.key(a, state.epoch, state.next + offset),
                .options = .{ .skip_metadata_probe = true, .max_response_bytes = 16384, .cancellation = log.cancellation },
            };
            var client = log.client;
            client.allocator = a;
            self.results = try client.getObjects(log.bucket, requests[0..count]);
            if (log.observation_metrics) |metrics| {
                metrics.batch_waves += 1;
                metrics.prefetched_slots += @intCast(count);
            }
            for (self.results) |result| if (result) |value| {
                if (value.body.len > 16384) return error.InvalidImmutableLog;
            };
            self.epoch = state.epoch;
            self.first = state.next;
        }
        fn contains(self: ObservationBatch, state: State) bool {
            return std.mem.eql(u8, &self.epoch, &state.epoch) and state.next >= self.first and state.next - self.first < self.results.len;
        }
    };
    fn observeCertifiedMode(self: Log, result_alloc: A, source: State, authority: ProofAuthority, proof: WriteProof, follow_owners: bool, first: ?*const ?types.GetResult, anchor: ProofTime) !CertifiedRecovery {
        try authority.check(self, source, proof);
        var arena = std.heap.ArenaAllocator.init(result_alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var batch = ObservationBatch{ .arena = std.heap.ArenaAllocator.init(result_alloc) };
        defer {
            batch.clear();
            batch.arena.deinit();
        }
        var consecutive: usize = 0;
        var root_buffer: [max_root_bytes]u8 = undefined;
        var state = source;
        for (0..max_steps) |step| {
            std.mem.copyForwards(u8, root_buffer[0..state.root.len], state.root);
            state.root = root_buffer[0..state.root.len];
            _ = arena.reset(.retain_capacity);
            try authority.check(self, source, proof);
            if (!follow_owners and !std.mem.eql(u8, &state.owner, &self.owner)) return error.ImmutableLogFenced;
            // Keep idle and one-append observations on the single-probe path.
            // Once a tail is established, overlap at most eight independent
            // addresses. Serial-only providers retain their existing path.
            if (self.read_batch and self.client.vtable.get_objects != null and consecutive >= 2 and !batch.contains(state) and state.next < epoch_commits)
                try batch.load(self, state, max_steps - step);
            const stored = if (step == 0 and first != null) first.?.* else if (batch.contains(state)) blk: {
                if (batch.results[@intCast(state.next - batch.first)]) |value| break :blk value;
                // A prefetched absence may precede acceptance of its parent.
                // Re-probe it AFTER consuming the accepted contiguous prefix.
                batch.clear();
                if (self.observation_metrics) |metrics| metrics.absence_rechecks += 1;
                break :blk try self.read(a, try self.key(a, state.epoch, state.next));
            } else try self.read(a, try self.key(a, state.epoch, state.next));
            try authority.check(self, source, proof);
            const item = stored orelse {
                state.root = try result_alloc.dupe(u8, state.root);
                return .{ .recovery = .{ .state = state }, .proof = try authority.issue(self, state, anchor) };
            };
            const next = try frame(a, item.body, state.epoch, state.next);
            if (next.parent_revision != state.revision) return error.InvalidImmutableLog;
            if (next.kind == .seal) {
                if (self.observation_metrics) |metrics| metrics.seals_followed += 1;
                if (!std.mem.eql(u8, next.state.root, state.root)) return error.InvalidImmutableLog;
                consecutive = 0;
            } else if (!std.mem.eql(u8, &next.state.owner, &state.owner)) return error.InvalidImmutableLog;
            if (next.kind == .commit) {
                consecutive += 1;
                if (self.observation_metrics) |metrics| metrics.slots_followed += 1;
            }
            state = next.state;
        }
        if (!follow_owners and !std.mem.eql(u8, &state.owner, &self.owner)) return error.ImmutableLogFenced;
        state.root = try result_alloc.dupe(u8, state.root);
        // Intermediate roots may already be retired. Carry the old deadline;
        // only the fresh absence of a next slot permits a new observation lease.
        return .{ .recovery = .{ .state = state, .has_more = true }, .proof = try authority.issue(self, state, proof.anchor) };
    }
    pub fn retirable(self: Log, a: A, keys: []const []const u8, before: u64) ![]const []const u8 {
        if (keys.len > 32) return error.ImmutableLogBatchExceeded;
        const cp = (try self.checkpoint(a)) orelse return error.ImmutableLogCheckpointMissing;
        if (cp.state.revision < before) return error.InvalidImmutableLog;
        const prefix = try std.fmt.allocPrint(a, "{s}/epochs/", .{self.prefix});
        var result = std.ArrayList([]const u8).empty;
        for (keys) |k| {
            if (!std.mem.startsWith(u8, k, prefix)) continue;
            const suffix = k[prefix.len..];
            if (suffix.len < 34 or suffix[32] != '/') continue;
            var epoch: [16]u8 = undefined;
            _ = std.fmt.hexToBytes(&epoch, suffix[0..32]) catch continue;
            const name = suffix[33..];
            if (std.mem.eql(u8, name, "START")) {
                const stored = (try self.read(a, k)) orelse continue;
                const start_state = (try std.json.parseFromSlice(State, a, stored.body, .{})).value;
                try validate(start_state);
                if (!std.mem.eql(u8, &start_state.epoch, &epoch) or start_state.next != 0) return error.InvalidImmutableLog;
                if (start_state.revision < before) try result.append(a, k);
                continue;
            }
            const slot: ?u64 = if (std.mem.eql(u8, name, "SEALED")) null else blk: {
                if (name.len != 25 or !std.mem.endsWith(u8, name, ".json")) continue;
                break :blk std.fmt.parseInt(u64, name[0..20], 10) catch continue;
            };
            const stored = (try self.read(a, k)) orelse continue;
            const item = try frame(a, stored.body, epoch, slot);
            if (slot == null and item.kind != .seal) return error.InvalidImmutableLog;
            // The post-PUT checkpoint-floor check prevents old in-flight
            // writers acknowledging a recreated, pruned slot (the ABA case).
            if (item.state.revision < before) try result.append(a, k);
        }
        return result.toOwnedSlice(a);
    }
    fn requireCheckpoint(self: Log, a: A) !void {
        const k = try std.fmt.allocPrint(a, "{s}/CHECKPOINT_REQUIRED", .{self.prefix});
        if (try self.read(a, k)) |value| {
            if (!std.mem.eql(u8, value.body, "1")) return error.InvalidImmutableLog;
        } else try self.immutable(a, k, "1");
    }
    /// At most one successful checkpoint overwrite per second. A forced
    /// checkpoint reports false until its rate window opens; callers retry.
    pub fn writeCheckpoint(self: Log, a: A, state: State, force: bool) !bool {
        try self.unfenced(a, state);
        const k = try std.fmt.allocPrint(a, "{s}/CHECKPOINT", .{self.prefix});
        const previous = try self.read(a, k);
        const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
        if (previous) |value| {
            const old = (try std.json.parseFromSlice(Checkpoint, a, value.body, .{})).value;
            if (old.state.revision >= state.revision) {
                if (force) try self.requireCheckpoint(a);
                return true;
            }
            if (now < old.written_ms +| 1000) return false;
        } else if (!force and state.revision < self.checkpoint_commits) return false;
        const bytes = try std.json.Stringify.valueAlloc(a, Checkpoint{ .written_ms = now, .state = state }, .{});
        if (bytes.len > 16384) return error.InvalidImmutableLog;
        var client = self.client;
        client.allocator = a;
        var receipt = client.compareAndPut(self.bucket, k, bytes, if (previous) |value| value.metadata.writeRevision() orelse return error.ConditionalReadUnsupported else null, .{ .content_type = "application/json", .cancellation = self.cancellation }) catch |err| switch (err) {
            error.PreconditionFailed, error.ObjectStoreRateLimited => return false,
            else => return err,
        };
        receipt.deinit(a);
        if (force) try self.requireCheckpoint(a);
        return true;
    }
};

test "immutable log fences old owners and recovers rotated commits from a checkpoint" {
    const testing = std.testing;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(testing.allocator);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("test");
    var log = Log{ .client = client, .bucket = "test", .prefix = "db", .owner = @splat(1), .io = testing.io };
    var current = (try log.recover(a, null, true)).state;
    const stale = current;
    for (0..130) |i| {
        current = try log.append(a, current, try std.fmt.allocPrint(a, "root-{d}", .{i}));
    }
    var recovered = try log.recover(a, null, true);
    while (recovered.has_more) recovered = try log.recover(a, recovered.state, true);
    try testing.expectEqualStrings("root-129", recovered.state.root);
    log.owner = @splat(2);
    const winner = (try log.recover(a, recovered.state, true)).state;
    try testing.expect(std.mem.eql(u8, &winner.owner, &log.owner));
    log.owner = @splat(1);
    try testing.expectError(error.ImmutableLogFenced, log.append(a, stale, "stale"));
    try testing.expectError(error.ImmutableLogFenced, log.recover(a, current, false));
}

test "immutable log rejects forged cached cursors before acknowledging roots" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(std.testing.allocator);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("fixture");
    const log = Log{ .client = client, .bucket = "fixture", .prefix = "db", .owner = @splat(1), .io = std.testing.io };
    var state = (try log.recover(a, null, true)).state;
    state = try log.append(a, state, "real-root");
    var forged = state;
    forged.root = "forged-root";
    try std.testing.expectError(error.InvalidImmutableLog, log.recover(a, forged, false));
    try std.testing.expectError(error.InvalidImmutableLog, log.append(a, forged, "unreachable"));
    forged = state;
    forged.epoch = @splat(7);
    forged.next = 0;
    try std.testing.expectError(error.InvalidImmutableLog, log.append(a, forged, "unreachable"));
}

test "immutable log checkpoints short recovery tails without making hint success part of commit" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(std.testing.allocator);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("fixture");
    const log = Log{ .client = client, .bucket = "fixture", .prefix = "db", .owner = @splat(1), .io = std.testing.io, .checkpoint_commits = 8 };
    var state = (try log.recover(a, null, true)).state;
    // Open the rate window without sleeping or depending on the test clock.
    var receipt = try client.putObject("fixture", "db/CHECKPOINT", try std.json.Stringify.valueAlloc(a, Checkpoint{ .written_ms = 0, .state = state }, .{}), .{});
    receipt.deinit(std.testing.allocator);
    for (0..8) |i| state = try log.append(a, state, try std.fmt.allocPrint(a, "root-{d}", .{i}));
    try std.testing.expectEqual(state.revision, (try log.checkpoint(a)).?.state.revision);
    state = try log.append(a, state, "root-8");
    memory.resetOperationCount();
    const recovered = try log.recover(a, null, false);
    try std.testing.expectEqualStrings("root-8", recovered.state.root);
    try std.testing.expect(memory.operationCount() <= 8);
    // A throttled hint still returns the immutable durable commit's result.
    const cp = (try log.checkpoint(a)).?;
    receipt = try client.putObject("fixture", "db/CHECKPOINT", try std.json.Stringify.valueAlloc(a, Checkpoint{ .written_ms = std.math.maxInt(u64), .state = cp.state }, .{}), .{});
    receipt.deinit(std.testing.allocator);
    for (9..17) |i| state = try log.append(a, state, try std.fmt.allocPrint(a, "root-{d}", .{i}));
    try std.testing.expectEqual(cp.state.revision, (try log.checkpoint(a)).?.state.revision);
    try std.testing.expectEqualStrings("root-16", (try log.recover(a, null, false)).state.root);
}

test "immutable log cold observers authenticate sealed owners without writing or repairing hints" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    const first = Log{ .client = client, .bucket = "db", .prefix = "cold", .owner = @splat(1), .io = std.testing.io };
    var state = (try first.recover(m, null, true)).state;
    state = try first.append(m, state, "committed");
    _ = try first.writeCheckpoint(m, state, true);
    const Hook = struct {
        fn noCheckpoint(raw: *anyopaque, alloc: A, bucket: []const u8, key_: []const u8, body: []const u8, opts: types.PutOptions) !types.PutResult {
            if (std.mem.endsWith(u8, key_, "/CHECKPOINT")) return error.InjectedHintFailure;
            const backend: *@import("memory.zig").MemoryClient = @ptrCast(@alignCast(raw));
            var base = backend.client();
            base.allocator = alloc;
            return base.putObject(bucket, key_, body, opts);
        }
        fn noWrites(_: *anyopaque, _: A, _: []const u8, _: []const u8, _: []const u8, _: types.PutOptions) !types.PutResult {
            return error.ReadOnlyObserverWrote;
        }
    };
    var owner_table = client.vtable.*;
    owner_table.put_object = Hook.noCheckpoint;
    var owner_client = client;
    owner_client.vtable = &owner_table;
    const second = Log{ .client = owner_client, .bucket = "db", .prefix = "cold", .owner = @splat(2), .io = std.testing.io };
    const taken = (try second.recover(m, state, true)).state;
    const marker = try std.fmt.allocPrint(m, "cold/epochs/{s}/SEALED", .{std.fmt.bytesToHex(state.epoch, .lower)});
    const start = try std.fmt.allocPrint(m, "cold/epochs/{s}/START", .{std.fmt.bytesToHex(taken.epoch, .lower)});
    try client.deleteObject("db", marker, .{});
    try client.deleteObject("db", start, .{});
    var read_table = client.vtable.*;
    read_table.put_object = Hook.noWrites;
    read_table.put_objects = null;
    var read_client = client;
    read_client.vtable = &read_table;
    const observer = Log{ .client = read_client, .bucket = "db", .prefix = "cold", .owner = @splat(9), .io = std.testing.io };
    const observed = try observer.observeCommitted(m, null);
    try std.testing.expect(!observed.has_more);
    try std.testing.expectEqualStrings("committed", observed.state.root);
    try std.testing.expectEqual(taken.owner, observed.state.owner);
    try std.testing.expectError(error.FileNotFound, client.statObject("db", marker));
    try std.testing.expectError(error.FileNotFound, client.statObject("db", start));
    var forged = state;
    forged.root = "forged";
    try std.testing.expectError(error.InvalidImmutableLog, observer.observeCommitted(m, forged));
    try std.testing.expectError(error.ImmutableLogFenced, first.observe(m, state));
    const repaired = (try second.recover(m, null, true)).state;
    try std.testing.expectEqual(taken.revision, repaired.revision);
    _ = try second.append(m, repaired, "writer-still-owned");
}

test "immutable log partial epoch announcement batches recover from the accepted seal" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    const Hook = struct {
        fn partial(raw: *anyopaque, alloc: A, bucket: []const u8, items: []const types.PutObjectRequest) ![]types.PutResult {
            const backend: *@import("memory.zig").MemoryClient = @ptrCast(@alignCast(raw));
            var base = backend.client();
            base.allocator = alloc;
            var first = try base.putObject(bucket, items[0].key, items[0].body, items[0].options);
            first.deinit(alloc);
            return error.InjectedAnnouncementFailure;
        }
    };
    var table = client.vtable.*;
    table.put_objects = Hook.partial;
    client.vtable = &table;
    const log = Log{ .client = client, .bucket = "db", .prefix = "partial", .owner = @splat(1), .io = std.testing.io };
    try std.testing.expectError(error.InjectedAnnouncementFailure, log.recover(m, null, true));
    const observed = try log.observeCommitted(m, null);
    try std.testing.expectEqualStrings("", observed.state.root);
    try std.testing.expectEqual(log.owner, observed.state.owner);
    const repaired = (try log.recover(m, null, true)).state;
    try std.testing.expectEqual(observed.state.revision, repaired.revision);
    _ = try log.append(m, repaired, "recovered");
    try std.testing.expectEqualStrings("recovered", (try log.observeCommitted(m, null)).state.root);
}

test "immutable log recovery bounds scratch memory across a full tail of large roots" {
    const a = std.testing.allocator;
    var writer_arena = std.heap.ArenaAllocator.init(a);
    defer writer_arena.deinit();
    const m = writer_arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    const log = Log{ .client = client, .bucket = "db", .prefix = "bounded", .owner = @splat(1), .io = std.testing.io };
    var state = (try log.recover(m, null, true)).state;
    // Keep the initial checkpoint while simulating a long-lived writer.
    var root: [max_root_bytes]u8 = @splat('r');
    root[0] = '"';
    for (0..63) |_| state = try log.append(m, state, &root);
    var buffer: [256 * 1024]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&buffer);
    const observed = try log.observeCommitted(bounded.allocator(), null);
    try std.testing.expect(!observed.has_more);
    try std.testing.expectEqual(state.revision, observed.state.revision);
    try std.testing.expectEqualStrings(state.root, observed.state.root);
    // Output owns its root after scratch teardown, including escaped JSON.
    try std.testing.expectEqual(@as(u8, '"'), observed.state.root[0]);
    try std.testing.expect(bounded.end_index < buffer.len);
}

const ProofTestBackend = struct {
    memory: *@import("memory.zig").MemoryClient,
    time: ProofTime = .{ .wall_ms = 100000, .mono_ms = 1000 },
    delay_and_retire: ?Log = null,
    lost_ack: bool = false,
    read_batch_delay_ms: u64 = 0,
    scalar_reads: usize = 0,
    batch_reads: usize = 0,
    maximum_batch: usize = 0,
    append_after_batch: ?struct { log: Log, state: State, authority: ProofAuthority, proof: WriteProof } = null,
    fail_batch: bool = false,
    fn clock(raw: *const anyopaque) ProofTime {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        return self.time;
    }
    fn get(raw: *anyopaque, a: A, bucket: []const u8, name: []const u8, opts: types.GetOptions) !types.GetResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var base = self.memory.client();
        base.allocator = a;
        self.scalar_reads += 1;
        return base.getObject(bucket, name, opts);
    }
    fn put(raw: *anyopaque, a: A, bucket: []const u8, name: []const u8, body: []const u8, opts: types.PutOptions) !types.PutResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var base = self.memory.client();
        base.allocator = a;
        if (self.delay_and_retire) |winner| {
            self.delay_and_retire = null;
            const accepted = (try winner.recover(a, null, true)).state;
            // Advance the fixture's checkpoint write clock without sleeping.
            // Collection is legal only after its durable floor covers the seal.
            var old = (try winner.checkpoint(a)).?;
            old.written_ms = 0;
            const checkpoint_key = try std.fmt.allocPrint(a, "{s}/CHECKPOINT", .{winner.prefix});
            const checkpoint_body = try std.json.Stringify.valueAlloc(a, old, .{});
            var reset = try base.putObject(bucket, checkpoint_key, checkpoint_body, .{});
            reset.deinit(a);
            if (!try winner.writeCheckpoint(a, accepted, true)) return error.FixtureCheckpointMissing;
            try base.deleteObject(bucket, name, .{});
            const epoch_prefix = name[0..std.mem.lastIndexOfScalar(u8, name, '/').?];
            try base.deleteObject(bucket, try std.fmt.allocPrint(a, "{s}/SEALED", .{epoch_prefix}), .{});
            try base.deleteObject(bucket, try std.fmt.allocPrint(a, "{s}/START", .{epoch_prefix}), .{});
            self.time.wall_ms += 60001;
            self.time.mono_ms += 60001;
        }
        var result = try base.putObject(bucket, name, body, opts);
        if (self.lost_ack) {
            self.lost_ack = false;
            result.deinit(a);
            return error.InjectedLostAck;
        }
        return result;
    }
    fn getMany(raw: *anyopaque, a: A, bucket: []const u8, items: []const types.GetObjectRequest) ![]?types.GetResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var base = self.memory.client();
        base.allocator = a;
        self.batch_reads += 1;
        self.maximum_batch = @max(self.maximum_batch, items.len);
        const results = try base.getObjects(bucket, items);
        errdefer {
            for (results) |*result| if (result.*) |*value| value.deinit(a);
            a.free(results);
        }
        if (self.append_after_batch) |late| {
            self.append_after_batch = null;
            _ = try late.log.appendCertified(a, late.state, "late-accepted", late.authority, late.proof);
        }
        if (self.fail_batch) return error.InjectedBatchFailure;
        self.time.wall_ms += self.read_batch_delay_ms;
        self.time.mono_ms += self.read_batch_delay_ms;
        return results;
    }
    fn client(self: *@This(), table: *Client.VTable) Client {
        const base = self.memory.client();
        table.* = base.vtable.*;
        table.get_object = get;
        table.get_objects = if (self.read_batch_delay_ms > 0) getMany else null;
        table.put_object = put;
        return .{ .allocator = base.allocator, .ptr = self, .vtable = table };
    }
};

test "immutable log bounded certificates use one fresh slot write and one fresh observation probe" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var base = memory.client();
    try base.makeBucket("db");
    var backend = ProofTestBackend{ .memory = &memory };
    var table: Client.VTable = undefined;
    const log = Log{ .client = backend.client(&table), .bucket = "db", .prefix = "proof", .owner = @splat(1), .io = std.testing.io };
    const authority = ProofAuthority{ .key = @splat(42), .scope = @splat(9), .clock_ptr = &backend, .clock_fn = ProofTestBackend.clock };
    var state = (try log.recover(m, null, true)).state;
    var proof = try authority.issue(log, state, authority.now());
    for (0..64) |i| {
        memory.resetOperationCount();
        const root = try std.fmt.allocPrint(m, "root-{d}", .{i});
        const next = try log.appendCertified(m, state, root, authority, proof);
        state = next.recovery.state;
        proof = next.proof;
        try std.testing.expectEqual(@as(u64, 1), memory.operationCount());
    }
    memory.resetOperationCount();
    const observed = try log.observeCertified(m, state, authority, proof, true);
    try std.testing.expectEqual(@as(u64, 1), memory.operationCount());
    try std.testing.expectEqualStrings(state.root, observed.recovery.state.root);
    try std.testing.expectError(error.ImmutableLogRotationRequired, log.appendCertified(m, state, "overflow", authority, proof));
    const rotated = (try log.recover(m, state, false)).state;
    try std.testing.expectEqual(@as(u64, 0), rotated.next);
    const followed = try log.observeCertified(m, state, authority, proof, true);
    try std.testing.expectEqual(rotated.revision, followed.recovery.state.revision);
}

test "immutable log bounded certificates bind source clocks scope and ownership without authority IO" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var base = memory.client();
    try base.makeBucket("db");
    var backend = ProofTestBackend{ .memory = &memory };
    var table: Client.VTable = undefined;
    const log = Log{ .client = backend.client(&table), .bucket = "db", .prefix = "proof", .owner = @splat(1), .io = std.testing.io };
    const authority = ProofAuthority{ .key = @splat(42), .scope = @splat(9), .clock_ptr = &backend, .clock_fn = ProofTestBackend.clock };
    const state = (try log.recover(m, null, true)).state;
    const proof = try authority.issue(log, state, authority.now());
    var forged = state;
    forged.root = "forged";
    memory.resetOperationCount();
    try std.testing.expectError(error.InvalidImmutableLogProof, log.appendCertified(m, forged, "new", authority, proof));
    var other = authority;
    other.scope = @splat(10);
    try std.testing.expectError(error.InvalidImmutableLogProof, log.appendCertified(m, state, "new", other, proof));
    backend.time.wall_ms -= 1;
    try std.testing.expectError(error.ImmutableLogProofExpired, log.appendCertified(m, state, "new", authority, proof));
    backend.time.wall_ms = 100000;
    backend.time.mono_ms += 30000;
    try std.testing.expectError(error.ImmutableLogProofExpired, log.appendCertified(m, state, "new", authority, proof));
    try std.testing.expectEqual(@as(u64, 0), memory.operationCount());
}

test "immutable log bounded certificate cannot acknowledge a delayed recreated slot after takeover and collection" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var base = memory.client();
    try base.makeBucket("db");
    var backend = ProofTestBackend{ .memory = &memory };
    var table: Client.VTable = undefined;
    const log = Log{ .client = backend.client(&table), .bucket = "db", .prefix = "proof", .owner = @splat(1), .io = std.testing.io };
    const authority = ProofAuthority{ .key = @splat(42), .scope = @splat(9), .clock_ptr = &backend, .clock_fn = ProofTestBackend.clock };
    const state = (try log.recover(m, null, true)).state;
    const proof = try authority.issue(log, state, authority.now());
    var winner = log;
    winner.owner = @splat(2);
    backend.delay_and_retire = winner;
    try std.testing.expectError(error.ImmutableLogProofExpired, log.appendCertified(m, state, "orphan", authority, proof));
    const recovered = try winner.recover(m, null, false);
    try std.testing.expectEqualStrings("", recovered.state.root);
    try std.testing.expectEqualSlices(u8, &winner.owner, &recovered.state.owner);
}

test "immutable log bounded certificates reject ambiguous replay and preserve accepted receipts through recovery" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var base = memory.client();
    try base.makeBucket("db");
    var backend = ProofTestBackend{ .memory = &memory };
    var table: Client.VTable = undefined;
    const log = Log{ .client = backend.client(&table), .bucket = "db", .prefix = "proof", .owner = @splat(1), .io = std.testing.io };
    const authority = ProofAuthority{ .key = @splat(42), .scope = @splat(9), .clock_ptr = &backend, .clock_fn = ProofTestBackend.clock };
    const state = (try log.recover(m, null, true)).state;
    const proof = try authority.issue(log, state, authority.now());
    backend.lost_ack = true;
    try std.testing.expectError(error.InjectedLostAck, log.appendCertified(m, state, "accepted", authority, proof));
    try std.testing.expectError(error.PreconditionFailed, log.appendCertified(m, state, "accepted", authority, proof));
    const recovered = try log.observeCertified(m, state, authority, proof, true);
    try std.testing.expectEqualStrings("accepted", recovered.recovery.state.root);
    var winner = log;
    winner.owner = @splat(2);
    _ = try winner.recover(m, null, true);
    try std.testing.expectError(error.PreconditionFailed, log.appendCertified(m, recovered.recovery.state, "stale", authority, recovered.proof));
}

test "immutable log bounded certificate idle refresh authenticates the current GC floor without replay" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var base = memory.client();
    try base.makeBucket("db");
    var backend = ProofTestBackend{ .memory = &memory };
    var table: Client.VTable = undefined;
    const log = Log{ .client = backend.client(&table), .bucket = "db", .prefix = "proof", .owner = @splat(1), .io = std.testing.io };
    const authority = ProofAuthority{ .key = @splat(42), .scope = @splat(9), .clock_ptr = &backend, .clock_fn = ProofTestBackend.clock };
    const state = (try log.recover(m, null, true)).state;
    const proof = try authority.issue(log, state, authority.now());
    backend.time.wall_ms += 30000;
    backend.time.mono_ms += 30000;
    memory.resetOperationCount();
    const renewed = try log.refreshCertificate(m, state, authority, proof);
    try std.testing.expectEqual(@as(u64, 1), memory.operationCount());
    const observed = try log.observeCertified(m, state, authority, renewed, true);
    try std.testing.expectEqual(@as(u64, 2), memory.operationCount());
    try std.testing.expectEqual(state.revision, observed.recovery.state.revision);
    backend.time.wall_ms = proof.anchor.wall_ms - 1;
    memory.resetOperationCount();
    try std.testing.expectError(error.ImmutableLogProofExpired, log.refreshCertificate(m, state, authority, proof));
    try std.testing.expectEqual(@as(u64, 0), memory.operationCount());
}

test "immutable log bounded batch revalidation anchors before storage and rejects late proof renewal" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var base = memory.client();
    try base.makeBucket("db");
    var backend = ProofTestBackend{ .memory = &memory, .read_batch_delay_ms = 30000 };
    var table: Client.VTable = undefined;
    const log = Log{ .client = backend.client(&table), .bucket = "db", .prefix = "proof", .owner = @splat(1), .io = std.testing.io };
    const authority = ProofAuthority{ .key = @splat(42), .scope = @splat(9), .clock_ptr = &backend, .clock_fn = ProofTestBackend.clock };
    const state = (try log.recover(m, null, true)).state;
    const proof = try authority.issue(log, state, authority.now());
    backend.time.wall_ms += 30000;
    backend.time.mono_ms += 30000;
    memory.resetOperationCount();
    try std.testing.expectError(error.ImmutableLogProofExpired, log.refreshAndObserveCertified(m, state, authority, proof, true));
    try std.testing.expectEqual(@as(u64, 2), memory.operationCount());
    backend.read_batch_delay_ms = 1;
    const fresh = try log.refreshAndObserveCertified(m, state, authority, proof, true);
    try std.testing.expectEqual(state.revision, fresh.recovery.state.revision);
    try std.testing.expectEqual(backend.time.wall_ms - 1, fresh.proof.anchor.wall_ms);
}

test "immutable log bounded cold observation follows the checkpoint chain without ownership writes" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var base = memory.client();
    try base.makeBucket("db");
    var backend = ProofTestBackend{ .memory = &memory };
    var table: Client.VTable = undefined;
    const log = Log{ .client = backend.client(&table), .bucket = "db", .prefix = "cold", .owner = @splat(1), .io = std.testing.io };
    const authority = ProofAuthority{ .key = @splat(42), .scope = @splat(9), .clock_ptr = &backend, .clock_fn = ProofTestBackend.clock };
    const initial = (try log.recover(m, null, true)).state;
    memory.resetOperationCount();
    const empty = try log.observeFromCheckpointCertified(m, authority);
    try std.testing.expectEqual(@as(u64, 2), memory.operationCount());
    try std.testing.expectEqual(initial.revision, empty.recovery.state.revision);
    var state = initial;
    for (0..63) |_| state = try log.append(m, state, "committed");
    var replacement = log;
    replacement.owner = @splat(2);
    state = try replacement.seal(m, state, replacement.owner);
    state = try replacement.append(m, state, "new owner");
    const partial = try log.observeFromCheckpointCertified(m, authority);
    try std.testing.expect(partial.recovery.has_more);
    try std.testing.expectEqual(@as(u64, 0), partial.recovery.state.next);
    const observed = try log.observeCertified(m, partial.recovery.state, authority, partial.proof, true);
    try std.testing.expect(!observed.recovery.has_more);
    try std.testing.expectEqual(state.revision, observed.recovery.state.revision);
    try std.testing.expectEqualStrings("new owner", observed.recovery.state.root);
    try std.testing.expectEqual(state.owner, observed.recovery.state.owner);
    try authority.check(log, observed.recovery.state, observed.proof);
}

test "immutable log batched catchup bounds waves and keeps idle and one append probes unchanged" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var base = memory.client();
    try base.makeBucket("db");
    var backend = ProofTestBackend{ .memory = &memory, .read_batch_delay_ms = 1 };
    var table: Client.VTable = undefined;
    const log = Log{ .client = backend.client(&table), .bucket = "db", .prefix = "catchup", .owner = @splat(1), .io = std.testing.io, .read_batch = true };
    const auth = ProofAuthority{ .key = @splat(42), .scope = @splat(9), .clock_ptr = &backend, .clock_fn = ProofTestBackend.clock };
    const initial = (try log.recover(m, null, true)).state;
    const proof = try auth.issue(log, initial, auth.now());
    backend.scalar_reads = 0;
    _ = try log.observeCertified(m, initial, auth, proof, true);
    try std.testing.expectEqual(@as(usize, 1), backend.scalar_reads);
    try std.testing.expectEqual(@as(usize, 0), backend.batch_reads);
    var accepted = try log.appendCertified(m, initial, "one", auth, proof);
    backend.scalar_reads = 0;
    _ = try log.observeCertified(m, initial, auth, proof, true);
    try std.testing.expectEqual(@as(usize, 2), backend.scalar_reads);
    try std.testing.expectEqual(@as(usize, 0), backend.batch_reads);
    for (1..32) |_| accepted = try log.appendCertified(m, accepted.recovery.state, "many", auth, accepted.proof);
    backend.scalar_reads = 0;
    const observed = try log.observeCertified(m, initial, auth, proof, true);
    try std.testing.expectEqual(accepted.recovery.state.revision, observed.recovery.state.revision);
    try std.testing.expect(!observed.recovery.has_more);
    try std.testing.expect(backend.scalar_reads + backend.batch_reads <= 7);
    try std.testing.expectEqual(@as(usize, 8), backend.maximum_batch);
    var serial = log;
    serial.read_batch = false;
    backend.scalar_reads = 0;
    const before = backend.batch_reads;
    const original = try serial.observeCertified(m, initial, auth, proof, true);
    try std.testing.expectEqual(@as(usize, 33), backend.scalar_reads);
    try std.testing.expectEqual(before, backend.batch_reads);
    try std.testing.expectEqualStrings(original.recovery.state.root, observed.recovery.state.root);
}

test "immutable log batched absence is rechecked after the accepted prefix and unused read errors survive" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var base = memory.client();
    try base.makeBucket("db");
    var backend = ProofTestBackend{ .memory = &memory, .read_batch_delay_ms = 1 };
    var table: Client.VTable = undefined;
    const log = Log{ .client = backend.client(&table), .bucket = "db", .prefix = "batch-absence", .owner = @splat(1), .io = std.testing.io, .read_batch = true };
    const auth = ProofAuthority{ .key = @splat(42), .scope = @splat(9), .clock_ptr = &backend, .clock_fn = ProofTestBackend.clock };
    const initial = (try log.recover(m, null, true)).state;
    const proof = try auth.issue(log, initial, auth.now());
    var accepted = CertifiedRecovery{ .recovery = .{ .state = initial }, .proof = proof };
    for (0..8) |_| accepted = try log.appendCertified(m, accepted.recovery.state, "prefix", auth, accepted.proof);
    backend.append_after_batch = .{ .log = log, .state = accepted.recovery.state, .authority = auth, .proof = accepted.proof };
    const observed = try log.observeCertified(m, initial, auth, proof, true);
    try std.testing.expectEqualStrings("late-accepted", observed.recovery.state.root);
    try std.testing.expectEqual(accepted.recovery.state.revision + 1, observed.recovery.state.revision);
    backend.fail_batch = true;
    try std.testing.expectError(error.InjectedBatchFailure, log.observeCertified(m, initial, auth, proof, true));
}

test "immutable log idle batch rebases on a newer durable floor without legacy recovery" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var base = memory.client();
    try base.makeBucket("db");
    var backend = ProofTestBackend{ .memory = &memory, .read_batch_delay_ms = 1 };
    var table: Client.VTable = undefined;
    const log = Log{ .client = backend.client(&table), .bucket = "db", .prefix = "rebase", .owner = @splat(1), .io = std.testing.io, .read_batch = true };
    const auth = ProofAuthority{ .key = @splat(42), .scope = @splat(9), .clock_ptr = &backend, .clock_fn = ProofTestBackend.clock };
    const initial = (try log.recover(m, null, true)).state;
    const proof = try auth.issue(log, initial, auth.now());
    const accepted = try log.appendCertified(m, initial, "advanced", auth, proof);
    // The existing checkpoint rate limiter stays in force; alter only the
    // fixture's previous publication timestamp to make this update eligible.
    var cp = (try log.checkpoint(m)).?;
    cp.written_ms = 0;
    var receipt = try base.putObject("db", "rebase/CHECKPOINT", try std.json.Stringify.valueAlloc(m, cp, .{}), .{});
    receipt.deinit(a);
    try std.testing.expect(try log.writeCheckpoint(m, accepted.recovery.state, true));
    backend.time.wall_ms += 31000;
    backend.time.mono_ms += 31000;
    backend.scalar_reads = 0;
    backend.read_batch_delay_ms = 29000;
    const original_anchor = auth.now();
    const observed = try log.refreshAndObserveCertified(m, initial, auth, proof, true);
    try std.testing.expectEqualDeep(original_anchor, observed.proof.anchor);
    try std.testing.expectEqual(accepted.recovery.state.revision, observed.recovery.state.revision);
    try std.testing.expectEqual(@as(usize, 1), backend.scalar_reads);
    try std.testing.expectEqual(@as(usize, 1), backend.batch_reads);
}

test "immutable log batched observation follows accepted seals and never grants a replaced writer authority" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var base = memory.client();
    try base.makeBucket("db");
    var backend = ProofTestBackend{ .memory = &memory, .read_batch_delay_ms = 1 };
    var table: Client.VTable = undefined;
    const log = Log{ .client = backend.client(&table), .bucket = "db", .prefix = "batch-seal", .owner = @splat(1), .io = std.testing.io, .read_batch = true };
    const auth = ProofAuthority{ .key = @splat(42), .scope = @splat(9), .clock_ptr = &backend, .clock_fn = ProofTestBackend.clock };
    const initial = (try log.recover(m, null, true)).state;
    const proof = try auth.issue(log, initial, auth.now());
    var accepted = CertifiedRecovery{ .recovery = .{ .state = initial }, .proof = proof };
    for (0..8) |_| accepted = try log.appendCertified(m, accepted.recovery.state, "old", auth, accepted.proof);
    var winner = log;
    winner.owner = @splat(2);
    const claimed = (try winner.recover(m, null, true)).state;
    _ = try winner.appendCertified(m, claimed, "winner", auth, try auth.issue(winner, claimed, auth.now()));
    const observed = try log.observeCertified(m, initial, auth, proof, true);
    try std.testing.expectEqualStrings("winner", observed.recovery.state.root);
    try std.testing.expectEqualDeep(winner.owner, observed.recovery.state.owner);
    try std.testing.expectError(error.ImmutableLogFenced, log.observeCertified(m, initial, auth, proof, false));
}

test "immutable log batched scratch and decoded results release every allocation failure" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const m = arena.allocator();
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var base = memory.client();
    try base.makeBucket("db");
    var backend = ProofTestBackend{ .memory = &memory, .read_batch_delay_ms = 1 };
    var table: Client.VTable = undefined;
    const log = Log{ .client = backend.client(&table), .bucket = "db", .prefix = "batch-failure", .owner = @splat(1), .io = std.testing.io, .read_batch = true };
    const auth = ProofAuthority{ .key = @splat(42), .scope = @splat(9), .clock_ptr = &backend, .clock_fn = ProofTestBackend.clock };
    const initial = (try log.recover(m, null, true)).state;
    const proof = try auth.issue(log, initial, auth.now());
    var accepted = CertifiedRecovery{ .recovery = .{ .state = initial }, .proof = proof };
    for (0..8) |_| accepted = try log.appendCertified(m, accepted.recovery.state, "accepted", auth, accepted.proof);
    const Fixture = struct {
        fn observe(failing: A, l: Log, authority: ProofAuthority, source: State, certificate: WriteProof, b: *ProofTestBackend) !void {
            b.time = certificate.anchor;
            const value = try l.observeCertified(failing, source, authority, certificate, true);
            defer failing.free(value.recovery.state.root);
            try std.testing.expectEqualStrings("accepted", value.recovery.state.root);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Fixture.observe, .{ log, auth, initial, proof, &backend });
}
