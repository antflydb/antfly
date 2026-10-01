// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Immutable staged output sets. This storage primitive is not an activation
//! entrypoint: ordered command authorization, admission, receipts, recovery,
//! and all semantic readers must be integrated before enabling head publication.
const std = @import("std");
const keys = @import("../internal_keys.zig");
const publication = @import("artifact_publication.zig");
const chunks = @import("artifact_chunk_manifest.zig");
const scopes = @import("artifact_generation_scope.zig");
const Digest = publication.Digest;
const spec_len = 252;
const state_len = spec_len + chunks.encoded_len + 8 + 32;

pub const Spec = struct {
    authority: publication.Authority,
    scope_digest: Digest,
    input_digest: Digest,
    output: chunks.Manifest,
    incarnation: u64,

    pub fn init(authority: publication.Authority, scope_key: []const u8, input: Digest, output: chunks.Manifest, incarnation: u64) !Spec {
        if (!scopes.isKey(scope_key)) return error.InvalidBatchRequest;
        var scope_digest: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(scope_key, &scope_digest, .{});
        const result: Spec = .{ .authority = authority, .scope_digest = scope_digest, .input_digest = input, .output = output, .incarnation = incarnation };
        _ = try decode(&result.encode());
        return result;
    }
    pub fn encode(self: Spec) [spec_len]u8 {
        var raw: [spec_len]u8 = undefined;
        @memcpy(raw[0..4], "ACG2");
        @memcpy(raw[4..28], &self.authority.namespace);
        std.mem.writeInt(u64, raw[28..36], self.authority.epoch, .little);
        @memcpy(raw[36..68], &self.authority.catalog_digest);
        @memcpy(raw[68..100], &self.scope_digest);
        @memcpy(raw[100..132], &self.input_digest);
        @memcpy(raw[132..212], &self.output.encode());
        std.mem.writeInt(u64, raw[212..220], self.incarnation, .little);
        std.crypto.hash.sha2.Sha256.hash(raw[0..220], raw[220..252], .{});
        return raw;
    }
    pub fn decode(raw: []const u8) !Spec {
        if (raw.len != spec_len or !std.mem.eql(u8, raw[0..4], "ACG2")) return error.ArtifactCatalogCorrupt;
        var checksum: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0..220], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, raw[220..252])) return error.ArtifactCatalogCorrupt;
        const result: Spec = .{ .authority = .{ .namespace = raw[4..28].*, .epoch = std.mem.readInt(u64, raw[28..36], .little), .catalog_digest = raw[36..68].* }, .scope_digest = raw[68..100].*, .input_digest = raw[100..132].*, .output = try chunks.Manifest.decode(raw[132..212]), .incarnation = std.mem.readInt(u64, raw[212..220], .little) };
        if (result.incarnation == 0 or result.authority.epoch == 0 or std.mem.allEqual(u8, &result.authority.namespace, 0) or std.mem.allEqual(u8, &result.authority.catalog_digest, 0)) return error.ArtifactCatalogCorrupt;
        return result;
    }
    pub fn id(self: Spec) Digest {
        var digest: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(&self.encode(), &digest, .{});
        return digest;
    }
    fn requireAuthority(self: Spec, txn: anytype) !void {
        const current = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
        if (!std.meta.eql(current, self.authority)) return error.ArtifactCatalogDrift;
    }
};

pub const State = struct {
    spec: Spec,
    progress: chunks.Manifest,
    retiring: bool = false,
    retired_through: u32 = 0,
    fn encode(self: State) [state_len]u8 {
        var raw: [state_len]u8 = undefined;
        @memcpy(raw[0..spec_len], &self.spec.encode());
        @memcpy(raw[spec_len..][0..chunks.encoded_len], &self.progress.encode());
        const phase = spec_len + chunks.encoded_len;
        @memset(raw[phase..][0..8], 0);
        raw[phase] = @intFromBool(self.retiring);
        std.mem.writeInt(u32, raw[phase + 4 ..][0..4], self.retired_through, .little);
        std.crypto.hash.sha2.Sha256.hash(raw[0 .. state_len - 32], raw[state_len - 32 ..], .{});
        return raw;
    }
    pub fn decode(raw: []const u8) !State {
        if (raw.len != state_len) return error.ArtifactCatalogCorrupt;
        var checksum: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0 .. state_len - 32], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, raw[state_len - 32 ..])) return error.ArtifactCatalogCorrupt;
        const phase = spec_len + chunks.encoded_len;
        if (raw[phase] > 1 or !std.mem.allEqual(u8, raw[phase + 1 ..][0..3], 0)) return error.ArtifactCatalogCorrupt;
        const result: State = .{ .spec = try Spec.decode(raw[0..spec_len]), .progress = try chunks.Manifest.decode(raw[spec_len..][0..chunks.encoded_len]), .retiring = raw[phase] == 1, .retired_through = std.mem.readInt(u32, raw[phase + 4 ..][0..4], .little) };
        if (result.retired_through > result.progress.count or (!result.retiring and result.retired_through != 0)) return error.ArtifactCatalogCorrupt;
        if (result.progress.count > result.spec.output.count or result.progress.payload_bytes > result.spec.output.payload_bytes or
            (result.progress.count == result.spec.output.count and !std.meta.eql(result.progress, result.spec.output))) return error.ArtifactCatalogCorrupt;
        return result;
    }
};

fn physicalKey(alloc: std.mem.Allocator, scope: []const u8, kind: u8, generation: ?Digest) ![]u8 {
    const physical_kind = try scopes.physicalKind(scope, kind);
    const length = scope.len + @as(usize, if (generation != null) 33 else 0);
    const result = try alloc.alloc(u8, length);
    @memcpy(result[0..scope.len], scope);
    result[keys.findComponentTerminator(scope, 1).? + 2] = physical_kind;
    if (generation) |id| {
        result[scope.len] = 1; // distinct from an optional encoded unit scope
        @memcpy(result[scope.len + 1 ..], &id);
    }
    return result;
}

/// One high-water mark per stream, rather than a tombstone per retired output
/// set. Begin reserves a strictly increasing incarnation in the writer. It is
/// safe to remove completed GC state: delayed begins cannot recreate old IDs.
const Clock = struct {
    namespace: publication.Namespace,
    epoch: u64,
    incarnation: u64,
    scope_digest: Digest,
    fn encode(self: Clock) [108]u8 {
        var raw: [108]u8 = undefined;
        @memcpy(raw[0..4], "AGC1");
        @memcpy(raw[4..28], &self.namespace);
        std.mem.writeInt(u64, raw[28..36], self.epoch, .little);
        std.mem.writeInt(u64, raw[36..44], self.incarnation, .little);
        @memcpy(raw[44..76], &self.scope_digest);
        std.crypto.hash.sha2.Sha256.hash(raw[0..76], raw[76..108], .{});
        return raw;
    }
    fn decode(raw: []const u8) !Clock {
        if (raw.len != 108 or !std.mem.eql(u8, raw[0..4], "AGC1")) return error.ArtifactCatalogCorrupt;
        var digest: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw[0..76], &digest, .{});
        if (!std.mem.eql(u8, &digest, raw[76..108])) return error.ArtifactCatalogCorrupt;
        const clock: Clock = .{ .namespace = raw[4..28].*, .epoch = std.mem.readInt(u64, raw[28..36], .little), .incarnation = std.mem.readInt(u64, raw[36..44], .little), .scope_digest = raw[44..76].* };
        if (clock.epoch == 0 or clock.incarnation == 0 or std.mem.allEqual(u8, &clock.namespace, 0)) return error.ArtifactCatalogCorrupt;
        return clock;
    }
};

/// Read-only proposal in the producer's pinned input snapshot. Begin performs
/// the reservation atomically; competing proposals must retry on conflict.
pub fn proposeIncarnation(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority, scope: []const u8) !u64 {
    if (!scopes.isKey(scope)) return error.InvalidBatchRequest;
    const current = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    if (!std.meta.eql(current, authority)) return error.ArtifactCatalogDrift;
    const key = try physicalKey(alloc, scope, keys.producer_generation_clock_kind, null);
    defer alloc.free(key);
    const raw = txn.get(key) catch |err| if (err == error.NotFound) return 1 else return err;
    const clock = try Clock.decode(raw);
    var digest: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(scope, &digest, .{});
    if (!std.mem.eql(u8, &digest, &clock.scope_digest)) return error.ArtifactCatalogCorrupt;
    if (!std.mem.eql(u8, &clock.namespace, &authority.namespace) or clock.epoch > authority.epoch) return error.ArtifactCatalogDrift;
    if (clock.epoch < authority.epoch) return 1;
    return std.math.add(u64, clock.incarnation, 1) catch error.ResourceBudgetExceeded;
}

pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    spec: Spec,
    head_key: []const u8,
    state_key: []const u8,
    row_prefix: []const u8,
    clock_key: []const u8,

    pub fn init(alloc: std.mem.Allocator, scope: []const u8, spec: Spec) !Plan {
        if (!scopes.isKey(scope)) return error.InvalidBatchRequest;
        _ = try Spec.decode(&spec.encode());
        var digest: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(scope, &digest, .{});
        if (!std.mem.eql(u8, &digest, &spec.scope_digest)) return error.InvalidBatchRequest;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        const head = try physicalKey(a, scope, keys.producer_generation_head_kind, null);
        const state = try physicalKey(a, scope, keys.producer_generation_state_kind, spec.id());
        const prefix = try physicalKey(a, scope, keys.producer_generation_row_kind, spec.id());
        const clock = try physicalKey(a, scope, keys.producer_generation_clock_kind, null);
        return .{ .arena = arena, .spec = spec, .head_key = head, .state_key = state, .row_prefix = prefix, .clock_key = clock };
    }
    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn load(self: *const Plan, txn: anytype) !State {
        const state = try State.decode(try txn.get(self.state_key));
        if (!std.meta.eql(state.spec, self.spec)) return error.ArtifactCatalogCorrupt;
        return state;
    }
    pub fn begin(self: *const Plan, txn: anytype) !bool {
        try self.spec.requireAuthority(txn);
        const raw = txn.get(self.state_key) catch |err| if (err == error.NotFound) null else return err;
        if (raw != null) {
            if ((try self.load(txn)).retiring) return error.EnrichmentSourceChanged;
            return false;
        }
        if (try self.wasReserved(txn)) return error.EnrichmentSourceChanged;
        const state: State = .{ .spec = self.spec, .progress = chunks.Builder.init().finish() };
        try txn.put(self.clock_key, &(Clock{ .namespace = self.spec.authority.namespace, .epoch = self.spec.authority.epoch, .incarnation = self.spec.incarnation, .scope_digest = self.spec.scope_digest }).encode());
        try txn.put(self.state_key, &state.encode());
        return true;
    }
    /// Called in the same transaction as semantic receipts/outbox/cut markers.
    /// A mandatory owner guard validates the complete input read-set and catalog
    /// producer authorization; staging success itself is never that authority.
    pub fn publish(self: *const Plan, txn: anytype, expected_head: ?Digest, guard: anytype) !bool {
        try self.spec.requireAuthority(txn);
        try guard.validate(txn);
        const state = self.load(txn) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
        if (state.retiring) return error.EnrichmentSourceChanged;
        if (!std.meta.eql(state.progress, self.spec.output)) return error.ArtifactPublicationPending;
        const current = txn.get(self.head_key) catch |err| if (err == error.NotFound) null else return err;
        if (current) |raw| {
            const head = try Spec.decode(raw);
            if (std.meta.eql(head, self.spec)) return false;
            if (expected_head == null or !std.mem.eql(u8, &head.id(), &expected_head.?)) return error.EnrichmentSourceChanged;
        } else if (expected_head != null) return error.EnrichmentSourceChanged;
        try txn.put(self.head_key, &self.spec.encode());
        return true;
    }

    fn wasReserved(self: *const Plan, txn: anytype) !bool {
        const raw = txn.get(self.clock_key) catch |err| if (err == error.NotFound) return false else return err;
        const clock = try Clock.decode(raw);
        if (!std.mem.eql(u8, &clock.scope_digest, &self.spec.scope_digest)) return error.ArtifactCatalogCorrupt;
        if (!std.mem.eql(u8, &clock.namespace, &self.spec.authority.namespace)) return error.ArtifactCatalogDrift;
        return clock.epoch > self.spec.authority.epoch or (clock.epoch == self.spec.authority.epoch and clock.incarnation >= self.spec.incarnation);
    }

    fn requireUnselected(self: *const Plan, txn: anytype) !void {
        const raw = txn.get(self.head_key) catch |err| if (err == error.NotFound) return else return err;
        const head = try Spec.decode(raw);
        if (!std.mem.eql(u8, &head.scope_digest, &self.spec.scope_digest)) return error.ArtifactCatalogCorrupt;
        if (std.meta.eql(head, self.spec)) return error.ArtifactPublicationPending;
    }

    fn requireRetirementAuthority(self: *const Plan, txn: anytype, expected: publication.Authority) !void {
        const current = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
        if (!std.meta.eql(current, expected) or current.epoch < self.spec.authority.epoch or
            !std.mem.eql(u8, &current.namespace, &self.spec.authority.namespace)) return error.ArtifactCatalogDrift;
    }

    /// Owner-authenticated abandonment/retirement. The head exclusion and
    /// durable fence precede all physical deletion; pinned readers use MVCC.
    /// Caller policy (expiry, obsolete input, etc.) is a mandatory guard.
    pub fn retire(self: *const Plan, txn: anytype, authority: publication.Authority, guard: anytype) !bool {
        try self.requireRetirementAuthority(txn, authority);
        try guard.validate(txn);
        try self.requireUnselected(txn);
        if (!try self.wasReserved(txn)) return error.ArtifactCatalogCorrupt;
        var state = self.load(txn) catch |err| {
            if (err == error.NotFound and try self.wasReserved(txn)) return false;
            return err;
        };
        if (state.retiring) return false;
        state.retiring = true;
        try txn.put(self.state_key, &state.encode());
        return true;
    }
};

pub const PreparedAppend = struct {
    arena: std.heap.ArenaAllocator,
    previous: State,
    next: State,
    rows: []const struct { key: []const u8, value: []const u8 },

    /// Hashing and ownership preparation happen outside the serialized writer.
    /// Pages are bounded; one oversized member may use the existing command cap.
    pub fn init(alloc: std.mem.Allocator, plan: *const Plan, previous: State, payloads: []const []const u8) !PreparedAppend {
        if (previous.retiring or !std.meta.eql(previous.spec, plan.spec) or payloads.len == 0 or payloads.len > 128 or payloads.len > plan.spec.output.count -| previous.progress.count) return error.InvalidBatchRequest;
        _ = try State.decode(&previous.encode());
        var bytes: usize = 0;
        const key_bytes = std.math.add(usize, plan.row_prefix.len, 4) catch return error.InvalidBatchRequest;
        for (payloads) |value| {
            bytes = std.math.add(usize, bytes, value.len) catch return error.InvalidBatchRequest;
            bytes = std.math.add(usize, bytes, key_bytes) catch return error.InvalidBatchRequest;
        }
        const limit: usize = if (payloads.len == 1) publication.max_payload_bytes else 64 * 1024;
        if (bytes > limit) return error.InvalidBatchRequest;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        const rows = try a.alloc(@typeInfo(@FieldType(PreparedAppend, "rows")).pointer.child, payloads.len);
        var builder = try chunks.Builder.fromCheckpoint(previous.progress);
        for (payloads, rows) |value, *row| {
            const key = try a.alloc(u8, plan.row_prefix.len + 4);
            @memcpy(key[0..plan.row_prefix.len], plan.row_prefix);
            std.mem.writeInt(u32, key[plan.row_prefix.len..][0..4], builder.count, .big);
            try builder.append(builder.count, value);
            row.* = .{ .key = key, .value = try a.dupe(u8, value) };
        }
        const next: State = .{ .spec = plan.spec, .progress = builder.finish() };
        _ = State.decode(&next.encode()) catch return error.InvalidBatchRequest;
        return .{ .arena = arena, .previous = previous, .next = next, .rows = rows };
    }
    pub fn deinit(self: *PreparedAppend) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn stage(self: *const PreparedAppend, plan: *const Plan, txn: anytype) !bool {
        if (!std.meta.eql(plan.spec, self.previous.spec)) return error.InvalidBatchRequest;
        try plan.spec.requireAuthority(txn);
        const current = plan.load(txn) catch |err| if (err == error.NotFound) return error.EnrichmentSourceChanged else return err;
        if (current.retiring) return error.EnrichmentSourceChanged;
        if (!std.meta.eql(current, self.previous)) {
            if (current.progress.count < self.next.progress.count) return error.EnrichmentSourceChanged;
            for (self.rows) |row| {
                const existing = txn.get(row.key) catch |err| if (err == error.NotFound) return error.ArtifactCatalogCorrupt else return err;
                if (!std.mem.eql(u8, existing, row.value)) return error.EnrichmentSourceChanged;
            }
            return false;
        }
        for (self.rows) |row| try txn.put(row.key, row.value);
        try txn.put(plan.state_key, &self.next.encode());
        return true;
    }
};

pub const PreparedRetirement = struct {
    arena: std.heap.ArenaAllocator,
    authority: publication.Authority,
    previous: State,
    next: State,
    keys: []const []const u8,

    /// Prepare a bounded deletion page without reading row bodies. Persistent
    /// progress belongs to the generation state; no in-memory cursor is needed
    /// after a restart, and terminal cleanup keeps just the stream's clock.
    pub fn init(alloc: std.mem.Allocator, plan: *const Plan, authority: publication.Authority, previous: State, max_rows: u32) !PreparedRetirement {
        if (!previous.retiring or max_rows == 0 or max_rows > 128 or !std.meta.eql(previous.spec, plan.spec)) return error.InvalidBatchRequest;
        _ = try State.decode(&previous.encode());
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        const key_bytes = std.math.add(usize, plan.row_prefix.len, 4) catch return error.InvalidBatchRequest;
        if (key_bytes > publication.max_payload_bytes) return error.InvalidBatchRequest;
        const by_bytes: u32 = @intCast(@max(@as(usize, 1), (64 * 1024) / key_bytes));
        const count = @min(max_rows, by_bytes, previous.progress.count - previous.retired_through);
        const rows = try a.alloc([]const u8, count);
        for (rows, 0..) |*row, i| {
            const key = try a.alloc(u8, plan.row_prefix.len + 4);
            @memcpy(key[0..plan.row_prefix.len], plan.row_prefix);
            std.mem.writeInt(u32, key[plan.row_prefix.len..][0..4], previous.retired_through + @as(u32, @intCast(i)), .big);
            row.* = key;
        }
        var next = previous;
        next.retired_through += count;
        return .{ .arena = arena, .authority = authority, .previous = previous, .next = next, .keys = rows };
    }
    pub fn deinit(self: *PreparedRetirement) void {
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn stage(self: *const PreparedRetirement, plan: *const Plan, txn: anytype) !bool {
        if (!std.meta.eql(plan.spec, self.previous.spec)) return error.InvalidBatchRequest;
        try plan.requireRetirementAuthority(txn, self.authority);
        try plan.requireUnselected(txn);
        if (!try plan.wasReserved(txn)) return error.ArtifactCatalogCorrupt;
        const current = plan.load(txn) catch |err| if (err == error.NotFound) return false else return err;
        if (!current.retiring or !std.meta.eql(current.progress, self.previous.progress)) return error.EnrichmentSourceChanged;
        if (!std.meta.eql(current, self.previous)) {
            if (current.retired_through >= self.next.retired_through) return false;
            return error.EnrichmentSourceChanged;
        }
        for (self.keys) |key| try txn.delete(key);
        if (self.next.retired_through == self.next.progress.count) {
            try txn.delete(plan.state_key);
        } else try txn.put(plan.state_key, &self.next.encode());
        return true;
    }
};

test "ordered artifact inventory generation pages charge physical keys as well as payloads" {
    const alloc = std.testing.allocator;
    const unit = try alloc.alloc(u8, 64 * 1024);
    defer alloc.free(unit);
    @memset(unit, 'u');
    const scope = try chunks.scopedKeyAlloc(alloc, "doc", "chunks", unit);
    defer alloc.free(scope);
    const authority: publication.Authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) };
    var output = chunks.Builder.init();
    try output.append(0, "x");
    try output.append(1, "x");
    const spec = try Spec.init(authority, scope, @splat(3), output.finish(), 1);
    var plan = try Plan.init(alloc, scope, spec);
    defer plan.deinit();
    const initial: State = .{ .spec = spec, .progress = chunks.Builder.init().finish() };
    try std.testing.expectError(error.InvalidBatchRequest, PreparedAppend.init(alloc, &plan, initial, &.{ "x", "x" }));
    var single = try PreparedAppend.init(alloc, &plan, initial, &.{"x"});
    defer single.deinit();
    var retire = try PreparedRetirement.init(alloc, &plan, authority, .{ .spec = spec, .progress = output.finish(), .retiring = true }, 128);
    defer retire.deinit();
    try std.testing.expectEqual(@as(usize, 1), retire.keys.len);
    try std.testing.expect(retire.keys[0].len > 64 * 1024);
}

/// Resolve a logical member in one pinned snapshot. A published head owns the
/// entire ordinal space, including absence; never fall back to legacy rows
/// after selecting it. Non-chunk artifacts retain their ordinary lookup path.
/// The returned bytes are borrowed from the transaction, not from the plan.
pub fn readMember(alloc: std.mem.Allocator, txn: anytype, logical_key: []const u8) !?[]const u8 {
    var input = try captureInput(alloc, txn, logical_key);
    defer input.deinit();
    return input.value;
}

/// One snapshot's logical value and visibility witness. Payloads are borrowed
/// from the transaction; only the head key is owned. A selected generation's
/// provenance certifies the whole immutable set, not a stale legacy member.
pub const Input = struct {
    alloc: std.mem.Allocator,
    /// Unit absence must be a certified deletion, never an incomplete upload.
    require_absence_proof: bool = false,
    head_key: ?[]u8 = null,
    head_value: ?[]const u8 = null,
    value: ?[]const u8 = null,
    pub fn deinit(self: *Input) void {
        if (self.head_key) |key| self.alloc.free(key);
        self.* = undefined;
    }
    pub fn proofKey(self: Input, logical: []const u8) []const u8 {
        return if (self.head_value != null) self.head_key.? else logical;
    }
    pub fn proofValue(self: Input) ?[]const u8 {
        return self.head_value orelse self.value;
    }
    pub fn observe(self: Input, token: anytype, txn: anytype, logical: []const u8) !void {
        if (self.head_key) |key| try token.observe(key, self.head_value, try publication.artifactRevision(txn, token.namespace, key));
        // The immutable generation rows are covered by the head proof. Legacy
        // members additionally require their physical read-set witness.
        if (self.head_value == null) try token.observe(logical, self.value, try publication.artifactRevision(txn, token.namespace, logical));
    }
};

pub fn captureInput(alloc: std.mem.Allocator, txn: anytype, logical: []const u8) !Input {
    var input: Input = .{ .alloc = alloc };
    errdefer input.deinit();
    const scope = (try chunks.keyForMemberAlloc(alloc, logical)) orelse {
        input.value = txn.get(logical) catch |err| if (err == error.NotFound) null else return err;
        return input;
    };
    defer alloc.free(scope);
    input.head_key = try physicalKey(alloc, scope, keys.producer_generation_head_kind, null);
    input.head_value = txn.get(input.head_key.?) catch |err| if (err == error.NotFound) null else return err;
    if (input.head_value) |raw| {
        var plan = try Plan.init(alloc, scope, try Spec.decode(raw));
        defer plan.deinit();
        const ordinal = std.mem.readInt(u32, logical[logical.len - 4 ..][0..4], .big);
        if (ordinal < plan.spec.output.count) {
            const key = try alloc.alloc(u8, plan.row_prefix.len + 4);
            defer alloc.free(key);
            @memcpy(key[0..plan.row_prefix.len], plan.row_prefix);
            std.mem.writeInt(u32, key[plan.row_prefix.len..][0..4], ordinal, .big);
            input.value = txn.get(key) catch |err| if (err == error.NotFound) return error.ArtifactCatalogCorrupt else return err;
        }
    } else input.value = txn.get(logical) catch |err| if (err == error.NotFound) null else return err;
    return input;
}

/// The transaction is borrowed and pinned for this entire view. Reads never
/// fall back to an old physical tail or mix a later head into this generation.
pub fn View(comptime Txn: type) type {
    return struct {
        txn: *Txn,
        plan: Plan,
        pub const Row = struct { ordinal: u32, value: []const u8 };
        pub const Cursor = struct {
            alloc: std.mem.Allocator,
            physical: ?Txn.CursorAdapter,
            start_key: []const u8,
            prefix: []const u8,
            count: u32,
            ordinal: u32,
            started: bool = false,
            pub fn close(self: *Cursor) void {
                if (self.physical) |*cursor| cursor.close();
                self.alloc.free(self.start_key);
                self.* = undefined;
            }
            /// Borrowed until the next cursor operation; one seek then a
            /// sequential scan, not one LSM point lookup per projected row.
            pub fn next(self: *Cursor) !?Row {
                if (self.ordinal >= self.count) return null;
                const entry = (if (self.started) try self.physical.?.next() else try self.physical.?.seekAtOrAfter(self.start_key)) orelse return error.ArtifactCatalogCorrupt;
                self.started = true;
                if (entry.key.len != self.prefix.len + 4 or !std.mem.startsWith(u8, entry.key, self.prefix) or std.mem.readInt(u32, entry.key[self.prefix.len..][0..4], .big) != self.ordinal) return error.ArtifactCatalogCorrupt;
                const ordinal = self.ordinal;
                self.ordinal += 1;
                return .{ .ordinal = ordinal, .value = entry.value };
            }
        };
        pub fn open(alloc: std.mem.Allocator, txn: *Txn, scope: []const u8) !?@This() {
            if (!scopes.isKey(scope)) return error.InvalidBatchRequest;
            const head_key = try physicalKey(alloc, scope, keys.producer_generation_head_kind, null);
            defer alloc.free(head_key);
            const raw = txn.get(head_key) catch |err| if (err == error.NotFound) return null else return err;
            return .{ .txn = txn, .plan = try Plan.init(alloc, scope, try Spec.decode(raw)) };
        }
        pub fn deinit(self: *@This()) void {
            self.plan.deinit();
            self.* = undefined;
        }
        /// The view and its snapshot must outlive this cursor.
        pub fn openCursor(self: *@This(), alloc: std.mem.Allocator, start: u32) !Cursor {
            if (start >= self.plan.spec.output.count) return .{ .alloc = alloc, .physical = null, .start_key = "", .prefix = self.plan.row_prefix, .count = self.plan.spec.output.count, .ordinal = start };
            const key = try alloc.alloc(u8, self.plan.row_prefix.len + 4);
            errdefer alloc.free(key);
            @memcpy(key[0..self.plan.row_prefix.len], self.plan.row_prefix);
            std.mem.writeInt(u32, key[self.plan.row_prefix.len..][0..4], start, .big);
            return .{ .alloc = alloc, .physical = try self.txn.openPhysicalCursorAdapter(), .start_key = key, .prefix = self.plan.row_prefix, .count = self.plan.spec.output.count, .ordinal = start };
        }
        pub fn get(self: *const @This(), alloc: std.mem.Allocator, ordinal: u32) !?[]const u8 {
            if (ordinal >= self.plan.spec.output.count) return null;
            const key = try alloc.alloc(u8, self.plan.row_prefix.len + 4);
            defer alloc.free(key);
            @memcpy(key[0..self.plan.row_prefix.len], self.plan.row_prefix);
            std.mem.writeInt(u32, key[self.plan.row_prefix.len..][0..4], ordinal, .big);
            return self.txn.get(key) catch |err| if (err == error.NotFound) error.ArtifactCatalogCorrupt else err;
        }
    };
}
