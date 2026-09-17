// Copyright 2026 Antfly, Inc.
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

//! Optimistic, bounded build pages. Preparation never holds apply-exclusive.
//! Publication fences the immutable plan, namespace, owner range, progress CAS,
//! and each source row. Live mutations already maintain the selected generation,
//! so changed/deleted candidates can be skipped without losing coverage.
const std = @import("std");
const platform_time = @import("antfly_platform").time;
const catalog = @import("relational_index_catalog.zig");
const plans = @import("relational_index_plan.zig");
const records = @import("relational_index_records.zig");
const tuples = @import("relational_index_keys.zig");
const registry = @import("schema_registry.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const row_store = @import("relational_store.zig");
const internal = @import("../internal_keys.zig");
const docstore = @import("../docstore.zig");
const range_state = @import("range_state.zig");
const maintenance = @import("relational_index_maintenance_contract.zig");
const Allocator = std.mem.Allocator;
const Digest = [32]u8;
pub const progress_prefix = "\x00\x00__metadata__:relational_index_progress:";
const header_len = 140;
const max_cursor_bytes = 1024 * 1024;
const full_range = [_]u8{0} ** 8;

fn digest(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

fn prefixEnd(alloc: Allocator, prefix: []const u8) ![]const u8 {
    var len = prefix.len;
    while (len > 0) {
        len -= 1;
        if (prefix[len] != 255) {
            const end = try alloc.dupe(u8, prefix[0 .. len + 1]);
            end[len] += 1;
            return end;
        }
    }
    return error.InvalidRelationalIndexProgress;
}

pub const State = enum(u8) { building = 0, ready = 1, failed = 2 };
pub const Failure = enum(u8) { none = 0, incompatible_schema = 1, invalid_row = 2 };

/// Deterministic row failures are durable job outcomes, shared by primary
/// construction and forward/reverse repair. Resource failures must propagate
/// without poisoning a generation that can succeed on the next attempt.
pub fn classifyRowFailure(err: anyerror) ?Failure {
    if (@import("../../schema/relational_expression_errors.zig").isInvalidInput(err)) return .invalid_row;
    return switch (err) {
        error.UnknownSchemaVersion, error.RelationalIndexColumnNotFound, error.RelationalIndexColumnTypeMismatch, error.RelationalRowSchemaMismatch => .incompatible_schema,
        error.InvalidRelationalRow, error.UnsupportedRelationalRowVersion, error.RelationalRowChecksumMismatch, error.InvalidColumnValue => .invalid_row,
        else => null,
    };
}
pub const Phase = enum(u8) { primary = 0, forward = 1, reverse = 2 };

pub const Progress = struct {
    id: records.Id,
    owner: Digest,
    comparison: Digest,
    state: State = .building,
    rows_scanned: u64 = 0,
    cursor: []const u8 = "",
    failure: Failure = .none,
    phase: Phase = .primary,
    attempt: u64 = 0,
    last_maintenance: Digest = @splat(0),

    pub fn encode(self: Progress, alloc: Allocator) ![]u8 {
        if (self.cursor.len > max_cursor_bytes or (self.state == .ready and self.cursor.len != 0) or
            (self.state == .failed) != (self.failure != .none))
            return error.InvalidRelationalIndexProgress;
        const out = try alloc.alloc(u8, header_len + self.cursor.len + 32);
        @memcpy(out[0..4], "AIRP");
        std.mem.writeInt(u32, out[4..8], 2, .little);
        @memcpy(out[8..20], &self.id.encode());
        @memcpy(out[20..52], &self.owner);
        @memcpy(out[52..84], &self.comparison);
        out[84] = @intFromEnum(self.state);
        out[85] = @intFromEnum(self.failure);
        out[86] = @intFromEnum(self.phase);
        out[87] = 0;
        std.mem.writeInt(u64, out[88..96], self.rows_scanned, .little);
        std.mem.writeInt(u32, out[96..100], @intCast(self.cursor.len), .little);
        std.mem.writeInt(u64, out[100..108], self.attempt, .little);
        @memcpy(out[108..140], &self.last_maintenance);
        @memcpy(out[header_len..][0..self.cursor.len], self.cursor);
        @memcpy(out[out.len - 32 ..], &digest(out[0 .. out.len - 32]));
        return out;
    }

    /// Borrows the cursor from bytes. Framing and checksum validation allocate
    /// nothing; progress cannot force an unbounded cursor allocation on reopen.
    pub fn decode(bytes: []const u8) !Progress {
        if (bytes.len < header_len + 32 or bytes.len > header_len + max_cursor_bytes + 32 or
            !std.mem.eql(u8, bytes[0..4], "AIRP") or std.mem.readInt(u32, bytes[4..8], .little) != 2 or
            bytes[87] != 0) return error.InvalidRelationalIndexProgress;
        const size = std.mem.readInt(u32, bytes[96..100], .little);
        if (size != bytes.len - header_len - 32 or
            !std.mem.eql(u8, bytes[bytes.len - 32 ..], &digest(bytes[0 .. bytes.len - 32])))
            return error.InvalidRelationalIndexProgress;
        const state: State = switch (bytes[84]) {
            0 => .building,
            1 => .ready,
            2 => .failed,
            else => return error.InvalidRelationalIndexProgress,
        };
        if (state == .ready and size != 0) return error.InvalidRelationalIndexProgress;
        const failure: Failure = switch (bytes[85]) {
            0 => .none,
            1 => .incompatible_schema,
            2 => .invalid_row,
            else => return error.InvalidRelationalIndexProgress,
        };
        if ((state == .failed) != (failure != .none)) return error.InvalidRelationalIndexProgress;
        return .{
            .id = try records.Id.decode(bytes[8..20]),
            .owner = bytes[20..52].*,
            .comparison = bytes[52..84].*,
            .state = state,
            .failure = failure,
            .rows_scanned = std.mem.readInt(u64, bytes[88..96], .little),
            .cursor = bytes[header_len..][0..size],
            .phase = std.enums.fromInt(Phase, bytes[86]) orelse return error.InvalidRelationalIndexProgress,
            .attempt = std.mem.readInt(u64, bytes[100..108], .little),
            .last_maintenance = bytes[108..140].*,
        };
    }

    pub fn matches(self: Progress, index: plans.BoundIndex, owner: Digest) bool {
        return self.id.mapKey() == index.id().mapKey() and std.mem.eql(u8, &self.owner, &owner) and
            std.mem.eql(u8, &self.comparison, &index.tuple.fingerprint);
    }
};

/// Shared by exact-proof retry/repair admission. Page publication preserves the
/// attempt and bounded last-command receipt through failure and completion.
pub fn resetProgress(current: Progress) !Progress {
    return .{ .id = current.id, .owner = current.owner, .comparison = current.comparison, .attempt = try std.math.add(u64, current.attempt, 1), .last_maintenance = current.last_maintenance };
}

pub fn progressKey(id: records.Id) [progress_prefix.len + records.Id.encoded_len]u8 {
    var key: [progress_prefix.len + records.Id.encoded_len]u8 = undefined;
    @memcpy(key[0..progress_prefix.len], progress_prefix);
    @memcpy(key[progress_prefix.len..], &id.encode());
    return key;
}

fn getOptional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

pub fn ownership(txn: *docstore.DocStore.Txn) !Digest {
    const range = (try getOptional(txn, range_state.range_key)) orelse &full_range;
    const identity = @import("doc_identity.zig");
    const namespace = (try identity.loadNamespaceTxn(txn)) orelse identity.default_namespace;
    var namespace_bytes: [24]u8 = undefined;
    identity.encodeNamespace(&namespace_bytes, namespace);
    // Range bounds alone are not ownership: a restored/reassigned incarnation
    // may have exactly the same bounds while carrying an old coverage receipt.
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly relational index coverage owner v1");
    hash.update(&namespace_bytes);
    hash.update(range);
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}

pub fn status(txn: *docstore.DocStore.Txn, index: plans.BoundIndex) !Progress {
    return statusWithOwnership(txn, index, try ownership(txn));
}

pub fn statusWithOwnership(txn: *docstore.DocStore.Txn, index: plans.BoundIndex, owner: Digest) !Progress {
    return (try statusProofWithOwnership(txn, index, owner)).progress;
}

pub const StatusProof = struct { progress: Progress, digest: Digest, maintenance_epoch: u64, last_maintenance_request: Digest };

pub fn statusProofWithOwnership(txn: *docstore.DocStore.Txn, index: plans.BoundIndex, owner: Digest) !StatusProof {
    const raw = try getOptional(txn, &progressKey(index.id()));
    const control = try maintenance.readControl(txn, index.id());
    var current = Progress{ .id = index.id(), .owner = owner, .comparison = index.tuple.fingerprint };
    if (raw) |bytes| {
        const decoded = try Progress.decode(bytes);
        if (decoded.matches(index, owner)) current = decoded;
    }
    if (!std.mem.eql(u8, &current.last_maintenance, &control.last_request)) {
        current = .{ .id = index.id(), .owner = owner, .comparison = index.tuple.fingerprint, .attempt = control.epoch, .last_maintenance = control.last_request };
    }
    var storage: [header_len + 32]u8 = undefined;
    var arena = std.heap.FixedBufferAllocator.init(&storage);
    const proof = if (raw) |bytes| digest(bytes) else digest(try current.encode(arena.allocator()));
    return .{ .progress = current, .digest = proof, .maintenance_epoch = control.epoch, .last_maintenance_request = control.last_request };
}

pub fn progressDigest(txn: *docstore.DocStore.Txn, index: plans.BoundIndex) !Digest {
    return (try statusProofWithOwnership(txn, index, try ownership(txn))).digest;
}

pub const Budget = struct {
    /// Document prefixes in primary construction, generation-local records
    /// in verification. Artifact/index fanout is skipped by a prefix seek.
    records: usize = 256,
    bytes: usize = 1024 * 1024,
    time_ns: u64 = 5 * std.time.ns_per_ms,

    fn validate(self: Budget) !void {
        if (self.records == 0 or self.records > 4096 or self.bytes == 0 or self.bytes > 16 * 1024 * 1024 or
            self.time_ns == 0 or self.time_ns > std.time.ns_per_s) return error.InvalidRelationalIndexBudget;
    }
};

pub const Page = struct {
    arena: std.heap.ArenaAllocator,
    pinned: catalog.WriteSnapshot,
    index_offset: usize,
    namespace_generation: u64,
    expected: ?[]const u8,
    control: maintenance.Control,
    next: Progress,
    candidates: []const Candidate,
    failed_source: ?FailedSource = null,
    consumed: bool = false,
    /// Logical scan work, independent of payload size and unrelated indexes.
    records_examined: usize = 0,

    const Candidate = struct {
        primary: ?[]const u8 = null,
        document: []const u8 = "",
        hash: ?Digest = null,
        tuple: ?[]const u8 = null,
        payload: []const u8 = "",
        observed_key: ?[]const u8 = null,
        observed_hash: Digest = @splat(0),
        delete_observed: bool = false,
        delete_reverse: ?[]const u8 = null,
        nonmember: bool = false,
    };
    const FailedSource = struct { primary: []const u8, hash: Digest };

    fn prepareTuple(alloc: Allocator, payload_alloc: Allocator, core: anytype, pinned: catalog.WriteSnapshot, index: plans.BoundIndex, value: []const u8, source: *?registry.SchemaView, projected: *?tuples.TuplePlan, projected_cover: *?@import("relational_index_cover.zig").Source, projected_predicate: *?@import("relational_index_predicate.zig").Source, encoded: *std.ArrayList(u8)) !?[]const u8 {
        const version = try row_store.rowSchemaVersion(value);
        if (source.* == null or source.*.?.version() != version) {
            if (projected.*) |*tuple| tuple.deinit();
            projected.* = null;
            if (projected_cover.*) |*cover| cover.deinit();
            projected_cover.* = null;
            if (projected_predicate.*) |*condition| condition.deinit();
            projected_predicate.* = null;
            if (source.*) |*view| view.release();
            source.* = null;
            source.* = if (pinned.plan.schemaView().version() == version) pinned.plan.schemaView().clone() else (try core.acquireSchemaVersionView(version)) orelse return error.UnknownSchemaVersion;
            if (index.predicate) |condition| projected_predicate.* = try condition.projectSource(alloc, source.*.?.tableSchema().*, source.*.?.physicalLayout());
        }
        const typed = try codec.ordinalRowView(value, source.*.?.tableSchema().*, source.*.?.physicalLayout());
        encoded.clearRetainingCapacity();
        if (projected_predicate.*) |condition| if (!try condition.matches(alloc, encoded, typed)) {
            encoded.clearRetainingCapacity();
            return null;
        };
        encoded.clearRetainingCapacity();
        if (projected.* == null) {
            projected.* = try index.tuple.projectSource(alloc, source.*.?.tableSchema().*, source.*.?.physicalLayout());
            if (index.cover) |cover| projected_cover.* = try cover.projectSource(alloc, source.*.?.tableSchema().*, source.*.?.physicalLayout());
        }
        _ = try projected.*.?.append(alloc, encoded, typed);
        return if (index.cover) |cover| try cover.encodeSource(payload_alloc, typed, &projected_cover.*.?) else "";
    }

    pub fn deinit(self: *Page) void {
        self.pinned.deinit();
        self.arena.deinit();
        self.* = undefined;
    }

    /// Core supplies immutable historical schema views. Its store and registry
    /// must remain alive; callers hold the DB lifecycle/snapshot admission lease.
    pub fn prepare(alloc: Allocator, io: ?std.Io, core: anytype, name: []const u8, budget: Budget) !?Page {
        try budget.validate();
        if (io) |runtime_io| try runtime_io.checkCancel();
        const namespace_generation = core.schemaNamespaceGeneration();
        var pinned = core.relational_indexes.acquire() orelse return error.IndexNotFound;
        var transferred = false;
        defer if (!transferred) pinned.deinit();
        const index_offset = for (pinned.plan.boundIndexes(), 0..) |index, i| {
            if (std.mem.eql(u8, index.name, name)) break i;
        } else return error.IndexNotFound;
        const index = pinned.plan.boundIndexes()[index_offset];
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer if (!transferred) arena.deinit();
        const page_alloc = arena.allocator();
        var read = try core.store.beginReadTxn();
        defer read.abort();
        const proof = try statusProofWithOwnership(&read, index, try ownership(&read));
        var progress = proof.progress;
        if (progress.state != .building) return null;
        if (progress.phase != .primary) return prepareDerived(alloc, io, core, name, budget);
        const expected = if (try getOptional(&read, &progressKey(index.id()))) |raw| try page_alloc.dupe(u8, raw) else null;
        const range_raw = (try getOptional(&read, range_state.range_key)) orelse &full_range;
        const range = try range_state.decodeRangeAlloc(page_alloc, range_raw);
        const lower = try internal.documentExactPrefixAlloc(page_alloc, range.start);
        const upper: []const u8 = if (range.end.len != 0) try internal.documentExactPrefixAlloc(page_alloc, range.end) else &.{internal.user_namespace + 1};
        if (progress.cursor.len != 0 and (std.mem.order(u8, progress.cursor, lower) == .lt or std.mem.order(u8, progress.cursor, upper) != .lt))
            return error.InvalidRelationalIndexProgress;
        var cursor = try read.openCursor();
        defer cursor.close();
        cursor.setUpperBound(upper);
        var candidates = std.ArrayList(Candidate).empty;
        var source: ?registry.SchemaView = null;
        defer if (source) |*view| view.release();
        var projected: ?tuples.TuplePlan = null;
        defer if (projected) |*tuple| tuple.deinit();
        var projected_cover: ?@import("relational_index_cover.zig").Source = null;
        defer if (projected_cover) |*cover| cover.deinit();
        var projected_predicate: ?@import("relational_index_predicate.zig").Source = null;
        defer if (projected_predicate) |*condition| condition.deinit();
        var encoded = std.ArrayList(u8).empty;
        defer encoded.deinit(alloc);
        var inspected: usize = 0;
        var bytes: usize = 0;
        const started = platform_time.monotonicNs();
        var after = progress.cursor;
        var exhausted = true;
        var failed_source: ?FailedSource = null;
        var entry = try cursor.seekAtOrAfter(if (after.len == 0) lower else after);
        while (entry) |kv| : (entry = try cursor.seekAtOrAfter(after)) {
            if (io) |runtime_io| try runtime_io.checkCancel();
            if (std.mem.order(u8, kv.key, upper) != .lt) break;
            if (kv.key.len > max_cursor_bytes) return error.InvalidRelationalIndexProgress;
            inspected += 1;
            bytes +|= kv.key.len;
            // Visit each document once, not each of its M index/artifact
            // companions. The successor is an exclusive document-prefix cut;
            // it is safe to persist and seek after cancellation/reopen.
            const term = internal.findComponentTerminator(kv.key, 1) orelse return error.InvalidRelationalIndexProgress;
            after = try prefixEnd(page_alloc, kv.key[0 .. term + 2]);
            const document = (try internal.decodeDocumentComponentAlloc(page_alloc, kv.key)).?;
            const primary = try internal.relationalRowKeyAlloc(page_alloc, document);
            const row = if (std.mem.eql(u8, kv.key, primary)) kv.value else try getOptional(&read, primary);
            if (row) |raw| {
                bytes +|= raw.len;
                const payload = prepareTuple(alloc, page_alloc, core, pinned, index, raw, &source, &projected, &projected_cover, &projected_predicate, &encoded) catch |err| {
                    progress.failure = classifyRowFailure(err) orelse return err;
                    progress.state = .failed;
                    failed_source = .{ .primary = primary, .hash = digest(raw) };
                    candidates.clearRetainingCapacity();
                    break;
                };
                try candidates.append(page_alloc, .{ .primary = primary, .document = document, .hash = digest(raw), .tuple = if (payload != null) try page_alloc.dupe(u8, encoded.items) else null, .payload = payload orelse "", .nonmember = payload == null });
                progress.rows_scanned = try std.math.add(u64, progress.rows_scanned, 1);
            } else {
                // Also repair reverse-only orphans whose ownership record was
                // lost. This shares the primary pass, never a second global scan.
                var reverse = std.ArrayList(u8).empty;
                try records.appendReverseKey(page_alloc, &reverse, index.id(), document);
                if (try getOptional(&read, reverse.items)) |raw| {
                    bytes +|= raw.len;
                    try candidates.append(page_alloc, .{ .primary = primary, .observed_key = reverse.items, .observed_hash = digest(raw), .delete_observed = true });
                }
            }
            if (inspected >= budget.records or bytes >= budget.bytes or platform_time.monotonicNs() - started >= budget.time_ns) {
                exhausted = false;
                break;
            }
        }
        if (failed_source == null and exhausted) progress.phase = .forward;
        progress.cursor = if (failed_source == null and exhausted) "" else after;
        transferred = true;
        return .{ .arena = arena, .pinned = pinned, .index_offset = index_offset, .namespace_generation = namespace_generation, .expected = expected, .control = .{ .epoch = proof.maintenance_epoch, .last_request = proof.last_maintenance_request }, .next = progress, .candidates = candidates.items, .failed_source = failed_source, .records_examined = inspected };
    }

    fn prepareDerived(alloc: Allocator, io: ?std.Io, core: anytype, name: []const u8, budget: Budget) !?Page {
        const namespace = core.schemaNamespaceGeneration();
        var pinned = core.relational_indexes.acquire() orelse return error.IndexNotFound;
        var transferred = false;
        defer if (!transferred) pinned.deinit();
        const offset = for (pinned.plan.boundIndexes(), 0..) |index, i| {
            if (std.mem.eql(u8, index.name, name)) break i;
        } else return error.IndexNotFound;
        const index = pinned.plan.boundIndexes()[offset];
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer if (!transferred) arena.deinit();
        const page_alloc = arena.allocator();
        var read = try core.store.beginReadTxn();
        defer read.abort();
        const proof = try statusProofWithOwnership(&read, index, try ownership(&read));
        var progress = proof.progress;
        if (progress.state != .building or progress.phase == .primary) return null;
        const expected = if (try getOptional(&read, &progressKey(index.id()))) |raw| try page_alloc.dupe(u8, raw) else null;
        const range = try range_state.decodeRangeAlloc(page_alloc, (try getOptional(&read, range_state.range_key)) orelse &full_range);
        // Both verification passes are generation-local. Retirement has its
        // own durable GC queue; building one index must not scrub all others.
        const prefix = if (progress.phase == .forward) try records.forwardPrefix(index.id()) else try records.ownershipPrefix(index.id());
        const lower: []const u8 = &prefix;
        const upper = try prefixEnd(page_alloc, lower);
        if (progress.cursor.len != 0 and (std.mem.order(u8, progress.cursor, lower) == .lt or std.mem.order(u8, progress.cursor, upper) != .lt)) return error.InvalidRelationalIndexProgress;
        var cursor = try read.openCursor();
        defer cursor.close();
        cursor.setUpperBound(upper);
        var candidates = std.ArrayList(Candidate).empty;
        var source: ?registry.SchemaView = null;
        defer if (source) |*view| view.release();
        var projected: ?tuples.TuplePlan = null;
        defer if (projected) |*tuple| tuple.deinit();
        var projected_cover: ?@import("relational_index_cover.zig").Source = null;
        defer if (projected_cover) |*cover| cover.deinit();
        var projected_predicate: ?@import("relational_index_predicate.zig").Source = null;
        defer if (projected_predicate) |*condition| condition.deinit();
        var encoded = std.ArrayList(u8).empty;
        defer encoded.deinit(alloc);
        const started = platform_time.monotonicNs();
        var inspected: usize = 0;
        var bytes: usize = 0;
        var after = progress.cursor;
        var exhausted = true;
        var failed_source: ?FailedSource = null;
        var entry = try cursor.seekAtOrAfter(if (after.len == 0) lower else after);
        while (entry) |kv| : (entry = try cursor.next()) {
            if (progress.cursor.len != 0 and std.mem.order(u8, kv.key, progress.cursor) != .gt) continue;
            if (std.mem.order(u8, kv.key, upper) != .lt) break;
            if (io) |runtime| try runtime.checkCancel();
            if (kv.key.len > max_cursor_bytes) return error.InvalidRelationalIndexProgress;
            after = try page_alloc.dupe(u8, kv.key);
            inspected += 1;
            bytes +|= kv.key.len +| kv.value.len;
            examine: {
                var candidate = Candidate{ .observed_key = after, .observed_hash = digest(kv.value) };
                var document_key: []const u8 = undefined;
                var observed_tuple: ?[]const u8 = null;
                if (progress.phase == .forward) {
                    const owner = records.forwardOwnership(kv.key) catch {
                        candidate.delete_observed = true;
                        try candidates.append(page_alloc, candidate);
                        break :examine;
                    };
                    observed_tuple = owner.tuple;
                    const raw_key = try page_alloc.alloc(u8, owner.document_component.len + 1);
                    raw_key[0] = internal.user_namespace;
                    @memcpy(raw_key[1..], owner.document_component);
                    document_key = raw_key;
                } else {
                    const document_component = records.ownershipDocument(kv.key) catch {
                        candidate.delete_observed = true;
                        try candidates.append(page_alloc, candidate);
                        break :examine;
                    };
                    const raw_key = try page_alloc.alloc(u8, document_component.len + 1);
                    raw_key[0] = internal.user_namespace;
                    @memcpy(raw_key[1..], document_component);
                    document_key = raw_key;
                }
                const document = (try internal.decodeDocumentComponentAlloc(page_alloc, document_key)) orelse return error.InvalidRelationalIndexForwardKey;
                if (!range.contains(document)) {
                    candidate.delete_observed = true;
                    try candidates.append(page_alloc, candidate);
                    break :examine;
                }
                candidate.document = document;
                const primary = try internal.relationalRowKeyAlloc(page_alloc, document);
                candidate.primary = primary;
                const row = try getOptional(&read, primary);
                if (row) |raw| {
                    bytes +|= raw.len;
                    candidate.hash = digest(raw);
                    const payload = prepareTuple(alloc, page_alloc, core, pinned, index, raw, &source, &projected, &projected_cover, &projected_predicate, &encoded) catch |err| {
                        progress.failure = classifyRowFailure(err) orelse return err;
                        progress.state = .failed;
                        failed_source = .{ .primary = primary, .hash = candidate.hash.? };
                        candidates.clearRetainingCapacity();
                        break :examine;
                    };
                    candidate.tuple = if (payload != null) try page_alloc.dupe(u8, encoded.items) else null;
                    candidate.payload = payload orelse "";
                    candidate.nonmember = payload == null;
                    candidate.delete_observed = if (payload == null) true else if (observed_tuple) |old| !std.mem.eql(u8, old, encoded.items) else false;
                } else {
                    candidate.delete_observed = true;
                    if (progress.phase == .forward) {
                        var reverse = std.ArrayList(u8).empty;
                        try records.appendReverseKey(page_alloc, &reverse, index.id(), document);
                        candidate.delete_reverse = reverse.items;
                    }
                }
                try candidates.append(page_alloc, candidate);
            }
            if (failed_source != null) break;
            if (inspected >= budget.records or bytes >= budget.bytes or platform_time.monotonicNs() -| started >= budget.time_ns) {
                exhausted = false;
                break;
            }
        }
        if (failed_source == null and exhausted) {
            if (progress.phase == .forward) progress.phase = .reverse else progress.state = .ready;
            progress.cursor = "";
        } else progress.cursor = after;
        transferred = true;
        return .{ .arena = arena, .pinned = pinned, .index_offset = offset, .namespace_generation = namespace, .expected = expected, .control = .{ .epoch = proof.maintenance_epoch, .last_request = proof.last_maintenance_request }, .next = progress, .candidates = candidates.items, .failed_source = failed_source, .records_examined = inspected };
    }

    /// Caller holds apply-exclusive and rechecks its HA/ownership authority.
    /// No progress is published before the same transaction's index writes.
    pub fn commit(self: *Page, core: anytype) !void {
        if (self.consumed) return error.RelationalIndexPageConsumed;
        self.consumed = true;
        if (core.schemaNamespaceGeneration() != self.namespace_generation or !core.relational_indexes.isCurrent(self.pinned))
            return error.PreparedGenerationChanged;
        var manager = try core.initTxnManager();
        defer manager.deinit();
        try manager.checkOrdinaryWriteConflict(&maintenance.controlKey(self.next.id));
        var txn = try core.store.beginWriteTxn();
        errdefer txn.abort();
        if (!std.mem.eql(u8, &self.next.owner, &(try ownership(&txn)))) return error.PreparedGenerationChanged;
        const control = try maintenance.readControl(&txn, self.next.id);
        if (control.epoch != self.control.epoch or !std.mem.eql(u8, &control.last_request, &self.control.last_request)) return error.PreparedGenerationChanged;
        const key = progressKey(self.next.id);
        const actual = try getOptional(&txn, &key);
        if ((actual == null) != (self.expected == null) or (actual != null and !std.mem.eql(u8, actual.?, self.expected.?)))
            return error.PreparedGenerationChanged;
        if (self.failed_source) |failed| {
            // A repaired/deleted bad row invalidates failure just as a changed
            // source invalidates successful work. Do not publish stale failure.
            const current = (try getOptional(&txn, failed.primary)) orelse return error.PreparedGenerationChanged;
            if (!std.mem.eql(u8, &failed.hash, &digest(current))) return error.PreparedGenerationChanged;
        }
        var writer = records.Writer.init(self.arena.allocator());
        defer writer.deinit();
        const index = self.pinned.plan.boundIndexes()[self.index_offset];
        for (self.candidates) |candidate| {
            if (candidate.primary) |primary| {
                const current = try getOptional(&txn, primary);
                if ((current == null) != (candidate.hash == null)) continue;
                if (current) |bytes| if (!std.mem.eql(u8, &candidate.hash.?, &digest(bytes))) continue;
            }
            if (candidate.observed_key) |observed| {
                const current = (try getOptional(&txn, observed)) orelse continue;
                if (!std.mem.eql(u8, &candidate.observed_hash, &digest(current))) continue;
            }
            if (candidate.delete_observed) try deleteDerived(self.arena.allocator(), &txn, candidate.observed_key.?);
            if (candidate.delete_reverse) |reverse| try deleteDerived(self.arena.allocator(), &txn, reverse);
            if (candidate.nonmember) try writer.removeForRepair(&txn, index, candidate.document);
            if (candidate.tuple) |tuple| _ = try writer.repairCovered(&txn, index, candidate.document, tuple, candidate.payload);
        }
        const encoded = try self.next.encode(self.arena.allocator());
        try txn.put(&key, encoded);
        try txn.commit();
    }
};

fn deleteDerived(alloc: Allocator, txn: anytype, key: []const u8) !void {
    if (records.isOwnershipKey(key)) {
        var reverse = std.ArrayList(u8).empty;
        defer reverse.deinit(alloc);
        // Malformed ownership keys have no reconstructible companion.
        records.appendReverseFromOwnership(alloc, &reverse, key) catch |err| switch (err) {
            error.InvalidRelationalIndexReverseKey, error.InvalidRelationalIndexId => {},
            else => return err,
        };
        if (reverse.items.len != 0) try deleteDerived(alloc, txn, reverse.items);
    }
    if (internal.isRelationalIndexReverseKey(key)) {
        var ownership_key = std.ArrayList(u8).empty;
        defer ownership_key.deinit(alloc);
        try records.appendOwnershipFromReverse(alloc, &ownership_key, key);
        txn.delete(ownership_key.items) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }
    txn.delete(key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}

test "relational index progress checksums framing and readiness are strict" {
    const alloc = std.testing.allocator;
    const original = Progress{ .id = .{ .generation = 7, .slot = 2 }, .owner = @splat(1), .comparison = @splat(2), .rows_scanned = 19, .cursor = "\x01row\x00\x00\x12" };
    const encoded = try original.encode(alloc);
    defer alloc.free(encoded);
    const decoded = try Progress.decode(encoded);
    try std.testing.expectEqualStrings(original.cursor, decoded.cursor);
    try std.testing.expectEqual(original.rows_scanned, decoded.rows_scanned);
    encoded[20] ^= 1;
    try std.testing.expectError(error.InvalidRelationalIndexProgress, Progress.decode(encoded));
    var invalid = original;
    invalid.state = .ready;
    try std.testing.expectError(error.InvalidRelationalIndexProgress, invalid.encode(alloc));
}
