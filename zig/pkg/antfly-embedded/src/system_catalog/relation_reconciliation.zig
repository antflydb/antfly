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

//! Resumable, isolated relation ownership reconciliation. Preparation runs in
//! a pinned source transaction; apply runs in the metadata owner's existing
//! transaction and must abort on error. No reader/writer activation happens
//! here: ready candidates still require capability-gated root publication.
const std = @import("std");
const names = @import("relation_names.zig");
const A = std.mem.Allocator;
const job_prefix = "\x00\x00__metadata__:sql_relation_reconciliation:v1:";
const candidate_prefix = "\x00\x00__metadata_derived__:sql_relation_candidate:v1:";
pub const max_cursor_bytes = 512;
pub const max_tables_per_page = 64;
pub const max_page_bytes = 4 * 1024 * 1024;

pub const Epoch = struct {
    incarnation: [16]u8,
    revision: u64,
    pub fn eql(self: Epoch, other: Epoch) bool {
        return self.revision == other.revision and std.mem.eql(u8, &self.incarnation, &other.incarnation);
    }
};
pub const Phase = enum(u8) { building = 1, verifying_source = 2, verifying_candidate = 3, ready = 4 };
pub const Totals = struct {
    rows: u64 = 0,
    claims: u64 = 0,
    source_hash: [32]u8 = @splat(0),
    // Addition modulo 2^256 of domain-separated claim hashes is independent
    // of source-table order versus candidate-name order. Counts and the
    // ordered source hash are verified separately; XOR would cancel pairs.
    claim_hash: [32]u8 = @splat(0),
};
pub const State = struct {
    group_id: u64,
    job_id: [16]u8,
    epoch: Epoch,
    phase: Phase = .building,
    cursor_len: u16 = 0,
    cursor_bytes: [max_cursor_bytes]u8 = @splat(0),
    expected: Totals = .{},
    pass: Totals = .{},

    pub fn init(group: u64, id: [16]u8, epoch: Epoch) !State {
        const state: State = .{ .group_id = group, .job_id = id, .epoch = epoch };
        try state.validate();
        return state;
    }
    pub fn cursor(self: *const State) []const u8 {
        return self.cursor_bytes[0..self.cursor_len];
    }
    fn setCursor(self: *State, bytes: []const u8) !void {
        if (bytes.len > max_cursor_bytes) return error.CatalogCommandTooLarge;
        self.cursor_len = @intCast(bytes.len);
        @memset(&self.cursor_bytes, 0);
        @memcpy(self.cursor_bytes[0..bytes.len], bytes);
    }
    fn validate(self: *const State) !void {
        if (self.group_id == 0 or std.mem.allEqual(u8, &self.job_id, 0) or
            std.mem.allEqual(u8, &self.epoch.incarnation, 0) or self.cursor_len > max_cursor_bytes or
            !std.mem.allEqual(u8, self.cursor_bytes[self.cursor_len..], 0)) return error.InvalidCatalogRecord;
        if (self.phase == .building and !totalsEqual(self.expected, .{})) return error.InvalidCatalogRecord;
        if (self.expected.claims < self.expected.rows or
            (self.phase != .verifying_candidate and self.pass.claims < self.pass.rows)) return error.InvalidCatalogRecord;
        if (self.phase == .verifying_candidate and
            (self.pass.rows != 0 or !std.mem.allEqual(u8, &self.pass.source_hash, 0))) return error.InvalidCatalogRecord;
        if (self.phase != .building and self.pass.claims > self.expected.claims) return error.InvalidCatalogRecord;
        if (self.phase == .ready and (self.cursor_len != 0 or !totalsEqual(self.pass, .{}))) return error.InvalidCatalogRecord;
    }
    const magic = "AFRC01";
    pub const encoded_len = magic.len + 8 + 16 + 16 + 8 + 1 + 2 + max_cursor_bytes + 2 * (8 + 8 + 32 + 32);
    pub fn encode(self: *const State) ![encoded_len]u8 {
        try self.validate();
        var out: [encoded_len]u8 = undefined;
        @memcpy(out[0..magic.len], magic);
        var offset: usize = magic.len;
        inline for (.{ self.group_id, self.job_id, self.epoch.incarnation, self.epoch.revision, @as(u8, @backingInt(self.phase)), self.cursor_len, self.cursor_bytes, self.expected.rows, self.expected.claims, self.expected.source_hash, self.expected.claim_hash, self.pass.rows, self.pass.claims, self.pass.source_hash, self.pass.claim_hash }) |value| {
            const T = @TypeOf(value);
            const size = @sizeOf(T);
            switch (@typeInfo(T)) {
                .int => std.mem.writeInt(T, out[offset..][0..size], value, .big),
                .array => @memcpy(out[offset..][0..size], &value),
                else => unreachable,
            }
            offset += size;
        }
        return out;
    }
    pub fn decode(bytes: []const u8) !State {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..magic.len], magic)) return error.InvalidCatalogRecord;
        var out: State = undefined;
        var phase: u8 = undefined;
        var offset: usize = magic.len;
        inline for (.{ &out.group_id, &out.job_id, &out.epoch.incarnation, &out.epoch.revision, &phase, &out.cursor_len, &out.cursor_bytes, &out.expected.rows, &out.expected.claims, &out.expected.source_hash, &out.expected.claim_hash, &out.pass.rows, &out.pass.claims, &out.pass.source_hash, &out.pass.claim_hash }) |ptr| {
            const T = @typeInfo(@TypeOf(ptr)).pointer.child;
            const size = @sizeOf(T);
            ptr.* = switch (@typeInfo(T)) {
                .int => std.mem.readInt(T, bytes[offset..][0..size], .big),
                .array => bytes[offset..][0..size].*,
                else => unreachable,
            };
            offset += size;
        }
        out.phase = std.enums.fromInt(Phase, phase) orelse return error.InvalidCatalogRecord;
        try out.validate();
        return out;
    }
};

pub fn jobKey(buf: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidCatalogRecord;
    if (buf.len < job_prefix.len + 8) return error.NoSpaceLeft;
    @memcpy(buf[0..job_prefix.len], job_prefix);
    std.mem.writeInt(u64, buf[job_prefix.len..][0..8], group, .big);
    return buf[0 .. job_prefix.len + 8];
}
pub fn candidatePrefix(buf: []u8, state: *const State) ![]const u8 {
    try state.validate();
    if (buf.len < candidate_prefix.len + 24) return error.NoSpaceLeft;
    @memcpy(buf[0..candidate_prefix.len], candidate_prefix);
    std.mem.writeInt(u64, buf[candidate_prefix.len..][0..8], state.group_id, .big);
    @memcpy(buf[candidate_prefix.len + 8 ..][0..16], &state.job_id);
    return buf[0 .. candidate_prefix.len + 24];
}
pub fn candidateKey(buf: []u8, state: *const State, key: names.Key) ![]const u8 {
    try key.validate();
    const prefix = try candidatePrefix(buf, state);
    const end = prefix.len + 10 + key.name.len;
    if (buf.len < end) return error.NoSpaceLeft;
    std.mem.writeInt(u64, buf[prefix.len..][0..8], key.namespace_id, .big);
    std.mem.writeInt(u16, buf[prefix.len + 8 ..][0..2], @intCast(key.name.len), .big);
    @memcpy(buf[prefix.len + 10 .. end], key.name);
    return buf[0..end];
}
fn decodeCandidateKey(bytes: []const u8, state: *const State) !names.Key {
    var buf: [max_cursor_bytes]u8 = undefined;
    const prefix = try candidatePrefix(&buf, state);
    if (!std.mem.startsWith(u8, bytes, prefix) or bytes.len < prefix.len + 10) return error.InvalidCatalogRecord;
    const tail = bytes[prefix.len..];
    if (tail.len != 10 + @as(usize, std.mem.readInt(u16, tail[8..10], .big))) return error.InvalidCatalogRecord;
    const key: names.Key = .{ .namespace_id = std.mem.readInt(u64, tail[0..8], .big), .name = tail[10..] };
    try key.validate();
    return key;
}

/// The caller must prove that the job ID is fresh (never reuse an abandoned
/// candidate ID), and fence the authoritative source epoch in this transaction.
/// A replacement job leaves its old candidate isolated for bounded GC.
pub fn start(txn: anytype, state: *const State, current_epoch: Epoch, prior: ?[]const u8) !void {
    try checkEpoch(state, current_epoch);
    if (state.phase != .building or state.cursor_len != 0 or !totalsEqual(state.pass, .{})) return error.InvalidCatalogRecord;
    var buf: [128]u8 = undefined;
    const key = try jobKey(&buf, state.group_id);
    const found = txn.get(key) catch |err| blk: {
        if (err == error.NotFound) break :blk null;
        return err;
    };
    if (prior) |expected| {
        if (found == null or !std.mem.eql(u8, expected, found.?)) return error.CatalogGenerationChanged;
        const old = try State.decode(expected);
        if (old.group_id != state.group_id) return error.InvalidCatalogRecord;
        if (std.mem.eql(u8, &old.job_id, &state.job_id)) return error.CatalogGenerationChanged;
    } else if (found != null) return error.CatalogGenerationChanged;
    var candidate_buf: [max_cursor_bytes]u8 = undefined;
    if (try txn.hasPrefix(try candidatePrefix(&candidate_buf, state))) return error.CatalogGenerationChanged;
    const bytes = try state.encode();
    try txn.put(key, &bytes);
}

pub const SourceRow = struct { key: []const u8, table_id: u64, claims: []const names.Claim };
pub const CandidateRow = struct { key: []const u8, value: []const u8 };
pub const Page = struct {
    plan: names.Plan,
    before: State,
    after: State,
    claims: []const names.Claim,

    /// Source.nextAfter(cursor) must stream the complete authoritative table
    /// range, strictly after the lexical cursor, in a pinned source snapshot.
    /// Numeric table-ID ordering is not physical decimal-key ordering.
    /// It must honor the supplied cursor even when called again with an older
    /// one: a row inspected for byte/claim admission may not fit this page.
    /// Workers may instead open a fresh source cursor for every prepared page.
    pub fn prepareSource(a: A, state: State, epoch: Epoch, source: anytype) !Page {
        try checkEpoch(&state, epoch);
        if (state.phase != .building and state.phase != .verifying_source) return error.InvalidCatalogRecord;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const owned = arena.allocator();
        var after = state;
        var claims: std.ArrayList(names.Claim) = .empty;
        var bytes: usize = 0;
        var rows: usize = 0;
        while (rows < max_tables_per_page) {
            const row: SourceRow = (try source.nextAfter(after.cursor())) orelse {
                try finishSource(&after);
                break;
            };
            if (row.table_id == 0 or row.key.len == 0 or row.key.len > max_cursor_bytes or
                std.mem.order(u8, after.cursor(), row.key) != .lt or row.claims.len == 0) return error.InvalidCatalogRecord;
            if (row.claims.len > names.max_claims) return error.CatalogCommandTooLarge;
            var row_bytes: usize = row.key.len;
            for (row.claims) |claim| {
                try claim.key.validate();
                try claim.owner.validate();
                if (claim.owner.table_id != row.table_id) return error.InvalidCatalogRecord;
                row_bytes += claim.key.name.len + names.Owner.encoded_len + 10;
            }
            if (row_bytes > max_page_bytes) return error.CatalogCommandTooLarge;
            if (rows != 0 and (claims.items.len + row.claims.len > names.max_claims or bytes + row_bytes > max_page_bytes)) break;
            bytes += row_bytes;
            rows += 1;
            try appendSource(&after.pass, row);
            for (row.claims) |claim| try claims.append(owned, .{ .key = .{ .namespace_id = claim.key.namespace_id, .name = try owned.dupe(u8, claim.key.name) }, .owner = claim.owner });
            try after.setCursor(row.key);
        }
        // Reject duplicate names within this page before entering apply.
        const plan = try names.Plan.init(a, &.{}, claims.items);
        return .{ .plan = plan, .before = state, .after = after, .claims = plan.after };
    }

    /// Independently stream the isolated candidate range after source
    /// verification. Exact key/value fingerprints plus cardinality detect
    /// injected, omitted and forged entries without a whole-catalog set.
    pub fn prepareCandidate(a: A, state: State, epoch: Epoch, source: anytype) !Page {
        try checkEpoch(&state, epoch);
        if (state.phase != .verifying_candidate) return error.InvalidCatalogRecord;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const owned = arena.allocator();
        var after = state;
        var claims: std.ArrayList(names.Claim) = .empty;
        // Fixed cardinality/byte budgets guarantee yielding even on tiny rows.
        while (claims.items.len < max_tables_per_page) {
            const row: CandidateRow = (try source.nextAfter(after.cursor())) orelse {
                if (after.pass.claims != after.expected.claims or !std.mem.eql(u8, &after.pass.claim_hash, &after.expected.claim_hash)) return error.InvalidCatalogRecord;
                after.phase = .ready;
                after.pass = .{};
                try after.setCursor("");
                break;
            };
            if (std.mem.order(u8, after.cursor(), row.key) != .lt or row.key.len > max_cursor_bytes) return error.InvalidCatalogRecord;
            const key = try decodeCandidateKey(row.key, &state);
            const owner = try names.Owner.decode(row.value);
            after.pass.claims = std.math.add(u64, after.pass.claims, 1) catch return error.CatalogCommandTooLarge;
            if (after.pass.claims > after.expected.claims) return error.InvalidCatalogRecord;
            addClaimHash(&after.pass.claim_hash, try claimHash(.{ .key = key, .owner = owner }));
            try claims.append(owned, .{ .key = .{ .namespace_id = key.namespace_id, .name = try owned.dupe(u8, key.name) }, .owner = owner });
            try after.setCursor(row.key);
        }
        const plan = try names.Plan.init(a, &.{}, claims.items);
        return .{ .plan = plan, .before = state, .after = after, .claims = plan.after };
    }

    pub fn deinit(self: *Page) void {
        self.plan.deinit();
        self.* = undefined;
    }

    /// A failed apply MUST abort the caller's transaction. The job-state CAS
    /// rejects duplicate/stale deliveries, including deliveries after restart.
    /// Epoch comparison must use the authoritative source epoch read in this
    /// same transaction, not the preparer's cached value.
    pub fn apply(self: *const Page, txn: anytype, current_epoch: Epoch) !void {
        try checkEpoch(&self.before, current_epoch);
        var buf: [128]u8 = undefined;
        const key = try jobKey(&buf, self.before.group_id);
        const expected = try self.before.encode();
        const found = txn.get(key) catch |err| {
            if (err == error.NotFound) return error.CatalogGenerationChanged;
            return err;
        };
        if (!std.mem.eql(u8, &expected, found)) return error.CatalogGenerationChanged;
        var store = CandidateStore(@typeInfo(@TypeOf(txn)).pointer.child){ .txn = txn, .state = &self.before };
        if (self.before.phase == .building) {
            var writer: CandidateWriter(@typeInfo(@TypeOf(txn)).pointer.child) = .{ .base = store };
            try self.plan.apply(&writer);
        } else try self.plan.verifyPublished(&store);
        const value = try self.after.encode();
        try txn.put(key, &value);
    }
};

/// Read-only inspection of an isolated candidate. Candidate mutation is
/// private to the job-state-fenced building page; no supported producer may
/// change an earlier page after verification has begun.
pub fn CandidateStore(comptime Txn: type) type {
    return struct {
        txn: *Txn,
        state: *const State,
        pub fn getClaim(self: *@This(), key: names.Key) !?names.Owner {
            var buf: [max_cursor_bytes]u8 = undefined;
            const value = self.txn.get(try candidateKey(&buf, self.state, key)) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
            return try names.Owner.decode(value);
        }
    };
}
fn CandidateWriter(comptime Txn: type) type {
    return struct {
        base: CandidateStore(Txn),
        pub fn getClaim(self: *@This(), key: names.Key) !?names.Owner {
            return self.base.getClaim(key);
        }
        pub fn putClaim(self: *@This(), key: names.Key, owner: names.Owner) !void {
            var buf: [max_cursor_bytes]u8 = undefined;
            const value = try owner.encode();
            try self.base.txn.put(try candidateKey(&buf, self.base.state, key), &value);
        }
        pub fn deleteClaim(self: *@This(), key: names.Key) !void {
            var buf: [max_cursor_bytes]u8 = undefined;
            try self.base.txn.delete(try candidateKey(&buf, self.base.state, key));
        }
    };
}
fn checkEpoch(state: *const State, epoch: Epoch) !void {
    try state.validate();
    if (!state.epoch.eql(epoch)) return error.CatalogGenerationChanged;
}
fn totalsEqual(left: Totals, right: Totals) bool {
    return left.rows == right.rows and left.claims == right.claims and std.mem.eql(u8, &left.source_hash, &right.source_hash) and std.mem.eql(u8, &left.claim_hash, &right.claim_hash);
}
fn finishSource(state: *State) !void {
    if (state.phase == .building) {
        state.expected = state.pass;
        state.phase = .verifying_source;
    } else {
        if (!totalsEqual(state.expected, state.pass)) return error.InvalidCatalogRecord;
        state.phase = .verifying_candidate;
    }
    state.pass = .{};
    try state.setCursor("");
}
fn claimHash(claim: names.Claim) ![32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.relation-candidate.claim.v1");
    var key: [10]u8 = undefined;
    std.mem.writeInt(u64, key[0..8], claim.key.namespace_id, .big);
    std.mem.writeInt(u16, key[8..10], @intCast(claim.key.name.len), .big);
    hash.update(&key);
    hash.update(claim.key.name);
    const owner = try claim.owner.encode();
    hash.update(&owner);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}
fn addClaimHash(sum: *[32]u8, value: [32]u8) void {
    var carry: u16 = 0;
    for (sum, value) |*byte, add| {
        carry += @as(u16, byte.*) + add;
        byte.* = @truncate(carry);
        carry >>= 8;
    }
}
fn appendSource(totals: *Totals, row: SourceRow) !void {
    totals.rows = std.math.add(u64, totals.rows, 1) catch return error.CatalogCommandTooLarge;
    totals.claims = std.math.add(u64, totals.claims, row.claims.len) catch return error.CatalogCommandTooLarge;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.relation-candidate.source.v1");
    hash.update(&totals.source_hash);
    var numbers: [18]u8 = undefined;
    std.mem.writeInt(u16, numbers[0..2], @intCast(row.key.len), .big);
    std.mem.writeInt(u64, numbers[2..10], row.table_id, .big);
    std.mem.writeInt(u64, numbers[10..18], row.claims.len, .big);
    hash.update(&numbers);
    hash.update(row.key);
    for (row.claims) |claim| {
        const digest = try claimHash(claim);
        hash.update(&digest);
        addClaimHash(&totals.claim_hash, digest);
    }
    hash.final(&totals.source_hash);
}

const TestSource = struct {
    rows: []const SourceRow,
    pub fn nextAfter(self: *@This(), cursor: []const u8) !?SourceRow {
        for (self.rows) |row| if (std.mem.order(u8, cursor, row.key) == .lt) return row;
        return null;
    }
};
const TestCandidates = struct {
    rows: []const CandidateRow,
    pub fn nextAfter(self: *@This(), cursor: []const u8) !?CandidateRow {
        for (self.rows) |row| if (std.mem.order(u8, cursor, row.key) == .lt) return row;
        return null;
    }
};
const test_epoch: Epoch = .{ .incarnation = @splat(1), .revision = 17 };
const test_owner: names.Owner = .{ .table_id = 7, .schema_version = 3, .schema_digest = @splat(2), .kind = .table };
const test_claims = [_]names.Claim{.{ .key = .{ .namespace_id = 5, .name = "orders" }, .owner = test_owner }};
const test_rows = [_]SourceRow{.{ .key = "table:7", .table_id = 7, .claims = &test_claims }};

test "relation reconciliation binary state rejects unknown and noncanonical bytes" {
    const state = try State.init(41, @splat(3), test_epoch);
    const encoded = try state.encode();
    const decoded = try State.decode(&encoded);
    try std.testing.expectEqualSlices(u8, &encoded, &(try decoded.encode()));
    var bad = encoded;
    bad[0] = 'X';
    try std.testing.expectError(error.InvalidCatalogRecord, State.decode(&bad));
    bad = encoded;
    bad[6 + 8 + 16 + 16 + 8] = 99;
    try std.testing.expectError(error.InvalidCatalogRecord, State.decode(&bad));
    bad = encoded;
    bad[6 + 8 + 16 + 16 + 8 + 1 + 2] = 1;
    try std.testing.expectError(error.InvalidCatalogRecord, State.decode(&bad));
    try std.testing.expectError(error.InvalidCatalogRecord, State.decode(encoded[0 .. encoded.len - 1]));
    try std.testing.expectError(error.InvalidCatalogRecord, State.init(0, @splat(3), test_epoch));
    try std.testing.expectError(error.InvalidCatalogRecord, State.init(41, @splat(0), test_epoch));
}

test "relation reconciliation requires source and independent candidate verification" {
    const a = std.testing.allocator;
    const state = try State.init(41, @splat(3), test_epoch);
    var source: TestSource = .{ .rows = &test_rows };
    var build = try Page.prepareSource(a, state, test_epoch, &source);
    defer build.deinit();
    try std.testing.expectEqual(Phase.verifying_source, build.after.phase);
    try std.testing.expectEqual(@as(u64, 1), build.after.expected.rows);
    var verified = try Page.prepareSource(a, build.after, test_epoch, &source);
    defer verified.deinit();
    try std.testing.expectEqual(Phase.verifying_candidate, verified.after.phase);
    var key_buf: [max_cursor_bytes]u8 = undefined;
    const key = try candidateKey(&key_buf, &state, test_claims[0].key);
    const owner = try test_owner.encode();
    const candidates = [_]CandidateRow{.{ .key = key, .value = &owner }};
    var candidate_source: TestCandidates = .{ .rows = &candidates };
    var ready = try Page.prepareCandidate(a, verified.after, test_epoch, &candidate_source);
    defer ready.deinit();
    try std.testing.expectEqual(Phase.ready, ready.after.phase);
    try std.testing.expectEqual(@as(usize, 0), ready.after.cursor().len);
    try std.testing.expectError(error.InvalidCatalogRecord, Page.prepareSource(a, ready.after, test_epoch, &source));
    var missing_source: TestSource = .{ .rows = &.{} };
    try std.testing.expectError(error.InvalidCatalogRecord, Page.prepareSource(a, build.after, test_epoch, &missing_source));
    var missing_candidates: TestCandidates = .{ .rows = &.{} };
    try std.testing.expectError(error.InvalidCatalogRecord, Page.prepareCandidate(a, verified.after, test_epoch, &missing_candidates));
    var forged_owner = test_owner;
    forged_owner.schema_digest[0] ^= 1;
    const forged = try forged_owner.encode();
    const bad_rows = [_]CandidateRow{.{ .key = key, .value = &forged }};
    var bad_source: TestCandidates = .{ .rows = &bad_rows };
    try std.testing.expectError(error.InvalidCatalogRecord, Page.prepareCandidate(a, verified.after, test_epoch, &bad_source));
    var wrong_group = state;
    wrong_group.group_id = 42;
    const bad_key = try candidateKey(&key_buf, &wrong_group, test_claims[0].key);
    const wrong_rows = [_]CandidateRow{.{ .key = bad_key, .value = &owner }};
    var wrong_source: TestCandidates = .{ .rows = &wrong_rows };
    try std.testing.expectError(error.InvalidCatalogRecord, Page.prepareCandidate(a, verified.after, test_epoch, &wrong_source));
}

test "relation reconciliation pages bound work and resume by lexical cursor" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    const rows = try owned.alloc(SourceRow, max_tables_per_page + 1);
    for (rows, 0..) |*row, i| {
        const key = try std.fmt.allocPrint(owned, "table:{d:0>3}", .{i});
        const claims = try owned.alloc(names.Claim, 1);
        claims[0] = test_claims[0];
        claims[0].key.name = key;
        claims[0].owner.table_id = i + 1;
        row.* = .{ .key = key, .table_id = i + 1, .claims = claims };
    }
    var source: TestSource = .{ .rows = rows };
    const state = try State.init(41, @splat(3), test_epoch);
    var first = try Page.prepareSource(a, state, test_epoch, &source);
    defer first.deinit();
    try std.testing.expectEqual(Phase.building, first.after.phase);
    try std.testing.expectEqual(@as(u64, max_tables_per_page), first.after.pass.rows);
    try std.testing.expectEqualSlices(u8, rows[max_tables_per_page - 1].key, first.after.cursor());
    // Binary restart preserves the exact continuation point.
    const restarted = try State.decode(&(try first.after.encode()));
    var second = try Page.prepareSource(a, restarted, test_epoch, &source);
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 1), second.claims.len);
    try std.testing.expectEqual(Phase.verifying_source, second.after.phase);
    try std.testing.expectEqual(@as(u64, max_tables_per_page + 1), second.after.expected.rows);
    try std.testing.expectError(error.CatalogGenerationChanged, Page.prepareSource(a, state, .{ .incarnation = test_epoch.incarnation, .revision = 18 }, &source));
    try std.testing.expectError(error.CatalogGenerationChanged, Page.prepareSource(a, state, .{ .incarnation = @splat(4), .revision = test_epoch.revision }, &source));
}

test "relation reconciliation preparation unwinds allocation faults" {
    const T = struct {
        fn run(a: A) !void {
            const state = try State.init(41, @splat(3), test_epoch);
            var source: TestSource = .{ .rows = &test_rows };
            var page = try Page.prepareSource(a, state, test_epoch, &source);
            defer page.deinit();
            var verify = try Page.prepareSource(a, page.after, test_epoch, &source);
            defer verify.deinit();
            var buf: [max_cursor_bytes]u8 = undefined;
            const key = try candidateKey(&buf, &state, test_claims[0].key);
            const owner = try test_owner.encode();
            const rows = [_]CandidateRow{.{ .key = key, .value = &owner }};
            var candidates: TestCandidates = .{ .rows = &rows };
            var ready = try Page.prepareCandidate(a, verify.after, test_epoch, &candidates);
            defer ready.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, T.run, .{});
}

const TestTxn = struct {
    arena: std.heap.ArenaAllocator,
    values: std.StringHashMapUnmanaged([]const u8) = .empty,
    fn init() TestTxn {
        return .{ .arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    }
    fn deinit(self: *TestTxn) void {
        self.arena.deinit();
    }
    pub fn get(self: *TestTxn, key: []const u8) ![]const u8 {
        return self.values.get(key) orelse error.NotFound;
    }
    pub fn put(self: *TestTxn, key: []const u8, value: []const u8) !void {
        const a = self.arena.allocator();
        try self.values.put(a, try a.dupe(u8, key), try a.dupe(u8, value));
    }
    pub fn delete(self: *TestTxn, key: []const u8) !void {
        _ = self.values.remove(key);
    }
    pub fn hasPrefix(self: *TestTxn, prefix: []const u8) !bool {
        var keys = self.values.keyIterator();
        while (keys.next()) |key| if (std.mem.startsWith(u8, key.*, prefix)) return true;
        return false;
    }
};

test "relation reconciliation fences replacement jobs and rejects reused candidates" {
    var txn = TestTxn.init();
    defer txn.deinit();
    const a = std.testing.allocator;
    const initial = try State.init(41, @splat(3), test_epoch);
    try start(&txn, &initial, test_epoch, null);
    var source: TestSource = .{ .rows = &test_rows };
    var page = try Page.prepareSource(a, initial, test_epoch, &source);
    defer page.deinit();
    try page.apply(&txn, test_epoch);
    var active: names.Store(TestTxn) = .{ .txn = &txn, .alloc = a, .group_id = initial.group_id };
    try std.testing.expect((try active.getClaim(test_claims[0].key)) == null);
    const before = try page.after.encode();
    const next = try State.init(41, @splat(4), test_epoch);
    try std.testing.expectError(error.CatalogGenerationChanged, start(&txn, &next, test_epoch, null));
    try std.testing.expectError(error.CatalogGenerationChanged, start(&txn, &initial, test_epoch, &before));
    try start(&txn, &next, test_epoch, &before);
    try std.testing.expectError(error.CatalogGenerationChanged, page.apply(&txn, test_epoch));
    const next_encoded = try next.encode();
    // An abandoned candidate cannot be reused even when it is not the current
    // job. This is a prefix seek in the native store, not a candidate scan.
    try std.testing.expectError(error.CatalogGenerationChanged, start(&txn, &initial, test_epoch, &next_encoded));
    var old_candidates: CandidateStore(TestTxn) = .{ .txn = &txn, .state = &initial };
    try std.testing.expect(try old_candidates.getClaim(test_claims[0].key) != null);
    var new_candidates: CandidateStore(TestTxn) = .{ .txn = &txn, .state = &next };
    try std.testing.expect((try new_candidates.getClaim(test_claims[0].key)) == null);
}

test "relation reconciliation rejects collisions crossing page boundaries" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    const rows = try owned.alloc(SourceRow, max_tables_per_page + 1);
    for (rows, 0..) |*row, i| {
        const key = try std.fmt.allocPrint(owned, "table:{d:0>3}", .{i});
        const claims = try owned.alloc(names.Claim, 1);
        claims[0] = test_claims[0];
        claims[0].key.name = if (i == max_tables_per_page) rows[0].claims[0].key.name else key;
        claims[0].owner.table_id = i + 1;
        row.* = .{ .key = key, .table_id = i + 1, .claims = claims };
    }
    var txn = TestTxn.init();
    defer txn.deinit();
    const initial = try State.init(41, @splat(3), test_epoch);
    try start(&txn, &initial, test_epoch, null);
    var source: TestSource = .{ .rows = rows };
    var first = try Page.prepareSource(a, initial, test_epoch, &source);
    defer first.deinit();
    try first.apply(&txn, test_epoch);
    var second = try Page.prepareSource(a, first.after, test_epoch, &source);
    defer second.deinit();
    try std.testing.expectError(error.CatalogAlreadyExists, second.apply(&txn, test_epoch));
    var buf: [128]u8 = undefined;
    try std.testing.expectEqualSlices(u8, &(try first.after.encode()), try txn.get(try jobKey(&buf, initial.group_id)));
}

test "relation reconciliation scratch is bounded independently of source table count" {
    const GeneratedSource = struct {
        key: [32]u8 = undefined,
        claim: [1]names.Claim = undefined,
        pub fn nextAfter(self: *@This(), after: []const u8) !?SourceRow {
            const id: u64 = if (after.len == 0) 1 else (try std.fmt.parseInt(u64, after["table:".len..], 10)) + 1;
            if (id > 1_000_000) return null;
            const key = try std.fmt.bufPrint(&self.key, "table:{d:0>8}", .{id});
            self.claim[0] = test_claims[0];
            self.claim[0].key.name = key;
            self.claim[0].owner.table_id = id;
            return .{ .key = key, .table_id = id, .claims = &self.claim };
        }
    };
    var source: GeneratedSource = .{};
    var buffer: [64 * 1024]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&buffer);
    const state = try State.init(41, @splat(3), test_epoch);
    var page = try Page.prepareSource(bounded.allocator(), state, test_epoch, &source);
    defer page.deinit();
    try std.testing.expectEqual(Phase.building, page.after.phase);
    try std.testing.expectEqual(@as(u64, max_tables_per_page), page.after.pass.rows);
    try std.testing.expectEqual(@as(usize, max_tables_per_page), page.claims.len);
}

test "relation reconciliation claim budget retains the unadmitted source row" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const owned = arena.allocator();
    const claims = try owned.alloc(names.Claim, names.max_claims);
    for (claims, 0..) |*claim, i| {
        claim.* = test_claims[0];
        claim.key.name = try std.fmt.allocPrint(owned, "index_{d}", .{i});
        claim.owner.kind = if (i == 0) .table else .index;
    }
    var second_claim = test_claims[0];
    second_claim.owner.table_id = 8;
    const rows = [_]SourceRow{
        .{ .key = "table:7", .table_id = 7, .claims = claims },
        .{ .key = "table:8", .table_id = 8, .claims = &.{second_claim} },
    };
    var source: TestSource = .{ .rows = &rows };
    const initial = try State.init(41, @splat(3), test_epoch);
    var first = try Page.prepareSource(a, initial, test_epoch, &source);
    defer first.deinit();
    try std.testing.expectEqualSlices(u8, rows[0].key, first.after.cursor());
    try std.testing.expectEqual(@as(usize, names.max_claims), first.claims.len);
    var second = try Page.prepareSource(a, first.after, test_epoch, &source);
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 1), second.claims.len);
    try std.testing.expectEqual(@as(u64, 2), second.after.expected.rows);
    try std.testing.expectEqual(@as(u64, names.max_claims + 1), second.after.expected.claims);
}
