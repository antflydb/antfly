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
const retirement_prefix = "\x00\x00__metadata__:sql_relation_retirement:v1:";
const root_prefix = "\x00\x00__metadata__:sql_relation_root:v1:";
const source_prefix = "\x00\x00__metadata__:sql_relation_source:v1:";
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
/// Permanent for this exact source epoch, not a resource/transport failure.
/// Retain the phase/cursor/totals so recovery can verify partial candidates.
pub const FailureReason = enum(u8) {
    none = 0,
    name_conflict = 1,
    source_limit = 2,
    pub fn fromError(err: anyerror) ?FailureReason {
        return switch (err) {
            error.CatalogAlreadyExists => .name_conflict,
            error.CatalogCommandTooLarge => .source_limit,
            else => null,
        };
    }
};
/// Generation IDs are big-endian monotonic counters, allocated by CAS from
/// the retained current job. They are never recycled, including after GC.
pub const Generation = struct {
    group_id: u64,
    job_id: [16]u8,
    const magic = "AFRG01";
    pub const encoded_len = magic.len + 8 + 16;
    pub fn of(state: *const State) Generation {
        return .{ .group_id = state.group_id, .job_id = state.job_id };
    }
    pub fn eql(self: Generation, other: Generation) bool {
        return self.group_id == other.group_id and std.mem.eql(u8, &self.job_id, &other.job_id);
    }
    fn validate(self: Generation) !void {
        if (self.group_id == 0 or std.mem.allEqual(u8, &self.job_id, 0)) return error.InvalidCatalogRecord;
    }
    pub fn encode(self: Generation) ![encoded_len]u8 {
        try self.validate();
        var bytes: [encoded_len]u8 = undefined;
        @memcpy(bytes[0..magic.len], magic);
        std.mem.writeInt(u64, bytes[magic.len..][0..8], self.group_id, .big);
        @memcpy(bytes[magic.len + 8 ..], &self.job_id);
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Generation {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..magic.len], magic)) return error.InvalidCatalogRecord;
        const result: Generation = .{ .group_id = std.mem.readInt(u64, bytes[magic.len..][0..8], .big), .job_id = bytes[magic.len + 8 ..][0..16].* };
        try result.validate();
        return result;
    }
};
pub fn nextJobId(prior: ?*const State) ![16]u8 {
    const last: u128 = if (prior) |state| blk: {
        try state.validate();
        break :blk std.mem.readInt(u128, &state.job_id, .big);
    } else 0;
    const next = std.math.add(u128, last, 1) catch return error.CatalogGenerationExhausted;
    var bytes: [16]u8 = undefined;
    std.mem.writeInt(u128, &bytes, next, .big);
    return bytes;
}
pub const Retirement = struct {
    generation: Generation,
    cursor_len: u16 = 0,
    cursor_bytes: [max_cursor_bytes]u8 = @splat(0),
    const magic = "AFRT01";
    pub const encoded_len = magic.len + 8 + 16 + 2 + max_cursor_bytes;
    pub fn init(generation: Generation) Retirement {
        return .{ .generation = generation };
    }
    pub fn cursor(self: *const Retirement) []const u8 {
        return self.cursor_bytes[0..self.cursor_len];
    }
    pub fn encode(self: *const Retirement) ![encoded_len]u8 {
        try self.generation.validate();
        if (self.cursor_len > max_cursor_bytes or !std.mem.allEqual(u8, self.cursor_bytes[self.cursor_len..], 0)) return error.InvalidCatalogRecord;
        if (self.cursor_len != 0) _ = try decodeCandidateGenerationKey(self.cursor(), self.generation);
        var bytes: [encoded_len]u8 = undefined;
        @memcpy(bytes[0..magic.len], magic);
        std.mem.writeInt(u64, bytes[magic.len..][0..8], self.generation.group_id, .big);
        @memcpy(bytes[magic.len + 8 ..][0..16], &self.generation.job_id);
        std.mem.writeInt(u16, bytes[magic.len + 24 ..][0..2], self.cursor_len, .big);
        @memcpy(bytes[magic.len + 26 ..], &self.cursor_bytes);
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Retirement {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..magic.len], magic)) return error.InvalidCatalogRecord;
        const result: Retirement = .{
            .generation = .{ .group_id = std.mem.readInt(u64, bytes[magic.len..][0..8], .big), .job_id = bytes[magic.len + 8 ..][0..16].* },
            .cursor_len = std.mem.readInt(u16, bytes[magic.len + 24 ..][0..2], .big),
            .cursor_bytes = bytes[magic.len + 26 ..][0..max_cursor_bytes].*,
        };
        _ = try result.encode();
        return result;
    }
};
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
    failure: FailureReason = .none,
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
        if (self.phase == .ready and self.failure != .none) return error.InvalidCatalogRecord;
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
    const magic = "AFRC03";
    pub const encoded_len = magic.len + 8 + 16 + 16 + 8 + 2 + 2 + max_cursor_bytes + 2 * (8 + 8 + 32 + 32);
    pub fn encode(self: *const State) ![encoded_len]u8 {
        try self.validate();
        var out: [encoded_len]u8 = undefined;
        @memcpy(out[0..magic.len], magic);
        var offset: usize = magic.len;
        inline for (.{ self.group_id, self.job_id, self.epoch.incarnation, self.epoch.revision, @as(u8, @backingInt(self.phase)), @as(u8, @backingInt(self.failure)), self.cursor_len, self.cursor_bytes, self.expected.rows, self.expected.claims, self.expected.source_hash, self.expected.claim_hash, self.pass.rows, self.pass.claims, self.pass.source_hash, self.pass.claim_hash }) |value| {
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
        var failure: u8 = undefined;
        var offset: usize = magic.len;
        inline for (.{ &out.group_id, &out.job_id, &out.epoch.incarnation, &out.epoch.revision, &phase, &failure, &out.cursor_len, &out.cursor_bytes, &out.expected.rows, &out.expected.claims, &out.expected.source_hash, &out.expected.claim_hash, &out.pass.rows, &out.pass.claims, &out.pass.source_hash, &out.pass.claim_hash }) |ptr| {
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
        out.failure = std.enums.fromInt(FailureReason, failure) orelse return error.InvalidCatalogRecord;
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
    return candidateGenerationPrefix(buf, Generation.of(state));
}
pub fn candidateGenerationPrefix(buf: []u8, generation: Generation) ![]const u8 {
    try generation.validate();
    if (buf.len < candidate_prefix.len + 24) return error.NoSpaceLeft;
    @memcpy(buf[0..candidate_prefix.len], candidate_prefix);
    std.mem.writeInt(u64, buf[candidate_prefix.len..][0..8], generation.group_id, .big);
    @memcpy(buf[candidate_prefix.len + 8 ..][0..16], &generation.job_id);
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
    return decodeCandidateGenerationKey(bytes, Generation.of(state));
}
fn decodeCandidateGenerationKey(bytes: []const u8, generation: Generation) !names.Key {
    var buf: [max_cursor_bytes]u8 = undefined;
    const prefix = try candidateGenerationPrefix(&buf, generation);
    if (!std.mem.startsWith(u8, bytes, prefix) or bytes.len < prefix.len + 10) return error.InvalidCatalogRecord;
    const tail = bytes[prefix.len..];
    if (tail.len != 10 + @as(usize, std.mem.readInt(u16, tail[8..10], .big))) return error.InvalidCatalogRecord;
    const key: names.Key = .{ .namespace_id = std.mem.readInt(u64, tail[0..8], .big), .name = tail[10..] };
    try key.validate();
    return key;
}
pub fn logicalCandidateKey(bytes: []const u8, generation: Generation) !names.Key {
    return decodeCandidateGenerationKey(bytes, generation);
}

pub fn rootKey(buf: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidCatalogRecord;
    if (buf.len < root_prefix.len + 8) return error.NoSpaceLeft;
    @memcpy(buf[0..root_prefix.len], root_prefix);
    std.mem.writeInt(u64, buf[root_prefix.len..][0..8], group, .big);
    return buf[0 .. root_prefix.len + 8];
}
pub fn retirementKey(buf: []u8, generation: Generation) ![]const u8 {
    try generation.validate();
    if (buf.len < retirement_prefix.len + 24) return error.NoSpaceLeft;
    @memcpy(buf[0..retirement_prefix.len], retirement_prefix);
    std.mem.writeInt(u64, buf[retirement_prefix.len..][0..8], generation.group_id, .big);
    @memcpy(buf[retirement_prefix.len + 8 ..][0..16], &generation.job_id);
    return buf[0 .. retirement_prefix.len + 24];
}

/// Independent of job/GC progress: only authoritative source mutations advance
/// this clock, in their existing transaction. Zero denotes an untracked source,
/// not an empty catalog.
pub fn sourceKey(buf: []u8, group: u64) ![]const u8 {
    return groupPrefix(buf, .source, group);
}
pub fn sourceRevision(bytes: []const u8) !u64 {
    if (bytes.len != 8) return error.InvalidCatalogRecord;
    const revision = std.mem.readInt(u64, bytes[0..8], .big);
    if (revision == 0) return error.InvalidCatalogRecord;
    return revision;
}
pub fn readSourceRevision(reader: anytype, group: u64) !u64 {
    var buf: [128]u8 = undefined;
    return if (try optionalGet(reader, try sourceKey(&buf, group))) |bytes| try sourceRevision(bytes) else 0;
}
pub fn advanceSource(txn: anytype, group: u64) !void {
    const previous = try readSourceRevision(txn, group);
    const next = std.math.add(u64, previous, 1) catch return error.CatalogGenerationExhausted;
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, next, .big);
    var buf: [128]u8 = undefined;
    try txn.put(try sourceKey(&buf, group), &bytes);
}
/// Ordinary writers must not implicitly enable a new protocol on mixed-version
/// peers. Capability-gated adoption installs the first clock; writers advance
/// only that explicitly tracked authority. No public adoption occurs here.
pub fn advanceTrackedSource(txn: anytype, group: u64) !void {
    if (try readSourceRevision(txn, group) != 0) try advanceSource(txn, group);
}

pub const RecordKind = enum { source, job, root, retirement, candidate };
pub fn allGroupsPrefix(kind: RecordKind) []const u8 {
    return switch (kind) {
        .source => source_prefix,
        .job => job_prefix,
        .root => root_prefix,
        .retirement => retirement_prefix,
        .candidate => candidate_prefix,
    };
}
pub fn groupPrefix(buf: []u8, kind: RecordKind, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidCatalogRecord;
    const prefix = allGroupsPrefix(kind);
    if (buf.len < prefix.len + 8) return error.NoSpaceLeft;
    @memcpy(buf[0..prefix.len], prefix);
    std.mem.writeInt(u64, buf[prefix.len..][0..8], group, .big);
    return buf[0 .. prefix.len + 8];
}
pub fn candidatesForGroup(buf: []u8, group: u64) ![]const u8 {
    return groupPrefix(buf, .candidate, group);
}
pub fn retirementsForGroup(buf: []u8, group: u64) ![]const u8 {
    return groupPrefix(buf, .retirement, group);
}
pub const Record = struct { kind: RecordKind, group_id: u64, generation: ?Generation = null };
pub fn classify(key: []const u8) !?Record {
    inline for (std.meta.tags(RecordKind)) |kind| {
        const prefix = allGroupsPrefix(kind);
        if (std.mem.startsWith(u8, key, prefix)) {
            const tail = key[prefix.len..];
            if (tail.len < 8) return error.InvalidCatalogRecord;
            const group = std.mem.readInt(u64, tail[0..8], .big);
            if (group == 0) return error.InvalidCatalogRecord;
            switch (kind) {
                .source, .job, .root => {
                    if (tail.len != 8) return error.InvalidCatalogRecord;
                    return .{ .kind = kind, .group_id = group };
                },
                .retirement, .candidate => {
                    if (tail.len < 24) return error.InvalidCatalogRecord;
                    const generation: Generation = .{ .group_id = group, .job_id = tail[8..24].* };
                    try generation.validate();
                    if (kind == .retirement) {
                        if (tail.len != 24) return error.InvalidCatalogRecord;
                    } else _ = try decodeCandidateGenerationKey(key, generation);
                    return .{ .kind = kind, .group_id = group, .generation = generation };
                },
            }
        }
    }
    return null;
}
fn optionalGet(reader: anytype, key: []const u8) !?[]const u8 {
    return reader.get(key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}

/// Owned, bounded scheduler observation from one pinned authority. A caller
/// must still use CAS intents: this is neither a reservation nor serving proof.
/// There is only one protected root, so finding the oldest eligible retirement
/// needs one seek and at most one successor, regardless of backlog size.
pub const Work = struct {
    epoch: ?Epoch,
    current: ?State,
    root: ?Generation,
    garbage: ?Retirement = null,

    pub fn read(reader: anytype, group: u64, epoch: ?Epoch) !Work {
        const revision = try readSourceRevision(reader, group);
        if (epoch) |value| {
            if (value.revision == 0 or value.revision != revision or
                std.mem.allEqual(u8, &value.incarnation, 0)) return error.InvalidCatalogRecord;
        } else if (revision != 0) return error.InvalidCatalogRecord;
        const verified = try Verifier(@TypeOf(reader.*)).init(reader, group);
        if (revision == 0 and (verified.current != null or verified.root != null)) return error.InvalidCatalogRecord;
        var result: Work = .{ .epoch = epoch, .current = verified.current, .root = verified.root };
        var buf: [128]u8 = undefined;
        const prefix = try retirementsForGroup(&buf, group);
        var cursor = try reader.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(prefix);
        while (entry) |row| {
            if (!std.mem.startsWith(u8, row.key, prefix)) break;
            const record = (try classify(row.key)) orelse return error.InvalidCatalogRecord;
            const retired = try Retirement.decode(row.value);
            const current = result.current orelse return error.InvalidCatalogRecord;
            if (record.kind != .retirement or record.group_id != group or
                !retired.generation.eql(record.generation.?) or
                std.mem.order(u8, &retired.generation.job_id, &current.job_id) != .lt) return error.InvalidCatalogRecord;
            if (result.root) |root| if (root.eql(retired.generation)) {
                if (retired.cursor_len != 0) return error.InvalidCatalogRecord;
                entry = try cursor.next();
                continue;
            };
            result.garbage = retired;
            break;
        }
        return result;
    }
};

/// A streaming consistency verifier shared by borrowed snapshot maps and
/// pinned checkpoint transactions. It owns no catalog-size map or schema DOM.
/// Candidate state remains immutable/unpublished: root activation and mutable
/// active-generation verification must be wired before serving is enabled.
pub fn Verifier(comptime Reader: type) type {
    return struct {
        reader: *Reader,
        group_id: u64,
        current: ?State,
        root: ?Generation,
        claims: u64 = 0,
        claim_hash: [32]u8 = @splat(0),
        pub fn init(reader: *Reader, group: u64) !@This() {
            var buf: [128]u8 = undefined;
            const current = if (try optionalGet(reader, try jobKey(&buf, group))) |bytes| try State.decode(bytes) else null;
            if (current) |state| if (state.group_id != group) return error.InvalidCatalogRecord;
            const root = if (try optionalGet(reader, try rootKey(&buf, group))) |bytes| try Generation.decode(bytes) else null;
            var result: @This() = .{ .reader = reader, .group_id = group, .current = current, .root = root };
            if (root) |generation| {
                if (generation.group_id != group) return error.InvalidCatalogRecord;
                if (try result.retirementFor(generation)) |retired| {
                    // GC must never have progressed into a published root.
                    if (retired.cursor_len != 0) return error.InvalidCatalogRecord;
                } else if (current.?.phase != .ready) return error.InvalidCatalogRecord;
            }
            return result;
        }
        fn retirementFor(self: *@This(), generation: Generation) !?Retirement {
            const current = self.current orelse return error.InvalidCatalogRecord;
            const order = std.mem.order(u8, &generation.job_id, &current.job_id);
            if (generation.group_id != self.group_id or order == .gt) return error.InvalidCatalogRecord;
            if (order == .eq) return null;
            var buf: [128]u8 = undefined;
            const bytes = (try optionalGet(self.reader, try retirementKey(&buf, generation))) orelse return error.InvalidCatalogRecord;
            const retired = try Retirement.decode(bytes);
            if (!retired.generation.eql(generation)) return error.InvalidCatalogRecord;
            return retired;
        }
        pub fn feed(self: *@This(), key: []const u8, value: []const u8) !void {
            const record = (try classify(key)) orelse return;
            if (record.group_id != self.group_id) return error.InvalidCatalogRecord;
            switch (record.kind) {
                .source => _ = try sourceRevision(value),
                .job => {
                    const state = try State.decode(value);
                    if (self.current == null or !std.mem.eql(u8, &(try self.current.?.encode()), &(try state.encode()))) return error.InvalidCatalogRecord;
                },
                .root => {
                    if (self.root == null or !(try Generation.decode(value)).eql(self.root.?)) return error.InvalidCatalogRecord;
                },
                .retirement => {
                    const retired = try Retirement.decode(value);
                    if (!retired.generation.eql(record.generation.?)) return error.InvalidCatalogRecord;
                    if (try self.retirementFor(retired.generation) == null) return error.InvalidCatalogRecord;
                },
                .candidate => {
                    const generation = record.generation.?;
                    const owner = try names.Entry.decode(value);
                    const logical = try decodeCandidateGenerationKey(key, generation);
                    if (try self.retirementFor(generation)) |retired| {
                        if (std.mem.order(u8, retired.cursor(), key) != .lt) return error.InvalidCatalogRecord;
                    } else {
                        self.claims = std.math.add(u64, self.claims, 1) catch return error.InvalidCatalogRecord;
                        addClaimHash(&self.claim_hash, try claimHash(try names.Claim.fromEntry(logical, owner)));
                    }
                },
            }
        }
        pub fn finish(self: *const @This()) !void {
            if (self.current) |state| {
                const expected = if (state.phase == .building) state.pass else state.expected;
                if (self.claims != expected.claims or !std.mem.eql(u8, &self.claim_hash, &expected.claim_hash)) return error.InvalidCatalogRecord;
            } else if (self.claims != 0) return error.InvalidCatalogRecord;
        }
    };
}

/// Verify authenticated replay against a previously verified receiver cut.
/// Only changed records are fed; before/after readers pin the same metadata
/// transaction. This is not a seed verifier or serving capability proof.
pub fn ReplayVerifier(comptime Before: type, comptime After: type) type {
    return struct {
        arena: std.heap.ArenaAllocator,
        before: *Before,
        after: *After,
        group_id: u64,
        old: ?State,
        current: ?State,
        root: ?Generation,
        count: u64 = 0,
        hash: [32]u8 = @splat(0),
        records: usize = 0,
        retirements: std.AutoHashMapUnmanaged([16]u8, void) = .empty,
        pub fn init(a: A, before: *Before, after: *After, group: u64) !@This() {
            var buf: [128]u8 = undefined;
            const key = try jobKey(&buf, group);
            const old = if (try optionalGet(before, key)) |bytes| try State.decode(bytes) else null;
            const current = if (try optionalGet(after, key)) |bytes| try State.decode(bytes) else null;
            if (old) |prior| {
                const next = current orelse return error.InvalidCatalogRecord;
                if (prior.group_id != group or std.mem.order(u8, &prior.job_id, &next.job_id) == .gt) return error.InvalidCatalogRecord;
                if (std.mem.eql(u8, &prior.job_id, &next.job_id)) {
                    if (prior.failure != .none and !std.meta.eql(prior, next)) return error.InvalidCatalogRecord;
                    if (!prior.epoch.eql(next.epoch) or @backingInt(next.phase) < @backingInt(prior.phase)) return error.InvalidCatalogRecord;
                    if (prior.phase != .building and !totalsEqual(prior.expected, next.expected)) return error.InvalidCatalogRecord;
                    if (prior.phase == next.phase and (next.pass.rows < prior.pass.rows or next.pass.claims < prior.pass.claims or
                        std.mem.order(u8, next.cursor(), prior.cursor()) == .lt)) return error.InvalidCatalogRecord;
                }
            }
            if (current) |next| if (next.group_id != group) return error.InvalidCatalogRecord;
            const root_key = try rootKey(&buf, group);
            const old_root = if (try optionalGet(before, root_key)) |bytes| try Generation.decode(bytes) else null;
            const root = if (try optionalGet(after, root_key)) |bytes| try Generation.decode(bytes) else null;
            if (old_root) |prior| {
                const next = root orelse return error.InvalidCatalogRecord;
                if (prior.group_id != group or std.mem.order(u8, &prior.job_id, &next.job_id) == .gt) return error.InvalidCatalogRecord;
            }
            // Checks root ancestry/readiness with point reads only. Full
            // candidate cardinality is checked by the delta below, not finish.
            _ = try Verifier(After).init(after, group);
            var result: @This() = .{ .arena = .init(a), .before = before, .after = after, .group_id = group, .old = old, .current = current, .root = root };
            if (old) |prior| if (current) |next| if (std.mem.eql(u8, &prior.job_id, &next.job_id)) {
                const totals = if (prior.phase == .building) prior.pass else prior.expected;
                result.count = totals.claims;
                result.hash = totals.claim_hash;
            };
            return result;
        }
        pub fn deinit(self: *@This()) void {
            self.arena.deinit();
            self.* = undefined;
        }
        fn checkRetirement(self: *@This(), generation: Generation) !void {
            if (self.retirements.contains(generation.job_id)) return;
            if (self.retirements.count() == names.max_claims) return error.CatalogCommandTooLarge;
            const current = self.current orelse return error.InvalidCatalogRecord;
            if (generation.group_id != self.group_id or std.mem.order(u8, &generation.job_id, &current.job_id) != .lt) return error.InvalidCatalogRecord;
            var buf: [128]u8 = undefined;
            const key = try retirementKey(&buf, generation);
            const old = if (try optionalGet(self.before, key)) |bytes| try Retirement.decode(bytes) else null;
            const next = if (try optionalGet(self.after, key)) |bytes| try Retirement.decode(bytes) else null;
            if (old) |prior| {
                if (!prior.generation.eql(generation)) return error.InvalidCatalogRecord;
            } else {
                const prior = self.old orelse return error.InvalidCatalogRecord;
                if (!Generation.of(&prior).eql(generation) or std.mem.order(u8, &prior.job_id, &current.job_id) != .lt) return error.InvalidCatalogRecord;
            }
            const resume_key = if (old) |*prior| prior.cursor() else "";
            if (next) |*retired| {
                if (!retired.generation.eql(generation) or std.mem.order(u8, retired.cursor(), resume_key) == .lt) return error.InvalidCatalogRecord;
                if (self.root) |published| if (published.eql(generation) and retired.cursor_len != 0) return error.InvalidCatalogRecord;
            } else if (self.root) |published| if (published.eql(generation)) return error.InvalidCatalogRecord;
            // A bounded successor probe catches skipped GC entries and intent
            // removal with remaining data. Resume past the original cursor,
            // not the generation's already tombstoned prefix.
            var prefix_buf: [max_cursor_bytes]u8 = undefined;
            const prefix = try candidateGenerationPrefix(&prefix_buf, generation);
            var cursor = try self.after.openCursor();
            defer cursor.close();
            var entry = try cursor.seekAtOrAfter(if (resume_key.len == 0) prefix else resume_key);
            if (entry) |row| if (std.mem.eql(u8, row.key, resume_key)) {
                entry = try cursor.next();
            };
            if (entry) |row| if (std.mem.startsWith(u8, row.key, prefix)) {
                const retired = next orelse return error.InvalidCatalogRecord;
                if (std.mem.order(u8, row.key, retired.cursor()) != .gt) return error.InvalidCatalogRecord;
            };
            try self.retirements.put(self.arena.allocator(), generation.job_id, {});
        }
        pub fn feed(self: *@This(), key: []const u8) !void {
            const record = (try classify(key)) orelse return;
            if (record.group_id != self.group_id) return error.InvalidCatalogRecord;
            self.records += 1;
            if (self.records > names.max_claims + max_tables_per_page) return error.CatalogCommandTooLarge;
            const old = try optionalGet(self.before, key);
            const next = try optionalGet(self.after, key);
            switch (record.kind) {
                .source => {
                    const value = next orelse return error.InvalidCatalogRecord;
                    const revision = try sourceRevision(value);
                    if (old) |bytes| if (revision < try sourceRevision(bytes)) return error.InvalidCatalogRecord;
                },
                .job, .root => {}, // Whole-record fences were checked by init.
                .retirement => try self.checkRetirement(record.generation.?),
                .candidate => {
                    const current = self.current orelse return error.InvalidCatalogRecord;
                    const generation = record.generation.?;
                    const prior = if (old) |bytes| try names.Entry.decode(bytes) else null;
                    const owner = if (next) |bytes| try names.Entry.decode(bytes) else null;
                    if (prior) |value| if (owner) |final| if (value.eql(final)) return;
                    if (std.mem.eql(u8, &generation.job_id, &current.job_id)) {
                        if (prior != null or owner == null) return error.InvalidCatalogRecord;
                        if (self.old) |before| if (std.mem.eql(u8, &before.job_id, &current.job_id) and before.phase != .building) return error.InvalidCatalogRecord;
                        self.count = std.math.add(u64, self.count, 1) catch return error.InvalidCatalogRecord;
                        const logical = try decodeCandidateGenerationKey(key, generation);
                        addClaimHash(&self.hash, try claimHash(try names.Claim.fromEntry(logical, owner.?)));
                    } else {
                        if (prior == null or owner != null) return error.InvalidCatalogRecord;
                        try self.checkRetirement(generation);
                        var buf: [128]u8 = undefined;
                        if (try optionalGet(self.after, try retirementKey(&buf, generation))) |bytes| {
                            const retired = try Retirement.decode(bytes);
                            if (std.mem.order(u8, key, retired.cursor()) == .gt) return error.InvalidCatalogRecord;
                        }
                        if (self.root) |published| if (published.eql(generation)) return error.InvalidCatalogRecord;
                    }
                },
            }
        }
        pub fn finish(self: *@This()) !void {
            if (self.current) |current| {
                const expected = if (current.phase == .building) current.pass else current.expected;
                if (self.count != expected.claims or !std.mem.eql(u8, &self.hash, &expected.claim_hash)) return error.InvalidCatalogRecord;
                if (self.old) |old| if (!std.mem.eql(u8, &old.job_id, &current.job_id)) try self.checkRetirement(Generation.of(&old));
            } else if (self.count != 0) return error.InvalidCatalogRecord;
        }
    };
}

/// Fence the authoritative source epoch in the caller's write transaction.
/// The retained job is also a generation high-water mark: never delete it.
/// Replacement atomically retires the old candidate; errors require abort.
pub fn start(txn: anytype, state: *const State, current_epoch: Epoch, prior: ?[]const u8) !void {
    try checkEpoch(state, current_epoch);
    if (state.failure != .none or state.phase != .building or state.cursor_len != 0 or !totalsEqual(state.pass, .{})) return error.InvalidCatalogRecord;
    var buf: [128]u8 = undefined;
    const key = try jobKey(&buf, state.group_id);
    const found = txn.get(key) catch |err| blk: {
        if (err == error.NotFound) break :blk null;
        return err;
    };
    var retired: ?Generation = null;
    if (prior) |expected| {
        if (found == null or !std.mem.eql(u8, expected, found.?)) return error.CatalogGenerationChanged;
        const old = try State.decode(expected);
        if (old.group_id != state.group_id) return error.InvalidCatalogRecord;
        if (std.mem.order(u8, &old.job_id, &state.job_id) != .lt) return error.CatalogGenerationChanged;
        retired = Generation.of(&old);
    } else if (found != null) return error.CatalogGenerationChanged;
    var candidate_buf: [max_cursor_bytes]u8 = undefined;
    if (try txn.hasPrefix(try candidatePrefix(&candidate_buf, state))) return error.CatalogGenerationChanged;
    if (retired) |generation| {
        var retirement_buf: [128]u8 = undefined;
        const retired_key = try retirementKey(&retirement_buf, generation);
        const existing = txn.get(retired_key) catch |err| blk: {
            if (err == error.NotFound) break :blk null;
            return err;
        };
        if (existing != null) return error.InvalidCatalogRecord;
        const retirement = Retirement.init(generation);
        try txn.put(retired_key, &(try retirement.encode()));
    }
    const bytes = try state.encode();
    try txn.put(key, &bytes);
}

pub const SourceRow = struct { key: []const u8, table_id: u64, pending_table_id: ?u64 = null, claims: []const names.Claim };
/// Derived locally from a permanent source error, never accepted as a leader
/// assertion. Recording it changes only the job record in the caller's txn.
/// Stale epochs/CAS and all write errors require the caller to abort.
pub const FailurePlan = struct {
    before: State,
    reason: FailureReason,
    pub fn apply(self: *const FailurePlan, txn: anytype, epoch: Epoch) !void {
        try checkEpoch(&self.before, epoch);
        if (self.reason == .none or self.before.phase == .ready) return error.InvalidCatalogRecord;
        var buf: [128]u8 = undefined;
        const key = try jobKey(&buf, self.before.group_id);
        const found = (try optionalGet(txn, key)) orelse return error.CatalogGenerationChanged;
        if (!std.mem.eql(u8, &(try self.before.encode()), found)) return error.CatalogGenerationChanged;
        var after = self.before;
        after.failure = self.reason;
        try txn.put(key, &(try after.encode()));
    }
};
pub const CandidateRow = struct { key: []const u8, value: []const u8 };
/// An exclusive lexical cursor is committed with every deletion page, so LSM
/// tombstones preceding it are not repeatedly traversed after churn/restart.
/// No O(catalog-size) key list or indefinitely retained per-generation tombstone
/// is needed. The current job high-water mark prevents ABA after intent removal.
pub const GarbagePage = struct {
    arena: std.heap.ArenaAllocator,
    before: Retirement,
    rows: []const CandidateRow,
    pub fn prepare(a: A, before: Retirement, source: anytype) !GarbagePage {
        _ = try before.encode();
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var rows: std.ArrayList(CandidateRow) = .empty;
        var after: []const u8 = before.cursor();
        while (rows.items.len < max_tables_per_page) {
            const row: CandidateRow = (try source.nextAfter(after)) orelse break;
            if (row.key.len > max_cursor_bytes or std.mem.order(u8, after, row.key) != .lt) return error.InvalidCatalogRecord;
            _ = try decodeCandidateGenerationKey(row.key, before.generation);
            _ = try names.Entry.decode(row.value);
            const key = try owned.dupe(u8, row.key);
            try rows.append(owned, .{ .key = key, .value = try owned.dupe(u8, row.value) });
            after = key;
        }
        return .{ .arena = arena, .before = before, .rows = try rows.toOwnedSlice(owned) };
    }
    pub fn deinit(self: *GarbagePage) void {
        self.arena.deinit();
        self.* = undefined;
    }
    /// Returns true only when the retired range and intent are both removed.
    /// The active root and high-water mark are read in this SAME transaction;
    /// publication must likewise fence its current ready job before root swap.
    /// Snapshot readers must pin their metadata transaction across root lookup
    /// and ownership reads. Any error requires abort, including delete failure.
    pub fn apply(self: *const GarbagePage, txn: anytype) !bool {
        if (self.rows.len > max_tables_per_page) return error.CatalogCommandTooLarge;
        const generation = self.before.generation;
        var retirement_buf: [128]u8 = undefined;
        const key = try retirementKey(&retirement_buf, generation);
        const bytes = txn.get(key) catch |err| {
            if (err == error.NotFound) return error.CatalogGenerationChanged;
            return err;
        };
        if (!std.mem.eql(u8, &(try self.before.encode()), bytes)) return error.CatalogGenerationChanged;
        var job_buf: [128]u8 = undefined;
        const current = try State.decode(try txn.get(try jobKey(&job_buf, generation.group_id)));
        if (current.group_id != generation.group_id) return error.InvalidCatalogRecord;
        if (std.mem.order(u8, &generation.job_id, &current.job_id) != .lt) return error.CatalogGenerationChanged;
        var root_buf: [128]u8 = undefined;
        const root = txn.get(try rootKey(&root_buf, generation.group_id)) catch |err| blk: {
            if (err == error.NotFound) break :blk null;
            return err;
        };
        if (root) |value| {
            const published = try Generation.decode(value);
            if (published.group_id != generation.group_id or std.mem.order(u8, &published.job_id, &current.job_id) == .gt) return error.InvalidCatalogRecord;
            if (published.eql(generation)) return error.CatalogGenerationChanged;
        }
        // Check every exact before-image before deleting any candidate entry.
        var after = self.before;
        for (self.rows) |row| {
            _ = try decodeCandidateGenerationKey(row.key, generation);
            if (std.mem.order(u8, after.cursor(), row.key) != .lt) return error.InvalidCatalogRecord;
            after.cursor_len = @intCast(row.key.len);
            @memset(&after.cursor_bytes, 0);
            @memcpy(after.cursor_bytes[0..row.key.len], row.key);
            const found = txn.get(row.key) catch |err| {
                if (err == error.NotFound) return error.CatalogGenerationChanged;
                return err;
            };
            if (!std.mem.eql(u8, row.value, found)) return error.CatalogGenerationChanged;
        }
        for (self.rows) |row| try txn.delete(row.key);
        var prefix_buf: [max_cursor_bytes]u8 = undefined;
        const prefix = try candidateGenerationPrefix(&prefix_buf, generation);
        var cursor = try txn.openCursor();
        defer cursor.close();
        var remaining = try cursor.seekAtOrAfter(if (after.cursor_len == 0) prefix else after.cursor());
        if (remaining) |entry| if (std.mem.eql(u8, entry.key, after.cursor())) {
            remaining = try cursor.next();
        };
        const done = if (remaining) |entry| !std.mem.startsWith(u8, entry.key, prefix) else true;
        if (done) try txn.delete(key) else try txn.put(key, &(try after.encode()));
        return done;
    }
};
pub const Page = struct {
    plan: names.EntryPlan,
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
        var transferred = false;
        defer if (!transferred) arena.deinit();
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
                std.mem.order(u8, after.cursor(), row.key) != .lt or row.claims.len == 0 or row.pending_table_id == 0) return error.InvalidCatalogRecord;
            if (row.claims.len > names.max_claims) return error.CatalogCommandTooLarge;
            var row_bytes: usize = row.key.len;
            for (row.claims) |claim| {
                try claim.key.validate();
                const entry = try claim.entry();
                if (entry.active) |owner| if (owner.table_id != row.table_id) return error.InvalidCatalogRecord;
                if (entry.pending) |owner| if (owner.table_id != (row.pending_table_id orelse row.table_id)) return error.InvalidCatalogRecord;
                row_bytes += claim.key.name.len + names.Entry.encoded_len + 10;
            }
            if (row_bytes > max_page_bytes) return error.CatalogCommandTooLarge;
            if (rows != 0 and (claims.items.len + row.claims.len > names.max_claims or bytes + row_bytes > max_page_bytes)) break;
            bytes += row_bytes;
            rows += 1;
            try appendSource(&after.pass, row);
            for (row.claims) |claim| try claims.append(owned, .{ .key = .{ .namespace_id = claim.key.namespace_id, .name = try owned.dupe(u8, claim.key.name) }, .owner = claim.owner, .pending = claim.pending });
            try after.setCursor(row.key);
        }
        // Reject duplicate names within this page before entering apply.
        transferred = true;
        return own(arena, state, after, claims.items);
    }

    /// Independently stream the isolated candidate range after source
    /// verification. Exact key/value fingerprints plus cardinality detect
    /// injected, omitted and forged entries without a whole-catalog set.
    pub fn prepareCandidate(a: A, state: State, epoch: Epoch, source: anytype) !Page {
        try checkEpoch(&state, epoch);
        if (state.phase != .verifying_candidate) return error.InvalidCatalogRecord;
        var arena = std.heap.ArenaAllocator.init(a);
        var transferred = false;
        defer if (!transferred) arena.deinit();
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
            const owner = try names.Entry.decode(row.value);
            after.pass.claims = std.math.add(u64, after.pass.claims, 1) catch return error.CatalogCommandTooLarge;
            if (after.pass.claims > after.expected.claims) return error.InvalidCatalogRecord;
            const claim = try names.Claim.fromEntry(key, owner);
            addClaimHash(&after.pass.claim_hash, try claimHash(claim));
            try claims.append(owned, .{ .key = .{ .namespace_id = key.namespace_id, .name = try owned.dupe(u8, key.name) }, .owner = claim.owner, .pending = claim.pending });
            try after.setCursor(row.key);
        }
        transferred = true;
        return own(arena, state, after, claims.items);
    }
    fn own(arena: std.heap.ArenaAllocator, before: State, after: State, claims: []const names.Claim) !Page {
        return .{ .plan = try names.EntryPlan.takeClaims(arena, claims), .before = before, .after = after, .claims = claims };
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
        if (self.after.phase == .ready) {
            // Preparation observed EOF in a different pinned transaction.
            // Recheck that boundary before sealing, including an empty final
            // page after an exact page-size cut. Seek past the verified tail,
            // not from the prefix, to avoid rereading the full generation.
            var tail_buf: [max_cursor_bytes]u8 = undefined;
            var tail_len: usize = self.before.cursor_len;
            @memcpy(tail_buf[0..tail_len], self.before.cursor());
            for (self.claims) |claim| {
                var claim_buf: [max_cursor_bytes]u8 = undefined;
                const physical = try candidateKey(&claim_buf, &self.before, claim.key);
                if (std.mem.order(u8, tail_buf[0..tail_len], physical) == .lt) {
                    @memcpy(tail_buf[0..physical.len], physical);
                    tail_len = physical.len;
                }
            }
            var prefix_buf: [max_cursor_bytes]u8 = undefined;
            const prefix = try candidatePrefix(&prefix_buf, &self.before);
            var cursor = try txn.openCursor();
            defer cursor.close();
            var remaining = try cursor.seekAtOrAfter(if (tail_len == 0) prefix else tail_buf[0..tail_len]);
            if (remaining) |row| if (std.mem.eql(u8, row.key, tail_buf[0..tail_len])) {
                remaining = try cursor.next();
            };
            if (remaining) |row| if (std.mem.startsWith(u8, row.key, prefix)) return error.InvalidCatalogRecord;
        }
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
            return if (try self.getEntry(key)) |entry| entry.active else null;
        }
        pub fn getEntry(self: *@This(), key: names.Key) !?names.Entry {
            var buf: [max_cursor_bytes]u8 = undefined;
            const value = self.txn.get(try candidateKey(&buf, self.state, key)) catch |err| {
                if (err == error.NotFound) return null;
                return err;
            };
            return try names.Entry.decode(value);
        }
    };
}
fn CandidateWriter(comptime Txn: type) type {
    return struct {
        base: CandidateStore(Txn),
        pub fn getEntry(self: *@This(), key: names.Key) !?names.Entry {
            return self.base.getEntry(key);
        }
        pub fn putEntry(self: *@This(), key: names.Key, entry: names.Entry) !void {
            var buf: [max_cursor_bytes]u8 = undefined;
            const value = try entry.encode();
            try self.base.txn.put(try candidateKey(&buf, self.base.state, key), &value);
        }
        pub fn deleteEntry(self: *@This(), key: names.Key) !void {
            var buf: [max_cursor_bytes]u8 = undefined;
            try self.base.txn.delete(try candidateKey(&buf, self.base.state, key));
        }
    };
}
fn checkEpoch(state: *const State, epoch: Epoch) !void {
    try state.validate();
    if (state.failure != .none or !state.epoch.eql(epoch)) return error.CatalogGenerationChanged;
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
    hash.update("antfly.relation-candidate.claim.v2");
    var key: [10]u8 = undefined;
    std.mem.writeInt(u64, key[0..8], claim.key.namespace_id, .big);
    std.mem.writeInt(u16, key[8..10], @intCast(claim.key.name.len), .big);
    hash.update(&key);
    hash.update(claim.key.name);
    const owner = try (try claim.entry()).encode();
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
    hash.update("antfly.relation-candidate.source.v2");
    hash.update(&totals.source_hash);
    var numbers: [26]u8 = undefined;
    std.mem.writeInt(u16, numbers[0..2], @intCast(row.key.len), .big);
    std.mem.writeInt(u64, numbers[2..10], row.table_id, .big);
    std.mem.writeInt(u64, numbers[10..18], row.claims.len, .big);
    std.mem.writeInt(u64, numbers[18..26], row.pending_table_id orelse 0, .big);
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
    bad[6 + 8 + 16 + 16 + 8 + 2 + 2] = 1;
    try std.testing.expectError(error.InvalidCatalogRecord, State.decode(&bad));
    bad = encoded;
    bad[6 + 8 + 16 + 16 + 8 + 1] = 99;
    try std.testing.expectError(error.InvalidCatalogRecord, State.decode(&bad));
    try std.testing.expectError(error.InvalidCatalogRecord, State.decode(encoded[0 .. encoded.len - 1]));
    try std.testing.expectError(error.InvalidCatalogRecord, State.init(0, @splat(3), test_epoch));
    try std.testing.expectError(error.InvalidCatalogRecord, State.init(41, @splat(0), test_epoch));
}

test "relation reconciliation permanent failure is fenced terminal and retains verifiable candidates" {
    const a = std.testing.allocator;
    var txn = TestTxn.init();
    defer txn.deinit();
    const initial = try State.init(41, try nextJobId(null), test_epoch);
    try start(&txn, &initial, test_epoch, null);
    var source: TestSource = .{ .rows = &test_rows };
    var page = try Page.prepareSource(a, initial, test_epoch, &source);
    defer page.deinit();
    try page.apply(&txn, test_epoch);
    const failure: FailurePlan = .{ .before = page.after, .reason = .name_conflict };
    var moved = test_epoch;
    moved.revision += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, failure.apply(&txn, moved));
    const stale: FailurePlan = .{ .before = initial, .reason = .name_conflict };
    try std.testing.expectError(error.CatalogGenerationChanged, stale.apply(&txn, test_epoch));
    const Fault = struct {
        base: *TestTxn,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.base.get(key);
        }
        pub fn put(_: *@This(), _: []const u8, _: []const u8) !void {
            return error.InjectedWriteFailure;
        }
    };
    var fault: Fault = .{ .base = &txn };
    try std.testing.expectError(error.InjectedWriteFailure, failure.apply(&fault, test_epoch));
    var buf: [128]u8 = undefined;
    const key = try jobKey(&buf, 41);
    try std.testing.expectEqualSlices(u8, &(try page.after.encode()), try txn.get(key));
    try failure.apply(&txn, test_epoch);
    const failed = try State.decode(try txn.get(key));
    var expected = page.after;
    expected.failure = .name_conflict;
    try std.testing.expect(std.meta.eql(expected, failed));
    try std.testing.expectError(error.CatalogGenerationChanged, failure.apply(&txn, test_epoch));
    try std.testing.expectError(error.CatalogGenerationChanged, Page.prepareSource(a, failed, test_epoch, &source));
    var verifier = try Verifier(TestTxn).init(&txn, 41);
    var entries = txn.values.iterator();
    while (entries.next()) |entry| try verifier.feed(entry.key_ptr.*, entry.value_ptr.*);
    try verifier.finish();
    var cleared = TestTxn.init();
    defer cleared.deinit();
    entries = txn.values.iterator();
    while (entries.next()) |entry| try cleared.put(entry.key_ptr.*, entry.value_ptr.*);
    try cleared.put(key, &(try page.after.encode()));
    {
        var replay = try ReplayVerifier(TestTxn, TestTxn).init(a, &cleared, &txn, 41);
        defer replay.deinit();
        try replay.feed(key);
        try replay.finish();
    }
    try std.testing.expectError(error.InvalidCatalogRecord, ReplayVerifier(TestTxn, TestTxn).init(a, &txn, &cleared, 41));
    const successor = try State.init(41, try nextJobId(&failed), moved);
    try start(&txn, &successor, moved, &(try failed.encode()));
    try std.testing.expectEqual(FailureReason.none, (try State.decode(try txn.get(key))).failure);
    for ([_]anyerror{ error.OutOfMemory, error.InputOutput, error.InvalidCatalogRecord, error.CatalogGenerationChanged }) |err|
        try std.testing.expect(FailureReason.fromError(err) == null);
    try std.testing.expectEqual(FailureReason.source_limit, FailureReason.fromError(error.CatalogCommandTooLarge).?);
}

test "relation reconciliation compound candidates retain active visibility and verify reserved successor bytes" {
    const a = std.testing.allocator;
    var successor = test_owner;
    successor.table_id = 8;
    successor.phase = .reserved;
    successor.publication_id = @splat(9);
    const claims = [_]names.Claim{
        .{ .key = test_claims[0].key, .owner = test_owner, .pending = successor },
        .{ .key = .{ .namespace_id = 5, .name = "new_idx" }, .owner = successor },
    };
    try std.testing.expectError(error.InvalidCatalogRecord, names.Plan.init(a, &.{}, &claims));
    const rows = [_]SourceRow{.{ .key = "table:7", .table_id = 7, .pending_table_id = 8, .claims = &claims }};
    var source: TestSource = .{ .rows = &rows };
    const initial = try State.init(41, try nextJobId(null), test_epoch);
    var txn = TestTxn.init();
    defer txn.deinit();
    try start(&txn, &initial, test_epoch, null);
    var build = try Page.prepareSource(a, initial, test_epoch, &source);
    defer build.deinit();
    try build.apply(&txn, test_epoch);
    var reader: CandidateStore(TestTxn) = .{ .txn = &txn, .state = &initial };
    try std.testing.expect((try reader.getClaim(claims[0].key)).?.eql(test_owner));
    try std.testing.expect((try reader.getEntry(claims[0].key)).?.pending.?.eql(successor));
    try std.testing.expect(try reader.getClaim(claims[1].key) == null);
    try std.testing.expect((try reader.getEntry(claims[1].key)).?.pending.?.eql(successor));
    var verified = try Page.prepareSource(a, build.after, test_epoch, &source);
    defer verified.deinit();
    try verified.apply(&txn, test_epoch);
    var key_buffers: [2][max_cursor_bytes]u8 = undefined;
    var candidate_rows: [2]CandidateRow = undefined;
    for (claims, &key_buffers, &candidate_rows) |claim, *buffer, *row| {
        const key = try candidateKey(buffer, &initial, claim.key);
        row.* = .{ .key = key, .value = try txn.get(key) };
    }
    var candidates: TestCandidates = .{ .rows = &candidate_rows };
    var ready = try Page.prepareCandidate(a, verified.after, test_epoch, &candidates);
    defer ready.deinit();
    try ready.apply(&txn, test_epoch);
    try std.testing.expectEqual(Phase.ready, ready.after.phase);
    var verifier = try Verifier(TestTxn).init(&txn, 41);
    var entries = txn.values.iterator();
    while (entries.next()) |entry| try verifier.feed(entry.key_ptr.*, entry.value_ptr.*);
    try verifier.finish();
    var forged = try claims[0].entry();
    forged.pending.?.schema_digest[0] ^= 1;
    const forged_bytes = try forged.encode();
    candidate_rows[0].value = &forged_bytes;
    try std.testing.expectError(error.InvalidCatalogRecord, Page.prepareCandidate(a, verified.after, test_epoch, &candidates));
    candidate_rows[0].value = try txn.get(candidate_rows[0].key);
    var invalid_rows = rows;
    invalid_rows[0].pending_table_id = null;
    var invalid_source: TestSource = .{ .rows = &invalid_rows };
    try std.testing.expectError(error.InvalidCatalogRecord, Page.prepareSource(a, initial, test_epoch, &invalid_source));
    const Probe = struct {
        fn prepare(alloc: A, state: State, input: []const SourceRow) !void {
            var stream: TestSource = .{ .rows = input };
            var page = try Page.prepareSource(alloc, state, test_epoch, &stream);
            defer page.deinit();
        }
    };
    var no_resize = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Probe.prepare, .{ initial, &rows });
    const replacement = try State.init(41, try nextJobId(&ready.after), test_epoch);
    try start(&txn, &replacement, test_epoch, &(try ready.after.encode()));
    var garbage = try GarbagePage.prepare(a, Retirement.init(Generation.of(&initial)), &candidates);
    defer garbage.deinit();
    try std.testing.expect(try garbage.apply(&txn));
    try std.testing.expect(try reader.getEntry(claims[0].key) == null);
    try std.testing.expect(try reader.getEntry(claims[1].key) == null);
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
    const owner = try (names.Entry{ .active = test_owner }).encode();
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
    const forged = try (names.Entry{ .active = forged_owner }).encode();
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
            const owner = try (names.Entry{ .active = test_owner }).encode();
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
    cursor_seeks: usize = 0,
    cursor_nexts: usize = 0,
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
    pub fn openCursor(self: *TestTxn) !Cursor {
        return .{ .txn = self };
    }
    const Cursor = struct {
        txn: *TestTxn,
        last: []const u8 = "",
        pub fn close(_: *@This()) void {}
        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?CandidateRow {
            self.txn.cursor_seeks += 1;
            return self.find(key, false);
        }
        pub fn next(self: *@This()) !?CandidateRow {
            self.txn.cursor_nexts += 1;
            return self.find(self.last, true);
        }
        fn find(self: *@This(), key: []const u8, exclusive: bool) ?CandidateRow {
            var entries = self.txn.values.iterator();
            var best: ?CandidateRow = null;
            while (entries.next()) |entry| {
                const order = std.mem.order(u8, entry.key_ptr.*, key);
                if (order == .lt or (exclusive and order == .eq)) continue;
                if (best == null or std.mem.order(u8, entry.key_ptr.*, best.?.key) == .lt)
                    best = .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
            }
            if (best) |entry| self.last = entry.key;
            return best;
        }
    };
};

test "relation reconciliation scheduler observation bounds backlog reads and skips the protected root" {
    var txn = TestTxn.init();
    defer txn.deinit();
    var buf: [128]u8 = undefined;
    const empty = try Work.read(&txn, 41, null);
    try std.testing.expect(empty.epoch == null and empty.current == null and empty.garbage == null);
    for (0..test_epoch.revision) |_| try advanceSource(&txn, 41);
    const current = try State.init(41, blk: {
        var id: [16]u8 = undefined;
        std.mem.writeInt(u128, &id, 129, .big);
        break :blk id;
    }, test_epoch);
    try txn.put(try jobKey(&buf, 41), &(try current.encode()));
    var protected: Generation = undefined;
    for (1..129) |i| {
        var id: [16]u8 = undefined;
        std.mem.writeInt(u128, &id, i, .big);
        const generation: Generation = .{ .group_id = 41, .job_id = id };
        if (i == 1) protected = generation;
        const retirement = Retirement.init(generation);
        try txn.put(try retirementKey(&buf, generation), &(try retirement.encode()));
    }
    try txn.put(try rootKey(&buf, 41), &(try protected.encode()));
    txn.cursor_seeks = 0;
    txn.cursor_nexts = 0;
    const work = try Work.read(&txn, 41, test_epoch);
    try std.testing.expectEqual(@as(u128, 2), std.mem.readInt(u128, &work.garbage.?.generation.job_id, .big));
    try std.testing.expect(std.meta.eql(current, work.current.?));
    try std.testing.expect(work.root.?.eql(protected));
    try std.testing.expectEqual(@as(usize, 1), txn.cursor_seeks);
    try std.testing.expectEqual(@as(usize, 1), txn.cursor_nexts);
    // The returned cut owns its bytes; later progress cannot alter it.
    try txn.delete(try retirementKey(&buf, work.garbage.?.generation));
    const next = try Work.read(&txn, 41, test_epoch);
    try std.testing.expectEqual(@as(u128, 3), std.mem.readInt(u128, &next.garbage.?.generation.job_id, .big));
    try std.testing.expectEqual(@as(u128, 2), std.mem.readInt(u128, &work.garbage.?.generation.job_id, .big));
    try std.testing.expectError(error.InvalidCatalogRecord, Work.read(&txn, 41, null));
    var moved = test_epoch;
    moved.revision += 1;
    try std.testing.expectError(error.InvalidCatalogRecord, Work.read(&txn, 41, moved));
    // A syntactically valid retirement cannot smuggle another generation.
    try txn.put(try retirementKey(&buf, next.garbage.?.generation), &(try work.garbage.?.encode()));
    try std.testing.expectError(error.InvalidCatalogRecord, Work.read(&txn, 41, test_epoch));
}

test "relation reconciliation scheduler observation handles protected-only and orphaned retirement cuts" {
    var txn = TestTxn.init();
    defer txn.deinit();
    var buf: [128]u8 = undefined;
    for (0..test_epoch.revision) |_| try advanceSource(&txn, 41);
    const prior = try State.init(41, try nextJobId(null), test_epoch);
    const current = try State.init(41, try nextJobId(&prior), test_epoch);
    try txn.put(try jobKey(&buf, 41), &(try current.encode()));
    const protected = Generation.of(&prior);
    const retirement = Retirement.init(protected);
    try txn.put(try retirementKey(&buf, protected), &(try retirement.encode()));
    try txn.put(try rootKey(&buf, 41), &(try protected.encode()));
    // An adjacent group is not part of this job's backlog.
    const adjacent = Retirement.init(.{ .group_id = 42, .job_id = prior.job_id });
    try txn.put(try retirementKey(&buf, adjacent.generation), &(try adjacent.encode()));
    try std.testing.expect((try Work.read(&txn, 41, test_epoch)).garbage == null);
    try txn.delete(try rootKey(&buf, 41));
    try std.testing.expect((try Work.read(&txn, 41, test_epoch)).garbage.?.generation.eql(protected));
    try txn.delete(try jobKey(&buf, 41));
    try std.testing.expectError(error.InvalidCatalogRecord, Work.read(&txn, 41, test_epoch));
}

test "relation reconciliation seals only a transactionally verified candidate tail" {
    const a = std.testing.allocator;
    for ([_]usize{ 0, 1, max_tables_per_page }) |count| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const owned = arena.allocator();
        var txn = TestTxn.init();
        defer txn.deinit();
        const initial = try State.init(41, @splat(3), test_epoch);
        try start(&txn, &initial, test_epoch, null);
        const claims = try owned.alloc(names.Claim, count);
        for (claims, 0..) |*claim, i| claim.* = .{
            .key = .{ .namespace_id = 5, .name = try std.fmt.allocPrint(owned, "row:{d:0>4}", .{i}) },
            .owner = test_owner,
        };
        const row: SourceRow = .{ .key = "table:7", .table_id = 7, .claims = claims };
        var source: TestSource = .{ .rows = if (count == 0) &.{} else &.{row} };
        var build = try Page.prepareSource(a, initial, test_epoch, &source);
        defer build.deinit();
        try build.apply(&txn, test_epoch);
        var verify = try Page.prepareSource(a, build.after, test_epoch, &source);
        defer verify.deinit();
        try verify.apply(&txn, test_epoch);
        const candidates = try owned.alloc(CandidateRow, count);
        for (claims, candidates) |claim, *candidate| {
            var buf: [max_cursor_bytes]u8 = undefined;
            const key = try candidateKey(&buf, &initial, claim.key);
            candidate.* = .{ .key = try owned.dupe(u8, key), .value = try txn.get(key) };
        }
        var candidate_source: TestCandidates = .{ .rows = candidates };
        var first_page = try Page.prepareCandidate(a, verify.after, test_epoch, &candidate_source);
        defer first_page.deinit();
        var final_page: ?Page = null;
        defer if (final_page) |*page| page.deinit();
        if (first_page.after.phase != .ready) {
            try first_page.apply(&txn, test_epoch);
            final_page = try Page.prepareCandidate(a, first_page.after, test_epoch, &candidate_source);
            try std.testing.expectEqual(@as(usize, 0), final_page.?.claims.len);
        }
        const page = if (final_page) |*value| value else &first_page;
        try std.testing.expectEqual(Phase.ready, page.after.phase);
        var key_buf: [max_cursor_bytes]u8 = undefined;
        const extra = try candidateKey(&key_buf, &initial, .{ .namespace_id = 5, .name = "late_extra" });
        try txn.put(extra, &(try (names.Entry{ .active = test_owner }).encode()));
        txn.cursor_seeks = 0;
        txn.cursor_nexts = 0;
        try std.testing.expectError(error.InvalidCatalogRecord, page.apply(&txn, test_epoch));
        try std.testing.expectEqual(@as(usize, 1), txn.cursor_seeks);
        try std.testing.expectEqual(@as(usize, if (count == 0) 0 else 1), txn.cursor_nexts);
        var job_buf: [128]u8 = undefined;
        try std.testing.expectEqualSlices(u8, &(try page.before.encode()), try txn.get(try jobKey(&job_buf, initial.group_id)));
        try txn.delete(extra);
        // Another generation must not be mistaken for a late candidate.
        const future = try State.init(41, try nextJobId(&initial), test_epoch);
        var future_buf: [max_cursor_bytes]u8 = undefined;
        try txn.put(try candidateKey(&future_buf, &future, test_claims[0].key), &(try (names.Entry{ .active = test_owner }).encode()));
        txn.cursor_seeks = 0;
        txn.cursor_nexts = 0;
        try page.apply(&txn, test_epoch);
        try std.testing.expectEqual(@as(usize, 1), txn.cursor_seeks);
        try std.testing.expectEqual(@as(usize, if (count == 0) 0 else 1), txn.cursor_nexts);
        try std.testing.expectEqualSlices(u8, &(try page.after.encode()), try txn.get(try jobKey(&job_buf, initial.group_id)));
    }
}

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
    // The retained high-water mark rejects older IDs even after replacement.
    try std.testing.expectError(error.CatalogGenerationChanged, start(&txn, &initial, test_epoch, &next_encoded));
    var old_candidates: CandidateStore(TestTxn) = .{ .txn = &txn, .state = &initial };
    try std.testing.expect(try old_candidates.getClaim(test_claims[0].key) != null);
    var new_candidates: CandidateStore(TestTxn) = .{ .txn = &txn, .state = &next };
    try std.testing.expect((try new_candidates.getClaim(test_claims[0].key)) == null);
    const future = try State.init(41, try nextJobId(&next), test_epoch);
    // A higher ID must also have an empty prefix: one native prefix seek,
    // rather than scanning all existing or abandoned candidate generations.
    var dirty_buf: [max_cursor_bytes]u8 = undefined;
    try txn.put(try candidateKey(&dirty_buf, &future, test_claims[0].key), &(try (names.Entry{ .active = test_owner }).encode()));
    try std.testing.expectError(error.CatalogGenerationChanged, start(&txn, &future, test_epoch, &next_encoded));
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

test "relation reconciliation garbage collection fences roots exact values and generation reuse" {
    const a = std.testing.allocator;
    var txn = TestTxn.init();
    defer txn.deinit();
    const initial = try State.init(41, try nextJobId(null), test_epoch);
    try start(&txn, &initial, test_epoch, null);
    var source: TestSource = .{ .rows = &test_rows };
    var build = try Page.prepareSource(a, initial, test_epoch, &source);
    defer build.deinit();
    try build.apply(&txn, test_epoch);
    const other_group = try State.init(42, initial.job_id, test_epoch);
    try start(&txn, &other_group, test_epoch, null);
    var other_build = try Page.prepareSource(a, other_group, test_epoch, &source);
    defer other_build.deinit();
    try other_build.apply(&txn, test_epoch);
    const next = try State.init(41, try nextJobId(&build.after), test_epoch);
    try start(&txn, &next, test_epoch, &(try build.after.encode()));
    var candidate_buf: [max_cursor_bytes]u8 = undefined;
    const candidate_key = try candidateKey(&candidate_buf, &initial, test_claims[0].key);
    const value = try (names.Entry{ .active = test_owner }).encode();
    const rows = [_]CandidateRow{.{ .key = candidate_key, .value = &value }};
    var candidates: TestCandidates = .{ .rows = &rows };
    var garbage = try GarbagePage.prepare(a, Retirement.init(Generation.of(&initial)), &candidates);
    defer garbage.deinit();
    var root_buf: [128]u8 = undefined;
    const root_key = try rootKey(&root_buf, initial.group_id);
    try txn.put(root_key, &(try Generation.of(&initial).encode()));
    try std.testing.expectError(error.CatalogGenerationChanged, garbage.apply(&txn));
    try std.testing.expectEqualSlices(u8, &value, try txn.get(candidate_key));
    try txn.put(root_key, "unknown-root-format");
    try std.testing.expectError(error.InvalidCatalogRecord, garbage.apply(&txn));
    try txn.delete(root_key);
    var forged = test_owner;
    forged.schema_digest[0] ^= 1;
    try txn.put(candidate_key, &(try forged.encode()));
    try std.testing.expectError(error.CatalogGenerationChanged, garbage.apply(&txn));
    try txn.put(candidate_key, &value);
    try std.testing.expect(try garbage.apply(&txn));
    var other_candidates: CandidateStore(TestTxn) = .{ .txn = &txn, .state = &other_group };
    try std.testing.expect(try other_candidates.getClaim(test_claims[0].key) != null);
    try std.testing.expectError(error.NotFound, txn.get(candidate_key));
    var retirement_buf: [128]u8 = undefined;
    try std.testing.expectError(error.NotFound, txn.get(try retirementKey(&retirement_buf, Generation.of(&initial))));
    try std.testing.expectError(error.CatalogGenerationChanged, garbage.apply(&txn));
    // GC removed both data and intent, but retained the one per-group high-
    // water mark. Older IDs cannot be resurrected without a tombstone leak.
    try std.testing.expectError(error.CatalogGenerationChanged, start(&txn, &initial, test_epoch, &(try next.encode())));
    const final = try State.init(41, try nextJobId(&next), test_epoch);
    try start(&txn, &final, test_epoch, &(try next.encode()));
    var empty: TestCandidates = .{ .rows = &.{} };
    var empty_gc = try GarbagePage.prepare(a, Retirement.init(Generation.of(&next)), &empty);
    defer empty_gc.deinit();
    try std.testing.expect(try empty_gc.apply(&txn));
    var max = final;
    max.job_id = @splat(255);
    try std.testing.expectError(error.CatalogGenerationExhausted, nextJobId(&max));
    const encoded = try Generation.of(&final).encode();
    try std.testing.expect((try Generation.decode(&encoded)).eql(Generation.of(&final)));
    try std.testing.expectError(error.InvalidCatalogRecord, Generation.decode(encoded[0 .. encoded.len - 1]));
}

test "relation reconciliation garbage preparation unwinds allocation faults" {
    const T = struct {
        fn run(a: A) !void {
            const state = try State.init(41, try nextJobId(null), test_epoch);
            var buf: [max_cursor_bytes]u8 = undefined;
            const key = try candidateKey(&buf, &state, test_claims[0].key);
            const value = try (names.Entry{ .active = test_owner }).encode();
            const rows = [_]CandidateRow{.{ .key = key, .value = &value }};
            var source: TestCandidates = .{ .rows = &rows };
            var page = try GarbagePage.prepare(a, Retirement.init(Generation.of(&state)), &source);
            defer page.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, T.run, .{});
}

test "relation reconciliation retirement cursor rejects foreign and noncanonical bytes" {
    const state = try State.init(41, try nextJobId(null), test_epoch);
    var retirement = Retirement.init(Generation.of(&state));
    const encoded = try retirement.encode();
    const decoded = try Retirement.decode(&encoded);
    try std.testing.expectEqualSlices(u8, &encoded, &(try decoded.encode()));
    retirement.cursor_bytes[0] = 1;
    try std.testing.expectError(error.InvalidCatalogRecord, retirement.encode());
    retirement = Retirement.init(Generation.of(&state));
    var other = state;
    other.group_id = 42;
    const key = try candidateKey(&retirement.cursor_bytes, &other, test_claims[0].key);
    retirement.cursor_len = @intCast(key.len);
    try std.testing.expectError(error.InvalidCatalogRecord, retirement.encode());
    retirement = Retirement.init(Generation.of(&state));
    const own_key = try candidateKey(&retirement.cursor_bytes, &state, test_claims[0].key);
    retirement.cursor_len = @intCast(own_key.len);
    const valid = try retirement.encode();
    const round_trip = try Retirement.decode(&valid);
    try std.testing.expectEqualSlices(u8, own_key, round_trip.cursor());
    try std.testing.expectError(error.InvalidCatalogRecord, Retirement.decode(valid[0 .. valid.len - 1]));
}

test "relation reconciliation stored cut verification rejects missing forged and orphan state" {
    const T = struct {
        fn verify(txn: *TestTxn, group: u64) !void {
            var verifier = try Verifier(TestTxn).init(txn, group);
            var rows = txn.values.iterator();
            while (rows.next()) |row| {
                const record = (try classify(row.key_ptr.*)) orelse continue;
                if (record.group_id == group) try verifier.feed(row.key_ptr.*, row.value_ptr.*);
            }
            try verifier.finish();
        }
    };
    const a = std.testing.allocator;
    var txn = TestTxn.init();
    defer txn.deinit();
    const initial = try State.init(41, try nextJobId(null), test_epoch);
    try start(&txn, &initial, test_epoch, null);
    var source: TestSource = .{ .rows = &test_rows };
    var page = try Page.prepareSource(a, initial, test_epoch, &source);
    defer page.deinit();
    try page.apply(&txn, test_epoch);
    try T.verify(&txn, 41);
    var buf: [max_cursor_bytes]u8 = undefined;
    const key = try candidateKey(&buf, &initial, test_claims[0].key);
    const owner = try (names.Entry{ .active = test_owner }).encode();
    try txn.delete(key);
    try std.testing.expectError(error.InvalidCatalogRecord, T.verify(&txn, 41));
    var forged = test_owner;
    forged.schema_digest[0] ^= 1;
    try txn.put(key, &(try forged.encode()));
    try std.testing.expectError(error.InvalidCatalogRecord, T.verify(&txn, 41));
    try txn.put(key, &owner);
    var extra_buf: [max_cursor_bytes]u8 = undefined;
    const extra = try candidateKey(&extra_buf, &initial, .{ .namespace_id = 5, .name = "extra" });
    try txn.put(extra, &owner);
    try std.testing.expectError(error.InvalidCatalogRecord, T.verify(&txn, 41));
    try txn.delete(extra);
    const successor = try State.init(41, try nextJobId(&initial), test_epoch);
    try start(&txn, &successor, test_epoch, &(try page.after.encode()));
    try T.verify(&txn, 41);
    var retirement_buf: [128]u8 = undefined;
    const retirement_key = try retirementKey(&retirement_buf, Generation.of(&initial));
    var retired = Retirement.init(Generation.of(&initial));
    retired.cursor_len = @intCast(key.len);
    @memcpy(retired.cursor_bytes[0..key.len], key);
    try txn.put(retirement_key, &(try retired.encode()));
    try std.testing.expectError(error.InvalidCatalogRecord, T.verify(&txn, 41));
    retired = Retirement.init(Generation.of(&initial));
    try txn.put(retirement_key, &(try retired.encode()));
    try txn.delete(retirement_key);
    try std.testing.expectError(error.InvalidCatalogRecord, T.verify(&txn, 41));
    try txn.put(retirement_key, &(try retired.encode()));
    var job_buf: [128]u8 = undefined;
    const job_key = try jobKey(&job_buf, 41);
    try txn.delete(job_key);
    try std.testing.expectError(error.InvalidCatalogRecord, T.verify(&txn, 41));
    try txn.put(job_key, &(try successor.encode()));
    var root_buf: [128]u8 = undefined;
    const root_key = try rootKey(&root_buf, 41);
    try txn.put(root_key, &(try Generation.of(&successor).encode()));
    try std.testing.expectError(error.InvalidCatalogRecord, T.verify(&txn, 41));
    try txn.put(root_key, &(try Generation.of(&initial).encode()));
    try T.verify(&txn, 41);
    try std.testing.expectError(error.InvalidCatalogRecord, classify(job_key[0 .. job_key.len - 1]));
    var verifier = try Verifier(TestTxn).init(&txn, 41);
    var foreign = initial;
    foreign.group_id = 42;
    const foreign_key = try candidateKey(&buf, &foreign, test_claims[0].key);
    try std.testing.expectError(error.InvalidCatalogRecord, verifier.feed(foreign_key, &owner));
}

test "relation reconciliation source clock validates encoding overflow and replay monotonicity" {
    var before = TestTxn.init();
    defer before.deinit();
    var after = TestTxn.init();
    defer after.deinit();
    try std.testing.expectEqual(@as(u64, 0), try readSourceRevision(&before, 41));
    try advanceTrackedSource(&before, 41);
    try std.testing.expectEqual(@as(u64, 0), try readSourceRevision(&before, 41));
    try advanceSource(&before, 41);
    try advanceSource(&after, 41);
    try advanceSource(&after, 41);
    var buf: [128]u8 = undefined;
    const key = try sourceKey(&buf, 41);
    try std.testing.expectEqual(RecordKind.source, (try classify(key)).?.kind);
    try std.testing.expectEqual(@as(u64, 2), try readSourceRevision(&after, 41));
    {
        var verifier = try Verifier(TestTxn).init(&after, 41);
        try verifier.feed(key, try after.get(key));
        try verifier.finish();
    }
    {
        var replay = try ReplayVerifier(TestTxn, TestTxn).init(std.testing.allocator, &before, &after, 41);
        defer replay.deinit();
        try replay.feed(key);
        try replay.finish();
    }
    {
        var replay = try ReplayVerifier(TestTxn, TestTxn).init(std.testing.allocator, &after, &before, 41);
        defer replay.deinit();
        try std.testing.expectError(error.InvalidCatalogRecord, replay.feed(key));
    }
    try before.delete(key);
    {
        var replay = try ReplayVerifier(TestTxn, TestTxn).init(std.testing.allocator, &after, &before, 41);
        defer replay.deinit();
        try std.testing.expectError(error.InvalidCatalogRecord, replay.feed(key));
    }
    try std.testing.expectError(error.InvalidCatalogRecord, sourceRevision(""));
    try std.testing.expectError(error.InvalidCatalogRecord, sourceRevision(&@as([8]u8, @splat(0))));
    try after.put(key, &@as([8]u8, @splat(255)));
    try std.testing.expectError(error.CatalogGenerationExhausted, advanceSource(&after, 41));
    try std.testing.expectEqual(std.math.maxInt(u64), try readSourceRevision(&after, 41));
}

test "relation reconciliation replay verifies deltas immutable seals and bounded GC cuts" {
    const T = struct {
        fn copy(original: *TestTxn) !TestTxn {
            var result = TestTxn.init();
            errdefer result.deinit();
            var entries = original.values.iterator();
            while (entries.next()) |entry| try result.put(entry.key_ptr.*, entry.value_ptr.*);
            return result;
        }
        fn check(a: A, before: *TestTxn, after: *TestTxn, keys: []const []const u8) !void {
            var verifier = try ReplayVerifier(TestTxn, TestTxn).init(a, before, after, 41);
            defer verifier.deinit();
            for (keys) |key| try verifier.feed(key);
            try verifier.finish();
        }
    };
    const a = std.testing.allocator;
    var empty = TestTxn.init();
    defer empty.deinit();
    var built = TestTxn.init();
    defer built.deinit();
    const initial = try State.init(41, try nextJobId(null), test_epoch);
    try start(&built, &initial, test_epoch, null);
    var source: TestSource = .{ .rows = &test_rows };
    var page = try Page.prepareSource(a, initial, test_epoch, &source);
    defer page.deinit();
    try page.apply(&built, test_epoch);
    var job_buf: [128]u8 = undefined;
    const job_key = try jobKey(&job_buf, 41);
    var key_buf: [max_cursor_bytes]u8 = undefined;
    const key = try candidateKey(&key_buf, &initial, test_claims[0].key);
    try T.check(a, &empty, &built, &.{ job_key, key });
    try std.testing.expectError(error.InvalidCatalogRecord, T.check(a, &empty, &built, &.{job_key}));
    {
        var bad = try T.copy(&built);
        defer bad.deinit();
        var forged = test_owner;
        forged.schema_digest[0] ^= 1;
        try bad.put(key, &(try forged.encode()));
        try std.testing.expectError(error.InvalidCatalogRecord, T.check(a, &empty, &bad, &.{ job_key, key }));
        // Once sealed, even a matching altered fingerprint cannot authorize
        // changing the generation's previously verified owner bytes.
        var state = page.after;
        state.expected.claim_hash = try claimHash(.{ .key = test_claims[0].key, .owner = forged });
        try bad.put(job_key, &(try state.encode()));
        try std.testing.expectError(error.InvalidCatalogRecord, T.check(a, &built, &bad, &.{ job_key, key }));
        try bad.delete(job_key);
        try std.testing.expectError(error.InvalidCatalogRecord, T.check(a, &built, &bad, &.{job_key}));
    }
    var retired = try T.copy(&built);
    defer retired.deinit();
    const successor = try State.init(41, try nextJobId(&initial), test_epoch);
    try start(&retired, &successor, test_epoch, &(try page.after.encode()));
    var retirement_buf: [128]u8 = undefined;
    const retirement_key = try retirementKey(&retirement_buf, Generation.of(&initial));
    try T.check(a, &built, &retired, &.{ job_key, retirement_key });
    {
        var bad = try T.copy(&retired);
        defer bad.deinit();
        try bad.delete(retirement_key);
        try std.testing.expectError(error.InvalidCatalogRecord, T.check(a, &retired, &bad, &.{retirement_key}));
        var skipped = Retirement.init(Generation.of(&initial));
        skipped.cursor_len = @intCast(key.len);
        @memcpy(skipped.cursor_bytes[0..key.len], key);
        try bad.put(retirement_key, &(try skipped.encode()));
        try std.testing.expectError(error.InvalidCatalogRecord, T.check(a, &retired, &bad, &.{retirement_key}));
        const original = Retirement.init(Generation.of(&initial));
        try bad.put(retirement_key, &(try original.encode()));
        try bad.delete(key);
        try std.testing.expectError(error.InvalidCatalogRecord, T.check(a, &retired, &bad, &.{key}));
    }
    var collected = try T.copy(&retired);
    defer collected.deinit();
    try collected.delete(key);
    try collected.delete(retirement_key);
    try T.check(a, &retired, &collected, &.{ key, retirement_key });
    const Fault = struct {
        fn run(alloc: A, before: *TestTxn, after: *TestTxn, keys: []const []const u8) !void {
            try T.check(alloc, before, after, keys);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Fault.run, .{ &retired, &collected, @as([]const []const u8, &.{ key, retirement_key }) });
}
