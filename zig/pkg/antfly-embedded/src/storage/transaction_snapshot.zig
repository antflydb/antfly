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

//! Independent, read-only qualification of a derived transaction directory.
//! All evidence comes from one pinned image; legacy set proofs spill through
//! the common private-run machinery and never allocate a cohort-sized map.
const std = @import("std");
const debt = @import("transaction_debt.zig");
const cohort = @import("transaction_cohort_proof.zig");
const spill = @import("../spill_sort.zig");

const ProgressProof = struct {
    progress: ?debt.Progress,
    indexed: bool,
    cursor_found: bool = false,

    fn expectedAfter(self: *const @This(), kind: u64, digest: cohort.Digest) bool {
        const p = self.progress.?;
        return kind > p.offset or (kind == p.offset and std.mem.order(u8, &digest, &p.after_member) == .gt);
    }
    pub fn visit(self: *@This(), digest: cohort.Digest, flags: u8) !void {
        const p = self.progress orelse return;
        var enlisted = false;
        var resolved = false;
        switch (p.phase) {
            .none, .indexed => {},
            .enlisted => enlisted = flags & cohort.prefix_enlisted != 0,
            .resolved => {
                enlisted = flags & cohort.enlisted != 0;
                resolved = flags & cohort.prefix_resolved != 0;
            },
            .indexed_legacy => resolved = flags & cohort.prefix_resolved != 0,
            .cleanup => {
                enlisted = !self.indexed and flags & cohort.enlisted != 0;
                resolved = flags & cohort.legacy_resolved != 0;
                if (std.mem.eql(u8, &digest, &p.after_member) and
                    ((p.offset == 1 and enlisted) or (p.offset == 2 and resolved))) self.cursor_found = true;
                enlisted = enlisted and self.expectedAfter(1, digest);
                resolved = resolved and self.expectedAfter(2, digest);
            },
        }
        if (enlisted != (flags & cohort.seen_enlisted != 0) or resolved != (flags & cohort.seen_resolved != 0)) return error.InvalidTxnRecord;
    }
};

fn optional(view: anytype, key: []const u8) !?[]const u8 {
    return view.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

fn digestKey(comptime Api: type, comptime prefix: []const u8, id: debt.TxnId, digest: cohort.Digest) [prefix.len + 48]u8 {
    const scope = Api.sidecarKey(prefix, id);
    var key: [prefix.len + 48]u8 = undefined;
    @memcpy(key[0..scope.len], &scope);
    @memcpy(key[scope.len..], &digest);
    return key;
}

/// One coordinator record on a pinned read. Points are an independent scope
/// owned by the parent audit, which must not reuse it while this job runs.
/// Cursor-backed name/list slices survive turns; callbacks never do.
pub fn RecordAudit(comptime Read: type, comptime Points: type, comptime Api: type) type {
    const Cursor = @typeInfo(@TypeOf(@as(Read, undefined).openCursor())).error_union.payload;
    const Index = @typeInfo(@TypeOf(Api.index(@as([]const u8, undefined)))).error_union.payload;
    return struct {
        const Phase = enum { members, acks, enlisted, resolved, seen, proof, classify, done };
        const Stage = enum { read, hash, lookup, compare, submit };
        cursor: Cursor,
        points: Points,
        head: ?debt.Header,
        id: debt.TxnId,
        record_pending: bool,
        index: ?Index,
        progress: ?debt.Progress,
        visitor: ProgressProof,
        proof: cohort.Proof,
        phase: Phase = .members,
        stage: Stage = .read,
        started: bool = false,
        name: []const u8 = &.{},
        member: []const u8 = &.{},
        hash: std.crypto.hash.sha2.Sha256 = .init(.{}),
        hashed: usize = 0,
        compared: usize = 0,
        expected_digest: cohort.Digest = @splat(0),
        digest: cohort.Digest = @splat(0),
        flags: u8 = 0,
        enlisted_count: u64 = 0,
        resolved_count: u64 = 0,
        prefix_indexed: u64 = 0,
        last_indexed: cohort.Digest = @splat(0),
        list_loaded: bool = false,
        list_value: []const u8 = &.{},
        list_count: u32 = 0,
        list_seen: u32 = 0,
        list_offset: usize = 0,
        list_next: usize = 0,
        list_prefix_seen: u64 = 0,
        list_boundary: bool = false,
        counts: ?cohort.Counts = null,
        kind: ?debt.Kind = null,
        invalid: bool = false,
        last_units: usize = 0,
        last_hashed_bytes: usize = 0,
        last_compared_bytes: usize = 0,

        pub fn init(a: std.mem.Allocator, read: Read, points: Points, head: ?debt.Header, id: debt.TxnId, record_pending: bool, options: spill.Options, control: anytype) !@This() {
            try control.check();
            const raw = try optional(points, &Api.sidecarKey(Api.members, id));
            const index: ?Index = if (raw) |bytes| try Api.index(bytes) else null;
            const progress = if (head != null and head.?.pending != null and std.mem.eql(u8, &head.?.pending.?, &id)) head.?.progress else null;
            return .{
                .cursor = try read.openCursor(),
                .points = points,
                .head = head,
                .id = id,
                .record_pending = record_pending,
                .index = index,
                .progress = progress,
                .visitor = .{ .progress = progress, .indexed = index != null },
                .proof = cohort.Proof.initRetained(a, options),
            };
        }
        pub fn deinit(self: *@This()) void {
            self.proof.deinit();
            self.cursor.close();
            self.* = undefined;
        }
        pub fn result(self: *const @This()) !?debt.Kind {
            if (self.invalid or self.phase != .done) return error.SchemaValidationRuntimeRecoveryRequired;
            return self.kind;
        }
        pub fn step(self: *@This(), budget: spill.WorkBudget, control: anytype) !bool {
            try control.check();
            try budget.validate();
            if (self.invalid) return error.SchemaValidationRuntimeRecoveryRequired;
            self.last_units = 0;
            self.last_hashed_bytes = 0;
            self.last_compared_bytes = 0;
            if (self.phase == .done) return true;
            errdefer self.invalid = true;
            const started = if (budget.io) |io| std.Io.Clock.awake.now(io).nanoseconds else 0;
            while (self.last_units < budget.units) {
                try control.check();
                try self.workOne(control);
                self.last_units += 1;
                if (self.phase == .done) {
                    try control.check();
                    return true;
                }
                if (budget.io) |io| if (std.Io.Clock.awake.now(io).nanoseconds - started >= budget.time_ns) break;
            }
            return false;
        }
        fn nextPhase(self: *@This(), next: Phase) void {
            self.phase = next;
            self.stage = .read;
            self.started = false;
            self.list_loaded = false;
            self.list_value = &.{};
            self.name = &.{};
            self.member = &.{};
        }
        fn beginHash(self: *@This(), name: []const u8, flags: u8) void {
            self.name = name;
            self.flags = flags;
            self.hash = .init(.{});
            self.hashed = 0;
            self.stage = .hash;
        }
        fn scanIndexed(self: *@This(), comptime prefix: []const u8, comptime flags: u8) !void {
            const scope = Api.sidecarKey(prefix, self.id);
            const entry = if (!self.started) blk: {
                self.started = true;
                break :blk try self.cursor.seekAtOrAfter(&scope);
            } else try self.cursor.next();
            if (entry == null or !std.mem.startsWith(u8, entry.?.key, &scope)) {
                if (flags == cohort.enlisted) self.nextPhase(.acks) else {
                    if (self.index) |index| if (self.enlisted_count != index.enlisted or self.resolved_count != index.resolved) return error.InvalidTxnRecord;
                    self.nextPhase(.enlisted);
                }
                return;
            }
            const row = entry.?;
            if (flags == cohort.enlisted and std.mem.eql(u8, row.key, &scope)) return;
            if (self.index == null or row.key.len != scope.len + 32 or row.value.len == 0) return error.InvalidTxnRecord;
            self.expected_digest = row.key[scope.len..][0..32].*;
            self.beginHash(row.value, flags);
        }
        fn listPrefixFlag(self: *const @This()) u8 {
            const p = self.progress orelse return 0;
            return if (self.phase == .enlisted)
                (if (p.phase == .enlisted) cohort.prefix_enlisted else 0)
            else
                (if (p.phase == .resolved or p.phase == .indexed_legacy) cohort.prefix_resolved else 0);
        }
        fn scanList(self: *@This(), comptime prefix: []const u8) !void {
            const prefix_flag = self.listPrefixFlag();
            const p = if (prefix_flag != 0) self.progress else null;
            if (!self.list_loaded) {
                const key = Api.sidecarKey(prefix, self.id);
                const found = try self.cursor.seekAtOrAfter(&key);
                if (found == null or !std.mem.eql(u8, found.?.key, &key)) {
                    if (p) |value| if (value.offset != 0 or value.seen != 0 or value.expected != 0) return error.InvalidTxnRecord;
                    self.nextPhase(if (self.phase == .enlisted) .resolved else .seen);
                    return;
                }
                const bytes = found.?.value;
                if (bytes.len < 4) return error.InvalidTxnRecord;
                self.list_value = bytes;
                self.list_count = std.mem.readInt(u32, bytes[0..4], .little);
                self.list_seen = 0;
                self.list_offset = 4;
                self.list_prefix_seen = 0;
                self.list_boundary = p == null or p.?.offset == 0 or p.?.offset == 4;
                self.list_loaded = true;
                return;
            }
            if (self.list_seen == self.list_count) {
                if (self.list_offset != self.list_value.len) return error.InvalidTxnRecord;
                if (p) |value| if (!self.list_boundary or value.seen != self.list_prefix_seen or value.expected != (if (value.offset == 0) @as(u64, 0) else self.list_count)) return error.InvalidTxnRecord;
                self.nextPhase(if (self.phase == .enlisted) .resolved else .seen);
                return;
            }
            const bytes = self.list_value;
            const offset = self.list_offset;
            if (bytes.len - offset < 4) return error.InvalidTxnRecord;
            const length = std.mem.readInt(u32, bytes[offset..][0..4], .little);
            const begin = offset + 4;
            if (length == 0 or length > bytes.len - begin) return error.InvalidTxnRecord;
            self.list_next = begin + length;
            var flags: u8 = if (self.phase == .enlisted) cohort.original | (if (self.index == null) cohort.enlisted else @as(u8, 0)) else cohort.legacy_resolved;
            if (p) |value| if (self.list_next <= value.offset) {
                flags |= prefix_flag;
            };
            self.beginHash(bytes[begin..self.list_next], flags);
        }
        fn scanSeen(self: *@This()) !void {
            const p = self.progress orelse {
                self.nextPhase(.proof);
                return;
            };
            const prefix = debt.workPrefix(self.head.?.scope.generation, self.id, p.work_revision);
            const entry = if (!self.started) blk: {
                self.started = true;
                break :blk try self.cursor.seekAtOrAfter(&prefix);
            } else try self.cursor.next();
            if (entry == null or !std.mem.startsWith(u8, entry.?.key, &prefix)) {
                self.nextPhase(.proof);
                return;
            }
            const row = entry.?;
            if (row.key.len != prefix.len + 33 or !std.mem.eql(u8, row.value, &.{1}) or
                (row.key[prefix.len] != 1 and row.key[prefix.len] != 2)) return error.InvalidTxnRecord;
            self.digest = row.key[prefix.len + 1 ..][0..32].*;
            self.flags = if (row.key[prefix.len] == 1) cohort.seen_enlisted else cohort.seen_resolved;
            self.stage = .submit;
        }
        fn submit(self: *@This(), control: anytype) !void {
            if (!try self.proof.tryAddDigest(self.digest, self.flags, control)) {
                _ = try self.proof.work(.{ .units = 1 }, control);
                return;
            }
            switch (self.phase) {
                .members => self.enlisted_count = try std.math.add(u64, self.enlisted_count, 1),
                .acks => {
                    self.resolved_count = try std.math.add(u64, self.resolved_count, 1);
                    if (self.progress) |p| if (p.phase == .indexed and p.seen != 0 and std.mem.order(u8, &self.digest, &p.after_member) != .gt) {
                        self.prefix_indexed += 1;
                        self.last_indexed = self.digest;
                    };
                },
                .enlisted, .resolved => {
                    if (self.listPrefixFlag() != 0) {
                        const p = self.progress.?;
                        if (self.list_next <= p.offset) self.list_prefix_seen += 1;
                        if (self.list_next == p.offset) self.list_boundary = true;
                    }
                    self.list_offset = self.list_next;
                    self.list_seen += 1;
                },
                .seen => {},
                else => unreachable,
            }
            self.stage = .read;
            self.name = &.{};
            self.member = &.{};
        }
        fn workOne(self: *@This(), control: anytype) !void {
            switch (self.stage) {
                .hash => {
                    const take = @min(4096, self.name.len - self.hashed);
                    self.hash.update(self.name[self.hashed..][0..take]);
                    self.hashed += take;
                    self.last_hashed_bytes += take;
                    if (self.hashed == self.name.len) {
                        self.hash.final(&self.digest);
                        if ((self.phase == .members or self.phase == .acks) and !std.mem.eql(u8, &self.digest, &self.expected_digest)) return error.InvalidTxnRecord;
                        self.stage = if (self.phase == .acks) .lookup else .submit;
                    }
                },
                .lookup => {
                    self.member = (try optional(self.points, &digestKey(Api, Api.members, self.id, self.digest))) orelse return error.InvalidTxnRecord;
                    if (self.member.len != self.name.len) return error.InvalidTxnRecord;
                    self.compared = 0;
                    self.stage = .compare;
                },
                .compare => {
                    const take = @min(4096, self.name.len - self.compared);
                    if (!std.mem.eql(u8, self.name[self.compared..][0..take], self.member[self.compared..][0..take])) return error.InvalidTxnRecord;
                    self.compared += take;
                    self.last_compared_bytes += take;
                    if (self.compared == self.name.len) self.stage = .submit;
                },
                .submit => try self.submit(control),
                .read => switch (self.phase) {
                    .members => try self.scanIndexed(Api.members, cohort.enlisted),
                    .acks => try self.scanIndexed(Api.acks, cohort.indexed_resolved),
                    .enlisted => try self.scanList(Api.participants),
                    .resolved => try self.scanList(Api.resolved),
                    .seen => try self.scanSeen(),
                    .proof => {
                        if (try self.proof.finishStep(&self.visitor, .{ .units = 1 }, control)) |counts| {
                            self.counts = counts;
                            self.phase = .classify;
                        }
                    },
                    .classify => {
                        self.kind = try self.classify();
                        self.phase = .done;
                    },
                    .done => {},
                },
            }
        }
        fn classify(self: *const @This()) !?debt.Kind {
            const index = self.index;
            const counts = self.counts.?;
            const progress = self.progress;
            const prefix_indexed = self.prefix_indexed;
            const last_indexed = self.last_indexed;
            const visitor = self.visitor;
            if (index) |value| if (counts.enlisted != value.enlisted) return error.InvalidTxnRecord;
            if (progress) |p| {
                if (self.record_pending) return error.InvalidTxnRecord;
                switch (p.phase) {
                    .none => {
                        var empty = p;
                        empty.work_revision = 0;
                        if (!std.meta.eql(empty, debt.Progress{})) return error.InvalidTxnRecord;
                    },
                    .enlisted => if (index != null or p.enlisted != counts.prefix_enlisted or p.resolved != 0 or p.indexed_expected != 0 or !std.mem.allEqual(u8, &p.after_member, 0)) return error.InvalidTxnRecord,
                    .resolved => if (index != null or p.enlisted != counts.enlisted or p.resolved != counts.prefix_resolved or p.indexed_expected != 0 or !std.mem.allEqual(u8, &p.after_member, 0)) return error.InvalidTxnRecord,
                    .indexed => {
                        const value = index orelse return error.InvalidTxnRecord;
                        if (p.offset != 0 or p.seen != prefix_indexed or p.expected != value.resolved or p.indexed_expected != value.resolved or
                            p.enlisted != value.enlisted or p.resolved != prefix_indexed or !std.mem.eql(u8, &p.after_member, &last_indexed)) return error.InvalidTxnRecord;
                    },
                    .indexed_legacy => {
                        const value = index orelse return error.InvalidTxnRecord;
                        const resolved = std.math.add(u64, value.resolved, counts.prefix_extra_resolved) catch return error.InvalidTxnRecord;
                        if (p.enlisted != value.enlisted or p.indexed_expected != value.resolved or p.resolved != resolved or !std.mem.allEqual(u8, &p.after_member, 0)) return error.InvalidTxnRecord;
                    },
                    .cleanup => if (p.offset > 2 or p.seen != 0 or p.expected != 0 or p.enlisted != counts.enlisted or p.resolved != counts.resolved or
                        p.indexed_expected != 0 or (p.offset == 0 and !std.mem.allEqual(u8, &p.after_member, 0)) or
                        (p.offset != 0 and !visitor.cursor_found)) return error.InvalidTxnRecord,
                }
            }
            return if (self.record_pending or counts.enlisted != counts.resolved) .blocked else null;
        }
    };
}

pub const Stats = struct {
    records: u64 = 0,
    family_keys: u64 = 0,
    hashed_bytes: u64 = 0,
    compared_bytes: u64 = 0,
};

/// Full semantic transaction/cohort coverage on one retained image. Structural
/// directory coverage is a separate prerequisite, not inferred from this scan.
pub fn Audit(comptime Read: type, comptime Api: type) type {
    const Cursor = @typeInfo(@TypeOf(@as(Read, undefined).openCursor())).error_union.payload;
    const Points = @typeInfo(@TypeOf(@as(Read, undefined).openReadScope(@as(std.mem.Allocator, undefined)))).error_union.payload;
    const Record = RecordAudit(Read, *Points, Api);
    return struct {
        const Phase = enum { records, members, acks, participants, resolved, directory, done };
        alloc: std.mem.Allocator,
        read: Read,
        points: Points,
        cursor: Cursor,
        head: ?debt.Header,
        options: spill.Options,
        active: ?Record = null,
        current_id: debt.TxnId = @splat(0),
        current_pending: bool = false,
        phase: Phase = .records,
        started: bool = false,
        invalid: bool = false,
        stats: Stats = .{},
        last_units: usize = 0,

        pub fn init(a: std.mem.Allocator, read: Read, options: spill.Options, control: anytype) !@This() {
            try control.check();
            var owned = options;
            owned.cancellation = null;
            owned.directory = try a.dupe(u8, options.directory);
            errdefer a.free(owned.directory);
            var points = try read.openReadScope(a);
            errdefer points.close();
            const head = try debt.load(&points);
            return .{ .alloc = a, .read = read, .points = points, .cursor = try read.openCursor(), .head = head, .options = owned };
        }
        pub fn deinit(self: *@This()) void {
            if (self.active) |*record| record.deinit();
            self.cursor.close();
            self.points.close();
            self.alloc.free(self.options.directory);
            self.* = undefined;
        }
        pub fn result(self: *const @This()) !Stats {
            if (self.invalid or self.phase != .done) return error.SchemaValidationRuntimeRecoveryRequired;
            return self.stats;
        }
        pub fn step(self: *@This(), budget: spill.WorkBudget, control: anytype) !bool {
            try control.check();
            try budget.validate();
            if (self.invalid) return error.SchemaValidationRuntimeRecoveryRequired;
            self.last_units = 0;
            if (self.phase == .done) return true;
            errdefer self.invalid = true;
            const started = if (budget.io) |io| std.Io.Clock.awake.now(io).nanoseconds else 0;
            while (self.last_units < budget.units) {
                try control.check();
                try self.workOne(control);
                self.last_units += 1;
                if (self.phase == .done) {
                    try control.check();
                    return true;
                }
                if (budget.io) |io| if (std.Io.Clock.awake.now(io).nanoseconds - started >= budget.time_ns) break;
            }
            return false;
        }
        fn nextPhase(self: *@This(), phase: Phase) void {
            self.phase = phase;
            self.started = false;
        }
        fn checkDirectory(self: *@This(), actual: ?debt.Kind) !void {
            const h = self.head orelse return;
            if (h.phase == .retired) return;
            const stored = try debt.member(&self.points, h.scope.generation, self.current_id);
            const covered = h.phase != .records or (h.after != null and std.mem.order(u8, &self.current_id, &h.after.?) != .gt);
            if (stored == .dirty) {
                if (self.current_pending) return error.InvalidTxnRecord;
                const indexed = try optional(&self.points, &Api.sidecarKey(Api.members, self.current_id)) != null;
                if (indexed and try optional(&self.points, &Api.sidecarKey(Api.resolved, self.current_id)) == null) return error.InvalidTxnRecord;
            } else if ((covered or stored != null) and stored != actual) return error.InvalidTxnRecord;
        }
        fn scanRecords(self: *@This(), control: anytype) !void {
            if (self.active) |*record| {
                const complete = try record.step(.{ .units = 1 }, control);
                self.stats.hashed_bytes = try std.math.add(u64, self.stats.hashed_bytes, @intCast(record.last_hashed_bytes));
                self.stats.compared_bytes = try std.math.add(u64, self.stats.compared_bytes, @intCast(record.last_compared_bytes));
                if (!complete) return;
                try self.checkDirectory(try record.result());
                record.deinit();
                self.active = null;
                self.stats.records = try std.math.add(u64, self.stats.records, 1);
                return;
            }
            const entry = if (!self.started) blk: {
                self.started = true;
                break :blk try self.cursor.seekAtOrAfter(Api.records);
            } else try self.cursor.next();
            if (entry == null or !std.mem.startsWith(u8, entry.?.key, Api.records)) {
                self.nextPhase(.members);
                return;
            }
            const row = entry.?;
            if (row.key.len != Api.records.len + 16) return error.InvalidTxnRecord;
            self.current_id = row.key[Api.records.len..][0..16].*;
            const decoded = try Api.record(row.value);
            self.current_pending = decoded.status == .pending;
            self.active = try Record.init(self.alloc, self.read, &self.points, self.head, self.current_id, self.current_pending, self.options, control);
        }
        fn scanFamily(self: *@This(), comptime prefix: []const u8, next: Phase) !void {
            const entry = if (!self.started) blk: {
                self.started = true;
                break :blk try self.cursor.seekAtOrAfter(prefix);
            } else try self.cursor.next();
            if (entry == null or !std.mem.startsWith(u8, entry.?.key, prefix)) {
                self.nextPhase(next);
                return;
            }
            const row = entry.?;
            if (row.key.len < prefix.len + 16) return error.InvalidTxnRecord;
            const suffix = row.key.len - prefix.len;
            if (comptime std.mem.eql(u8, prefix, Api.members)) {
                if (suffix != 16 and suffix != 48) return error.InvalidTxnRecord;
            } else if (comptime std.mem.eql(u8, prefix, Api.acks)) {
                if (suffix != 48) return error.InvalidTxnRecord;
            } else if (suffix != 16) return error.InvalidTxnRecord;
            const id: debt.TxnId = row.key[prefix.len..][0..16].*;
            if (try optional(&self.points, &Api.recordKey(id)) == null) return error.InvalidTxnRecord;
            self.stats.family_keys = try std.math.add(u64, self.stats.family_keys, 1);
        }
        fn scanDirectory(self: *@This()) !void {
            const h = self.head orelse {
                self.phase = .done;
                return;
            };
            if (h.phase == .retired) {
                self.phase = .done;
                return;
            }
            const prefix = debt.memberPrefix(h.scope.generation);
            const entry = if (!self.started) blk: {
                self.started = true;
                break :blk try self.cursor.seekAtOrAfter(&prefix);
            } else try self.cursor.next();
            if (entry == null or !std.mem.startsWith(u8, entry.?.key, &prefix)) {
                self.phase = .done;
                return;
            }
            const row = entry.?;
            if (row.key.len != prefix.len + 16 or try optional(&self.points, &Api.recordKey(row.key[prefix.len..][0..16].*)) == null) return error.InvalidTxnRecord;
            self.stats.family_keys = try std.math.add(u64, self.stats.family_keys, 1);
        }
        fn workOne(self: *@This(), control: anytype) !void {
            switch (self.phase) {
                .records => try self.scanRecords(control),
                .members => try self.scanFamily(Api.members, .acks),
                .acks => try self.scanFamily(Api.acks, .participants),
                .participants => try self.scanFamily(Api.participants, .resolved),
                .resolved => try self.scanFamily(Api.resolved, .directory),
                .directory => try self.scanDirectory(),
                .done => {},
            }
        }
    };
}

pub fn validate(a: std.mem.Allocator, read: anytype, head: ?debt.Header, options: spill.Options, comptime Api: type) !void {
    const control = options.cancellation orelse @import("antfly_cancellation").CancellationToken.none;
    var audit = try Audit(@TypeOf(read), Api).init(a, read, options, control);
    defer audit.deinit();
    if (!std.meta.eql(head, audit.head)) return error.SchemaValidationJobChanged;
    while (!try audit.step(.{}, control)) {}
    _ = try audit.result();
}
