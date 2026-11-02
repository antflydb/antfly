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

//! Immutable physical source/parent retirement plan and compact owner progress.
//! This reuses the FK generation owner protocol. No receipt is inferred from
//! a source snapshot, a metadata reservation, or another owner's ACK.
const std = @import("std");
const local = @import("antfly_local_sources");
const fk = @import("fk_generation_publication.zig");
const manifests = @import("table_retirement_manifest.zig");
const tables = @import("table_manager.zig");
const records = @import("schema_validation_publication.zig");
const wire = @import("../api/relational_fk_generation_publication.zig");
const A = std.mem.Allocator;
pub const Lookup = @import("table_retirement_authority.zig").Lookup;
pub const Phase = enum { fencing_sources, staging_parents, activating_parents, acknowledging_parents, completing_coverage, ready };
pub const Step = enum(u3) { source_fenced, parent_staged, parent_activated, parent_acknowledged, parent_coverage };
pub const step_count = @typeInfo(Step).@"enum".field_names.len;
pub const Counts = [step_count]u16;
pub const Summary = struct {
    plan_digest: [32]u8,
    owners_digest: [32]u8,
    owner_count: u16,
    expected: Counts,
    child: fk.Plan.ChildIdentity,
    pub fn validate(self: @This()) !void {
        if (self.owner_count == 0 or self.owner_count > manifests.max_ranges or self.expected[0] == 0 or
            self.expected[0] > self.owner_count or self.expected[1] > self.owner_count or self.expected[2] != self.expected[1] or
            self.expected[3] != self.expected[1] or self.expected[4] > self.expected[1] or
            std.mem.allEqual(u8, &self.plan_digest, 0) or std.mem.allEqual(u8, &self.owners_digest, 0) or
            self.child.after_schema_version <= self.child.before_schema_version) return error.InvalidCatalogRecord;
    }
};
pub const State = struct {
    summary: Summary,
    phase: Phase = .fencing_sources,
    counts: Counts = @splat(0),
    receipts_xor: [step_count][32]u8 = @splat(@splat(0)),
    pub fn validate(self: @This()) !void {
        try self.summary.validate();
        for (self.counts, self.summary.expected, 0..) |count, expected, i| {
            if (count > expected or (count == 0 and !std.mem.allEqual(u8, &self.receipts_xor[i], 0))) return error.InvalidCatalogRecord;
        }
        const index: usize = @backingInt(self.phase);
        for (self.counts, self.summary.expected, 0..) |count, expected, i| {
            if (i < index and count != expected) return error.InvalidCatalogRecord;
            if (i > index and count != 0) return error.InvalidCatalogRecord;
        }
        if (self.phase != .ready and (self.summary.expected[index] == 0 or self.counts[index] == self.summary.expected[index])) return error.InvalidCatalogRecord;
    }
    pub fn next(self: @This(), step: Step, commitment: [32]u8) !State {
        try self.validate();
        if (self.phase == .ready or @backingInt(self.phase) != @backingInt(step)) return error.GenerationPublicationChanged;
        var result = self;
        const index = @backingInt(step);
        result.counts[index] += 1;
        for (&result.receipts_xor[index], commitment) |*byte, value| byte.* ^= value;
        if (result.counts[index] == result.summary.expected[index]) {
            result.phase = @fromBackingInt(@backingInt(result.phase) + 1);
            while (result.phase != .ready and result.summary.expected[@backingInt(result.phase)] == 0)
                result.phase = @fromBackingInt(@backingInt(result.phase) + 1);
        }
        try result.validate();
        return result;
    }
};
pub const Owner = struct {
    ordinal: u16,
    group: u64,
    table: u64,
    source: bool = false,
    parent: bool = false,
    coverage: bool = false,
    source_fence: [32]u8 = @splat(0),
    transitions: [32]u8 = @splat(0),
    pub fn validate(self: @This()) !void {
        if (self.ordinal >= manifests.max_ranges or self.group == 0 or self.table == 0 or (!self.source and !self.parent) or
            (self.coverage and (!self.parent or self.source)) or self.source == std.mem.allEqual(u8, &self.source_fence, 0) or
            self.parent == std.mem.allEqual(u8, &self.transitions, 0)) return error.InvalidCatalogRecord;
    }
    pub fn applies(self: @This(), step: Step) bool {
        return switch (step) {
            .source_fenced => self.source,
            .parent_coverage => self.coverage,
            else => self.parent,
        };
    }
};
pub const OwnerRecord = records.Record(Owner, "TRPO", 1024);
pub const Evidence = union(enum) {
    source: wire.SourceReceipt,
    parent: wire.Receipt,
    pub fn digest(self: @This()) [32]u8 {
        return switch (self) {
            .source => |value| value.digest(),
            .parent => |value| value.digest(),
        };
    }
};
pub const Receipt = struct { root: Lookup, ordinal: u16, step: Step, evidence: Evidence };
pub const ReceiptRecord = records.Record(Receipt, "TRPR", 4096);
pub const Prepared = struct { plan: fk.Plan, encoded: []const u8, owners: []Owner, summary: Summary };

pub fn prepare(a: A, scratch: A, plan: fk.Plan) !Prepared {
    try plan.validate(scratch);
    if (plan.sql_drop == null or plan.table_retirement == null) return error.InvalidGenerationPublication;
    var list: std.ArrayList(Owner) = .empty;
    var by_group: std.AutoHashMapUnmanaged(u64, usize) = .empty;
    defer by_group.deinit(scratch);
    for (plan.child_ranges, plan.child_fences) |range, fence| {
        const bytes = try fence.encode();
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(&bytes, &digest, .{});
        if ((try by_group.getOrPut(scratch, range.group_id)).found_existing) return error.InvalidGenerationPublication;
        by_group.getPtr(range.group_id).?.* = list.items.len;
        try list.append(a, .{ .ordinal = 0, .group = range.group_id, .table = range.table_id, .source = true, .source_fence = digest });
    }
    const digest = try plan.digest(scratch);
    for (plan.parents) |parent| {
        const admission = local.storage_db_relational_integrity_generation_admission;
        const transitions = try scratch.alloc(admission.Transition, parent.transitions.len);
        defer scratch.free(transitions);
        for (parent.transitions, transitions) |input, *output| output.* = .{ .child_table_id = input.child_table_id, .child_table_name = input.child_table_name, .constraint_name = input.constraint_name, .expected_generation = input.expected_generation, .next_generation = input.next_generation, .plan_id = plan.id, .decision_digest = digest };
        const transition_digest = try admission.transitionsDigest(transitions);
        for (parent.ranges) |range| {
            const source = if (by_group.get(range.group_id)) |index| &list.items[index] else null;
            if (source) |owner| {
                if (owner.table != plan.child_before.table_id or parent.table.table_id != owner.table or owner.parent) return error.InvalidGenerationPublication;
                owner.parent = true;
                owner.coverage = false;
                owner.transitions = transition_digest;
            } else {
                try by_group.put(scratch, range.group_id, list.items.len);
                try list.append(a, .{ .ordinal = 0, .group = range.group_id, .table = parent.table.table_id, .parent = true, .coverage = plan.needsParentCoverage(parent), .transitions = transition_digest });
            }
        }
    }
    if (list.items.len > manifests.max_ranges) return error.InvalidGenerationPublication;
    std.mem.sort(Owner, list.items, {}, struct {
        fn less(_: void, left: Owner, right: Owner) bool {
            return left.group < right.group;
        }
    }.less);
    var expected: Counts = @splat(0);
    var owner_hash = std.crypto.hash.Blake3.init(.{});
    owner_hash.update("antfly/table-retirement-physical-owners/v1\x00");
    var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(owner_hash, &.{});
    for (list.items, 0..) |*owner, i| {
        if (i != 0 and list.items[i - 1].group == owner.group) return error.InvalidGenerationPublication;
        owner.ordinal = @intCast(i);
        try owner.validate();
        inline for (std.meta.tags(Step)) |step| if (owner.applies(step)) {
            expected[@backingInt(step)] += 1;
        };
        try std.json.Stringify.value(owner.*, .{}, &writer.writer);
        try writer.writer.writeByte(0);
    }
    var owners_digest: [32]u8 = undefined;
    writer.hasher.final(&owners_digest);
    const summary: Summary = .{ .plan_digest = digest, .owners_digest = owners_digest, .owner_count = @intCast(list.items.len), .expected = expected, .child = try plan.childIdentity(scratch) };
    try summary.validate();
    const encoded = try std.json.Stringify.valueAlloc(a, plan, .{});
    if (encoded.len > fk.max_bytes) return error.CatalogCommandTooLarge;
    return .{ .plan = plan, .encoded = encoded, .owners = list.items, .summary = summary };
}

/// Match all declared metadata effects before installing any owner work. A
/// digest is not authorization to withdraw an unrelated schema or parent edge.
pub fn requireManifest(a: A, lookup: Lookup, plan: Prepared, manifest: manifests.Manifest) !void {
    if (!std.mem.eql(u8, &lookup.id, &plan.plan.id) or lookup.table != manifest.root.table_id or lookup.group != manifest.metadata_group_id or
        !std.meta.eql(plan.plan.table_retirement.?, try manifest.reference()) or !tables.tableDefinitionsEqual(plan.plan.child_before, manifest.root) or
        !std.meta.eql(plan.plan.sql_drop.?.owner, manifest.guard.owner) or manifest.view_backings.len != 0 or plan.plan.parents.len != manifest.outgoing_parents.len) return error.InvalidGenerationPublication;
    const expected_guard = try std.json.Stringify.valueAlloc(a, manifest.guard, .{});
    defer a.free(expected_guard);
    const actual_guard = try std.json.Stringify.valueAlloc(a, plan.plan.sql_drop.?, .{});
    defer a.free(actual_guard);
    if (!std.mem.eql(u8, expected_guard, actual_guard)) return error.InvalidGenerationPublication;
    try requireRanges(plan.plan.child_ranges, manifest.root_ranges);
    for (plan.plan.parents, manifest.outgoing_parents) |parent, id| {
        if (parent.table.table_id != id) return error.InvalidGenerationPublication;
        const expected = if (id == manifest.root.table_id) manifest.root else blk: {
            for (manifest.survivors) |pair| if (pair.before.table_id == id) break :blk pair.before;
            for (manifest.external_parents) |item| if (item.table.table_id == id) break :blk item.table;
            return error.InvalidGenerationPublication;
        };
        if (!tables.tableDefinitionsEqual(parent.table, expected)) return error.InvalidGenerationPublication;
        const ranges = if (id == manifest.root.table_id) manifest.root_ranges else blk: {
            for (manifest.external_parents) |item| if (item.table.table_id == id) break :blk item.ranges;
            var begin: usize = 0;
            while (begin < manifest.survivor_ranges.len and manifest.survivor_ranges[begin].table_id < id) : (begin += 1) {}
            var end = begin;
            while (end < manifest.survivor_ranges.len and manifest.survivor_ranges[end].table_id == id) : (end += 1) {}
            break :blk manifest.survivor_ranges[begin..end];
        };
        try requireRanges(parent.ranges, ranges);
    }
}
pub fn requireRanges(left: anytype, right: []const local.common_topology_records.RangeRecord) !void {
    if (left.len != right.len or right.len > manifests.max_ranges) return error.InvalidGenerationPublication;
    // Manifest ranges are ordered by group; owner plans are ordered by key.
    // Binary search plus a fixed membership bitmap avoids an N² join and any
    // apply-lock allocation even for the maximum 4096-owner cut.
    var previous_table: u64 = 0;
    var previous_group: u64 = 0;
    for (right) |range| {
        if (range.table_id == 0 or range.group_id == 0 or range.table_id < previous_table or (range.table_id == previous_table and range.group_id <= previous_group)) return error.InvalidGenerationPublication;
        previous_table = range.table_id;
        previous_group = range.group_id;
    }
    var seen: [(manifests.max_ranges + 63) / 64]u64 = @splat(0);
    for (left) |item| {
        const range = if (comptime @hasField(@TypeOf(item), "range")) item.range else item;
        var low: usize = 0;
        var high = right.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            if (right[middle].table_id < range.table_id or (right[middle].table_id == range.table_id and right[middle].group_id < range.group_id)) low = middle + 1 else high = middle;
        }
        if (low == right.len or right[low].group_id != range.group_id or !tables.rangeRecordsEqual(range, right[low])) return error.InvalidGenerationPublication;
        const mask = @as(u64, 1) << @as(u6, @intCast(low % 64));
        if (seen[low / 64] & mask != 0) return error.InvalidGenerationPublication;
        seen[low / 64] |= mask;
    }
}

pub fn verifyEvidence(lookup: Lookup, state: State, owner: Owner, step: Step, evidence: Evidence) ![32]u8 {
    try lookup.validate();
    try state.validate();
    try owner.validate();
    if (!owner.applies(step) or owner.ordinal >= state.summary.owner_count or (step == .source_fenced) != (evidence == .source)) return error.InvalidGenerationPublication;
    switch (evidence) {
        .source => |receipt| {
            const child = state.summary.child;
            if (!std.mem.eql(u8, &receipt.plan_id, &lookup.id) or receipt.child_table_id != lookup.table or receipt.child_group_id != owner.group or
                receipt.action != .fence or receipt.reference_coverage_ready or receipt.applied_term == 0 or receipt.applied_index == 0 or
                !std.mem.eql(u8, &receipt.plan_digest, &state.summary.plan_digest) or !std.mem.eql(u8, &receipt.fence_digest, &owner.source_fence) or
                receipt.before_schema_version != child.before_schema_version or receipt.after_schema_version != child.after_schema_version or
                !std.mem.eql(u8, &receipt.before_schema_digest, &child.before_schema_digest) or !std.mem.eql(u8, &receipt.after_schema_digest, &child.after_schema_digest) or
                !std.mem.eql(u8, &receipt.before_catalog_digest, &child.before_catalog_digest) or !std.mem.eql(u8, &receipt.after_catalog_digest, &child.after_catalog_digest)) return error.InvalidGenerationPublication;
        },
        .parent => |receipt| {
            const action: wire.Action = switch (step) {
                .parent_staged => .stage,
                .parent_activated => .activate,
                .parent_acknowledged, .parent_coverage => .acknowledge,
                else => unreachable,
            };
            if (!std.mem.eql(u8, &receipt.plan_id, &lookup.id) or receipt.child_table_id != lookup.table or receipt.parent_table_id != owner.table or receipt.parent_group_id != owner.group or
                receipt.action != action or receipt.reference_coverage_ready != (step == .parent_coverage) or receipt.applied_term == 0 or receipt.applied_index == 0 or
                !std.mem.eql(u8, &receipt.decision_digest, &state.summary.plan_digest) or !std.mem.eql(u8, &receipt.transitions_digest, &owner.transitions)) return error.InvalidGenerationPublication;
        },
    }
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/table-retirement-owner-ack/v1\x00");
    hash.update(&.{@backingInt(step)});
    var ordinal: [2]u8 = undefined;
    std.mem.writeInt(u16, &ordinal, owner.ordinal, .little);
    hash.update(&ordinal);
    hash.update(&evidence.digest());
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}

pub const Kind = enum { plan, owner, receipt, work };
pub fn prefix(buffer: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:table-retirement-physical:", .{group});
}
pub fn key(buffer: []u8, lookup: Lookup, kind: Kind, ordinal: u16, step: Step) ![]const u8 {
    try lookup.validate();
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}:{s}:{s}:{d}:{x:0>4}", .{ try prefix(&base, lookup.group), lookup.table, std.fmt.bytesToHex(lookup.id, .lower), @tagName(kind), @backingInt(step), ordinal });
}
pub const Address = struct { lookup: Lookup, kind: Kind, ordinal: u16, step: Step };
pub fn address(group: u64, bytes: []const u8) !?Address {
    var buffer: [128]u8 = undefined;
    const start = try prefix(&buffer, group);
    if (!std.mem.startsWith(u8, bytes, start)) return null;
    var parts = std.mem.splitScalar(u8, bytes[start.len..], ':');
    var value: Address = .{ .lookup = .{ .group = group, .table = std.fmt.parseInt(u64, parts.next() orelse return error.InvalidCatalogRecord, 16) catch return error.InvalidCatalogRecord, .id = undefined }, .kind = undefined, .ordinal = 0, .step = .source_fenced };
    _ = std.fmt.hexToBytes(&value.lookup.id, parts.next() orelse return error.InvalidCatalogRecord) catch return error.InvalidCatalogRecord;
    const kind = parts.next() orelse return error.InvalidCatalogRecord;
    value.kind = std.meta.stringToEnum(Kind, kind) orelse return error.InvalidCatalogRecord;
    value.step = std.enums.fromInt(Step, std.fmt.parseInt(u8, parts.next() orelse return error.InvalidCatalogRecord, 10) catch return error.InvalidCatalogRecord) orelse return error.InvalidCatalogRecord;
    value.ordinal = std.fmt.parseInt(u16, parts.next() orelse return error.InvalidCatalogRecord, 16) catch return error.InvalidCatalogRecord;
    if (parts.next() != null or ((value.kind == .plan or value.kind == .owner) and value.step != .source_fenced) or (value.kind == .plan and value.ordinal != 0)) return error.InvalidCatalogRecord;
    var canonical: [256]u8 = undefined;
    if (!std.mem.eql(u8, bytes, try key(&canonical, value.lookup, value.kind, value.ordinal, value.step))) return error.InvalidCatalogRecord;
    return value;
}

pub fn workPrefix(buffer: []u8, lookup: Lookup, step: Step) ![]const u8 {
    try lookup.validate();
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}:{s}:work:{d}:", .{ try prefix(&base, lookup.group), lookup.table, std.fmt.bytesToHex(lookup.id, .lower), @backingInt(step) });
}

test "table retirement manifest physical model enforces complete ordered barriers and zero-parent completion" {
    const summary: Summary = .{
        .plan_digest = @splat(1),
        .owners_digest = @splat(2),
        .owner_count = 3,
        .expected = .{ 2, 1, 1, 1, 1 },
        .child = .{ .before_schema_version = 4, .after_schema_version = 5, .before_schema_digest = @splat(3), .after_schema_digest = @splat(4), .before_catalog_digest = @splat(5), .after_catalog_digest = @splat(6) },
    };
    var state: State = .{ .summary = summary };
    try state.validate();
    try std.testing.expectError(error.GenerationPublicationChanged, state.next(.parent_staged, @splat(7)));
    state = try state.next(.source_fenced, @splat(7));
    try std.testing.expectEqual(Phase.fencing_sources, state.phase);
    var torn = state;
    torn.phase = .staging_parents;
    try std.testing.expectError(error.InvalidCatalogRecord, torn.validate());
    state = try state.next(.source_fenced, @splat(8));
    inline for (.{ Step.parent_staged, Step.parent_activated, Step.parent_acknowledged, Step.parent_coverage }) |step| {
        try std.testing.expectEqual(@backingInt(step), @backingInt(state.phase));
        state = try state.next(step, @splat(@as(u8, @backingInt(step)) + 9));
        const bytes = try std.json.Stringify.valueAlloc(std.testing.allocator, state, .{});
        defer std.testing.allocator.free(bytes);
        var decoded = try std.json.parseFromSlice(State, std.testing.allocator, bytes, .{});
        defer decoded.deinit();
        try decoded.value.validate();
        try std.testing.expectEqualDeep(state, decoded.value);
    }
    try std.testing.expectEqual(Phase.ready, state.phase);
    try std.testing.expectError(error.GenerationPublicationChanged, state.next(.parent_coverage, @splat(7)));
    state = .{ .summary = summary };
    state.summary.owner_count = 1;
    state.summary.expected = .{ 1, 0, 0, 0, 0 };
    state = try state.next(.source_fenced, @splat(7));
    try std.testing.expectEqual(Phase.ready, state.phase);
}

test "table retirement manifest physical keys isolate owner queues by root and barrier" {
    const lookup: Lookup = .{ .group = 41, .table = 101, .id = @splat(7) };
    var key_buffer: [256]u8 = undefined;
    var prefix_buffer: [256]u8 = undefined;
    inline for (std.meta.tags(Step)) |step| {
        const start = try workPrefix(&prefix_buffer, lookup, step);
        for ([_]u16{ 0, 15, 16, 4095 }) |ordinal| {
            const bytes = try key(&key_buffer, lookup, .work, ordinal, step);
            try std.testing.expect(std.mem.startsWith(u8, bytes, start));
            try std.testing.expectEqualDeep(Address{ .lookup = lookup, .kind = .work, .ordinal = ordinal, .step = step }, (try address(41, bytes)).?);
            try std.testing.expect(try address(42, bytes) == null);
            const previous = bytes[bytes.len - 1];
            key_buffer[bytes.len - 1] = 'G';
            try std.testing.expectError(error.InvalidCatalogRecord, address(41, bytes));
            key_buffer[bytes.len - 1] = previous;
        }
    }
    const invalid_plan = try key(&key_buffer, lookup, .plan, 0, .parent_staged);
    try std.testing.expectError(error.InvalidCatalogRecord, address(41, invalid_plan));
    const invalid_owner = try key(&key_buffer, lookup, .owner, 1, .parent_coverage);
    try std.testing.expectError(error.InvalidCatalogRecord, address(41, invalid_owner));
}

test "table retirement manifest physical range rejoin handles maximum fanout without an allocated index" {
    const a = std.testing.allocator;
    const Range = local.common_topology_records.RangeRecord;
    const expected = try a.alloc(Range, manifests.max_ranges);
    defer a.free(expected);
    const candidate = try a.alloc(Range, manifests.max_ranges);
    defer a.free(candidate);
    for (expected, 0..) |*range, i| range.* = .{ .group_id = i + 1, .table_id = 101, .start_key = "" };
    for (candidate, 0..) |*range, i| range.* = expected[expected.len - i - 1];
    try requireRanges(candidate, expected);
    candidate[0] = candidate[1];
    try std.testing.expectError(error.InvalidGenerationPublication, requireRanges(candidate, expected));
    candidate[0] = expected[expected.len - 1];
    candidate[0].range_id += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, requireRanges(candidate, expected));
    candidate[0] = expected[expected.len - 1];
    candidate[0].group_id += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, requireRanges(candidate, expected));
    try std.testing.expectError(error.InvalidGenerationPublication, requireRanges(candidate[1..], expected));
}

test "table retirement manifest survivor owner cover joins table group order to physical key order" {
    const Range = local.common_topology_records.RangeRecord;
    const expected = [_]Range{
        .{ .table_id = 10, .group_id = 9, .start_key = "m" },
        .{ .table_id = 10, .group_id = 12, .start_key = "", .end_key = "m" },
        .{ .table_id = 20, .group_id = 3, .start_key = "z" },
        .{ .table_id = 20, .group_id = 7, .start_key = "", .end_key = "z" },
    };
    const Intent = struct { range: Range };
    var physical_order = [_]Intent{ .{ .range = expected[1] }, .{ .range = expected[0] }, .{ .range = expected[3] }, .{ .range = expected[2] } };
    try requireRanges(&physical_order, &expected);
    physical_order[0].range.table_id = 20;
    try std.testing.expectError(error.InvalidGenerationPublication, requireRanges(&physical_order, &expected));
    physical_order[0].range = expected[0];
    try std.testing.expectError(error.InvalidGenerationPublication, requireRanges(&physical_order, &expected));
    physical_order[0].range = expected[1];
    physical_order[0].range.end_key = "n";
    try std.testing.expectError(error.InvalidGenerationPublication, requireRanges(&physical_order, &expected));
    var unordered = expected;
    std.mem.swap(Range, &unordered[1], &unordered[2]);
    try std.testing.expectError(error.InvalidGenerationPublication, requireRanges(&physical_order, &unordered));
}
