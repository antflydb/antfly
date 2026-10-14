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

//! Bounded source delivery preparation. These data commitments grant neither
//! metadata authority nor permission to consume a durable source batch.
const std = @import("std");
const claims = @import("relational_index_private_claims.zig");
const Source = claims.SourceStream;
const integrity = @import("relational_integrity_contract.zig");
const jobs = @import("relational_index_owner_job.zig");
const progress = @import("relational_index_publication_progress.zig");
const Allocator = std.mem.Allocator;
const Digest = integrity.Digest;
const Context = @import("../../api/operation.zig").RequestContext;

pub const Target = struct {
    owner: progress.Owner,
    start: []const u8 = "",
    end: []const u8 = "",
};

/// Owned immutable complete range cut. Build once per admitted cohort, not
/// once per source page. Ordinals preserve metadata identity after range sort.
pub const Cohort = struct {
    arena: std.heap.ArenaAllocator,
    targets: []const Target,
    by_ordinal: []const u16,
    digest: Digest,

    pub fn init(alloc: Allocator, input: []const Target) !Cohort {
        if (input.len == 0 or input.len > progress.max_owners) return error.InvalidIndexPublicationCandidate;
        var bytes: usize = input.len * (@sizeOf(Target) + @sizeOf(u64) + @sizeOf(u16));
        for (input) |target| {
            bytes = std.math.add(usize, bytes, target.start.len) catch return error.IndexPublicationTooLarge;
            bytes = std.math.add(usize, bytes, target.end.len) catch return error.IndexPublicationTooLarge;
        }
        if (bytes > 8 * 1024 * 1024) return error.IndexPublicationTooLarge;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        const targets = try a.dupe(Target, input);
        const by_ordinal = try a.alloc(u16, input.len);
        const groups = try a.alloc(u64, input.len);
        var seen: [progress.max_owners]bool = @splat(false);
        for (targets, groups) |*target, *group| {
            try target.owner.validate();
            const owner = target.owner;
            if (owner.ordinal >= input.len or seen[owner.ordinal] or
                !std.mem.eql(u8, &owner.plan_id, &input[0].owner.plan_id) or
                !std.mem.eql(u8, &owner.plan_digest, &input[0].owner.plan_digest) or
                owner.namespace.table_id != input[0].owner.namespace.table_id or
                (target.end.len != 0 and std.mem.order(u8, target.start, target.end) != .lt))
                return error.InvalidIndexPublicationCandidate;
            seen[owner.ordinal] = true;
            group.* = owner.group_id;
            target.start = try a.dupe(u8, target.start);
            target.end = try a.dupe(u8, target.end);
        }
        std.mem.sort(u64, groups, {}, std.sort.asc(u64));
        for (groups[1..], groups[0 .. groups.len - 1]) |next, prior|
            if (next == prior) return error.InvalidIndexPublicationCandidate;
        std.mem.sort(Target, targets, {}, struct {
            fn less(_: void, left: Target, right: Target) bool {
                return std.mem.order(u8, left.start, right.start) == .lt;
            }
        }.less);
        if (targets[0].start.len != 0 or targets[targets.len - 1].end.len != 0) return error.InvalidIndexPublicationCandidate;
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-claim-cohort/v1");
        for (targets, 0..) |target, i| {
            if (i != 0 and (targets[i - 1].end.len == 0 or !std.mem.eql(u8, targets[i - 1].end, target.start))) return error.InvalidIndexPublicationCandidate;
            by_ordinal[target.owner.ordinal] = @intCast(i);
            const owner = target.owner;
            state.update(&owner.plan_id);
            state.update(&owner.plan_digest);
            state.update(&owner.fence_digest);
            var numbers: [35]u8 = undefined;
            std.mem.writeInt(u16, numbers[0..2], owner.ordinal, .little);
            for ([_]u64{ owner.group_id, owner.namespace.table_id, owner.namespace.shard_id, owner.namespace.range_id }, 0..) |value, j|
                std.mem.writeInt(u64, numbers[2 + j * 8 ..][0..8], value, .little);
            numbers[34] = @backingInt(owner.authority);
            state.update(&numbers);
            hashField(&state, target.start);
            hashField(&state, target.end);
        }
        var digest: Digest = undefined;
        state.final(&digest);
        return .{ .arena = arena, .targets = targets, .by_ordinal = by_ordinal, .digest = digest };
    }

    pub fn deinit(self: *Cohort) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn route(self: *const Cohort, key: Digest, comparisons: *usize) !u16 {
        var low: usize = 0;
        var high: usize = self.targets.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            comparisons.* +|= 1;
            if (std.mem.order(u8, self.targets[middle].start, &key) != .gt) low = middle + 1 else high = middle;
        }
        if (low == 0) return error.InvalidIndexPublicationCandidate;
        const index = low - 1;
        const end = self.targets[index].end;
        if (end.len != 0 and std.mem.order(u8, &key, end) != .lt) return error.InvalidIndexPublicationCandidate;
        return @intCast(index);
    }

    pub fn requireBinding(self: *const Cohort, binding: jobs.Binding) !void {
        _ = try binding.encode();
        if (binding.ordinal >= self.by_ordinal.len) return error.PrivateIndexOwnerJobChanged;
        const owner = self.targets[self.by_ordinal[binding.ordinal]].owner;
        if (binding.phase != .reserved or binding.authority != owner.authority or
            !std.mem.eql(u8, &binding.id, &owner.plan_id) or !std.mem.eql(u8, &binding.plan_digest, &owner.plan_digest) or
            !std.mem.eql(u8, &binding.owner_digest, &owner.fence_digest) or
            !binding.fence.namespace.eql(owner.namespace) or binding.fence.owner_group_id != owner.group_id)
            return error.PrivateIndexOwnerJobChanged;
    }
};

/// Borrowed only while preparing. Row slices in the resulting plan borrow the
/// owned batch payload, not the temporary View or OwnedBatch struct address.
pub const View = struct {
    header: Source.Header,
    scope: Source.Scope,
    rows: []const Source.Row,

    pub fn fromBatch(batch: *const Source.OwnedBatch) View {
        return .{ .header = batch.header, .scope = batch.scope, .rows = batch.rows() };
    }

    pub fn validate(self: View, binding: jobs.Binding) !void {
        return self.validateControlled(binding, .{});
    }

    pub fn validateControlled(self: View, binding: jobs.Binding, context: Context) !void {
        try context.ensureActive();
        const raw = &self.header.bytes;
        _ = try Source.Header.decode(raw, binding, std.mem.readInt(u64, raw[104..112], .little));
        if (self.rows.len > claims.max_claims or self.rows.len != std.mem.readInt(u32, raw[96..100], .little) or
            !std.mem.eql(u8, &try self.scope.digest(), raw[40..72]) or
            !std.mem.eql(u8, &self.scope.generation, raw[72..88]) or
            !std.mem.eql(u8, &self.scope.tuple_fingerprint, raw[192..224]) or
            self.scope.schema_version != binding.physical_candidate.schema_version or
            self.scope.nulls_not_distinct != (raw[185] != 0)) return error.InvalidPrivateIndexClaimInventory;
        var state = std.crypto.hash.Blake3.init(.{});
        state.update(self.scope.table_name);
        var bytes: usize = self.scope.table_name.len;
        var previous: ?[]const u8 = null;
        for (self.rows) |row| {
            try context.ensureActive();
            if (previous) |prior| if (std.mem.order(u8, prior, row.key) != .lt) return error.InvalidPrivateIndexClaimInventory;
            previous = row.key;
            _ = try Source.targetBytes(self.scope.table_name.len, row.key.len, row.tuple.len);
            bytes += 8 + row.key.len + row.tuple.len;
            if (bytes > self.header.payloadLen()) return error.InvalidPrivateIndexClaimInventory;
            var lengths: [8]u8 = undefined;
            std.mem.writeInt(u32, lengths[0..4], @intCast(row.key.len), .little);
            std.mem.writeInt(u32, lengths[4..8], @intCast(row.tuple.len), .little);
            state.update(&lengths);
            state.update(row.key);
            state.update(row.tuple);
        }
        var digest: Digest = undefined;
        state.final(&digest);
        if (bytes != self.header.payloadLen() or !std.mem.eql(u8, &digest, raw[224..256])) return error.InvalidPrivateIndexClaimInventory;
    }
};

pub const Group = struct {
    target_index: u16,
    first: u16,
    count: u16,
    bytes: usize,
    row_mask: [16]u8 = @splat(0),
};

pub const Plan = struct {
    source: jobs.Binding,
    source_digest: Digest,
    cohort_digest: Digest,
    row_count: usize,
    group_count: usize,
    comparisons: usize,
    claims: [claims.max_claims]integrity.Claim,
    groups: [claims.max_claims]Group,
    digest: Digest,

    /// O(rows log owners + rows log rows), bounded scratch and no allocator.
    /// Payload hashing happens here, never in the serialized commit section.
    pub fn prepare(cohort: *const Cohort, binding: jobs.Binding, view: View) !Plan {
        return prepareControlled(cohort, binding, view, .{});
    }

    pub fn prepareControlled(cohort: *const Cohort, binding: jobs.Binding, view: View, context: Context) !Plan {
        try context.ensureActive();
        try cohort.requireBinding(binding);
        try view.validateControlled(binding, context);
        if (view.header.bytes[184] == @backingInt(Source.State.failed)) return error.InvalidRelationalIndexProgress;
        const Item = struct { row: u16, target: u16, routing: Digest, bytes: usize };
        var items: [claims.max_claims]Item = undefined;
        var comparisons: usize = 0;
        for (view.rows, items[0..view.rows.len], 0..) |row, *item, i| {
            try context.ensureActive();
            const address = try integrity.Address.init(view.scope.generation, row.tuple);
            item.* = .{ .row = @intCast(i), .target = try cohort.route(address.routing, &comparisons), .routing = address.routing, .bytes = try Source.targetBytes(view.scope.table_name.len, row.key.len, row.tuple.len) };
        }
        const active = items[0..view.rows.len];
        std.mem.sort(Item, active, view.rows, struct {
            fn less(rows: []const Source.Row, a: Item, b: Item) bool {
                if (a.target != b.target) return a.target < b.target;
                const order = std.mem.order(u8, &a.routing, &b.routing);
                return if (order == .eq) std.mem.order(u8, rows[a.row].key, rows[b.row].key) == .lt else order == .lt;
            }
        }.less);
        var result: Plan = .{ .source = binding, .source_digest = view.header.digest(), .cohort_digest = cohort.digest, .row_count = view.rows.len, .group_count = 0, .comparisons = comparisons, .claims = undefined, .groups = undefined, .digest = undefined };
        for (active, 0..) |item, i| {
            try context.ensureActive();
            const row = view.rows[item.row];
            result.claims[i] = .{ .tuple = row.tuple, .parent_table = view.scope.table_name, .parent_key = row.key, .schema_version = view.scope.schema_version };
            const new_group = result.group_count == 0 or result.groups[result.group_count - 1].target_index != item.target or
                result.groups[result.group_count - 1].bytes + item.bytes > claims.max_bytes;
            if (new_group) {
                result.groups[result.group_count] = .{ .target_index = item.target, .first = @intCast(i), .count = 0, .bytes = 0 };
                result.group_count += 1;
            }
            const group = &result.groups[result.group_count - 1];
            group.count += 1;
            group.bytes += item.bytes;
            group.row_mask[item.row / 8] |= @as(u8, 1) << @as(u3, @intCast(item.row % 8));
        }
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/private-claim-delivery-plan/v1");
        state.update(&result.source_digest);
        state.update(&result.cohort_digest);
        for (result.groups[0..result.group_count]) |group| {
            var numbers: [14]u8 = undefined;
            std.mem.writeInt(u16, numbers[0..2], group.target_index, .little);
            std.mem.writeInt(u16, numbers[2..4], group.first, .little);
            std.mem.writeInt(u16, numbers[4..6], group.count, .little);
            std.mem.writeInt(u64, numbers[6..14], group.bytes, .little);
            state.update(&numbers);
            state.update(&group.row_mask);
        }
        state.final(&result.digest);
        return result;
    }

    pub fn groupClaims(self: *const Plan, group_index: usize) ![]const integrity.Claim {
        if (group_index >= self.group_count) return error.InvalidPrivateIndexClaimInventory;
        const group = self.groups[group_index];
        return self.claims[group.first .. group.first + group.count];
    }

    /// Check structural target identity. Fresh metadata permission, actual
    /// receiving-range enforcement and root authentication remain mandatory.
    pub fn requireTarget(self: *const Plan, cohort: *const Cohort, group_index: usize, target: jobs.Binding) !void {
        if (group_index >= self.group_count or !std.mem.eql(u8, &cohort.digest, &self.cohort_digest)) return error.PrivateIndexOwnerJobChanged;
        try cohort.requireBinding(target);
        if (cohort.by_ordinal[target.ordinal] != self.groups[group_index].target_index or
            !std.mem.eql(u8, &target.inputs_digest, &self.source.inputs_digest) or
            !std.mem.eql(u8, &target.schema_before, &self.source.schema_before) or
            !std.mem.eql(u8, &target.integrity_reserved, &self.source.integrity_reserved) or
            !std.mem.eql(u8, &target.integrity_candidate, &self.source.integrity_candidate) or
            target.physical_candidate.schema_version != self.source.physical_candidate.schema_version)
            return error.PrivateIndexOwnerJobChanged;
    }

    /// Exact complete cover, independent of completion order. This checks
    /// descriptors only, not remote durability/authentication or consumption.
    pub fn coverage(self: *const Plan, completed: []const u16) !Digest {
        if (self.row_count > claims.max_claims or self.group_count > self.row_count or
            completed.len != self.group_count) return error.InvalidPrivateIndexClaimReceipt;
        var seen: [claims.max_claims]bool = @splat(false);
        var mask: u128 = 0;
        for (completed) |index| {
            if (index >= self.group_count or seen[index]) return error.InvalidPrivateIndexClaimReceipt;
            seen[index] = true;
            const rows = std.mem.readInt(u128, &self.groups[index].row_mask, .little);
            if (mask & rows != 0 or @popCount(rows) != self.groups[index].count or
                @as(usize, self.groups[index].first) + self.groups[index].count > self.row_count)
                return error.InvalidPrivateIndexClaimReceipt;
            mask |= rows;
        }
        const expected: u128 = if (self.row_count == 128) std.math.maxInt(u128) else (@as(u128, 1) << @as(u7, @intCast(self.row_count))) - 1;
        if (mask != expected) return error.InvalidPrivateIndexClaimReceipt;
        return self.digest;
    }
};

fn hashField(state: *std.crypto.hash.Blake3, value: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, value.len, .little);
    state.update(&length);
    state.update(value);
}
