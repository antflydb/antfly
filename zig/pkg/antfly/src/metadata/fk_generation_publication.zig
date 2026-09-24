// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Durable child-FK schema publication. The owner request carries only a plan
//! identity; a parent/child owner fetches this metadata record at read-index
//! and verifies its own exact fenced descriptor before applying Raft control.
const std = @import("std");
const records = @import("../common/topology_records.zig");
const topology = @import("../storage/db/relational_integrity_topology_contract.zig");
const integrity = @import("../storage/db/relational_integrity_contract.zig");
const integrity_catalog = @import("../storage/db/relational_integrity_catalog.zig");
const table_manager = @import("table_manager.zig");

pub const Id = [16]u8;
pub const Digest = [32]u8;
pub const max_owners = 4096;
pub const max_constraints = 128;
pub const max_bytes = 2 * 1024 * 1024;

pub fn schemaHasForeignKeys(alloc: std.mem.Allocator, schema_json: []const u8) !bool {
    if (schema_json.len == 0) return false;
    const Shape = struct { foreign_keys: ?[]const std.json.Value = null };
    var parsed = try std.json.parseFromSlice(Shape, alloc, schema_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    return if (parsed.value.foreign_keys) |foreign_keys| foreign_keys.len != 0 else false;
}
pub const prefix = "\x00\x00__metadata__:fk_generation_publication:";

pub fn prefixForGroup(buf: []u8, metadata_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, prefix ++ "{d}:", .{metadata_group_id});
}

pub fn key(buf: []u8, metadata_group_id: u64, child_table_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, prefix ++ "{d}:{d}", .{ metadata_group_id, child_table_id });
}

pub fn tableLockKey(buf: []u8, metadata_group_id: u64, table_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_generation_table_lock:{d}:{d}", .{ metadata_group_id, table_id });
}

pub fn tableLockPrefix(buf: []u8, metadata_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_generation_table_lock:{d}:", .{metadata_group_id});
}

pub const initial_prefix = "\x00\x00__metadata__:fk_initial_create:";

pub fn initialPrefixForGroup(buf: []u8, metadata_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, initial_prefix ++ "{d}:", .{metadata_group_id});
}

pub fn initialKey(buf: []u8, metadata_group_id: u64, child_table_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, initial_prefix ++ "{d}:{d}", .{ metadata_group_id, child_table_id });
}

/// The fixed-width pending index supports one seek per supervisor tick; the
/// immutable publication record itself need not be scanned for other tables.
pub fn initialWorkPrefixForGroup(buf: []u8, metadata_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_work:{d}:", .{metadata_group_id});
}

pub fn initialWorkKey(buf: []u8, metadata_group_id: u64, child_table_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_work:{d}:{x:0>16}", .{ metadata_group_id, child_table_id });
}

pub fn initialNameKey(buf: []u8, metadata_group_id: u64, namespace_id: u64, logical_name: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_name:{d}:{d}:{s}", .{ metadata_group_id, namespace_id, logical_name });
}

pub fn initialNamePrefixForGroup(buf: []u8, metadata_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_name:{d}:", .{metadata_group_id});
}

pub fn initialPhysicalNameKey(buf: []u8, metadata_group_id: u64, physical_name: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_physical_name:{d}:{s}", .{ metadata_group_id, physical_name });
}

pub fn initialPhysicalNamePrefixForGroup(buf: []u8, metadata_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_physical_name:{d}:", .{metadata_group_id});
}

pub fn initialGroupKey(buf: []u8, metadata_group_id: u64, owner_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_group:{d}:{d}", .{ metadata_group_id, owner_group_id });
}

pub fn initialGroupPrefixForGroup(buf: []u8, metadata_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_group:{d}:", .{metadata_group_id});
}

pub const Transition = struct {
    child_table_id: u64,
    child_table_name: []const u8,
    constraint_name: []const u8,
    expected_generation: ?integrity.Generation,
    next_generation: ?integrity.Generation,

    pub fn validate(self: Transition, child: records.TableRecord) !void {
        if (self.child_table_id != child.table_id or !std.mem.eql(u8, self.child_table_name, child.name) or
            self.constraint_name.len == 0 or self.constraint_name.len > 256 or
            (self.expected_generation == null and self.next_generation == null)) return error.InvalidGenerationPublication;
        if (self.expected_generation) |generation| if (std.mem.allEqual(u8, &generation, 0)) return error.InvalidGenerationPublication;
        if (self.next_generation) |generation| if (std.mem.allEqual(u8, &generation, 0)) return error.InvalidGenerationPublication;
        if (self.expected_generation != null and self.next_generation != null and
            std.mem.eql(u8, &self.expected_generation.?, &self.next_generation.?)) return error.InvalidGenerationPublication;
    }
};

pub const Parent = struct {
    table: records.TableRecord,
    ranges: []const records.RangeRecord,
    fences: []const topology.Fence,
    transitions: []const Transition,
};

pub const Plan = struct {
    id: Id,
    child_before: records.TableRecord,
    child_after: records.TableRecord,
    /// Exact durable owner catalog at the source-fence cut. This is only a
    /// candidate at begin; every child source receipt must attest its digest
    /// before any parent may stage the resulting generation transition.
    child_catalog_before_b64: []const u8,
    child_ranges: []const records.RangeRecord,
    child_fences: []const topology.Fence,
    parents: []const Parent,

    pub fn validate(self: Plan, alloc: std.mem.Allocator) !void {
        if (std.mem.allEqual(u8, &self.id, 0) or self.child_before.table_id == 0 or
            self.child_before.table_id != self.child_after.table_id or
            !std.mem.eql(u8, self.child_before.name, self.child_after.name) or
            self.child_ranges.len == 0 or self.child_ranges.len > max_owners or
            self.child_fences.len != self.child_ranges.len or self.parents.len == 0 or
            self.parents.len > 128 or self.child_catalog_before_b64.len == 0 or
            self.child_catalog_before_b64.len > std.base64.standard.Encoder.calcSize(integrity_catalog.max_catalog_bytes)) return error.InvalidGenerationPublication;
        if (std.mem.eql(u8, self.child_before.schema_json, self.child_after.schema_json)) return error.InvalidGenerationPublication;
        const schema = @import("../schema/mod.zig");
        var before = try schema.parseValidatedTableSchema(alloc, self.child_before.schema_json);
        defer before.deinit(alloc);
        var after = try schema.parseValidatedTableSchema(alloc, self.child_after.schema_json);
        defer after.deinit(alloc);
        const next_version = std.math.add(u32, before.version, 1) catch return error.InvalidGenerationPublication;
        if (before.storage_mode != .relational or after.storage_mode != .relational or
            after.version != next_version)
            return error.InvalidGenerationPublication;
        const catalog_len = std.base64.standard.Decoder.calcSizeForSlice(self.child_catalog_before_b64) catch return error.InvalidGenerationPublication;
        if (catalog_len > integrity_catalog.max_catalog_bytes) return error.InvalidGenerationPublication;
        const catalog_bytes = try alloc.alloc(u8, catalog_len);
        defer alloc.free(catalog_bytes);
        std.base64.standard.Decoder.decode(catalog_bytes, self.child_catalog_before_b64) catch return error.InvalidGenerationPublication;
        var prior_catalog = try integrity_catalog.decode(alloc, catalog_bytes);
        defer prior_catalog.deinit();
        const incarnation = try integrity_catalog.incarnationFromTableId(self.child_before.table_id);
        if (!std.mem.eql(u8, &prior_catalog.incarnation, &incarnation) or
            prior_catalog.schema_version != before.version) return error.InvalidGenerationPublication;
        var compiled_before = try compileCatalog(alloc, before, self.child_before.table_id, null);
        defer compiled_before.deinit();
        if (!std.mem.eql(u8, &prior_catalog.schema_digest, &compiled_before.catalog.schema_digest)) return error.InvalidGenerationPublication;
        for (compiled_before.catalog.bindings) |binding| {
            const observed = prior_catalog.find(binding.definition.kind, binding.definition.name) orelse return error.InvalidGenerationPublication;
            if (!std.mem.eql(u8, &observed.definition.fingerprint, &binding.definition.fingerprint)) return error.InvalidGenerationPublication;
        }
        var compiled_after = try compileCatalog(alloc, after, self.child_after.table_id, catalog_bytes);
        defer compiled_after.deinit();
        const before_foreign = try before.relationalForeignKeyDefinitions(alloc);
        defer if (before_foreign.len > 0) alloc.free(before_foreign);
        const after_foreign = try after.relationalForeignKeyDefinitions(alloc);
        defer if (after_foreign.len > 0) alloc.free(after_foreign);
        try table_manager.validateCompleteKeyspaceRanges(self.child_ranges);
        try validateOwnerFences(self.id, self.child_before, self.child_ranges, self.child_fences, .child_generation_source);
        var owner_count = self.child_ranges.len;
        var constraint_count: usize = 0;
        for (self.parents, 0..) |parent, index| {
            if (parent.table.table_id == 0 or parent.table.table_id == self.child_before.table_id or
                parent.ranges.len == 0 or parent.ranges.len > max_owners or parent.fences.len != parent.ranges.len or
                parent.transitions.len == 0 or parent.transitions.len > max_constraints) return error.InvalidGenerationPublication;
            for (self.parents[0..index]) |prior| if (prior.table.table_id == parent.table.table_id) return error.InvalidGenerationPublication;
            try table_manager.validateCompleteKeyspaceRanges(parent.ranges);
            try validateOwnerFences(self.id, parent.table, parent.ranges, parent.fences, .child_generation_parent);
            owner_count = std.math.add(usize, owner_count, parent.ranges.len) catch return error.InvalidGenerationPublication;
            constraint_count = std.math.add(usize, constraint_count, parent.transitions.len) catch return error.InvalidGenerationPublication;
            if (owner_count > max_owners or constraint_count > max_constraints) return error.InvalidGenerationPublication;
            for (parent.transitions, 0..) |transition, ti| {
                try transition.validate(self.child_before);
                if (ti != 0 and std.mem.order(u8, parent.transitions[ti - 1].constraint_name, transition.constraint_name) != .lt) return error.InvalidGenerationPublication;
                const old = prior_catalog.find(.foreign_key, transition.constraint_name);
                const new = compiled_after.catalog.find(.foreign_key, transition.constraint_name);
                const before_parent = foreignParent(before_foreign, transition.constraint_name);
                const after_parent = foreignParent(after_foreign, transition.constraint_name);
                if (before_parent == null and after_parent == null) return error.InvalidGenerationPublication;
                const retires_here = if (before_parent) |name| std.mem.eql(u8, name, parent.table.name) else false;
                const adds_here = if (after_parent) |name| std.mem.eql(u8, name, parent.table.name) else false;
                if (!retires_here and !adds_here) return error.InvalidGenerationPublication;
                if (!generationEqual(if (retires_here and old != null) old.?.generation else null, transition.expected_generation) or
                    !generationEqual(if (adds_here and new != null) new.?.generation else null, transition.next_generation))
                    return error.InvalidGenerationPublication;
            }
        }
        // Every changed FK must have exactly one parent transition. Unchanged
        // declarations retain their generation and require no parent fence.
        for (before_foreign) |fk| try self.validateForeignCoverage(prior_catalog, compiled_after.catalog, before_foreign, after_foreign, fk.name);
        for (after_foreign) |fk| {
            var was_present = false;
            for (before_foreign) |old| if (std.mem.eql(u8, old.name, fk.name)) {
                was_present = true;
                break;
            };
            if (was_present) continue;
            try self.validateForeignCoverage(prior_catalog, compiled_after.catalog, before_foreign, after_foreign, fk.name);
        }
    }

    fn validateForeignCoverage(self: Plan, old_catalog: integrity_catalog.Catalog, new_catalog: integrity_catalog.Catalog, before_foreign: anytype, after_foreign: anytype, name: []const u8) !void {
        const old = old_catalog.find(.foreign_key, name);
        const new = new_catalog.find(.foreign_key, name);
        const same = if (old != null and new != null) std.mem.eql(u8, &old.?.generation, &new.?.generation) else old == null and new == null;
        const old_parent = foreignParent(before_foreign, name);
        const new_parent = foreignParent(after_foreign, name);
        const reparent = old_parent != null and new_parent != null and !std.mem.eql(u8, old_parent.?, new_parent.?);
        var count: usize = 0;
        for (self.parents) |parent| for (parent.transitions) |transition| {
            if (std.mem.eql(u8, transition.constraint_name, name)) count += 1;
        };
        if (count != @as(usize, if (same) 0 else if (reparent) 2 else 1)) return error.InvalidGenerationPublication;
    }

    pub fn digest(self: Plan, alloc: std.mem.Allocator) !Digest {
        try self.validate(alloc);
        const json = try std.json.Stringify.valueAlloc(alloc, self, .{});
        defer alloc.free(json);
        if (json.len > max_bytes) return error.InvalidGenerationPublication;
        var result: Digest = undefined;
        std.crypto.hash.Blake3.hash(json, &result, .{});
        return result;
    }

    pub const ChildIdentity = struct {
        before_schema_version: u32,
        before_schema_digest: Digest,
        before_catalog_digest: Digest,
        after_schema_version: u32,
        after_schema_digest: Digest,
        after_catalog_digest: Digest,
    };

    pub fn childIdentity(self: Plan, alloc: std.mem.Allocator) !ChildIdentity {
        const catalog_len = std.base64.standard.Decoder.calcSizeForSlice(self.child_catalog_before_b64) catch return error.InvalidGenerationPublication;
        if (catalog_len == 0 or catalog_len > integrity_catalog.max_catalog_bytes) return error.InvalidGenerationPublication;
        const old_bytes = try alloc.alloc(u8, catalog_len);
        defer alloc.free(old_bytes);
        std.base64.standard.Decoder.decode(old_bytes, self.child_catalog_before_b64) catch return error.InvalidGenerationPublication;
        var old_digest: Digest = undefined;
        std.crypto.hash.Blake3.hash(old_bytes, &old_digest, .{});
        var before = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, self.child_before.schema_json);
        defer before.deinit(alloc);
        var after = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, self.child_after.schema_json);
        defer after.deinit(alloc);
        var old_compiled = try compileCatalog(alloc, before, self.child_before.table_id, null);
        defer old_compiled.deinit();
        var next_compiled = try compileCatalog(alloc, after, self.child_after.table_id, old_bytes);
        defer next_compiled.deinit();
        var next_digest: Digest = undefined;
        std.crypto.hash.Blake3.hash(next_compiled.value, &next_digest, .{});
        return .{
            .before_schema_version = before.version,
            .before_schema_digest = old_compiled.catalog.schema_digest,
            .before_catalog_digest = old_digest,
            .after_schema_version = after.version,
            .after_schema_digest = next_compiled.catalog.schema_digest,
            .after_catalog_digest = next_digest,
        };
    }

    pub fn verifySourceReceipt(self: Plan, identity: ChildIdentity, plan_digest: Digest, receipt: @import("../api/relational_fk_generation_publication.zig").SourceReceipt) !void {
        if (!std.mem.eql(u8, &receipt.plan_id, &self.id) or
            !std.mem.eql(u8, &receipt.plan_digest, &plan_digest) or
            receipt.child_table_id != self.child_before.table_id or
            receipt.applied_term == 0 or receipt.applied_index == 0) return error.InvalidGenerationPublication;
        const index = for (self.child_ranges, 0..) |range, i| {
            if (range.group_id == receipt.child_group_id) break i;
        } else return error.InvalidGenerationPublication;
        const fence_bytes = try self.child_fences[index].encode();
        var fence_digest: Digest = undefined;
        std.crypto.hash.Blake3.hash(&fence_bytes, &fence_digest, .{});
        if (!std.mem.eql(u8, &receipt.fence_digest, &fence_digest)) return error.InvalidGenerationPublication;

        if (receipt.before_schema_version != identity.before_schema_version or receipt.after_schema_version != identity.after_schema_version or
            !std.mem.eql(u8, &receipt.before_schema_digest, &identity.before_schema_digest) or
            !std.mem.eql(u8, &receipt.after_schema_digest, &identity.after_schema_digest) or
            !std.mem.eql(u8, &receipt.before_catalog_digest, &identity.before_catalog_digest) or
            !std.mem.eql(u8, &receipt.after_catalog_digest, &identity.after_catalog_digest)) return error.InvalidGenerationPublication;
    }

    pub fn verifyParentReceipt(self: Plan, alloc: std.mem.Allocator, plan_digest: Digest, receipt: @import("../api/relational_fk_generation_publication.zig").Receipt) !void {
        if (!std.mem.eql(u8, &receipt.plan_id, &self.id) or
            receipt.child_table_id != self.child_before.table_id or
            receipt.applied_term == 0 or receipt.applied_index == 0) return error.InvalidGenerationPublication;
        if (!std.mem.eql(u8, &receipt.decision_digest, &plan_digest)) return error.InvalidGenerationPublication;
        const parent = for (self.parents) |candidate| {
            if (candidate.table.table_id == receipt.parent_table_id) break candidate;
        } else return error.InvalidGenerationPublication;
        for (parent.ranges) |range| if (range.group_id == receipt.parent_group_id) break else return error.InvalidGenerationPublication;
        const admission = @import("../storage/db/relational_integrity_generation_admission.zig");
        const transitions = try alloc.alloc(admission.Transition, parent.transitions.len);
        defer alloc.free(transitions);
        for (parent.transitions, transitions) |transition, *out| out.* = .{
            .child_table_id = transition.child_table_id,
            .child_table_name = transition.child_table_name,
            .constraint_name = transition.constraint_name,
            .expected_generation = transition.expected_generation,
            .next_generation = transition.next_generation,
            .plan_id = self.id,
            .decision_digest = plan_digest,
        };
        const expected = try admission.transitionsDigest(transitions);
        if (!std.mem.eql(u8, &receipt.transitions_digest, &expected)) return error.InvalidGenerationPublication;
    }
};

/// Initial FK-bearing CREATE has no old child generation. Metadata reserves
/// this candidate invisibly, provisions each child owner under a plan-bound
/// zero-write fence, and publishes only after every parent accepts the new
/// generation. It deliberately does not invent an old schema/source receipt.
pub const InitialCreatePlan = struct {
    id: Id,
    catalog_id: u64,
    expected_catalog_revision: u64,
    tablespace_id: u64 = 0,
    min_ranges_explicit: bool = false,
    child: records.TableRecord,
    child_ranges: []const records.RangeRecord,
    parents: []const Parent,
    logical_name: []const u8,
    namespace_id: u64,

    pub fn validate(self: InitialCreatePlan, alloc: std.mem.Allocator) !void {
        if (std.mem.allEqual(u8, &self.id, 0) or self.catalog_id < 3 or self.namespace_id == 0 or
            self.logical_name.len == 0 or self.logical_name.len > 256 or
            self.child.table_id == 0 or self.child.name.len == 0 or
            self.child_ranges.len == 0 or self.child_ranges.len > max_owners or
            self.parents.len == 0 or self.parents.len > max_constraints) return error.InvalidGenerationPublication;
        const expected_physical_name = try std.fmt.allocPrint(alloc, "table:{d}", .{self.catalog_id});
        defer alloc.free(expected_physical_name);
        const expected_physical_id = std.hash.Wyhash.hash(0x54424c45, expected_physical_name);
        if (!std.mem.eql(u8, self.child.name, expected_physical_name) or
            self.child.table_id != (if (expected_physical_id == 0) @as(u64, 1) else expected_physical_id))
            return error.InvalidGenerationPublication;
        var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, self.child.schema_json);
        defer parsed.deinit(alloc);
        if (parsed.storage_mode != .relational) return error.InvalidGenerationPublication;
        const fks = try parsed.relationalForeignKeyDefinitions(alloc);
        defer if (fks.len > 0) alloc.free(fks);
        if (fks.len == 0 or fks.len > max_constraints) return error.InvalidGenerationPublication;
        var compiled = try compileCatalog(alloc, parsed, self.child.table_id, null);
        defer compiled.deinit();
        try table_manager.validateCompleteKeyspaceRanges(self.child_ranges);
        for (self.child_ranges) |range| if (range.table_id != self.child.table_id) return error.InvalidGenerationPublication;
        if (self.child_ranges.len != @as(usize, self.child.min_ranges)) return error.InvalidGenerationPublication;
        var owners = self.child_ranges.len;
        var transitions: usize = 0;
        for (self.parents, 0..) |parent, index| {
            if (parent.table.table_id == 0 or parent.table.table_id == self.child.table_id or
                parent.ranges.len == 0 or parent.ranges.len > max_owners or
                parent.fences.len != parent.ranges.len or parent.transitions.len == 0 or
                parent.transitions.len > max_constraints) return error.InvalidGenerationPublication;
            for (self.parents[0..index]) |prior| if (prior.table.table_id == parent.table.table_id) return error.InvalidGenerationPublication;
            try table_manager.validateCompleteKeyspaceRanges(parent.ranges);
            try validateOwnerFences(self.id, parent.table, parent.ranges, parent.fences, .child_generation_parent);
            owners = std.math.add(usize, owners, parent.ranges.len) catch return error.InvalidGenerationPublication;
            transitions = std.math.add(usize, transitions, parent.transitions.len) catch return error.InvalidGenerationPublication;
            if (owners > max_owners or transitions > max_constraints) return error.InvalidGenerationPublication;
            for (parent.transitions, 0..) |transition, ti| {
                try transition.validate(self.child);
                if (transition.expected_generation != null or transition.next_generation == null or
                    (ti != 0 and std.mem.order(u8, parent.transitions[ti - 1].constraint_name, transition.constraint_name) != .lt))
                    return error.InvalidGenerationPublication;
                const fk = compiled.catalog.find(.foreign_key, transition.constraint_name) orelse return error.InvalidGenerationPublication;
                if (!std.mem.eql(u8, &fk.generation, &transition.next_generation.?)) return error.InvalidGenerationPublication;
                const parent_name = foreignParent(fks, transition.constraint_name) orelse return error.InvalidGenerationPublication;
                if (!std.mem.eql(u8, parent.table.name, parent_name)) return error.InvalidGenerationPublication;
            }
        }
        if (transitions != fks.len) return error.InvalidGenerationPublication;
        const group_ids = try alloc.alloc(u64, owners);
        defer alloc.free(group_ids);
        var group_index: usize = 0;
        for (self.child_ranges) |range| {
            group_ids[group_index] = range.group_id;
            group_index += 1;
        }
        for (self.parents) |parent| for (parent.ranges) |range| {
            group_ids[group_index] = range.group_id;
            group_index += 1;
        };
        std.mem.sort(u64, group_ids, {}, std.sort.asc(u64));
        for (group_ids[1..], 1..) |group_id, index| {
            if (group_id == group_ids[index - 1]) return error.InvalidGenerationPublication;
        }
        for (fks) |fk| {
            var count: usize = 0;
            for (self.parents) |parent| for (parent.transitions) |transition| {
                if (std.mem.eql(u8, transition.constraint_name, fk.name)) count += 1;
            };
            if (count != 1) return error.InvalidGenerationPublication;
        }
    }

    pub fn digest(self: InitialCreatePlan, alloc: std.mem.Allocator) !Digest {
        try self.validate(alloc);
        const json = try std.json.Stringify.valueAlloc(alloc, self, .{});
        defer alloc.free(json);
        if (json.len > max_bytes) return error.InvalidGenerationPublication;
        var result: Digest = undefined;
        std.crypto.hash.Blake3.hash(json, &result, .{});
        return result;
    }

    pub const CandidateIdentity = struct {
        schema_version: u32,
        schema_digest: Digest,
        public_schema_json_digest: Digest,
        catalog_digest: Digest,
    };

    pub fn candidateIdentity(self: InitialCreatePlan, alloc: std.mem.Allocator) !CandidateIdentity {
        var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, self.child.schema_json);
        defer parsed.deinit(alloc);
        var compiled = try compileCatalog(alloc, parsed, self.child.table_id, null);
        defer compiled.deinit();
        var json_digest: Digest = undefined;
        std.crypto.hash.Blake3.hash(self.child.schema_json, &json_digest, .{});
        var catalog_digest: Digest = undefined;
        std.crypto.hash.Blake3.hash(compiled.value, &catalog_digest, .{});
        return .{ .schema_version = parsed.version, .schema_digest = compiled.catalog.schema_digest, .public_schema_json_digest = json_digest, .catalog_digest = catalog_digest };
    }

    pub fn verifyChildReceipt(self: InitialCreatePlan, identity: CandidateIdentity, plan_digest: Digest, receipt: @import("../api/relational_fk_generation_publication.zig").InitialChildReceipt) !void {
        if (!std.mem.eql(u8, &receipt.plan_id, &self.id) or
            receipt.child_table_id != self.child.table_id or
            !std.mem.eql(u8, &receipt.plan_digest, &plan_digest) or
            receipt.schema_version != identity.schema_version or
            !std.mem.eql(u8, &receipt.schema_digest, &identity.schema_digest) or
            !std.mem.eql(u8, &receipt.public_schema_json_digest, &identity.public_schema_json_digest) or
            !std.mem.eql(u8, &receipt.catalog_digest, &identity.catalog_digest) or
            receipt.row_count != 0 or receipt.applied_term == 0 or receipt.applied_index == 0)
            return error.InvalidGenerationPublication;
        const range = for (self.child_ranges) |candidate| {
            if (candidate.group_id == receipt.child_group_id) break candidate;
        } else return error.InvalidGenerationPublication;
        if (receipt.namespace.table_id != self.child.table_id or
            receipt.namespace.shard_id != table_manager.rangeDocIdentityShardId(range) or
            receipt.namespace.range_id != table_manager.rangeDocIdentityRangeId(range))
            return error.InvalidGenerationPublication;
    }

    pub fn verifyParentReceipt(self: InitialCreatePlan, alloc: std.mem.Allocator, plan_digest: Digest, receipt: @import("../api/relational_fk_generation_publication.zig").Receipt) !void {
        if (!std.mem.eql(u8, &receipt.plan_id, &self.id) or
            receipt.child_table_id != self.child.table_id or
            receipt.applied_term == 0 or receipt.applied_index == 0 or
            !std.mem.eql(u8, &receipt.decision_digest, &plan_digest))
            return error.InvalidGenerationPublication;
        const parent = for (self.parents) |candidate| {
            if (candidate.table.table_id == receipt.parent_table_id) break candidate;
        } else return error.InvalidGenerationPublication;
        for (parent.ranges) |range| if (range.group_id == receipt.parent_group_id) break else return error.InvalidGenerationPublication;
        const admission = @import("../storage/db/relational_integrity_generation_admission.zig");
        const transitions = try alloc.alloc(admission.Transition, parent.transitions.len);
        defer alloc.free(transitions);
        for (parent.transitions, transitions) |transition, *out| out.* = .{
            .child_table_id = transition.child_table_id,
            .child_table_name = transition.child_table_name,
            .constraint_name = transition.constraint_name,
            .expected_generation = null,
            .next_generation = transition.next_generation,
            .plan_id = self.id,
            .decision_digest = plan_digest,
        };
        if (!std.mem.eql(u8, &receipt.transitions_digest, &try admission.transitionsDigest(transitions)))
            return error.InvalidGenerationPublication;
    }
};

/// The private owner receives only this identity request. Metadata derives
/// the candidate and exact owner descriptor from a committed read-index cut;
/// neither the worker nor the caller may supply schema bytes or a route.
pub const InitialChildDecisionRequest = struct {
    plan_id: Id,
    child_table_id: u64,
    child_group_id: u64,
    action: @import("../api/relational_fk_generation_publication.zig").InitialChildAction,
};

pub const InitialCreatePrepareRequest = struct {
    namespace_id: u64,
    logical_name: []const u8,
    tablespace_name: ?[]const u8 = null,
    min_ranges_explicit: bool = false,
    /// Fully normalized create definition; metadata assigns physical identity
    /// and computes owner ranges from its current catalog/fence cut.
    candidate: records.TableRecord,
};

pub const InitialCreatePrepare = struct {
    catalog_id: u64,
    expected_catalog_revision: u64,
    tablespace_id: u64,
    min_ranges_explicit: bool,
    child: records.TableRecord,
    child_ranges: []const records.RangeRecord,
    transition_generation: u64,
    metadata_incarnation: @import("incarnation.zig").MetadataClusterIncarnation,
    metadata_epoch: u64,
};

pub const InitialChildDecision = struct {
    plan_id: Id,
    plan_digest: Digest,
    revision: u64,
    phase: enum { provisioning_child, canceling, published_hidden, publishing_child },
    child: records.TableRecord,
    range: records.RangeRecord,
    catalog_b64: []const u8,
    catalog_digest: Digest,
    schema_digest: Digest,
    metadata_group_id: u64,
    metadata_incarnation: @import("incarnation.zig").MetadataClusterIncarnation,
    metadata_epoch: u64,
    routable: bool = false,

    pub fn validate(self: InitialChildDecision) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or std.mem.allEqual(u8, &self.plan_digest, 0) or
            self.revision == 0 or self.child.table_id == 0 or self.range.table_id != self.child.table_id or
            self.range.group_id == 0 or self.metadata_group_id == 0 or
            self.catalog_b64.len == 0 or self.catalog_b64.len > std.base64.standard.Encoder.calcSize(integrity_catalog.max_catalog_bytes) or
            std.mem.allEqual(u8, &self.catalog_digest, 0) or std.mem.allEqual(u8, &self.schema_digest, 0) or self.routable)
            return error.InvalidGenerationPublication;
    }
};

pub const InitialParentDecision = struct {
    phase: InitialPhase,
    metadata_group_id: u64,
    metadata_incarnation: @import("incarnation.zig").MetadataClusterIncarnation,
    metadata_epoch: u64,
    plan_digest: Digest,
    fence: topology.Fence,
    transitions: []const Transition,
    parent_table: records.TableRecord,
    parent_range: records.RangeRecord,
};

pub const InitialPhase = enum {
    provisioning_child,
    staging_parents,
    activating_parents,
    acknowledging_parents,
    published_hidden,
    publishing_child,
    published,
    canceling,
    canceled,
};

pub const InitialCommand = struct {
    plan_id: Id,
    child_table_id: u64,
    expected_revision: u64,
    action: enum { begin, child_provisioned, parent_staged, parent_activated, parent_acknowledged, child_released, publish_child, cancel, child_canceled, parent_canceled },
    plan: ?InitialCreatePlan = null,
    child_receipt: ?@import("../api/relational_fk_generation_publication.zig").InitialChildReceipt = null,
    parent_receipt: ?@import("../api/relational_fk_generation_publication.zig").Receipt = null,

    pub fn validateShape(self: InitialCommand) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or self.child_table_id == 0) return error.InvalidGenerationPublication;
        switch (self.action) {
            .begin => if (self.expected_revision != 0 or self.plan == null or self.child_receipt != null or self.parent_receipt != null or
                !std.mem.eql(u8, &self.plan_id, &self.plan.?.id) or self.child_table_id != self.plan.?.child.table_id) return error.InvalidGenerationPublication,
            .child_provisioned, .child_released, .child_canceled => if (self.expected_revision == 0 or self.plan != null or self.child_receipt == null or self.parent_receipt != null) return error.InvalidGenerationPublication,
            .parent_staged, .parent_activated, .parent_acknowledged, .parent_canceled => if (self.expected_revision == 0 or self.plan != null or self.child_receipt != null or self.parent_receipt == null) return error.InvalidGenerationPublication,
            .publish_child, .cancel => if (self.expected_revision == 0 or self.plan != null or self.child_receipt != null or self.parent_receipt != null) return error.InvalidGenerationPublication,
        }
    }
};

pub const InitialPublication = struct {
    plan: InitialCreatePlan,
    plan_digest: Digest,
    candidate: InitialCreatePlan.CandidateIdentity,
    revision: u64,
    phase: InitialPhase,
    child_provisioned: []const Receipt = &.{},
    parent_staged: []const Receipt = &.{},
    parent_activated: []const Receipt = &.{},
    parent_acknowledged: []const Receipt = &.{},
    child_released: []const Receipt = &.{},
    child_canceled: []const Receipt = &.{},
    parent_canceled: []const Receipt = &.{},

    pub fn apply(self: InitialPublication, alloc: std.mem.Allocator, command: InitialCommand) !InitialPublication {
        try command.validateShape();
        if (command.action == .begin or command.child_table_id != self.plan.child.table_id or
            !std.mem.eql(u8, &command.plan_id, &self.plan.id) or command.expected_revision != self.revision)
            return error.GenerationPublicationChanged;
        var next = self;
        next.revision = std.math.add(u64, self.revision, 1) catch return error.GenerationPublicationChanged;
        switch (command.action) {
            .begin => unreachable,
            .child_provisioned, .child_released, .child_canceled => {
                const receipt = command.child_receipt.?;
                const expected_action: @import("../api/relational_fk_generation_publication.zig").InitialChildAction = switch (command.action) {
                    .child_provisioned => .provision,
                    .child_released => .release,
                    .child_canceled => .cancel,
                    else => unreachable,
                };
                if (receipt.action != expected_action) return error.GenerationPublicationChanged;
                try self.plan.verifyChildReceipt(self.candidate, self.plan_digest, receipt);
                const compact: Receipt = .{ .group_id = receipt.child_group_id, .digest = receipt.digest() };
                switch (command.action) {
                    .child_provisioned => {
                        if (self.phase != .provisioning_child) return error.GenerationPublicationChanged;
                        next.child_provisioned = try appendReceipt(alloc, self.child_provisioned, compact);
                        if (next.child_provisioned.len == self.plan.child_ranges.len) next.phase = .staging_parents;
                    },
                    .child_released => {
                        if (self.phase != .published_hidden) return error.GenerationPublicationChanged;
                        next.child_released = try appendReceipt(alloc, self.child_released, compact);
                        if (next.child_released.len == self.plan.child_ranges.len) next.phase = .publishing_child;
                    },
                    .child_canceled => {
                        if (self.phase != .canceling) return error.GenerationPublicationChanged;
                        next.child_canceled = try appendReceipt(alloc, self.child_canceled, compact);
                    },
                    else => unreachable,
                }
            },
            .parent_staged, .parent_activated, .parent_acknowledged, .parent_canceled => {
                const receipt = command.parent_receipt.?;
                const expected_action: @import("../api/relational_fk_generation_publication.zig").Action = switch (command.action) {
                    .parent_staged => .stage,
                    .parent_activated => .activate,
                    .parent_acknowledged => .acknowledge,
                    .parent_canceled => .cancel,
                    else => unreachable,
                };
                if (receipt.action != expected_action) return error.GenerationPublicationChanged;
                try self.plan.verifyParentReceipt(alloc, self.plan_digest, receipt);
                const compact: Receipt = .{ .group_id = receipt.parent_group_id, .digest = receipt.digest() };
                var parent_count: usize = 0;
                for (self.plan.parents) |parent| parent_count += parent.ranges.len;
                switch (command.action) {
                    .parent_staged => {
                        if (self.phase != .staging_parents) return error.GenerationPublicationChanged;
                        next.parent_staged = try appendReceipt(alloc, self.parent_staged, compact);
                        if (next.parent_staged.len == parent_count) next.phase = .activating_parents;
                    },
                    .parent_activated => {
                        if (self.phase != .activating_parents) return error.GenerationPublicationChanged;
                        next.parent_activated = try appendReceipt(alloc, self.parent_activated, compact);
                        if (next.parent_activated.len == parent_count) next.phase = .acknowledging_parents;
                    },
                    .parent_acknowledged => {
                        if (self.phase != .acknowledging_parents) return error.GenerationPublicationChanged;
                        next.parent_acknowledged = try appendReceipt(alloc, self.parent_acknowledged, compact);
                        if (next.parent_acknowledged.len == parent_count) next.phase = .published_hidden;
                    },
                    .parent_canceled => {
                        if (self.phase != .canceling) return error.GenerationPublicationChanged;
                        next.parent_canceled = try appendReceipt(alloc, self.parent_canceled, compact);
                    },
                    else => unreachable,
                }
            },
            .publish_child => {
                if (self.phase != .publishing_child) return error.GenerationPublicationChanged;
                next.phase = .published;
            },
            .cancel => {
                if ((self.phase != .provisioning_child and self.phase != .staging_parents and self.phase != .activating_parents) or self.parent_activated.len != 0)
                    return error.GenerationPublicationChanged;
                next.phase = .canceling;
            },
        }
        if (next.phase == .canceling and next.child_canceled.len == self.plan.child_ranges.len) {
            var parent_count: usize = 0;
            for (self.plan.parents) |parent| parent_count += parent.ranges.len;
            if (next.parent_canceled.len == parent_count) next.phase = .canceled;
        }
        try next.validateState(alloc);
        return next;
    }

    pub fn validateState(self: InitialPublication, alloc: std.mem.Allocator) !void {
        if (self.revision == 0 or std.mem.allEqual(u8, &self.plan_digest, 0)) return error.InvalidGenerationPublication;
        const child_groups = try alloc.dupe(records.RangeRecord, self.plan.child_ranges);
        defer alloc.free(child_groups);
        std.mem.sort(records.RangeRecord, child_groups, {}, struct {
            fn less(_: void, lhs: records.RangeRecord, rhs: records.RangeRecord) bool {
                return lhs.group_id < rhs.group_id;
            }
        }.less);
        try validateReceipts(child_groups, self.child_provisioned);
        try validateReceipts(child_groups, self.child_released);
        try validateReceipts(child_groups, self.child_canceled);
        var parent_groups: std.ArrayList(records.RangeRecord) = .empty;
        defer parent_groups.deinit(alloc);
        for (self.plan.parents) |parent| try parent_groups.appendSlice(alloc, parent.ranges);
        std.mem.sort(records.RangeRecord, parent_groups.items, {}, struct {
            fn less(_: void, lhs: records.RangeRecord, rhs: records.RangeRecord) bool {
                return lhs.group_id < rhs.group_id;
            }
        }.less);
        for (parent_groups.items[1..], 1..) |range, index| if (parent_groups.items[index - 1].group_id == range.group_id) return error.InvalidGenerationPublication;
        try validateReceipts(parent_groups.items, self.parent_staged);
        try validateReceipts(parent_groups.items, self.parent_activated);
        try validateReceipts(parent_groups.items, self.parent_acknowledged);
        try validateReceipts(parent_groups.items, self.parent_canceled);
        if (self.phase != .canceling and self.phase != .canceled and (self.child_canceled.len != 0 or self.parent_canceled.len != 0)) return error.InvalidGenerationPublication;
        switch (self.phase) {
            .provisioning_child => if (self.parent_staged.len != 0 or self.child_released.len != 0) return error.InvalidGenerationPublication,
            .staging_parents => if (self.child_provisioned.len != self.plan.child_ranges.len or self.parent_activated.len != 0) return error.InvalidGenerationPublication,
            .activating_parents => if (self.parent_staged.len != parent_groups.items.len or self.parent_acknowledged.len != 0) return error.InvalidGenerationPublication,
            .acknowledging_parents => if (self.parent_activated.len != parent_groups.items.len or self.child_released.len != 0) return error.InvalidGenerationPublication,
            .published_hidden, .publishing_child => if (self.parent_acknowledged.len != parent_groups.items.len or self.child_provisioned.len != self.plan.child_ranges.len) return error.InvalidGenerationPublication,
            .published => if (self.child_released.len != self.plan.child_ranges.len) return error.InvalidGenerationPublication,
            .canceling, .canceled => if (self.parent_activated.len != 0 or self.parent_acknowledged.len != 0 or self.child_released.len != 0) return error.InvalidGenerationPublication,
        }
        if (self.phase == .canceled and (self.child_canceled.len != self.plan.child_ranges.len or self.parent_canceled.len != parent_groups.items.len)) return error.InvalidGenerationPublication;
    }

    /// A supervisor receives only one bounded next step. The immutable plan
    /// and accumulated owner receipts stay in metadata; owners fetch their
    /// exact decision independently from the read-index endpoint.
    pub fn nextWork(self: InitialPublication) !?InitialWork {
        const target: InitialWork.Target = switch (self.phase) {
            .provisioning_child => try self.nextChild(self.child_provisioned, .provision),
            .staging_parents => self.nextParent(self.parent_staged, .stage) orelse return error.InvalidGenerationPublication,
            .activating_parents => self.nextParent(self.parent_activated, .activate) orelse return error.InvalidGenerationPublication,
            .acknowledging_parents => self.nextParent(self.parent_acknowledged, .acknowledge) orelse return error.InvalidGenerationPublication,
            .published_hidden => try self.nextChild(self.child_released, .release),
            .publishing_child => .publish_child,
            .canceling => blk: {
                if (self.nextParent(self.parent_canceled, .cancel)) |parent| break :blk parent;
                break :blk try self.nextChild(self.child_canceled, .cancel);
            },
            .published, .canceled => return null,
        };
        return .{ .plan_id = self.plan.id, .child_table_id = self.plan.child.table_id, .child_table_name = self.plan.child.name, .revision = self.revision, .phase = self.phase, .target = target };
    }

    fn nextChild(self: InitialPublication, receipts: []const Receipt, action: @import("../api/relational_fk_generation_publication.zig").InitialChildAction) !InitialWork.Target {
        for (self.plan.child_ranges) |range| {
            if (!hasReceipt(receipts, range.group_id)) return .{ .child = .{ .group_id = range.group_id, .action = action } };
        }
        return error.InvalidGenerationPublication;
    }

    fn nextParent(self: InitialPublication, receipts: []const Receipt, action: @import("../api/relational_fk_generation_publication.zig").Action) ?InitialWork.Target {
        for (self.plan.parents) |parent| for (parent.ranges) |range| {
            if (!hasReceipt(receipts, range.group_id)) return .{ .parent = .{
                .table_name = parent.table.name,
                .table_id = parent.table.table_id,
                .group_id = range.group_id,
                .action = action,
            } };
        };
        return null;
    }
};

pub const InitialWork = struct {
    plan_id: Id,
    child_table_id: u64,
    child_table_name: []const u8,
    revision: u64,
    phase: InitialPhase,
    target: Target,

    pub const Target = union(enum) {
        child: struct { group_id: u64, action: @import("../api/relational_fk_generation_publication.zig").InitialChildAction },
        parent: struct { table_name: []const u8, table_id: u64, group_id: u64, action: @import("../api/relational_fk_generation_publication.zig").Action },
        publish_child,
    };
};

fn hasReceipt(receipts: []const Receipt, group_id: u64) bool {
    var low: usize = 0;
    var high = receipts.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (receipts[mid].group_id < group_id) low = mid + 1 else high = mid;
    }
    return low < receipts.len and receipts[low].group_id == group_id;
}

fn generationEqual(lhs: ?integrity.Generation, rhs: ?integrity.Generation) bool {
    if (lhs == null or rhs == null) return lhs == null and rhs == null;
    return std.mem.eql(u8, &lhs.?, &rhs.?);
}

fn foreignParent(foreign: anytype, name: []const u8) ?[]const u8 {
    for (foreign) |fk| if (std.mem.eql(u8, fk.name, name)) return fk.parent_table;
    return null;
}

pub fn compileCatalog(alloc: std.mem.Allocator, parsed: @import("../schema/mod.zig").ParsedTableSchema, table_id: u64, previous: ?[]const u8) !integrity_catalog.Update {
    const schema_api = @import("../schema/mod.zig");
    const native_schema = @import("../storage/schema.zig");
    const declarations = @import("../schema/relational_declarations.zig");
    const runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer native_schema.freeSchema(alloc, runtime);
    const bytes = try native_schema.serializeSchema(alloc, runtime);
    defer alloc.free(bytes);
    var digest: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &digest, .{});
    const definitions = try declarations.definitionFingerprints(alloc, parsed, runtime);
    defer declarations.freeDefinitions(alloc, definitions);
    return integrity_catalog.prepare(alloc, previous, try integrity_catalog.incarnationFromTableId(table_id), runtime.version, digest, definitions);
}

/// Canonical helper for the API plan builder. Names in the returned slice are
/// owned by `alloc`; a request arena can release them with the full plan.
pub const DerivedTransition = struct {
    parent_table_name: []const u8,
    transition: Transition,
};

pub fn freeDerivedTransitions(alloc: std.mem.Allocator, transitions: []const DerivedTransition) void {
    for (transitions) |entry| {
        alloc.free(entry.parent_table_name);
        alloc.free(entry.transition.child_table_name);
        alloc.free(entry.transition.constraint_name);
    }
    alloc.free(transitions);
}

fn appendDerived(result: *std.ArrayList(DerivedTransition), alloc: std.mem.Allocator, parent_name: []const u8, child_id: u64, child_name: []const u8, constraint_name: []const u8, expected: ?integrity.Generation, next: ?integrity.Generation) !void {
    const parent_copy = try alloc.dupe(u8, parent_name);
    errdefer alloc.free(parent_copy);
    const child_copy = try alloc.dupe(u8, child_name);
    errdefer alloc.free(child_copy);
    const constraint_copy = try alloc.dupe(u8, constraint_name);
    errdefer alloc.free(constraint_copy);
    try result.append(alloc, .{ .parent_table_name = parent_copy, .transition = .{
        .child_table_id = child_id,
        .child_table_name = child_copy,
        .constraint_name = constraint_copy,
        .expected_generation = expected,
        .next_generation = next,
    } });
}

pub fn deriveTransitions(alloc: std.mem.Allocator, child_table_id: u64, child_table_name: []const u8, before_json: []const u8, after_json: []const u8, old_catalog_b64: []const u8) ![]const DerivedTransition {
    if (child_table_id == 0 or child_table_name.len == 0 or child_table_name.len > 256) return error.InvalidGenerationPublication;
    const schema = @import("../schema/mod.zig");
    var before = try schema.parseValidatedTableSchema(alloc, before_json);
    defer before.deinit(alloc);
    var after = try schema.parseValidatedTableSchema(alloc, after_json);
    defer after.deinit(alloc);
    const catalog_len = std.base64.standard.Decoder.calcSizeForSlice(old_catalog_b64) catch return error.InvalidGenerationPublication;
    if (catalog_len == 0 or catalog_len > integrity_catalog.max_catalog_bytes) return error.InvalidGenerationPublication;
    const old_bytes = try alloc.alloc(u8, catalog_len);
    defer alloc.free(old_bytes);
    std.base64.standard.Decoder.decode(old_bytes, old_catalog_b64) catch return error.InvalidGenerationPublication;
    var old_catalog = try integrity_catalog.decode(alloc, old_bytes);
    defer old_catalog.deinit();
    if (old_catalog.schema_version != before.version or
        !std.mem.eql(u8, &old_catalog.incarnation, &(try integrity_catalog.incarnationFromTableId(child_table_id)))) return error.InvalidGenerationPublication;
    var next_catalog = try compileCatalog(alloc, after, child_table_id, old_bytes);
    defer next_catalog.deinit();
    const before_fks = try before.relationalForeignKeyDefinitions(alloc);
    defer if (before_fks.len > 0) alloc.free(before_fks);
    const after_fks = try after.relationalForeignKeyDefinitions(alloc);
    defer if (after_fks.len > 0) alloc.free(after_fks);
    var result: std.ArrayList(DerivedTransition) = .empty;
    errdefer {
        for (result.items) |entry| {
            alloc.free(entry.parent_table_name);
            alloc.free(entry.transition.child_table_name);
            alloc.free(entry.transition.constraint_name);
        }
        result.deinit(alloc);
    }
    for (before_fks) |fk| {
        const old = old_catalog.find(.foreign_key, fk.name) orelse return error.InvalidGenerationPublication;
        const new = next_catalog.catalog.find(.foreign_key, fk.name);
        const new_parent = foreignParent(after_fks, fk.name);
        const same_parent = if (new_parent) |name| std.mem.eql(u8, fk.parent_table, name) else false;
        if (same_parent and new != null and std.mem.eql(u8, &old.generation, &new.?.generation)) continue;
        try appendDerived(&result, alloc, fk.parent_table, child_table_id, child_table_name, fk.name, old.generation, if (same_parent and new != null) new.?.generation else null);
    }
    for (after_fks) |fk| {
        const old_parent = foreignParent(before_fks, fk.name);
        if (old_parent != null and std.mem.eql(u8, old_parent.?, fk.parent_table)) continue;
        const new = next_catalog.catalog.find(.foreign_key, fk.name) orelse return error.InvalidGenerationPublication;
        try appendDerived(&result, alloc, fk.parent_table, child_table_id, child_table_name, fk.name, null, new.generation);
    }
    std.mem.sort(DerivedTransition, result.items, {}, struct {
        fn less(_: void, lhs: DerivedTransition, rhs: DerivedTransition) bool {
            const parent_order = std.mem.order(u8, lhs.parent_table_name, rhs.parent_table_name);
            return if (parent_order == .eq) std.mem.order(u8, lhs.transition.constraint_name, rhs.transition.constraint_name) == .lt else parent_order == .lt;
        }
    }.less);
    return result.toOwnedSlice(alloc);
}

pub fn deriveInitialTransitions(alloc: std.mem.Allocator, child_table_id: u64, child_table_name: []const u8, schema_json: []const u8) ![]const DerivedTransition {
    if (child_table_id == 0 or child_table_name.len == 0 or child_table_name.len > 256) return error.InvalidGenerationPublication;
    var parsed = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    if (parsed.storage_mode != .relational) return error.InvalidGenerationPublication;
    var compiled = try compileCatalog(alloc, parsed, child_table_id, null);
    defer compiled.deinit();
    const fks = try parsed.relationalForeignKeyDefinitions(alloc);
    defer if (fks.len > 0) alloc.free(fks);
    if (fks.len == 0 or fks.len > max_constraints) return error.InvalidGenerationPublication;
    var result: std.ArrayList(DerivedTransition) = .empty;
    errdefer {
        for (result.items) |entry| {
            alloc.free(entry.parent_table_name);
            alloc.free(entry.transition.child_table_name);
            alloc.free(entry.transition.constraint_name);
        }
        result.deinit(alloc);
    }
    for (fks) |fk| {
        const binding = compiled.catalog.find(.foreign_key, fk.name) orelse return error.InvalidGenerationPublication;
        try appendDerived(&result, alloc, fk.parent_table, child_table_id, child_table_name, fk.name, null, binding.generation);
    }
    std.mem.sort(DerivedTransition, result.items, {}, struct {
        fn less(_: void, lhs: DerivedTransition, rhs: DerivedTransition) bool {
            const parent_order = std.mem.order(u8, lhs.parent_table_name, rhs.parent_table_name);
            return if (parent_order == .eq) std.mem.order(u8, lhs.transition.constraint_name, rhs.transition.constraint_name) == .lt else parent_order == .lt;
        }
    }.less);
    return result.toOwnedSlice(alloc);
}

fn validateOwnerFences(id: Id, table: records.TableRecord, ranges: []const records.RangeRecord, fences: []const topology.Fence, role: topology.Role) !void {
    for (ranges, fences) |range, fence| {
        if (range.table_id != table.table_id or fence.role != role or
            fence.owner_group_id != range.group_id or fence.peer_group_id != range.group_id or
            fence.namespace.table_id != table.table_id or
            fence.namespace.shard_id != table_manager.rangeDocIdentityShardId(range) or
            fence.namespace.range_id != table_manager.rangeDocIdentityRangeId(range) or
            fence.transition_id != std.mem.readInt(u64, id[0..8], .little) or
            fence.attempt != std.mem.readInt(u64, id[8..16], .little)) return error.InvalidGenerationPublication;
        _ = try fence.encode();
    }
}

pub const Phase = enum { fencing_child, staging_parents, activating_parents, acknowledging_parents, publishing_child, installing_child, published, canceling, canceled };
pub const Receipt = struct { group_id: u64, digest: Digest };

pub const Command = struct {
    plan_id: Id,
    child_table_id: u64,
    expected_revision: u64,
    action: enum { begin, child_fenced, parent_staged, parent_activated, parent_acknowledged, publish_child, child_installed, cancel, child_canceled, parent_canceled },
    plan: ?Plan = null,
    source_receipt: ?@import("../api/relational_fk_generation_publication.zig").SourceReceipt = null,
    parent_receipt: ?@import("../api/relational_fk_generation_publication.zig").Receipt = null,

    pub fn validateShape(self: Command) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or self.child_table_id == 0) return error.InvalidGenerationPublication;
        switch (self.action) {
            .begin => if (self.expected_revision != 0 or self.plan == null or self.source_receipt != null or self.parent_receipt != null or
                !std.mem.eql(u8, &self.plan_id, &self.plan.?.id) or self.child_table_id != self.plan.?.child_before.table_id) return error.InvalidGenerationPublication,
            .child_fenced, .child_installed, .child_canceled => if (self.expected_revision == 0 or self.plan != null or self.source_receipt == null or self.parent_receipt != null) return error.InvalidGenerationPublication,
            .parent_staged, .parent_activated, .parent_acknowledged, .parent_canceled => if (self.expected_revision == 0 or self.plan != null or self.source_receipt != null or self.parent_receipt == null) return error.InvalidGenerationPublication,
            .publish_child, .cancel => if (self.expected_revision == 0 or self.plan != null or self.source_receipt != null or self.parent_receipt != null) return error.InvalidGenerationPublication,
        }
    }
};

pub const Publication = struct {
    plan: Plan,
    plan_digest: Digest,
    child_identity: Plan.ChildIdentity,
    revision: u64,
    phase: Phase,
    child_fenced: []const Receipt = &.{},
    parent_staged: []const Receipt = &.{},
    parent_activated: []const Receipt = &.{},
    parent_acknowledged: []const Receipt = &.{},
    child_installed: []const Receipt = &.{},
    child_canceled: []const Receipt = &.{},
    parent_canceled: []const Receipt = &.{},

    pub fn validate(self: Publication, alloc: std.mem.Allocator) !void {
        if (self.revision == 0 or !std.mem.eql(u8, &self.plan_digest, &try self.plan.digest(alloc))) return error.InvalidGenerationPublication;
        try self.validateState(alloc);
    }

    /// ACK application never reparses or recompiles the immutable plan. The
    /// metadata transaction loads it from the durable publication record and
    /// compares its stored digest/revision before calling this bounded check.
    pub fn validateState(self: Publication, alloc: std.mem.Allocator) !void {
        if (self.revision == 0 or std.mem.allEqual(u8, &self.plan_digest, 0)) return error.InvalidGenerationPublication;
        const child_groups = try alloc.dupe(records.RangeRecord, self.plan.child_ranges);
        defer alloc.free(child_groups);
        std.mem.sort(records.RangeRecord, child_groups, {}, struct {
            fn less(_: void, a: records.RangeRecord, b: records.RangeRecord) bool {
                return a.group_id < b.group_id;
            }
        }.less);
        try validateReceipts(child_groups, self.child_fenced);
        try validateReceipts(child_groups, self.child_installed);
        try validateReceipts(child_groups, self.child_canceled);
        var parent_groups: std.ArrayList(records.RangeRecord) = .empty;
        defer parent_groups.deinit(alloc);
        for (self.plan.parents) |parent| try parent_groups.appendSlice(alloc, parent.ranges);
        std.mem.sort(records.RangeRecord, parent_groups.items, {}, struct {
            fn less(_: void, a: records.RangeRecord, b: records.RangeRecord) bool {
                return a.group_id < b.group_id;
            }
        }.less);
        for (parent_groups.items[1..], 1..) |range, i| if (parent_groups.items[i - 1].group_id == range.group_id) return error.InvalidGenerationPublication;
        try validateReceipts(parent_groups.items, self.parent_staged);
        try validateReceipts(parent_groups.items, self.parent_activated);
        try validateReceipts(parent_groups.items, self.parent_acknowledged);
        try validateReceipts(parent_groups.items, self.parent_canceled);
        if (self.phase != .canceling and self.phase != .canceled and (self.child_canceled.len != 0 or self.parent_canceled.len != 0)) return error.InvalidGenerationPublication;
        switch (self.phase) {
            .fencing_child => if (self.parent_staged.len != 0 or self.parent_activated.len != 0 or self.parent_acknowledged.len != 0 or self.child_installed.len != 0) return error.InvalidGenerationPublication,
            .staging_parents => if (self.child_fenced.len != self.plan.child_ranges.len or self.parent_activated.len != 0 or self.parent_acknowledged.len != 0 or self.child_installed.len != 0) return error.InvalidGenerationPublication,
            .activating_parents => if (self.child_fenced.len != self.plan.child_ranges.len or self.parent_staged.len != parent_groups.items.len or self.parent_acknowledged.len != 0 or self.child_installed.len != 0) return error.InvalidGenerationPublication,
            .acknowledging_parents => if (self.parent_activated.len != parent_groups.items.len or self.child_installed.len != 0) return error.InvalidGenerationPublication,
            .publishing_child, .installing_child => if (self.child_fenced.len != self.plan.child_ranges.len or self.parent_acknowledged.len != parent_groups.items.len) return error.InvalidGenerationPublication,
            .published => if (self.child_installed.len != self.plan.child_ranges.len) return error.InvalidGenerationPublication,
            .canceling, .canceled => if (self.parent_activated.len != 0 or self.parent_acknowledged.len != 0 or self.child_installed.len != 0) return error.InvalidGenerationPublication,
        }
        if (self.phase == .canceled and (self.child_canceled.len != self.plan.child_ranges.len or self.parent_canceled.len != parent_groups.items.len)) return error.InvalidGenerationPublication;
    }

    pub fn apply(self: Publication, alloc: std.mem.Allocator, command: Command) !Publication {
        try command.validateShape();
        if (command.action == .begin or command.child_table_id != self.plan.child_before.table_id or
            !std.mem.eql(u8, &command.plan_id, &self.plan.id) or
            command.expected_revision != self.revision) return error.GenerationPublicationChanged;
        var next = self;
        next.revision = std.math.add(u64, self.revision, 1) catch return error.GenerationPublicationChanged;
        switch (command.action) {
            .begin => unreachable,
            .child_fenced => {
                if (self.phase != .fencing_child or command.source_receipt.?.action != .fence) return error.GenerationPublicationChanged;
                try self.plan.verifySourceReceipt(self.child_identity, self.plan_digest, command.source_receipt.?);
                next.child_fenced = try appendReceipt(alloc, self.child_fenced, .{ .group_id = command.source_receipt.?.child_group_id, .digest = command.source_receipt.?.digest() });
                if (next.child_fenced.len == self.plan.child_ranges.len) next.phase = .staging_parents;
            },
            .parent_staged => {
                if (self.phase != .staging_parents or command.parent_receipt.?.action != .stage) return error.GenerationPublicationChanged;
                try self.plan.verifyParentReceipt(alloc, self.plan_digest, command.parent_receipt.?);
                next.parent_staged = try appendReceipt(alloc, self.parent_staged, .{ .group_id = command.parent_receipt.?.parent_group_id, .digest = command.parent_receipt.?.digest() });
                var count: usize = 0;
                for (self.plan.parents) |parent| count += parent.ranges.len;
                if (next.parent_staged.len == count) next.phase = .activating_parents;
            },
            .parent_activated => {
                if (self.phase != .activating_parents or command.parent_receipt.?.action != .activate) return error.GenerationPublicationChanged;
                try self.plan.verifyParentReceipt(alloc, self.plan_digest, command.parent_receipt.?);
                next.parent_activated = try appendReceipt(alloc, self.parent_activated, .{ .group_id = command.parent_receipt.?.parent_group_id, .digest = command.parent_receipt.?.digest() });
                var count: usize = 0;
                for (self.plan.parents) |parent| count += parent.ranges.len;
                if (next.parent_activated.len == count) next.phase = .acknowledging_parents;
            },
            .parent_acknowledged => {
                if (self.phase != .acknowledging_parents or command.parent_receipt.?.action != .acknowledge) return error.GenerationPublicationChanged;
                try self.plan.verifyParentReceipt(alloc, self.plan_digest, command.parent_receipt.?);
                next.parent_acknowledged = try appendReceipt(alloc, self.parent_acknowledged, .{ .group_id = command.parent_receipt.?.parent_group_id, .digest = command.parent_receipt.?.digest() });
                var count: usize = 0;
                for (self.plan.parents) |parent| count += parent.ranges.len;
                if (next.parent_acknowledged.len == count) next.phase = .publishing_child;
            },
            .publish_child => {
                if (self.phase != .publishing_child) return error.GenerationPublicationChanged;
                next.phase = .installing_child;
            },
            .child_installed => {
                if (self.phase != .installing_child or command.source_receipt.?.action != .install) return error.GenerationPublicationChanged;
                try self.plan.verifySourceReceipt(self.child_identity, self.plan_digest, command.source_receipt.?);
                next.child_installed = try appendReceipt(alloc, self.child_installed, .{ .group_id = command.source_receipt.?.child_group_id, .digest = command.source_receipt.?.digest() });
                if (next.child_installed.len == self.plan.child_ranges.len) next.phase = .published;
            },
            .cancel => {
                if ((self.phase != .fencing_child and self.phase != .staging_parents and self.phase != .activating_parents) or
                    self.parent_activated.len != 0) return error.GenerationPublicationChanged;
                next.phase = .canceling;
            },
            .child_canceled => {
                if (self.phase != .canceling or command.source_receipt.?.action != .cancel) return error.GenerationPublicationChanged;
                try self.plan.verifySourceReceipt(self.child_identity, self.plan_digest, command.source_receipt.?);
                next.child_canceled = try appendReceipt(alloc, self.child_canceled, .{ .group_id = command.source_receipt.?.child_group_id, .digest = command.source_receipt.?.digest() });
            },
            .parent_canceled => {
                if (self.phase != .canceling or command.parent_receipt.?.action != .cancel) return error.GenerationPublicationChanged;
                try self.plan.verifyParentReceipt(alloc, self.plan_digest, command.parent_receipt.?);
                next.parent_canceled = try appendReceipt(alloc, self.parent_canceled, .{ .group_id = command.parent_receipt.?.parent_group_id, .digest = command.parent_receipt.?.digest() });
            },
        }
        if (next.phase == .canceling and next.child_canceled.len == self.plan.child_ranges.len) {
            var parent_count: usize = 0;
            for (self.plan.parents) |parent| parent_count += parent.ranges.len;
            if (next.parent_canceled.len == parent_count) next.phase = .canceled;
        }
        try next.validateState(alloc);
        return next;
    }
};

fn validateReceipts(ranges: []const records.RangeRecord, receipts: []const Receipt) !void {
    if (receipts.len > ranges.len) return error.InvalidGenerationPublication;
    for (receipts, 0..) |receipt, index| {
        if (receipt.group_id == 0 or std.mem.allEqual(u8, &receipt.digest, 0)) return error.InvalidGenerationPublication;
        if (index != 0 and receipts[index - 1].group_id >= receipt.group_id) return error.InvalidGenerationPublication;
        var lo: usize = 0;
        var hi = ranges.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (ranges[mid].group_id < receipt.group_id) lo = mid + 1 else hi = mid;
        }
        if (lo == ranges.len or ranges[lo].group_id != receipt.group_id) return error.InvalidGenerationPublication;
    }
}

fn appendReceipt(alloc: std.mem.Allocator, receipts: []const Receipt, incoming: Receipt) ![]const Receipt {
    for (receipts) |receipt| if (receipt.group_id == incoming.group_id) return error.GenerationPublicationChanged;
    const result = try alloc.alloc(Receipt, receipts.len + 1);
    var inserted = false;
    var out: usize = 0;
    for (receipts) |receipt| {
        if (!inserted and incoming.group_id < receipt.group_id) {
            result[out] = incoming;
            out += 1;
            inserted = true;
        }
        result[out] = receipt;
        out += 1;
    }
    if (!inserted) result[out] = incoming;
    return result;
}

pub const DecisionRequest = struct {
    plan_id: Id,
    parent_table_id: u64,
    parent_group_id: u64,
    child_table_id: u64,
    child_table_name: []const u8,
    action: @import("../api/relational_fk_generation_publication.zig").Action,
};

pub const SourceDecisionRequest = struct {
    plan_id: Id,
    child_table_id: u64,
    child_table_name: []const u8,
    child_group_id: u64,
    action: @import("../api/relational_fk_generation_publication.zig").SourceAction,
};

pub const Decision = struct {
    phase: Phase,
    metadata_group_id: u64,
    metadata_incarnation: @import("incarnation.zig").MetadataClusterIncarnation,
    metadata_epoch: u64,
    plan_digest: Digest,
    fence: topology.Fence,
    transitions: []const Transition,
    parent_table: records.TableRecord,
    parent_range: records.RangeRecord,
};

pub const SourceDecision = struct {
    phase: Phase,
    metadata_group_id: u64,
    metadata_incarnation: @import("incarnation.zig").MetadataClusterIncarnation,
    metadata_epoch: u64,
    plan_digest: Digest,
    fence: topology.Fence,
    child_before: records.TableRecord,
    child_after: records.TableRecord,
    child_catalog_before_b64: []const u8,
    child_range: records.RangeRecord,
};

test "FK generation publication rejects an unbound or empty plan" {
    try std.testing.expect(try schemaHasForeignKeys(std.testing.allocator, "{\"foreign_keys\":[{}]}"));
    try std.testing.expect(try schemaHasForeignKeys(std.testing.allocator, "{\"foreign_\\u006beys\":[{}]}"));
    try std.testing.expect(!try schemaHasForeignKeys(std.testing.allocator, "{\"foreign_keys\":[]}"));
    const plan: Plan = .{
        .id = @splat(0),
        .child_before = .{ .table_id = 1, .name = "child" },
        .child_after = .{ .table_id = 1, .name = "child" },
        .child_catalog_before_b64 = "",
        .child_ranges = &.{},
        .child_fences = &.{},
        .parents = &.{},
    };
    try std.testing.expectError(error.InvalidGenerationPublication, plan.validate(std.testing.allocator));
}

test "FK generation publication derives history-bound replacement and reparenting" {
    const alloc = std.testing.allocator;
    const before_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"fk","child_columns":["id"],"parent_table":"parent_a","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const changed_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"fk","child_columns":["id"],"parent_table":"parent_a","parent_columns":["id"],"on_delete":"cascade"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const reparent_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"fk","child_columns":["id"],"parent_table":"parent_b","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    var before = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, before_json);
    defer before.deinit(alloc);
    var compiled = try compileCatalog(alloc, before, 101, null);
    defer compiled.deinit();
    const old_b64 = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(compiled.value.len));
    defer alloc.free(old_b64);
    _ = std.base64.standard.Encoder.encode(old_b64, compiled.value);
    const changed = try deriveTransitions(alloc, 101, "child", before_json, changed_json, old_b64);
    defer freeDerivedTransitions(alloc, changed);
    try std.testing.expectEqual(@as(usize, 1), changed.len);
    try std.testing.expectEqualStrings("parent_a", changed[0].parent_table_name);
    try std.testing.expect(changed[0].transition.expected_generation != null);
    try std.testing.expect(changed[0].transition.next_generation != null);
    try std.testing.expect(!generationEqual(changed[0].transition.expected_generation, changed[0].transition.next_generation));
    const reparented = try deriveTransitions(alloc, 101, "child", before_json, reparent_json, old_b64);
    defer freeDerivedTransitions(alloc, reparented);
    try std.testing.expectEqual(@as(usize, 2), reparented.len);
    try std.testing.expectEqualStrings("parent_a", reparented[0].parent_table_name);
    try std.testing.expectEqualStrings("parent_b", reparented[1].parent_table_name);
    try std.testing.expect(reparented[0].transition.expected_generation != null and reparented[0].transition.next_generation == null);
    try std.testing.expect(reparented[1].transition.expected_generation == null and reparented[1].transition.next_generation != null);

    var id: Id = @splat(0);
    std.mem.writeInt(u64, id[0..8], 7, .little);
    std.mem.writeInt(u64, id[8..16], 1, .little);
    const child_range: records.RangeRecord = .{ .table_id = 101, .group_id = 301, .range_id = 301, .doc_identity_shard_id = 301, .doc_identity_range_id = 301, .start_key = "" };
    const parent_range: records.RangeRecord = .{ .table_id = 202, .group_id = 401, .range_id = 401, .doc_identity_shard_id = 401, .doc_identity_range_id = 401, .start_key = "" };
    const child_fence: topology.Fence = .{ .transition_id = 7, .attempt = 1, .peer_group_id = 301, .owner_group_id = 301, .role = .child_generation_source, .namespace = .{ .table_id = 101, .shard_id = 301, .range_id = 301 }, .catalog_digest = @splat(1) };
    const parent_fence: topology.Fence = .{ .transition_id = 7, .attempt = 1, .peer_group_id = 401, .owner_group_id = 401, .role = .child_generation_parent, .namespace = .{ .table_id = 202, .shard_id = 401, .range_id = 401 }, .catalog_digest = @splat(2) };
    const plan: Plan = .{
        .id = id,
        .child_before = .{ .table_id = 101, .name = "child", .schema_json = before_json },
        .child_after = .{ .table_id = 101, .name = "child", .schema_json = changed_json },
        .child_catalog_before_b64 = old_b64,
        .child_ranges = &.{child_range},
        .child_fences = &.{child_fence},
        .parents = &.{.{ .table = .{ .table_id = 202, .name = "parent_a" }, .ranges = &.{parent_range}, .fences = &.{parent_fence}, .transitions = &.{changed[0].transition} }},
    };
    const digest = try plan.digest(alloc);
    const publication: Publication = .{ .plan = plan, .plan_digest = digest, .child_identity = try plan.childIdentity(alloc), .revision = 1, .phase = .fencing_child };
    try publication.validate(alloc);
    try std.testing.expectError(error.GenerationPublicationChanged, publication.apply(alloc, .{ .plan_id = id, .child_table_id = 101, .expected_revision = 2, .action = .cancel }));
    var forged = plan;
    var wrong = changed[0].transition;
    wrong.next_generation = @splat(9);
    forged.parents = &.{.{ .table = .{ .table_id = 202, .name = "parent_a" }, .ranges = &.{parent_range}, .fences = &.{parent_fence}, .transitions = &.{wrong} }};
    try std.testing.expectError(error.InvalidGenerationPublication, forged.validate(alloc));
}
