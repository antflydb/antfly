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

const std = @import("std");
const catalog = @import("catalog.zig");
const v = @import("value.zig");
const ast = @import("ast.zig");
/// Temporary owner decoding shares quota/failure wrappers but can release its
/// pages even when the execution allocator is a retaining arena.
pub fn reclaimingAllocator(input: std.mem.Allocator) std.mem.Allocator {
    var probe = std.heap.ArenaAllocator.init(input);
    var current = input;
    for (0..16) |_| {
        if (current.vtable != probe.allocator().vtable) break;
        const arena: *std.heap.ArenaAllocator = @ptrCast(@alignCast(current.ptr));
        const next = arena.child_allocator;
        if (next.ptr == current.ptr and next.vtable == current.vtable) break;
        current = next;
    }
    return current;
}
/// `unfiltered` certifies that every page in this scan enumerates the complete
/// authorized table, without restricting it to the supplied property filters.
/// An index/predicate-restricted scan must leave it false. Consumers still apply
/// all label/predicate checks; the certificate only enables a reusable lookup.
pub fn Page(comptime T: type) type {
    return struct {
        items: []const T,
        /// Optional exact continuation after each emitted item. An empty slice
        /// on a zero-row capability probe certifies batched resumability.
        cursors: ?[]const ?[]const u8 = null,
        next: ?[]const u8 = null,
        examined: ?usize = null,
        unfiltered: bool = false,
        /// Ordered keys are monotone. False requires boundary-tie refinement
        /// before a row goal may stop consumption.
        stable_ties: bool = true,
        /// Primary encounter provenance for refinement across routed owners.
        encounter_owners: ?[]const usize = null,
    };
}
pub const FilterOp = enum { eq, lt, lte, gt, gte, in };
pub const PropertyFilter = struct { property: []const u8, value: v.Value, op: FilterOp = .eq };
pub const PropertyOrder = struct {
    property: []const u8,
    descending: bool = false,
    /// A consumer-confirmed complete key group can be excluded on refill.
    /// This is an index tuple bound, including NULL, not a Cypher comparison.
    /// Only a single varying key may currently carry this hint.
    after: ?v.Value = null,
    exclude_null: bool = false,
};
pub const Seed = struct { node: *const v.Node, score: f64 };
pub const SearchInput = struct { parameters: v.Value, prepared_json: []const u8 = "", max_examined: usize = 1_000_000 };
pub const SearchPage = struct { seeds: []const Seed, examined: usize = 0, approximate: bool = false };
pub const StartupWork = struct { scanned_anchors: usize = 0, explored_edges: usize = 0, intermediate_states: usize = 0 };
/// Statement-owned execution accounting. Adapters may retain this capability
/// only until runtime.execute returns; durable commit must not invoke it after
/// releasing the read authority or publishing a decision.
pub const Work = struct {
    ptr: *anyopaque,
    charge: *const fn (*anyopaque, usize) anyerror!void,
    value: *const fn (*anyopaque, v.Value) anyerror!void,
    remaining: ?*const fn (*anyopaque) usize = null,
};
/// A bounded owner-call meter. Routed replies report these units to the
/// coordinator so work spent behind an RPC is part of the statement ceiling.
pub const Meter = struct {
    a: std.mem.Allocator,
    backend: Backend,
    limit: usize = 1_000_000,
    units: usize = 0,
    pub fn work(self: *Meter) Work {
        return .{ .ptr = self, .charge = charge, .value = value, .remaining = remaining };
    }
    fn charge(ptr: *anyopaque, amount: usize) !void {
        const self: *Meter = @ptrCast(@alignCast(ptr));
        // Owner checkpoints can validate index/schema authority. Amortize that
        // cost over at most 64 work units, while checking large reservations
        // before the corresponding traversal begins.
        if (self.units == 0 or amount >= 64 or self.units / 64 != (self.units +| amount) / 64)
            try self.backend.vtable.checkpoint(self.backend.ptr);
        if (amount > self.limit -| self.units) return error.CypherStateBudgetExceeded;
        self.units += amount;
    }
    fn value(ptr: *anyopaque, input: v.Value) !void {
        const self: *Meter = @ptrCast(@alignCast(ptr));
        var evaluator: @import("value_work.zig").Work = .{ .a = self.a, .ptr = self, .charge = charge };
        try evaluator.output(input);
    }
    fn remaining(ptr: *anyopaque) usize {
        const self: *Meter = @ptrCast(@alignCast(ptr));
        return self.limit -| self.units;
    }
};
pub const Mutation = union(enum) {
    create_node: *const v.Node,
    create_relationship: *const v.Relationship,
    /// Null-valued properties remove the property, as in Cypher SET.
    set_properties: struct { target: v.EntityIdentity, properties: []const v.Entry, replace: bool },
    remove_properties: struct { target: v.EntityIdentity, names: []const []const u8 },
    delete_node: struct { identity: v.Identity, detach: bool },
    delete_relationship: v.RelationshipIdentity,
    pub fn validateProperties(self: Mutation) !void {
        const entries = switch (self) {
            .create_node => |node| node.properties,
            .create_relationship => |relationship| relationship.properties,
            .set_properties => |update| update.properties,
            else => return,
        };
        try v.validatePropertyValue(.{ .map = entries });
        const node = switch (self) {
            .create_node => true,
            .set_properties => |update| update.target == .node,
            else => false,
        };
        if (node) for (entries) |entry| {
            // These are native ingestion commands, never persistent node data.
            // Interpreting SET as a side-channel graph/vector write would make
            // its overlay disagree with the durable entity and its ownership.
            if (entry.value != .null and (std.mem.eql(u8, entry.name, "_edges") or std.mem.eql(u8, entry.name, "_embeddings"))) return error.CypherInvalidPropertyValue;
        };
    }
};
/// All returned values are allocated in the supplied execution allocator and
/// remain valid through execution. Read callbacks enforce row/field policies
/// and share the statement's pinned visibility cut. A missing authorized node
/// is null; hidden nodes are not legal intermediate traversal endpoints.
pub const Backend = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    pub const VTable = struct {
        /// Borrowed until reset. Null retains complete entities, an empty set
        /// requests identities only. Authorization always sees the full row.
        set_read_fields: ?*const fn (*anyopaque, ?[]const []const u8) void = null,
        bind: *const fn (*anyopaque, std.mem.Allocator, catalog.Graph) anyerror!void,
        checkpoint: *const fn (*anyopaque) anyerror!void,
        scan: *const fn (*anyopaque, std.mem.Allocator, []const u8, ?[]const u8, usize) anyerror!Page(*const v.Node),
        /// Typed comparisons are candidate restrictions; the runtime always
        /// rechecks Cypher semantics after authorization and projection.
        scan_where: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const PropertyFilter, ?[]const u8, usize) anyerror!Page(*const v.Node) = null,
        /// A non-null page certifies monotone Cypher order keys. stable_ties
        /// additionally certifies primary encounter order for equal keys.
        /// Consumers must refine ties or decline metadata-only coarse proofs.
        /// Empty order requests primary encounter order under the filters;
        /// indexes can attest when every physical key is equality-fixed.
        /// Examined includes hidden/rejected/speculative records across owners.
        /// limit=0 probes the pinned ready index without reading documents:
        /// certify with empty items, examined=0 and a resumable proof cursor.
        scan_ordered: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const PropertyFilter, []const PropertyOrder, ?[]const u8, usize, usize) anyerror!?Page(*const v.Node) = null,
        /// Optional snapshot-bound index cardinality evidence, capped by limit.
        /// Unknown/broad/unindexed populations return null; planning consumes
        /// work quota without binding query anchors or advancing a scan cursor.
        estimate_where: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const PropertyFilter, usize) anyerror!?usize = null,
        /// Authorized document lookup also serves nonvertex producers. The runtime
        /// applies graph membership before admitting traversal endpoints.
        get: *const fn (*anyopaque, std.mem.Allocator, v.Identity) anyerror!?*const v.Node,
        /// Aligned, authorized lookups within the same retained cut. Calls are
        /// bounded to 128 identities; null entries preserve hidden/missing rows.
        get_many: ?*const fn (*anyopaque, std.mem.Allocator, []const v.Identity) anyerror![]const ?*const v.Node = null,
        /// Resolve the current statement postimage of a retained relationship
        /// alias. Mutable adapters provide this even without read projection.
        get_relationship: ?*const fn (*anyopaque, std.mem.Allocator, v.RelationshipIdentity) anyerror!?*const v.Relationship = null,
        expand: *const fn (*anyopaque, std.mem.Allocator, catalog.Relationship, v.Identity, ast.Direction, ?[]const u8, usize) anyerror!Page(*const v.Relationship),
        /// Snapshot-local indexed candidates for a fixed edge with two bound
        /// endpoints. Properties are conservative hints; residuals remain mandatory.
        expand_between: ?*const fn (*anyopaque, std.mem.Allocator, catalog.Relationship, v.Identity, v.Identity, ast.Direction, []const PropertyFilter, ?[]const u8, usize, usize) anyerror!Page(*const v.Relationship) = null,
        search: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, SearchInput) anyerror!SearchPage = null,
        normalize_node: ?*const fn (*anyopaque, std.mem.Allocator, *const v.Node, bool) anyerror!*const v.Node = null,
        /// Mutable adapters retain full postimages for commit. Reads project
        /// them separately so WITH CHECK cannot grant USING visibility.
        project_postimage: ?*const fn (*anyopaque, std.mem.Allocator, *const v.Node) anyerror!?*const v.Node = null,
        project_node: ?*const fn (*anyopaque, std.mem.Allocator, *const v.Node) anyerror!?*const v.Node = null,
        /// Rebuild source-document contributions against a staged postimage.
        document_relationships: ?*const fn (*anyopaque, std.mem.Allocator, *const v.Node) anyerror![]const *const v.Relationship = null,
        /// Mutation adapters must support read-your-writes, transactional MERGE
        /// uniqueness, and atomic rollback of every staged mutation on failure.
        set_work: ?*const fn (*anyopaque, ?Work) void = null,
        begin: ?*const fn (*anyopaque) anyerror!void = null,
        startup_work: ?*const fn (*anyopaque) StartupWork = null,
        mutate: ?*const fn (*anyopaque, std.mem.Allocator, Mutation) anyerror!void = null,
        commit: ?*const fn (*anyopaque) anyerror!void = null,
        rollback: ?*const fn (*anyopaque) void = null,
        new_identity: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, bool) anyerror!v.Identity = null,
    };
};
