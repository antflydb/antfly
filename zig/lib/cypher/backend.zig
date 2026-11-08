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
pub const FanoutAllocator = @import("fanout.zig").Allocator;
pub const CursorTrace = @import("cursor_trace.zig").Trace;
pub fn Page(comptime T: type) type {
    return struct {
        items: []const T,
        /// Optional exact continuation after each emitted item. An empty slice
        /// on a zero-row capability probe certifies batched resumability.
        cursors: ?[]const ?[]const u8 = null,
        cursor_trace: ?*const CursorTrace = null,
        next: ?[]const u8 = null,
        examined: ?usize = null,
        unfiltered: bool = false,
        seek_window: ?SeekWindow = null,
        /// Ordered keys are monotone. False requires boundary-tie refinement
        /// before a row goal may stop consumption.
        stable_ties: bool = true,
        /// Complete ordered keys are NULL/integer/string/boolean, with identical
        /// representations for Cypher-equal tuples across this table's owners.
        /// This permits observing a projected key without primary tie repair.
        exact_keys: bool = false,
        /// Primary encounter provenance for refinement across routed owners.
        encounter_owners: ?[]const usize = null,
    };
}
/// Owner-selected access path. The first request carries the complete predicate;
/// later requests may send only this certified domain suffix (at most 128 items).
/// Null filter means the chosen path does not partition an IN domain: its IN
/// hints may be omitted, while the runtime retains every authoritative predicate.
pub const SeekWindow = struct { filter: ?usize, offset: usize = 0, primary: bool = false, prefix_filters: []const usize = &.{} };
/// A bounded indexed probe never falls back to a primary scan. Null means the
/// capability is unsupported. Incomplete probes are costing evidence, never query
/// bindings: their record/seek work must remain charged through Work, including
/// quota declines. Only complete, admitted pages consume logical anchors.
pub const Probe = struct { page: Page(*const v.Node), complete: bool };
/// Exact scalar COUNT over a licensed single-node match. Owners evaluate the
/// original Cypher predicate after policy projection, never admission hints.
/// A reduction request scans once for at most 64 total scalar arguments.
/// An empty argument vector is legacy COUNT(*); a NULL argument denotes '*'.
pub const max_count_arguments = 64;
pub const CountInput = struct { arguments: []const ?*const ast.Expr = &.{}, node: ast.Node, predicate: ?*const ast.Expr = null, parameters: []const v.Entry = &.{}, source: []const u8 = "", max_examined: usize = 1_000_000, max_work: usize = 1_000_000, memory_bytes: usize = 64 << 20 };
/// Incomplete attempts carry physical progress without asserting a count.
/// Null is reserved for unsupported operations which performed no reads.
pub const CountResult = struct {
    /// Matching rows, independently of nullable aggregate arguments.
    count: usize,
    /// Non-NULL counts in request order, owned by the reply allocator.
    counts: []const usize = &.{},
    examined: usize,
    complete: bool = true,
    pub fn validate(self: @This(), maximum: usize) !void {
        if (self.examined > maximum or self.count > self.examined or (!self.complete and self.count != 0)) return error.CypherInvalidBackendPage;
        if (self.counts.len > max_count_arguments) return error.CypherInvalidBackendPage;
        for (self.counts) |value| if (value > self.count) return error.CypherInvalidBackendPage;
    }
    pub fn validateFor(self: @This(), maximum: usize, arguments: usize) !void {
        try self.validate(maximum);
        if (arguments > max_count_arguments or (self.complete and self.counts.len != arguments) or (!self.complete and self.counts.len != 0 and self.counts.len != arguments)) return error.CypherInvalidBackendPage;
    }
};
/// First adjacency pages for a bounded frontier. NULL slots decline cache
/// admission without inventing EOF. Examined includes discarded page reads.
pub const ExpansionBatch = struct { pages: []const ?Page(*const v.Relationship), examined: usize };
/// Conservative physical endpoint envelope from immutable graph roots. Bounds
/// are inclusive. NULL means unknown, empty certifies no physical endpoints.
/// Stale records and producer keys may widen the envelope, never narrow it.
pub const EndpointRange = struct {
    empty: bool = false,
    min: []const u8 = "",
    max: []const u8 = "",
    pub fn contains(self: @This(), key: []const u8) bool {
        return !self.empty and std.mem.order(u8, key, self.min) != .lt and std.mem.order(u8, key, self.max) != .gt;
    }
};
pub const AdjacencyRoute = struct { index: []const u8, outgoing: ?EndpointRange = null, incoming: ?EndpointRange = null };
pub fn validateAdjacencyRoutes(routes: []const AdjacencyRoute) !void {
    if (routes.len > 1024) return error.CypherInvalidBackendPage;
    for (routes, 0..) |route, i| {
        if (route.index.len == 0 or route.index.len > 1024) return error.CypherInvalidBackendPage;
        for (routes[0..i]) |prior| if (std.mem.eql(u8, prior.index, route.index)) return error.CypherInvalidBackendPage;
        for ([_]?EndpointRange{ route.outgoing, route.incoming }) |maybe| if (maybe) |range| {
            if (range.min.len > 8192 or range.max.len > 8192 or (!range.empty and std.mem.order(u8, range.min, range.max) == .gt)) return error.CypherInvalidBackendPage;
        };
    }
}

pub const FilterOp = enum { eq, lt, lte, gt, gte, in, is_null };
/// Private compiler proof: a distinct, homogeneous, non-null primitive set.
/// Inexact integers cannot be lowered into rounded DOUBLE equality domains.
pub const ScalarDomain = enum { string, boolean, integer, integer_inexact_double };
/// Private transport proof over the complete certified domain. A planning
/// request carries first/middle/last samples and emits no partitioned rows.
/// max_string_bytes licenses a worst-case escaped bound before first emission.
pub const DomainPlan = struct { length: usize, max_string_bytes: usize = 0 };
pub const PropertyFilter = struct { property: []const u8, value: v.Value, op: FilterOp = .eq, domain: ?ScalarDomain = null, plan: ?DomainPlan = null };
pub const PropertyOrder = struct {
    property: []const u8,
    descending: bool = false,
    /// A consumer-confirmed complete key group can be excluded on refill.
    /// This is an index tuple bound, including NULL, not a Cypher comparison.
    /// All keys must carry their component of one complete tuple boundary.
    after: ?v.Value = null,
    exclude_null: bool = false,
};
pub const Seed = struct { node: *const v.Node, score: f64 };
pub const SearchUsage = struct { examined: ?usize = null };
pub const SearchInput = struct { usage: ?*SearchUsage = null, parameters: v.Value, prepared_json: []const u8 = "", max_examined: usize = 1_000_000 };
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
        // Each callback creates a fresh metadata cache. Its entries describe
        // borrowed owner values and must not accumulate in a caller arena.
        var scratch = std.heap.ArenaAllocator.init(reclaimingAllocator(self.a));
        defer scratch.deinit();
        var evaluator: @import("value_work.zig").Work = .{ .a = scratch.allocator(), .ptr = self, .charge = charge };
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
        /// Certifies every mapped index in this retained backend stores edges
        /// under their logical source key. Document ranges alone are no proof:
        /// producer-owned edges may reside in another owner's immutable root.
        source_owned_adjacency: bool = false,
        adjacency_routes: ?*const fn (*anyopaque, std.mem.Allocator, []const u8) anyerror![]const AdjacencyRoute = null,
        /// Row transports benefit from partial counts. A direct local adapter
        /// can expose the owner operation while retaining its existing evaluator.
        prefer_count_nodes: bool = true,
        count_nodes: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, CountInput) anyerror!?CountResult = null,
        expand_many: ?*const fn (*anyopaque, std.mem.Allocator, catalog.Relationship, []const v.Identity, ast.Direction, usize, usize) anyerror!?ExpansionBatch = null,
        /// Borrowed compiler proof, reset after each execution/pull. Capable
        /// adapters project only after complete source authorization.
        set_read_demand: ?*const fn (*anyopaque, ?@import("read_fields.zig").Demand) void = null,
        /// Legacy field union. Null retains complete entities; an empty set
        /// requests identities only. Authorization always sees the full row.
        set_read_fields: ?*const fn (*anyopaque, ?[]const []const u8) void = null,
        bind: *const fn (*anyopaque, std.mem.Allocator, catalog.Graph) anyerror!void,
        checkpoint: *const fn (*anyopaque) anyerror!void,
        scan: *const fn (*anyopaque, std.mem.Allocator, []const u8, ?[]const u8, usize) anyerror!Page(*const v.Node),
        /// Typed comparisons are candidate restrictions; the runtime always
        /// rechecks Cypher semantics after authorization and projection.
        scan_where: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const PropertyFilter, ?[]const u8, usize) anyerror!Page(*const v.Node) = null,
        planned_domains: bool = false,
        probe_where: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const PropertyFilter, usize) anyerror!?Probe = null,
        /// A non-null page certifies monotone Cypher order keys. stable_ties
        /// additionally certifies primary encounter order for equal keys.
        /// Consumers must refine ties or decline metadata-only coarse proofs.
        /// Empty order requests primary encounter order under the filters;
        /// indexes can attest when every physical key is equality-fixed.
        /// Examined includes hidden/rejected/speculative records across owners.
        /// limit=0 probes the pinned ready index without reading documents:
        /// certify with empty items, examined=0 and a resumable proof cursor.
        scan_ordered: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const PropertyFilter, []const PropertyOrder, ?[]const u8, usize, usize) anyerror!?Page(*const v.Node) = null,
        /// Optional snapshot-bound candidate cardinality upper bound, capped
        /// by limit. Index evidence or maintained whole-population summaries
        /// serve advisory costing only. Unknown populations return null;
        /// planning consumes work quota without binding query anchors.
        estimate_where: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const PropertyFilter, usize) anyerror!?usize = null,
        /// Maintained physical population at the retained cut. No row sampling;
        /// TTL, membership and policies can only reduce this advisory bound.
        population_upper_bound: ?*const fn (*anyopaque, std.mem.Allocator, []const u8) anyerror!?u64 = null,
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

test "owner value accounting reclaims metadata outside a retaining caller arena" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
    };
    var memory: @import("budget.zig") = .{ .backing = std.testing.allocator, .limit = 16 << 10 };
    var caller = std.heap.ArenaAllocator.init(memory.allocator());
    defer caller.deinit();
    var token: u8 = 0;
    const owner: Backend = .{ .ptr = &token, .vtable = &.{ .bind = undefined, .checkpoint = Fixture.check, .scan = undefined, .get = undefined, .expand = undefined } };
    var meter: Meter = .{ .a = caller.allocator(), .backend = owner };
    const payload: [4096]u8 = @splat('x');
    const node: v.Node = .{ .identity = .{ .table = "docs", .key = "n0" }, .properties = &.{.{ .name = "payload", .value = .{ .string = &payload } }} };
    for (0..256) |_| try meter.work().value(meter.work().ptr, .{ .node = &node });
    try std.testing.expect(meter.units > 256);
    try std.testing.expectEqual(@as(usize, 0), memory.live);
    try std.testing.expect(memory.peak < memory.limit);
}
