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

//! Shared typed-row read execution. No SQL parser, HTTP types, or JSON row
//! parsing belongs here. One reader pins both a store snapshot and its schema;
//! every page sees the same rows even if live writes, DDL, or GC race it.
const std = @import("std");
const time = @import("antfly_platform").time;
const docstore = @import("../docstore.zig");
const schema = @import("../schema.zig");
const internal = @import("../internal_keys.zig");
const registry = @import("schema_registry.zig");
const catalog = @import("relational_index_catalog.zig");
const plans = @import("relational_index_plan.zig");
const records = @import("relational_index_records.zig");
const jobs = @import("relational_index_jobs.zig");
const tuples = @import("relational_index_keys.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const range_state = @import("range_state.zig");
const ttl = @import("../ttl.zig");
const predicates = @import("relational_predicate.zig");
const row_cursor_codec = @import("relational_row_cursor.zig");
const Allocator = std.mem.Allocator;

pub const Value = tuples.Value;
pub const Condition = predicates.Condition;
/// Bounds use index order, including descending components. A partial tuple
/// denotes the whole matching left prefix, not an implicit NULL suffix.
pub const Bound = struct { values: []const Value, inclusive: bool = true };
pub const PrimaryBound = struct { key: []const u8, inclusive: bool = true };
/// Internal authorization/legacy-filter adapter. Context is borrowed for the
/// reader's lifetime; it sees the verified full typed row before projection.
pub const RowFilter = struct {
    context: *anyopaque,
    matches: *const fn (*anyopaque, Allocator, []const u8, codec.OrdinalRowView) anyerror!bool,
};
pub const Request = struct {
    include_primary_digest: bool = false,
    /// Stateful internal readers already retain a cursor; public stateless
    /// pages opt in to owned external continuation tokens.
    include_cursor: bool = false,
    /// Null selects primary-key order. Named indexes must have a durable
    /// ready proof for this exact generation and owned range.
    index: ?[]const u8 = null,
    /// Bounded planner hint for equality prefixes, range suffixes and proven partial indexes.
    /// Callers requiring primary-key order must leave this disabled.
    auto_index: bool = false,
    after: ?[]const u8 = null,
    lower: ?Bound = null,
    upper: ?Bound = null,
    primary_lower: ?PrimaryBound = null,
    primary_upper: ?PrimaryBound = null,
    expected_schema_version: ?u32 = null,
    row_filter: ?RowFilter = null,
    /// Explicit column names. Empty means an empty projection, not SELECT *.
    fields: []const []const u8 = &.{},
    /// Conjunction of typed comparisons; SQL UNKNOWN does not match WHERE.
    conditions: []const Condition = &.{},
};

pub const Budget = struct {
    rows: usize = 128,
    records: usize = 1024,
    output_bytes: usize = 1024 * 1024,
    time_ns: u64 = 5 * std.time.ns_per_ms,

    fn validate(self: Budget) !void {
        if (self.rows == 0 or self.rows > 4096 or self.records == 0 or self.records > 65_536 or
            self.output_bytes == 0 or self.output_bytes > 16 * 1024 * 1024 or
            self.time_ns == 0 or self.time_ns > std.time.ns_per_s) return error.InvalidRelationalRowsBudget;
    }
};

pub const Row = struct {
    key: []const u8,
    json: []const u8,
    typed: ?std.json.Value = null,
    sql_nulls: ?[]const bool = null,
    json_null_fields: []const []const u8 = &.{},
    version: u64,
    schema_version: u32,
    semantic_hash: [32]u8,
    expected_content_digest: ?[32]u8 = null,
    cursor: ?[]const u8 = null,
};

pub const Page = struct {
    arena: std.heap.ArenaAllocator,
    rows: []const Row,
    more: bool,
    records_examined: usize,
    output_bytes: usize,
    /// Work counters distinguish covering scans from primary-row fallbacks.
    primary_lookups: usize = 0,
    index_only_rows: usize = 0,

    pub fn deinit(self: *Page) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const Reader = struct {
    row_policy_lease: ?@import("row_policy_gate.zig").Gate.Lease = null,
    alloc: Allocator,
    arena: std.heap.ArenaAllocator,
    read: docstore.DocStore.Txn,
    active: registry.SchemaView,
    index_plan: ?plans.View,
    /// Snapshot-local planner work, distinct from execution-page work.
    planner_candidates: usize = 0,
    planner_records: usize = 0,
    index: ?plans.BoundIndex,
    lower: []const u8,
    upper: []const u8,
    owned_lower: []const u8,
    owned_upper: []const u8,
    fields: []const []const u8,
    now_ns: u64,
    authenticated: bool,
    include_primary_digest: bool,
    include_cursor: bool,
    cursor_identity: ?[row_cursor_codec.identity_len]u8,
    row_filter: ?RowFilter,
    index_only: bool = false,
    covering_projection: ?codec.OrdinalProjectionPlan = null,
    covering_conditions: []predicates.Source = &.{},
    source: ?registry.SchemaView = null,
    selected: ?codec.OrdinalProjectionPlan = null,
    conditions: []predicates.Plan,
    /// Exact conjuncts discharged by the pinned READY partial-index proof.
    /// These are skipped only on the index-only path; primary reads still
    /// evaluate the complete query against the authoritative row.
    implied_conditions: []const bool,
    source_conditions: []predicates.Source = &.{},
    // Snapshot-local LRU: version numbers are not identities across restores.
    // Retain at most eight bindings / 2 MiB, or one oversized working binding.
    bindings: [8]?*SourceBinding = @splat(null),
    binding_bytes: usize = 0,
    source_compilations: usize = 0,
    source_cache_hits: usize = 0,
    failed_row: ?Row = null,
    after: std.ArrayList(u8) = .empty,
    done: bool = false,

    const SourceBinding = struct {
        arena: std.heap.ArenaAllocator,
        view: registry.SchemaView,
        projection: codec.OrdinalProjectionPlan,
        conditions: []predicates.Source,

        fn destroy(self: *SourceBinding, alloc: Allocator) void {
            self.view.release();
            self.arena.deinit();
            alloc.destroy(self);
        }
    };

    fn evictSourceBinding(self: *Reader, slot: usize) void {
        if (self.bindings[slot]) |binding| {
            self.binding_bytes -= binding.arena.queryCapacity();
            binding.destroy(self.alloc);
            self.bindings[slot] = null;
        }
    }

    /// Caller fences schema/catalog publication while this snapshot is opened.
    /// Owns request data except row_filter.context, which the caller retains
    /// through reader.deinit. The reader must close before its DB closes.
    pub fn open(alloc: Allocator, store: *docstore.DocStore, active_view: registry.SchemaView, indexes: ?catalog.WriteSnapshot, request: Request, now_ns: u64) !Reader {
        return openSnapshot(alloc, try store.beginReadTxn(), store.valuesAreAuthenticated(), active_view, indexes, request, now_ns);
    }

    /// Consumes the transaction on both success and error. Schema/index pins
    /// and this transaction must have been captured under one publication
    /// fence; predicate compilation and bounded planning need no apply lock.
    pub fn openSnapshot(alloc: Allocator, snapshot: docstore.DocStore.Txn, authenticated: bool, active_view: registry.SchemaView, indexes: ?catalog.WriteSnapshot, request: Request, now_ns: u64) !Reader {
        var read = snapshot;
        errdefer read.abort();
        if (active_view.storageMode() != .relational) return error.RelationalTableRequired;
        if (request.expected_schema_version) |version| if (version != active_view.version()) return error.PreparedGenerationChanged;
        if (request.fields.len > 256 or request.conditions.len > 256 or (request.index == null and (request.lower != null or request.upper != null)))
            return error.InvalidRelationalRowsRequest;
        if (request.index != null and (request.primary_lower != null or request.primary_upper != null)) return error.InvalidRelationalRowsRequest;
        if (request.after != null and request.index == null) return error.InvalidRelationalRowsRequest;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const fields = try owned.alloc([]const u8, request.fields.len);
        for (request.fields, fields, 0..) |field, *copy, i| {
            if (active_view.physicalLayout().ordinalForName(active_view.tableSchema().relational_columns, field) == null)
                return error.RelationalIndexColumnNotFound;
            for (fields[0..i]) |prior| if (std.mem.eql(u8, field, prior)) return error.InvalidRelationalRowsRequest;
            copy.* = try owned.dupe(u8, field);
        }
        const range_raw = read.get(range_state.range_key) catch |err| switch (err) {
            error.NotFound => &([_]u8{0} ** 8),
            else => return err,
        };
        const range = try range_state.decodeRangeAlloc(owned, range_raw);
        const owned_lower = try internal.documentExactPrefixAlloc(owned, range.start);
        const owned_upper: []const u8 = if (range.end.len == 0) &.{internal.user_namespace + 1} else try internal.documentExactPrefixAlloc(owned, range.end);
        var lower: []const u8 = owned_lower;
        var upper: []const u8 = owned_upper;
        var selected_index: ?plans.BoundIndex = null;
        var cursor_identity: ?[row_cursor_codec.identity_len]u8 = null;
        if (request.primary_lower) |bound| {
            const prefix = try internal.documentExactPrefixAlloc(owned, bound.key);
            const requested = if (bound.inclusive) prefix else (try internal.nextPrefixAlloc(owned, prefix)) orelse return error.InvalidRelationalRowsRequest;
            if (std.mem.order(u8, requested, lower) == .gt) lower = requested;
        }
        if (request.primary_upper) |bound| {
            const prefix = try internal.documentExactPrefixAlloc(owned, bound.key);
            const requested = if (!bound.inclusive) prefix else (try internal.nextPrefixAlloc(owned, prefix)) orelse return error.InvalidRelationalRowsRequest;
            if (std.mem.order(u8, requested, upper) == .lt) upper = requested;
        }
        if (request.index) |name| {
            const pinned = indexes orelse return error.IndexNotFound;
            if (pinned.plan.schemaView().epoch != active_view.epoch) return error.PreparedGenerationChanged;
            const raw_head = try read.get(catalog.head_key);
            if (!(try catalog.Head.decode(raw_head)).eql(pinned.head)) return error.PreparedGenerationChanged;
            selected_index = for (pinned.plan.boundIndexes()) |index| {
                if (std.mem.eql(u8, index.name, name)) break index;
            } else return error.IndexNotFound;
            if ((try jobs.status(&read, selected_index.?)).state != .ready) return error.RelationalIndexNotReady;
            if (request.include_cursor or request.after != null) cursor_identity = row_cursor_codec.identity(active_view.version(), selected_index.?.name, selected_index.?.tuple.fingerprint);
            const prefix = try records.forwardPrefix(selected_index.?.id());
            lower = try owned.dupe(u8, &prefix);
            upper = (try internal.nextPrefixAlloc(owned, &prefix)).?;
            if (request.lower) |bound| lower = try boundKey(owned, selected_index.?, bound, false);
            if (request.upper) |bound| upper = try boundKey(owned, selected_index.?, bound, true);
            if (std.mem.order(u8, lower, upper) == .gt) return error.InvalidRelationalRowsRequest;
            if (request.after) |encoded| {
                const index = selected_index.?;
                const suffix = try row_cursor_codec.decode(owned, encoded, cursor_identity.?);
                const key = try std.mem.concat(owned, u8, &.{ &prefix, suffix });
                _ = records.parseForward(key, index) catch return error.InvalidRelationalRowsRequest;
                const requested = (try internal.nextPrefixAlloc(owned, key)) orelse return error.InvalidRelationalRowsRequest;
                if (std.mem.order(u8, requested, lower) == .gt) lower = requested;
            }
        }
        const conditions = try alloc.alloc(predicates.Plan, request.conditions.len);
        var initialized: usize = 0;
        errdefer {
            for (conditions[0..initialized]) |*condition| condition.deinit();
            alloc.free(conditions);
        }
        for (conditions, request.conditions) |*plan, condition| {
            plan.* = try predicates.Plan.init(alloc, active_view.tableSchema().*, active_view.physicalLayout(), condition);
            initialized += 1;
        }

        // Candidate metadata belongs to the immutable pinned index plan. Bound
        // at most sixteen promising paths, then compare actual snapshot-local
        // cardinality probes. This never extrapolates a sample into invented
        // table statistics; saturated samples remain lower bounds. All query
        // predicates remain residual checks against the selected snapshot.
        var planner_candidates: usize = 0;
        var planner_records: usize = 0;
        if (selected_index == null and request.auto_index and request.primary_lower == null and request.primary_upper == null) {
            if (indexes) |pinned| {
                if (pinned.plan.schemaView().epoch != active_view.epoch) return error.PreparedGenerationChanged;
                const raw_head = try read.get(catalog.head_key);
                if (!(try catalog.Head.decode(raw_head)).eql(pinned.head)) return error.PreparedGenerationChanged;
                var candidates: [16]Candidate = undefined;
                var candidate_count: usize = 0;
                const implication = try owned.alloc(bool, conditions.len);
                const KeyConditions = struct { equality: ?Value = null, has_range: bool = false };
                var keyed: std.AutoHashMapUnmanaged(u64, KeyConditions) = .empty;
                try keyed.ensureTotalCapacity(owned, @intCast(conditions.len));
                for (request.conditions, conditions) |condition, compiled| {
                    const identity = @as(u64, compiled.tuple.keys[0].ordinal) * 2 + @intFromBool(compiled.tuple.keys[0].fold_ascii);
                    const entry = keyed.getOrPutAssumeCapacity(identity);
                    if (!entry.found_existing) entry.value_ptr.* = .{};
                    switch (condition.op) {
                        .eq, .is_not_distinct => if (entry.value_ptr.equality == null) {
                            entry.value_ptr.equality = condition.value;
                        },
                        .gt, .gte, .lt, .lte => if (condition.value != .null) {
                            entry.value_ptr.has_range = true;
                        },
                        else => {},
                    }
                }
                for (pinned.plan.boundIndexes()) |candidate| {
                    if (candidate.tuple.keys.len == 0) continue;
                    var equality_count: usize = 0;
                    var candidate_values: [32]Value = @splat(.null);
                    for (candidate.tuple.keys) |key| {
                        if (equality_count == candidate_values.len or key.expression != null) break;
                        const operand = keyed.get(@as(u64, key.ordinal) * 2 + @intFromBool(key.fold_ascii)) orelse break;
                        candidate_values[equality_count] = operand.equality orelse break;
                        equality_count += 1;
                    }
                    var has_range = false;
                    if (equality_count < @min(candidate.tuple.keys.len, candidate_values.len)) {
                        const key = candidate.tuple.keys[equality_count];
                        if (key.expression == null) if (keyed.get(@as(u64, key.ordinal) * 2 + @intFromBool(key.fold_ascii))) |operand| {
                            has_range = operand.has_range;
                        };
                    }
                    if (equality_count == 0 and !has_range) continue;
                    // Reject unusable access paths without touching durable
                    // readiness records or evaluating partial-index proofs.
                    @memset(implication, false);
                    if (candidate.predicate) |predicate| {
                        if (!predicate.impliedByAndMark(conditions, implication)) continue;
                    }
                    var covering = false;
                    if (candidate.cover) |cover| {
                        covering = request.row_filter == null and !request.include_primary_digest;
                        for (fields) |field| {
                            if (!cover.contains(field)) covering = false;
                        }
                        for (request.conditions, implication) |condition, implied| {
                            if (!implied and !cover.contains(condition.column)) covering = false;
                        }
                    }
                    const score = equality_count * 4 + @as(usize, @intFromBool(has_range)) * 2 + @as(usize, @intFromBool(covering));
                    var position: usize = 0;
                    while (position < candidate_count and (candidates[position].score > score or
                        (candidates[position].score == score and std.mem.lessThan(u8, candidates[position].index.name, candidate.name)))) : (position += 1)
                    {}
                    if (position == candidates.len) continue;
                    const end = @min(candidate_count, candidates.len - 1);
                    std.mem.copyBackwards(Candidate, candidates[position + 1 .. end + 1], candidates[position..end]);
                    candidates[position] = .{ .index = candidate, .score = score, .covering = covering, .equality_count = equality_count, .has_range = has_range, .values = candidate_values };
                    candidate_count = @min(candidate_count + 1, candidates.len);
                }
                if (candidate_count != 0) {
                    const owner = try jobs.ownership(&read);
                    var best_cost: usize = std.math.maxInt(usize);
                    var best_complete = false;
                    var best_candidate: ?*Candidate = null;
                    for (candidates[0..candidate_count]) |*candidate| {
                        planner_candidates += 1;
                        if ((try jobs.statusWithOwnership(&read, candidate.index, owner)).state != .ready) continue;
                        if (candidate_count == 1) {
                            selected_index = candidate.index;
                            best_candidate = candidate;
                            break;
                        }
                        var scratch = std.heap.ArenaAllocator.init(alloc);
                        defer scratch.deinit();
                        const bounds = try candidate.bounds(scratch.allocator(), request.conditions, conditions);
                        var count: usize = 0;
                        var complete = std.mem.order(u8, bounds.lower, bounds.upper) != .lt;
                        if (std.mem.order(u8, bounds.lower, bounds.upper) == .lt) {
                            var cursor = try read.openCursor();
                            defer cursor.close();
                            cursor.setUpperBound(bounds.upper);
                            var entry = try cursor.seekAtOrAfter(bounds.lower);
                            while (entry != null) {
                                count += 1;
                                if (count == 8) break;
                                entry = try cursor.next();
                            }
                            complete = entry == null;
                            planner_records += count;
                        }
                        const cost = count * (1 + @as(usize, @intFromBool(!candidate.covering)));
                        if (selected_index == null or (complete and !best_complete) or (complete == best_complete and cost < best_cost)) {
                            selected_index = candidate.index;
                            best_candidate = candidate;
                            best_cost = cost;
                            best_complete = complete;
                        }
                    }
                    if (best_candidate) |candidate| {
                        const bounds = try candidate.bounds(owned, request.conditions, conditions);
                        lower = bounds.lower;
                        upper = bounds.upper;
                    }
                }
            }
        }
        const implied_conditions = try owned.alloc(bool, conditions.len);
        @memset(implied_conditions, false);
        if (selected_index) |index| if (index.predicate) |condition| {
            if (!condition.impliedByAndMark(conditions, implied_conditions)) return error.PartialIndexPredicateNotImplied;
        };
        var index_only = selected_index != null and request.row_filter == null and !request.include_primary_digest;
        if (selected_index) |index| {
            if (index.cover) |cover| {
                for (fields) |field| if (!cover.contains(field)) {
                    index_only = false;
                };
                for (request.conditions, implied_conditions) |condition, implied| if (!implied and !cover.contains(condition.column)) {
                    index_only = false;
                };
            } else index_only = false;
        }
        return .{
            .alloc = alloc,
            .arena = arena,
            .read = read,
            .active = active_view.clone(),
            .index_plan = if (selected_index != null) indexes.?.plan.clone() else null,
            .planner_candidates = planner_candidates,
            .planner_records = planner_records,
            .index = selected_index,
            .lower = lower,
            .upper = upper,
            .owned_lower = owned_lower,
            .owned_upper = owned_upper,
            .fields = fields,
            .now_ns = now_ns,
            .authenticated = authenticated,
            .include_primary_digest = request.include_primary_digest,
            .include_cursor = request.include_cursor,
            .cursor_identity = cursor_identity,
            .row_filter = request.row_filter,
            .index_only = index_only,
            .conditions = conditions,
            .implied_conditions = implied_conditions,
            .done = std.mem.order(u8, lower, upper) != .lt,
        };
    }

    const Candidate = struct {
        index: plans.BoundIndex,
        score: usize,
        covering: bool,
        equality_count: usize,
        has_range: bool,
        values: [32]Value,

        fn bounds(self: *Candidate, alloc: Allocator, requested: []const Condition, compiled: []const predicates.Plan) !struct { lower: []const u8, upper: []const u8 } {
            var lower = try boundKey(alloc, self.index, .{ .values = self.values[0..self.equality_count] }, false);
            var upper = try boundKey(alloc, self.index, .{ .values = self.values[0..self.equality_count] }, true);
            if (self.has_range) {
                const key = self.index.tuple.keys[self.equality_count];
                var endpoints: [2]?usize = .{ null, null };
                for (requested, compiled, 0..) |condition, plan, i| {
                    if (plan.tuple.keys[0].ordinal != key.ordinal or plan.tuple.keys[0].fold_ascii != key.fold_ascii or condition.value == .null) continue;
                    const logical_upper = switch (condition.op) {
                        .gt, .gte => false,
                        .lt, .lte => true,
                        else => continue,
                    };
                    const slot = @intFromBool(logical_upper);
                    if (endpoints[slot]) |prior| {
                        const order = std.mem.order(u8, plan.operand, compiled[prior].operand);
                        if ((logical_upper and order == .gt) or (!logical_upper and order == .lt)) continue;
                        if (order == .eq and (condition.op == .gte or condition.op == .lte)) continue;
                    }
                    endpoints[slot] = i;
                }
                // Compare precompiled scalar operands before encoding tuples:
                // repeated constraints must not multiply wide-prefix memory.
                for (endpoints, 0..) |endpoint, slot| {
                    const condition = requested[endpoint orelse continue];
                    const is_upper = (slot == 1) != key.descending;
                    self.values[self.equality_count] = condition.value;
                    const bound = try boundKey(alloc, self.index, .{ .values = self.values[0 .. self.equality_count + 1], .inclusive = condition.op == .gte or condition.op == .lte }, is_upper);
                    if (is_upper) {
                        if (std.mem.order(u8, bound, upper) == .lt) upper = bound;
                    } else if (std.mem.order(u8, bound, lower) == .gt) lower = bound;
                }
            }
            return .{ .lower = lower, .upper = upper };
        }
    };

    fn boundKey(alloc: Allocator, index: plans.BoundIndex, bound: Bound, upper: bool) ![]const u8 {
        var key = std.ArrayList(u8).empty;
        // The caller's arena owns this scratch and its result.
        try key.appendSlice(alloc, &(try records.forwardPrefix(index.id())));
        _ = try index.tuple.appendValues(alloc, &key, bound.values);
        if (upper == bound.inclusive) return (try internal.nextPrefixAlloc(alloc, key.items)) orelse error.InvalidRelationalIndexBound;
        return key.items;
    }

    pub fn deinit(self: *Reader) void {
        if (self.row_policy_lease) |*lease| lease.release();
        if (self.failed_row) |row| self.alloc.free(row.key);
        if (self.covering_projection) |*plan| plan.deinit();
        for (self.covering_conditions) |*condition| condition.deinit();
        self.alloc.free(self.covering_conditions);
        for (0..self.bindings.len) |slot| self.evictSourceBinding(slot);
        for (self.conditions) |*condition| condition.deinit();
        self.alloc.free(self.conditions);
        if (self.index_plan) |*plan| plan.release();
        self.active.release();
        self.read.abort();
        self.after.deinit(self.alloc);
        self.arena.deinit();
        self.* = undefined;
    }

    fn coveringView(self: *Reader, key: []const u8, value: []const u8) !codec.OrdinalRowView {
        const cover = &self.index.?.cover.?;
        const payload = try records.forwardPayload(key, value);
        const row = try cover.decode(payload);
        if (self.covering_projection == null) {
            const selected = try codec.OrdinalProjectionPlan.init(self.alloc, cover.table(), &cover.layout, self.fields);
            errdefer {
                var cleanup = selected;
                cleanup.deinit();
            }
            var residual_count: usize = 0;
            for (self.implied_conditions) |implied| if (!implied) {
                residual_count += 1;
            };
            const conditions = try self.alloc.alloc(predicates.Source, residual_count);
            var initialized: usize = 0;
            errdefer {
                for (conditions[0..initialized]) |*condition| condition.deinit();
                self.alloc.free(conditions);
            }
            for (self.conditions, self.implied_conditions) |*plan, implied| {
                if (implied) continue;
                conditions[initialized] = try plan.projectSource(self.alloc, cover.table(), &cover.layout);
                initialized += 1;
            }
            self.covering_conditions = conditions;
            self.covering_projection = selected;
        }
        return row;
    }

    fn rowView(self: *Reader, raw: []const u8) !codec.OrdinalRowView {
        const version = try codec.rowSchemaVersion(raw);
        if (self.source == null or self.source.?.version() != version) {
            for (self.bindings, 0..) |entry, slot| {
                const binding = entry orelse continue;
                if (binding.view.version() != version) continue;
                std.mem.copyBackwards(?*SourceBinding, self.bindings[1 .. slot + 1], self.bindings[0..slot]);
                self.bindings[0] = binding;
                self.source = binding.view;
                self.selected = binding.projection;
                self.source_conditions = binding.conditions;
                self.source_cache_hits += 1;
                return if (self.authenticated)
                    try codec.ordinalRowViewTrusted(raw, binding.view.tableSchema().*, binding.view.physicalLayout())
                else
                    try codec.ordinalRowViewSelective(raw, binding.view.tableSchema().*, binding.view.physicalLayout());
            }
            const binding = try self.alloc.create(SourceBinding);
            errdefer self.alloc.destroy(binding);
            binding.arena = std.heap.ArenaAllocator.init(self.alloc);
            errdefer binding.arena.deinit();
            const alloc = binding.arena.allocator();
            // Fault historical layouts through this read snapshot, not through
            // the live registry: a whole-store restore may reuse version IDs.
            const source = if (version == self.active.version()) self.active.clone() else blk: {
                const key = try schema.schemaVersionKeyAlloc(alloc, version);
                const encoded = self.read.get(key) catch |err| switch (err) {
                    error.NotFound => return error.UnknownSchemaVersion,
                    else => return err,
                };
                const table = try schema.deserializeSchema(alloc, encoded);
                if (table.version != version) return error.RelationalRowSchemaMismatch;
                break :blk registry.SchemaView{ .epoch = try registry.Epoch.createOwned(alloc, table) };
            };
            errdefer {
                var release = source;
                release.release();
            }
            for (self.fields) |field| {
                const current_ordinal = self.active.physicalLayout().ordinalForName(self.active.tableSchema().relational_columns, field).?;
                const source_ordinal = source.physicalLayout().ordinalForName(source.tableSchema().relational_columns, field) orelse continue;
                if (self.active.tableSchema().relational_columns[current_ordinal].column_type != source.tableSchema().relational_columns[source_ordinal].column_type)
                    return error.RelationalIndexColumnTypeMismatch;
            }
            const source_conditions = try alloc.alloc(predicates.Source, self.conditions.len);
            var initialized: usize = 0;
            errdefer {
                for (source_conditions[0..initialized]) |*condition| condition.deinit();
                alloc.free(source_conditions);
            }
            for (source_conditions, self.conditions) |*condition, *plan| {
                condition.* = try plan.projectSource(alloc, source.tableSchema().*, source.physicalLayout());
                initialized += 1;
            }
            const selected = try codec.OrdinalProjectionPlan.init(alloc, source.tableSchema().*, source.physicalLayout(), self.fields);
            binding.view = source;
            binding.projection = selected;
            binding.conditions = source_conditions;
            const bytes = binding.arena.queryCapacity();
            self.evictSourceBinding(self.bindings.len - 1);
            var slot = self.bindings.len - 1;
            while (slot > 0 and self.binding_bytes + bytes > 2 * 1024 * 1024) {
                slot -= 1;
                self.evictSourceBinding(slot);
            }
            std.mem.copyBackwards(?*SourceBinding, self.bindings[1..], self.bindings[0 .. self.bindings.len - 1]);
            self.bindings[0] = binding;
            self.binding_bytes += bytes;
            self.source_compilations += 1;
            self.source = source;
            self.selected = selected;
            self.source_conditions = source_conditions;
        }
        return if (self.authenticated)
            try codec.ordinalRowViewTrusted(raw, self.source.?.tableSchema().*, self.source.?.physicalLayout())
        else
            try codec.ordinalRowViewSelective(raw, self.source.?.tableSchema().*, self.source.?.physicalLayout());
    }

    fn observeFailedRow(self: *Reader, key: []const u8, raw: []const u8) !void {
        const version = try codec.rowWriteTimestampNs(raw);
        const schema_version = try codec.rowSchemaVersion(raw);
        const decoded = (try internal.decodeStoredDocumentRowKeyAlloc(self.alloc, key)) orelse return error.InvalidRelationalRowsRequest;
        if (self.failed_row) |old| self.alloc.free(old.key);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(raw, &digest, .{});
        self.failed_row = .{ .key = decoded, .json = "{}", .version = version, .schema_version = schema_version, .semantic_hash = @splat(0), .expected_content_digest = digest };
    }

    /// Page continuation advances only after successful preparation. An OOM,
    /// cancellation, or corrupt row cannot silently consume part of the result.
    pub fn nextPage(self: *Reader, alloc: Allocator, io: ?std.Io, budget: Budget) !Page {
        if (self.row_policy_lease) |*lease| try lease.checkAt(@intCast(@divFloor(time.realtimeNs(), std.time.ns_per_s)));
        var page = try self.nextPageFormat(alloc, io, budget, false);
        errdefer page.deinit();
        if (self.row_policy_lease) |*lease| try lease.checkAt(@intCast(@divFloor(time.realtimeNs(), std.time.ns_per_s)));
        return page;
    }

    pub fn nextTypedPage(self: *Reader, alloc: Allocator, io: ?std.Io, budget: Budget) !Page {
        if (self.row_policy_lease) |*lease| try lease.checkAt(@intCast(@divFloor(time.realtimeNs(), std.time.ns_per_s)));
        var page = try self.nextPageFormat(alloc, io, budget, true);
        errdefer page.deinit();
        if (self.row_policy_lease) |*lease| try lease.checkAt(@intCast(@divFloor(time.realtimeNs(), std.time.ns_per_s)));
        return page;
    }

    fn nextPageFormat(self: *Reader, alloc: Allocator, io: ?std.Io, budget: Budget, typed_output: bool) !Page {
        try budget.validate();
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        var result = Page{ .arena = undefined, .rows = &.{}, .more = false, .records_examined = 0, .output_bytes = 0 };
        if (self.done) {
            result.arena = arena;
            return result;
        }
        const page = arena.allocator();
        var output = std.ArrayList(Row).empty;
        var predicate_scratch = std.ArrayList(u8).empty;
        defer predicate_scratch.deinit(alloc);
        var continuation = std.ArrayList(u8).empty;
        defer continuation.deinit(alloc);
        try continuation.appendSlice(alloc, self.after.items);
        var successor = std.ArrayList(u8).empty;
        defer successor.deinit(alloc);
        var cursor = try self.read.openCursor();
        defer cursor.close();
        cursor.setUpperBound(self.upper);
        const start = if (self.after.items.len == 0) self.lower else if (self.index != null) self.after.items else try internal.documentPrefixSuccessor(alloc, &successor, self.after.items);
        var entry = try cursor.seekAtOrAfter(start);
        var exhausted = true;
        const started = time.monotonicNs();
        while (entry) |kv| : (entry = if (self.index != null) try cursor.next() else try cursor.seekAtOrAfter(try internal.documentPrefixSuccessor(alloc, &successor, continuation.items))) {
            if (io) |runtime_io| try runtime_io.checkCancel();
            if (self.after.items.len != 0 and std.mem.order(u8, kv.key, self.after.items) != .gt) continue;
            if (std.mem.order(u8, kv.key, self.upper) != .lt) break;
            result.records_examined += 1;
            var scratch = std.heap.ArenaAllocator.init(alloc);
            defer scratch.deinit();
            const temporary = scratch.allocator();
            const primary: ?[]const u8 = if (self.index) |index| blk: {
                const ownership = try records.parseForward(kv.key, index);
                var key = std.ArrayList(u8).empty;
                try key.append(temporary, internal.user_namespace);
                try key.appendSlice(temporary, ownership.document_component);
                try key.append(temporary, internal.relational_row_kind);
                if (std.mem.order(u8, key.items, self.owned_lower) == .lt or std.mem.order(u8, key.items, self.owned_upper) != .lt) break :blk null;
                break :blk key.items;
            } else blk: {
                // The first companion can sort before the primary row. Probe
                // the exact primary once, then skip the entire document family.
                if (internal.isRelationalRowKey(kv.key)) break :blk kv.key;
                const end = (internal.findComponentTerminator(kv.key, 1) orelse return error.InvalidInternalUserKey) + 2;
                const key = try temporary.alloc(u8, end + 1);
                @memcpy(key[0..end], kv.key[0..end]);
                key[end] = internal.relational_row_kind;
                break :blk key;
            };
            if (primary) |key| {
                if (!self.index_only and !std.mem.eql(u8, key, kv.key)) result.primary_lookups += 1;
                const raw = if (self.index_only or std.mem.eql(u8, key, kv.key)) kv.value else self.read.get(key) catch |err| switch (err) {
                    error.NotFound => {
                        if (self.index != null) return error.InvalidRelationalIndexForwardKey;
                        // Orphan companions still consume one bounded unit of
                        // work, but cannot force a scan through every artifact.
                        continuation.clearRetainingCapacity();
                        try continuation.appendSlice(alloc, kv.key);
                        if (result.records_examined >= budget.records or time.monotonicNs() - started >= budget.time_ns) {
                            exhausted = false;
                            break;
                        }
                        continue;
                    },
                    else => return err,
                };
                const row = if (self.index_only) try self.coveringView(kv.key, raw) else self.rowView(raw) catch |err| {
                    if (err == error.RelationalIndexColumnTypeMismatch) try self.observeFailedRow(key, raw);
                    return err;
                };
                const expired = self.active.visibilityTtlDurationNs() != 0 and row.writeTimestampNs() != 0 and
                    ttl.isExpired(row.writeTimestampNs(), self.active.visibilityTtlDurationNs(), self.now_ns);
                var decoded_key: ?[]const u8 = null;
                const matches = check: {
                    if (expired) break :check false;
                    for (if (self.index_only) self.covering_conditions else self.source_conditions) |condition| {
                        if (io) |runtime_io| try runtime_io.checkCancel();
                        if (!(try condition.evaluate(alloc, &predicate_scratch, row)).matches()) break :check false;
                    }
                    if (self.row_filter) |filter| {
                        decoded_key = (try internal.decodeStoredDocumentRowKeyAlloc(temporary, key)).?;
                        if (!try filter.matches(filter.context, temporary, decoded_key.?, row)) break :check false;
                    }
                    break :check true;
                };
                if (matches) {
                    const projection = if (self.index_only) self.covering_projection.? else self.selected.?;
                    const typed_projection = if (typed_output) try row.projectSqlTypedAlloc(page, projection) else null;
                    const typed = if (typed_projection) |projected| projected.value else null;
                    const json = if (typed_output) "" else try row.projectAlloc(temporary, projection);
                    const json_null_fields = if (typed_output) &.{} else try row.projectJsonNullFieldsAlloc(page, projection);
                    var null_metadata_bytes: usize = json_null_fields.len * @sizeOf([]const u8);
                    for (json_null_fields) |name| null_metadata_bytes +|= name.len;
                    const document = decoded_key orelse (try internal.decodeStoredDocumentRowKeyAlloc(temporary, key)).?;
                    const row_cursor = if (self.include_cursor) blk: {
                        const identity = self.cursor_identity orelse break :blk null;
                        break :blk try row_cursor_codec.encode(temporary, identity, kv.key[records.forward_prefix_len..]);
                    } else null;
                    const size = (if (typed) |value| try typedSize(value, 0) else json.len) +| (if (typed_projection) |projected| projected.sql_nulls.len else @as(usize, 0)) +| document.len +| (if (row_cursor) |encoded| encoded.len else @as(usize, 0)) +| null_metadata_bytes;
                    if (size > budget.output_bytes) {
                        if (!self.index_only) try self.observeFailedRow(key, raw);
                        return error.RelationalRowResultTooLarge;
                    }
                    if (size > budget.output_bytes - result.output_bytes) {
                        exhausted = false;
                        break;
                    }
                    try output.append(page, .{
                        .key = try page.dupe(u8, document),
                        .json = try page.dupe(u8, json),
                        .typed = typed,
                        .sql_nulls = if (typed_projection) |projected| projected.sql_nulls else null,
                        .json_null_fields = json_null_fields,
                        .version = row.writeTimestampNs(),
                        .schema_version = if (self.index_only) try @import("relational_index_cover.zig").Plan.sourceVersion(try records.forwardPayload(kv.key, raw)) else row.table_schema.version,
                        .semantic_hash = row.semanticHash(),
                        .cursor = if (row_cursor) |encoded| try page.dupe(u8, encoded) else null,
                        .expected_content_digest = if (self.include_primary_digest) blk: {
                            var digest: [32]u8 = undefined;
                            std.crypto.hash.sha2.Sha256.hash(raw, &digest, .{});
                            break :blk digest;
                        } else null,
                    });
                    result.output_bytes += size;
                    if (self.index_only) result.index_only_rows += 1;
                }
            }
            continuation.clearRetainingCapacity();
            try continuation.appendSlice(alloc, kv.key);
            if (output.items.len >= budget.rows or result.records_examined >= budget.records or time.monotonicNs() - started >= budget.time_ns) {
                exhausted = false;
                break;
            }
        }
        try self.after.ensureTotalCapacity(self.alloc, continuation.items.len);
        @memcpy(self.after.items.ptr[0..continuation.items.len], continuation.items);
        self.after.items.len = continuation.items.len;
        self.done = exhausted;
        result.arena = arena;
        result.rows = output.items;
        result.more = !exhausted;
        return result;
    }

    fn typedSize(value: std.json.Value, depth: usize) error{RelationalRowResultTooLarge}!usize {
        if (depth > 64) return error.RelationalRowResultTooLarge;
        var size: usize = @sizeOf(std.json.Value);
        switch (value) {
            .string, .number_string => |text| size +|= text.len,
            .array => |items| for (items.items) |item| {
                size +|= try typedSize(item, depth + 1);
            },
            .object => |object| for (object.keys(), object.values()) |key, item| {
                size +|= key.len +| try typedSize(item, depth + 1);
            },
            else => {},
        }
        return size;
    }
};
