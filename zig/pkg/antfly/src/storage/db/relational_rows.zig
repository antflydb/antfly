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
    /// Null selects primary-key order. Named indexes must have a durable
    /// ready proof for this exact generation and owned range.
    index: ?[]const u8 = null,
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
    version: u64,
    schema_version: u32,
    semantic_hash: [32]u8,
};

pub const Page = struct {
    arena: std.heap.ArenaAllocator,
    rows: []const Row,
    more: bool,
    records_examined: usize,
    output_bytes: usize,

    pub fn deinit(self: *Page) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const Reader = struct {
    alloc: Allocator,
    arena: std.heap.ArenaAllocator,
    read: docstore.DocStore.Txn,
    active: registry.SchemaView,
    index_plan: ?plans.View,
    index: ?plans.BoundIndex,
    lower: []const u8,
    upper: []const u8,
    owned_lower: []const u8,
    owned_upper: []const u8,
    fields: []const []const u8,
    now_ns: u64,
    authenticated: bool,
    row_filter: ?RowFilter,
    source: ?registry.SchemaView = null,
    selected: ?codec.OrdinalProjectionPlan = null,
    conditions: []predicates.Plan,
    source_conditions: []predicates.Source = &.{},
    after: std.ArrayList(u8) = .empty,
    done: bool = false,

    /// Caller fences schema/catalog publication while this snapshot is opened.
    /// Owns request data except row_filter.context, which the caller retains
    /// through reader.deinit. The reader must close before its DB closes.
    pub fn open(alloc: Allocator, store: *docstore.DocStore, active_view: registry.SchemaView, indexes: ?catalog.WriteSnapshot, request: Request, now_ns: u64) !Reader {
        if (active_view.storageMode() != .relational) return error.RelationalTableRequired;
        if (request.expected_schema_version) |version| if (version != active_view.version()) return error.PreparedGenerationChanged;
        if (request.fields.len > 256 or request.conditions.len > 256 or (request.index == null and (request.lower != null or request.upper != null)))
            return error.InvalidRelationalRowsRequest;
        if (request.index != null and (request.primary_lower != null or request.primary_upper != null)) return error.InvalidRelationalRowsRequest;
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
        var read = try store.beginReadTxn();
        errdefer read.abort();
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
            const prefix = try records.forwardPrefix(selected_index.?.id());
            lower = try owned.dupe(u8, &prefix);
            upper = (try internal.nextPrefixAlloc(owned, &prefix)).?;
            if (request.lower) |bound| lower = try boundKey(owned, selected_index.?, bound, false);
            if (request.upper) |bound| upper = try boundKey(owned, selected_index.?, bound, true);
            if (std.mem.order(u8, lower, upper) == .gt) return error.InvalidRelationalRowsRequest;
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
        return .{
            .alloc = alloc,
            .arena = arena,
            .read = read,
            .active = active_view.clone(),
            .index_plan = if (selected_index != null) indexes.?.plan.clone() else null,
            .index = selected_index,
            .lower = lower,
            .upper = upper,
            .owned_lower = owned_lower,
            .owned_upper = owned_upper,
            .fields = fields,
            .now_ns = now_ns,
            .authenticated = store.valuesAreAuthenticated(),
            .row_filter = request.row_filter,
            .conditions = conditions,
            .done = std.mem.order(u8, lower, upper) != .lt,
        };
    }

    fn boundKey(alloc: Allocator, index: plans.BoundIndex, bound: Bound, upper: bool) ![]const u8 {
        var key = std.ArrayList(u8).empty;
        // The caller's arena owns this scratch and its result.
        try key.appendSlice(alloc, &(try records.forwardPrefix(index.id())));
        _ = try index.tuple.appendValues(alloc, &key, bound.values);
        if (upper == bound.inclusive) return (try internal.nextPrefixAlloc(alloc, key.items)) orelse error.InvalidRelationalIndexBound;
        return key.items;
    }

    pub fn deinit(self: *Reader) void {
        for (self.source_conditions) |*condition| condition.deinit();
        self.alloc.free(self.source_conditions);
        for (self.conditions) |*condition| condition.deinit();
        self.alloc.free(self.conditions);
        if (self.selected) |*plan| plan.deinit();
        if (self.source) |*view| view.release();
        if (self.index_plan) |*plan| plan.release();
        self.active.release();
        self.read.abort();
        self.after.deinit(self.alloc);
        self.arena.deinit();
        self.* = undefined;
    }

    fn rowView(self: *Reader, raw: []const u8) !codec.OrdinalRowView {
        const version = try codec.rowSchemaVersion(raw);
        if (self.source == null or self.source.?.version() != version) {
            // Fault historical layouts through this read snapshot, not through
            // the live registry: a whole-store restore may reuse version IDs.
            const source = if (version == self.active.version()) self.active.clone() else blk: {
                const key = try schema.schemaVersionKeyAlloc(self.alloc, version);
                defer self.alloc.free(key);
                const encoded = self.read.get(key) catch |err| switch (err) {
                    error.NotFound => return error.UnknownSchemaVersion,
                    else => return err,
                };
                const table = try schema.deserializeSchema(self.alloc, encoded);
                errdefer schema.freeSchema(self.alloc, table);
                if (table.version != version) return error.RelationalRowSchemaMismatch;
                break :blk registry.SchemaView{ .epoch = try registry.Epoch.createOwned(self.alloc, table) };
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
            const source_conditions = try self.alloc.alloc(predicates.Source, self.conditions.len);
            var initialized: usize = 0;
            errdefer {
                for (source_conditions[0..initialized]) |*condition| condition.deinit();
                self.alloc.free(source_conditions);
            }
            for (source_conditions, self.conditions) |*condition, *plan| {
                condition.* = try plan.projectSource(self.alloc, source.tableSchema().*, source.physicalLayout());
                initialized += 1;
            }
            const selected = try codec.OrdinalProjectionPlan.init(self.alloc, source.tableSchema().*, source.physicalLayout(), self.fields);
            for (self.source_conditions) |*condition| condition.deinit();
            self.alloc.free(self.source_conditions);
            if (self.selected) |*old| old.deinit();
            if (self.source) |*old| old.release();
            self.source = source;
            self.selected = selected;
            self.source_conditions = source_conditions;
        }
        return if (self.authenticated)
            try codec.ordinalRowViewTrusted(raw, self.source.?.tableSchema().*, self.source.?.physicalLayout())
        else
            try codec.ordinalRowViewSelective(raw, self.source.?.tableSchema().*, self.source.?.physicalLayout());
    }

    /// Page continuation advances only after successful preparation. An OOM,
    /// cancellation, or corrupt row cannot silently consume part of the result.
    pub fn nextPage(self: *Reader, alloc: Allocator, io: ?std.Io, budget: Budget) !Page {
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
        var cursor = try self.read.openCursor();
        defer cursor.close();
        cursor.setUpperBound(self.upper);
        var entry = try cursor.seekAtOrAfter(if (self.after.items.len == 0) self.lower else self.after.items);
        var exhausted = true;
        const started = time.monotonicNs();
        while (entry) |kv| : (entry = try cursor.next()) {
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
            } else if (internal.isRelationalRowKey(kv.key)) kv.key else null;
            if (primary) |key| {
                const raw = if (self.index != null) self.read.get(key) catch |err| switch (err) {
                    error.NotFound => return error.InvalidRelationalIndexForwardKey,
                    else => return err,
                } else kv.value;
                const row = try self.rowView(raw);
                const expired = self.active.tableSchema().ttl_duration_ns != 0 and row.writeTimestampNs() != 0 and
                    ttl.isExpired(row.writeTimestampNs(), self.active.tableSchema().ttl_duration_ns, self.now_ns);
                var decoded_key: ?[]const u8 = null;
                const matches = check: {
                    if (expired) break :check false;
                    for (self.source_conditions) |condition| {
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
                    const json = try row.projectAlloc(temporary, self.selected.?);
                    const document = decoded_key orelse (try internal.decodeStoredDocumentRowKeyAlloc(temporary, key)).?;
                    const size = json.len + document.len;
                    if (size > budget.output_bytes) return error.RelationalRowResultTooLarge;
                    if (size > budget.output_bytes - result.output_bytes) {
                        exhausted = false;
                        break;
                    }
                    try output.append(page, .{
                        .key = try page.dupe(u8, document),
                        .json = try page.dupe(u8, json),
                        .version = row.writeTimestampNs(),
                        .schema_version = row.table_schema.version,
                        .semantic_hash = row.semanticHash(),
                    });
                    result.output_bytes += size;
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
};
