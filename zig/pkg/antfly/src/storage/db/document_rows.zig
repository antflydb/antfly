// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Retained document SQL read view. Shares docstore snapshot, schema epoch,
//! owner-prefix traversal, TTL and authorization machinery with native reads.
const std = @import("std");
const store_mod = @import("../docstore.zig");
const registry = @import("schema_registry.zig");
const types = @import("types.zig");
const internal = @import("../internal_keys.zig");
const time = @import("antfly_platform").time;
const ttl = @import("../ttl.zig");
const graph = @import("query/graph_exec.zig");
const document = @import("../../sql/document_row.zig");
const catalog = @import("../../sql/catalog.zig");
const scalar = @import("../../sql/scalar.zig");
const View = @import("../relational_read_view.zig").View;

pub const Session = struct {
    row_policy_lease: ?@import("row_policy_gate.zig").Gate.Lease = null,
    range_proofs: ?[]const @import("../range_protection.zig").Proof = null,
    alloc: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    store: *store_mod.DocStore,
    txn: store_mod.DocStore.Txn,
    schema: ?registry.SchemaView,
    table: catalog.Table,
    projection: document.Projection,
    include_document: bool,
    include_primary_digest: bool,
    condition_projection: document.Projection,
    conditions: []const types.RelationalRowQuery.Condition,
    filter: ?graph.PreparedPatternFilter,
    range: types.ByteRange,
    from: []const u8,
    to: []const u8,
    inclusive_from: bool,
    exclusive_to: bool,
    lower: []const u8,
    upper: []const u8,
    after: std.ArrayListUnmanaged(u8) = .empty,
    started: bool = false,
    done: bool = false,
    failed: bool = false,
    now_ns: u64,
    cancellation: @FieldType(types.ScanOptions, "cancellation"),
    deadline_ns: ?u64,

    /// Takes ownership of txn on every outcome; clones the borrowed epoch.
    pub fn openSnapshot(alloc: std.mem.Allocator, store: *store_mod.DocStore, txn: store_mod.DocStore.Txn, schema: ?registry.SchemaView, range: types.ByteRange, from: []const u8, to: []const u8, opts: types.ScanOptions, now_ns: u64) !*Session {
        var read = txn;
        errdefer read.abort();
        if (schema) |view| if (view.storageMode() != .document) return error.UnsupportedSqlExecution;
        if (opts.include_range_proofs) if (schema) |view| if (view.tableSchema().ttl_duration_ns != 0) return error.UnsupportedSqlExecution;
        if (opts.relational_query_json.len > 1024 * 1024 or opts.limit > 4096) return error.InvalidRelationalRowsRequest;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const input = opts.relational_query orelse try std.json.parseFromSliceLeaky(types.RelationalRowQuery, owned, opts.relational_query_json, .{ .allocate = .alloc_always, .parse_numbers = false, .ignore_unknown_fields = true });
        if (input.fields.len > 256 or input.conditions.len > 256 or input.index != null or input.after != null or input.lower != null or input.upper != null) return error.InvalidRelationalRowsRequest;
        const version = if (schema) |view| view.version() else 0;
        if (input.schema_version) |expected| if (expected != version) return error.PreparedGenerationChanged;
        const columns = if (schema) |view| if (view.validator()) |validator| try document.deriveColumns(owned, validator.schema) else &.{} else &.{};
        const table: catalog.Table = .{ .id = 0, .physical_name = "", .schema_version = version, .storage_mode = .document, .columns = columns };
        const projection = try document.Projection.init(owned, table, input.fields);
        const condition_fields = try owned.alloc([]const u8, input.conditions.len);
        for (input.conditions, condition_fields) |condition, *field| {
            _ = try table.column(condition.column);
            field.* = condition.column;
            if (condition.collation) |collation| if (!std.mem.eql(u8, collation, "binary")) return error.UnsupportedSqlExecution;
            switch (condition.op) {
                .eq, .ne, .lt, .lte, .gt, .gte, .is_null, .is_not_null => {},
                else => return error.UnsupportedSqlExecution,
            }
        }
        const condition_projection = try document.Projection.init(owned, table, condition_fields);
        const conditions = try owned.alloc(types.RelationalRowQuery.Condition, input.conditions.len);
        for (input.conditions, conditions) |condition, *copy| {
            copy.* = condition;
            copy.column = try owned.dupe(u8, condition.column);
            copy.collation = if (condition.collation) |text| try owned.dupe(u8, text) else null;
            copy.value = if (condition.value) |value| try @import("../typed_json.zig").clone(owned, value) else null;
        }
        var filter = if (opts.filter_query_json.len != 0) try graph.PreparedPatternFilter.init(alloc, opts.filter_query_json) else null;
        errdefer if (filter) |*compiled| compiled.deinit();
        const lower_raw = if (from.len == 0 or std.mem.order(u8, from, range.start) == .lt) range.start else from;
        const upper_raw = if (to.len == 0 or (range.end.len != 0 and std.mem.order(u8, range.end, to) == .lt)) range.end else to;
        const session = try alloc.create(Session);
        errdefer alloc.destroy(session);
        session.* = .{
            .range_proofs = if (opts.include_range_proofs) try @import("../range_protection.zig").capture(owned, &read, lower_raw, upper_raw) else null,
            .alloc = alloc,
            .arena = undefined,
            .store = store,
            .txn = read,
            .schema = null,
            .table = table,
            .projection = projection,
            .include_document = opts.sql_document_preimage,
            .include_primary_digest = opts.include_content_hashes or opts.sql_document_preimage,
            .condition_projection = condition_projection,
            .conditions = conditions,
            .filter = filter,
            .range = .{ .start = try owned.dupe(u8, range.start), .end = try owned.dupe(u8, range.end) },
            .from = try owned.dupe(u8, from),
            .to = try owned.dupe(u8, to),
            .inclusive_from = opts.inclusive_from,
            .exclusive_to = opts.exclusive_to,
            .lower = try internal.documentRangeLowerAlloc(owned, lower_raw),
            .upper = if (upper_raw.len != 0) (try internal.documentRangeUpperAlloc(owned, upper_raw)) orelse &.{internal.user_namespace + 1} else &.{internal.user_namespace + 1},
            .now_ns = now_ns,
            .cancellation = opts.cancellation,
            .deadline_ns = opts.execution_deadline_ns,
        };
        // Epoch retain is the last fallible initialization boundary.
        session.schema = if (schema) |view| view.clone() else null;
        // Move arena state only after all initializer allocations. Copying it
        // before bounds/proofs allocate a new chunk loses that chunk on close.
        session.arena = arena;
        return session;
    }

    pub fn deinit(self: *Session) void {
        if (self.row_policy_lease) |*lease| lease.release();
        self.txn.abort();
        if (self.schema) |*schema| schema.release();
        if (self.filter) |*filter| filter.deinit();
        self.after.deinit(self.alloc);
        self.arena.deinit();
        self.alloc.destroy(self);
    }

    pub fn rangeProofs(self: *Session, alloc: std.mem.Allocator) ![]@import("../range_protection.zig").Proof {
        try self.checkpoint();
        return alloc.dupe(@import("../range_protection.zig").Proof, self.range_proofs orelse return error.SqlRangeTrackingRequired);
    }

    fn checkpoint(self: *const Session) !void {
        if (self.failed) return error.InvalidSqlBackendResponse;
        if (self.cancellation) |token| try token.check();
        if (self.deadline_ns) |deadline| if (time.monotonicNs() >= deadline) return error.DeadlineExceeded;
        if (self.row_policy_lease) |*lease| try lease.checkAt(@intCast(@divFloor(time.realtimeNs(), std.time.ns_per_s)));
    }

    /// Uses the same pinned public validator as native schema admission. SQL
    /// does not implement a second defaults/coercion/generated-value engine.
    /// All output belongs to the bounded caller arena.
    pub fn normalizeRows(self: *Session, alloc: std.mem.Allocator, writes: []const types.BatchWrite) ![]types.BatchWrite {
        if (writes.len > 4096) return error.InvalidArgument;
        const result = try alloc.alloc(types.BatchWrite, writes.len);
        // Parse/validator scratch is row-local, not retained for the batch.
        // Only canonical bytes and null provenance cross the commit boundary.
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        var total: usize = 0;
        for (writes, result) |write, *out| {
            try self.checkpoint();
            _ = scratch.reset(.retain_capacity);
            const temporary = scratch.allocator();
            var value = try std.json.parseFromSliceLeaky(std.json.Value, temporary, write.value, .{ .parse_numbers = false });
            if (value != .object) return error.InvalidBatchRequest;
            // A JSON column's SQL NULL is represented by absence, whereas a
            // JSON null datum is an explicit null member in document storage.
            for (self.table.columns) |column| if (column.type == .json) {
                if (value.object.get(column.path)) |cell| if (cell == .null) {
                    const explicit = for (write.json_null_fields) |name| {
                        if (std.mem.eql(u8, name, column.path)) break true;
                    } else false;
                    if (!explicit) _ = value.object.orderedRemove(column.path);
                };
            };
            if (self.schema) |view| if (view.validator()) |validator| try validator.prepareValue(temporary, temporary, &value);
            var json_nulls: std.ArrayList([]const u8) = .empty;
            for (self.table.columns) |column| if (column.type == .json) {
                if (value.object.get(column.path)) |cell| if (cell == .null) try json_nulls.append(alloc, try alloc.dupe(u8, column.path));
            };
            const bytes = try std.json.Stringify.valueAlloc(alloc, value, .{});
            total = std.math.add(usize, total, bytes.len +| write.key.len) catch return error.RelationalRowResultTooLarge;
            if (total > 16 * 1024 * 1024) return error.RelationalRowResultTooLarge;
            out.* = .{ .key = try alloc.dupe(u8, write.key), .value = bytes, .json_null_fields = try json_nulls.toOwnedSlice(alloc) };
        }
        try self.checkpoint();
        return result;
    }

    pub fn next(self: *Session, alloc: std.mem.Allocator, limit: u32) !View.Page {
        if (limit == 0 or limit > 4096) return error.InvalidRelationalRowsBudget;
        try self.checkpoint();
        errdefer self.failed = true;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        if (self.done) return .{ .arena = arena, .rows = &.{}, .after = null };
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const lower = if (self.started) blk: {
            const prefix = try internal.documentExactPrefixAlloc(owned, self.after.items);
            prefix[prefix.len - 1] = 1;
            break :blk prefix;
        } else self.lower;
        const Context = struct {
            session: *Session,
            owned: std.mem.Allocator,
            scratch: *std.heap.ArenaAllocator,
            rows: std.ArrayList(View.Row) = .empty,
            limit: u32,
            visited: usize = 0,
            bytes: usize = 0,
            begin: u64,
            more: bool = false,
            previous_after: []const u8 = "",

            fn admit(raw: ?*anyopaque, candidate: []const u8) !store_mod.DocStore.ScanAction {
                const ctx: *@This() = @ptrCast(@alignCast(raw.?));
                try ctx.session.checkpoint();
                if (ctx.visited != 0 and (ctx.rows.items.len >= ctx.limit or ctx.visited >= 1024 or ctx.bytes >= 16 * 1024 * 1024 or time.monotonicNs() -| ctx.begin >= 5 * std.time.ns_per_ms)) {
                    ctx.more = true;
                    return .stop;
                }
                _ = ctx.scratch.reset(.retain_capacity);
                ctx.previous_after = try ctx.scratch.allocator().dupe(u8, ctx.session.after.items);
                const key = (try internal.decodePrimaryDocumentKeyAlloc(ctx.scratch.allocator(), candidate)) orelse return error.InvalidInternalUserKey;
                if (ctx.session.to.len != 0) {
                    const order = std.mem.order(u8, key, ctx.session.to);
                    if (order == .gt or (order == .eq and ctx.session.exclusive_to)) return .stop;
                }
                if (ctx.session.range.end.len != 0 and std.mem.order(u8, key, ctx.session.range.end) != .lt) return .stop;
                try ctx.session.after.resize(ctx.session.alloc, key.len);
                @memcpy(ctx.session.after.items, key);
                ctx.session.started = true;
                ctx.visited += 1;
                return .@"continue";
            }

            fn visit(raw: ?*anyopaque, _: []const u8, bytes: []const u8) !store_mod.DocStore.ScanAction {
                const ctx: *@This() = @ptrCast(@alignCast(raw.?));
                const session = ctx.session;
                const key = session.after.items;
                if (!session.range.contains(key) or (session.from.len != 0 and !session.inclusive_from and std.mem.eql(u8, session.from, key))) return .@"continue";
                if (bytes.len > 16 * 1024 * 1024) return error.RelationalRowResultTooLarge;
                const temporary = ctx.scratch.allocator();
                const duration = if (session.schema) |schema| schema.visibilityTtlDurationNs() else 0;
                const version: u64 = version: {
                    const ttl_key = try internal.ttlKeyAlloc(temporary, key);
                    const timestamp = session.txn.get(ttl_key) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                    if (timestamp) |value| {
                        if (value.len != 8) return error.InvalidData;
                        const ns = std.mem.readInt(u64, value[0..8], .little);
                        if (duration != 0 and ns != 0 and ttl.isExpired(ns, duration, session.now_ns)) return .@"continue";
                        break :version ns;
                    }
                    break :version 0;
                };
                const root = try std.json.parseFromSliceLeaky(std.json.Value, temporary, bytes, .{ .parse_numbers = false });
                if (session.filter) |*filter| if (!try filter.matchesJson(temporary, key, root)) return .@"continue";
                if (session.conditions.len != 0) {
                    const condition_row = try session.condition_projection.projectValue(temporary, key, 0, root);
                    for (session.conditions) |condition| {
                        const cell = try condition_row.cell(condition.column);
                        const rhs = condition.value orelse .null;
                        const matched = switch (condition.op) {
                            .is_null => cell.sql_null,
                            .is_not_null => !cell.sql_null,
                            else => blk: {
                                if (cell.sql_null or rhs == .null) break :blk false;
                                const order = try scalar.compare(cell.value, rhs);
                                break :blk switch (condition.op) {
                                    .eq => order == .eq,
                                    .ne => order != .eq,
                                    .lt => order == .lt,
                                    .lte => order != .gt,
                                    .gt => order == .gt,
                                    .gte => order != .lt,
                                    else => unreachable,
                                };
                            },
                        };
                        if (!matched) return .@"continue";
                    }
                }
                const charge = bytes.len +| key.len +| session.projection.columns.len *| 128 +| @sizeOf(View.Row);
                if (charge > 16 * 1024 * 1024) return error.RelationalRowResultTooLarge;
                if (charge > 16 * 1024 * 1024 - ctx.bytes) {
                    try session.after.resize(session.alloc, ctx.previous_after.len);
                    @memcpy(session.after.items, ctx.previous_after);
                    ctx.more = true;
                    return .stop;
                }
                const projected = try session.projection.projectValue(ctx.owned, key, version, root);
                const digest: ?[32]u8 = if (session.include_primary_digest) blk: {
                    var result: [32]u8 = undefined;
                    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
                    break :blk result;
                } else null;
                ctx.bytes += charge;
                try ctx.rows.append(ctx.owned, .{ .id = projected.id, .version = projected.version, .schema_version = session.table.schema_version, .value = projected.value, .sql_nulls = projected.sql_nulls, .expected_content_digest = digest, .document = if (session.include_document) try @import("../typed_json.zig").clone(ctx.owned, root) else null });
                return .@"continue";
            }
        };
        var context = Context{ .session = self, .owned = owned, .scratch = &scratch, .limit = limit, .begin = time.monotonicNs() };
        try self.store.scanDocumentRowsReadTxnWithContext(&self.txn, lower, self.upper, &context, Context.admit, Context.visit);
        try self.checkpoint();
        self.done = !context.more;
        return .{ .arena = arena, .rows = try context.rows.toOwnedSlice(owned), .after = if (context.more) try owned.dupe(u8, self.after.items) else null };
    }
};
