// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Preparation-only capsule for genuine schema-free document tables. It uses
//! the normal observation/coalescer/receipt/transaction pipeline; no schema,
//! trigger VM, defaults or additional transaction decision is synthesized.
const std = @import("std");
const local = @import("antfly_local_sources");
const http = @import("http_server.zig");
const execution = @import("sql_execution.zig");
const graph = @import("../system_catalog/graph_preparation.zig");
const snapshot = @import("../system_catalog/trigger_snapshot.zig");
const types = local.storage_db_types;
const contract = local.api_distributed_txn_contract;
const handoff = local.sql_catalog.trigger_handoff;
const manifest = local.sql_catalog.trigger_manifest;
const A = std.mem.Allocator;
const Table = local.common_topology_records.TableRecord;
const Context = local.api_operation.RequestContext;
const max_bytes = 16 * 1024 * 1024;
const CapturedReader = struct {
    pub fn isRecoveryExecution(_: @This()) bool {
        return true;
    }
    pub fn get(_: @This(), _: A, _: []const u8) !?[]u8 {
        return error.PreparedReadSetChanged;
    }
};
pub const Owner = struct {
    a: A,
    server: *http.ApiHttpServer,
    context: Context,
    captured: snapshot.Snapshot,
    statement: [16]u8,
    clock: manifest.Clock,
    names: []const []const u8,
    bytes: usize,
    filter: ?local.search_pattern_filter.PreparedPatternFilter = null,
    receipt: ?handoff.Receipt = null,
    prepared: ?contract.TableCommitRequest = null,

    pub fn create(server: *http.ApiHttpServer, a: A, context: Context, identity: ?http.AuthenticatedIdentity, table: Table, names: []const []const u8, revision: ?u64, statement: [16]u8, clock: manifest.Clock) !*Owner {
        if (table.schema_json.len != 0) return error.CatalogGenerationChanged;
        var private = context;
        private.setting_admin = true;
        private.sql_completion = null;
        private.sql_completion_submission = null;
        private.sql_command_fence = null;
        const encoded = try server.source.systemCatalog(a, private, .{ .sql_trigger_snapshot = .{ .table_id = table.table_id, .expected_revision = revision } });
        if (encoded.len > snapshot.max_snapshot_bytes) return error.SqlWorkingMemoryLimitExceeded;
        const cut = try std.json.parseFromSliceLeaky(snapshot.Snapshot, a, encoded, .{ .allocate = .alloc_always });
        if (cut.record.table_id != table.table_id or !std.mem.eql(u8, cut.record.name, table.name) or cut.record.schema_json.len != 0 or cut.incoming_tables.len != 0 or cut.incoming_records.len != 0 or cut.incoming_stamp.active_count != 0 or cut.source.value != .table) return error.CatalogGenerationChanged;
        try cut.source.value.table.binding.requireRequestAccess(context.native_service, context.sql_relation_owner);
        if ((try server.rowPolicyWriteProofForPurpose(a, context, table.table_id, table.name, cut.source.target.database, null, .sql_write)) != null) return error.RowPolicyCatalogChanged;
        const self = try a.create(Owner);
        self.* = .{ .a = a, .server = server, .context = context, .captured = cut, .statement = statement, .clock = clock, .names = names, .bytes = encoded.len };
        errdefer self.deinit();
        var actor = try http.cloneCatalogIdentity(a, identity);
        defer if (actor) |*value| value.deinit(a);
        if (actor) |*value| {
            const logical = try cut.source.target.resourceNameAlloc(a);
            try http.projectCatalogIdentity(a, value, logical, table.name);
        }
        if (try http.resolveEffectiveRowFilterJson(a, actor, table.name)) |filter| self.filter = try local.search_pattern_filter.PreparedPatternFilter.init(a, filter);
        return self;
    }
    pub fn deinit(self: *Owner) void {
        if (self.filter) |*filter| filter.deinit();
    }
    fn charge(self: *Owner, bytes: usize) !void {
        if (bytes > max_bytes -| self.bytes) return error.SqlWorkingMemoryLimitExceeded;
        self.bytes += bytes;
    }
    fn permits(self: *Owner, key: []const u8, value: []const u8) !void {
        var parsed = try std.json.parseFromSlice(std.json.Value, self.a, value, .{ .parse_numbers = false });
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidBatchRequest;
        if (self.filter) |*filter| if (!try filter.matchesJson(self.a, key, parsed.value)) return error.Forbidden;
    }
    fn admit(self: *Owner) !local.storage_db_prepared_graph_effects.Admission {
        const source = try graph.capture(self.a, self.captured, self.names);
        if (self.captured.metadata_group_id == 0) return error.InvalidSqlBackendResponse;
        if (self.captured.graph_intent_protocol >= 7) return source.admitted(self.captured.metadata_group_id);
        const io = self.server.sharedApiIo() orelse return error.SqlClockUnavailable;
        var nonce: [16]u8 = undefined;
        io.random(&nonce);
        const request: graph.Mutation = .{ .source = source, .admission = .{ .nonce = nonce, .head = self.captured.graph_outcome_head } };
        const observed = try graph.observation(self.a, request);
        var private = self.context;
        private.setting_admin = true;
        private.sql_completion = null;
        private.sql_completion_submission = null;
        private.sql_command_fence = null;
        const bytes = self.server.source.systemCatalog(self.a, private, .{ .sql_graph_preparation_admit = request }) catch |err| {
            if (err != error.MetadataMutationOutcomeUnknown and !std.mem.eql(u8, execution.diagnostic(err).code, "40003")) return err;
            const known = try self.server.source.systemCatalog(self.a, private, .{ .sql_graph_preparation_outcome = observed });
            if (known.len > graph.outcomes.max_record_bytes) return error.InvalidSqlBackendResponse;
            const value = (try std.json.parseFromSliceLeaky(?graph.outcomes.Outcome, self.a, known, .{})) orelse return error.SqlReadRetryRequired;
            _ = try value.result();
            return source.admitted(self.captured.metadata_group_id);
        };
        if (bytes.len > graph.outcomes.max_record_bytes) return error.InvalidSqlBackendResponse;
        _ = try (try std.json.parseFromSliceLeaky(graph.outcomes.Outcome, self.a, bytes, .{})).result();
        return source.admitted(self.captured.metadata_group_id);
    }
    pub fn prepare(self: *Owner, input: contract.TableCommitRequest) !contract.TableCommitRequest {
        if (self.prepared != null or input.relational_schema_version != null or input.schema_version != null) return error.CatalogGenerationChanged;
        try self.context.ensureActive();
        var keys: std.StringHashMapUnmanaged(void) = .empty;
        for (input.writes) |write| {
            if (write.json_null_fields.len != 0 or write.prepared_domain != null) return error.InvalidBatchRequest;
            try keys.put(self.a, write.key, {});
        }
        for (input.deletes) |key| try keys.put(self.a, key, {});
        for (input.transforms) |transform| try keys.put(self.a, transform.key, {});
        var read: local.storage_db_mutation_coalescing.TransformReadSnapshot = .{};
        read.entries = try self.a.alloc(local.storage_db_mutation_coalescing.TransformReadSnapshot.Entry, keys.count());
        var before: std.StringHashMapUnmanaged(?local.api_table_read_source.LookupResponse) = .empty;
        var iterator = keys.keyIterator();
        const reader = self.server.table_reads orelse return error.IntegrityCatalogUnavailable;
        for (read.entries, 0..) |*entry, index| {
            const key = iterator.next().?.*;
            const old = try reader.lookup(self.a, input.table_name, key, .{ .include_primary_digest = true, .execution_deadline_ns = self.context.deadline_ns, .execution_io = self.context.deadline_io, .cancellation = self.context.cancellation }, .read_index);
            if (old) |value| {
                if (value.version == 0 or value.expected_content_digest == null) return error.MissingPrimaryObservation;
                try self.charge(value.json.len);
                try self.permits(key, value.json);
            }
            try before.put(self.a, key, old);
            entry.* = .{ .key = key, .value = if (old) |value| value.json else null, .expected_version = if (old) |value| value.version else 0 };
            try read.positions.put(self.a, key, index);
        }
        const effective = try local.storage_db_mutation_coalescing.coalesceKeyValueRequest(CapturedReader{}, self.a, types.TransactionWrite, input.writes, input.deletes, input.transforms, &read);
        var predicates: std.ArrayList(types.TransactionVersionPredicate) = .empty;
        try predicates.appendSlice(self.a, input.predicates);
        var rows: std.ArrayList(handoff.Row) = .empty;
        var primary_deletes: std.ArrayList([]const u8) = .empty;
        var events: @FieldType(handoff.Header, "events") = .{};
        var owners: std.StringHashMapUnmanaged(void) = .empty;
        inline for (.{ effective.writes, effective.deletes }) |items| for (items) |item| {
            const writing = comptime @TypeOf(item) == types.TransactionWrite;
            const key = if (writing) item.key else item;
            const old = before.get(key).?;
            if (!writing and old == null) continue;
            const version = if (old) |value| value.version else 0;
            const digest = if (old) |value| value.expected_content_digest else null;
            var found = false;
            for (predicates.items) |*predicate| if (std.mem.eql(u8, predicate.key, key)) {
                if (predicate.expected_version != version or (predicate.expected_content_digest != null and !std.meta.eql(predicate.expected_content_digest, digest))) return error.VersionConflict;
                predicate.expected_content_digest = digest;
                found = true;
            };
            if (!found) try predicates.append(self.a, .{ .key = key, .expected_version = version, .expected_content_digest = digest });
            if (!writing) try primary_deletes.append(self.a, key);
            const event: @FieldType(handoff.Row, "event") = if (!writing) .delete else if (old == null) .insert else .update;
            switch (event) {
                .insert => events.insert = true,
                .update => events.update = true,
                .delete => events.delete = true,
                else => unreachable,
            }
            if (writing) {
                try self.permits(key, item.value);
                try self.charge(item.value.len);
                try owners.put(self.a, key, {});
            }
            try rows.append(self.a, .{ .key = key, .event = event, .preimage = if (version == 0) .absent else .{ .present = .{ .version = version, .digest = digest.? } }, .after = if (writing) try handoff.rowDigest(key, item.value, &.{}) else null });
        };
        for (input.transforms) |transform| if (!transform.upsert) {
            events.update = true;
        };
        var writes: std.ArrayList(types.GraphEdgeWrite) = .empty;
        var deletes: std.ArrayList(types.GraphEdgeDelete) = .empty;
        inline for (.{ input.graph_writes, effective.graph_writes.items }) |items| for (items) |write| {
            if (!owners.contains(write.producingDocument())) return error.InvalidPreparedGraphEffects;
            try self.charge(write.retainedBytes());
            try writes.append(self.a, write);
        };
        inline for (.{ input.graph_deletes, effective.graph_deletes.items }) |items| for (items) |remove| {
            if (!owners.contains(remove.producingDocument())) return error.InvalidPreparedGraphEffects;
            try self.charge(remove.retainedBytes());
            try deletes.append(self.a, remove);
        };
        var output = input;
        output.writes = effective.writes;
        output.deletes = try primary_deletes.toOwnedSlice(self.a);
        output.transforms = &.{};
        output.predicates = try predicates.toOwnedSlice(self.a);
        output.graph_writes = try writes.toOwnedSlice(self.a);
        output.graph_deletes = try deletes.toOwnedSlice(self.a);
        if (self.names.len != 0) {
            try @import("sql_trigger_authority.zig").requireConfiguration(self.server.cfg);
            const proof = try self.admit();
            var absent = try manifest.Absence.fromAdmission(proof, 0);
            absent.schema_present = false;
            try absent.requireNoSchema();
            const program: manifest.Manifest = .{ .format = 2, .metadata_incarnation = proof.incarnation, .catalog_revision = self.captured.revision, .table = try absent.table(), .absence = absent, .routines = &.{}, .clock = self.clock };
            std.mem.sort(handoff.Row, rows.items, {}, struct {
                fn less(_: void, left: handoff.Row, right: handoff.Row) bool {
                    return std.mem.lessThan(u8, left.key, right.key);
                }
            }.less);
            const receipt: handoff.Receipt = .{ .program = program, .processed = .{ .header = .{ .statement_id = self.statement, .metadata_incarnation = proof.incarnation, .table = program.table, .program_digest = try program.digest(), .events = events }, .rows = try rows.toOwnedSlice(self.a) } };
            try receipt.validate(self.a);
            self.receipt = receipt;
            output.graph_preparation = proof;
        }
        self.prepared = output;
        return output;
    }
    pub fn verifyFinal(self: *Owner, final: contract.TableCommitRequest) !void {
        const prepared = self.prepared orelse return error.InvalidSqlTriggerManifest;
        if (!std.mem.eql(u8, prepared.table_name, final.table_name) or prepared.writes.len != final.writes.len or prepared.deletes.len != final.deletes.len or final.transforms.len != 0) return error.InvalidSqlTriggerManifest;
        if (self.receipt) |receipt| try handoff.verifyPreparedNoSchema(self.a, &.{receipt}, true, final.writes, final.deletes, final.predicates);
        // Non-graph peers still retain their exact calculated literal images;
        // coordinator expansion may not silently reinterpret a transform.
        var expected: std.StringHashMapUnmanaged([]const u8) = .empty;
        for (prepared.writes) |write| try expected.put(self.a, write.key, write.value);
        for (final.writes) |actual| if (!std.mem.eql(u8, expected.get(actual.key) orelse return error.InvalidSqlTriggerManifest, actual.value)) return error.InvalidSqlTriggerManifest;
        var expected_deletes: std.StringHashMapUnmanaged(void) = .empty;
        for (prepared.deletes) |key| try expected_deletes.put(self.a, key, {});
        for (final.deletes) |key| if (!expected_deletes.contains(key)) return error.InvalidSqlTriggerManifest;
        var guards: std.StringHashMapUnmanaged(types.TransactionVersionPredicate) = .empty;
        for (final.predicates) |guard| {
            if ((try guards.getOrPut(self.a, guard.key)).found_existing) return error.InvalidSqlTriggerManifest;
            try guards.put(self.a, guard.key, guard);
        }
        for (prepared.predicates) |guard| {
            const actual = guards.get(guard.key) orelse return error.InvalidSqlTriggerManifest;
            if (actual.expected_version != guard.expected_version or !std.meta.eql(actual.expected_content_digest, guard.expected_content_digest)) return error.InvalidSqlTriggerManifest;
        }
    }
};
