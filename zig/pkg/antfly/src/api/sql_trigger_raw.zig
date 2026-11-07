// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Ordinary document/typed-row ingress shares the SQL trigger coordinator.
//! This owner exists only during preparation. The final transaction carries
//! immutable row evidence and a process-long signer, never these callbacks.
const std = @import("std");
const local = @import("antfly_local_sources");
const http = @import("http_server.zig");
const execution = @import("sql_execution.zig");
const triggers = @import("sql_trigger_commit.zig");
const schemaless = @import("sql_graph_schemaless.zig");
const domain = local.system_catalog_domain;
const contract = local.api_distributed_txn_contract;
const types = local.storage_db_types;
const operation = local.api_operation;
const A = std.mem.Allocator;
const Table = local.common_topology_records.TableRecord;

pub const CapturePhase = enum(u5) { resolve, authority, alignment, access, needed_definition, capture_definition, broker_create, capture_table, graph_prepare, prepare_rows, method_create, select_integrity, finish, sign, commit, authorize_integrity, integrity_preparation };
var capture_failure_phases: std.atomic.Value(u32) = .init(0);
pub fn captureFailure(comptime phase: CapturePhase, err: anyerror) anyerror {
    if (comptime @import("builtin").mode == .debug) {
        if (err == error.CatalogGenerationChanged)
            execution.diagnoseCatalogConflictTrace(err, @errorReturnTrace());
        if (err == error.InvalidSqlBackendResponse or err == error.InvalidSqlTriggerManifest or err == error.Forbidden or err == error.CatalogGenerationChanged) {
            const bit = @as(u32, 1) << @backingInt(phase);
            if (capture_failure_phases.fetchOr(bit, .monotonic) & bit == 0)
                std.debug.print("raw preparation capture failed phase={s} class={s}\n", .{ @tagName(phase), @errorName(err) });
        }
    }
    return err;
}

/// Check provenance against the ingress-owned DOM before any statement VM or
/// graph admission. Transport validation cannot infer logical SQL nulls.
pub fn requireNullFieldReferences(value: std.json.Value, fields: []const []const u8) !void {
    if (fields.len == 0) return;
    if (value != .object) return error.InvalidResponse;
    for (fields) |field| if ((value.object.get(field) orelse return error.InvalidResponse) != .null) return error.InvalidResponse;
}

pub const Owner = struct {
    backing: A,
    arena: std.heap.ArenaAllocator,
    identity: ?http.AuthenticatedIdentity = null,
    adapter: execution.Adapter,
    broker: ?*triggers.Broker = null,
    context: operation.RequestContext,
    tables: []const contract.TableCommitRequest = &.{},
    captured: []const ?domain.ResolvedTable = &.{},
    retained_bytes: usize = 0,
    schema_absent: std.AutoHashMapUnmanaged(u64, *schemaless.Owner) = .empty,
    opaque_budget: ?*local.sql_memory_budget = null,
    fn opaqueAllocator(self: *Owner) !A {
        if (self.opaque_budget == null) {
            const region = self.arena.allocator();
            const budget = try region.create(local.sql_memory_budget);
            budget.* = .{ .backing = region, .limit = 64 * 1024 * 1024, .monotonic = true };
            // The control cell lives in the same retained arena as its data;
            // no callback borrows the Raw.Owner after immutable handoff.
            self.opaque_budget = budget;
        }
        return self.opaque_budget.?.allocator();
    }
    fn opaqueFailure(self: *Owner, err: anyerror) anyerror {
        if (err == error.OutOfMemory) if (self.opaque_budget) |budget| if (budget.isExhausted()) return error.SqlWorkingMemoryLimitExceeded;
        return err;
    }
    fn charge(self: *Owner, bytes: usize) !void {
        if (bytes > 16 * 1024 * 1024 -| self.retained_bytes) return error.SqlWorkingMemoryLimitExceeded;
        self.retained_bytes += bytes;
    }

    /// `enabled` is binary capability, not client authority. The native reply
    /// separately attests actual activation174 and its complete incoming cut.
    pub fn create(server: *http.ApiHttpServer, backing: A, request: operation.RequestContext, input: []const contract.TableCommitRequest, enabled: bool) !?*Owner {
        if (!enabled or request.table_triggers != null or request.relational_recovery != .none) return null;
        for (input) |table| if (table.trigger_receipts.len != 0) return null;
        if (input.len == 0) return null;
        if (input.len > 256) return error.SqlProgramLimitExceeded;
        const self = try backing.create(Owner);
        self.* = .{ .backing = backing, .arena = .init(backing), .adapter = undefined, .context = request };
        errdefer self.deinit();
        const a = self.arena.allocator();
        const names = try a.alloc([]const u8, input.len);
        for (input, names) |table, *name| name.* = table.table_name;
        const lookup: domain.ResolveMany = .{ .storage_names = names, .include_storage_bindings = true, .include_query_definitions = true };
        const bytes = server.source.systemCatalog(a, request, .{ .resolve_many = lookup }) catch |err| return captureFailure(.resolve, err);
        const resolved = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{ .allocate = .alloc_always });
        resolved.validateAuthority(a, lookup) catch |err| return captureFailure(.authority, err);
        if (resolved.storage_tables.len != input.len) return captureFailure(.alignment, error.InvalidSqlBackendResponse);
        var needed = false;
        for (resolved.storage_tables, input) |maybe, table| {
            const binding = maybe orelse return error.TableNotFound;
            binding.requireRequestAccess(request.native_service, request.sql_relation_owner) catch |err| return captureFailure(.access, err);
            const graph_names = try graphIndexes(a, table);
            // Before174, retain existing ordinary atomic graph batches. A
            // genuinely coordinated graph request uses the new admission lane.
            const coordinated_graph = input.len != 1 or table.predicates.len != 0 or table.range_guards.len != 0;
            // Preserve ordinary atomic graph batches before174; actual
            // coordinated work shares the native schema-absence proof lane.
            if (graph_names.len != 0 and (binding.sql_trigger_functions_active or coordinated_graph)) needed = true;
            if (!binding.sql_trigger_functions_active) {
                if (binding.sql_incoming_foreign_keys != null) return error.CatalogGenerationChanged;
                continue;
            }
            if (binding.sql_incoming_foreign_keys) |incoming| {
                try incoming.validate();
                needed = needed or incoming.stamp.active_count != 0;
            } else needed = true;
            const definition = binding.query_definition orelse return captureFailure(.needed_definition, error.InvalidSqlBackendResponse);
            if (definition.schema_json.len == 0) continue;
            var schema = try local.schema_mod.parseValidatedTableSchema(a, definition.schema_json);
            defer schema.deinit(a);
            if (schema.sql_triggers) |state| needed = needed or state.value.entries.len != 0;
        }
        if (!needed) {
            self.deinit();
            return null;
        }
        if (request.row_policy_credential) |credential| {
            const identity: *const ?http.AuthenticatedIdentity = @ptrCast(@alignCast(credential));
            self.identity = identity.*;
        }
        if (server.cfg.auth_enabled and self.identity == null) return error.Forbidden;
        self.adapter = .{ .server = server, .identity = &self.identity, .context = request, .revision = resolved.revision };
        self.broker = triggers.Broker.create(a, &self.adapter) catch |err| return captureFailure(.broker_create, err);
        self.captured = resolved.storage_tables;
        // Capture every primary table and routine closure before the first
        // demanded VM operation. Cascades use indexed incoming declarations.
        for (resolved.storage_tables, input) |maybe, table| {
            const binding = maybe.?;
            const definition = binding.query_definition orelse return captureFailure(.capture_definition, error.InvalidSqlBackendResponse);
            const record: Table = .{ .table_id = binding.table_id, .name = binding.name, .schema_json = definition.schema_json };
            const graph_names = try graphIndexes(a, table);
            if (record.schema_json.len == 0) {
                const capsule = schemaless.Owner.create(server, try self.opaqueAllocator(), request, self.identity, record, graph_names, self.adapter.revision, self.broker.?.statement_id, self.broker.?.clock) catch |err| return self.opaqueFailure(err);
                self.schema_absent.put(a, record.table_id, capsule) catch |err| {
                    capsule.deinit();
                    return err;
                };
                try self.charge(capsule.bytes);
                try self.broker.?.rememberSchemaAbsent(capsule.captured);
            } else {
                if (graph_names.len != 0) _ = self.broker.?.ensureGraphPreparation(record, graph_names) catch |err| return captureFailure(.graph_prepare, err);
                _ = self.broker.?.captureTable(record) catch |err| return captureFailure(.capture_table, err);
            }
        }
        self.tables = self.prepareRows(input) catch |err| return captureFailure(.prepare_rows, err);
        self.context = self.broker.?.attachSql(request);
        return self;
    }

    fn graphIndexes(a: A, table: contract.TableCommitRequest) ![]const []const u8 {
        var indexes: std.StringHashMapUnmanaged(void) = .empty;
        defer indexes.deinit(a);
        for (table.graph_writes) |write| try indexes.put(a, write.index_name, {});
        for (table.graph_deletes) |remove| try indexes.put(a, remove.index_name, {});
        for (table.transforms) |transform| for (transform.operations) |op| {
            if (try local.storage_db_transform.graphProjectionPath(op.path)) |path| {
                switch (op.op) {
                    .push, .pull, .add_to_set => {},
                    else => return error.UnsupportedTransformOperation,
                }
                try indexes.put(a, path.index_name, {});
            }
        };
        if (indexes.count() > 64) return error.SqlProgramLimitExceeded;
        const names = try a.alloc([]const u8, indexes.count());
        var iterator = indexes.keyIterator();
        for (names) |*name| name.* = iterator.next().?.*;
        std.mem.sort([]const u8, names, {}, struct {
            fn less(_: void, left: []const u8, right: []const u8) bool {
                return std.mem.lessThan(u8, left, right);
            }
        }.less);
        return names;
    }

    fn prepareRows(self: *Owner, input: []const contract.TableCommitRequest) ![]const contract.TableCommitRequest {
        const a = self.arena.allocator();
        const output = try a.dupe(contract.TableCommitRequest, input);
        const shared = local.storage_db_mutation_coalescing;
        const CapturedReader = struct {
            pub fn isRecoveryExecution(_: @This()) bool {
                return true;
            }
            pub fn get(_: @This(), _: A, _: []const u8) !?[]u8 {
                return error.PreparedReadSetChanged;
            }
        };
        for (output, self.captured) |*table, maybe| {
            const binding = maybe orelse return error.TableNotFound;
            const definition = binding.query_definition orelse return error.InvalidSqlBackendResponse;
            const record: Table = .{ .table_id = binding.table_id, .name = binding.name, .schema_json = definition.schema_json };
            if (record.schema_json.len == 0) {
                const capsule = self.schema_absent.get(record.table_id) orelse return error.CatalogGenerationChanged;
                const before_bytes = capsule.bytes;
                table.* = capsule.prepare(table.*) catch |err| return self.opaqueFailure(err);
                try self.charge(capsule.bytes - before_bytes);
                continue;
            }
            const schema = try self.broker.?.captureTable(record);
            var keys: std.StringHashMapUnmanaged(void) = .empty;
            var replacements: std.StringHashMapUnmanaged(void) = .empty;
            var origins: std.StringHashMapUnmanaged(std.ArrayList(types.TransformOp)) = .empty;
            for (table.writes) |write| {
                try keys.put(a, write.key, {});
                try replacements.put(a, write.key, {});
            }
            for (table.deletes) |key| try keys.put(a, key, {});
            for (table.transforms) |transform| {
                try keys.put(a, transform.key, {});
                const origin = try origins.getOrPut(a, transform.key);
                if (!origin.found_existing) origin.value_ptr.* = .empty;
                try origin.value_ptr.appendSlice(a, transform.operations);
            }
            var observed: std.StringHashMapUnmanaged(?local.api_table_read_source.LookupResponse) = .empty;
            var observed_values: std.StringHashMapUnmanaged(std.json.Value) = .empty;
            var snapshot: shared.TransformReadSnapshot = .{};
            var json_columns: std.ArrayList([]const u8) = .empty;
            if (schema.storage_mode == .relational) for (schema.columns) |column| if (column.type == .json) try json_columns.append(a, column.path);
            snapshot.json_columns = json_columns.items;
            snapshot.entries = try a.alloc(shared.TransformReadSnapshot.Entry, keys.count());
            var iterator = keys.keyIterator();
            var i: usize = 0;
            while (iterator.next()) |key| : (i += 1) {
                const old = try self.captureBefore(record, key.*);
                try observed.put(a, key.*, old);
                if (old) |value| {
                    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, value.json, .{ .parse_numbers = false });
                    try requireNullFieldReferences(parsed, value.json_null_fields);
                    try observed_values.put(a, key.*, parsed);
                }
                snapshot.entries[i] = .{ .key = key.*, .value = if (old) |value| value.json else null, .expected_version = if (old) |value| value.version else 0, .json_null_fields = if (old) |value| value.json_null_fields else &.{} };
                try snapshot.positions.put(a, key.*, i);
            }
            const effective = try shared.coalesceKeyValueRequest(CapturedReader{}, a, types.TransactionWrite, table.writes, table.deletes, table.transforms, &snapshot);
            // Parse every final candidate once before any statement callback;
            // row preparation later mutates this same owned DOM.
            var candidates: std.StringHashMapUnmanaged(std.json.Value) = .empty;
            for (effective.writes) |write| {
                try self.charge(write.value.len);
                const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, write.value, .{ .allocate = .alloc_always, .parse_numbers = false });
                try requireNullFieldReferences(parsed, write.json_null_fields);
                try candidates.put(a, write.key, parsed);
            }
            const graph_names = try graphIndexes(a, table.*);
            const graph_admission = if (graph_names.len != 0) try self.broker.?.ensureGraphPreparation(record, graph_names) else null;
            var effective_owner_keys: std.StringHashMapUnmanaged(void) = .empty;
            for (effective.writes) |write| try effective_owner_keys.put(a, write.key, {});
            for (table.graph_writes) |write| if (!effective_owner_keys.contains(write.producingDocument())) return error.InvalidPreparedGraphEffects;
            for (table.graph_deletes) |remove| if (!effective_owner_keys.contains(remove.producingDocument())) return error.InvalidPreparedGraphEffects;
            var writes: std.ArrayList(types.TransactionWrite) = .empty;
            var deletes: std.ArrayList([]const u8) = .empty;
            var predicates: std.ArrayList(types.TransactionVersionPredicate) = .empty;
            try predicates.appendSlice(a, table.predicates);
            var insert_event = false;
            var update_event = false;
            const delete_event = effective.deletes.len != 0;
            var updated: std.StringHashMapUnmanaged(void) = .empty;
            var provenance: std.StringHashMapUnmanaged(?types.DocumentTransform) = .empty;
            var effective_keys: std.StringHashMapUnmanaged(void) = .empty;
            for (effective.entries) |entry| try effective_keys.put(a, entry.key, {});
            for (effective.writes) |write| {
                const origin: ?types.DocumentTransform = if (!replacements.contains(write.key)) if (origins.get(write.key)) |ops| .{ .key = write.key, .operations = ops.items } else null else null;
                try provenance.put(a, write.key, origin);
                if (observed.get(write.key).? != null) {
                    update_event = true;
                    for (try self.updatedColumns(schema, origin)) |name| try updated.put(a, name, {});
                } else insert_event = true;
            }
            // A demanded missing non-upsert is a zero-row UPDATE statement;
            // its statement triggers retain the original column provenance.
            for (table.transforms) |transform| if (!transform.upsert and !effective_keys.contains(transform.key)) {
                update_event = true;
                for (try self.updatedColumns(schema, transform)) |name| try updated.put(a, name, {});
            };
            const statement_columns = try a.alloc([]const u8, updated.count());
            var updated_iterator = updated.keyIterator();
            for (statement_columns) |*name| name.* = updated_iterator.next().?.*;
            std.mem.sort([]const u8, statement_columns, {}, struct {
                fn less(_: void, x: []const u8, y: []const u8) bool {
                    return std.mem.lessThan(u8, x, y);
                }
            }.less);
            if (insert_event) try self.broker.?.begin(record, .insert, &.{});
            if (update_event) try self.broker.?.begin(record, .update, statement_columns);
            if (delete_event) try self.broker.?.begin(record, .delete, &.{});
            for (effective.writes) |write| {
                try self.row(record, schema, write.key, candidates.getPtr(write.key).?, write.json_null_fields, false, provenance.get(write.key).?, observed.get(write.key).?, observed_values.get(write.key), &writes, &deletes, &predicates);
            }
            for (effective.deletes) |key| try self.row(record, schema, key, null, &.{}, true, null, observed.get(key).?, observed_values.get(key), &writes, &deletes, &predicates);
            const provider = self.broker.?.attach(self.context).table_triggers.?;
            if (insert_event) try provider.end_statement.?(provider.ptr, record, .insert, &.{});
            if (update_event) try provider.end_statement.?(provider.ptr, record, .update, statement_columns);
            if (delete_event) try provider.end_statement.?(provider.ptr, record, .delete, &.{});
            // BEFORE can skip a primary replacement. Its index projections
            // must disappear in the same decision, rather than leak afterward.
            var accepted: std.StringHashMapUnmanaged(void) = .empty;
            for (writes.items) |write| try accepted.put(a, write.key, {});
            var graph_writes: std.ArrayList(types.GraphEdgeWrite) = .empty;
            var graph_deletes: std.ArrayList(types.GraphEdgeDelete) = .empty;
            for (table.graph_writes) |write| if (accepted.contains(write.producingDocument())) {
                try self.charge(write.retainedBytes());
                try graph_writes.append(a, write);
            };
            for (effective.graph_writes.items) |write| if (accepted.contains(write.producingDocument())) {
                try self.charge(write.retainedBytes());
                try graph_writes.append(a, write);
            };
            for (table.graph_deletes) |remove| if (accepted.contains(remove.producingDocument())) {
                try self.charge(remove.retainedBytes());
                try graph_deletes.append(a, remove);
            };
            for (effective.graph_deletes.items) |remove| if (accepted.contains(remove.producingDocument())) {
                try self.charge(remove.retainedBytes());
                try graph_deletes.append(a, remove);
            };
            table.graph_writes = try graph_writes.toOwnedSlice(a);
            table.graph_deletes = try graph_deletes.toOwnedSlice(a);
            // An explicit absent-state rowless receipt still needs the same
            // graph source witness even when every demanded transform is a no-op.
            table.graph_preparation = graph_admission;
            table.writes = try writes.toOwnedSlice(a);
            table.deletes = try deletes.toOwnedSlice(a);
            table.transforms = &.{};
            table.predicates = try predicates.toOwnedSlice(a);
        }
        return output;
    }

    fn captureBefore(self: *Owner, record: Table, key: []const u8) !?local.api_table_read_source.LookupResponse {
        const a = self.arena.allocator();
        try self.context.ensureActive();
        const reader = self.adapter.server.table_reads orelse return error.IntegrityCatalogUnavailable;
        var options: types.LookupOptions = .{ .include_primary_digest = true, .include_json_null_fields = true, .execution_deadline_ns = self.context.deadline_ns, .execution_io = self.context.deadline_io, .cancellation = self.context.cancellation };
        if (self.context.row_policy_integrity_read) |authority| if (try authority.authorize(authority.ptr, a, record.name)) |proof| {
            options.row_policy_principal_proof = proof.token;
            options.row_policy_database = proof.database;
        };
        const old = try reader.lookup(a, record.name, key, options, .read_index);
        if (old) |value| {
            if (value.version == 0 or value.expected_content_digest == null) return error.MissingPrimaryObservation;
            try self.charge(value.json.len);
        }
        return old;
    }

    fn row(self: *Owner, record: Table, schema: local.sql_catalog.Table, key: []const u8, candidate: ?*std.json.Value, flags: []const []const u8, deleting: bool, transform: ?types.DocumentTransform, old: ?local.api_table_read_source.LookupResponse, old_value: ?std.json.Value, writes: *std.ArrayList(types.TransactionWrite), deletes: *std.ArrayList([]const u8), predicates: *std.ArrayList(types.TransactionVersionPredicate)) !void {
        const a = self.arena.allocator();
        try self.context.ensureActive();
        const before_version: u64 = if (old) |value| value.version else 0;
        const before_digest: ?[32]u8 = if (old) |value| value.expected_content_digest else null;
        var found = false;
        for (predicates.items) |*previous| if (std.mem.eql(u8, previous.key, key)) {
            if (previous.expected_version != before_version or (previous.expected_content_digest != null and !std.meta.eql(previous.expected_content_digest, before_digest))) return error.VersionConflict;
            previous.expected_content_digest = before_digest;
            found = true;
            break;
        };
        if (!found) try predicates.append(a, .{ .key = key, .expected_version = before_version, .expected_content_digest = before_digest });
        if (deleting and old == null) return;
        if (!deleting and candidate == null) return;
        const event: @FieldType(local.sql_catalog.trigger_provider.Input, "event") = if (deleting) .delete else if (old == null) .insert else .update;
        const updated = if (event == .update) try self.updatedColumns(schema, transform) else &.{};
        try self.broker.?.begin(record, event, updated);
        const provider = self.broker.?.attach(self.context).table_triggers.?;
        const prepared = try provider.before(provider.ptr, a, .{ .table = record, .key = key, .event = event, .before_parsed = if (old) |value| .{ .value = old_value orelse return error.InvalidSqlBackendResponse, .json_null_fields = value.json_null_fields, .version = value.version, .digest = value.expected_content_digest } else null, .after = candidate, .json_null_fields = flags, .update_columns = updated });
        if (!prepared.skipped) {
            if (candidate) |value| {
                try writes.append(a, .{ .key = key, .value = try std.json.Stringify.valueAlloc(a, value.*, .{}), .json_null_fields = try local.storage_db_types.cloneJsonNullFields(a, prepared.json_null_fields), .prepared_domain = try self.broker.?.preparedDomain(schema.id) });
            } else try deletes.append(a, key);
            try provider.after(provider.ptr, prepared.stage);
        }
    }
    fn updatedColumns(self: *Owner, table: local.sql_catalog.Table, transform: ?types.DocumentTransform) ![]const []const u8 {
        const a = self.arena.allocator();
        var names: std.ArrayList([]const u8) = .empty;
        for (table.columns) |column| {
            if (transform) |value| {
                const selected = for (value.operations) |op| {
                    const path = if (column.document_path.len != 0) column.document_path else &.{column.path};
                    if (try local.storage_db_transform.touchesDeclaredPath(a, op, path)) break true;
                } else false;
                if (!selected) continue;
            }
            try names.append(a, column.name);
        }
        return names.toOwnedSlice(a);
    }
    pub fn finish(self: *Owner, tables: []const contract.TableCommitRequest) ![]const contract.TableCommitRequest {
        const a = self.arena.allocator();
        const broker = self.broker orelse return error.InvalidSqlTriggerManifest;
        var capsules = self.schema_absent.valueIterator();
        while (capsules.next()) |capsule| {
            const table = for (tables) |candidate| {
                if (std.mem.eql(u8, candidate.table_name, capsule.*.captured.record.name)) break candidate;
            } else return error.CatalogGenerationChanged;
            capsule.*.verifyFinal(table) catch |err| return self.opaqueFailure(err);
        }
        try broker.bindPhysicalPredicates(tables);
        try broker.finish();
        const receipts = try broker.receipts(a);
        const output = try a.dupe(contract.TableCommitRequest, tables);
        for (output) |*table| {
            const captured_id = broker.capturedTableId(table.table_name);
            const opaque_receipt = if (captured_id) |id| if (self.schema_absent.get(id)) |capsule| capsule.receipt else null else null;
            // Check the original participant before graph filtering can erase
            // any supplied projection. FK-only parent owners carry no VM event.
            table.trigger_receipts = try selectParticipantReceipts(a, table.*, captured_id, receipts, opaque_receipt);
            // Integrity/FK expansion may have removed a previously selected
            // primary owner. Keep its graph projections in that same outcome.
            if (table.graph_writes.len != 0 or table.graph_deletes.len != 0) {
                var owners: std.StringHashMapUnmanaged(void) = .empty;
                for (table.writes) |write| try owners.put(a, write.key, {});
                var graph_writes: std.ArrayList(types.GraphEdgeWrite) = .empty;
                var graph_deletes: std.ArrayList(types.GraphEdgeDelete) = .empty;
                for (table.graph_writes) |write| if (owners.contains(write.producingDocument())) try graph_writes.append(a, write);
                for (table.graph_deletes) |remove| if (owners.contains(remove.producingDocument())) try graph_deletes.append(a, remove);
                table.graph_writes = try graph_writes.toOwnedSlice(a);
                table.graph_deletes = try graph_deletes.toOwnedSlice(a);
            }
        }
        return @import("sql_trigger_authority.zig").attachMinters(self.adapter.server, a, self.context, output);
    }
    fn clearSchemaAbsent(self: *Owner) void {
        var capsules = self.schema_absent.valueIterator();
        while (capsules.next()) |capsule| capsule.*.deinit();
        self.schema_absent.deinit(self.arena.allocator());
        self.schema_absent = .empty;
    }
    pub fn detach(self: *Owner, prepared: *local.api_relational_integrity_commit.Prepared) void {
        self.broker.?.deinit();
        self.broker = null;
        self.clearSchemaAbsent();
        prepared.retained_arena = self.arena;
        const backing = self.backing;
        backing.destroy(self);
    }
    pub fn deinit(self: *Owner) void {
        if (self.broker) |broker| broker.deinit();
        self.clearSchemaAbsent();
        self.arena.deinit();
        self.backing.destroy(self);
    }
};

/// Transfer only completed, owned data. No borrowed arena allocator or
/// request-local dispatcher callback may outlive this preparation operation.
pub fn finishPreparation(owner: *?*Owner, prepared: *local.api_relational_integrity_commit.Prepared) !void {
    if (owner.*) |raw| {
        if (prepared.retained_arena != null) return error.InvalidSqlTriggerManifest;
        prepared.tables = try raw.finish(prepared.tables);
        raw.detach(prepared);
        owner.* = null;
    }
}

/// This is receipt routing, not effect admission. Native integrity generation,
/// exact row/read predicates and the existing coordinator still authorize the
/// parent-index effects. An uncaptured owner cannot carry any primary, graph,
/// statement-trigger or maintenance work through this path.
fn requireIntegrityOnlyParticipant(table: contract.TableCommitRequest) !void {
    const defaults: contract.TableCommitRequest = .{ .table_name = table.table_name };
    inline for (@typeInfo(contract.TableCommitRequest).@"struct".field_names) |name| {
        const allowed = comptime for (.{ "table_name", "schema_version", "relational_schema_version", "relational_integrity_generation_set", "sql_persistence", "relation_owner_minter", "row_policy_principal_proof", "row_policy_database", "row_policy_admitted_at_seconds", "predicates", "range_guards", "integrity", "integrity_commands" }) |field| {
            if (std.mem.eql(u8, name, field)) break true;
        } else false;
        if (comptime allowed) continue;
        const value = @field(table, name);
        const expected = @field(defaults, name);
        const changed = switch (@typeInfo(@TypeOf(value))) {
            .pointer => |pointer| checked: {
                if (comptime pointer.size != .slice) @compileError("classify new participant effect field " ++ name);
                if (comptime @field(@as(contract.TableCommitRequest, .{ .table_name = "" }), name).len != 0) @compileError("classify nonempty default participant effect field " ++ name);
                break :checked value.len != 0;
            },
            .optional => checked: {
                if (comptime @field(@as(contract.TableCommitRequest, .{ .table_name = "" }), name) != null) @compileError("classify nonnull default participant effect field " ++ name);
                break :checked value != null;
            },
            else => !std.meta.eql(value, expected),
        };
        if (changed) return error.CatalogGenerationChanged;
    }
    if (table.integrity_commands.len == 0 and table.integrity.len == 0 and table.predicates.len == 0 and table.range_guards.len == 0) return error.CatalogGenerationChanged;
}

fn selectParticipantReceipts(a: A, table: contract.TableCommitRequest, captured_id: ?u64, receipts: []const local.sql_catalog.trigger_handoff.Receipt, opaque_receipt: ?local.sql_catalog.trigger_handoff.Receipt) ![]const local.sql_catalog.trigger_handoff.Receipt {
    const id = captured_id orelse {
        try requireIntegrityOnlyParticipant(table);
        if (opaque_receipt != null) return error.CatalogGenerationChanged;
        return &.{};
    };
    var selected: std.ArrayList(local.sql_catalog.trigger_handoff.Receipt) = .empty;
    for (receipts) |receipt| if (receipt.program.table.id == id) try selected.append(a, receipt);
    if (opaque_receipt) |receipt| {
        if (receipt.program.table.id != id) return error.CatalogGenerationChanged;
        try selected.append(a, receipt);
    }
    return selected.toOwnedSlice(a);
}

pub fn checkIntegrityReceiptSelection(alloc: A) !void {
    var region = std.heap.ArenaAllocator.init(alloc);
    defer region.deinit();
    const a = region.allocator();
    const native = local.sql_catalog.trigger_native_admission_tests;
    const fixture = try native.configuredAuthorityFixture(a);
    const receipts = &.{fixture.captured};
    const integrity = local.storage_db_relational_integrity_contract;
    const command: integrity.Command = .{ .address = try integrity.Address.init(@splat(1), "parent_n:10"), .operation = .{ .attach = .{ .child_table = "children", .child_key = "a", .constraint_name = "children_parent_n_fkey", .constraint_generation = @splat(2) } } };
    const parent: contract.TableCommitRequest = .{ .table_name = "parents", .relational_schema_version = 1, .relational_integrity_generation_set = @splat(3), .integrity_commands = &.{command}, .predicates = &.{.{ .key = "p", .expected_version = 4, .expected_content_digest = @splat(5) }} };
    // The child receipt must not be donated to the genuine parent-index
    // participant, including when a changed FK appends a new parent attach.
    try std.testing.expectEqual(@as(usize, 0), (try selectParticipantReceipts(a, parent, null, receipts, null)).len);
    const selected = try selectParticipantReceipts(a, .{ .table_name = "children" }, fixture.captured.program.table.id, receipts, null);
    try std.testing.expectEqual(@as(usize, 1), selected.len);
    try std.testing.expectEqual(fixture.captured.program.table.id, selected[0].program.table.id);
    var bad = parent;
    bad.writes = &.{.{ .key = "a", .value = "{}" }};
    try std.testing.expectError(error.CatalogGenerationChanged, selectParticipantReceipts(a, bad, null, receipts, null));
    bad = parent;
    bad.deletes = &.{"a"};
    try std.testing.expectError(error.CatalogGenerationChanged, selectParticipantReceipts(a, bad, null, receipts, null));
    bad = parent;
    bad.transforms = &.{.{ .key = "a", .operations = &.{} }};
    try std.testing.expectError(error.CatalogGenerationChanged, selectParticipantReceipts(a, bad, null, receipts, null));
    bad = parent;
    bad.graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R" }};
    try std.testing.expectError(error.CatalogGenerationChanged, selectParticipantReceipts(a, bad, null, receipts, null));
    bad = parent;
    bad.graph_deletes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R" }};
    try std.testing.expectError(error.CatalogGenerationChanged, selectParticipantReceipts(a, bad, null, receipts, null));
    bad = parent;
    bad.graph_preparation = .{ .metadata_group_id = 1, .incarnation = @splat(1), .source_revision = 2, .table_id = 99, .schema_digest = @splat(3), .indexes = &.{.{ .name = "g", .digest = @splat(4) }} };
    try std.testing.expectError(error.CatalogGenerationChanged, selectParticipantReceipts(a, bad, null, receipts, null));
    bad = parent;
    bad.trigger_receipts = receipts;
    try std.testing.expectError(error.CatalogGenerationChanged, selectParticipantReceipts(a, bad, null, receipts, null));
    bad = parent;
    bad.relational_repair = true;
    try std.testing.expectError(error.CatalogGenerationChanged, selectParticipantReceipts(a, bad, null, receipts, null));
    bad = parent;
    bad.relational_activation = .{ .routing_key = "p", .expected = null, .next = "next" };
    try std.testing.expectError(error.CatalogGenerationChanged, selectParticipantReceipts(a, bad, null, receipts, null));
    bad = parent;
    bad.restore_staging_scope = @splat(1);
    try std.testing.expectError(error.CatalogGenerationChanged, selectParticipantReceipts(a, bad, null, receipts, null));
    try std.testing.expectError(error.CatalogGenerationChanged, selectParticipantReceipts(a, parent, null, receipts, fixture.captured));
    try std.testing.expectError(error.CatalogGenerationChanged, selectParticipantReceipts(a, .{ .table_name = "uncaptured_zero_row_statement" }, null, receipts, null));
}
