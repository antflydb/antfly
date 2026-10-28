// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Host-owned native table callbacks for the actual compiled API consumer.
//! The immutable local fixture does not certify Raft or process recovery.
const std = @import("std");
const local = @import("antfly_local_sources");
const metadata = @import("../metadata/api.zig");
const catalog_mod = @import("table_catalog.zig");
const system = local.system_catalog_domain;
const reads_mod = @import("table_reads.zig");
const read_contract = local.api_table_read_source;
const owner_mod = @import("kernel_owner_source.zig");
const runtime = @import("antfly_runtime_abi");
const schema_json = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}";

const Catalog = struct {
    records: [1]local.common_topology_records.TableRecord = .{.{ .table_id = 7, .name = "usage_records", .schema_json = schema_json, .indexes_json = "{}" }},
    ranges: [1]local.common_topology_records.RangeRecord = .{.{ .table_id = 7, .group_id = 7001, .range_id = 1, .start_key = "", .end_key = null }},
    fn status(_: *anyopaque) !metadata.MetadataStatus {
        return .{ .metadata_group_id = 1, .metadata_epoch = 7, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .metrics = .{} };
    }
    fn snapshot(raw: *anyopaque) !metadata.AdminSnapshot {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return .{ .status = try status(raw), .nodes = &.{}, .stores = &.{}, .tables = &self.records, .ranges = &self.ranges, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
    }
    fn freeSnapshot(_: *anyopaque, _: *metadata.AdminSnapshot) void {}
    fn linearizable(raw: *anyopaque, context: local.api_operation.RequestContext) !?metadata.AdminSnapshot {
        try context.ensureActive();
        return try snapshot(raw);
    }
    const Adapter = catalog_mod.TestAdminRoutingAdapter(snapshot, freeSnapshot);
    fn routingSnapshot(raw: *anyopaque, deadline: ?u64) !metadata.CatalogRoutingSnapshot {
        var result = try Adapter.routingSnapshot(raw, deadline);
        result.catalog_revision = 7;
        result.change_token.revision = 7;
        return result;
    }
    fn tableSnapshot(raw: *anyopaque, table: []const u8, deadline: ?u64) !metadata.CatalogRoutingSnapshot {
        if (!std.mem.eql(u8, table, "usage_records")) return error.TableNotFound;
        return routingSnapshot(raw, deadline);
    }
    fn validate(raw: *anyopaque, contract: metadata.CatalogPublicationContract) !bool {
        const current = try snapshot(raw);
        return contract.matches(&current);
    }
    fn source(self: *@This()) catalog_mod.CatalogSource {
        return .{ .ptr = self, .vtable = &.{ .admin_snapshot = snapshot, .free_admin_snapshot = freeSnapshot, .routing_snapshot = routingSnapshot, .linearizable_routing_snapshot = routingSnapshot, .table_routing_snapshot = tableSnapshot, .linearizable_table_routing_snapshot = tableSnapshot, .free_routing_snapshot = Adapter.freeRoutingSnapshot, .validate_publication = validate } };
    }
    fn call(raw: *anyopaque, allocator: std.mem.Allocator, context: local.api_operation.RequestContext, input: @import("../system_catalog/server_call.zig").Call) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try context.ensureActive();
        if (input == .policy_publication_status) return allocator.dupe(u8, "null");
        if (input == .setting_snapshot) {
            const principal = context.setting_read_principal orelse return error.Forbidden;
            if (context.setting_admin or !std.mem.eql(u8, principal, input.setting_snapshot.principal) or !std.mem.eql(u8, input.setting_snapshot.database, "default")) return error.Forbidden;
            return std.json.Stringify.valueAlloc(allocator, .{ .scope = input.setting_snapshot, .epoch = @as(u64, 1), .definitions = &.{} }, .{});
        }
        if (input == .read and input.read.kind == .namespace and std.mem.eql(u8, input.read.database, "default") and std.mem.eql(u8, input.read.name orelse "", "public")) return allocator.dupe(u8, "{}");
        if (input != .resolve_many) return error.PgwireFixtureUnexpectedCatalogCall;
        if (input.resolve_many.expected_revision) |revision| if (revision != 7) return error.CatalogGenerationChanged;
        if (input.resolve_many.storage_names.len != 0) {
            if (input.resolve_many.storage_names.len != 1 or !std.mem.eql(u8, input.resolve_many.storage_names[0], "usage_records")) return error.PgwireFixtureUnexpectedCatalogCall;
            const logical = try (system.Target{ .table = "usage_records" }).resourceNameAlloc(allocator);
            defer allocator.free(logical);
            return std.json.Stringify.valueAlloc(allocator, system.ResolvedMany{ .revision = 7, .tables = &.{}, .logical_names = &.{logical} }, .{});
        }
        if (input.resolve_many.targets.len == 0) return std.json.Stringify.valueAlloc(allocator, system.ResolvedMany{ .revision = 7, .tables = &.{} }, .{});
        if (input.resolve_many.targets.len != 1 or !std.mem.eql(u8, input.resolve_many.targets[0].table, "usage_records")) return error.PgwireFixtureUnexpectedCatalogCall;
        const record = self.records[0];
        const tables = [_]?system.ResolvedTable{.{ .table_id = record.table_id, .name = record.name, .query_definition = if (input.resolve_many.include_query_definitions) .{ .table_id = record.table_id, .schema_json = schema_json, .read_schema_json = "", .indexes_json = "{}" } else null }};
        return std.json.Stringify.valueAlloc(allocator, system.ResolvedMany{ .revision = 7, .tables = &tables }, .{});
    }
};

pub const Fixture = struct {
    directory: local.common_test_directory.TestDirectory = undefined,
    catalog: Catalog = .{},
    owners: owner_mod.ProvisionedKernelOwnerSource = undefined,
    reads: reads_mod.ProvisionedTableReadSource = undefined,
    read_vtable: read_contract.TableReadSource.VTable = undefined,
    fault: std.atomic.Value(u8) = .init(0),
    captures: std.atomic.Value(usize) = .init(0),
    closes: std.atomic.Value(usize) = .init(0),
    live_captures: std.atomic.Value(usize) = .init(0),
    pages: std.atomic.Value(usize) = .init(0),
    refusals: std.atomic.Value(usize) = .init(0),
    nested_calls: std.atomic.Value(usize) = .init(0),
    pub const FaultStage = enum(u8) { capture = 1, page = 2, after_page = 3 };
    pub const FaultReason = enum(u8) { memory = 1, canceled = 2, unavailable = 3 };

    pub fn refuseNext(self: *Fixture, stage: FaultStage, reason: FaultReason) void {
        const previous = self.fault.swap((@backingInt(stage) << 4) | @backingInt(reason), .acq_rel);
        std.debug.assert(previous == 0);
    }
    fn takeFault(self: *Fixture, stage: FaultStage) !void {
        const fault = self.fault.load(.acquire);
        if (fault == 0 or fault >> 4 != @backingInt(stage)) return;
        if (self.fault.cmpxchgStrong(fault, 0, .acq_rel, .acquire) != null) return;
        _ = self.refusals.fetchAdd(1, .acq_rel);
        return switch (@as(FaultReason, @fromBackingInt(@intCast(fault & 15)))) {
            .memory => error.OutOfMemory,
            .canceled => error.Canceled,
            .unavailable => error.RemoteReadUnavailable,
        };
    }
    pub var boundary_calls: std.atomic.Value(usize) = .init(0);
    pub fn init(self: *Fixture, allocator: std.mem.Allocator, backend: *local.storage_background_runtime.BackendRuntime) !void {
        boundary_calls.store(0, .release);
        self.directory = try local.common_test_directory.TestDirectory.init("antfly-compiled-api-native-table");
        errdefer self.directory.cleanup();
        const barrier = @import("../raft/mod.zig").read_gate.alreadyReadSafeBarrier();
        self.owners = owner_mod.ProvisionedKernelOwnerSource.init(allocator, self.directory.path(), self.catalog.source(), barrier);
        errdefer self.owners.deinit();
        _ = try self.owners.reconcileTableGroup(7001, "usage_records");
        _ = (try self.owners.writeSource().batchGroupLocal(allocator, 7001, "usage_records", .{ .writes = &.{ .{ .key = "four", .value = "{\"id\":4}" }, .{ .key = "seven", .value = "{\"id\":7}" } }, .timestamp_ns = 42 })) orelse return error.ExpectedNativeWrite;
        self.reads = reads_mod.ProvisionedTableReadSource.init(self.directory.path(), self.catalog.source(), barrier);
        _ = self.reads.withLocalReadSource(self.owners.readSource());
        self.reads.backend_runtime = backend;
    }
    /// Exceed a native scan page so suspension must retain physical read state.
    pub fn addContinuationRows(self: *Fixture, allocator: std.mem.Allocator) !void {
        var keys: [300][16]u8 = undefined;
        var values: [300][32]u8 = undefined;
        var writes: [300]local.storage_db_types.BatchWrite = undefined;
        for (&writes, &keys, &values, 0..) |*write, *key, *value, index| {
            write.* = .{
                .key = try std.fmt.bufPrint(key, "z{d:0>3}", .{index}),
                .value = try std.fmt.bufPrint(value, "{{\"id\":{d}}}", .{index + 100}),
            };
        }
        _ = (try self.owners.writeSource().batchGroupLocal(allocator, 7001, "usage_records", .{ .writes = &writes, .timestamp_ns = 43 })) orelse return error.ExpectedNativeWrite;
    }
    pub fn deinit(self: *Fixture) void {
        std.debug.assert(self.live_captures.load(.acquire) == 0);
        std.debug.assert(self.captures.load(.acquire) == self.closes.load(.acquire));
        self.owners.deinit();
        self.directory.cleanup();
    }
    pub fn statusSource(self: *Fixture) !@import("http_server.zig").StatusSource {
        return .{ .ptr = &self.catalog, .routing = try self.catalog.source().routingSource(), .vtable = &.{ .status = Catalog.status, .system_catalog = Catalog.call, .supports_query_definitions = true, .admin_snapshot = Catalog.snapshot, .linearizable_snapshot = Catalog.linearizable, .free_admin_snapshot = Catalog.freeSnapshot } };
    }
    pub fn readSource(self: *Fixture) read_contract.TableReadSource {
        var result = self.reads.source();
        self.read_vtable = result.vtable.*;
        self.read_vtable.open_relational_statement_with_admission = openStatement;
        if (result.vtable.open_relational_read != null) self.read_vtable.open_relational_read = openRead;
        result.vtable = &self.read_vtable;
        result.boundary_dispatch = countedDispatch;
        return result;
    }
    fn closeNative(self: *Fixture, statement: read_contract.RelationalStatementRead) void {
        statement.deinit();
        _ = self.closes.fetchAdd(1, .acq_rel);
        const previous = self.live_captures.fetchSub(1, .acq_rel);
        std.debug.assert(previous != 0);
    }
    fn openRead(raw: *anyopaque, allocator: std.mem.Allocator, table: []const u8, from: []const u8, to: []const u8, options: local.storage_db_types.ScanOptions, consistency: @import("../raft/mod.zig").ReadConsistency) !?read_contract.RelationalReadView {
        const reads: *reads_mod.ProvisionedTableReadSource = @ptrCast(@alignCast(raw));
        const self: *Fixture = @alignCast(@fieldParentPtr("reads", reads));
        const view = (try self.reads.source().openRelationalRead(allocator, table, from, to, options, consistency)) orelse return null;
        _ = self.captures.fetchAdd(1, .acq_rel);
        _ = self.live_captures.fetchAdd(1, .acq_rel);
        errdefer {
            view.deinit();
            _ = self.closes.fetchAdd(1, .acq_rel);
            const previous = self.live_captures.fetchSub(1, .acq_rel);
            std.debug.assert(previous != 0);
        }
        try self.takeFault(.capture);
        const wrapper = try allocator.create(CheckedView);
        wrapper.* = .{ .fixture = self, .native = view, .owns_native = true, .allocator = allocator };
        wrapper.configure();
        return .{ .ptr = wrapper, .vtable = &wrapper.vtable, .boundary_dispatch = CheckedView.dispatch };
    }
    fn openStatement(raw: *anyopaque, allocator: std.mem.Allocator, scans: []const read_contract.RelationalStatementScan, consistency: @import("../raft/mod.zig").ReadConsistency, control: local.common_read_admission.Port) !read_contract.RelationalStatementRead {
        const reads: *reads_mod.ProvisionedTableReadSource = @ptrCast(@alignCast(raw));
        const self: *Fixture = @alignCast(@fieldParentPtr("reads", reads));
        const statement = try self.reads.source().openRelationalStatementWithAdmission(allocator, scans, consistency, control);
        _ = self.captures.fetchAdd(1, .acq_rel);
        _ = self.live_captures.fetchAdd(1, .acq_rel);
        errdefer self.closeNative(statement);
        try self.takeFault(.capture);
        const owner = try allocator.create(CheckedStatement);
        errdefer allocator.destroy(owner);
        const wrappers = try allocator.alloc(CheckedView, statement.views.len);
        errdefer allocator.free(wrappers);
        const views = try allocator.alloc(read_contract.RelationalReadView, statement.views.len);
        errdefer allocator.free(views);
        for (statement.views, wrappers, views) |view, *wrapper, *projected| {
            wrapper.* = .{ .fixture = self, .native = view };
            wrapper.configure();
            projected.* = .{ .ptr = wrapper, .vtable = &wrapper.vtable, .boundary_dispatch = CheckedView.dispatch };
        }
        owner.* = .{ .fixture = self, .allocator = allocator, .native = statement, .wrappers = wrappers, .views = views };
        owner.vtable = .{
            .close = CheckedStatement.close,
            .range_proofs = if (statement.vtable.range_proofs != null) CheckedStatement.proofs else null,
            .range_proofs_with_admission = if (statement.vtable.range_proofs_with_admission != null) CheckedStatement.proofsWithAdmission else null,
        };
        return .{ .ptr = owner, .views = views, .vtable = &owner.vtable, .boundary_dispatch = CheckedStatement.dispatch };
    }
    const CheckedStatement = struct {
        fixture: *Fixture,
        allocator: std.mem.Allocator,
        native: read_contract.RelationalStatementRead,
        wrappers: []CheckedView,
        views: []read_contract.RelationalReadView,
        vtable: read_contract.RelationalStatementRead.VTable = undefined,
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.fixture.closeNative(self.native);
            self.allocator.free(self.views);
            self.allocator.free(self.wrappers);
            self.allocator.destroy(self);
        }
        fn proofs(raw: *anyopaque, allocator: std.mem.Allocator, index: usize) ![]read_contract.RelationalStatementRead.OwnerRangeProof {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.native.rangeProofs(allocator, index);
        }
        fn proofsWithAdmission(raw: *anyopaque, allocator: std.mem.Allocator, index: usize, control: local.common_read_admission.Port) ![]read_contract.RelationalStatementRead.OwnerRangeProof {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.native.rangeProofsWithAdmission(allocator, index, control);
        }
        fn dispatch(contract: *const runtime.native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) runtime.error_abi.Status {
            const statement: read_contract.RelationalStatementRead = .{ .ptr = undefined, .views = &.{}, .vtable = undefined };
            return statement.boundary_dispatch(contract, callback, args, output);
        }
    };
    const CheckedView = struct {
        fixture: *Fixture,
        native: read_contract.RelationalReadView,
        owns_native: bool = false,
        allocator: std.mem.Allocator = undefined,
        vtable: read_contract.RelationalReadView.VTable = undefined,
        fn configure(self: *@This()) void {
            const original = self.native.vtable;
            self.vtable = .{
                .next = next,
                .next_with_admission = if (original.next_with_admission != null) nextWithAdmission else null,
                .close = if (self.owns_native) closeOwned else borrowedClose,
                .range_proofs = if (original.range_proofs != null) proofs else null,
                .range_proofs_with_admission = if (original.range_proofs_with_admission != null) proofsWithAdmission else null,
                .normalize = if (original.normalize != null) normalize else null,
                .normalize_with_admission = if (original.normalize_with_admission != null) normalizeWithAdmission else null,
            };
        }
        fn next(raw: *anyopaque, allocator: std.mem.Allocator, limit: u32) !read_contract.RelationalReadView.Page {
            return read(@ptrCast(@alignCast(raw)), allocator, limit, null);
        }
        fn nextWithAdmission(raw: *anyopaque, allocator: std.mem.Allocator, limit: u32, control: local.common_read_admission.Port) !read_contract.RelationalReadView.Page {
            return read(@ptrCast(@alignCast(raw)), allocator, limit, control);
        }
        fn read(self: *@This(), allocator: std.mem.Allocator, limit: u32, control: ?local.common_read_admission.Port) !read_contract.RelationalReadView.Page {
            _ = self.fixture.nested_calls.fetchAdd(1, .acq_rel);
            try self.fixture.takeFault(.page);
            var page = if (control) |port| try self.native.nextWithAdmission(allocator, limit, port) else try self.native.next(allocator, limit);
            errdefer page.deinit();
            _ = self.fixture.pages.fetchAdd(1, .acq_rel);
            try self.fixture.takeFault(.after_page);
            return page;
        }
        // Views are borrowed from the collective statement owner, matching
        // SQL's native StatementRead contract. Only that owner closes storage.
        fn borrowedClose(_: *anyopaque) void {}
        fn closeOwned(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.native.deinit();
            _ = self.fixture.closes.fetchAdd(1, .acq_rel);
            const previous = self.fixture.live_captures.fetchSub(1, .acq_rel);
            std.debug.assert(previous != 0);
            self.allocator.destroy(self);
        }
        fn proofs(raw: *anyopaque, allocator: std.mem.Allocator) ![]local.storage_range_protection.Proof {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.native.rangeProofs(allocator);
        }
        fn proofsWithAdmission(raw: *anyopaque, allocator: std.mem.Allocator, control: local.common_read_admission.Port) ![]local.storage_range_protection.Proof {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.native.rangeProofsWithAdmission(allocator, control);
        }
        fn normalize(raw: *anyopaque, allocator: std.mem.Allocator, writes: []const local.storage_db_types.BatchWrite) ![]local.storage_db_types.BatchWrite {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.native.normalize(allocator, writes);
        }
        fn normalizeWithAdmission(raw: *anyopaque, allocator: std.mem.Allocator, writes: []const local.storage_db_types.BatchWrite, control: local.common_read_admission.Port) ![]local.storage_db_types.BatchWrite {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.native.normalizeWithAdmission(allocator, writes, control);
        }
        fn dispatch(contract: *const runtime.native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) runtime.error_abi.Status {
            const view: read_contract.RelationalReadView = .{ .ptr = undefined, .vtable = undefined };
            return view.boundary_dispatch(contract, callback, args, output);
        }
    };
    fn countedDispatch(contract: *const runtime.native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) runtime.error_abi.Status {
        _ = boundary_calls.fetchAdd(1, .acq_rel);
        const source: read_contract.TableReadSource = .{ .ptr = undefined, .vtable = undefined };
        return source.boundary_dispatch(contract, callback, args, output);
    }
};
