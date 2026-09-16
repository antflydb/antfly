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

//! A private, immutable routing projection for one unpublished restore cohort.
//! It has no fallback to the live catalog. Normal hosted readers, durable 2PC,
//! identity fences and caches consume this capability unchanged.
const std = @import("std");
const metadata = @import("../metadata/api.zig");
const tables = @import("../metadata/table_manager.zig");
const routing = @import("table_catalog.zig");
const reads = @import("table_reads.zig");
const writes = @import("table_writes.zig");
const Scope = @import("../storage/db/restore_staging_contract.zig").Scope;

pub const Owner = struct { group_id: u64, scope: Scope };
pub const Authority = struct {
    ptr: *anyopaque,
    /// The metadata restore driver verifies the same active job/plan identity
    /// after a linearizable barrier. Cancellation/publication revoke this view.
    verify: *const fn (*anyopaque, [16]u8, [32]u8) anyerror!bool,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    const VTable = struct { verify: *const fn (*anyopaque, [16]u8, [32]u8) anyerror!bool };
    const BoundaryAbi = @import("../runtime_callback_abi.zig").Boundary(VTable);

    fn permits(self: @This(), id: [16]u8, digest: [32]u8) !bool {
        return BoundaryAbi.call("verify", self.boundary_dispatch, self.verify, .{ self.ptr, id, digest });
    }
};

pub const Catalog = struct {
    alloc: std.mem.Allocator,
    snapshot: metadata.AdminSnapshot,
    owners: []const Owner,
    plan_id: [16]u8,
    plan_digest: [32]u8,
    authority: Authority,
    io: ?@import("../runtime_io_abi.zig").Borrow = null,

    /// All snapshot/owner slices are immutable and borrowed for this Catalog's
    /// lifetime. The driver owns their arena through completion of a page/2PC.
    pub fn init(alloc: std.mem.Allocator, snapshot: metadata.AdminSnapshot, owners: []const Owner, authority: Authority) !Catalog {
        if (snapshot.tables.len == 0 or snapshot.tables.len > 128 or snapshot.ranges.len == 0 or snapshot.ranges.len > 4096 or owners.len != snapshot.ranges.len or snapshot.status.metadata_incarnation == null) return error.InvalidRestoreStagingCommand;
        const first = owners[0].scope;
        for (owners, 0..) |owner, index| {
            try owner.scope.validate();
            if (!std.mem.eql(u8, &first.plan_id, &owner.scope.plan_id) or !std.mem.eql(u8, &first.plan_digest, &owner.scope.plan_digest)) return error.RestoreStagingScopeChanged;
            for (owners[0..index]) |previous| if (previous.group_id == owner.group_id) return error.InvalidRestoreStagingCommand;
            const range = for (snapshot.ranges) |range| {
                if (range.group_id == owner.group_id) break range;
            } else return error.RestoreStagingScopeChanged;
            if (range.table_id != owner.scope.target_namespace.table_id or range.group_id != owner.scope.target_namespace.shard_id or
                range.range_id != owner.scope.target_namespace.range_id or range.restore_backup_id.len != 0) return error.RestoreStagingScopeChanged;
            const present = for (snapshot.tables) |table| {
                if (table.table_id == range.table_id) break true;
            } else false;
            if (!present) return error.RestoreStagingScopeChanged;
        }
        return .{ .alloc = alloc, .snapshot = snapshot, .owners = owners, .plan_id = first.plan_id, .plan_digest = first.plan_digest, .authority = authority };
    }

    pub fn source(self: *Catalog) routing.CatalogSource {
        return .{ .ptr = self, .io = self.io, .vtable = &.{
            .admin_snapshot = adminSnapshot,
            .free_admin_snapshot = freeAdminSnapshot,
            .catalog_identity = catalogIdentity,
            .routing_snapshot = routingSnapshot,
            .linearizable_routing_snapshot = routingSnapshot,
            .free_routing_snapshot = freeRoutingSnapshot,
            .restore_scope_for_group = restoreScopeForGroup,
            .restore_plan_for_group = restorePlanForGroup,
            .requires_linearizable_publication_fence = true,
            .validate_publication = validatePublication,
            .validate_table_publication = validateTablePublication,
        } };
    }
    fn cast(ptr: *anyopaque) *Catalog {
        return @ptrCast(@alignCast(ptr));
    }
    fn verify(self: *Catalog) !void {
        if (!try self.authority.permits(self.plan_id, self.plan_digest)) return error.RestoreStagingScopeChanged;
    }
    fn adminSnapshot(ptr: *anyopaque) !metadata.AdminSnapshot {
        const self = cast(ptr);
        try self.verify();
        return self.snapshot;
    }
    fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata.AdminSnapshot) void {
        snapshot.* = undefined;
    }
    fn catalogIdentity(ptr: *anyopaque) !metadata.CatalogIdentity {
        const self = cast(ptr);
        try self.verify();
        return .{ .metadata_group_id = self.snapshot.status.metadata_group_id, .metadata_incarnation = self.snapshot.status.metadata_incarnation.? };
    }
    fn routingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata.CatalogRoutingSnapshot {
        const self = cast(ptr);
        try self.source().budget(deadline_ns).checkpoint();
        try self.verify();
        return .{ .metadata_group_id = self.snapshot.status.metadata_group_id, .metadata_incarnation = self.snapshot.status.metadata_incarnation, .catalog_revision = self.snapshot.status.metadata_epoch, .tables = self.snapshot.tables, .ranges = self.snapshot.ranges, .change_token = .{ .metadata_group_id = self.snapshot.status.metadata_group_id, .metadata_incarnation = self.snapshot.status.metadata_incarnation, .revision = self.snapshot.status.metadata_epoch } };
    }
    fn freeRoutingSnapshot(_: *anyopaque, snapshot: *metadata.CatalogRoutingSnapshot) void {
        snapshot.* = undefined;
    }
    fn restorePlanForGroup(ptr: *anyopaque, name: []const u8, group_id: u64) !?[16]u8 {
        _ = try restoreScopeForGroup(ptr, name, group_id);
        return cast(ptr).plan_id;
    }

    fn restoreScopeForGroup(ptr: *anyopaque, name: []const u8, group_id: u64) !?[32]u8 {
        const self = cast(ptr);
        try self.verify();
        const table_id = for (self.snapshot.tables) |table| {
            if (std.mem.eql(u8, table.name, name)) break table.table_id;
        } else return error.RestoreStagingScopeChanged;
        for (self.owners) |owner| if (owner.group_id == group_id) {
            if (owner.scope.target_namespace.table_id != table_id) return error.RestoreStagingScopeChanged;
            return owner.scope.digest();
        };
        return error.RestoreStagingScopeChanged;
    }
    fn validatePublication(ptr: *anyopaque, contract: metadata.CatalogPublicationContract) !bool {
        const self = cast(ptr);
        try self.verify();
        return contract.matches(&self.snapshot);
    }
    fn validateTablePublication(ptr: *anyopaque, contract: metadata.CatalogTablePublicationContract) !bool {
        const self = cast(ptr);
        try self.verify();
        return contract.matches(&self.snapshot);
    }
};

pub const Sources = struct {
    reader: reads.HostedProvisionedTableReadSource,
    writer: writes.HostedProvisionedTableWriteSource,
    pub fn bind(self: *Sources, catalog: *Catalog, reader: reads.HostedProvisionedTableReadSource, writer: writes.HostedProvisionedTableWriteSource) void {
        self.reader = reader;
        self.writer = writer;
        self.reader.catalog = catalog.source();
        self.writer.catalog = catalog.source();
        // The local adapter recognizes only an exact native staging scope;
        // it leases the already provisioned hidden owner without public lookup.
    }
};

pub const ValidationCursor = struct {
    phase: enum { unique, foreign_key, complete } = .unique,
    owner_index: u32 = 0,
};

/// Metadata's existing restore scheduler owns this adapter; it is not a new
/// queue or a public query route. Templates are lightweight hosted sources.
pub const ValidationPort = struct {
    status: @import("http_server.zig").StatusSource,
    /// Internal embedding boundary. Implementations borrow the exact private
    /// catalog for the session lifetime and must not fall back to live names.
    factory: ?SourceFactory = null,

    pub const SourcePair = struct {
        reader: @import("table_read_source.zig").TableReadSource,
        writer: @import("table_write_source.zig").TableWriteSource,
        owner: ?*anyopaque = null,
        release: ?*const fn (*anyopaque) void = null,
        pub fn deinit(self: *@This()) void {
            if (self.release) |callback| callback(self.owner.?);
            self.* = undefined;
        }
    };
    pub const SourceFactory = struct {
        ptr: *anyopaque,
        secondary: ?*anyopaque = null,
        bind: *const fn (*anyopaque, ?*anyopaque, *Catalog) anyerror!SourcePair,
        boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

        const VTable = struct { bind: *const fn (*anyopaque, ?*anyopaque, *Catalog) anyerror!SourcePair };
        const BoundaryAbi = @import("../runtime_callback_abi.zig").Boundary(VTable);

        fn bindSources(self: @This(), catalog: *Catalog) !SourcePair {
            return BoundaryAbi.call("bind", self.boundary_dispatch, self.bind, .{ self.ptr, self.secondary, catalog });
        }

        pub fn hosted(reader: *reads.HostedProvisionedTableReadSource, writer: *writes.HostedProvisionedTableWriteSource) @This() {
            return .{ .ptr = reader, .secondary = writer, .bind = bindHosted };
        }

        pub fn local(reader: *reads.ProvisionedTableReadSource, writer: *writes.ProvisionedTableWriteSource) @This() {
            return .{ .ptr = reader, .secondary = writer, .bind = bindLocal };
        }

        fn bindHosted(ptr: *anyopaque, secondary: ?*anyopaque, catalog: *Catalog) !SourcePair {
            const Owned = struct {
                alloc: std.mem.Allocator,
                sources: Sources,
                fn release(raw: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.alloc.destroy(self);
                }
            };
            const reader: *reads.HostedProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
            const writer: *writes.HostedProvisionedTableWriteSource = @ptrCast(@alignCast(secondary.?));
            const owned = try catalog.alloc.create(Owned);
            owned.alloc = catalog.alloc;
            catalog.io = reader.catalog.io;
            owned.sources.bind(catalog, reader.*, writer.*);
            return .{ .reader = owned.sources.reader.source(), .writer = owned.sources.writer.source(), .owner = owned, .release = Owned.release };
        }

        fn bindLocal(ptr: *anyopaque, secondary: ?*anyopaque, catalog: *Catalog) !SourcePair {
            const Owned = struct {
                alloc: std.mem.Allocator,
                reader: reads.ProvisionedTableReadSource,
                writer: writes.ProvisionedTableWriteSource,
                fn release(raw: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.alloc.destroy(self);
                }
            };
            const reader: *reads.ProvisionedTableReadSource = @ptrCast(@alignCast(ptr));
            const writer: *writes.ProvisionedTableWriteSource = @ptrCast(@alignCast(secondary.?));
            const owned = try catalog.alloc.create(Owned);
            owned.* = .{ .alloc = catalog.alloc, .reader = reader.*, .writer = writer.* };
            catalog.io = reader.catalog.io;
            owned.reader.catalog = catalog.source();
            owned.writer.catalog = catalog.source();
            return .{ .reader = owned.reader.source(), .writer = owned.writer.source(), .owner = owned, .release = Owned.release };
        }
    };

    pub fn validate(self: @This(), alloc: std.mem.Allocator, job: @import("../metadata/restore_staging.zig").Job, cursor: *ValidationCursor, request: @import("operation.zig").RequestContext) !bool {
        const session = try self.prepare(alloc, job, request);
        defer session.deinit();
        return session.step(alloc, cursor, request);
    }

    /// Retain the immutable plan/routing projection for one bounded scheduler
    /// slice. Authority is checked independently before every page.
    pub fn prepare(self: @This(), alloc: std.mem.Allocator, job: @import("../metadata/restore_staging.zig").Job, request: @import("operation.zig").RequestContext) !*ValidationSession {
        try request.ensureActive();
        const staging = @import("../metadata/restore_staging.zig");
        const current = (try self.status.getRestoreStagingProgress(alloc, job.plan.id, request)) orelse return error.RestoreStagingScopeChanged;
        if (current.state != .importing and current.state != .validating) return error.RestoreStagingScopeChanged;
        var live = (try self.status.linearizableSnapshot(request)) orelse return error.CatalogRoutingUnavailable;
        errdefer self.status.freeAdminSnapshot(&live);
        const session = try alloc.create(ValidationSession);
        errdefer alloc.destroy(session);
        session.* = .{ .alloc = alloc, .port = self, .arena = std.heap.ArenaAllocator.init(alloc), .live = live, .catalog = undefined, .request = request };
        errdefer session.arena.deinit();
        const owned = session.arena.allocator();
        // Own the plan bytes; the driver's next metadata update may replace its
        // previous Job even while this slice retains the routing projection.
        const plan_bytes = try std.json.Stringify.valueAlloc(owned, job.plan, .{});
        const pinned = try std.json.parseFromSlice(staging.Plan, owned, plan_bytes, .{ .allocate = .alloc_always });
        var private_tables: std.ArrayList(tables.TableRecord) = .empty;
        var private_ranges: std.ArrayList(tables.RangeRecord) = .empty;
        var owners: std.ArrayList(Owner) = .empty;
        for (pinned.value.targets) |target| {
            var table = target.table;
            table.restore_backup_id = "";
            try private_tables.append(owned, table);
            for (target.ranges) |target_range| {
                var range = target_range;
                range.restore_backup_id = "";
                try private_ranges.append(owned, range);
                try owners.append(owned, .{ .group_id = range.group_id, .scope = try staging.ownerScope(owned, pinned.value, job.plan_digest, target, target_range) });
            }
        }
        var snapshot = live;
        snapshot.tables = private_tables.items;
        snapshot.ranges = private_ranges.items;
        session.catalog = try Catalog.init(alloc, snapshot, owners.items, .{ .ptr = session, .verify = ValidationSession.verify });
        const factory = self.factory orelse return error.RestoreValidationPending;
        session.bound = try factory.bindSources(&session.catalog);
        return session;
    }
};

pub const ValidationSession = struct {
    alloc: std.mem.Allocator,
    port: ValidationPort,
    arena: std.heap.ArenaAllocator,
    live: metadata.AdminSnapshot,
    catalog: Catalog,
    bound: ValidationPort.SourcePair = undefined,
    request: @import("operation.zig").RequestContext,

    pub fn deinit(self: *@This()) void {
        self.bound.deinit();
        self.port.status.freeAdminSnapshot(&self.live);
        self.arena.deinit();
        self.alloc.destroy(self);
    }

    fn verify(ptr: *anyopaque, _: [16]u8, _: [32]u8) !bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try self.request.ensureActive();
        return true;
    }

    pub fn step(self: *@This(), alloc: std.mem.Allocator, cursor: *ValidationCursor, request: @import("operation.zig").RequestContext) !bool {
        try request.ensureActive();
        self.request = request;
        const current = (try self.port.status.getRestoreStagingProgress(alloc, self.catalog.plan_id, request)) orelse return error.RestoreStagingScopeChanged;
        if (current.state != .importing and current.state != .validating) return error.RestoreStagingScopeChanged;
        return validateSlice(alloc, &self.catalog, self.bound.reader, self.bound.writer, cursor);
    }
};

/// One bounded native activation page, through the same typed planner and 2PC
/// used for ordinary writes. The caller persists this small scheduling cursor
/// in its existing restore job; authoritative progress remains owner-local.
/// No owner receipt may be frozen until this global barrier returns true.
pub fn validateSlice(alloc: std.mem.Allocator, catalog: *Catalog, reader: reads.TableReadSource, writer: writes.TableWriteSource, cursor: *ValidationCursor) !bool {
    try catalog.verify();
    if (cursor.phase == .complete) return true;
    if (cursor.owner_index > catalog.snapshot.ranges.len) return error.InvalidRestoreStagingCommand;
    if (cursor.owner_index == catalog.snapshot.ranges.len) {
        cursor.owner_index = 0;
        cursor.phase = if (cursor.phase == .unique) .foreign_key else .complete;
        return cursor.phase == .complete;
    }
    const owner = catalog.snapshot.ranges[cursor.owner_index];
    const table = for (catalog.snapshot.tables) |table| {
        if (table.table_id == owner.table_id) break table;
    } else return error.RestoreStagingScopeChanged;
    // Unconstrained document and typed tables have no distributed activation
    // work. Their physical/index readiness still belongs to the per-owner
    // validation barrier; do not require an unrelated routed read-index here.
    if (!try @import("relational_integrity_commit.zig").requiresActivation(alloc, table.schema_json)) {
        cursor.owner_index += 1;
        return false;
    }
    var status = (try reader.integrityActivation(alloc, table.name, owner.start_key, "{\"mode\":\"status\"}")) orelse return error.IntegrityCatalogUnavailable;
    defer status.deinit(alloc);
    const State = @import("../storage/db/relational_integrity_activation_contract.zig").State;
    var parsed = try std.json.parseFromSlice(struct { state: State, unique_covered: bool, failure: []const u8 = "" }, alloc, status.json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    // Unlike live activation, all rows of this hidden cohort have finished
    // import and cannot acquire a new MATCH PARTIAL witness from client writes.
    // A retryable live diagnostic therefore rejects this immutable restore.
    if (parsed.value.state == .invalid or parsed.value.failure.len != 0) return error.ConstraintActivationFailed;
    if ((cursor.phase == .unique and parsed.value.unique_covered) or parsed.value.state == .enforced) {
        cursor.owner_index += 1;
        return false;
    }
    _ = try @import("relational_activation_worker.zig").runPage(alloc, reader, writer, catalog.snapshot.tables, catalog.snapshot.ranges, owner);
    return false;
}

test "distributed txn staged mixed restore rebuilds fresh FK claims with durable 2PC and hides invalid cohorts" {
    const db_mod = @import("../storage/db/db.zig");
    const types = @import("../storage/db/types.zig");
    const native = @import("../storage/db/restore_staging_contract.zig");
    const activation = @import("../storage/db/relational_integrity_activation_contract.zig");
    const distributed = @import("distributed_txn.zig");
    const contract = @import("distributed_txn_contract.zig");
    const read_gate = @import("../raft/read_gate.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const row_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const parent_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":0}],"unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const child_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"parent_fk","child_columns":["id"],"parent_table":"parent","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    for ([_]bool{ false, true }, 0..) |invalid, trial| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const owned = arena.allocator();
        var dbs: [3]*db_mod.DB = undefined;
        var opened: usize = 0;
        defer for (dbs[0..opened]) |db| {
            db.close();
            alloc.destroy(db);
        };
        var scopes: [3]Owner = undefined;
        var table_records: [3]tables.TableRecord = undefined;
        var range_records: [3]tables.RangeRecord = undefined;
        for ([_][]const u8{ "parent", "child", "docs" }, 0..) |name, index| {
            const source_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/source-{d}-{d}", .{ tmp.sub_path, trial, index });
            const target_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/target-{d}-{d}", .{ tmp.sub_path, trial, index });
            const source_namespace: @import("../storage/db/doc_identity.zig").Namespace = .{ .table_id = 10 + index, .shard_id = 20 + index, .range_id = 20 + index };
            const target_namespace: @import("../storage/db/doc_identity.zig").Namespace = .{ .table_id = 100 + index, .shard_id = 200 + index, .range_id = 200 + index };
            var options: db_mod.OpenOptions = .{ .identity_namespace = source_namespace, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
            {
                var source = try db_mod.DB.open(alloc, source_path, options);
                defer source.close();
                try source.setSchemaJson(alloc, if (index == 2) "{}" else row_schema);
                try source.batch(.{ .timestamp_ns = 123, .writes = &.{.{ .key = "row", .value = if (invalid and index == 1) "{\"id\":2}" else "{\"id\":1}" }} });
            }
            options.open_mode = .query_readonly;
            var source = try db_mod.DB.open(alloc, source_path, options);
            defer source.close();
            options.open_mode = .writer;
            options.identity_namespace = target_namespace;
            const schema_json = switch (index) {
                0 => parent_schema,
                1 => child_schema,
                else => "{}",
            };
            const target = try alloc.create(db_mod.DB);
            target.* = db_mod.DB.open(alloc, target_path, options) catch |err| {
                alloc.destroy(target);
                return err;
            };
            dbs[index] = target;
            opened += 1;
            try target.setSchemaJson(alloc, schema_json);
            const schema_bytes = try @import("../storage/schema.zig").serializeSchema(owned, target.core.schema orelse .{});
            const scope: Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(@intCast(index + 3)), .source_namespace = source_namespace, .target_namespace = target_namespace, .target_schema_digest = native.digest(schema_bytes) };
            scopes[index] = .{ .group_id = target_namespace.shard_id, .scope = scope };
            try target.reserveRestoreStaging(alloc, scope.plan_id, scope.plan_digest, target_namespace);
            try target.beginRestoreStaging(alloc, scope);
            for (0..20) |_| {
                if (try target.importRestoreStagingPage(alloc, scope, &source, 1, .none) == .imported) break;
            } else return error.RestoreDidNotConverge;
            target.close();
            target.* = try db_mod.DB.open(alloc, target_path, options);
            try std.testing.expectError(error.RestoreStagingInProgress, target.lookup(alloc, "row", .{}));
            table_records[index] = .{ .table_id = target_namespace.table_id, .name = name, .placement_role = "data", .schema_json = schema_json };
            range_records[index] = .{ .group_id = target_namespace.shard_id, .range_id = target_namespace.range_id, .table_id = target_namespace.table_id, .start_key = "", .doc_identity_shard_id = target_namespace.shard_id, .doc_identity_range_id = target_namespace.range_id };
        }
        const Fixture = struct {
            dbs: [3]*db_mod.DB,
            catalog: *Catalog,
            sequence: u8 = 0,
            active: bool = true,
            fn verify(ptr: *anyopaque, _: [16]u8, _: [32]u8) !bool {
                return @as(*@This(), @ptrCast(@alignCast(ptr))).active;
            }
            fn index(self: *@This(), name: []const u8) !usize {
                for (self.catalog.snapshot.tables, 0..) |table, i| if (std.mem.eql(u8, table.name, name)) return i;
                return error.TableNotFound;
            }
            fn lookup(ptr: *anyopaque, allocator: std.mem.Allocator, name: []const u8, key: []const u8, opts: types.LookupOptions, consistency: read_gate.ReadConsistency) !?reads.LookupResponse {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try std.testing.expectEqual(read_gate.ReadConsistency.read_index, consistency);
                const i = try self.index(name);
                if (i == 2) return error.UnexpectedUnconstrainedActivationRead;
                var scoped = opts;
                scoped.restore_staging_scope = try self.catalog.source().restoreScopeForGroup(name, 200 + i);
                const row = (try self.dbs[i].lookup(allocator, key, scoped)) orelse return null;
                return .{ .json = row.json, .version = try self.dbs[i].getTimestamp(allocator, key) };
            }
            fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: types.ScanOptions, _: read_gate.ReadConsistency) !?reads.ScanResponse {
                return error.UnexpectedCall;
            }
            fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: types.SearchRequest, _: read_gate.ReadConsistency) !?@import("query_response.zig").QueryResponse {
                return error.UnexpectedCall;
            }
            fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: types.BatchRequest) !?void {
                return error.UnexpectedCall;
            }
            fn begin(ptr: *anyopaque, allocator: std.mem.Allocator, group: u64, name: []const u8, req: distributed.TxnBeginRequest) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                const participant = try distributed.participantIdForGroupScoped(allocator, name, group, req.restore_staging_scope, req.restore_staging_plan_id);
                defer allocator.free(participant);
                _ = try self.dbs[try self.index(name)].beginTransactionScoped(req.txn_id, req.begin_timestamp, req.begin_timestamp, req.participants, std.mem.eql(u8, participant, req.participants[0]), false, req.restore_staging_scope);
            }
            fn prepare(ptr: *anyopaque, _: std.mem.Allocator, _: u64, name: []const u8, req: distributed.TxnPrepareRequest) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try self.dbs[try self.index(name)].writeTransaction(req.txn_id, req.req);
            }
            fn resolve(ptr: *anyopaque, _: std.mem.Allocator, _: u64, name: []const u8, req: distributed.TxnResolveRequest) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try self.dbs[try self.index(name)].resolveTransactionIntents(req.txn_id, req.status, req.commit_version);
            }
            fn status(ptr: *anyopaque, _: std.mem.Allocator, _: u64, name: []const u8, id: types.TxnId) !types.TxnStatus {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                return self.dbs[try self.index(name)].getTransactionStatus(id);
            }
            fn commit(ptr: *anyopaque, allocator: std.mem.Allocator, requests: []const contract.TableCommitRequest, sync: types.SyncLevel, cancellation: types.CancellationToken) !?contract.CommitOutcome {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try cancellation.check();
                self.sequence += 1;
                return try distributed.executeMultiTableCommit(allocator, self.catalog.source(), .{ .ptr = self, .vtable = &.{ .begin_group = begin, .prepare_group = prepare, .resolve_group = resolve, .status_group = status } }, @splat(self.sequence), @as(u64, self.sequence) * 1000, @as(u64, self.sequence) * 1000 + 1, requests, sync, null);
            }
        };
        var private_catalog: Catalog = undefined;
        var fixture: Fixture = .{ .dbs = dbs, .catalog = &private_catalog };
        private_catalog = try Catalog.init(alloc, .{ .status = .{ .metadata_group_id = 1, .metadata_incarnation = @splat(1), .metrics = .{} }, .tables = &table_records, .ranges = &range_records, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} }, &scopes, .{ .ptr = &fixture, .verify = Fixture.verify });
        const reader: reads.TableReadSource = .{ .ptr = &fixture, .vtable = &.{ .lookup = Fixture.lookup, .scan = Fixture.scan, .query = Fixture.query } };
        const writer: writes.TableWriteSource = .{ .ptr = &fixture, .vtable = &.{ .batch = Fixture.batch, .commit_batch_with_cancellation = Fixture.commit } };
        var validation: ValidationCursor = .{};
        for (0..80) |_| {
            const complete = validateSlice(alloc, &private_catalog, reader, writer, &validation) catch |err| {
                if (invalid and err == error.ConstraintActivationFailed) break;
                return err;
            };
            if (complete) break;
        } else return error.ActivationDidNotConverge;
        if (invalid) {
            const bytes = (try dbs[1].core.getStoreValue(owned, activation.key)).?;
            const progress = try activation.Progress.decode(bytes);
            try std.testing.expectEqual(activation.State.invalid, progress.state);
            try std.testing.expectEqualStrings("foreign_key_parent_missing", progress.failure);
            try std.testing.expectError(error.ConstraintActivationFailed, dbs[1].finishRestoreStaging(alloc, scopes[1].scope.digest(), .validated));
            for (dbs, scopes) |db, owner| _ = try db.finishRestoreStaging(alloc, owner.scope.digest(), .canceled);
            for (dbs) |db| try std.testing.expectError(error.RestoreStagingCanceled, db.lookup(alloc, "row", .{}));
        } else {
            // The existing per-owner restore preparation owns local physical
            // CHECK/index readiness; the distributed barrier above is not a
            // replacement for each replica's verified physical projection.
            for (dbs, scopes) |db, owner| {
                for (0..128) |_| {
                    if (try db.prepareRestoreStagingIndexesStep(alloc, owner.scope.digest())) break;
                } else return error.RestorePreparationDidNotConverge;
            }
            for (dbs, scopes) |db, owner| _ = try db.finishRestoreStaging(alloc, owner.scope.digest(), .validated);
            try std.testing.expectError(error.RestoreStagingScopeChanged, dbs[0].beginTransactionScoped(@splat(99), 999, 999, &.{}, true, false, scopes[0].scope.digest()));
            for (dbs, scopes) |db, owner| _ = try db.finishRestoreStaging(alloc, owner.scope.digest(), .published);
            for (dbs) |db| {
                var row = (try db.lookup(alloc, "row", .{})).?;
                row.deinit(alloc);
            }
        }
        fixture.active = false;
        try std.testing.expectError(error.RestoreStagingScopeChanged, private_catalog.source().restoreScopeForGroup("parent", 200));
    }
}
