// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Protocol for a trusted provider maintenance controller. This is an Antfly
//! integration protocol, not an endpoint in the Iceberg REST specification.
//! The controller owns physical deletion and must coordinate the catalog's
//! writers, external readers, and Antfly's shared durable snapshot-pin registry.
const std = @import("std");
const types = @import("types.zig");
const rest = @import("rest.zig");
const storage = @import("objectstore");
const A = std.mem.Allocator;
pub const Provider = enum { nessie, polaris };
pub const Config = struct {
    provider: Provider,
    connection: []const u8,
    uri: []const u8,
    pub fn validate(self: Config) !void {
        if (self.connection.len == 0) return error.InvalidLakeMaintenanceProvider;
        const parsed = std.Uri.parse(self.uri) catch return error.InvalidLakeMaintenanceProvider;
        if ((!std.mem.eql(u8, parsed.scheme, "https") and !std.mem.eql(u8, parsed.scheme, "http")) or parsed.host == null or parsed.user != null or parsed.password != null or parsed.query != null or parsed.fragment != null) return error.InvalidLakeMaintenanceProvider;
    }
};
pub const Policy = struct { operation_id: []const u8, dry_run: bool, retain_ms: u64, keep_latest: usize, max_deleted: usize };
pub const ReaderRegistry = struct {
    protocol: []const u8 = "antfly-snapshot-pins-v1",
    connection: []const u8,
    bucket: []const u8,
    prefix: []const u8,
    /// Admission checks retirement before and after its conditional pin write.
    lease_grace_ms: u64 = 30_000,
};
pub const Capabilities = struct {
    protocol: u8,
    provider: Provider,
    catalog_uri: []const u8,
    writer_fencing: bool = false,
    external_reader_protection: bool = false,
    native_reader_registry: bool = false,
    immutable_retirement: bool = false,
    idempotent_jobs: bool = false,
    nessie_references: bool = false,
    polaris_table_roots: bool = false,
    pub fn validate(self: Capabilities, config: Config, catalog_uri: []const u8) !void {
        if (self.protocol != 1 or self.provider != config.provider or !std.mem.eql(u8, self.catalog_uri, catalog_uri) or !self.writer_fencing or !self.external_reader_protection or !self.native_reader_registry or !self.immutable_retirement or !self.idempotent_jobs) return error.LakeVacuumCatalogCoordinationRequired;
        switch (config.provider) {
            .nessie => if (!self.nessie_references) return error.LakeVacuumCatalogCoordinationRequired,
            .polaris => if (!self.polaris_table_roots) return error.LakeVacuumCatalogCoordinationRequired,
        }
    }
};
pub const State = enum { queued, running, complete, rejected };
pub const Result = struct {
    protocol: u8,
    provider: Provider,
    operation_id: []const u8,
    request_hash: []const u8,
    table_uuid: []const u8,
    state: State,
    expired_snapshots: usize = 0,
    eligible_objects: usize = 0,
    deleted_objects: usize = 0,
    retained_objects: usize = 0,
};
const Intent = struct { version: u8 = 1, policy_hash: []const u8, request_hash: []const u8, body: []const u8 };
pub const Controller = struct {
    config: Config,
    catalog: types.Config,
    transport: rest.Transport,
    journal: rest.Journal,
    context: types.Context,
    fn endpoint(self: Controller, a: A, suffix: []const u8) ![]u8 {
        return std.fmt.allocPrint(a, "{s}/v1/antfly/maintenance/{s}", .{ std.mem.trimEnd(u8, self.config.uri, "/"), suffix });
    }
    /// Journal the original authority/roots before submission. Replays use that
    /// exact body even if the provider has already advanced or expired metadata.
    pub fn run(self: Controller, a: A, source_uri: []const u8, table_uuid: []const u8, metadata_location: []const u8, protected: []const []const u8, readers: ReaderRegistry, policy: Policy) ![]u8 {
        try self.config.validate();
        try self.catalog.validate();
        if (self.catalog.type != .rest or policy.operation_id.len == 0 or policy.operation_id.len > 256 or policy.retain_ms < 600000 or policy.keep_latest == 0 or policy.keep_latest > 1024 or policy.max_deleted == 0 or policy.max_deleted > 4096) return error.InvalidLakeMaintenanceLimits;
        try self.context.ensureActive();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const capability_url = try self.endpoint(scratch, "capabilities");
        var capability_response = try self.transport.request(a, .GET, capability_url, null, null, self.context);
        defer capability_response.deinit(a);
        if (capability_response.status != 200 or capability_response.body.len > 16384) return error.LakeVacuumCatalogCoordinationRequired;
        const capabilities = try std.json.parseFromSliceLeaky(Capabilities, scratch, capability_response.body, .{ .ignore_unknown_fields = true });
        try capabilities.validate(self.config, self.catalog.uri.?);
        const policy_bytes = try std.json.Stringify.valueAlloc(scratch, .{ .config = self.config, .catalog = self.catalog, .source_uri = source_uri, .table_uuid = table_uuid, .readers = readers, .policy = policy }, .{});
        const policy_hash = types.digestHex(policy_bytes);
        const path = try std.fmt.allocPrint(scratch, "{s}/{s}.json", .{ self.journal.prefix, types.digestHex(policy.operation_id) });
        var client = self.journal.client;
        client.allocator = a;
        var saved = client.getObject(self.journal.bucket, path, .{ .max_response_bytes = types.max_commit_bytes, .cancellation = types.contextCancellation(&self.context) }) catch |err| switch (err) {
            error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
            else => return err,
        };
        defer if (saved) |*value| value.deinit(a);
        var intent: Intent = undefined;
        if (saved) |value| {
            intent = try std.json.parseFromSliceLeaky(Intent, scratch, value.body, .{ .allocate = .alloc_always });
            if (intent.version != 1 or !std.mem.eql(u8, intent.policy_hash, &policy_hash) or !std.mem.eql(u8, intent.request_hash, &types.digestHex(intent.body))) return error.LakeCommitIdReused;
        } else {
            const body = try std.json.Stringify.valueAlloc(scratch, .{ .protocol = @as(u8, 1), .provider = self.config.provider, .operation_id = policy.operation_id, .catalog = .{ .uri = self.catalog.uri, .namespace = self.catalog.namespace, .name = self.catalog.name, .warehouse = self.catalog.warehouse }, .source_uri = source_uri, .table_uuid = table_uuid, .expected_metadata_location = metadata_location, .protected_snapshots = protected, .reader_registry = readers, .policy = policy }, .{});
            const body_hash = types.digestHex(body);
            intent = .{ .policy_hash = &policy_hash, .request_hash = try scratch.dupe(u8, &body_hash), .body = body };
            const bytes = try std.json.Stringify.valueAlloc(scratch, intent, .{});
            if (bytes.len > types.max_commit_bytes) return error.LakeVacuumBudgetExceeded;
            var published = client.putObject(self.journal.bucket, path, bytes, .{ .if_none_match = true, .cancellation = types.contextCancellation(&self.context) }) catch |err| {
                // Lost success or a concurrent claimant must be retried from
                // the durable intent. Never submit an unproved alternate body.
                try self.context.ensureActive();
                return err;
            };
            published.deinit(a);
        }
        const suffix = try std.fmt.allocPrint(scratch, "jobs/{s}", .{intent.request_hash});
        const url = try self.endpoint(scratch, suffix);
        var response = try self.transport.request(a, .POST, url, intent.body, intent.request_hash, self.context);
        defer response.deinit(a);
        if ((response.status != 200 and response.status != 202) or response.body.len > 16384) return error.LakeMaintenanceProviderUnavailable;
        const result = try std.json.parseFromSliceLeaky(Result, scratch, response.body, .{ .ignore_unknown_fields = true });
        if (result.protocol != 1 or result.provider != self.config.provider or !std.mem.eql(u8, result.operation_id, policy.operation_id) or !std.mem.eql(u8, result.request_hash, intent.request_hash) or !std.mem.eql(u8, result.table_uuid, table_uuid) or (result.state != .complete and result.deleted_objects != 0) or (policy.dry_run and result.deleted_objects != 0)) return error.InvalidLakeMaintenanceReceipt;
        if (result.state == .rejected) return error.LakeCommitConflict;
        // Response owns its strings after the scratch arena closes.
        return a.dupe(u8, response.body);
    }
};

test "provider maintenance requires catalog-specific writer and reader authority" {
    const config: Config = .{ .provider = .nessie, .connection = "maintenance", .uri = "https://maintenance.example" };
    var capabilities: Capabilities = .{ .protocol = 1, .provider = .nessie, .catalog_uri = "https://catalog.example", .writer_fencing = true, .external_reader_protection = true, .native_reader_registry = true, .immutable_retirement = true, .idempotent_jobs = true, .nessie_references = true };
    try capabilities.validate(config, "https://catalog.example");
    capabilities.nessie_references = false;
    try std.testing.expectError(error.LakeVacuumCatalogCoordinationRequired, capabilities.validate(config, "https://catalog.example"));
    capabilities.nessie_references = true;
    capabilities.external_reader_protection = false;
    try std.testing.expectError(error.LakeVacuumCatalogCoordinationRequired, capabilities.validate(config, "https://catalog.example"));
    capabilities.external_reader_protection = true;
    try std.testing.expectError(error.LakeVacuumCatalogCoordinationRequired, capabilities.validate(config, "https://another.example"));
    const polaris: Config = .{ .provider = .polaris, .connection = "maintenance", .uri = "https://maintenance.example" };
    capabilities.provider = .polaris;
    try std.testing.expectError(error.LakeVacuumCatalogCoordinationRequired, capabilities.validate(polaris, "https://catalog.example"));
    capabilities.polaris_table_roots = true;
    try capabilities.validate(polaris, "https://catalog.example");
}

test "both provider maintenance adapters replay durable requests and reject unbound receipts" {
    const Fixture = struct {
        provider: Provider,
        first_body: ?[]u8 = null,
        posts: usize = 0,
        wrong_receipt: bool = false,
        unsafe: bool = false,
        fn call(raw: *anyopaque, a: A, method: @import("httpx").Method, uri: []const u8, body: ?[]const u8, id: ?[]const u8, context: types.Context) !rest.Response {
            try context.ensureActive();
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (method == .GET) {
                try std.testing.expect(std.mem.endsWith(u8, uri, "/capabilities"));
                return .{ .status = 200, .body = try std.json.Stringify.valueAlloc(a, Capabilities{ .protocol = 1, .provider = self.provider, .catalog_uri = "https://catalog.example", .writer_fencing = !self.unsafe, .external_reader_protection = true, .native_reader_registry = true, .immutable_retirement = true, .idempotent_jobs = true, .nessie_references = self.provider == .nessie, .polaris_table_roots = self.provider == .polaris }, .{}) };
            }
            self.posts += 1;
            try std.testing.expect(method == .POST);
            const payload = body.?;
            if (self.first_body) |first| try std.testing.expectEqualStrings(first, payload) else self.first_body = try a.dupe(u8, payload);
            const hash = types.digestHex(payload);
            try std.testing.expectEqualStrings(&hash, id.?);
            try std.testing.expect(std.mem.endsWith(u8, uri, &hash));
            var parsed = try std.json.parseFromSlice(std.json.Value, a, payload, .{});
            defer parsed.deinit();
            return .{ .status = 200, .body = try std.json.Stringify.valueAlloc(a, Result{ .protocol = 1, .provider = self.provider, .operation_id = parsed.value.object.get("operation_id").?.string, .request_hash = &hash, .table_uuid = if (self.wrong_receipt) "wrong-incarnation" else "table-uuid", .state = .complete, .deleted_objects = 1 }, .{}) };
        }
    };
    const a = std.testing.allocator;
    for ([_]Provider{ .nessie, .polaris }) |provider| {
        var fixture: Fixture = .{ .provider = provider };
        defer if (fixture.first_body) |body| a.free(body);
        var memory = storage.MemoryClient.init(a);
        defer memory.deinit();
        const controller: Controller = .{ .config = .{ .provider = provider, .connection = "provider", .uri = "https://maintenance.example" }, .catalog = .{ .type = .rest, .connection = "catalog", .uri = "https://catalog.example", .namespace = &.{"test"}, .name = "items" }, .transport = .{ .ptr = &fixture, .request_fn = Fixture.call }, .journal = .{ .client = memory.client(), .bucket = "native", .prefix = "jobs" }, .context = .{} };
        const readers: ReaderRegistry = .{ .connection = "native", .bucket = "native", .prefix = "pins" };
        const policy: Policy = .{ .operation_id = "vacuum", .dry_run = false, .retain_ms = 600000, .keep_latest = 2, .max_deleted = 10 };
        const first = try controller.run(a, "s3://lake/items", "table-uuid", "metadata/old", &.{"1"}, readers, policy);
        a.free(first);
        // Reopening the controller uses object-store authority, even after the
        // catalog changed because the first job already completed remotely.
        const reopened = controller;
        const second = try reopened.run(a, "s3://lake/items", "table-uuid", "metadata/new", &.{"2"}, readers, policy);
        a.free(second);
        try std.testing.expectEqual(@as(usize, 2), fixture.posts);
        var changed = policy;
        changed.max_deleted = 11;
        try std.testing.expectError(error.LakeCommitIdReused, controller.run(a, "s3://lake/items", "table-uuid", "metadata/new", &.{}, readers, changed));
        fixture.wrong_receipt = true;
        try std.testing.expectError(error.InvalidLakeMaintenanceReceipt, controller.run(a, "s3://lake/items", "table-uuid", "metadata/new", &.{}, readers, policy));
        fixture.unsafe = true;
        try std.testing.expectError(error.LakeVacuumCatalogCoordinationRequired, controller.run(a, "s3://lake/items", "table-uuid", "metadata/new", &.{}, readers, policy));
        try std.testing.expectEqual(@as(usize, 3), fixture.posts);
    }
}
