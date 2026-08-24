// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Deterministic external-lake discovery, cache, and query histories.

const std = @import("std");
const objectstore = @import("objectstore");
const vopr = @import("vopr");
const binding_mod = @import("../serverless/external_source/catalog_binding.zig");
const external_source = @import("../serverless/external_source/types.zig");
const iceberg_metadata = @import("../serverless/external_source/iceberg_metadata.zig");
const iceberg_snapshot = @import("../serverless/query/lake_iceberg_snapshot.zig");
const lake_object_reader = @import("../serverless/query/lake_object_reader.zig");
const lake = @import("../serverless/query/lake_parquet_rowgroup.zig");
const range_io = @import("../serverless/query/lake_range_io.zig");
const lake_rows = @import("../serverless/query/lake_rows.zig");
const lake_scan_plan = @import("../serverless/query/lake_scan_plan.zig");

pub const Scenario = struct {
    pub const name = "external-lake";
    pub const version: u32 = 2;
    const sound_id = vopr.id.stable(name, "composition-sound");
    const complete_id = vopr.id.stable(name, "mode-completes");
    const recovery_id = vopr.id.stable(name, "restart-recovers");
    pub const properties = &[_]vopr.property.Declaration{
        .{ .id = sound_id, .name = name ++ ".composition-sound", .kind = .always },
        .{ .id = complete_id, .name = name ++ ".mode-completes", .kind = .reachable },
        .{ .id = recovery_id, .name = name ++ ".restart-recovers", .kind = .reachable },
    };

    const Mode = enum {
        cached_read,
        version_isolation,
        short_response,
        timeout,
        admission_limit,
        full_composition,
        schema_evolution,
        stale_object_version,
        object_deletion,
        ambiguous_download,
        cache_eviction,
        restart_recovery,
    };
    const mode_ids = [_]vopr.id.StableId{
        vopr.id.stable(name, "cached-read"),
        vopr.id.stable(name, "version-isolation"),
        vopr.id.stable(name, "short-response"),
        vopr.id.stable(name, "timeout"),
        vopr.id.stable(name, "admission-limit"),
        vopr.id.stable(name, "full-composition"),
        vopr.id.stable(name, "schema-evolution"),
        vopr.id.stable(name, "stale-object-version"),
        vopr.id.stable(name, "object-deletion"),
        vopr.id.stable(name, "ambiguous-download"),
        vopr.id.stable(name, "cache-eviction"),
        vopr.id.stable(name, "restart-recovery"),
    };
    const mode_names = [_][]const u8{
        name ++ ".cached-read",
        name ++ ".version-isolation",
        name ++ ".short-response",
        name ++ ".timeout",
        name ++ ".admission-limit",
        name ++ ".full-composition",
        name ++ ".schema-evolution",
        name ++ ".stale-object-version",
        name ++ ".object-deletion",
        name ++ ".ambiguous-download",
        name ++ ".cache-eviction",
        name ++ ".restart-recovery",
    };

    const metadata_uri = "s3://bucket/t/metadata/v1.metadata.json";
    const manifest_uri = "s3://bucket/t/metadata/m-a.avro";
    const data_uri = "s3://bucket/t/data/a.parquet";
    const projection = [_][]const u8{"amount"};

    const State = struct {
        allocator: std.mem.Allocator,
        sim: vopr.vopr_io.VoprIo,
        cache: lake.ObjectRangeCache = .{},
        calls: u64 = 0,
        progress: u64 = 0,
        mode: Mode = .cached_read,
        sound: bool = true,
        complete: bool = false,
        restart_recovered: bool = false,

        fn reader(self: *@This()) lake.ObjectRangeReader {
            return .{ .ctx = self, .read_range_alloc = readRange };
        }

        fn readRange(raw: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.mode == .timeout) return error.Timeout;
            const actual_len = if (self.mode == .short_response) len - 1 else len;
            const bytes = try alloc.alloc(u8, actual_len);
            for (bytes, 0..) |*byte, index| byte.* = @intCast((offset + index) & 0xff);
            return bytes;
        }

        fn read(self: *@This(), object_version: []const u8) ![]u8 {
            const object = range_io.ObjectRef{
                .bucket = "bucket",
                .key = "table/data.parquet",
                .byte_len = 64,
                .version = .{ .version_id = object_version },
            };
            const planned = try range_io.planParquetFooterRead(object, 8);
            return self.cache.readAlloc(self.allocator, self.reader(), planned);
        }

        fn runBoundary(self: *@This()) !void {
            switch (self.mode) {
                .cached_read => {
                    const first = try self.read("v1");
                    defer self.allocator.free(first);
                    const second = try self.read("v1");
                    defer self.allocator.free(second);
                    self.sound = self.calls == 1 and std.mem.eql(u8, first, second);
                },
                .version_isolation => {
                    const first = try self.read("v1");
                    defer self.allocator.free(first);
                    const second = try self.read("v2");
                    defer self.allocator.free(second);
                    self.sound = self.calls == 2;
                },
                .short_response => {
                    _ = self.read("short") catch |err| {
                        self.sound = err == error.InvalidLakeRangeRead;
                        return;
                    };
                    self.sound = false;
                },
                .timeout => {
                    _ = self.read("timeout") catch |err| {
                        self.sound = err == error.Timeout;
                        return;
                    };
                    self.sound = false;
                },
                .admission_limit => {
                    self.cache.policy.setFetchLimit(4);
                    _ = self.read("limited") catch |err| {
                        self.sound = err == error.LakeRangeReadTooLarge and self.calls == 0;
                        return;
                    };
                    self.sound = false;
                },
                else => unreachable,
            }
        }

        fn putObject(client: *objectstore.Client, key: []const u8, bytes: []const u8) !void {
            var result = try client.putObject("bucket", key, bytes, .{});
            result.deinit(client.allocator);
        }

        fn queryInventory(
            self: *@This(),
            client: objectstore.Client,
            faults: *objectstore.ScriptedFaultClient,
            cache: *lake.ObjectRangeCache,
            parquet: []const u8,
            mode: Mode,
        ) !bool {
            var inventory = try iceberg_snapshot.readSnapshotInventoryAlloc(self.allocator, .{
                .client = client,
                .source_id = "events",
                .metadata_uri = metadata_uri,
                .cache = cache,
            });
            defer inventory.deinit(self.allocator);
            try iceberg_snapshot.pinInventoryDataFileObjectVersions(self.allocator, client, &inventory);
            self.progress += 1;

            switch (mode) {
                .stale_object_version => {
                    const replacement = try self.allocator.dupe(u8, parquet);
                    defer self.allocator.free(replacement);
                    replacement[0] ^= 0xff;
                    var mutable_client = client;
                    try putObject(&mutable_client, "t/data/a.parquet", replacement);
                },
                .object_deletion => {
                    var mutable_client = client;
                    try mutable_client.deleteObject("bucket", "t/data/a.parquet", .{});
                },
                .ambiguous_download => faults.completeNextGetThenFail(error.Timeout),
                else => {},
            }

            var range_reader = lake_object_reader.ObjectStorageRangeReader.initWithRetry(client, .{ .max_attempts = 2 });
            var discovered = lake.discoverSupportedI64ObjectRangeRowGroupsFromCachedFootersAlloc(
                self.allocator,
                range_reader.parquetReader(),
                cache,
                inventory,
                &projection,
                16,
            ) catch |err| switch (mode) {
                .stale_object_version => return err == error.PreconditionFailed,
                .object_deletion => return err == error.FileNotFound or err == error.ObjectNotFound,
                else => return err,
            };
            defer discovered.deinit(self.allocator);

            if (mode == .stale_object_version or mode == .object_deletion) return false;
            const binding = binding_mod.Binding{
                .table_id = "events",
                .format = .iceberg,
                .source_uri = "s3://bucket/t",
                .snapshot_mode = .{ .snapshot_id = discovered.inventory.snapshot_id },
                .schema_fingerprint = discovered.inventory.schema_fingerprint,
            };
            try binding.validateReadOnlyMvp();

            const hits_before = cache.statsSnapshot().hits;
            const first_matches = try queryAmount(self.allocator, range_reader.parquetReader(), cache, binding, discovered.inventory);
            const second_matches = try queryAmount(self.allocator, range_reader.parquetReader(), cache, binding, discovered.inventory);
            const stats = cache.statsSnapshot();
            self.progress += 2;

            if (!first_matches or !second_matches or stats.hits <= hits_before) return false;
            if (mode == .cache_eviction and stats.evicted_bytes == 0) return false;
            return true;
        }

        fn queryAmount(
            alloc: std.mem.Allocator,
            object_reader: lake.ObjectRangeReader,
            cache: *lake.ObjectRangeCache,
            binding: binding_mod.Binding,
            inventory: external_source.Inventory,
        ) !bool {
            var result = try lake.querySupportedI64ObjectRangeRowsAlloc(alloc, .{
                .binding = binding,
                .reader = object_reader,
                .cache = cache,
                .inventory = inventory,
                .projected_columns = &projection,
                .predicate = lake_rows.Predicate{
                    .column = "amount",
                    .op = .eq_i64,
                    .i64_value = 20,
                },
            });
            defer result.deinit(alloc);
            return result.total == 1 and result.rows.len == 1 and
                result.rows[0].find("amount").?.value.?.i64 == 20 and
                result.rows[0].row_ref.external.row_ordinal == 1;
        }

        fn runComposition(self: *@This()) !void {
            const parquet = try lake.buildTestPlainI64ParquetObjectAlloc(self.allocator, &.{.{
                .column_id = "amount",
                .field_id = 2,
                .values = &.{ 10, 20, 30 },
            }});
            defer self.allocator.free(parquet);
            const manifest = try iceberg_snapshot.buildTestDataManifestAlloc(self.allocator, &.{.{
                .path = data_uri,
                .rows = 3,
                .bytes = parquet.len,
            }});
            defer self.allocator.free(manifest);
            const manifest_list = try iceberg_snapshot.buildTestManifestListAlloc(
                self.allocator,
                manifest_uri,
                manifest.len,
                1,
                3,
            );
            defer self.allocator.free(manifest_list);

            var memory = objectstore.MemoryClient.init(self.allocator);
            defer memory.deinit();
            var faults = objectstore.ScriptedFaultClient.init(self.allocator, memory.client());
            defer faults.deinit();
            var client = faults.client();
            try client.makeBucket("bucket");
            try putObject(&client, "t/metadata/v1.metadata.json", metadataJson());
            try putObject(&client, "t/metadata/snap-12.avro", manifest_list);
            try putObject(&client, "t/metadata/m-a.avro", manifest);
            try putObject(&client, "t/data/a.parquet", parquet);

            if (self.mode == .restart_recovery) {
                {
                    var persistent = try self.openPersistent();
                    defer persistent.deinit();
                    var cache = lake.ObjectRangeCache{};
                    defer cache.deinit(self.allocator);
                    cache.persistent = &persistent;
                    self.sound = try self.queryInventory(client, &faults, &cache, parquet, .full_composition);
                    persistent.flush();
                }
                if (!self.sound) return;
                try self.sim.crashFileSystem();
                const gets_before = faults.get_attempts;
                {
                    var reopened = try self.openPersistent();
                    defer reopened.deinit();
                    var cache = lake.ObjectRangeCache{};
                    defer cache.deinit(self.allocator);
                    cache.persistent = &reopened;
                    self.sound = try self.queryInventory(client, &faults, &cache, parquet, .full_composition);
                }
                self.restart_recovered = self.sound and faults.get_attempts == gets_before;
                self.sound = self.restart_recovered;
            } else if (self.mode == .full_composition) {
                var persistent = try self.openPersistent();
                defer persistent.deinit();
                var cache = lake.ObjectRangeCache{};
                defer cache.deinit(self.allocator);
                cache.persistent = &persistent;
                self.sound = try self.queryInventory(client, &faults, &cache, parquet, self.mode);
                persistent.flush();
            } else {
                var cache = lake.ObjectRangeCache{};
                defer cache.deinit(self.allocator);
                if (self.mode == .cache_eviction) {
                    cache.policy.setTotalLimit(metadataJson().len + manifest_list.len + manifest.len);
                }
                const gets_before = faults.get_attempts;
                self.sound = try self.queryInventory(client, &faults, &cache, parquet, self.mode);
                if (self.mode == .ambiguous_download) {
                    self.sound = self.sound and faults.get_attempts >= gets_before + 2;
                }
            }
            self.calls = faults.get_attempts;
        }

        fn openPersistent(self: *@This()) !lake.PersistentObjectRangeCache {
            return try lake.PersistentObjectRangeCache.initWithPolicy(self.sim.io(), "/external-lake-cache", .{
                .max_total_bytes = 1024 * 1024,
                .max_entries = 64,
                .max_write_queue_bytes = 1024 * 1024,
                .max_write_queue_entries = 64,
                .durability = .durable,
            });
        }

        fn runSchemaEvolution(self: *@This()) !void {
            var current = try iceberg_metadata.parseMetadataPlanAlloc(
                self.allocator,
                "s3://bucket/t/metadata/v2.metadata.json",
                evolvedMetadataJson(),
                null,
            );
            defer current.deinit(self.allocator);
            var historical = try iceberg_metadata.parseMetadataPlanAlloc(
                self.allocator,
                "s3://bucket/t/metadata/v2.metadata.json",
                evolvedMetadataJson(),
                "11",
            );
            defer historical.deinit(self.allocator);

            var inventory = external_source.Inventory{
                .format = .iceberg,
                .source_id = try self.allocator.dupe(u8, "events"),
                .source_uri = try self.allocator.dupe(u8, "s3://bucket/t"),
                .snapshot_id = try self.allocator.dupe(u8, current.current_snapshot_id),
                .schema_fingerprint = try self.allocator.dupe(u8, current.schema_fingerprint),
                .files = try self.allocator.alloc(external_source.FileEntry, 0),
            };
            defer inventory.deinit(self.allocator);
            const current_binding = binding_mod.Binding{
                .table_id = "events",
                .format = .iceberg,
                .source_uri = "s3://bucket/t",
                .snapshot_mode = .{ .snapshot_id = current.current_snapshot_id },
                .schema_fingerprint = current.schema_fingerprint,
            };
            try lake_scan_plan.validateBindingInventory(current_binding, inventory);
            const stale_binding = binding_mod.Binding{
                .table_id = "events",
                .format = .iceberg,
                .source_uri = "s3://bucket/t",
                .snapshot_mode = .{ .snapshot_id = historical.current_snapshot_id },
                .schema_fingerprint = historical.schema_fingerprint,
            };
            const stale_rejected = if (lake_scan_plan.validateBindingInventory(stale_binding, inventory)) |_| false else |err| err == error.ExternalLakeSnapshotMismatch;
            self.sound = stale_rejected and
                !std.mem.eql(u8, current.schema_fingerprint, historical.schema_fingerprint) and
                std.mem.eql(u8, current.fieldNameForId(3).?, "region") and
                historical.fieldNameForId(3) == null;
            self.progress += 2;
        }

        fn run(self: *@This()) !void {
            switch (self.mode) {
                .cached_read, .version_isolation, .short_response, .timeout, .admission_limit => try self.runBoundary(),
                .schema_evolution => try self.runSchemaEvolution(),
                else => try self.runComposition(),
            }
            self.progress += 1;
            self.complete = true;
        }
    };

    pub const World = struct { state: *State };
    pub fn init(allocator: std.mem.Allocator) !World {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.* = .{
            .allocator = allocator,
            .sim = try vopr.vopr_io.VoprIo.init(.{
                .seed = 0xe17e_4a4e,
                .required = .of(&.{ .clock_read, .files, .task_scheduling, .synchronization, .deterministic_entropy }),
            }),
        };
        return .{ .state = state };
    }
    pub fn deinit(world: *World, allocator: std.mem.Allocator) void {
        world.state.cache.deinit(allocator);
        world.state.sim.deinit();
        allocator.destroy(world.state);
        world.* = undefined;
    }
    pub fn enumerate(world: *World, list: *vopr.transition.List, allocator: std.mem.Allocator) !void {
        if (world.state.complete) return;
        inline for (std.meta.tags(Mode), mode_ids, mode_names) |mode, id, mode_name| try list.append(allocator, .{
            .id = id,
            .name = mode_name,
            .kind = switch (mode) {
                .cached_read, .version_isolation, .full_composition, .schema_evolution, .restart_recovery => .workload,
                else => .fault,
            },
        });
    }
    pub fn execute(world: *World, selected: vopr.transition.Transition, events: *vopr.event.Sink, allocator: std.mem.Allocator) !vopr.outcome.TransitionOutcome {
        var found = false;
        inline for (std.meta.tags(Mode), mode_ids) |mode, id| if (selected.id == id) {
            world.state.mode = mode;
            try world.state.run();
            try world.state.sim.ensureNoCapabilityViolation();
            found = true;
        };
        if (!found) return error.InvalidExternalLakeTransition;
        try events.emitNamed(allocator, .domain, selected.name, world.state.calls);
        return .applied();
    }
    pub fn observe(world: *World, builder: *vopr.observation.Builder, allocator: std.mem.Allocator) !void {
        try builder.addNamed(allocator, name ++ ".calls", world.state.calls);
        try builder.addNamed(allocator, name ++ ".progress", world.state.progress);
        try builder.addNamed(allocator, name ++ ".complete", @intFromBool(world.state.complete));
    }
    pub fn evaluate(world: *World, sink: *vopr.property.Sink, allocator: std.mem.Allocator) !void {
        try sink.check(allocator, sound_id, world.state.sound);
        try sink.check(allocator, complete_id, world.state.complete);
        try sink.check(allocator, recovery_id, world.state.mode != .restart_recovery or world.state.restart_recovered);
    }
    pub fn healthSnapshot(world: *World) vopr.health.Snapshot {
        const resources = world.state.sim.resourceSnapshot();
        return .{
            .progress_expected = true,
            .progress_units = world.state.progress,
            .active_tasks = resources.active_tasks,
            .open_descriptors = resources.live_file_handles + resources.live_sockets,
            .cleanup_complete = world.state.complete and resources.active_tasks == 0 and resources.live_file_handles == 0,
        };
    }
    pub fn done(world: *World) bool {
        return world.state.complete;
    }
};

fn metadataJson() []const u8 {
    return
    \\{
    \\  "format-version": 2,
    \\  "table-uuid": "uuid-events",
    \\  "location": "s3://bucket/t",
    \\  "current-schema-id": 7,
    \\  "schemas": [{
    \\    "schema-id": 7,
    \\    "fields": [{"id": 2, "name": "amount", "required": true, "type": "long"}]
    \\  }],
    \\  "current-snapshot-id": 12,
    \\  "snapshots": [{
    \\    "snapshot-id": 12,
    \\    "sequence-number": 42,
    \\    "schema-id": 7,
    \\    "timestamp-ms": 1700000000000,
    \\    "manifest-list": "s3://bucket/t/metadata/snap-12.avro"
    \\  }]
    \\}
    ;
}

fn evolvedMetadataJson() []const u8 {
    return
    \\{
    \\  "format-version": 2,
    \\  "table-uuid": "uuid-events",
    \\  "location": "s3://bucket/t",
    \\  "current-schema-id": 8,
    \\  "schemas": [
    \\    {"schema-id": 7, "fields": [{"id": 2, "name": "amount", "required": true, "type": "long"}]},
    \\    {"schema-id": 8, "fields": [
    \\      {"id": 2, "name": "amount", "required": true, "type": "long"},
    \\      {"id": 3, "name": "region", "required": false, "type": "string"}
    \\    ]}
    \\  ],
    \\  "current-snapshot-id": 12,
    \\  "snapshots": [
    \\    {"snapshot-id": 11, "sequence-number": 41, "schema-id": 7, "timestamp-ms": 1699999999000, "manifest-list": "s3://bucket/t/metadata/snap-11.avro"},
    \\    {"snapshot-id": 12, "sequence-number": 42, "schema-id": 8, "timestamp-ms": 1700000000000, "manifest-list": "s3://bucket/t/metadata/snap-12.avro"}
    \\  ]
    \\}
    ;
}

test "external lake VOPR exact replays full Iceberg and Parquet lifecycle faults" {
    const backend_ids = vopr.vopr_io.artifactBackendIds();
    for (Scenario.mode_ids) |mode_id| {
        var scripted = vopr.choice.Scripted{ .selections = &.{mode_id} };
        var artifact = try vopr.runner.run(Scenario, std.testing.allocator, scripted.source(), .{
            .system = "antfly",
            .transition_budget = 1,
            .backend_ids = &backend_ids,
        });
        defer artifact.deinit();
        try std.testing.expectEqual(@as(u64, 0), artifact.summary.?.property_failures);
        var replayed = try vopr.replay.exact(Scenario, std.testing.allocator, &artifact);
        replayed.deinit();
    }
}
