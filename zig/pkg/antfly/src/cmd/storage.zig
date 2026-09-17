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

//! Storage migration operator: drive online HTTP jobs or migrate a stopped
//! table under the catalog lock shared with standalone startup. Offline mode
//! must not run alongside an older binary that does not acquire that lock.
const std = @import("std");
const antfly = struct {
    const vector_migration = @import("../common/vector_migration.zig");
    const migration_files = @import("../common/migration_files.zig");
    const metadata = @import("../metadata/mod.zig");
    const vector_migration_offline = @import("../storage/vector_migration_offline.zig");
};
const migration = antfly.vector_migration;

pub fn runFromIterator(init: std.process.Init, iterator: *std.process.Args.Iterator) !void {
    const kind = iterator.next() orelse {
        printUsage();
        return;
    };
    if (std.mem.eql(u8, kind, "--help") or std.mem.eql(u8, kind, "-h")) {
        printUsage();
        return;
    }
    if (!std.mem.eql(u8, kind, "migrate")) return error.InvalidArguments;
    const alloc = init.arena.allocator();
    var arguments: std.ArrayListUnmanaged([]const u8) = .empty;
    while (iterator.next()) |arg| try arguments.append(alloc, arg);
    const args = arguments.items;
    var url: ?[]const u8 = null;
    var action: []const u8 = "run";
    var catalog_path: ?[]const u8 = null;
    var replicas: ?[]const u8 = null;
    var table_name: ?[]const u8 = null;
    var job_id: ?[]const u8 = null;
    var target: ?[]const u8 = null;
    var budget: migration.Budget = .{};
    var once = false;
    var cancelling = false;
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--help") or std.mem.eql(u8, args[i], "-h")) {
            printUsage();
            return;
        }
        if (std.mem.eql(u8, args[i], "--cancel")) {
            cancelling = true;
            continue;
        }
        if (std.mem.eql(u8, args[i], "--once")) {
            once = true;
            continue;
        }
        if (i + 1 == args.len) return error.MissingOptionValue;
        const value = args[i + 1];
        if (std.mem.eql(u8, args[i], "--to")) target = value else if (std.mem.eql(u8, args[i], "--url")) url = value else if (std.mem.eql(u8, args[i], "--action")) action = value else if (std.mem.eql(u8, args[i], "--catalog")) catalog_path = value else if (std.mem.eql(u8, args[i], "--replica-root")) replicas = value else if (std.mem.eql(u8, args[i], "--table")) table_name = value else if (std.mem.eql(u8, args[i], "--job")) job_id = value else if (std.mem.eql(u8, args[i], "--batch-bytes")) budget.batch_bytes = try std.fmt.parseInt(u64, value, 10) else if (std.mem.eql(u8, args[i], "--batch-rows")) budget.batch_rows = try std.fmt.parseInt(u32, value, 10) else if (std.mem.eql(u8, args[i], "--temporary-bytes")) budget.temporary_bytes = try std.fmt.parseInt(u64, value, 10) else if (std.mem.eql(u8, args[i], "--disk-reserve-bytes")) budget.disk_reserve_bytes = try std.fmt.parseInt(u64, value, 10) else return error.UnknownOption;
        i += 1;
    }
    if (!std.mem.eql(u8, target orelse return error.InvalidArguments, "vector-store")) return error.InvalidArguments;
    if (url) |base| {
        if (catalog_path != null or replicas != null or once or cancelling) return error.InvalidArguments;
        return runOnline(init, base, table_name orelse return error.InvalidArguments, .{
            .job_id = job_id orelse return error.InvalidArguments,
            .mode = .online,
            .budget = budget,
        }, action);
    }
    const status_only = std.mem.eql(u8, action, "status");
    if ((!std.mem.eql(u8, action, "run") and !status_only) or (once and cancelling) or (status_only and (once or cancelling))) return error.InvalidArguments;
    const path = catalog_path orelse return error.ExpectedCatalogReplicaRootTableAndJob;
    const root = replicas orelse return error.ExpectedCatalogReplicaRootTableAndJob;
    const name = table_name orelse return error.ExpectedCatalogReplicaRootTableAndJob;
    const request = migration.Request{ .job_id = job_id orelse return error.ExpectedCatalogReplicaRootTableAndJob, .mode = .offline, .budget = budget };
    try request.validate();
    var catalog = try @import("../standalone/offline_catalog.zig").Catalog.open(alloc, init.io, path);
    defer catalog.deinit();
    const json_alloc = catalog.document.arena.allocator();
    const table_value = try catalog.findTable(name);
    const table_json = try std.json.Stringify.valueAlloc(alloc, table_value.*, .{});
    if (status_only) {
        std.debug.print("{s}\n", .{table_json});
        return;
    }
    var table = try std.json.parseFromSlice(antfly.metadata.TableRecord, alloc, table_json, .{ .ignore_unknown_fields = true });
    defer table.deinit();
    if (table.value.desired_replica_count != 1 or table.value.min_ranges != 1 or
        table.value.read_schema_json.len != 0 or table.value.restore_backup_id.len != 0) return error.VectorStoreRequiresLocalSingleShardTable;
    var replication = try std.json.parseFromSlice(std.json.Value, alloc, table.value.replication_sources_json, .{});
    defer replication.deinit();
    if (replication.value != .array or replication.value.array.items.len != 0) return error.VectorStoreRequiresLocalSingleShardTable;
    const ranges_json = try std.json.Stringify.valueAlloc(alloc, catalog.document.value.object.get("ranges") orelse return error.InvalidCatalog, .{});
    var ranges = try std.json.parseFromSlice([]const antfly.metadata.RangeRecord, alloc, ranges_json, .{ .ignore_unknown_fields = true });
    defer ranges.deinit();
    const range = blk: {
        var selected: ?antfly.metadata.RangeRecord = null;
        for (ranges.value) |entry| if (entry.table_id == table.value.table_id) {
            if (selected != null or entry.start_key.len != 0 or (entry.end_key != null and entry.end_key.?.len != 0) or entry.restore_backup_id.len != 0)
                return error.VectorStoreRequiresLocalSingleShardTable;
            selected = entry;
        };
        break :blk selected orelse return error.TableNotFound;
    };
    if (table.value.storage_migration) |admitted| {
        if (!admitted.eql(.{ .request = request })) return error.VectorMigrationIdempotencyConflict;
    } else if (!cancelling and table.value.storage.dense_embeddings == .primary_lsm) {
        const admission_json = try std.json.Stringify.valueAlloc(alloc, migration.Admission{ .request = request }, .{});
        const admitted = try std.json.parseFromSliceLeaky(std.json.Value, json_alloc, admission_json, .{ .allocate = .alloc_always });
        try table_value.object.put(json_alloc, "storage_migration", admitted);
        try catalog.publish(table_value.*);
    }
    const db_path = try antfly.metadata.groupDbPathFromReplicaRoot(alloc, root, range.group_id);
    if (cancelling) {
        try antfly.vector_migration_offline.cancel(std.heap.smp_allocator, init.io, db_path, request, .{ .identity_namespace = .{
            .table_id = table.value.table_id,
            .shard_id = antfly.metadata.table_manager.rangeDocIdentityShardId(range),
            .range_id = antfly.metadata.table_manager.rangeDocIdentityRangeId(range),
        } });
        _ = table_value.object.swapRemove("storage_migration");
        try catalog.publish(table_value.*);
        std.debug.print("offline vector migration cancelled\n", .{});
        return;
    }
    const result = antfly.vector_migration_offline.run(std.heap.smp_allocator, init.io, db_path, request, .{
        .open = .{
            .identity_namespace = .{
                .table_id = table.value.table_id,
                .shard_id = antfly.metadata.table_manager.rangeDocIdentityShardId(range),
                .range_id = antfly.metadata.table_manager.rangeDocIdentityRangeId(range),
            },
        },
        .max_steps = if (once) 1 else 0,
        .progress_fn = printProgress,
    }) catch |err| {
        // An exact retry can briefly readmit a terminal cancelled job. Its
        // durable receipt proves no migration remains; do not strand the
        // stopped server behind the marker installed by this invocation.
        if (err == error.VectorMigrationCancelled) {
            _ = table_value.object.swapRemove("storage_migration");
            try catalog.publish(table_value.*);
        }
        return err;
    };
    if (result == .complete) {
        var storage = std.json.ObjectMap{};
        try storage.put(json_alloc, "dense_embeddings", .{ .string = "vector_store" });
        try table_value.object.put(json_alloc, "storage", .{ .object = storage });
        _ = table_value.object.swapRemove("storage_migration");
        try catalog.publish(table_value.*);
    }
    std.debug.print("offline vector migration {s}\n", .{@tagName(result)});
}
fn printProgress(_: ?*anyopaque, raw: []const u8) !void {
    std.debug.print("{s}\n", .{raw});
}

fn printUsage() void {
    std.debug.print(
        \\usage: antfly storage migrate --table NAME --to vector-store --job ID [options]
        \\
        \\Online: --url http://HOST:PORT [--action run|start|step|publish|cancel|status]
        \\Offline: --catalog PATH --replica-root PATH [--once | --cancel | --action status]
        \\Budgets: --batch-bytes N --batch-rows N --temporary-bytes N --disk-reserve-bytes N
        \\
        \\Online defaults to run: advance bounded steps and publish verified coverage.
        \\ANTFLY_API_KEY supplies a Bearer token. Stop the driver to pause; capture continues.
        \\Offline requires a stopped standalone server. --once advances one bounded step.
        \\Resume with the same job ID and budgets; cancellation is prepublication only.
        \\
    , .{});
}

fn runOnline(init: std.process.Init, base: []const u8, table: []const u8, request: migration.Request, action: []const u8) !void {
    try request.validate();
    const drive = std.mem.eql(u8, action, "run");
    var next: migration.Action = if (drive) .start else std.meta.stringToEnum(migration.Action, action) orelse return error.InvalidArguments;
    const alloc = init.gpa;
    var encoded: std.Io.Writer.Allocating = .init(alloc);
    defer encoded.deinit();
    for (table) |byte| {
        if (std.ascii.isAlphanumeric(byte) or std.mem.indexOfScalar(u8, "-._~", byte) != null) {
            try encoded.writer.writeByte(byte);
        } else {
            try encoded.writer.print("%{X:0>2}", .{byte});
        }
    }
    const trimmed = std.mem.trimEnd(u8, base, "/");
    const api_root = if (std.mem.endsWith(u8, trimmed, "/db/v1")) "" else "/db/v1";
    const url = try std.fmt.allocPrint(alloc, "{s}{s}/tables/{s}/storage/migrations", .{ trimmed, api_root, encoded.written() });
    defer alloc.free(url);
    const job_url = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ url, request.job_id });
    defer alloc.free(job_url);
    const token = init.environ_map.get("ANTFLY_API_KEY");
    const authorization = if (token) |key| try std.fmt.allocPrint(alloc, "Bearer {s}", .{key}) else null;
    defer if (authorization) |value| alloc.free(value);
    const headers: [1][2][]const u8 = .{.{ "Authorization", authorization orelse "" }};
    var http = @import("httpx").Client.initWithConfig(alloc, init.io, .{});
    defer http.deinit();
    while (true) {
        // Free each response before the next bounded step: a long migration
        // must not retain its entire progress history in the process arena.
        const body = if (next == .start)
            try std.json.Stringify.valueAlloc(alloc, migration.CreateRequest{ .job_id = request.job_id, .target = .vector_store, .budget = request.budget }, .{})
        else
            try std.json.Stringify.valueAlloc(alloc, .{ .action = next }, .{});
        defer alloc.free(body);
        var response = try http.request(if (next == .status) .GET else .POST, if (next == .start) url else job_url, .{
            .json = if (next == .status) null else body,
            .headers = if (authorization != null) &headers else null,
            .timeout_ms = 300_000,
        });
        defer response.deinit();
        const raw = response.body orelse return error.InvalidVectorMigrationState;
        if (response.status.code >= 300) {
            std.debug.print("migration HTTP {d}: {s}\nRetry with the same job ID and budgets after resolving the error.\n", .{ response.status.code, raw });
            return error.MigrationRequestFailed;
        }
        if (!drive) {
            std.debug.print("{s}\n", .{raw});
            return;
        }
        var job = try std.json.parseFromSlice(migration.Job, alloc, raw, .{});
        defer job.deinit();
        try job.value.validate();
        std.debug.print("{s}\n", .{raw});
        if (!drive or job.value.phase == .complete or job.value.phase == .cancelled) return;
        next = if (job.value.phase == .ready) .publish else .step;
        if (job.value.phase == .serving) try init.io.sleep(.fromMilliseconds(250), .awake);
    }
}
