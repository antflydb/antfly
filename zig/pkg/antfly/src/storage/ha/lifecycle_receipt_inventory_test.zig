// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const admin_api = @import("../../admin/mod.zig");
const fs_paths = @import("../../common/fs_paths.zig");
const admin_exec = @import("admin_exec.zig");
const fencing = @import("fencing.zig");
const http_admin = @import("http_admin.zig");
const ledger_mod = @import("lifecycle_receipt_ledger.zig");
const seed_activation = @import("seed_activation.zig");
const seed_capture = @import("seed_capture.zig");

const digest_a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const digest_b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const digest_c = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const digest_d = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

fn walOptions() @import("../wal.zig").WalOptions {
    // LMDB gives the corruption test a deterministic way to model a torn
    // durable tail. Production uses the default durable WAL backend.
    return .{ .backend = .lmdb };
}

fn writeFile(path: []const u8, body: []const u8) !void {
    if (std.fs.path.dirname(path)) |parent| try fs_paths.createDirPathPortable(std.testing.io, parent);
    var file = try std.Io.Dir.cwd().createFile(std.testing.io, path, .{ .truncate = true });
    defer file.close(std.testing.io);
    try file.writeStreamingAll(std.testing.io, body);
    try file.sync(std.testing.io);
}

fn captureReceiptJSON(alloc: std.mem.Allocator, generation: []const u8, checkpoint_lsn: u64) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, .{
        .format_version = @as(u16, 2),
        .generation = generation,
        .slot_name = "standby-a",
        .topology_id = "topology-a",
        .topology_generation = @as(u64, 9),
        .node_id = "primary-a",
        .target_pvc_name = "standby-a-data",
        .target_pvc_uid = "pvc-uid-9",
        .cluster_id = @as(u64, 100),
        .shard_id = @as(u64, 10),
        .table_id = @as(u64, 20),
        .timeline_id = @as(u64, 4),
        .epoch = @as(u64, 6),
        .source_plan_sha256 = digest_a,
        .manifest_id = generation,
        .backup_lsn = checkpoint_lsn - 1,
        .checkpoint_lsn = checkpoint_lsn,
        .end_record_lsn = checkpoint_lsn + 1,
        .manifest_sha256 = digest_b,
        .file_count = @as(usize, 3),
        .total_bytes = @as(u64, 4096),
    }, .{});
}

fn activationReceiptJSON(alloc: std.mem.Allocator, generation: []const u8, checkpoint_lsn: u64) ![]u8 {
    const generation_path = try std.fmt.allocPrint(alloc, "generations/{s}", .{generation});
    defer alloc.free(generation_path);
    return try std.json.Stringify.valueAlloc(alloc, .{
        .format_version = @as(u16, 1),
        .generation = generation,
        .slot_name = "standby-a",
        .cluster_id = @as(u64, 100),
        .shard_id = @as(u64, 10),
        .table_id = @as(u64, 20),
        .timeline_id = @as(u64, 4),
        .epoch = @as(u64, 6),
        .manifest_id = generation,
        .backup_lsn = checkpoint_lsn - 1,
        .checkpoint_lsn = checkpoint_lsn,
        .seed_receipt_sha256 = digest_c,
        .capture_receipt_sha256 = digest_a,
        .manifest_sha256 = digest_b,
        .aggregate_sha256 = digest_d,
        .generation_path = generation_path,
        .topology_id = "topology-a",
        .topology_generation = @as(u64, 9),
        .node_id = "standby-a",
        .target_pvc_name = "standby-a-data",
        .target_pvc_uid = "pvc-uid-9",
    }, .{});
}

fn writeAuthoritativeReceipt(root: []const u8, kind: ledger_mod.Kind, generation: []const u8, body: []const u8) !void {
    const alloc = std.testing.allocator;
    const path = switch (kind) {
        .capture => try std.fs.path.join(alloc, &.{ root, seed_capture.generations_dir_name, generation, seed_capture.complete_name }),
        .activation => try std.fs.path.join(alloc, &.{ root, seed_activation.generations_dir_name, generation, seed_activation.generation_receipt_name }),
    };
    defer alloc.free(path);
    try writeFile(path, body);
    if (kind == .activation) {
        const active = try std.fs.path.join(alloc, &.{ root, seed_activation.active_receipt_name });
        defer alloc.free(active);
        try writeFile(active, body);
    }
}

fn recordCapture(ledger: *ledger_mod.Ledger, alloc: std.mem.Allocator, root: []const u8, generation: []const u8, checkpoint_lsn: u64, at_ns: u64) !ledger_mod.AppendResult {
    const receipt = try captureReceiptJSON(alloc, generation, checkpoint_lsn);
    defer alloc.free(receipt);
    try writeAuthoritativeReceipt(root, .capture, generation, receipt);
    return try ledger.recordCapture(receipt, .{ .pod_uid = "pod-primary-1", .recorded_at_unix_ns = at_ns });
}

fn recordActivation(ledger: *ledger_mod.Ledger, alloc: std.mem.Allocator, root: []const u8, generation: []const u8, checkpoint_lsn: u64, at_ns: u64) !ledger_mod.AppendResult {
    const receipt = try activationReceiptJSON(alloc, generation, checkpoint_lsn);
    defer alloc.free(receipt);
    try writeAuthoritativeReceipt(root, .activation, generation, receipt);
    return try ledger.recordActivation(receipt, .{ .pod_uid = "pod-standby-1", .recorded_at_unix_ns = at_ns });
}

test "storage.ha lifecycle ledger is durable idempotent and survives generation gc" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);

    var ledger = try ledger_mod.Ledger.open(alloc, root, .{ .wal_options = walOptions() });
    const first = try recordCapture(&ledger, alloc, root, "capture-001", 41, 101);
    try std.testing.expect(first.appended);
    const receipt = try captureReceiptJSON(alloc, "capture-001", 41);
    defer alloc.free(receipt);
    const retry = try ledger.recordCapture(receipt, .{ .pod_uid = "pod-primary-2", .recorded_at_unix_ns = 999 });
    try std.testing.expect(!retry.appended);
    try std.testing.expectEqual(first.cursor, retry.cursor);
    ledger.close();

    // Local generation GC is allowed to delete the authoritative generation;
    // the runtime-owned journal must retain the exact historical receipt.
    const generation_root = try std.fs.path.join(alloc, &.{ root, seed_capture.generations_dir_name, "capture-001" });
    defer alloc.free(generation_root);
    try std.Io.Dir.cwd().deleteTree(std.testing.io, generation_root);

    ledger = try ledger_mod.Ledger.open(alloc, root, .{ .wal_options = walOptions() });
    defer ledger.close();
    var page = try ledger.readPage(alloc, .capture, .{ .limit = 10 }, .{
        .authoritative_root = root,
        .runtime = .{ .node_id = "primary-a", .role = .primary, .pod_uid = "pod-primary-3", .fenced = false },
    });
    defer page.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), page.entries.len);
    try std.testing.expectEqual(first.cursor, page.entries[0].cursor);
    try std.testing.expectEqualStrings("capture-001", page.entries[0].generation);
    try std.testing.expectEqualStrings("topology-a", page.entries[0].topology_id);
    try std.testing.expectEqual(@as(u64, 9), page.entries[0].topology_generation);
    try std.testing.expectEqualStrings("primary-a", page.entries[0].node_id);
    try std.testing.expectEqualStrings("standby-a-data", page.entries[0].target_pvc_name);
    try std.testing.expectEqualStrings("pvc-uid-9", page.entries[0].target_pvc_uid);
    try std.testing.expectEqualStrings(receipt, page.entries[0].receipt_json);
    try std.testing.expectEqual(@as(usize, 64), page.entries[0].receipt_sha256.len);
    try std.testing.expectEqual(ledger_mod.AuthoritativeState.missing, page.entries[0].authoritative_state);
    try std.testing.expectEqualStrings("pod-primary-1", page.entries[0].pod_uid.?);
    try std.testing.expectEqual(@as(u64, 101), page.entries[0].recorded_at_unix_ns);
    try std.testing.expectEqualStrings("pod-primary-3", page.runtime.pod_uid.?);
}

const ConcurrentRecorder = struct {
    ledger: *ledger_mod.Ledger,
    receipt: []const u8,
    result: ?ledger_mod.AppendResult = null,
    err: ?anyerror = null,

    fn run(self: *@This()) void {
        self.result = self.ledger.recordCapture(self.receipt, .{ .pod_uid = "pod-concurrent", .recorded_at_unix_ns = 202 }) catch |err| {
            self.err = err;
            return;
        };
    }
};

test "storage.ha lifecycle ledger serializes concurrent publication and rejects identity mutation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const receipt = try captureReceiptJSON(alloc, "capture-concurrent", 51);
    defer alloc.free(receipt);
    try writeAuthoritativeReceipt(root, .capture, "capture-concurrent", receipt);

    var ledger = try ledger_mod.Ledger.open(alloc, root, .{ .wal_options = walOptions() });
    defer ledger.close();
    var left = ConcurrentRecorder{ .ledger = &ledger, .receipt = receipt };
    var right = ConcurrentRecorder{ .ledger = &ledger, .receipt = receipt };
    const left_thread = try std.Thread.spawn(.{}, ConcurrentRecorder.run, .{&left});
    const right_thread = try std.Thread.spawn(.{}, ConcurrentRecorder.run, .{&right});
    left_thread.join();
    right_thread.join();
    if (left.err) |err| return err;
    if (right.err) |err| return err;
    try std.testing.expectEqual(left.result.?.cursor, right.result.?.cursor);
    try std.testing.expect(left.result.?.appended != right.result.?.appended);

    const conflicting = try captureReceiptJSON(alloc, "capture-concurrent", 52);
    defer alloc.free(conflicting);
    try std.testing.expectError(error.LifecycleReceiptConflict, ledger.recordCapture(conflicting, .{}));

    var page = try ledger.readPage(alloc, .capture, .{ .limit = 10 }, .{ .authoritative_root = root });
    defer page.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), page.entries.len);
}

test "storage.ha lifecycle ledger reports bounded cursor truncation and collection gaps" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    var ledger = try ledger_mod.Ledger.open(alloc, root, .{ .max_events = 2, .wal_options = walOptions() });
    defer ledger.close();
    _ = try recordCapture(&ledger, alloc, root, "capture-001", 41, 101);
    _ = try recordCapture(&ledger, alloc, root, "capture-002", 51, 102);
    _ = try recordCapture(&ledger, alloc, root, "capture-003", 61, 103);
    _ = try recordCapture(&ledger, alloc, root, "capture-004", 71, 104);

    var first_page = try ledger.readPage(alloc, .capture, .{ .after = 1, .limit = 1 }, .{ .authoritative_root = root });
    defer first_page.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 3), first_page.first_cursor);
    try std.testing.expectEqual(@as(u64, 4), first_page.end_cursor);
    try std.testing.expectEqual(@as(u64, 3), first_page.next_cursor);
    try std.testing.expect(first_page.history_truncated);
    try std.testing.expect(first_page.gap);
    try std.testing.expect(first_page.has_more);
    try std.testing.expectEqualStrings("capture-003", first_page.entries[0].generation);

    var second_page = try ledger.readPage(alloc, .capture, .{ .after = first_page.next_cursor, .limit = 1 }, .{ .authoritative_root = root });
    defer second_page.deinit(alloc);
    try std.testing.expect(!second_page.gap);
    try std.testing.expect(!second_page.has_more);
    try std.testing.expectEqual(@as(u64, 4), second_page.next_cursor);
    try std.testing.expectEqualStrings("capture-004", second_page.entries[0].generation);
}

test "storage.ha lifecycle ledger fails closed on a torn final record" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    var ledger = try ledger_mod.Ledger.open(alloc, root, .{ .wal_options = walOptions() });
    const first = try recordActivation(&ledger, alloc, root, "activate-001", 61, 201);
    const second = try recordActivation(&ledger, alloc, root, "activate-002", 71, 202);
    try std.testing.expectEqual(first.cursor + 1, second.cursor);
    try ledger.injectTornRecordForTest(second.cursor);
    ledger.close();

    ledger = try ledger_mod.Ledger.open(alloc, root, .{ .wal_options = walOptions() });
    defer ledger.close();
    try std.testing.expectError(error.CorruptLifecycleReceiptLedger, ledger.readPage(alloc, .activation, .{ .limit = 10 }, .{ .authoritative_root = root }));
}

const ChangedCounter = struct {
    count: usize = 0,

    fn run(ptr: *anyopaque) void {
        const self: *ChangedCounter = @ptrCast(@alignCast(ptr));
        self.count += 1;
    }
};

fn fencedStore(alloc: std.mem.Allocator, root: []const u8) !fencing.Store {
    const raw = try std.fs.path.join(alloc, &.{ root, "fence.wal" });
    defer alloc.free(raw);
    const path = try alloc.dupeSentinel(u8, raw, 0);
    defer alloc.free(path);
    var store = try fencing.Store.open(alloc, path.ptr, .{});
    errdefer store.close();
    const receipt = try store.acquirePromotionFence(.{
        .identity = .{ .cluster_id = 100, .shard_id = 10, .table_id = 20, .timeline_id = 4, .epoch = 6 },
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-a",
        .new_timeline_id = 5,
        .new_epoch = 7,
        .required_lsn = 51,
        .observed_lsn = 51,
        .reason = "automatic failover",
    });
    fencing.freeReceipt(alloc, receipt);
    return store;
}

test "storage.ha lifecycle receipt route is authenticated read only fenced visible and restart durable" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const capture_root = try std.fs.path.join(alloc, &.{ root, "captures" });
    defer alloc.free(capture_root);
    const activation_root = try std.fs.path.join(alloc, &.{ root, "active" });
    defer alloc.free(activation_root);
    var capture_ledger = try ledger_mod.Ledger.open(alloc, capture_root, .{ .wal_options = walOptions() });
    _ = try recordCapture(&capture_ledger, alloc, capture_root, "capture-001", 41, 101);
    capture_ledger.close();
    var activation_ledger = try ledger_mod.Ledger.open(alloc, activation_root, .{ .wal_options = walOptions() });
    _ = try recordActivation(&activation_ledger, alloc, activation_root, "activate-001", 61, 201);
    activation_ledger.close();
    var fence_store = try fencedStore(alloc, root);
    defer fence_store.close();
    var changed = ChangedCounter{};
    const route = admin_api.routes.ha_seed_lifecycle_receipts;

    var server = http_admin.Server.initWithOptions(alloc, admin_exec.Context{ .fence_store = &fence_store }, .{
        .bearer_token = "runtime-token",
        .lifecycle_receipts = .{
            .capture_root = capture_root,
            .activation_root = activation_root,
            .wal_options = walOptions(),
            .node_id = "primary-a",
            .role = .primary,
            .pod_uid = "pod-primary-restarted",
        },
        .state_changed = .{ .ptr = &changed, .run_fn = ChangedCounter.run },
    });
    defer server.deinit();

    var unauthorized = try server.handle(.{ .method = .GET, .uri = route ++ "?kind=capture" });
    defer unauthorized.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 401), unauthorized.status);

    var capture = try server.handle(.{
        .method = .GET,
        .uri = route ++ "?kind=capture&after=0&limit=1",
        .authorization = "Bearer runtime-token",
    });
    defer capture.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), capture.status);
    try std.testing.expect(std.mem.indexOf(u8, capture.body, "\"generation\":\"capture-001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, capture.body, "\"fenced\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, capture.body, "\"pod_uid\":\"pod-primary-restarted\"") != null);
    try std.testing.expectEqual(@as(usize, 0), changed.count);

    var wrong_method = try server.handle(.{
        .method = .POST,
        .uri = route ++ "?kind=capture",
        .authorization = "Bearer runtime-token",
    });
    defer wrong_method.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 405), wrong_method.status);

    // Rebuilding the adapter models a runtime restart. The next query resumes
    // from durable cursor state, not an in-memory response cache.
    server.deinit();
    server = http_admin.Server.initWithOptions(alloc, .{ .fence_store = &fence_store }, .{
        .bearer_token = "runtime-token",
        .lifecycle_receipts = .{
            .capture_root = capture_root,
            .activation_root = activation_root,
            .wal_options = walOptions(),
            .node_id = "standby-a",
            .role = .standby,
            .pod_uid = "pod-standby-restarted",
        },
        .state_changed = .{ .ptr = &changed, .run_fn = ChangedCounter.run },
    });
    var activation = try server.handle(.{
        .method = .GET,
        .uri = route ++ "?kind=activation&after=0&limit=1",
        .authorization = "Bearer runtime-token",
    });
    defer activation.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), activation.status);
    try std.testing.expect(std.mem.indexOf(u8, activation.body, "\"target_pvc_uid\":\"pvc-uid-9\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, activation.body, "\"role\":\"standby\"") != null);
    try std.testing.expectEqual(@as(usize, 0), changed.count);
}

test "storage.ha lifecycle receipt route rejects ambiguous or malformed cursor queries" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    var ledger = try ledger_mod.Ledger.open(alloc, root, .{ .wal_options = walOptions() });
    _ = try recordCapture(&ledger, alloc, root, "capture-001", 41, 101);
    ledger.close();
    const route = admin_api.routes.ha_seed_lifecycle_receipts;
    var server = http_admin.Server.initWithOptions(alloc, .{}, .{
        .bearer_token = "runtime-token",
        .lifecycle_receipts = .{ .capture_root = root, .activation_root = root, .wal_options = walOptions() },
    });
    defer server.deinit();

    for ([_][]const u8{
        "",
        "?kind=unknown",
        "?kind=capture&kind=activation",
        "?kind=capture&after=-1",
        "?kind=capture&after=1&after=2",
        "?kind=capture&limit=0",
        "?kind=capture&limit=1001",
        "?kind=capture&limit=1&limit=2",
        "?kind=capture&unknown=value",
        "?kind=%ZZ",
    }) |query| {
        const uri = try std.fmt.allocPrint(alloc, "{s}{s}", .{ route, query });
        defer alloc.free(uri);
        var response = try server.handle(.{ .method = .GET, .uri = uri, .authorization = "Bearer runtime-token" });
        defer response.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 400), response.status);
    }
}
