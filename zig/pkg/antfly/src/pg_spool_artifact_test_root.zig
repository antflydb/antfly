// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
pub const antfly_sources = @import("source_owner_physical.zig");
const spool = @import("metadata/pg_spool.zig");
const adapter = @import("metadata/pg_spool_artifacts.zig");
const slot = @import("metadata/pg_slot.zig");
const artifacts = @import("serverless/artifacts/store.zig");
const FsStore = @import("serverless/artifacts/fs_store.zig").FsStore;
test "pg spool artifact adapter persists verified pages closure and exact replay across reopen" {
    const a = std.testing.allocator;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/pg-spool", .{temp.sub_path});
    defer a.free(path);
    var fs = try FsStore.init(a, path);
    defer fs.deinit();
    var store = fs.artifactStore();
    try std.testing.expectError(error.PgSpoolStorageUnsupported, adapter.Adapter.init(store, .{ .domain = @splat(1), .attempt = @splat(1) }, .write, .{}));
    store = try fs.spoolArtifactStore(.{});
    const namespace = store.namespace_identity.?;
    var state = try slot.State.initialize(@splat(1), "slot", @splat(5), 1024 * 1024);
    state = try state.acquire(@splat(2), 0, 100, 150);
    const session = state.session.?;
    var scope: artifacts.UploadScope = .{ .domain = slot.spool_control.Scope.domainFor(slot.feedId(state.database, state.name), 1), .attempt = @splat(2) };
    std.mem.writeInt(u64, scope.attempt[0..8], 1, .big);
    var no_proof_vtable = store.vtable.*;
    no_proof_vtable.scoped_uploads_reclaimed = null;
    var no_proof_store = store;
    no_proof_store.vtable = &no_proof_vtable;
    try std.testing.expectError(error.PgSpoolStorageUnsupported, adapter.Adapter.init(no_proof_store, scope, .write, .{}));
    var writer = try adapter.Adapter.init(store, scope, .write, .{});
    try std.testing.expectError(error.PgSpoolStorageUnsupported, writer.requireShared());
    const rows = 130;
    const size = 32;
    const bytes = 47 + rows * (size + 13);
    const plan: spool.Plan = .{ .binding = .{ .feed = slot.feedId(state.database, state.name), .epoch = 1, .definition = @splat(2), .projection = @splat(3), .storage = writer.storageIdentity() }, .before = @splat(5), .after = @splat(6), .completion = @splat(7), .start_lsn = state.next_lsn, .bytes = bytes, .commit_offset = bytes - 26, .xid = 42, .commit_time = -1 };
    const accounting_scope: slot.spool_control.Scope = .{ .namespace = namespace, .domain = scope.domain, .attempt = scope.attempt };
    const reserved = try state.reserveSpool(a, session, 101, 1, accounting_scope, slot.spool_control.Spec.fromPlan(plan));
    defer a.free(reserved.spool.records);
    const sealed = try spool.testBuild(a, &writer, plan, rows, size);
    _ = try writer.recordClosure(sealed);
    const published = try reserved.publishSpool(a, session, 102, 1, sealed.identity, sealed.serialization);
    defer a.free(published.spool.records);
    defer a.free(published.entries);
    try published.validate();
    const id = try scope.artifactId(&std.fmt.bytesToHex(sealed.identity, .lower));
    try std.testing.expect(try writer.store.retainedClosure(&id, .{}));
    fs.deinit();
    fs = try FsStore.initExisting(a, path);
    store = try fs.spoolArtifactStore(.{});
    try std.testing.expectEqual(namespace, store.namespace_identity.?);
    var reader_store = try adapter.Adapter.init(store, scope, .read, .{});
    const reader = try spool.Reader.open(a, &reader_store, sealed.identity, plan.binding);
    const row = (try reader.message(a, &reader_store, 21 + 129 * 45, 1024)).?;
    defer a.free(row);
    try std.testing.expect(std.mem.indexOf(u8, row, "0000000000000129") != null);
    try std.testing.expect(try reader_store.store.retainedClosure(&id, .{}));
    try std.testing.expectError(error.ReadOnly, reader_store.put(spool.digest(row), row));
    // Store payload access cannot bypass checksum/size verification.
    try std.testing.expectError(error.ArtifactIntegrityMismatch, reader_store.get(a, spool.digest(row), row.len + 1));
    const confirmed = try published.confirm(a, session, 103, published.next_lsn);
    defer a.free(confirmed.spool.records);
    defer a.free(confirmed.entries);
    try std.testing.expectEqual(published.spool.charged_bytes, confirmed.spool.charged_bytes);
    try std.testing.expectEqual(.garbage, confirmed.spool.records[0].phase);
    // Drain all writers behind a durable floor before deleting any object or
    // returning quota. The floor survives removal of the attempt inventory.
    try reader_store.store.retireScopedUploads(scope.domain, 1, .{});
    try std.testing.expectError(error.ArtifactUploadAttemptRetired, writer.put(spool.digest("late"), "late"));
    try std.testing.expectError(error.ArtifactUploadAttemptRetired, writer.recordClosure(sealed));
    const Collector = struct {
        store: *artifacts.ArtifactStore,
        scope: artifacts.UploadScope,
        token: ?[99]u8 = null,
        visited: usize = 0,
        fn visit(raw: *anyopaque, actual: artifacts.UploadScope, artifact: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(self.scope, actual);
            try self.store.delete(artifact);
            self.visited += 1;
        }
        fn checkpoint(raw: *anyopaque, token: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (token.len != 99) return error.TestUnexpectedResult;
            self.token = token[0..99].*;
        }
    };
    var collector: Collector = .{ .store = &reader_store.store, .scope = scope };
    var turns: usize = 0;
    while (true) {
        turns += 1;
        try std.testing.expect(turns < 1000);
        // Do not mutate the visitor's borrowed continuation when checkpointing.
        const prior = collector.token;
        reader_store.store.visitScopedUploads(scope.domain, .{ .ptr = &collector, .visit = Collector.visit, .checkpoint = Collector.checkpoint, .continuation = if (prior) |*token| token else null, .only_attempt = scope.attempt, .fencing_floor = 1, .fencing_cutoff = 2, .max_entries = 7, .cleanup_staging = true, .reclaim_retired_inventory = true }, .{}) catch |err| switch (err) {
            error.ArtifactEnumerationPaused => continue,
            else => return err,
        };
        break;
    }
    try std.testing.expect(collector.visited > 130);
    try std.testing.expect(turns > 1);
    const payload_attempt = try std.fmt.allocPrint(a, "{s}/graph/{s}/{s}", .{ fs.root_dir, std.fmt.bytesToHex(&scope.domain, .lower), std.fmt.bytesToHex(&scope.attempt, .lower) });
    defer a.free(payload_attempt);
    const inventory_attempt = try std.fmt.allocPrint(a, "{s}/{s}/{s}", .{ fs.inventory_root, std.fmt.bytesToHex(&scope.domain, .lower), std.fmt.bytesToHex(&scope.attempt, .lower) });
    defer a.free(inventory_attempt);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, payload_attempt, .{}));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, inventory_attempt, .{}));
    try std.testing.expect(!try reader_store.store.retainedClosure(&id, .{}));
    const checkpointed = try confirmed.checkpointSpoolCleanup(a, 1, plan.binding.storage, 0, .{ .revision = 1, .fenced = true, .complete = true, .proof = @splat(5) });
    defer a.free(checkpointed.spool.records);
    const reclaimed = try checkpointed.reclaimSpool(a, 1, plan.binding.storage, @splat(5));
    defer a.free(reclaimed.spool.records);
    try reclaimed.validate();
    try std.testing.expectEqual(@as(u64, 0), reclaimed.spool.charged_bytes);
    // Inventory is advisory. Rebuilding it cannot erase the authoritative
    // payload-namespace floor and revive an old upload reservation.
    try std.Io.Dir.cwd().deleteTree(std.testing.io, fs.inventory_root);
    fs.deinit();
    fs = try FsStore.initExisting(a, path);
    var reopened_store = fs.artifactStore();
    try std.testing.expectError(error.ArtifactUploadAttemptRetired, reopened_store.putScoped(scope, "late after reopen", .{}));
    // Lower repeated floors cannot reopen retired upload IDs.
    try reopened_store.retireScopedUploads(scope.domain, 1, .{});
    try std.testing.expectError(error.ArtifactUploadAttemptRetired, reopened_store.putScoped(scope, "still retired", .{}));
    var fresh = scope;
    std.mem.writeInt(u64, fresh.attempt[0..8], 2, .big);
    var next_object = try reopened_store.putScoped(fresh, "new reservation", .{});
    defer next_object.deinit(a);
}
/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};

test "pg spool artifact durable retirement drains writers and rejects corrupt floors" {
    const a = std.testing.allocator;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/fenced-spool", .{temp.sub_path});
    defer a.free(path);
    var first = try FsStore.init(a, path);
    defer first.deinit();
    const other_inventory = try std.fmt.allocPrint(a, "{s}/alternate-inventory", .{path});
    defer a.free(other_inventory);
    var second = try FsStore.initExistingWithInventory(a, path, other_inventory);
    defer second.deinit();
    const scope = try artifacts.UploadScope.forPublication(@splat(41), 1, std.testing.io);
    var held = try first.scopeWriteGuard(std.testing.io, scope, .{});
    var held_live = true;
    defer if (held_live) held.deinit();
    const Stop = struct {
        checks: u32 = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            if (self.checks >= 4) return error.DeadlineExceeded;
        }
    };
    var stop: Stop = .{};
    try std.testing.expectError(error.DeadlineExceeded, second.retireScopedUploads(scope.domain, 1, .{ .ptr = &stop, .check_fn = Stop.check }));
    held.deinit();
    held_live = false;
    try second.retireScopedUploads(scope.domain, 1, .{});
    var store = first.artifactStore();
    try std.testing.expectError(error.ArtifactUploadAttemptRetired, store.putScoped(scope, "old writer", .{}));
    var fresh = scope;
    std.mem.writeInt(u64, fresh.attempt[0..8], 2, .big);
    var allowed = try store.putScoped(fresh, "new reservation", .{});
    defer allowed.deinit(a);
    const floor_path = try std.fmt.allocPrint(a, "{s}/.upload-fences-v1/floors/{s}", .{ first.root_dir, std.fmt.bytesToHex(&scope.domain, .lower) });
    defer a.free(floor_path);
    {
        const file = try std.Io.Dir.cwd().createFile(std.testing.io, floor_path, .{});
        defer file.close(std.testing.io);
        var buffer: [32]u8 = undefined;
        var writer = file.writer(std.testing.io, &buffer);
        try writer.interface.writeAll("broken");
        try writer.end();
        try file.sync(std.testing.io);
    }
    // Neither upload nor lower/idempotent retirement can erase a corrupt floor.
    try std.testing.expectError(error.ArtifactUploadFenceCorrupt, store.putScoped(fresh, "must fail closed", .{}));
    try std.testing.expectError(error.ArtifactUploadFenceCorrupt, second.retireScopedUploads(scope.domain, 1, .{}));
}

test "pg spool artifact cleanup worker resumes durable cursors across reopen and unknown outcomes" {
    try @import("pg_spool_cleanup_test.zig").run();
}

test "pg spool artifact upload worker fences retries and publishes only frozen historical bytes" {
    try @import("pg_spool_upload_test.zig").run();
}

test "pg spool artifact preflight identity distinguishes message boundaries" {
    const upload = @import("metadata/pg_spool_upload.zig");
    var first = upload.WireHasher.init();
    try first.add("R");
    try first.add("IUD");
    var second = upload.WireHasher.init();
    try second.add("RI");
    try second.add("UD");
    try std.testing.expect(!std.mem.eql(u8, &first.final(), &second.final()));
    try std.testing.expectError(error.InvalidPgSpool, second.add(""));
}

test "pg spool artifact namespace is durable and rejects a corrupt or replaced inventory" {
    const a = std.testing.allocator;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/namespace", .{temp.sub_path});
    defer a.free(path);
    const identity_path = try std.fs.path.join(a, &.{ path, ".upload-inventory", ".spool-identity-v1" });
    defer a.free(identity_path);
    const original = blk: {
        var store = try FsStore.init(a, path);
        defer store.deinit();
        break :blk (try store.spoolArtifactStore(.{})).namespace_identity.?;
    };
    {
        var reopened = try FsStore.initExisting(a, path);
        defer reopened.deinit();
        try std.testing.expectEqual(original, (try reopened.spoolArtifactStore(.{})).namespace_identity.?);
        try @import("serverless/artifacts/fs_store.zig").writeFileAtomicallyWithCancellation(identity_path, &@as([68]u8, @splat(1)), .{});
        try std.testing.expectError(error.ArtifactUploadFenceCorrupt, reopened.spoolArtifactStore(.{}));
    }
    const retired = try std.fmt.allocPrint(a, "{s}-retired", .{path});
    defer a.free(retired);
    const cwd = std.Io.Dir.cwd();
    try cwd.rename(path, cwd, retired, std.testing.io);
    defer cwd.deleteTree(std.testing.io, retired) catch {};
    var replacement = try FsStore.init(a, path);
    defer replacement.deinit();
    try std.testing.expect(!std.meta.eql(original, (try replacement.spoolArtifactStore(.{})).namespace_identity.?));
}
