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
    store.namespace_identity = @splat(9);
    var scope: artifacts.UploadScope = .{ .domain = @splat(1), .attempt = @splat(2) };
    std.mem.writeInt(u64, scope.attempt[0..8], 1, .big);
    var writer = try adapter.Adapter.init(store, scope, .write, .{});
    try std.testing.expectError(error.PgSpoolStorageUnsupported, writer.requireShared());
    const rows = 130;
    const size = 32;
    const bytes = 47 + rows * (size + 13);
    const plan: spool.Plan = .{ .binding = .{ .feed = @splat(1), .epoch = 1, .definition = @splat(2), .projection = @splat(3), .storage = writer.storageIdentity() }, .before = @splat(5), .after = @splat(6), .completion = @splat(7), .start_lsn = 10, .bytes = bytes, .commit_offset = bytes - 26, .xid = 42, .commit_time = -1 };
    const sealed = try spool.testBuild(a, &writer, plan, rows, size);
    _ = try writer.recordClosure(sealed);
    const id = try scope.artifactId(&std.fmt.bytesToHex(sealed.identity, .lower));
    try std.testing.expect(try writer.store.retainedClosure(&id, .{}));
    fs.deinit();
    fs = try FsStore.initExisting(a, path);
    store = fs.artifactStore();
    store.namespace_identity = @splat(9);
    var reader_store = try adapter.Adapter.init(store, scope, .read, .{});
    const reader = try spool.Reader.open(a, &reader_store, sealed.identity, plan.binding);
    const row = (try reader.message(a, &reader_store, 21 + 129 * 45, 1024)).?;
    defer a.free(row);
    try std.testing.expect(std.mem.indexOf(u8, row, "0000000000000129") != null);
    try std.testing.expect(try reader_store.store.retainedClosure(&id, .{}));
    try std.testing.expectError(error.ReadOnly, reader_store.put(spool.digest(row), row));
    // Store payload access cannot bypass checksum/size verification.
    try std.testing.expectError(error.ArtifactIntegrityMismatch, reader_store.get(a, spool.digest(row), row.len + 1));
}
/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
