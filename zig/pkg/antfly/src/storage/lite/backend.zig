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

const std = @import("std");
const db_mod = @import("../db/db.zig");
const backend_erased = @import("../backend_erased.zig");
const lsm_backend = @import("../lsm_backend/mod.zig");
const docstore = @import("docstore.zig");

const Allocator = std.mem.Allocator;

pub const native = @import("native.zig");
pub const CheckReport = native.CheckReport;
pub const VacuumReport = lsm_backend.AfliteContainerStorage.VacuumReport;

pub const EngineKind = enum {
    /// Temporary bridge implementation. The public Lite API should depend on
    /// this module, not directly on the container, so the native backend can
    /// replace it without changing callers.
    bridge_lsm_container,

    /// Native v1 `.aflite` file engine. It owns real Lite pages and checkpoint
    /// roots, but does not yet expose the full Antfly DB storage interface.
    native_single_file,
};

pub const OpenOptions = struct {
    engine: EngineKind = .bridge_lsm_container,
    read_only: bool = false,
};

pub fn checkFile(allocator: Allocator, path: []const u8) !CheckReport {
    if (try hasNativeMagic(allocator, path)) return try native.checkFile(allocator, path);
    return toCheckReport(try lsm_backend.AfliteContainerStorage.checkFile(allocator, path));
}

pub const Handle = struct {
    allocator: Allocator,
    engine: EngineKind,
    bridge_storage: ?*lsm_backend.AfliteContainerStorage = null,
    native_docstore: ?*docstore.Store = null,
    native_runtime_store: ?*backend_erased.Store = null,

    pub fn open(allocator: Allocator, path: []const u8, opts: OpenOptions) !Handle {
        return switch (opts.engine) {
            .bridge_lsm_container => try openBridgeLsmContainer(allocator, path, opts),
            .native_single_file => try openNativeSingleFile(allocator, path, opts),
        };
    }

    pub fn deinit(self: *Handle) void {
        switch (self.engine) {
            .bridge_lsm_container => {
                if (self.bridge_storage) |storage| {
                    storage.deinit();
                    self.allocator.destroy(storage);
                    self.bridge_storage = null;
                }
            },
            .native_single_file => {
                if (self.native_runtime_store) |runtime_store| {
                    runtime_store.deinit();
                    self.allocator.destroy(runtime_store);
                    self.native_runtime_store = null;
                }
                if (self.native_docstore) |store| {
                    store.close();
                    self.allocator.destroy(store);
                    self.native_docstore = null;
                }
            },
        }
        self.* = undefined;
    }

    pub fn configureDbOpenOptions(self: *Handle, opts: *db_mod.OpenOptions) !void {
        switch (self.engine) {
            .bridge_lsm_container => {
                const storage = self.bridge_storage.?.storage();
                opts.storage = storage;
                opts.index_backends.text_lsm_storage = storage;
                opts.index_backends.dense_lsm_storage = storage;
                opts.index_backends.sparse_lsm_storage = storage;
                opts.index_backends.graph_lsm_storage = storage;
                opts.external_derived_checkpoints = false;
            },
            .native_single_file => {
                opts.primary_backend = .{ .mem = .{} };
                opts.primary_runtime_store = self.native_runtime_store.?;
                opts.external_derived_checkpoints = false;
            },
        }
    }

    pub fn check(self: *Handle) !CheckReport {
        return switch (self.engine) {
            .bridge_lsm_container => toCheckReport(try self.bridge_storage.?.check()),
            .native_single_file => try self.native_docstore.?.file.check(),
        };
    }

    pub fn vacuum(self: *Handle) !VacuumReport {
        return switch (self.engine) {
            .bridge_lsm_container => try self.bridge_storage.?.vacuum(),
            .native_single_file => error.NativeVacuumNotImplemented,
        };
    }
};

fn openBridgeLsmContainer(allocator: Allocator, path: []const u8, opts: OpenOptions) !Handle {
    var storage = try allocator.create(lsm_backend.AfliteContainerStorage);
    errdefer allocator.destroy(storage);

    storage.* = try lsm_backend.AfliteContainerStorage.openWithOptions(allocator, path, .{
        .read_only = opts.read_only,
    });
    errdefer storage.deinit();

    return .{
        .allocator = allocator,
        .engine = .bridge_lsm_container,
        .bridge_storage = storage,
    };
}

fn openNativeSingleFile(allocator: Allocator, path: []const u8, opts: OpenOptions) !Handle {
    const store = try allocator.create(docstore.Store);
    errdefer allocator.destroy(store);

    store.* = try docstore.Store.open(allocator, path, opts.read_only);
    errdefer store.close();

    const runtime_store = try allocator.create(backend_erased.Store);
    errdefer allocator.destroy(runtime_store);

    runtime_store.* = try store.runtimeStore(allocator);
    errdefer runtime_store.deinit();

    return .{
        .allocator = allocator,
        .engine = .native_single_file,
        .native_docstore = store,
        .native_runtime_store = runtime_store,
    };
}

fn toCheckReport(report: lsm_backend.AfliteContainerStorage.CheckReport) CheckReport {
    return .{
        .valid = report.valid,
        .file_size = report.file_size,
        .valid_prefix_size = report.valid_prefix_size,
        .tail_bytes = report.tail_bytes,
        .record_count = report.record_count,
        .live_file_count = report.live_file_count,
        .live_bytes = report.live_bytes,
        .compact_size = report.compact_size,
        .reclaimable_bytes = report.reclaimable_bytes,
        .issue = report.issue,
    };
}

fn hasNativeMagic(allocator: Allocator, path: []const u8) !bool {
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only });
    defer file.close(io);

    var prefix: [native.magic.len]u8 = undefined;
    const read = try file.readPositionalAll(io, &prefix, 0);
    return read == native.magic.len and std.mem.eql(u8, &prefix, native.magic);
}

fn testPath(allocator: Allocator, tmp: std.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

test "lite backend native engine creates and checks aflite file" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-backend.aflite");
    defer allocator.free(path);

    var handle = try Handle.open(allocator, path, .{ .engine = .native_single_file });
    defer handle.deinit();

    try handle.native_docstore.?.file.putDocument("doc:1", "value");

    const report = try handle.check();
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(@as(u64, 1), report.record_count);

    var db_opts = db_mod.OpenOptions{};
    try handle.configureDbOpenOptions(&db_opts);
    try std.testing.expect(db_opts.primary_runtime_store != null);
    try std.testing.expect(db_opts.primary_backend == .mem);
    try std.testing.expectError(error.NativeVacuumNotImplemented, handle.vacuum());
}

test "lite backend native engine can back db primary documents" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-db.aflite");
    defer allocator.free(path);

    {
        var handle = try Handle.open(allocator, path, .{ .engine = .native_single_file });
        defer handle.deinit();

        var db_opts = db_mod.OpenOptions{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .start_optional_runtimes = false,
            .ttl_cleanup = .{ .enabled = false },
        };
        try handle.configureDbOpenOptions(&db_opts);

        var db = try db_mod.DB.open(allocator, path, db_opts);
        defer db.close();

        try db.batch(.{
            .writes = &.{.{
                .key = "doc:lite-native",
                .value = "{\"name\":\"native\"}",
            }},
            .sync_level = .write,
        });

        const value = try db.get(allocator, "doc:lite-native") orelse return error.MissingNativeLiteDocument;
        defer allocator.free(value);
        try std.testing.expectEqualStrings("{\"name\":\"native\"}", value);
    }

    {
        var handle = try Handle.open(allocator, path, .{
            .engine = .native_single_file,
            .read_only = true,
        });
        defer handle.deinit();

        var db_opts = db_mod.OpenOptions{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .start_optional_runtimes = false,
            .ttl_cleanup = .{ .enabled = false },
        };
        try handle.configureDbOpenOptions(&db_opts);

        var db = try db_mod.DB.open(allocator, path, db_opts);
        defer db.close();

        const value = try db.get(allocator, "doc:lite-native") orelse return error.MissingNativeLiteDocument;
        defer allocator.free(value);
        try std.testing.expectEqualStrings("{\"name\":\"native\"}", value);
    }
}

test "lite backend native read-only open requires an existing file" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-missing.aflite");
    defer allocator.free(path);

    try std.testing.expectError(error.FileNotFound, Handle.open(allocator, path, .{
        .engine = .native_single_file,
        .read_only = true,
    }));
}
