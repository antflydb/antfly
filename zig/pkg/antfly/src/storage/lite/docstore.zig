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

//! Runtime document-store adapter over native `.aflite` document pages.

const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const backend_adapter = @import("../backend_adapter.zig");
const backend_erased = @import("../backend_erased.zig");
const backend_types = @import("../backend_types.zig");
const native = @import("native.zig");

const Allocator = std.mem.Allocator;

pub const Store = struct {
    allocator: Allocator,
    file: native.NativeFile,
    read_only: bool = false,
    mutex: std.atomic.Mutex = .unlocked,
    writer_active: bool = false,

    pub fn open(allocator: Allocator, path: []const u8, read_only: bool) !Store {
        const file = native.NativeFile.open(allocator, path, read_only) catch |err| switch (err) {
            error.FileNotFound => {
                if (read_only) return err;
                return .{
                    .allocator = allocator,
                    .file = try native.NativeFile.create(allocator, path),
                    .read_only = read_only,
                };
            },
            else => return err,
        };
        return .{
            .allocator = allocator,
            .file = file,
            .read_only = read_only,
        };
    }

    pub fn close(self: *Store) void {
        self.file.close();
        self.* = undefined;
    }

    pub fn backendStore(self: *Store) NativeBackendStore {
        return NativeBackendStore.init(self);
    }

    pub fn runtimeStore(self: *Store, allocator: Allocator) !backend_erased.Store {
        return try backend_erased.storeFrom(allocator, self.backendStore());
    }

    pub fn vacuum(self: *Store) !native.VacuumReport {
        if (self.read_only) return error.ReadOnly;
        lockStore(self);
        defer self.mutex.unlock();
        return try self.file.vacuum();
    }

    const NativeBackendStore = backend_adapter.Store(Store, Txn, Txn, Txn, .{
        .capabilities = capabilities,
        .begin_read = beginRead,
        .begin_write = beginWrite,
        .begin_batch = beginBatch,
        .begin_batch_with_options = beginBatchWithOptions,
    });

    pub fn capabilities(_: *Store) backend_types.Capabilities {
        return .{
            .ordered_ranges = true,
            .reverse_ranges = true,
            .cursors = true,
            .native_namespaces = false,
            .write_batches = .atomic,
            .single_writer = true,
            .read_snapshots = .snapshot,
        };
    }

    pub fn beginRead(self: *Store) !Txn {
        return try Txn.openRead(self);
    }

    pub fn beginWrite(self: *Store) !Txn {
        if (self.read_only) return error.ReadOnly;
        return try Txn.openWrite(self);
    }

    pub fn beginBatch(self: *Store) !Txn {
        return try self.beginWrite();
    }

    pub fn beginBatchWithOptions(self: *Store, options: backend_types.BatchOptions) !Txn {
        _ = options;
        return try self.beginBatch();
    }
};

const PendingMutation = struct {
    key: []u8,
    value: ?[]u8 = null,
};

pub const Txn = struct {
    allocator: Allocator,
    store: ?*Store = null,
    docs: []native.OwnedDocument = &.{},
    pending: std.ArrayListUnmanaged(PendingMutation) = .empty,
    read_only: bool = true,
    writer_reserved: bool = false,

    pub fn openRead(store: *Store) !Txn {
        lockStore(store);
        defer store.mutex.unlock();
        return .{
            .allocator = store.allocator,
            .docs = try store.file.snapshotDocumentsAlloc(store.allocator),
            .read_only = true,
        };
    }

    pub fn openWrite(store: *Store) !Txn {
        lockStore(store);
        defer store.mutex.unlock();

        if (store.writer_active) return error.FileBusy;
        store.writer_active = true;
        errdefer store.writer_active = false;

        return .{
            .allocator = store.allocator,
            .store = store,
            .docs = try store.file.snapshotDocumentsAlloc(store.allocator),
            .read_only = false,
            .writer_reserved = true,
        };
    }

    pub fn abort(self: *Txn) void {
        self.freePending();
        native.NativeFile.freeSnapshotDocuments(self.allocator, self.docs);
        self.releaseWriterSlot();
        self.* = undefined;
    }

    pub fn commit(self: *Txn) !void {
        const store = self.store orelse return error.ReadOnly;
        const allocator = self.allocator;
        var mutations = try allocator.alloc(native.DocumentMutation, self.pending.items.len);
        defer allocator.free(mutations);
        for (self.pending.items, 0..) |pending, i| {
            mutations[i] = .{
                .key = pending.key,
                .value = pending.value orelse "",
                .is_delete = pending.value == null,
            };
        }

        lockStore(store);
        defer store.mutex.unlock();
        errdefer {
            if (self.writer_reserved) {
                store.writer_active = false;
                self.writer_reserved = false;
            }
        }
        try store.file.putDocumentBatch(mutations);
        if (self.writer_reserved) {
            store.writer_active = false;
            self.writer_reserved = false;
        }

        self.freePending();
        native.NativeFile.freeSnapshotDocuments(self.allocator, self.docs);
        self.* = undefined;
    }

    pub fn get(self: *Txn, key: []const u8) ![]const u8 {
        if (self.pending.items.len > 0) {
            var i = self.pending.items.len;
            while (i > 0) {
                i -= 1;
                const pending = self.pending.items[i];
                if (!std.mem.eql(u8, pending.key, key)) continue;
                return pending.value orelse error.NotFound;
            }
        }
        const idx = lowerBoundDocs(self.docs, key);
        if (idx >= self.docs.len or !std.mem.eql(u8, self.docs[idx].key, key)) return error.NotFound;
        return self.docs[idx].value;
    }

    pub fn put(self: *Txn, key: []const u8, value: []const u8) !void {
        if (self.read_only) return error.ReadOnly;
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_value);
        try self.pending.append(self.allocator, .{ .key = owned_key, .value = owned_value });
    }

    pub fn delete(self: *Txn, key: []const u8) !void {
        if (self.read_only) return error.ReadOnly;
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        try self.pending.append(self.allocator, .{ .key = owned_key });
    }

    pub fn openCursor(self: *Txn) !Cursor {
        return .{ .txn = self };
    }

    fn freePending(self: *Txn) void {
        for (self.pending.items) |pending| {
            self.allocator.free(pending.key);
            if (pending.value) |value| self.allocator.free(value);
        }
        self.pending.deinit(self.allocator);
        self.pending = .empty;
    }

    fn releaseWriterSlot(self: *Txn) void {
        if (!self.writer_reserved) return;
        const store = self.store orelse return;
        lockStore(store);
        defer store.mutex.unlock();
        store.writer_active = false;
        self.writer_reserved = false;
    }
};

pub const Cursor = struct {
    txn: *Txn,
    current: ?usize = null,
    upper_bound: ?[]const u8 = null,

    pub fn close(_: *Cursor) void {}

    pub fn first(self: *Cursor) !backend_adapter.Entry {
        if (self.txn.docs.len == 0) return error.NotFound;
        self.current = 0;
        return self.entryAt(0);
    }

    pub fn last(self: *Cursor) !backend_adapter.Entry {
        if (self.txn.docs.len == 0) return error.NotFound;
        const idx = self.txn.docs.len - 1;
        self.current = idx;
        return self.entryAt(idx);
    }

    pub fn next(self: *Cursor) !backend_adapter.Entry {
        const current = self.current orelse return error.NotFound;
        const next_idx = current + 1;
        if (next_idx >= self.txn.docs.len) return error.NotFound;
        self.current = next_idx;
        return self.entryAt(next_idx);
    }

    pub fn prev(self: *Cursor) !backend_adapter.Entry {
        const current = self.current orelse return error.NotFound;
        if (current == 0) return error.NotFound;
        const prev_idx = current - 1;
        self.current = prev_idx;
        return self.entryAt(prev_idx);
    }

    pub fn seekAtOrAfter(self: *Cursor, key: []const u8) !backend_adapter.Entry {
        const idx = lowerBoundDocs(self.txn.docs, key);
        if (idx >= self.txn.docs.len) return error.NotFound;
        self.current = idx;
        return self.entryAt(idx);
    }

    pub fn seekAtOrBefore(self: *Cursor, key: []const u8) !backend_adapter.Entry {
        const idx = lowerBoundDocs(self.txn.docs, key);
        if (idx < self.txn.docs.len and std.mem.eql(u8, self.txn.docs[idx].key, key)) {
            self.current = idx;
            return self.entryAt(idx);
        }
        if (idx == 0) return error.NotFound;
        self.current = idx - 1;
        return self.entryAt(idx - 1);
    }

    pub fn setUpperBound(self: *Cursor, upper: ?[]const u8) void {
        self.upper_bound = upper;
    }

    fn entryAt(self: *Cursor, idx: usize) !backend_adapter.Entry {
        const doc = self.txn.docs[idx];
        if (self.upper_bound) |upper| {
            if (std.mem.order(u8, doc.key, upper) != .lt) return error.NotFound;
        }
        return .{ .key = doc.key, .value = doc.value };
    }
};

fn lowerBoundDocs(docs: []const native.OwnedDocument, key: []const u8) usize {
    var low: usize = 0;
    var high: usize = docs.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (std.mem.order(u8, docs[mid].key, key) == .lt) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low;
}

fn lockStore(store: *Store) void {
    platform_sync.lockYielding(&store.mutex);
}

fn testPath(allocator: Allocator, tmp: std.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

test "lite native docstore runtime persists atomic batch" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore.aflite");
    defer allocator.free(path);

    {
        var store = try Store.open(allocator, path, false);
        defer store.close();

        var runtime = try store.runtimeStore(allocator);
        defer runtime.deinit();

        var batch = try runtime.beginBatch();
        try batch.put("doc:b", "second");
        try batch.put("doc:a", "first");
        try batch.put("doc:b", "newer second");
        try batch.put("doc:c", "deleted");
        try batch.delete("doc:c");
        try batch.commit();
    }

    var reopened = try Store.open(allocator, path, true);
    defer reopened.close();

    var runtime = try reopened.runtimeStore(allocator);
    defer runtime.deinit();

    var read = try runtime.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("first", try read.get("doc:a"));
    try std.testing.expectEqualStrings("newer second", try read.get("doc:b"));
    try std.testing.expectError(error.NotFound, read.get("doc:c"));
}

test "lite native docstore runtime scans ordered snapshot" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-scan.aflite");
    defer allocator.free(path);

    var store = try Store.open(allocator, path, false);
    defer store.close();

    var runtime = try store.runtimeStore(allocator);
    defer runtime.deinit();

    {
        var batch = try runtime.beginBatch();
        try batch.put("doc:b", "second");
        try batch.put("doc:a", "first");
        try batch.put("doc:c", "third");
        try batch.commit();
    }

    var read = try runtime.beginRead();
    defer read.abort();

    var cursor = try read.openCursor();
    defer cursor.close();
    const first = (try cursor.first()).?;
    try std.testing.expectEqualStrings("doc:a", first.key);
    const next = (try cursor.next()).?;
    try std.testing.expectEqualStrings("doc:b", next.key);
    const seek = (try cursor.seekAtOrAfter("doc:bb")).?;
    try std.testing.expectEqualStrings("doc:c", seek.key);
}

test "lite native docstore reserves one writer until abort or commit" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-single-writer.aflite");
    defer allocator.free(path);

    var store = try Store.open(allocator, path, false);
    defer store.close();

    var writer = try store.beginWrite();
    try writer.put("doc:a", "first");
    try std.testing.expectError(error.FileBusy, store.beginWrite());

    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get("doc:a"));

    writer.abort();

    var committed = try store.beginWrite();
    try committed.put("doc:a", "committed");
    try committed.commit();

    var next_writer = try store.beginWrite();
    defer next_writer.abort();
    try std.testing.expectEqualStrings("committed", try next_writer.get("doc:a"));
}
