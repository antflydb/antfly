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

//! Storage access to durable pending replication effects. Callers hold their
//! mutation/apply fence when reading or clearing. Publication and HA policy
//! are deliberately absent from this owner.
const std = @import("std");
const Allocator = std.mem.Allocator;
const docstore = @import("../docstore.zig");
const outbox = @import("durable_outbox.zig");
const admission = @import("../../common/read_admission.zig");

/// Caller-root ownership of a captured immutable v2 intent. Each payload view
/// is a synchronous borrow; the owner outlives it. Its bytes remain valid after
/// lookup/import leases retire and after the source DocStore closes.
pub const OwnedIntent = struct {
    raw: @import("../backend_erased.zig").OwnedValue,
    kind: outbox.Kind,
    identity: outbox.RecoveryIdentity,
    from_lsn: u64,

    pub fn borrow(self: *const @This()) outbox.DurableReplicationOutbox {
        const raw = self.raw.bytes();
        return .{ .from_lsn = self.from_lsn, .payload = raw[outbox.replication_outbox_header_len .. raw.len - outbox.replication_outbox_checksum_len], .recovery_identity = self.identity };
    }
    pub fn deinit(self: *@This()) void {
        self.raw.deinit();
        self.* = undefined;
    }
};

/// Point capture of a known v2 key, followed by bounded AHO1 verification.
/// Key enumeration and final publication/generation checks belong to callers.
pub const OwnedIntentRead = struct {
    pub const Status = enum { pending, ready };
    lookup: ?docstore.DocStore.OwnedInternalValueRead,
    value: ?@import("../backend_erased.zig").OwnedValue = null,
    decoder: outbox.DecodeCursor = .{},
    kind: outbox.Kind,
    identity: outbox.RecoveryIdentity,
    failure: ?anyerror = null,
    closed: bool = false,

    pub fn step(self: *@This(), quantum: usize, control: admission.Port) !Status {
        if (quantum == 0 or quantum > 64 * 1024) return error.InvalidArgument;
        if (self.failure) |err| return err;
        if (self.closed) return error.TransactionClosed;
        return self.advance(quantum, control) catch |err| {
            const cause = blk: {
                control.checkpoint() catch |original| break :blk original;
                break :blk err;
            };
            self.deinit();
            self.failure = cause;
            return cause;
        };
    }
    fn advance(self: *@This(), quantum: usize, control: admission.Port) !Status {
        try control.checkpoint();
        if (self.lookup) |*lookup| {
            if (try lookup.step(quantum, control) == .ready) {
                self.value = try lookup.takeOutput();
                self.lookup = null;
            }
            return .pending;
        }
        return if (try self.decoder.step(self.value.?.bytes(), quantum, control) == .ready) .ready else .pending;
    }
    pub fn takeOutput(self: *@This()) !OwnedIntent {
        if (self.failure) |err| return err;
        if (self.closed or self.lookup != null) return error.InvalidArgument;
        const raw = self.value orelse return error.InvalidArgument;
        const checked = try self.decoder.view(raw.bytes());
        self.value = null;
        self.closed = true;
        return .{ .raw = raw, .kind = self.kind, .identity = self.identity, .from_lsn = checked.from_lsn };
    }
    pub fn deinit(self: *@This()) void {
        if (self.lookup) |*lookup| lookup.deinit();
        self.lookup = null;
        if (self.value) |*value| value.deinit();
        self.value = null;
        self.closed = true;
    }
};

pub fn beginOwnedIntentRead(allocator: Allocator, store: *docstore.DocStore, key: []const u8, control: admission.Port) !OwnedIntentRead {
    try control.checkpoint();
    const kind = try outbox.durableReplicationOutboxKindFromKey(key);
    const identity = key[key.len - outbox.replication_outbox_id_len ..][0..outbox.replication_outbox_id_len].*;
    var lookup = try store.beginOwnedInternalValueRead(allocator, key, control);
    errdefer lookup.deinit();
    try control.checkpoint();
    return .{ .lookup = lookup, .kind = kind, .identity = identity };
}

pub const IntentKey = [outbox.replication_outbox_v2_prefix.len + 1 + outbox.replication_outbox_id_len]u8;
pub const Selection = union(enum) { legacy, empty, key: IntentKey };

/// Select encoded metadata keys without requesting value materialization. The
/// snapshot/index/cursor block work remains synchronous; this is not a physical
/// byte bound. Callers hold apply and use the returned key only after capture.
pub fn selectIntentKey(store: *docstore.DocStore, pinned: ?[]const u8, after: ?[]const u8, control: admission.Port) !Selection {
    var txn = try store.beginReadTxnWithAdmission(.transient, control);
    defer txn.abort();
    var cursor = try txn.openPhysicalCursorWithAdmission(control);
    defer cursor.close();
    if (cursor.vtable.set_keys_only == null) return error.ExecutionWaitUnsupported;
    cursor.setKeysOnly();
    for ([_][]const u8{ outbox.replication_batch_outbox_key, outbox.replication_replay_outbox_key, outbox.replication_schema_outbox_key }) |legacy| {
        if (try cursor.seekAtOrAfterWithAdmission(legacy, control)) |entry| {
            if (std.mem.eql(u8, entry.key, legacy)) {
                try control.checkpoint();
                return .legacy;
            }
        }
    }
    if (pinned) |key| {
        if (try cursor.seekAtOrAfterWithAdmission(key, control)) |entry| {
            if (std.mem.eql(u8, entry.key, key)) {
                try control.checkpoint();
                return .{ .key = try intentKey(entry.key) };
            }
        }
    }
    var next = try cursor.seekAtOrAfterWithAdmission(after orelse outbox.replication_outbox_v2_prefix, control);
    if (after) |key| {
        if (next) |entry| if (std.mem.eql(u8, entry.key, key)) {
            next = try cursor.nextWithAdmission(control);
        };
        if (next == null or !std.mem.startsWith(u8, next.?.key, outbox.replication_outbox_v2_prefix))
            next = try cursor.seekAtOrAfterWithAdmission(outbox.replication_outbox_v2_prefix, control);
    }
    try control.checkpoint();
    const entry = next orelse return .empty;
    if (!std.mem.startsWith(u8, entry.key, outbox.replication_outbox_v2_prefix)) return .empty;
    return .{ .key = try intentKey(entry.key) };
}
fn intentKey(key: []const u8) !IntentKey {
    _ = try outbox.durableReplicationOutboxKindFromKey(key);
    return key[0..@sizeOf(IntentKey)].*;
}

const outbox_upper = "\x00\x00__metadata__:ha_outbox_v2;";

/// One snapshot for every legacy probe and v2 seek. A discovered key hands
/// that snapshot directly to value capture; validation emits scalar presence.
pub const OwnedSelection = struct {
    const Phase = enum { batch_setup, batch_scan, replay_setup, replay_scan, schema_setup, schema_scan, pinned_setup, pinned_scan, next_setup, next_scan, wrap_setup, wrap_scan, done };
    cursor: ?docstore.DocStore.OwnedInternalKeyCursor,
    pinned: ?IntentKey,
    after: ?IntentKey,
    validation: bool,
    revision: u64,
    query_revision: u64,
    result: ?Selection = null,
    phase: Phase = .batch_setup,

    pub fn init(allocator: Allocator, store: *docstore.DocStore, pinned: ?IntentKey, after: ?IntentKey, validation: bool, control: admission.Port) !@This() {
        var cursor = try store.beginOwnedInternalKeyCursor(allocator, control);
        errdefer cursor.deinit();
        try control.checkpoint();
        return .{ .cursor = cursor, .pinned = pinned, .after = after, .validation = validation, .revision = cursor.revision, .query_revision = cursor.query_revision };
    }
    fn exact(self: *@This(), comptime name: []const u8, control: admission.Port) !void {
        try self.cursor.?.seek(name, name ++ "\x00", false, control);
    }
    fn finish(self: *@This(), result: Selection) void {
        self.result = result;
        self.phase = .done;
        if (self.validation or std.meta.activeTag(result) != .key) self.deinitCursor();
    }
    fn legacy(self: *@This(), comptime name: []const u8, next: Phase, quantum: usize, control: admission.Port) !void {
        if (try self.cursor.?.step(quantum, control) != .ready) return;
        if (try self.cursor.?.key(control)) |found| {
            if (!std.mem.eql(u8, found, name)) return error.InvalidHAOutbox;
            self.finish(.legacy);
        } else self.phase = next;
    }
    pub fn step(self: *@This(), quantum: usize, control: admission.Port) !OwnedIntentRead.Status {
        if (quantum == 0 or quantum > 65536) return error.InvalidArgument;
        try control.checkpoint();
        switch (self.phase) {
            .batch_setup => {
                try self.exact(outbox.replication_batch_outbox_key, control);
                self.phase = .batch_scan;
            },
            .batch_scan => try self.legacy(outbox.replication_batch_outbox_key, .replay_setup, quantum, control),
            .replay_setup => {
                try self.exact(outbox.replication_replay_outbox_key, control);
                self.phase = .replay_scan;
            },
            .replay_scan => try self.legacy(outbox.replication_replay_outbox_key, .schema_setup, quantum, control),
            .schema_setup => {
                try self.exact(outbox.replication_schema_outbox_key, control);
                self.phase = .schema_scan;
            },
            .schema_scan => try self.legacy(outbox.replication_schema_outbox_key, .pinned_setup, quantum, control),
            .pinned_setup => {
                if (self.pinned) |*pinned| {
                    var upper: [@sizeOf(IntentKey) + 1]u8 = undefined;
                    @memcpy(upper[0..pinned.len], pinned);
                    upper[pinned.len] = 0;
                    try self.cursor.?.seek(pinned, &upper, false, control);
                    self.phase = .pinned_scan;
                } else self.phase = .next_setup;
            },
            .pinned_scan => {
                if (try self.cursor.?.step(quantum, control) == .ready) {
                    if (try self.cursor.?.key(control)) |found| {
                        if (!std.mem.eql(u8, found, &self.pinned.?)) return error.InvalidHAOutbox;
                        self.finish(.{ .key = try intentKey(found) });
                    } else if (self.validation) self.finish(.empty) else self.phase = .next_setup;
                }
            },
            .next_setup, .wrap_setup => {
                const wrap = self.phase == .wrap_setup;
                const lower: []const u8 = if (!wrap and self.after != null) &self.after.? else outbox.replication_outbox_v2_prefix;
                try self.cursor.?.seek(lower, outbox_upper, !wrap and self.after != null, control);
                self.phase = if (wrap) .wrap_scan else .next_scan;
            },
            .next_scan, .wrap_scan => {
                if (try self.cursor.?.step(quantum, control) == .ready) {
                    if (try self.cursor.?.key(control)) |found| self.finish(.{ .key = try intentKey(found) }) else if (self.phase == .next_scan and self.after != null) self.phase = .wrap_setup else self.finish(.empty);
                }
            },
            .done => {},
        }
        try control.checkpoint();
        return if (self.phase == .done) .ready else .pending;
    }
    pub fn intoIntentRead(self: *@This(), control: admission.Port) !OwnedIntentRead {
        if (self.phase != .done or self.validation or std.meta.activeTag(self.result.?) != .key) return error.InvalidArgument;
        const selected = self.result.?.key;
        const kind = try outbox.durableReplicationOutboxKindFromKey(&selected);
        const read = try self.cursor.?.intoValueRead(control);
        self.deinitCursor();
        return .{ .lookup = read, .kind = kind, .identity = selected[selected.len - 16 ..][0..16].* };
    }
    fn deinitCursor(self: *@This()) void {
        if (self.cursor) |*cursor| cursor.deinit();
        self.cursor = null;
    }
    pub fn deinit(self: *@This()) void {
        self.deinitCursor();
    }
};

/// Separate DB-owned capture state. A synchronous borrower holds mutex until
/// its publisher returns, then unlocks BEFORE clearing a key. Clear operations
/// retire state only after releasing apply. Teardown joins users before reset.
/// The fair resume key is scalar metadata and owns no snapshot or payload.
pub const IntentCapture = struct {
    const Namespace = @import("doc_identity_namespace.zig").Namespace;
    allocator: ?Allocator = null,
    mutex: std.atomic.Mutex = .unlocked,
    source: ?*docstore.DocStore = null,
    namespace: Namespace = .{},
    generation: u64 = 0,
    source_revision: u64 = 0,
    key: ?IntentKey = null,
    after: ?IntentKey = null,
    discovery: ?OwnedSelection = null,
    selection_result: ?Selection = null,
    // Monotonic observation only, never publication authority. The retry owner
    // distinguishes yielded runnable work from an acknowledgement/fence wait.
    progress_generation: std.atomic.Value(u64) = .init(0),
    selection_query_revision: u64 = 0,
    validation: ?OwnedSelection = null,
    validation_result: ?Selection = null,
    validation_query_revision: u64 = 0,
    validation_fresh: bool = false,
    read: ?OwnedIntentRead = null,
    value: ?OwnedIntent = null,

    pub fn init(allocator: Allocator) @This() {
        return .{ .allocator = allocator };
    }
    pub fn matches(self: *const @This(), source: *docstore.DocStore, namespace: Namespace, generation: u64) bool {
        const revision = source.portableImportRevision() catch return false;
        return self.source == source and self.namespace.eql(namespace) and self.generation == generation and self.source_revision == revision;
    }
    pub fn hasWork(self: *const @This()) bool {
        return self.discovery != null or self.validation != null or self.read != null;
    }
    /// Called under apply. The selector owns one snapshot across all probes;
    /// its eventual point reader receives that snapshot without reopening.
    pub fn beginDiscovery(self: *@This(), source: *docstore.DocStore, namespace: Namespace, generation: u64, control: admission.Port) !void {
        self.discard();
        const allocator = self.allocator orelse return error.OutOfMemory;
        const selection = try OwnedSelection.init(allocator, source, null, self.after, false, control);
        self.source = source;
        self.namespace = namespace;
        self.generation = generation;
        self.source_revision = selection.revision;
        self.discovery = selection;
        _ = self.progress_generation.fetchAdd(1, .release);
    }
    pub fn beginValidation(self: *@This(), control: admission.Port) !void {
        const allocator = self.allocator orelse return error.OutOfMemory;
        if (self.value == null or self.key == null) return error.InvalidArgument;
        self.validation = try OwnedSelection.init(allocator, self.source.?, self.key, null, true, control);
        self.validation_result = null;
        self.validation_fresh = false;
        _ = self.progress_generation.fetchAdd(1, .release);
    }
    /// Compatibility for explicit known-key users. Managed discovery instead
    /// transfers its snapshot through OwnedSelection.intoIntentRead.
    pub fn prepare(self: *@This(), source: *docstore.DocStore, namespace: Namespace, generation: u64, key: IntentKey, control: admission.Port) !void {
        try control.checkpoint();
        if (self.matches(source, namespace, generation)) {
            if (self.key) |current| if (std.mem.eql(u8, &current, &key)) return;
        } else self.after = null;
        self.discard();
        const allocator = self.allocator orelse return error.OutOfMemory;
        var read = try beginOwnedIntentRead(allocator, source, &key, control);
        errdefer read.deinit();
        const revision = try source.portableImportRevision();
        try control.checkpoint();
        self.source = source;
        self.namespace = namespace;
        self.generation = generation;
        self.source_revision = revision;
        self.key = key;
        self.read = read;
        _ = self.progress_generation.fetchAdd(1, .release);
    }
    pub fn step(self: *@This(), quantum: usize, control: admission.Port) !OwnedIntentRead.Status {
        if (quantum == 0 or quantum > 65536) return error.InvalidArgument;
        const status = self.advance(quantum, control) catch |err| {
            self.discard();
            return err;
        };
        _ = self.progress_generation.fetchAdd(1, .release);
        return status;
    }
    fn advance(self: *@This(), quantum: usize, control: admission.Port) !OwnedIntentRead.Status {
        try control.checkpoint();
        if (self.discovery) |*selection| {
            if (try selection.step(quantum, control) != .ready) return .pending;
            self.selection_query_revision = selection.query_revision;
            self.selection_result = selection.result;
            if (std.meta.activeTag(selection.result.?) == .key) {
                self.key = selection.result.?.key;
                self.read = try selection.intoIntentRead(control);
                self.selection_result = null;
            }
            selection.deinit();
            self.discovery = null;
            return if (self.read != null) .pending else .ready;
        }
        if (self.validation) |*selection| {
            if (try selection.step(quantum, control) != .ready) return .pending;
            self.validation_result = selection.result;
            self.validation_query_revision = selection.query_revision;
            self.validation_fresh = true;
            selection.deinit();
            self.validation = null;
            return .ready;
        }
        if (self.value != null or self.selection_result != null) return .ready;
        const read = if (self.read) |*pending| pending else return error.InvalidArgument;
        const status = try read.step(quantum, control);
        if (status == .ready) {
            self.value = try read.takeOutput();
            self.read = null;
        }
        return status;
    }
    /// Caller holds mutex, or has joined all users. No authority is needed.
    pub fn discard(self: *@This()) void {
        if (self.discovery) |*selection| selection.deinit();
        self.discovery = null;
        if (self.validation) |*selection| selection.deinit();
        self.validation = null;
        self.selection_result = null;
        self.validation_result = null;
        self.validation_fresh = false;
        if (self.read) |*read| read.deinit();
        self.read = null;
        if (self.value) |*value| value.deinit();
        self.value = null;
        self.key = null;
    }
    pub fn retireIdentity(self: *@This(), identity: outbox.RecoveryIdentity) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.discovery != null) {
            self.discard();
            return;
        }
        if (self.key) |key| if (std.mem.eql(u8, key[key.len - identity.len ..], &identity)) {
            self.after = key;
            self.discard();
        };
    }
    pub fn reset(self: *@This()) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.discard();
        self.source = null;
        self.after = null;
    }
};

pub const PendingPage = struct {
    legacy_batch: ?[]u8,
    legacy_replay: ?[]u8,
    legacy_schema: ?[]u8,
    entries: []docstore.OwnedKVPair,

    pub fn isEmpty(self: PendingPage) bool {
        return self.legacy_batch == null and self.legacy_replay == null and
            self.legacy_schema == null and self.entries.len == 0;
    }

    pub fn deinit(self: *PendingPage, alloc: Allocator) void {
        if (self.legacy_batch) |bytes| alloc.free(bytes);
        if (self.legacy_replay) |bytes| alloc.free(bytes);
        if (self.legacy_schema) |bytes| alloc.free(bytes);
        docstore.DocStore.freeResults(alloc, self.entries);
        self.* = undefined;
    }
};

/// Own one bounded page and all three rolling-upgrade singleton obligations.
/// Buffers survive releasing the apply fence so remote waits do not hold it.
pub fn readPending(alloc: Allocator, store: *docstore.DocStore) !PendingPage {
    const batch = try readOptional(alloc, store, outbox.replication_batch_outbox_key);
    errdefer if (batch) |bytes| alloc.free(bytes);
    const replay = try readOptional(alloc, store, outbox.replication_replay_outbox_key);
    errdefer if (replay) |bytes| alloc.free(bytes);
    const schema = try readOptional(alloc, store, outbox.replication_schema_outbox_key);
    errdefer if (schema) |bytes| alloc.free(bytes);
    return .{
        .legacy_batch = batch,
        .legacy_replay = replay,
        .legacy_schema = schema,
        .entries = try store.scanPrefixPage(alloc, outbox.replication_outbox_v2_prefix, null, outbox.replication_outbox_recovery_batch_size),
    };
}

/// The apply fence keeps the legacy singletons and mutation page stable while
/// one admitted snapshot reads both. The original port is a synchronous borrow;
/// no returned buffer, cursor or reader retains it. Rejection never writes.
pub fn readPendingWithAdmission(alloc: Allocator, store: *docstore.DocStore, control: admission.Port) !PendingPage {
    try control.checkpoint();
    var txn = try store.beginReadTxnWithAdmission(.transient, control);
    defer txn.abort();
    const keys = [_][]const u8{ outbox.replication_batch_outbox_key, outbox.replication_replay_outbox_key, outbox.replication_schema_outbox_key };
    var values: [keys.len]?[]const u8 = undefined;
    try txn.getManySortedWithAdmission(&keys, &values, control);
    const batch = if (values[0]) |value| try copyChecked(alloc, value, control) else null;
    errdefer if (batch) |bytes| alloc.free(bytes);
    const replay = if (values[1]) |value| try copyChecked(alloc, value, control) else null;
    errdefer if (replay) |bytes| alloc.free(bytes);
    const schema = if (values[2]) |value| try copyChecked(alloc, value, control) else null;
    errdefer if (schema) |bytes| alloc.free(bytes);
    var cursor = try txn.openCursorWithAdmission(control);
    defer cursor.close();
    var entries: std.ArrayListUnmanaged(docstore.OwnedKVPair) = .empty;
    errdefer {
        for (entries.items) |entry| {
            alloc.free(entry.key);
            alloc.free(entry.value);
        }
        entries.deinit(alloc);
    }
    var next = try cursor.seekAtOrAfterWithAdmission(outbox.replication_outbox_v2_prefix, control);
    while (next) |entry| {
        try control.checkpoint();
        if (!std.mem.startsWith(u8, entry.key, outbox.replication_outbox_v2_prefix)) break;
        const pair = try copyEntryChecked(alloc, entry.key, entry.value, control);
        entries.append(alloc, pair) catch |err| {
            alloc.free(pair.key);
            alloc.free(pair.value);
            return err;
        };
        if (entries.items.len == outbox.replication_outbox_recovery_batch_size) break;
        next = try cursor.nextWithAdmission(control);
    }
    const owned = try entries.toOwnedSlice(alloc);
    errdefer docstore.DocStore.freeResults(alloc, owned);
    try control.checkpoint();
    return .{ .legacy_batch = batch, .legacy_replay = replay, .legacy_schema = schema, .entries = owned };
}

/// Presence only: a pre-decision probe must not copy or decode an outbox
/// payload merely to reject admission. The caller holds flush and apply fences.
pub fn hasPendingWithAdmission(store: *docstore.DocStore, control: admission.Port) !bool {
    try control.checkpoint();
    var txn = try store.beginReadTxnWithAdmission(.transient, control);
    defer txn.abort();
    const keys = [_][]const u8{ outbox.replication_batch_outbox_key, outbox.replication_replay_outbox_key, outbox.replication_schema_outbox_key };
    var values: [keys.len]?[]const u8 = undefined;
    try txn.getManySortedWithAdmission(&keys, &values, control);
    for (values) |value| if (value != null) {
        try control.checkpoint();
        return true;
    };
    var cursor = try txn.openCursorWithAdmission(control);
    defer cursor.close();
    const first = try cursor.seekAtOrAfterWithAdmission(outbox.replication_outbox_v2_prefix, control);
    try control.checkpoint();
    return if (first) |entry| std.mem.startsWith(u8, entry.key, outbox.replication_outbox_v2_prefix) else false;
}

const copy_quantum_bytes = 64 * 1024;
fn copyChecked(alloc: Allocator, bytes: []const u8, control: admission.Port) ![]u8 {
    try control.checkpoint();
    const result = try alloc.alloc(u8, bytes.len);
    errdefer alloc.free(result);
    var offset: usize = 0;
    while (offset < bytes.len) {
        try control.checkpoint();
        const length = @min(bytes.len - offset, copy_quantum_bytes);
        @memcpy(result[offset..][0..length], bytes[offset..][0..length]);
        offset += length;
    }
    try control.checkpoint();
    return result;
}

fn copyEntryChecked(alloc: Allocator, key: []const u8, value: []const u8, control: admission.Port) !docstore.OwnedKVPair {
    const owned_key = try copyChecked(alloc, key, control);
    errdefer alloc.free(owned_key);
    return .{ .key = owned_key, .value = try copyChecked(alloc, value, control) };
}

/// Clear only the mutation which the replication adapter successfully
/// published. No prefix deletion or arbitrary internal key access is exposed.
pub fn clearPublished(store: *docstore.DocStore, key: []const u8) !void {
    if (!std.mem.eql(u8, key, outbox.replication_batch_outbox_key) and
        !std.mem.eql(u8, key, outbox.replication_replay_outbox_key) and
        !std.mem.eql(u8, key, outbox.replication_schema_outbox_key))
        _ = try outbox.durableReplicationOutboxKindFromKey(key);
    try store.putBatch(&.{}, &.{key});
}

fn readOptional(alloc: Allocator, store: *docstore.DocStore, key: []const u8) !?[]u8 {
    return store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

test "storage.hot_standby durable outbox storage pages legacy and mutation obligations without broad deletion" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    const value = try outbox.encodeDurableReplicationOutboxAlloc(alloc, 7, "pending");
    defer alloc.free(value);
    try store.putBatch(&.{
        .{ .key = outbox.replication_batch_outbox_key, .value = value },
        .{ .key = outbox.replication_replay_outbox_key, .value = value },
        .{ .key = outbox.replication_schema_outbox_key, .value = value },
        .{ .key = "unrelated", .value = "keep" },
    }, &.{});
    for (0..outbox.replication_outbox_recovery_batch_size + 3) |i| {
        const key = try outbox.durableReplicationOutboxKeyAlloc(alloc, .batch, @intCast(i + 1), 1, "pending");
        defer alloc.free(key);
        try store.putBatch(&.{.{ .key = key, .value = value }}, &.{});
    }
    {
        var page = try readPending(alloc, &store);
        defer page.deinit(alloc);
        try std.testing.expect(!page.isEmpty());
        try std.testing.expectEqual(outbox.replication_outbox_recovery_batch_size, page.entries.len);
        try std.testing.expectEqualSlices(u8, value, page.legacy_batch.?);
        try std.testing.expectEqualSlices(u8, value, page.legacy_replay.?);
        try std.testing.expectEqualSlices(u8, value, page.legacy_schema.?);
        try clearPublished(&store, page.entries[0].key);
        try std.testing.expectError(error.NotFound, store.get(alloc, page.entries[0].key));
        const surviving = try store.get(alloc, page.entries[1].key);
        defer alloc.free(surviving);
        try std.testing.expectEqualSlices(u8, value, surviving);
        try std.testing.expectError(error.InvalidHAOutbox, clearPublished(&store, "unrelated"));
        try clearPublished(&store, outbox.replication_batch_outbox_key);
        try clearPublished(&store, outbox.replication_replay_outbox_key);
        try clearPublished(&store, outbox.replication_schema_outbox_key);
    }
    while (true) {
        var page = try readPending(alloc, &store);
        defer page.deinit(alloc);
        if (page.isEmpty()) break;
        for (page.entries) |entry| try clearPublished(&store, entry.key);
    }
    const surviving = try store.get(alloc, "unrelated");
    defer alloc.free(surviving);
    try std.testing.expectEqualStrings("keep", surviving);
}

test "storage.hot_standby durable outbox storage releases partial pages on allocation failure" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    const key = try outbox.durableReplicationOutboxKeyAlloc(alloc, .schema, 9, 1, "pending");
    defer alloc.free(key);
    try store.putBatch(&.{
        .{ .key = outbox.replication_batch_outbox_key, .value = "legacy batch" },
        .{ .key = outbox.replication_replay_outbox_key, .value = "legacy replay" },
        .{ .key = outbox.replication_schema_outbox_key, .value = "legacy schema" },
        .{ .key = key, .value = "pending" },
    }, &.{});
    const Check = struct {
        pub fn read(allocator: Allocator, source: *docstore.DocStore) !void {
            var page = try readPending(allocator, source);
            defer page.deinit(allocator);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.read, .{&store});
}

test "durable outbox storage admitted page rejects every original checkpoint and preserves exact pending effects" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    const large = try alloc.alloc(u8, 2 * copy_quantum_bytes + 17);
    defer alloc.free(large);
    @memset(large, 0x5a);
    try store.putBatch(&.{
        .{ .key = outbox.replication_batch_outbox_key, .value = large },
        .{ .key = outbox.replication_replay_outbox_key, .value = "legacy replay" },
        .{ .key = outbox.replication_schema_outbox_key, .value = "legacy schema" },
        .{ .key = "unrelated", .value = "keep" },
    }, &.{});
    for (0..outbox.replication_outbox_recovery_batch_size + 3) |i| {
        const key = try outbox.durableReplicationOutboxKeyAlloc(alloc, .batch, @intCast(i + 1), 1, "pending");
        defer alloc.free(key);
        try store.putBatch(&.{.{ .key = key, .value = if (i == 0) large else "pending" }}, &.{});
    }
    const State = struct {
        checks: usize = 0,
        fail_at: ?usize = null,
        rejection: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.fail_at == self.checks) return self.rejection;
        }
        fn wait(_: *anyopaque) !void {
            return error.UnexpectedProgressWait;
        }
    };
    var state: State = .{};
    const payload: admission.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .wait_for_progress = State.wait } };
    const control = admission.Port.init(&payload);
    var expected = try readPending(alloc, &store);
    defer expected.deinit(alloc);
    {
        var page = try readPendingWithAdmission(alloc, &store, control);
        defer page.deinit(alloc);
        try std.testing.expectEqual(outbox.replication_outbox_recovery_batch_size, page.entries.len);
        try std.testing.expectEqualSlices(u8, expected.legacy_batch.?, page.legacy_batch.?);
        try std.testing.expectEqualSlices(u8, expected.legacy_replay.?, page.legacy_replay.?);
        try std.testing.expectEqualSlices(u8, expected.legacy_schema.?, page.legacy_schema.?);
        for (expected.entries, page.entries) |wanted, actual| {
            try std.testing.expectEqualSlices(u8, wanted.key, actual.key);
            try std.testing.expectEqualSlices(u8, wanted.value, actual.value);
        }
    }
    const successful_checks = state.checks;
    const snapshot_refs = backend.state.?.refs;
    try std.testing.expect(successful_checks > 2 * outbox.replication_outbox_recovery_batch_size);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |rejection| {
        for (1..successful_checks + 1) |fail_at| {
            state = .{ .fail_at = fail_at, .rejection = rejection };
            try std.testing.expectError(rejection, readPendingWithAdmission(alloc, &store, control));
            try std.testing.expectEqual(fail_at, state.checks);
            try std.testing.expectEqual(snapshot_refs, backend.state.?.refs);
        }
    }
    // Every failed read must release its physical reader/cursor and temporary
    // copies. The same store remains reusable, with no deletion or replay.
    state = .{};
    var retry = try readPendingWithAdmission(alloc, &store, control);
    defer retry.deinit(alloc);
    try std.testing.expectEqual(expected.entries.len, retry.entries.len);
    for (expected.entries, retry.entries) |wanted, actual| {
        try std.testing.expectEqualSlices(u8, wanted.key, actual.key);
        try std.testing.expectEqualSlices(u8, wanted.value, actual.value);
    }
    try std.testing.expectEqualSlices(u8, expected.legacy_batch.?, retry.legacy_batch.?);
    const unrelated = try store.get(alloc, "unrelated");
    defer alloc.free(unrelated);
    try std.testing.expectEqualStrings("keep", unrelated);
}

test "durable outbox storage admitted copies use finite byte quanta and release partial allocations" {
    const alloc = std.testing.allocator;
    const bytes = try alloc.alloc(u8, 2 * copy_quantum_bytes + 1);
    defer alloc.free(bytes);
    @memset(bytes, 0x6b);
    const State = struct {
        checks: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
        }
    };
    var state: State = .{};
    const payload: admission.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check } };
    const control = admission.Port.init(&payload);
    const result = try copyChecked(alloc, bytes, control);
    defer alloc.free(result);
    try std.testing.expectEqualSlices(u8, bytes, result);
    try std.testing.expectEqual(@as(usize, 5), state.checks);
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    const key = try outbox.durableReplicationOutboxKeyAlloc(alloc, .row_policy, 11, 1, "pending");
    defer alloc.free(key);
    try store.putBatch(&.{
        .{ .key = outbox.replication_batch_outbox_key, .value = bytes },
        .{ .key = outbox.replication_replay_outbox_key, .value = "legacy replay" },
        .{ .key = outbox.replication_schema_outbox_key, .value = "legacy schema" },
        .{ .key = key, .value = bytes },
    }, &.{});
    const Check = struct {
        fn read(allocator: Allocator, source: *docstore.DocStore, port: admission.Port) !void {
            var page = try readPendingWithAdmission(allocator, source, port);
            defer page.deinit(allocator);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.read, .{ &store, control });
}

test "durable outbox storage admitted read preserves original rejection during held physical reader gates" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    try store.putBatch(&.{.{ .key = outbox.replication_batch_outbox_key, .value = "pending" }}, &.{});
    const State = struct {
        checks: usize = 0,
        waits: usize = 0,
        reject: anyerror,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks >= 32) return self.reject;
        }
        fn wait(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.waits += 1;
        }
    };
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |rejection| {
        for (0..2) |gate| {
            if (gate == 0) store.payload_policy_mutex.lockUncancelable(std.testing.io) else try std.testing.expect(backend.mutex.tryLock());
            var held = true;
            defer if (held) {
                if (gate == 0) store.payload_policy_mutex.unlock(std.testing.io) else backend.mutex.unlock();
            };
            const refs = backend.state.?.refs;
            var state: State = .{ .reject = rejection };
            const payload: admission.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .wait_for_progress = State.wait } };
            try std.testing.expectError(rejection, readPendingWithAdmission(alloc, &store, admission.Port.init(&payload)));
            try std.testing.expectEqual(@as(usize, 32), state.checks);
            try std.testing.expect(state.waits > 0);
            try std.testing.expectEqual(refs, backend.state.?.refs);
            if (gate == 0) {
                try std.testing.expect(!store.payload_policy_mutex.tryLock());
                store.payload_policy_mutex.unlock(std.testing.io);
            } else {
                try std.testing.expect(!backend.mutex.tryLock());
                backend.mutex.unlock();
            }
            held = false;
            // The caller's existing gate was neither unlocked nor stolen.
            // Healthy legacy recovery still observes the same obligation.
            var retry = try readPending(alloc, &store);
            defer retry.deinit(alloc);
            try std.testing.expectEqualStrings("pending", retry.legacy_batch.?);
            try std.testing.expectEqual(@as(usize, 0), retry.entries.len);
        }
    }
}

const OwnedIntentTestAuthority = struct {
    rejected: bool = false,
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.rejected) return error.TestIntentOriginalRejection;
    }
    fn payload(self: *@This()) admission.Payload {
        return .{ .ptr = self, .vtable = &.{ .checkpoint = check } };
    }
};

test "durable outbox owned capture releases import reader and verifies after source close" {
    const a = std.testing.allocator;
    var backend = @import("../lsm_backend.zig").Backend.init(a, .{ .wal_enabled = false });
    var backend_open = true;
    defer if (backend_open) backend.close();
    var store = try docstore.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    var store_open = true;
    defer if (store_open) store.close();
    const payload = try a.alloc(u8, 128 * 1024 + 17);
    defer a.free(payload);
    for (payload, 0..) |*byte, i| byte.* = @truncate(i *% 7);
    const key = try outbox.durableReplicationOutboxKeyAlloc(a, .primary_effect, 73, 5, payload);
    defer a.free(key);
    const identity = key[key.len - 16 ..][0..16].*;
    const raw = try outbox.encodeDurableReplicationOutboxAlloc(a, 73, payload);
    defer a.free(raw);
    try store.put(key, raw);
    var authority: OwnedIntentTestAuthority = .{};
    const descriptor = authority.payload();
    var read = try beginOwnedIntentRead(a, &store, key, admission.Port.init(&descriptor));
    defer read.deinit();
    @memset(key, 0xdd); // Capture must own the caller's key.
    try std.testing.expect(store.portableImportHasReaders());
    try store.claimPortableImportPublicationWithAdmission(null);
    try std.testing.expectError(error.PortableImportPublicationInProgress, store.beginOwnedInternalValueRead(a, outbox.replication_batch_outbox_key, admission.Port.init(&descriptor)));
    store.cancelPortableImportPublication();
    try std.testing.expectError(error.InvalidArgument, read.step(0, admission.Port.init(&descriptor)));
    var units: usize = 0;
    while (read.lookup != null) : (units += 1) {
        // Each step borrows a fresh descriptor, which expires immediately.
        const fresh = authority.payload();
        try std.testing.expectEqual(OwnedIntentRead.Status.pending, try read.step(4093, admission.Port.init(&fresh)));
        try std.testing.expect(units < 1000);
    }
    try std.testing.expect(units > 1);
    try std.testing.expect(!store.portableImportHasReaders());
    try std.testing.expectError(error.InvalidArgument, read.takeOutput());
    store.close();
    store_open = false;
    backend.close();
    backend_open = false;
    @memset(std.mem.asBytes(&store), 0xa5);
    @memset(std.mem.asBytes(&backend), 0xa5);
    while (true) {
        const fresh = authority.payload();
        const before = read.decoder.checksum_offset;
        const status = try read.step(4093, admission.Port.init(&fresh));
        try std.testing.expect(read.decoder.checksum_offset - before <= 4093);
        if (status == .ready) break;
    }
    var owned = try read.takeOutput();
    defer owned.deinit();
    try std.testing.expectError(error.InvalidArgument, read.takeOutput());
    try std.testing.expectEqual(outbox.Kind.primary_effect, owned.kind);
    try std.testing.expectEqualSlices(u8, &identity, &owned.identity);
    const borrowed = owned.borrow();
    try std.testing.expectEqual(@as(u64, 73), borrowed.from_lsn);
    try std.testing.expectEqualSlices(u8, payload, borrowed.payload);
    read.deinit(); // Repeated cleanup cannot reclaim transferred output.
}

test "durable outbox owned capture cancellation corruption and unsupported lookup release every lease" {
    const a = std.testing.allocator;
    var backend = @import("../lsm_backend.zig").Backend.init(a, .{ .wal_enabled = false });
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const key = try outbox.durableReplicationOutboxKeyAlloc(a, .batch, 9, 1, "payload");
    defer a.free(key);
    const raw = try outbox.encodeDurableReplicationOutboxAlloc(a, 9, "payload");
    defer a.free(raw);
    try store.put(key, raw);
    var authority: OwnedIntentTestAuthority = .{};
    const descriptor = authority.payload();
    const control = admission.Port.init(&descriptor);
    try std.testing.expectError(error.InvalidHAOutbox, beginOwnedIntentRead(a, &store, outbox.replication_batch_outbox_key, control));
    try std.testing.expectError(error.InvalidArgument, store.beginOwnedInternalValueRead(a, "user-key", control));
    var legacy = store.runtime_store.vtable.*;
    legacy.begin_owned_value_read = null;
    const original = store.runtime_store.vtable;
    store.runtime_store.vtable = &legacy;
    try std.testing.expectError(error.ExecutionWaitUnsupported, beginOwnedIntentRead(a, &store, key, control));
    try std.testing.expect(!store.portableImportHasReaders());
    store.runtime_store.vtable = original;
    for ([_]bool{ false, true }) |after_capture| {
        var read = try beginOwnedIntentRead(a, &store, key, control);
        defer read.deinit();
        if (after_capture) {
            while (read.lookup != null) _ = try read.step(1, control);
            while (read.decoder.checksum_offset == 0) _ = try read.step(1, control);
        } else _ = try read.step(1, control);
        authority.rejected = true;
        try std.testing.expectError(error.TestIntentOriginalRejection, read.step(1, control));
        try std.testing.expect(!store.portableImportHasReaders());
        authority.rejected = false;
        try std.testing.expectError(error.TestIntentOriginalRejection, read.step(1, control));
        try std.testing.expectError(error.TestIntentOriginalRejection, read.takeOutput());
    }
    raw[raw.len - 1] ^= 1;
    try store.put(key, raw);
    var corrupt = try beginOwnedIntentRead(a, &store, key, control);
    defer corrupt.deinit();
    const result = blk: {
        while (true) _ = corrupt.step(1, control) catch |err| break :blk err;
    };
    try std.testing.expectEqual(error.InvalidHAOutbox, result);
    try std.testing.expect(!store.portableImportHasReaders());
    try clearPublished(&store, key);
    var missing = beginOwnedIntentRead(a, &store, key, control) catch |err| {
        try std.testing.expectEqual(error.NotFound, err);
        try std.testing.expect(!store.portableImportHasReaders());
        return;
    };
    defer missing.deinit();
    const missing_result = blk: {
        while (true) _ = missing.step(1, control) catch |err| break :blk err;
    };
    try std.testing.expectEqual(error.NotFound, missing_result);
    try std.testing.expect(!store.portableImportHasReaders());
}

test "durable outbox owned capture unwinds caller allocation refusal" {
    const a = std.testing.allocator;
    var backend = @import("../lsm_backend.zig").Backend.init(a, .{ .wal_enabled = false });
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const key = try outbox.durableReplicationOutboxKeyAlloc(a, .schema, 3, 2, "schema");
    defer a.free(key);
    const raw = try outbox.encodeDurableReplicationOutboxAlloc(a, 3, "schema");
    defer a.free(raw);
    try store.put(key, raw);
    const Probe = struct {
        fn run(root: Allocator, source: *docstore.DocStore, name: []const u8) !void {
            defer std.debug.assert(!source.portableImportHasReaders());
            var authority: OwnedIntentTestAuthority = .{};
            const descriptor = authority.payload();
            const control = admission.Port.init(&descriptor);
            var read = try beginOwnedIntentRead(root, source, name, control);
            defer read.deinit();
            while (try read.step(1, control) != .ready) {}
            var owned = try read.takeOutput();
            defer owned.deinit();
            try std.testing.expectEqualStrings("schema", owned.borrow().payload);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Probe.run, .{ &store, key });
}

test "durable outbox owned selection resumes after completion pins in-progress keys and preserves legacy debt" {
    const a = std.testing.allocator;
    var backend = @import("../lsm_backend.zig").Backend.init(a, .{ .wal_enabled = false });
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    var authority: OwnedIntentTestAuthority = .{};
    const descriptor = authority.payload();
    const control = admission.Port.init(&descriptor);
    try std.testing.expect(std.meta.activeTag(try selectIntentKey(&store, null, null, control)) == .empty);
    var keys: [3]IntentKey = undefined;
    for (0..3) |i| {
        const owned = try outbox.durableReplicationOutboxKeyAlloc(a, .replay, i + 1, 7, "selection");
        defer a.free(owned);
        keys[i] = try intentKey(owned);
        // Key selection must neither decode AHO1 nor resolve payload bytes.
        try store.put(owned, "not a valid AHO1 value");
    }
    const Sort = struct {
        fn less(_: void, left: IntentKey, right: IntentKey) bool {
            return std.mem.order(u8, &left, &right) == .lt;
        }
    };
    std.mem.sort(IntentKey, &keys, {}, Sort.less);
    try std.testing.expectEqualSlices(u8, &keys[0], &(try selectIntentKey(&store, null, null, control)).key);
    try std.testing.expectEqualSlices(u8, &keys[2], &(try selectIntentKey(&store, &keys[2], null, control)).key);
    try std.testing.expectEqualSlices(u8, &keys[1], &(try selectIntentKey(&store, null, &keys[0], control)).key);
    try std.testing.expectEqualSlices(u8, &keys[0], &(try selectIntentKey(&store, null, &keys[2], control)).key);
    try clearPublished(&store, &keys[1]);
    try std.testing.expectEqualSlices(u8, &keys[2], &(try selectIntentKey(&store, &keys[1], &keys[0], control)).key);
    try store.put(outbox.replication_batch_outbox_key, "legacy singleton");
    try std.testing.expect(std.meta.activeTag(try selectIntentKey(&store, &keys[2], &keys[0], control)) == .legacy);
    authority.rejected = true;
    try std.testing.expectError(error.TestIntentOriginalRejection, selectIntentKey(&store, null, null, control));
    try std.testing.expect(!store.portableImportHasReaders());
}

test "durable outbox owned DB capture guards namespace generation and retires matching identity without new authority" {
    const a = std.testing.allocator;
    var backend = @import("../lsm_backend.zig").Backend.init(a, .{ .wal_enabled = false });
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const key = try outbox.durableReplicationOutboxKeyAlloc(a, .replay, 7, 4, "value");
    defer a.free(key);
    const name = try intentKey(key);
    const encoded = try outbox.encodeDurableReplicationOutboxAlloc(a, 7, "value");
    defer a.free(encoded);
    try store.put(key, encoded);
    var authority: OwnedIntentTestAuthority = .{};
    const descriptor = authority.payload();
    const control = admission.Port.init(&descriptor);
    var capture: IntentCapture = .init(a);
    defer capture.reset();
    try capture.prepare(&store, .{ .table_id = 1 }, 3, name, control);
    try std.testing.expect(store.portableImportHasReaders());
    try std.testing.expect(capture.matches(&store, .{ .table_id = 1 }, 3));
    try std.testing.expect(!capture.matches(&store, .{ .table_id = 2 }, 3));
    try std.testing.expect(!capture.matches(&store, .{ .table_id = 1 }, 4));
    _ = try capture.step(1, control);
    const pending = capture.read.?.lookup.?.lookup.?.ptr;
    try capture.prepare(&store, .{ .table_id = 1 }, 3, name, control);
    try std.testing.expectEqual(pending, capture.read.?.lookup.?.lookup.?.ptr);
    try capture.prepare(&store, .{ .table_id = 2 }, 4, name, control);
    try std.testing.expect(capture.matches(&store, .{ .table_id = 2 }, 4));
    while (try capture.step(1, control) != .ready) {}
    try std.testing.expect(!store.portableImportHasReaders());
    try std.testing.expectEqualStrings("value", capture.value.?.borrow().payload);
    const result_ptr = capture.value.?.raw.bytes().ptr;
    try std.testing.expectEqual(OwnedIntentRead.Status.ready, try capture.step(1, control));
    try std.testing.expectEqual(result_ptr, capture.value.?.raw.bytes().ptr);
    authority.rejected = true;
    var unrelated: outbox.RecoveryIdentity = @splat(0);
    const actual = name[name.len - 16 ..][0..16].*;
    if (std.mem.eql(u8, &unrelated, &actual)) unrelated[0] = 1;
    capture.retireIdentity(unrelated);
    try std.testing.expect(capture.value != null);
    capture.retireIdentity(actual);
    try std.testing.expect(capture.key == null and capture.value == null and capture.read == null);
    try std.testing.expectEqualSlices(u8, &name, &capture.after.?);
    capture.reset();
    try std.testing.expect(capture.after == null and capture.source == null);
}

test "durable outbox owned selection shares snapshot across probes and value handoff and releases one import lease" {
    const a = std.testing.allocator;
    var backend = @import("../lsm_backend.zig").Backend.init(a, .{ .wal_enabled = false });
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const key = try outbox.durableReplicationOutboxKeyAlloc(a, .primary_effect, 7, 1, "original");
    defer a.free(key);
    const raw = try outbox.encodeDurableReplicationOutboxAlloc(a, 7, "original");
    defer a.free(raw);
    try store.put(key, raw);
    var authority: OwnedIntentTestAuthority = .{};
    const descriptor = authority.payload();
    const control = admission.Port.init(&descriptor);
    var selection = try OwnedSelection.init(a, &store, null, null, false, control);
    defer selection.deinit();
    const revision = selection.query_revision;
    try store.put(outbox.replication_batch_outbox_key, "later legacy obligation");
    try store.putBatch(&.{}, &.{key});
    try std.testing.expect(store.query_generation_revision.load(.acquire) != revision);
    var steps: usize = 0;
    while (try selection.step(1, control) != .ready) : (steps += 1) try std.testing.expect(steps < 1000);
    try std.testing.expectEqualSlices(u8, key, &selection.result.?.key);
    try std.testing.expectEqual(@as(usize, 1), store.portable_import_reader_state.load(.acquire));
    var read = try selection.intoIntentRead(control);
    defer read.deinit();
    selection.deinit();
    try std.testing.expectEqual(@as(usize, 1), store.portable_import_reader_state.load(.acquire));
    while (try read.step(1, control) != .ready) {}
    var value = try read.takeOutput();
    defer value.deinit();
    try std.testing.expectEqualStrings("original", value.borrow().payload);
    try std.testing.expect(!store.portableImportHasReaders());
    try std.testing.expectError(error.NotFound, store.get(a, key));
}

test "durable outbox owned selection cancellation and caller allocation failures retire cursor snapshot and transferred read" {
    const a = std.testing.allocator;
    var backend = @import("../lsm_backend.zig").Backend.init(a, .{ .wal_enabled = false });
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const key = try outbox.durableReplicationOutboxKeyAlloc(a, .batch, 7, 1, "payload");
    defer a.free(key);
    const raw = try outbox.encodeDurableReplicationOutboxAlloc(a, 7, "payload");
    defer a.free(raw);
    try store.put(key, raw);
    const Probe = struct {
        fn run(root: Allocator, source: *docstore.DocStore) !void {
            defer std.debug.assert(!source.portableImportHasReaders());
            var authority: OwnedIntentTestAuthority = .{};
            const descriptor = authority.payload();
            const control = admission.Port.init(&descriptor);
            var selection = try OwnedSelection.init(root, source, null, null, false, control);
            defer selection.deinit();
            while (try selection.step(1, control) != .ready) {}
            var read = try selection.intoIntentRead(control);
            defer read.deinit();
            while (try read.step(1, control) != .ready) {}
            var value = try read.takeOutput();
            defer value.deinit();
            try std.testing.expectEqualStrings("payload", value.borrow().payload);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Probe.run, .{&store});
    var authority: OwnedIntentTestAuthority = .{};
    const descriptor = authority.payload();
    const control = admission.Port.init(&descriptor);
    var capture: IntentCapture = .init(a);
    defer capture.reset();
    try capture.beginDiscovery(&store, .{}, 1, control);
    _ = try capture.step(1, control);
    authority.rejected = true;
    try std.testing.expectError(error.TestIntentOriginalRejection, capture.step(1, control));
    try std.testing.expect(!store.portableImportHasReaders());
    try std.testing.expect(!capture.hasWork());
}

test "durable outbox owned import revision changes only on publication and refuses wrap before admission" {
    const a = std.testing.allocator;
    var backend = @import("../lsm_backend.zig").Backend.init(a, .{ .wal_enabled = false });
    defer backend.close();
    var store = try docstore.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const initial = try store.portableImportRevision();
    try store.claimPortableImportPublicationWithAdmission(null);
    store.cancelPortableImportPublication();
    try std.testing.expectEqual(initial, try store.portableImportRevision());
    try store.claimPortableImportPublicationWithAdmission(null);
    store.finishPortableImportPublication();
    try std.testing.expectEqual(initial + 1, try store.portableImportRevision());
    store.portable_import_revision.store(std.math.maxInt(u64), .release);
    try std.testing.expectError(error.PortableImportRecoveryRequired, store.claimPortableImportPublicationWithAdmission(null));
    try std.testing.expect(!store.portableImportPublicationInProgress());
}
