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

//! Differential oracle: Lite's logical redo log (docs/design/lite-performance/
//! lite-single-commit-write-path.md §9, §9.7c, §9.7d) versus the legacy
//! copy-on-write commit path. Drives the same seeded, deterministic sequence
//! of random operations against two stores opened on the same primary
//! keyspace -- one with `use_redo_log = true` (the default), one with
//! `false` (the reference/legacy behavior) -- and after every step compares
//! every read API both expose. Any difference is the bug class this file
//! exists to catch systematically: a read path that forgot to consult
//! `SharedRedoOverlay`/`MergedCursor` (found and fixed four separate times
//! so far -- `docstore.Cursor`, `getDocumentsAtCheckpointWithState`,
//! `readBackingPages`'s read-ahead window, `transactionCatalogEntry`; see
//! §9.7c's own "found three times" writeup plus the fourth, catalog-append
//! fix on this branch).
//!
//! Run (Debug only; filtered -- see this package's AGENTS.md for the
//! two-step build-then-run split required for `lite-storage-test`):
//!   zig build lite-storage-test -- --test-filter "redo differential oracle"
//!
//! Default config: `default_seed_count` seeds x `default_step_count` steps,
//! `reclamation.page_reuse = false` (deterministic, avoids the allocator-
//! under-redo/indexed-reclamation interaction still being worked on
//! separately on `perf/lwa-redo-alloc`). Set
//! `ANTFLY_LITE_REDO_ORACLE_SEEDS`/`ANTFLY_LITE_REDO_ORACLE_STEPS` to widen
//! the sweep; set `ANTFLY_LITE_REDO_ORACLE_RECLAMATION=1` to additionally run
//! the wide sweep with `reclamation.page_reuse = true`; set
//! `ANTFLY_LITE_REDO_ORACLE_CRASH=1` to additionally run each seed's
//! operation stream through the existing `crash_consistency_test.zig`
//! sweep engine (`runCrashSweep`) under both settings, which validates each
//! mode independently against its own "acknowledged implies durable"
//! contract -- the two modes may legitimately differ in which
//! un-acknowledged writes survive a crash, so this is a same-mode-contract
//! check, not a cross-mode diff, by design (point 3 of the task this file
//! was built for).

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const docstore = @import("docstore.zig");
const native = @import("native.zig");
const native_catalog_backend = @import("../native_catalog_backend.zig");
const native_catalog_sink = @import("native_catalog_sink.zig");
const sim_io = @import("crash_sim_io.zig");
const crash_consistency = @import("crash_consistency_test.zig");

// ---------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------

const default_seed_count: u64 = 4;
const default_step_count: usize = 200;

fn getenvRaw(name: [*:0]const u8) ?[:0]const u8 {
    if (!builtin.link_libc) return null;
    const raw = std.c.getenv(name) orelse return null;
    return std.mem.span(raw);
}

fn envUsize(comptime name: [*:0]const u8, default: usize) usize {
    const raw = getenvRaw(name) orelse return default;
    return std.fmt.parseInt(usize, std.mem.trim(u8, raw, " \t\r\n"), 10) catch default;
}

fn envFlag(comptime name: [*:0]const u8) bool {
    const raw = getenvRaw(name) orelse return false;
    const trimmed = std.mem.trim(u8, raw, " \t\r\n");
    return trimmed.len != 0 and !std.mem.eql(u8, trimmed, "0");
}

// ---------------------------------------------------------------------
// Op generation
// ---------------------------------------------------------------------

const large_value_threshold: usize = 16 * 1024 + 1;

const BatchItem = struct { key: []const u8, value: ?[]const u8 };

const CatalogRef = struct { scope: []const u8, namespace: ?[]const u8, key: []const u8, value: []const u8 };

const OpTag = enum { put, put_large, delete, batch, catalog_put, catalog_delete, checkpoint, vacuum, reopen };

const PutRef = struct { key: []const u8, value: []const u8 };

const Op = union(OpTag) {
    put: PutRef,
    put_large: PutRef,
    delete: struct { key: []const u8 },
    batch: []const BatchItem,
    catalog_put: CatalogRef,
    catalog_delete: struct { scope: []const u8, namespace: ?[]const u8, key: []const u8 },
    checkpoint: void,
    vacuum: void,
    reopen: void,

    fn describe(self: Op, buf: []u8) []const u8 {
        return switch (self) {
            .put => |p| std.fmt.bufPrint(buf, "put({s}, {d} bytes)", .{ p.key, p.value.len }) catch "put(...)",
            .put_large => |p| std.fmt.bufPrint(buf, "put_large({s}, {d} bytes)", .{ p.key, p.value.len }) catch "put_large(...)",
            .delete => |d| std.fmt.bufPrint(buf, "delete({s})", .{d.key}) catch "delete(...)",
            .batch => |items| std.fmt.bufPrint(buf, "batch({d} items)", .{items.len}) catch "batch(...)",
            .catalog_put => |c| std.fmt.bufPrint(buf, "catalog_put(scope={s} ns={?s} key={s})", .{ c.scope, c.namespace, c.key }) catch "catalog_put(...)",
            .catalog_delete => |c| std.fmt.bufPrint(buf, "catalog_delete(scope={s} ns={?s} key={s})", .{ c.scope, c.namespace, c.key }) catch "catalog_delete(...)",
            .checkpoint => "checkpoint",
            .vacuum => "vacuum",
            .reopen => "reopen",
        };
    }
};

/// Tracks keys/catalog entries this run has created, so the generator can
/// pick an existing key to overwrite/delete instead of only ever growing
/// the keyspace. Backed by an arena: freed once, at the end of the whole
/// seeded run, never per-op.
const CatalogEntryRef = struct { scope: []const u8, namespace: ?[]const u8, key: []const u8 };

const Keyspace = struct {
    arena: Allocator,
    doc_keys: std.ArrayListUnmanaged([]const u8) = .empty,
    deleted_doc_keys: std.ArrayListUnmanaged([]const u8) = .empty,
    catalog_entries: std.ArrayListUnmanaged(CatalogEntryRef) = .empty,
    doc_counter: u64 = 0,
    catalog_counter: u64 = 0,

    fn newDocKey(self: *Keyspace, random: std.Random) ![]const u8 {
        const group = random.intRangeLessThan(u8, 0, 3);
        const n = self.doc_counter;
        self.doc_counter += 1;
        const key = try std.fmt.allocPrint(self.arena, "grp{d}/doc-{d:0>6}", .{ group, n });
        try self.doc_keys.append(self.arena, key);
        return key;
    }

    fn pickDocKey(self: *Keyspace, random: std.Random) ?[]const u8 {
        if (self.doc_keys.items.len == 0) return null;
        return self.doc_keys.items[random.intRangeLessThan(usize, 0, self.doc_keys.items.len)];
    }

    fn removeDocKey(self: *Keyspace, key: []const u8) void {
        for (self.doc_keys.items, 0..) |k, i| {
            if (std.mem.eql(u8, k, key)) {
                _ = self.doc_keys.swapRemove(i);
                self.deleted_doc_keys.append(self.arena, key) catch {};
                return;
            }
        }
    }

    fn newCatalogEntry(self: *Keyspace, random: std.Random) !CatalogEntryRef {
        const dir = random.intRangeLessThan(u8, 0, 3);
        const has_ns = random.boolean();
        const scope = try std.fmt.allocPrint(self.arena, "dir{d}/scope", .{dir});
        const namespace: ?[]const u8 = if (has_ns) try std.fmt.allocPrint(self.arena, "ns{d}", .{random.intRangeLessThan(u8, 0, 2)}) else null;
        const n = self.catalog_counter;
        self.catalog_counter += 1;
        const key = try std.fmt.allocPrint(self.arena, "cat-{d:0>6}", .{n});
        const entry: CatalogEntryRef = .{ .scope = scope, .namespace = namespace, .key = key };
        try self.catalog_entries.append(self.arena, entry);
        return entry;
    }

    fn pickCatalogEntry(self: *Keyspace, random: std.Random) ?CatalogEntryRef {
        if (self.catalog_entries.items.len == 0) return null;
        return self.catalog_entries.items[random.intRangeLessThan(usize, 0, self.catalog_entries.items.len)];
    }

    fn removeCatalogEntry(self: *Keyspace, key: []const u8) void {
        for (self.catalog_entries.items, 0..) |e, i| {
            if (std.mem.eql(u8, e.key, key)) {
                _ = self.catalog_entries.swapRemove(i);
                return;
            }
        }
    }
};

fn randomValue(arena: Allocator, random: std.Random, len: usize) ![]const u8 {
    const buf = try arena.alloc(u8, len);
    for (buf) |*b| b.* = 'a' + @as(u8, @intCast(random.intRangeLessThan(u8, 0, 26)));
    return buf;
}

fn generateOp(arena: Allocator, random: std.Random, keyspace: *Keyspace) !Op {
    const roll = random.intRangeLessThan(u8, 0, 100);
    if (roll < 30) {
        // put: 40% of the time overwrite an existing key when one exists.
        const key = if (keyspace.doc_keys.items.len != 0 and random.intRangeLessThan(u8, 0, 100) < 40)
            keyspace.pickDocKey(random).?
        else
            try keyspace.newDocKey(random);
        const value = try randomValue(arena, random, 1 + random.intRangeLessThan(usize, 0, 200));
        return .{ .put = .{ .key = key, .value = value } };
    } else if (roll < 35) {
        const key = if (keyspace.doc_keys.items.len != 0 and random.boolean())
            keyspace.pickDocKey(random).?
        else
            try keyspace.newDocKey(random);
        const value = try randomValue(arena, random, large_value_threshold + random.intRangeLessThan(usize, 0, 24 * 1024));
        return .{ .put_large = .{ .key = key, .value = value } };
    } else if (roll < 45) {
        if (keyspace.pickDocKey(random)) |key| {
            keyspace.removeDocKey(key);
            return .{ .delete = .{ .key = key } };
        }
        const key = try keyspace.newDocKey(random);
        const value = try randomValue(arena, random, 16);
        return .{ .put = .{ .key = key, .value = value } };
    } else if (roll < 55) {
        const count = 2 + random.intRangeLessThan(usize, 0, 4);
        const items = try arena.alloc(BatchItem, count);
        for (items) |*item| {
            const is_delete = keyspace.doc_keys.items.len != 0 and random.intRangeLessThan(u8, 0, 100) < 30;
            if (is_delete) {
                const key = keyspace.pickDocKey(random).?;
                keyspace.removeDocKey(key);
                item.* = .{ .key = key, .value = null };
            } else {
                const key = if (keyspace.doc_keys.items.len != 0 and random.boolean())
                    keyspace.pickDocKey(random).?
                else
                    try keyspace.newDocKey(random);
                item.* = .{ .key = key, .value = try randomValue(arena, random, 1 + random.intRangeLessThan(usize, 0, 200)) };
            }
        }
        return .{ .batch = items };
    } else if (roll < 70) {
        const entry = if (keyspace.catalog_entries.items.len != 0 and random.intRangeLessThan(u8, 0, 100) < 40)
            keyspace.pickCatalogEntry(random).?
        else
            try keyspace.newCatalogEntry(random);
        const value = try randomValue(arena, random, 1 + random.intRangeLessThan(usize, 0, 128));
        return .{ .catalog_put = .{ .scope = entry.scope, .namespace = entry.namespace, .key = entry.key, .value = value } };
    } else if (roll < 75) {
        if (keyspace.pickCatalogEntry(random)) |entry| {
            keyspace.removeCatalogEntry(entry.key);
            return .{ .catalog_delete = .{ .scope = entry.scope, .namespace = entry.namespace, .key = entry.key } };
        }
        const entry = try keyspace.newCatalogEntry(random);
        return .{ .catalog_put = .{ .scope = entry.scope, .namespace = entry.namespace, .key = entry.key, .value = "seed" } };
    } else if (roll < 85) {
        return .checkpoint;
    } else if (roll < 92) {
        return .vacuum;
    } else {
        return .reopen;
    }
}

// ---------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------

const Side = struct {
    allocator: Allocator,
    path: []const u8,
    store: docstore.Store,
    use_redo_log: bool,
    page_reuse: bool,

    fn reopen(self: *Side) !void {
        self.store.close();
        self.store = try docstore.Store.openWithOptions(self.allocator, self.path, .{
            .io = std.testing.io,
            .no_sync = true,
            .reclamation = .{ .page_reuse = self.page_reuse },
        });
    }

    fn close(self: *Side) void {
        self.store.close();
    }
};

fn applyOp(side: *Side, op: Op) !void {
    switch (op) {
        .put => |p| {
            var txn = try side.store.beginWrite();
            try txn.put(p.key, p.value);
            try txn.commit();
        },
        .put_large => |p| {
            var txn = try side.store.beginWrite();
            try txn.put(p.key, p.value);
            try txn.commit();
        },
        .delete => |d| {
            var txn = try side.store.beginWrite();
            try txn.delete(d.key);
            try txn.commit();
        },
        .batch => |items| {
            var txn = try side.store.beginWrite();
            for (items) |item| {
                if (item.value) |v| try txn.put(item.key, v) else try txn.delete(item.key);
            }
            try txn.commit();
        },
        .catalog_put => |c| {
            var txn = try side.store.beginWrite();
            try txn.putIndexCatalogBatch(c.scope, &.{.{ .namespace = c.namespace, .key = c.key, .value = c.value }});
            try txn.commit();
        },
        .catalog_delete => |c| {
            var txn = try side.store.beginWrite();
            try txn.putIndexCatalogBatch(c.scope, &.{.{ .namespace = c.namespace, .key = c.key, .is_delete = true }});
            try txn.commit();
        },
        .checkpoint => {
            // `use_redo_log` is this side's *creation-time* setting, not its
            // current one: `NativeFile.replaceWithPreparedGeneration`
            // deliberately and permanently drops `self.redo` the moment a
            // vacuum runs (native.zig:17374's own doc comment -- the
            // compacted file folds every committed mutation durably through
            // the vacuum copy pass itself, so it reverts to ordinary
            // manifest-commit durability and never re-enables the log).
            // Check the live field, not the static flag, or this call
            // spuriously fails with `error.InvalidTransactionState` on any
            // step sequence that vacuums before its next checkpoint --
            // confirmed live while qualifying this oracle, not a redo/legacy
            // divergence.
            if (side.store.file.redo != null) try side.store.file.checkpointRedoLog();
        },
        .vacuum => {
            _ = try side.store.vacuum();
        },
        .reopen => try side.reopen(),
    }
}

fn optionalEql(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null or b == null) return a == null and b == null;
    return std.mem.eql(u8, a.?, b.?);
}

const Failure = error{RedoDifferentialMismatch};

fn fail(seed: u64, step: usize, op: Op, comptime fmt: []const u8, args: anytype) Failure {
    var buf: [256]u8 = undefined;
    const desc = op.describe(&buf);
    std.debug.print("redo differential oracle: seed={d} step={d} op={s} ", .{ seed, step, desc });
    std.debug.print(fmt, args);
    std.debug.print("\n", .{});
    return error.RedoDifferentialMismatch;
}

/// Compares every read API both sides expose, for the state as of right
/// after `op` was applied identically to both. `seed`/`step`/`op` are only
/// for diagnostics on a mismatch.
fn compareAll(allocator: Allocator, seed: u64, step: usize, op: Op, redo: *Side, legacy: *Side, keyspace: *Keyspace) !void {
    // 1. Structural integrity: no crash happened here, so both must report
    // a clean, fully-valid file -- the `tail_bytes` tolerance
    // `crash_consistency_test.zig` needs for an interrupted append has no
    // place in an uncrashed step.
    const redo_check = try redo.store.file.check();
    const legacy_check = try legacy.store.file.check();
    if (!redo_check.valid) return fail(seed, step, op, "redo check() invalid issue={s}", .{redo_check.issue orelse "<none>"});
    if (!legacy_check.valid) return fail(seed, step, op, "legacy check() invalid issue={s}", .{legacy_check.issue orelse "<none>"});

    // 2. Document reads: point get, batch get, forward/backward full scan,
    // forward/backward bounded range scan.
    var redo_txn = try redo.store.beginRead();
    defer redo_txn.abort();
    var legacy_txn = try legacy.store.beginRead();
    defer legacy_txn.abort();

    // Probe set: every currently-live key, a handful of deleted keys (must
    // resolve to absent on both sides), and a couple of keys that never
    // existed at all.
    var probes = std.ArrayListUnmanaged([]const u8).empty;
    defer probes.deinit(allocator);
    try probes.appendSlice(allocator, keyspace.doc_keys.items);
    var deleted_sample: usize = 0;
    var di = keyspace.deleted_doc_keys.items.len;
    while (di > 0 and deleted_sample < 5) {
        di -= 1;
        try probes.append(allocator, keyspace.deleted_doc_keys.items[di]);
        deleted_sample += 1;
    }
    try probes.append(allocator, "grp0/doc-does-not-exist");
    try probes.append(allocator, "grp9/doc-never-written");

    for (probes.items) |key| {
        const r = redo_txn.get(key) catch |e| if (e == error.NotFound) null else return e;
        const l = legacy_txn.get(key) catch |e| if (e == error.NotFound) null else return e;
        if (!optionalEql(r, l)) return fail(seed, step, op, "point get mismatch key={s} redo={?s} legacy={?s}", .{ key, r, l });
    }

    // Batch get (getManySorted requires a sorted, deduplicated key slice).
    const sorted = try allocator.dupe([]const u8, probes.items);
    defer allocator.free(sorted);
    std.mem.sort([]const u8, sorted, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lessThan);
    var unique: std.ArrayListUnmanaged([]const u8) = .empty;
    defer unique.deinit(allocator);
    for (sorted) |k| {
        if (unique.items.len == 0 or !std.mem.eql(u8, unique.items[unique.items.len - 1], k)) try unique.append(allocator, k);
    }
    const redo_values = try allocator.alloc(?[]const u8, unique.items.len);
    defer allocator.free(redo_values);
    const legacy_values = try allocator.alloc(?[]const u8, unique.items.len);
    defer allocator.free(legacy_values);
    try redo_txn.getManySorted(unique.items, redo_values);
    try legacy_txn.getManySorted(unique.items, legacy_values);
    for (unique.items, redo_values, legacy_values) |key, r, l| {
        if (!optionalEql(r, l)) return fail(seed, step, op, "getManySorted mismatch key={s} redo={?s} legacy={?s}", .{ key, r, l });
    }

    // Full forward scan.
    try compareCursorScan(seed, step, op, &redo_txn, &legacy_txn, true);
    // Full backward scan.
    try compareCursorScan(seed, step, op, &redo_txn, &legacy_txn, false);

    // Bounded range/prefix scan: pick a prefix group and scan only within
    // it, forward and backward, via seekAtOrAfter + setUpperBound.
    if (unique.items.len >= 2) {
        const group = std.fmt.comptimePrint("grp{d}/", .{0});
        try compareRangeScan(seed, step, op, &redo_txn, &legacy_txn, group, true);
        try compareRangeScan(seed, step, op, &redo_txn, &legacy_txn, group, false);
    }

    // 3. Catalog reads: full catalog scan, plus a directory-cursor pass
    // over the reserved index-catalog root (every scope here is slash-free
    // past `dirN/scope`, so the "immediate children only" skip-subtree
    // branch participates without changing expected output versus a plain
    // scan -- see this file's own doc comment).
    try compareCatalogScan(allocator, seed, step, op, redo, legacy);
}

const CursorEntry = struct { key: []const u8, value: []const u8 };

fn cursorNextOrEnd(cursor: anytype, first_call: bool, forward: bool) !?CursorEntry {
    const entry = if (first_call)
        (if (forward) cursor.first() else cursor.last())
    else
        (if (forward) cursor.next() else cursor.prev());
    const resolved = entry catch |e| {
        if (e == error.NotFound) return null;
        return e;
    };
    return .{ .key = resolved.key, .value = resolved.value };
}

fn compareCursorScan(seed: u64, step: usize, op: Op, redo_txn: *docstore.Txn, legacy_txn: *docstore.Txn, forward: bool) !void {
    var rc = try redo_txn.openCursor();
    defer rc.close();
    var lc = try legacy_txn.openCursor();
    defer lc.close();

    var first_call = true;
    var count: usize = 0;
    while (true) {
        const r = try cursorNextOrEnd(&rc, first_call, forward);
        const l = try cursorNextOrEnd(&lc, first_call, forward);
        first_call = false;
        if (r == null or l == null) {
            if ((r == null) != (l == null)) {
                const which = if (r == null) "legacy has more" else "redo has more";
                return fail(seed, step, op, "cursor scan length mismatch forward={} at count={d}: {s}", .{ forward, count, which });
            }
            break;
        }
        if (!std.mem.eql(u8, r.?.key, l.?.key)) return fail(seed, step, op, "cursor scan key mismatch forward={} count={d} redo={s} legacy={s}", .{ forward, count, r.?.key, l.?.key });
        if (!std.mem.eql(u8, r.?.value, l.?.value)) return fail(seed, step, op, "cursor scan value mismatch forward={} key={s} redo={s} legacy={s}", .{ forward, r.?.key, r.?.value, l.?.value });
        count += 1;
    }
}

fn compareRangeScan(seed: u64, step: usize, op: Op, redo_txn: *docstore.Txn, legacy_txn: *docstore.Txn, prefix: []const u8, forward: bool) !void {
    var upper_buf: [64]u8 = undefined;
    @memcpy(upper_buf[0..prefix.len], prefix);
    upper_buf[prefix.len - 1] += 1;
    const upper = upper_buf[0..prefix.len];

    var rc = try redo_txn.openCursor();
    defer rc.close();
    var lc = try legacy_txn.openCursor();
    defer lc.close();
    rc.setUpperBound(upper);
    lc.setUpperBound(upper);

    var r_entry = (if (forward) rc.seekAtOrAfter(prefix) else rc.seekAtOrBefore(upper)) catch |e| if (e == error.NotFound) null else return e;
    var l_entry = (if (forward) lc.seekAtOrAfter(prefix) else lc.seekAtOrBefore(upper)) catch |e| if (e == error.NotFound) null else return e;
    var count: usize = 0;
    while (true) {
        if (r_entry == null or l_entry == null) {
            if ((r_entry == null) != (l_entry == null)) return fail(seed, step, op, "range scan length mismatch prefix={s} forward={} count={d}", .{ prefix, forward, count });
            break;
        }
        if (!std.mem.eql(u8, r_entry.?.key, l_entry.?.key)) return fail(seed, step, op, "range scan key mismatch prefix={s} forward={} count={d} redo={s} legacy={s}", .{ prefix, forward, count, r_entry.?.key, l_entry.?.key });
        if (!std.mem.eql(u8, r_entry.?.value, l_entry.?.value)) return fail(seed, step, op, "range scan value mismatch prefix={s} key={s} redo={s} legacy={s}", .{ prefix, r_entry.?.key, r_entry.?.value, l_entry.?.value });
        count += 1;
        r_entry = (if (forward) rc.next() else rc.prev()) catch |e| if (e == error.NotFound) null else return e;
        l_entry = (if (forward) lc.next() else lc.prev()) catch |e| if (e == error.NotFound) null else return e;
    }
}

fn compareCatalogScan(allocator: Allocator, seed: u64, step: usize, op: Op, redo: *Side, legacy: *Side) !void {
    {
        var rc = try redo.store.file.indexCatalogCursor(redo.store.file.activeCheckpoint(), "");
        defer rc.deinit();
        var lc = try legacy.store.file.indexCatalogCursor(legacy.store.file.activeCheckpoint(), "");
        defer lc.deinit();
        var count: usize = 0;
        while (true) {
            const r = try rc.nextRecordAlloc(allocator);
            defer if (r) |rec| {
                allocator.free(rec.key);
                allocator.free(rec.value);
            };
            const l = try lc.nextRecordAlloc(allocator);
            defer if (l) |rec| {
                allocator.free(rec.key);
                allocator.free(rec.value);
            };
            if (r == null or l == null) {
                if ((r == null) != (l == null)) return fail(seed, step, op, "catalog scan length mismatch at count={d}", .{count});
                break;
            }
            if (!std.mem.eql(u8, r.?.key, l.?.key)) return fail(seed, step, op, "catalog scan key mismatch count={d} redo={s} legacy={s}", .{ count, r.?.key, l.?.key });
            if (!std.mem.eql(u8, r.?.value, l.?.value)) return fail(seed, step, op, "catalog scan value mismatch key={s} redo={s} legacy={s}", .{ r.?.key, r.?.value, l.?.value });
            count += 1;
        }
    }
    // Directory-cursor pass over the reserved index-catalog root.
    {
        var rc = try redo.store.file.indexCatalogDirectoryCursor(redo.store.file.activeCheckpoint(), native_catalog_sink.reserved_prefix);
        defer rc.deinit();
        var lc = try legacy.store.file.indexCatalogDirectoryCursor(legacy.store.file.activeCheckpoint(), native_catalog_sink.reserved_prefix);
        defer lc.deinit();
        var count: usize = 0;
        while (true) {
            const r = try rc.nextRecordAlloc(allocator);
            defer if (r) |rec| {
                allocator.free(rec.key);
                allocator.free(rec.value);
            };
            const l = try lc.nextRecordAlloc(allocator);
            defer if (l) |rec| {
                allocator.free(rec.key);
                allocator.free(rec.value);
            };
            if (r == null or l == null) {
                if ((r == null) != (l == null)) return fail(seed, step, op, "catalog directory scan length mismatch at count={d}", .{count});
                break;
            }
            if (!std.mem.eql(u8, r.?.key, l.?.key)) return fail(seed, step, op, "catalog directory scan key mismatch count={d} redo={s} legacy={s}", .{ count, r.?.key, l.?.key });
            if (!std.mem.eql(u8, r.?.value, l.?.value)) return fail(seed, step, op, "catalog directory scan value mismatch key={s} redo={s} legacy={s}", .{ r.?.key, r.?.value, l.?.value });
            count += 1;
        }
    }
}

fn testPath(allocator: Allocator, tmp: std.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

/// Runs one seeded differential session: `steps` operations against a fresh
/// pair of stores (`use_redo_log = true` vs `false`), comparing every read
/// API after each step. Returns the step index at which a mismatch was
/// found (for shrinking), or `null` if the whole run matched.
fn runSeed(allocator: Allocator, seed: u64, steps: usize, page_reuse: bool) !void {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path_redo = try testPath(allocator, tmp, "redo-on.aflite");
    defer allocator.free(path_redo);
    const path_legacy = try testPath(allocator, tmp, "redo-off.aflite");
    defer allocator.free(path_legacy);

    var redo = Side{
        .allocator = allocator,
        .path = path_redo,
        .store = try docstore.Store.createWithOptions(allocator, path_redo, .{
            .io = std.testing.io,
            .no_sync = true,
            .use_redo_log = true,
            .reclamation = .{ .page_reuse = page_reuse },
        }),
        .use_redo_log = true,
        .page_reuse = page_reuse,
    };
    defer redo.close();
    var legacy = Side{
        .allocator = allocator,
        .path = path_legacy,
        .store = try docstore.Store.createWithOptions(allocator, path_legacy, .{
            .io = std.testing.io,
            .no_sync = true,
            .use_redo_log = false,
            .reclamation = .{ .page_reuse = page_reuse },
        }),
        .use_redo_log = false,
        .page_reuse = page_reuse,
    };
    defer legacy.close();

    var keyspace = Keyspace{ .arena = arena };

    var step: usize = 0;
    while (step < steps) : (step += 1) {
        const op = try generateOp(arena, random, &keyspace);
        applyOp(&redo, op) catch |err| {
            std.debug.print("redo differential oracle: seed={d} step={d} applying to redo side failed: {s}\n", .{ seed, step, @errorName(err) });
            return err;
        };
        applyOp(&legacy, op) catch |err| {
            std.debug.print("redo differential oracle: seed={d} step={d} applying to legacy side failed: {s}\n", .{ seed, step, @errorName(err) });
            return err;
        };
        try compareAll(allocator, seed, step, op, &redo, &legacy, &keyspace);
    }
}

/// On a mismatch, bisects the step count (same seed, fresh stores each try)
/// to find the smallest prefix of the operation stream that still
/// reproduces a failure -- the stream is a deterministic function of the
/// random draws consumed in order, so truncating it is a valid shrink.
fn shrinkAndReport(allocator: Allocator, seed: u64, failing_steps: usize, page_reuse: bool) void {
    var lo: usize = 1;
    var hi: usize = failing_steps;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (runSeed(allocator, seed, mid, page_reuse)) |_| {
            lo = mid + 1;
        } else |_| {
            hi = mid;
        }
    }
    std.debug.print("redo differential oracle: minimal reproduction seed={d} steps={d} page_reuse={}\n", .{ seed, hi, page_reuse });
}

fn runSweep(allocator: Allocator, seed_count: u64, steps: usize, page_reuse: bool) !void {
    var seed: u64 = 1;
    while (seed <= seed_count) : (seed += 1) {
        runSeed(allocator, seed, steps, page_reuse) catch |err| {
            std.debug.print("redo differential oracle: seed={d} failed with {s}; shrinking...\n", .{ seed, @errorName(err) });
            shrinkAndReport(allocator, seed, steps, page_reuse);
            return err;
        };
    }
}

test "storage.lite. redo differential oracle: redo log vs legacy commit path" {
    const allocator = std.testing.allocator;
    const seed_count = envUsize("ANTFLY_LITE_REDO_ORACLE_SEEDS", default_seed_count);
    const steps = envUsize("ANTFLY_LITE_REDO_ORACLE_STEPS", default_step_count);
    try runSweep(allocator, seed_count, steps, false);
    if (envFlag("ANTFLY_LITE_REDO_ORACLE_RECLAMATION")) {
        try runSweep(allocator, seed_count, steps, true);
    }
}

// ---------------------------------------------------------------------
// Crash semantics (point 3): each mode, independently, must satisfy the
// existing "acknowledged implies durable" contract (`crash_consistency_
// test.zig`'s own `checkCheckpointsViaNative`/`assertAcknowledgedWriteIsDurable`
// shape) across a crash. The two modes may legitimately differ in which
// *un-acknowledged* writes survive -- this is not a cross-mode diff, it is
// the same proven harness run twice, once per mode, against the exact same
// seeded operation stream this file's own generator produces.
// ---------------------------------------------------------------------

fn makeCrashRunner(comptime use_redo_log: bool, comptime seed: u64, comptime steps: usize) *const fn (allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(crash_consistency.Checkpoint)) anyerror!void {
    const impl = struct {
        fn run(allocator: Allocator, path: []const u8, io: std.Io, checkpoints: *std.ArrayListUnmanaged(crash_consistency.Checkpoint)) anyerror!void {
            // `checkpoints` (and the `Expectation`s it holds, keyed off this
            // run's generated strings) are read by the caller's own
            // `checkInvariants` *after* this function returns -- every
            // existing scenario in this file sidesteps that lifetime by
            // using compile-time string literals for every `Expectation`
            // field (see e.g. `runPrimarySingleDoc`). This scenario's keys
            // and values are generated at runtime, so they need an
            // allocator that outlives this call; a `defer`-torn-down arena
            // over the per-call `allocator` segfaults the caller's later
            // read (confirmed live while qualifying this oracle -- `0x55..`
            // poison, `finalAcknowledgedState` reading a freed `exp.key`).
            // `page_allocator` deliberately leaks this small, bounded
            // scratch instead (a few KB per sweep run) rather than
            // dangling it; it is never routed through `std.testing.
            // allocator`, so it does not trip this test's leak check.
            var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            const arena = arena_state.allocator();

            var prng = std.Random.DefaultPrng.init(seed);
            const random = prng.random();
            var keyspace = Keyspace{ .arena = arena };

            // Native-level directly, like every other scenario in this file
            // (`runPrimarySingleDoc` etc.) -- never `docstore.Store`/`Txn`:
            // a `Txn`'s pending-mutation list (`appendPending`'s own
            // `PendingNode`/owned key+value allocations) is only freed by
            // `Txn.abort()`/a successful `commit()`'s own bookkeeping, and
            // this scenario's `run` must propagate the shim's injected
            // crash error straight out of a write call (the harness's own
            // contract) with no chance to unwind a half-finished `Txn` --
            // confirmed live while qualifying this oracle: every crashed
            // run of a `docstore.Txn`-based version of this scenario leaked
            // the interrupted transaction's pending allocations. The native
            // layer's `putDocument`/`putDocumentBatch`/`deleteDocument` are
            // each one call with no separate abort step, exactly like the
            // hand-written scenarios already in this file.
            var file = if (use_redo_log)
                try native.NativeFile.createWithRedoLog(allocator, io, path, .{}, native.NativeFile.default_redo_log_capacity_bytes)
            else
                try native.NativeFile.createWithIo(allocator, io, path, .{});
            defer file.close();

            var last_value: std.StringHashMapUnmanaged([]const u8) = .empty;

            var step: usize = 0;
            while (step < steps) : (step += 1) {
                const op = try generateOp(arena, random, &keyspace);
                // Only document put/delete/batch ops carry an `Expectation`
                // the existing checker can verify (catalog/vacuum/checkpoint/
                // reopen have no equivalent in `checkCheckpointsViaNative`'s
                // document-keyed model); apply those as before but append no
                // checkpoint for them.
                switch (op) {
                    .put, .put_large => {
                        const kv: PutRef = switch (op) {
                            .put => |p| p,
                            .put_large => |p| p,
                            else => unreachable,
                        };
                        const before = last_value.get(kv.key);
                        try checkpoints.append(allocator, .{ .expectations = try arena.dupe(crash_consistency.Expectation, &.{.{ .key = kv.key, .before = before, .after = kv.value }}) });
                        try file.putDocument(kv.key, kv.value);
                        try last_value.put(arena, kv.key, kv.value);
                    },
                    .delete => |d| {
                        const before = last_value.get(d.key);
                        try checkpoints.append(allocator, .{ .expectations = try arena.dupe(crash_consistency.Expectation, &.{.{ .key = d.key, .before = before, .after = null }}) });
                        try file.deleteDocument(d.key);
                        _ = last_value.remove(d.key);
                    },
                    .batch => |items| {
                        const exps = try arena.alloc(crash_consistency.Expectation, items.len);
                        const mutations = try arena.alloc(native.DocumentMutation, items.len);
                        for (items, exps, mutations) |item, *exp, *mutation| {
                            exp.* = .{ .key = item.key, .before = last_value.get(item.key), .after = item.value };
                            mutation.* = if (item.value) |v| .{ .key = item.key, .value = v } else .{ .key = item.key, .is_delete = true };
                        }
                        try checkpoints.append(allocator, .{ .expectations = exps });
                        try file.putDocumentBatch(mutations);
                        for (items) |item| {
                            if (item.value) |v| try last_value.put(arena, item.key, v) else _ = last_value.remove(item.key);
                        }
                    },
                    .checkpoint => if (file.redo != null) try file.checkpointRedoLog(),
                    .vacuum => _ = try file.vacuum(),
                    .reopen => {
                        file.close();
                        file = try native.NativeFile.openWithIo(allocator, io, path, .{});
                    },
                    .catalog_put, .catalog_delete => {}, // no document-keyed expectation; skip for this checker.
                }
            }
        }
    };
    return impl.run;
}

fn runCrashSweepForMode(allocator: Allocator, comptime use_redo_log: bool, comptime seed: u64, comptime steps: usize) !void {
    var name_buf: [128]u8 = undefined;
    const name = try std.fmt.bufPrint(&name_buf, "redo differential oracle crash sweep use_redo_log={} seed={d}", .{ use_redo_log, seed });
    const scenario = crash_consistency.Scenario{
        .name = name,
        .run = makeCrashRunner(use_redo_log, seed, steps),
    };
    var report = try crash_consistency.runCrashSweep(allocator, scenario, .{ .mode = .quick, .quick_sample = 8 });
    defer report.deinit();
    if (!report.ok()) {
        for (report.failures.items) |f| {
            std.debug.print("redo differential oracle crash sweep: use_redo_log={} seed={d} barrier={d} when={s} power_loss={s}: {s}\n", .{ use_redo_log, seed, f.barrier.sequence, @tagName(f.when), @tagName(f.power_loss), f.detail });
        }
        return error.RedoDifferentialCrashContractViolated;
    }
}

test "storage.lite. redo differential oracle: crash contract holds under both commit paths" {
    if (!envFlag("ANTFLY_LITE_REDO_ORACLE_CRASH")) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    try runCrashSweepForMode(allocator, true, 1, 40);
    try runCrashSweepForMode(allocator, false, 1, 40);
}
