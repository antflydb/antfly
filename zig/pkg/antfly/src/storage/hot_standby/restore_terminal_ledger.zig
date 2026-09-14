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

//! Indexed cancellation authority. Durable applied-prefix admission floors
//! permit bounded reclamation only after exact native owner cleanup completes.
const std = @import("std");
const lsm = @import("../lsm_backend.zig");
const fs = @import("../../common/fs_paths.zig");
const record_mod = @import("replication_record.zig");
pub const replay_floor = @import("replay_floor.zig");
const ns: @import("../backend_types.zig").Namespace = .{ .name = "restore-terminal" };
const meta_ns: @import("../backend_types.zig").Namespace = .{ .name = "restore-terminal-meta" };
pub const directory = "restore-terminal-ledger";
pub const artifact_name = @import("restore_owner_contract.zig").terminal_artifact_name;
const magic = "AFHTERM2";
const header_size = magic.len + replay_floor.size;
pub const record_size = 148;
pub const export_buffer_bytes = 64 * 1024;
pub const import_batch_records = 128;

pub const Terminal = struct {
    group_id: u64,
    table_id: u64,
    cluster_id: u64,
    timeline_id: u64,
    epoch: u64,
    lsn: u64,
    scope: [32]u8,
    payload: [32]u8,

    pub fn fromRecord(record: record_mod.RecordView, scope: [32]u8) Terminal {
        var payload: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(record.payload, &payload, .{});
        return .{ .group_id = record.shard_id, .table_id = record.table_id, .cluster_id = record.cluster_id, .timeline_id = record.timeline_id, .epoch = record.epoch, .lsn = record.lsn, .scope = scope, .payload = payload };
    }

    pub fn encode(self: Terminal) ![record_size]u8 {
        if (self.group_id == 0 or self.table_id == 0 or self.cluster_id == 0 or self.timeline_id == 0 or self.epoch == 0 or self.lsn == 0) return error.InvalidRestoreTerminal;
        var out: [record_size]u8 = undefined;
        out[0..4].* = "ART1".*;
        inline for (.{ "group_id", "table_id", "cluster_id", "timeline_id", "epoch", "lsn" }, 0..) |field, index| std.mem.writeInt(u64, out[4 + index * 8 ..][0..8], @field(self, field), .big);
        out[52..84].* = self.scope;
        out[84..116].* = self.payload;
        std.crypto.hash.Blake3.hash(out[0..116], out[116..148], .{});
        return out;
    }

    pub fn decode(bytes: []const u8) !Terminal {
        if (bytes.len != record_size or !std.mem.eql(u8, bytes[0..4], "ART1")) return error.InvalidRestoreTerminal;
        var hash: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..116], &hash, .{});
        if (!std.mem.eql(u8, &hash, bytes[116..148])) return error.InvalidRestoreTerminal;
        var result: Terminal = undefined;
        inline for (.{ "group_id", "table_id", "cluster_id", "timeline_id", "epoch", "lsn" }, 0..) |field, index| @field(result, field) = std.mem.readInt(u64, bytes[4 + index * 8 ..][0..8], .big);
        result.scope = bytes[52..84].*;
        result.payload = bytes[84..116].*;
        _ = try result.encode();
        return result;
    }
};

fn key(group_id: u64) [8]u8 {
    var result: [8]u8 = undefined;
    std.mem.writeInt(u64, &result, group_id, .big);
    return result;
}

pub const Artifact = struct { size_bytes: u64, sha256: [32]u8, count: u64 };
pub const ReadTxn = @TypeOf(@as(*lsm.Backend, undefined).beginRead() catch unreachable);
pub const Snapshot = struct {
    txn: ReadTxn,
    pub fn deinit(self: *Snapshot) void {
        self.txn.abort();
    }
    pub fn get(self: *Snapshot, group_id: u64) !?Terminal {
        const bytes = self.txn.get(ns, &key(group_id)) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        const value = try Terminal.decode(bytes);
        if (value.group_id != group_id) return error.InvalidRestoreTerminal;
        return value;
    }
    pub fn floor(self: *Snapshot) !?replay_floor.Floor {
        const bytes = self.txn.get(meta_ns, "floor") catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try replay_floor.Floor.decode(bytes);
    }
    pub fn hasTerminals(self: *Snapshot) !bool {
        var cursor = try self.txn.openCursor(ns);
        defer cursor.close();
        return try cursor.first() != null;
    }
    pub fn requireReplay(self: *Snapshot, record: record_mod.RecordView) !void {
        if (try self.floor()) |current| if (try current.covers(.{ .cluster_id = record.cluster_id, .timeline_id = record.timeline_id, .epoch = record.epoch, .lsn = record.lsn })) return error.HAReplayBelowReclamationFloor;
    }
    pub fn exportFile(self: *Snapshot, io: std.Io, path: []const u8) !Artifact {
        var file = try std.Io.Dir.cwd().createFile(io, path, .{ .exclusive = true });
        defer file.close(io);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        var buffer: [export_buffer_bytes]u8 = undefined;
        @memcpy(buffer[0..magic.len], magic);
        @memset(buffer[magic.len..header_size], 0);
        if (try self.floor()) |current| buffer[magic.len..header_size].* = try current.encode();
        var buffered: usize = header_size;
        hash.update(buffer[0..header_size]);
        var count: u64 = 0;
        var cursor = try self.txn.openCursor(ns);
        defer cursor.close();
        var entry = try cursor.first();
        while (entry) |item| : (entry = try cursor.next()) {
            const value = try Terminal.decode(item.value);
            if (!std.mem.eql(u8, item.key, &key(value.group_id))) return error.InvalidRestoreTerminal;
            if (buffered + item.value.len > buffer.len) {
                try file.writeStreamingAll(io, buffer[0..buffered]);
                buffered = 0;
            }
            @memcpy(buffer[buffered..][0..item.value.len], item.value);
            buffered += item.value.len;
            hash.update(item.value);
            count += 1;
        }
        if (buffered != 0) try file.writeStreamingAll(io, buffer[0..buffered]);
        try file.sync(io);
        try fs.syncDirPortable(io, std.fs.path.dirname(path).?);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return .{ .size_bytes = header_size + count * record_size, .sha256 = digest, .count = count };
    }
};

pub const Ledger = struct {
    backend: lsm.BackendHandle,
    pub fn open(alloc: std.mem.Allocator, io: std.Io, metadata_root: []const u8) !Ledger {
        const path = try std.fs.path.join(alloc, &.{ metadata_root, directory });
        defer alloc.free(path);
        try fs.createDirPathPortable(io, path);
        var backend = try lsm.BackendHandle.open(alloc, path, .{ .backend = .{ .durability = .full }, .flush_threshold = 4096 });
        errdefer backend.close();
        try fs.syncDirPortable(io, path);
        try fs.syncDirPortable(io, metadata_root);
        if (std.fs.path.dirname(metadata_root)) |parent| try fs.syncDirPortable(io, parent);
        return .{ .backend = backend };
    }
    pub fn deinit(self: *Ledger) void {
        self.backend.close();
    }
    pub fn snapshot(self: *Ledger) !Snapshot {
        return .{ .txn = try self.backend.backend.beginRead() };
    }
    pub fn record(self: *Ledger, terminal: Terminal) !void {
        const bytes = try terminal.encode();
        var txn = try self.backend.backend.beginWrite();
        defer txn.abort();
        const existing = txn.get(ns, &key(terminal.group_id)) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (existing) |value| {
            const previous = try Terminal.decode(value);
            if (previous.table_id != terminal.table_id or previous.cluster_id != terminal.cluster_id or !std.mem.eql(u8, &previous.scope, &terminal.scope)) return error.RestoreStagingScopeChanged;
            return;
        }
        try txn.put(ns, &key(terminal.group_id), &bytes);
        try txn.commit();
    }
    /// Caller supplies durably persisted, contiguous HA applied progress. This
    /// commit must precede every proof deletion and every external GC receipt.
    pub fn advanceFloor(self: *Ledger, next: replay_floor.Floor) !void {
        const encoded = try next.encode();
        var txn = try self.backend.backend.beginWrite();
        defer txn.abort();
        const previous = txn.get(meta_ns, "floor") catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (previous) |bytes| if (try (try replay_floor.Floor.decode(bytes)).covers(next)) return;
        try txn.put(meta_ns, "floor", &encoded);
        try txn.commit();
    }

    pub const GCResult = struct { inspected: usize = 0, reclaimed: usize = 0 };
    pub const EligibleFn = *const fn (*anyopaque, Terminal) anyerror!bool;
    pub const max_gc_records = 128;
    /// Persistent seek cursor bounds both metadata work and owner-cleanup
    /// probes. The caller must prove exact native root/registry retirement;
    /// an applied LSN alone never authorizes deleting cleanup authority.
    pub fn gcStep(self: *Ledger, budget: usize, ctx: *anyopaque, eligible: EligibleFn) !GCResult {
        if (budget == 0 or budget > max_gc_records) return error.InvalidTerminalGCBudget;
        var snapshot_view = try self.snapshot();
        defer snapshot_view.deinit();
        const current = (try snapshot_view.floor()) orelse return .{};
        const after = snapshot_view.txn.get(meta_ns, "cursor") catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (after) |bytes| if (bytes.len != 8) return error.InvalidRestoreTerminal;
        var cursor = try snapshot_view.txn.openCursor(ns);
        defer cursor.close();
        var entry = if (after) |bytes| try cursor.seekAtOrAfter(bytes) else try cursor.first();
        if (entry) |item| if (after) |bytes| if (std.mem.eql(u8, item.key, bytes)) {
            entry = try cursor.next();
        };
        var result: GCResult = .{};
        var last: ?[8]u8 = null;
        var removals: [max_gc_records]u64 = undefined;
        while (entry) |item| {
            const terminal = try Terminal.decode(item.value);
            if (!std.mem.eql(u8, item.key, &key(terminal.group_id))) return error.InvalidRestoreTerminal;
            last = key(terminal.group_id);
            result.inspected += 1;
            if (try current.covers(.{ .cluster_id = terminal.cluster_id, .timeline_id = terminal.timeline_id, .epoch = terminal.epoch, .lsn = terminal.lsn })) {
                if (try eligible(ctx, terminal)) {
                    removals[result.reclaimed] = terminal.group_id;
                    result.reclaimed += 1;
                }
            }
            entry = try cursor.next();
            if (result.inspected == budget) break;
        }
        if (result.reclaimed == 0 and after == null and entry == null) return result;
        var txn = try self.backend.backend.beginWrite();
        defer txn.abort();
        for (removals[0..result.reclaimed]) |group_id| try txn.delete(ns, &key(group_id));
        if (entry != null and last != null) try txn.put(meta_ns, "cursor", &last.?) else try txn.delete(meta_ns, "cursor");
        try txn.commit();
        return result;
    }
    /// Bounded transfer memory and bounded transactions, independent of history.
    /// The enclosing unpublished seed verifies the complete artifact checksum.
    pub fn importFile(self: *Ledger, io: std.Io, path: []const u8, size_bytes: u64) !void {
        if (size_bytes < header_size or (size_bytes - header_size) % record_size != 0) return error.InvalidRestoreTerminal;
        var file = try std.Io.Dir.cwd().openFile(io, path, .{});
        defer file.close(io);
        var header: [header_size]u8 = undefined;
        if (try file.readPositionalAll(io, &header, 0) != header.len or !std.mem.eql(u8, header[0..magic.len], magic)) return error.InvalidRestoreTerminal;
        if (!std.mem.allEqual(u8, header[magic.len..], 0)) try self.advanceFloor(try replay_floor.Floor.decode(header[magic.len..]));
        var offset: u64 = header_size;
        var previous: u64 = 0;
        var buffer: [import_batch_records * record_size]u8 = undefined;
        while (offset < size_bytes) {
            const wanted: usize = @intCast(@min(buffer.len, size_bytes - offset));
            if (try file.readPositionalAll(io, buffer[0..wanted], offset) != wanted) return error.InvalidRestoreTerminal;
            var txn = try self.backend.backend.beginWrite();
            defer txn.abort();
            var position: usize = 0;
            while (position < wanted) : (position += record_size) {
                const bytes = buffer[position..][0..record_size];
                const value = try Terminal.decode(bytes);
                if (value.group_id <= previous) return error.InvalidRestoreTerminal;
                previous = value.group_id;
                try txn.put(ns, &key(value.group_id), bytes);
            }
            try txn.commit();
            offset += wanted;
        }
        var extra: [1]u8 = undefined;
        if (try file.readPositionalAll(io, &extra, offset) != 0) return error.InvalidRestoreTerminal;
        try self.backend.backend.sync(true);
    }
};

test "HA terminal ledger streams history beyond active owner cap with fixed buffers" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    const input = try std.fs.path.join(alloc, &.{ root, "input.bin" });
    defer alloc.free(input);
    const output = try std.fs.path.join(alloc, &.{ root, "output.bin" });
    defer alloc.free(output);
    const metadata = try std.fs.path.join(alloc, &.{ root, "metadata" });
    defer alloc.free(metadata);
    // Deliberately exceed the bounded active-owner topology. Neither fixture
    // construction nor import/export allocates an array proportional to this.
    const proof_count = 65_537;
    const size_bytes = header_size + proof_count * record_size;
    var expected_hash = std.crypto.hash.sha2.Sha256.init(.{});
    {
        var file = try std.Io.Dir.cwd().createFile(io, input, .{ .exclusive = true });
        defer file.close(io);
        try file.writeStreamingAll(io, magic);
        expected_hash.update(magic);
        try file.writeStreamingAll(io, &(@as([replay_floor.size]u8, @splat(0))));
        expected_hash.update(&(@as([replay_floor.size]u8, @splat(0))));
        var buffer: [import_batch_records * record_size]u8 = undefined;
        var start: usize = 0;
        while (start < proof_count) {
            const count: usize = @min(import_batch_records, proof_count - start);
            for (0..count) |index| {
                const terminal: Terminal = .{ .group_id = start + index + 1, .table_id = 7, .cluster_id = 10, .timeline_id = 1, .epoch = 1, .lsn = start + index + 2, .scope = @splat(1), .payload = @splat(2) };
                const encoded = try terminal.encode();
                @memcpy(buffer[index * record_size ..][0..record_size], &encoded);
            }
            const bytes = buffer[0 .. count * record_size];
            expected_hash.update(bytes);
            try file.writeStreamingAll(io, bytes);
            start += count;
        }
        try file.sync(io);
    }
    {
        var ledger = try Ledger.open(alloc, io, metadata);
        defer ledger.deinit();
        try ledger.importFile(io, input, size_bytes);
    }
    {
        var reopened = try Ledger.open(alloc, io, metadata);
        defer reopened.deinit();
        var snapshot = try reopened.snapshot();
        defer snapshot.deinit();
        for ([_]u64{ 1, 32_769, proof_count }) |id| {
            const terminal = (try snapshot.get(id)).?;
            try std.testing.expectEqual(id, terminal.group_id);
            try std.testing.expectEqual(id + 1, terminal.lsn);
        }
        const artifact = try snapshot.exportFile(io, output);
        var digest: [32]u8 = undefined;
        expected_hash.final(&digest);
        try std.testing.expectEqual(@as(u64, proof_count), artifact.count);
        try std.testing.expectEqual(@as(u64, size_bytes), artifact.size_bytes);
        try std.testing.expectEqualSlices(u8, &digest, &artifact.sha256);
        try std.testing.expect(export_buffer_bytes < artifact.size_bytes);
        try std.testing.expect(import_batch_records * record_size < artifact.size_bytes);
    }
    // Corruption at the first record of a later transaction must abort that
    // transaction without advancing its proof coverage. Partial output stays
    // inside the enclosing unpublished materialization.
    const damaged_metadata = try std.fs.path.join(alloc, &.{ root, "damaged" });
    defer alloc.free(damaged_metadata);
    {
        var file = try std.Io.Dir.cwd().openFile(io, output, .{ .mode = .read_write });
        defer file.close(io);
        try file.writePositionalAll(io, &.{0xff}, header_size + import_batch_records * record_size + 80);
        try file.sync(io);
    }
    var damaged = try Ledger.open(alloc, io, damaged_metadata);
    defer damaged.deinit();
    try std.testing.expectError(error.InvalidRestoreTerminal, damaged.importFile(io, output, size_bytes));
    var partial = try damaged.snapshot();
    defer partial.deinit();
    try std.testing.expect(try partial.get(import_batch_records) != null);
    try std.testing.expect(try partial.get(import_batch_records + 1) == null);
    try std.testing.expectError(error.InvalidRestoreTerminal, damaged.importFile(io, input, size_bytes - 1));
}

test "storage.hot_standby terminal floor bounds GC and preserves replay rejection through restart and seed" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    const source_root = try std.fs.path.join(alloc, &.{ root, "source" });
    defer alloc.free(source_root);
    const target_root = try std.fs.path.join(alloc, &.{ root, "target" });
    defer alloc.free(target_root);
    const artifact_path = try std.fs.path.join(alloc, &.{ root, "proofs.bin" });
    defer alloc.free(artifact_path);
    const Probe = struct {
        held: bool = true,
        calls: usize = 0,
        fn eligible(ptr: *anyopaque, terminal: Terminal) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return terminal.group_id != 1 or !self.held;
        }
    };
    var probe: Probe = .{};
    const floor: replay_floor.Floor = .{ .cluster_id = 7, .timeline_id = 1, .epoch = 1, .lsn = 60 };
    {
        var source = try Ledger.open(alloc, io, source_root);
        defer source.deinit();
        for (1..7) |group| try source.record(.{ .group_id = group, .table_id = 11, .cluster_id = 7, .timeline_id = 1, .epoch = 1, .lsn = group * 10, .scope = @splat(1), .payload = @splat(2) });
        try std.testing.expectEqual(@as(usize, 0), (try source.gcStep(2, &probe, Probe.eligible)).inspected);
        try source.advanceFloor(floor);
        // Crash boundary: floor is durable, no proof has been deleted yet.
    }
    var source = try Ledger.open(alloc, io, source_root);
    defer source.deinit();
    var old_snapshot = try source.snapshot();
    defer old_snapshot.deinit();
    try std.testing.expectEqual(floor, (try old_snapshot.floor()).?);
    const first = try source.gcStep(2, &probe, Probe.eligible);
    try std.testing.expectEqual(@as(usize, 2), first.inspected);
    try std.testing.expectEqual(@as(usize, 1), first.reclaimed);
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    _ = try source.gcStep(2, &probe, Probe.eligible);
    _ = try source.gcStep(2, &probe, Probe.eligible);
    {
        var view = try source.snapshot();
        defer view.deinit();
        try std.testing.expect(try view.get(1) != null);
        try std.testing.expect(try view.get(2) == null);
        try std.testing.expect(try view.get(6) == null);
        // Pinned old readers remain immutable during reclamation.
        try std.testing.expect(try old_snapshot.get(2) != null);
    }
    probe.held = false;
    try std.testing.expectEqual(@as(usize, 1), (try source.gcStep(2, &probe, Probe.eligible)).reclaimed);
    var latest = try source.snapshot();
    defer latest.deinit();
    const artifact = try latest.exportFile(io, artifact_path);
    try std.testing.expectEqual(@as(u64, 0), artifact.count);
    var target = try Ledger.open(alloc, io, target_root);
    defer target.deinit();
    try target.importFile(io, artifact_path, artifact.size_bytes);
    var imported = try target.snapshot();
    defer imported.deinit();
    try std.testing.expectEqual(floor, (try imported.floor()).?);
    var record: record_mod.RecordView = .{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 7, .timeline_id = 1, .epoch = 1, .lsn = 10, .previous_lsn = 9, .table_id = 11, .shard_id = 1, .payload = "stale begin" };
    try std.testing.expectError(error.HAReplayBelowReclamationFloor, imported.requireReplay(record));
    record.lsn = 61;
    try imported.requireReplay(record);
    record.timeline_id = 2;
    try std.testing.expectError(error.HAReplayFloorIdentityMismatch, imported.requireReplay(record));
}
