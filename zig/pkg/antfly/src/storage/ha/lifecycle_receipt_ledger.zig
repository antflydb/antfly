// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

//! Runtime-owned durable history for HA seed capture and activation receipts.
//!
//! The exact authoritative receipt is appended only after its filesystem
//! publication boundary is durable. WAL payloads are bounded by prefix
//! truncation, while the WAL's compact idempotency index retains identity and
//! digest bindings so a retry can never duplicate or mutate an old event.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;
const platform_time = @import("../../platform/time.zig");
const wal_mod = @import("../wal.zig");
const validation = @import("validation.zig");

pub const ledger_dir_name = ".antfly-ha-lifecycle-receipts.wal";
pub const default_max_events: usize = 4096;
pub const max_page_limit: usize = 1000;
const max_receipt_bytes: usize = 16 * 1024 * 1024;
const event_schema_version: u16 = 1;

pub const Kind = enum { capture, activation };
pub const RuntimeRole = enum { primary, standby, unknown };
pub const AuthoritativeState = enum { retained, missing };

pub const RuntimeObservation = struct {
    node_id: ?[]const u8 = null,
    role: RuntimeRole = .unknown,
    pod_uid: ?[]const u8 = null,
    fenced: bool = false,
    observed_at_unix_ns: u64 = 0,
};

pub const RecordMetadata = struct {
    pod_uid: ?[]const u8 = null,
    recorded_at_unix_ns: u64 = 0,
};

pub const OpenOptions = struct {
    max_events: usize = default_max_events,
    wal_options: wal_mod.WalOptions = .{},
};

pub const PageRequest = struct {
    /// Exclusive cursor. Zero means start with the retained prefix.
    after: u64 = 0,
    limit: usize = 100,
};

pub const ReadOptions = struct {
    authoritative_root: []const u8,
    runtime: RuntimeObservation = .{},
};

pub const AppendResult = struct {
    cursor: u64,
    appended: bool,
};

pub const Entry = struct {
    cursor: u64,
    kind: Kind,
    generation: []u8,
    slot_name: []u8,
    topology_id: []u8,
    topology_generation: u64,
    node_id: []u8,
    target_pvc_name: []u8,
    target_pvc_uid: []u8,
    receipt_sha256: []u8,
    receipt_json: []u8,
    recorded_at_unix_ns: u64,
    pod_uid: ?[]u8,
    authoritative_state: AuthoritativeState,

    fn deinit(self: *Entry, alloc: Allocator) void {
        alloc.free(self.generation);
        alloc.free(self.slot_name);
        alloc.free(self.topology_id);
        alloc.free(self.node_id);
        alloc.free(self.target_pvc_name);
        alloc.free(self.target_pvc_uid);
        alloc.free(self.receipt_sha256);
        alloc.free(self.receipt_json);
        if (self.pod_uid) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const OwnedRuntimeObservation = struct {
    node_id: ?[]u8,
    role: RuntimeRole,
    pod_uid: ?[]u8,
    fenced: bool,
    observed_at_unix_ns: u64,

    fn deinit(self: *OwnedRuntimeObservation, alloc: Allocator) void {
        if (self.node_id) |value| alloc.free(value);
        if (self.pod_uid) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const Page = struct {
    entries: []Entry,
    first_cursor: u64,
    end_cursor: u64,
    next_cursor: u64,
    history_truncated: bool,
    gap: bool,
    has_more: bool,
    runtime: OwnedRuntimeObservation,

    pub fn deinit(self: *Page, alloc: Allocator) void {
        for (self.entries) |*entry| entry.deinit(alloc);
        alloc.free(self.entries);
        self.runtime.deinit(alloc);
        self.* = undefined;
    }
};

const StoredEvent = struct {
    schema_version: u16 = event_schema_version,
    kind: Kind,
    generation: []const u8,
    slot_name: []const u8,
    topology_id: []const u8,
    topology_generation: u64,
    node_id: []const u8,
    target_pvc_name: []const u8,
    target_pvc_uid: []const u8,
    receipt_sha256: []const u8,
    receipt_json: []const u8,
    recorded_at_unix_ns: u64,
    pod_uid: ?[]const u8 = null,
};

const CaptureReceipt = struct {
    format_version: u16,
    generation: []const u8,
    slot_name: []const u8,
    topology_id: []const u8,
    topology_generation: u64,
    node_id: []const u8,
    target_pvc_name: []const u8,
    target_pvc_uid: []const u8,
    cluster_id: u64,
    shard_id: u64,
    table_id: u64,
    timeline_id: u64,
    epoch: u64,
    source_plan_sha256: []const u8,
    manifest_id: []const u8,
    backup_lsn: u64,
    checkpoint_lsn: u64,
    end_record_lsn: u64,
    manifest_sha256: []const u8,
    file_count: usize,
    total_bytes: u64,
};

const ActivationReceipt = struct {
    format_version: u16,
    generation: []const u8,
    slot_name: []const u8,
    cluster_id: u64,
    shard_id: u64,
    table_id: u64,
    timeline_id: u64,
    epoch: u64,
    manifest_id: []const u8,
    backup_lsn: u64,
    checkpoint_lsn: u64,
    seed_receipt_sha256: []const u8,
    capture_receipt_sha256: []const u8,
    manifest_sha256: []const u8,
    aggregate_sha256: []const u8,
    generation_path: []const u8,
    raw_generation_path: []const u8 = "",
    materialized_receipt_sha256: []const u8 = "",
    materialized_aggregate_sha256: []const u8 = "",
    target_local_node_id: u64 = 0,
    target_replica_id: u64 = 0,
    topology_id: []const u8,
    topology_generation: u64,
    node_id: []const u8,
    target_pvc_name: []const u8,
    target_pvc_uid: []const u8,
};

const NormalizedReceipt = struct {
    generation: []const u8,
    slot_name: []const u8,
    topology_id: []const u8,
    topology_generation: u64,
    node_id: []const u8,
    target_pvc_name: []const u8,
    target_pvc_uid: []const u8,
};

pub const Ledger = struct {
    alloc: Allocator,
    root: []u8,
    path: [:0]u8,
    wal: wal_mod.WAL,
    max_events: usize,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn open(alloc: Allocator, lifecycle_root: []const u8, options: OpenOptions) !Ledger {
        if (!validation.isAbsoluteNormalizedPath(lifecycle_root)) return error.InvalidLifecycleRoot;
        if (options.max_events == 0) return error.InvalidLifecycleRetention;
        const raw_path = try std.fs.path.join(alloc, &.{ lifecycle_root, ledger_dir_name });
        defer alloc.free(raw_path);
        const path = try alloc.dupeZ(u8, raw_path);
        errdefer alloc.free(path);
        const root = try alloc.dupe(u8, lifecycle_root);
        errdefer alloc.free(root);
        const wal = try wal_mod.WAL.open(path.ptr, options.wal_options);
        return .{
            .alloc = alloc,
            .root = root,
            .path = path,
            .wal = wal,
            .max_events = options.max_events,
        };
    }

    pub fn close(self: *Ledger) void {
        self.wal.close();
        self.alloc.free(self.root);
        self.alloc.free(self.path);
        self.* = undefined;
    }

    pub fn recordCapture(self: *Ledger, receipt_json: []const u8, metadata: RecordMetadata) !AppendResult {
        var parsed = std.json.parseFromSlice(CaptureReceipt, self.alloc, receipt_json, .{ .ignore_unknown_fields = false }) catch
            return error.InvalidLifecycleReceipt;
        defer parsed.deinit();
        if (parsed.value.format_version != 2) return error.InvalidLifecycleReceipt;
        try validateCaptureReceipt(parsed.value);
        return try self.record(.capture, normalizedCapture(parsed.value), receipt_json, metadata);
    }

    pub fn recordActivation(self: *Ledger, receipt_json: []const u8, metadata: RecordMetadata) !AppendResult {
        var parsed = std.json.parseFromSlice(ActivationReceipt, self.alloc, receipt_json, .{ .ignore_unknown_fields = false }) catch
            return error.InvalidLifecycleReceipt;
        defer parsed.deinit();
        if (parsed.value.format_version != 1 and parsed.value.format_version != 2) return error.InvalidLifecycleReceipt;
        try validateActivationReceipt(parsed.value);
        return try self.record(.activation, normalizedActivation(parsed.value), receipt_json, metadata);
    }

    fn record(
        self: *Ledger,
        kind: Kind,
        receipt: NormalizedReceipt,
        receipt_json: []const u8,
        metadata: RecordMetadata,
    ) !AppendResult {
        if (receipt_json.len == 0 or receipt_json.len > max_receipt_bytes) return error.InvalidLifecycleReceipt;
        lock(&self.mutex);
        defer self.mutex.unlock();

        var digest: [Sha256.digest_length]u8 = undefined;
        Sha256.hash(receipt_json, &digest, .{});
        var digest_hex: [Sha256.digest_length * 2]u8 = undefined;
        encodeHex(&digest_hex, &digest);
        const idempotency_key = try identityKeyAlloc(self.alloc, kind, receipt);
        defer self.alloc.free(idempotency_key);
        const event_json = try std.json.Stringify.valueAlloc(self.alloc, StoredEvent{
            .kind = kind,
            .generation = receipt.generation,
            .slot_name = receipt.slot_name,
            .topology_id = receipt.topology_id,
            .topology_generation = receipt.topology_generation,
            .node_id = receipt.node_id,
            .target_pvc_name = receipt.target_pvc_name,
            .target_pvc_uid = receipt.target_pvc_uid,
            .receipt_sha256 = &digest_hex,
            .receipt_json = receipt_json,
            .recorded_at_unix_ns = if (metadata.recorded_at_unix_ns != 0) metadata.recorded_at_unix_ns else platform_time.realtimeNs(),
            .pod_uid = metadata.pod_uid,
        }, .{});
        defer self.alloc.free(event_json);
        const appended = self.wal.appendIdempotent(idempotency_key, &digest_hex, event_json) catch |err| switch (err) {
            error.IdempotencyConflict => return error.LifecycleReceiptConflict,
            else => return err,
        };
        if (appended.appended and appended.lsn > self.max_events) {
            try self.wal.truncate(appended.lsn - self.max_events);
        }
        return .{ .cursor = appended.lsn, .appended = appended.appended };
    }

    pub fn readPage(self: *Ledger, alloc: Allocator, kind: Kind, request: PageRequest, options: ReadOptions) !Page {
        if (request.limit == 0 or request.limit > max_page_limit) return error.InvalidLifecyclePage;
        if (!validation.isAbsoluteNormalizedPath(options.authoritative_root)) return error.InvalidLifecycleRoot;
        lock(&self.mutex);
        defer self.mutex.unlock();

        const end_cursor = self.wal.lastLsn();
        const wal_entries = try self.wal.iterateFrom(alloc, 1);
        defer freeWalEntries(alloc, wal_entries);
        if (end_cursor != 0 and wal_entries.len == 0) return error.CorruptLifecycleReceiptLedger;
        var first_cursor: u64 = 0;
        if (wal_entries.len > 0) {
            first_cursor = wal_entries[0].lsn;
            var expected = first_cursor;
            for (wal_entries) |wal_entry| {
                if (wal_entry.lsn != expected) return error.CorruptLifecycleReceiptLedger;
                expected = std.math.add(u64, expected, 1) catch return error.CorruptLifecycleReceiptLedger;
            }
            if (wal_entries[wal_entries.len - 1].lsn != end_cursor) return error.CorruptLifecycleReceiptLedger;
        }

        var entries = std.ArrayListUnmanaged(Entry).empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(alloc);
            entries.deinit(alloc);
        }
        var has_more = false;
        for (wal_entries) |wal_entry| {
            var parsed = std.json.parseFromSlice(StoredEvent, alloc, wal_entry.data, .{ .ignore_unknown_fields = false }) catch
                return error.CorruptLifecycleReceiptLedger;
            defer parsed.deinit();
            try validateStoredEvent(alloc, parsed.value);
            if (wal_entry.lsn <= request.after or parsed.value.kind != kind) continue;
            if (entries.items.len >= request.limit) {
                has_more = true;
                continue;
            }
            try entries.append(alloc, try ownedEntry(alloc, wal_entry.lsn, parsed.value, options.authoritative_root));
        }
        const next_cursor = if (entries.items.len > 0)
            entries.items[entries.items.len - 1].cursor
        else if (request.after > end_cursor)
            request.after
        else
            end_cursor;
        const runtime = try ownRuntime(alloc, options.runtime);
        errdefer {
            var owned = runtime;
            owned.deinit(alloc);
        }
        return .{
            .entries = try entries.toOwnedSlice(alloc),
            .first_cursor = first_cursor,
            .end_cursor = end_cursor,
            .next_cursor = next_cursor,
            .history_truncated = first_cursor > 1,
            .gap = request.after != 0 and first_cursor != 0 and request.after +| 1 < first_cursor,
            .has_more = has_more,
            .runtime = runtime,
        };
    }

    pub fn injectTornRecordForTest(self: *Ledger, cursor: u64) !void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        try self.wal.injectCorruptEntryForTest(cursor);
    }
};

fn validateCaptureReceipt(receipt: CaptureReceipt) !void {
    try validateNormalized(normalizedCapture(receipt));
    if (receipt.cluster_id == 0 or receipt.timeline_id == 0 or receipt.epoch == 0 or
        receipt.manifest_id.len == 0 or receipt.backup_lsn == 0 or
        receipt.checkpoint_lsn < receipt.backup_lsn or receipt.end_record_lsn <= receipt.checkpoint_lsn or
        receipt.file_count == 0 or !validSha256(receipt.source_plan_sha256) or !validSha256(receipt.manifest_sha256))
        return error.InvalidLifecycleReceipt;
}

fn validateActivationReceipt(receipt: ActivationReceipt) !void {
    try validateNormalized(normalizedActivation(receipt));
    if (receipt.cluster_id == 0 or receipt.timeline_id == 0 or receipt.epoch == 0 or
        receipt.manifest_id.len == 0 or receipt.backup_lsn == 0 or receipt.checkpoint_lsn < receipt.backup_lsn or
        !validSha256(receipt.seed_receipt_sha256) or !validSha256(receipt.capture_receipt_sha256) or
        !validSha256(receipt.manifest_sha256) or
        !validSha256(receipt.aggregate_sha256) or !validation.isNormalizedPath(receipt.generation_path))
        return error.InvalidLifecycleReceipt;
    if (receipt.format_version == 2 and
        (!validation.isNormalizedPath(receipt.raw_generation_path) or
            std.mem.eql(u8, receipt.raw_generation_path, receipt.generation_path) or
            !validSha256(receipt.materialized_receipt_sha256) or
            !validSha256(receipt.materialized_aggregate_sha256) or
            receipt.target_local_node_id == 0 or receipt.target_replica_id == 0))
        return error.InvalidLifecycleReceipt;
}

fn validateNormalized(receipt: NormalizedReceipt) !void {
    if (!validation.isIdentifier(receipt.generation) or !validation.isIdentifier(receipt.slot_name) or
        !validation.isIdentifier(receipt.topology_id) or receipt.topology_generation == 0 or
        !validation.isIdentifier(receipt.node_id) or !validation.isIdentifier(receipt.target_pvc_name) or
        !validation.isIdentifier(receipt.target_pvc_uid)) return error.InvalidLifecycleReceipt;
}

fn normalizedCapture(receipt: CaptureReceipt) NormalizedReceipt {
    return .{
        .generation = receipt.generation,
        .slot_name = receipt.slot_name,
        .topology_id = receipt.topology_id,
        .topology_generation = receipt.topology_generation,
        .node_id = receipt.node_id,
        .target_pvc_name = receipt.target_pvc_name,
        .target_pvc_uid = receipt.target_pvc_uid,
    };
}

fn normalizedActivation(receipt: ActivationReceipt) NormalizedReceipt {
    return .{
        .generation = receipt.generation,
        .slot_name = receipt.slot_name,
        .topology_id = receipt.topology_id,
        .topology_generation = receipt.topology_generation,
        .node_id = receipt.node_id,
        .target_pvc_name = receipt.target_pvc_name,
        .target_pvc_uid = receipt.target_pvc_uid,
    };
}

fn validateStoredEvent(alloc: Allocator, event: StoredEvent) !void {
    if (event.schema_version != event_schema_version or event.recorded_at_unix_ns == 0 or !validSha256(event.receipt_sha256))
        return error.CorruptLifecycleReceiptLedger;
    if (event.pod_uid) |pod_uid| if (!validation.isIdentifier(pod_uid)) return error.CorruptLifecycleReceiptLedger;
    var digest: [Sha256.digest_length]u8 = undefined;
    Sha256.hash(event.receipt_json, &digest, .{});
    var digest_hex: [Sha256.digest_length * 2]u8 = undefined;
    encodeHex(&digest_hex, &digest);
    if (!std.mem.eql(u8, event.receipt_sha256, &digest_hex)) return error.CorruptLifecycleReceiptLedger;
    switch (event.kind) {
        .capture => {
            var parsed = std.json.parseFromSlice(CaptureReceipt, alloc, event.receipt_json, .{ .ignore_unknown_fields = false }) catch
                return error.CorruptLifecycleReceiptLedger;
            defer parsed.deinit();
            validateCaptureReceipt(parsed.value) catch return error.CorruptLifecycleReceiptLedger;
            try validateStoredNormalized(event, normalizedCapture(parsed.value));
        },
        .activation => {
            var parsed = std.json.parseFromSlice(ActivationReceipt, alloc, event.receipt_json, .{ .ignore_unknown_fields = false }) catch
                return error.CorruptLifecycleReceiptLedger;
            defer parsed.deinit();
            if (parsed.value.format_version != 1 and parsed.value.format_version != 2)
                return error.CorruptLifecycleReceiptLedger;
            validateActivationReceipt(parsed.value) catch return error.CorruptLifecycleReceiptLedger;
            try validateStoredNormalized(event, normalizedActivation(parsed.value));
        },
    }
}

fn validateStoredNormalized(event: StoredEvent, normalized: NormalizedReceipt) !void {
    if (!std.mem.eql(u8, event.generation, normalized.generation) or
        !std.mem.eql(u8, event.slot_name, normalized.slot_name) or
        !std.mem.eql(u8, event.topology_id, normalized.topology_id) or
        event.topology_generation != normalized.topology_generation or
        !std.mem.eql(u8, event.node_id, normalized.node_id) or
        !std.mem.eql(u8, event.target_pvc_name, normalized.target_pvc_name) or
        !std.mem.eql(u8, event.target_pvc_uid, normalized.target_pvc_uid))
        return error.CorruptLifecycleReceiptLedger;
}

fn ownedEntry(alloc: Allocator, cursor: u64, event: StoredEvent, authoritative_root: []const u8) !Entry {
    var out = Entry{
        .cursor = cursor,
        .kind = event.kind,
        .generation = try alloc.dupe(u8, event.generation),
        .slot_name = undefined,
        .topology_id = undefined,
        .topology_generation = event.topology_generation,
        .node_id = undefined,
        .target_pvc_name = undefined,
        .target_pvc_uid = undefined,
        .receipt_sha256 = undefined,
        .receipt_json = undefined,
        .recorded_at_unix_ns = event.recorded_at_unix_ns,
        .pod_uid = null,
        .authoritative_state = undefined,
    };
    errdefer alloc.free(out.generation);
    out.slot_name = try alloc.dupe(u8, event.slot_name);
    errdefer alloc.free(out.slot_name);
    out.topology_id = try alloc.dupe(u8, event.topology_id);
    errdefer alloc.free(out.topology_id);
    out.node_id = try alloc.dupe(u8, event.node_id);
    errdefer alloc.free(out.node_id);
    out.target_pvc_name = try alloc.dupe(u8, event.target_pvc_name);
    errdefer alloc.free(out.target_pvc_name);
    out.target_pvc_uid = try alloc.dupe(u8, event.target_pvc_uid);
    errdefer alloc.free(out.target_pvc_uid);
    out.receipt_sha256 = try alloc.dupe(u8, event.receipt_sha256);
    errdefer alloc.free(out.receipt_sha256);
    out.receipt_json = try alloc.dupe(u8, event.receipt_json);
    errdefer alloc.free(out.receipt_json);
    if (event.pod_uid) |pod_uid| out.pod_uid = try alloc.dupe(u8, pod_uid);
    errdefer if (out.pod_uid) |value| alloc.free(value);
    out.authoritative_state = try authoritativeState(alloc, authoritative_root, event);
    return out;
}

fn authoritativeState(alloc: Allocator, root: []const u8, event: StoredEvent) !AuthoritativeState {
    const path = switch (event.kind) {
        .capture => try std.fs.path.join(alloc, &.{ root, "generations", event.generation, "COMPLETE.json" }),
        .activation => try std.fs.path.join(alloc, &.{ root, "generations", event.generation, ".antfly-ha-active-generation.json" }),
    };
    defer alloc.free(path);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const raw = std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(max_receipt_bytes)) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return .missing,
        else => return err,
    };
    defer alloc.free(raw);
    if (!std.mem.eql(u8, raw, event.receipt_json)) return error.AuthoritativeLifecycleReceiptMismatch;
    return .retained;
}

fn ownRuntime(alloc: Allocator, runtime: RuntimeObservation) !OwnedRuntimeObservation {
    const node_id = if (runtime.node_id) |value| try alloc.dupe(u8, value) else null;
    errdefer if (node_id) |value| alloc.free(value);
    const pod_uid = if (runtime.pod_uid) |value| try alloc.dupe(u8, value) else null;
    return .{
        .node_id = node_id,
        .role = runtime.role,
        .pod_uid = pod_uid,
        .fenced = runtime.fenced,
        .observed_at_unix_ns = if (runtime.observed_at_unix_ns != 0) runtime.observed_at_unix_ns else platform_time.realtimeNs(),
    };
}

fn identityKeyAlloc(alloc: Allocator, kind: Kind, receipt: NormalizedReceipt) ![]u8 {
    var sha = Sha256.init(.{});
    hashField(&sha, @tagName(kind));
    hashField(&sha, receipt.generation);
    hashField(&sha, receipt.slot_name);
    hashField(&sha, receipt.topology_id);
    sha.update(&std.mem.toBytes(std.mem.nativeToLittle(u64, receipt.topology_generation)));
    hashField(&sha, receipt.node_id);
    hashField(&sha, receipt.target_pvc_name);
    hashField(&sha, receipt.target_pvc_uid);
    var digest: [Sha256.digest_length]u8 = undefined;
    sha.final(&digest);
    const out = try alloc.alloc(u8, Sha256.digest_length * 2);
    encodeHex(out, &digest);
    return out;
}

fn hashField(sha: *Sha256, value: []const u8) void {
    sha.update(&std.mem.toBytes(std.mem.nativeToLittle(u64, @intCast(value.len))));
    sha.update(value);
}

fn validSha256(value: []const u8) bool {
    if (value.len != Sha256.digest_length * 2) return false;
    for (value) |byte| if (!std.ascii.isDigit(byte) and (byte < 'a' or byte > 'f')) return false;
    return true;
}

fn encodeHex(out: []u8, digest: *const [Sha256.digest_length]u8) void {
    std.debug.assert(out.len == digest.len * 2);
    for (digest, 0..) |byte, index| {
        out[index * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[index * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
}

fn freeWalEntries(alloc: Allocator, entries: []wal_mod.WalEntry) void {
    for (entries) |entry| alloc.free(@constCast(entry.data));
    alloc.free(entries);
}

fn lock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.Thread.yield() catch {};
}
