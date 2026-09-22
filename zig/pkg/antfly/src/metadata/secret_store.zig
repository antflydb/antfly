// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Encrypted, scope-CAS metadata Raft persistence. Key providers run only in
//! preparation/read paths. Deterministic apply uses ciphertext framing alone.
const std = @import("std");
const collection = @import("../common/secret_collection.zig");
const contract = @import("../common/secret_contract.zig");
const service_mod = @import("service.zig");
const time = @import("antfly_platform").time;

pub const Store = collection.Store(Backend);
pub const max_command_bytes = collection.max_bytes + 8;

pub const Publication = struct { expected_revision: u64, scope: []const u8, bytes: []const u8 };

pub fn decodePublication(alloc: std.mem.Allocator, command: []const u8) !Publication {
    if (command.len < 28 or command.len > max_command_bytes) return error.CorruptInput;
    const bytes = command[8..];
    const scope = try collection.storedScope(bytes);
    var view = try collection.decode(alloc, scope, bytes);
    defer view.deinit(alloc);
    const expected = std.mem.readInt(u64, command[0..8], .little);
    if (expected == std.math.maxInt(u64) or view.revision != expected + 1) return error.CorruptInput;
    return .{ .expected_revision = expected, .scope = scope, .bytes = bytes };
}

pub fn encodePublication(alloc: std.mem.Allocator, expected_revision: u64, bytes: []const u8) ![]u8 {
    if (bytes.len > collection.max_bytes) return error.InvalidArgument;
    const out = try alloc.alloc(u8, 8 + bytes.len);
    errdefer alloc.free(out);
    std.mem.writeInt(u64, out[0..8], expected_revision, .little);
    @memcpy(out[8..], bytes);
    _ = try decodePublication(alloc, out);
    return out;
}

pub fn prefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:native_secrets_v1:{d}:", .{group_id});
}

pub fn keyForScope(buf: []u8, group_id: u64, scope: []const u8) ![]const u8 {
    try contract.validateName(scope);
    const prefix = try prefixForGroup(buf, group_id);
    const digest = collection.scopeDigest(scope);
    if (buf.len - prefix.len < digest.len) return error.NoSpaceLeft;
    @memcpy(buf[prefix.len..][0..digest.len], &digest);
    return buf[0 .. prefix.len + digest.len];
}

pub const Backend = struct {
    service: *service_mod.MetadataHttpService,

    pub fn read(self: *Backend, alloc: std.mem.Allocator, scope: []const u8) !collection.Snapshot {
        try self.service.ensureLinearizableRead();
        const store = self.service.projectedStore() orelse return error.Unavailable;
        return .{ .bytes = try store.getSecretCollection(alloc, self.service.metadata_group_id, scope) };
    }

    pub fn publish(self: *Backend, alloc: std.mem.Allocator, scope: []const u8, previous: collection.Snapshot, bytes: []const u8) !void {
        var before = try collection.decode(alloc, scope, previous.bytes);
        defer before.deinit(alloc);
        const encoded = try encodePublication(alloc, before.revision, bytes);
        defer alloc.free(encoded);
        self.service.lockCatalogMutation();
        defer self.service.unlockCatalogMutation();
        try self.service.ensureLinearizableRead();
        const store = self.service.projectedStore() orelse return error.Unavailable;
        const current = try store.getSecretCollection(alloc, self.service.metadata_group_id, scope);
        defer if (current) |b| alloc.free(b);
        var view = try collection.decode(alloc, scope, current);
        defer view.deinit(alloc);
        if (view.revision != before.revision) return error.Conflict;
        // Raft forwards this ciphertext CAS from followers to the current
        // leader. Do not require a local leader receipt or replay an ambiguous
        // proposal. AFSC's random publication ID also distinguishes deletes.
        self.service.proposeTransitionCommand(.{ .publish_secret_collection = encoded }) catch return error.OutcomeUnknown;
        const deadline = time.monotonicNs() +| 5 * std.time.ns_per_s;
        while (time.monotonicNs() < deadline) {
            self.service.ensureLinearizableReadWithContext(.{ .deadline_ns = deadline }) catch return error.OutcomeUnknown;
            const applied = store.getSecretCollection(alloc, self.service.metadata_group_id, scope) catch return error.OutcomeUnknown;
            defer if (applied) |b| alloc.free(b);
            if (applied) |b| {
                if (std.mem.eql(u8, b, bytes)) return;
            }
            var observed = collection.decode(alloc, scope, applied) catch return error.OutcomeUnknown;
            defer observed.deinit(alloc);
            // At exactly the proposed revision, a different publication proves
            // this CAS lost. A later revision could have overwritten our commit.
            if (observed.revision == before.revision + 1) return error.Conflict;
            if (observed.revision > before.revision + 1) return error.OutcomeUnknown;
            time.sleepNs(std.time.ns_per_ms);
        }
        return error.OutcomeUnknown;
    }
};
