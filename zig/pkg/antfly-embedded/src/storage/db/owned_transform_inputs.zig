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

//! Owned transform bases and semantic validation. Only bounded capture borrows
//! a Store; resolution and retained plans contain no transaction or runtime.
const std = @import("std");
const A = std.mem.Allocator;
const effects = @import("mutation_effects.zig");
const keys = @import("../internal_keys.zig");
const revisions = @import("../document_mutation_revision.zig");
const docstore = @import("../docstore.zig");

pub fn requiredBaseKeys(a: A, request: *const effects.OwnedRequest) ![][]const u8 {
    var established: std.StringHashMapUnmanaged(void) = .empty;
    defer established.deinit(a);
    var result: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer result.deinit(a);
    for (request.writes) |write| try established.put(a, write.key, {});
    for (request.deletes) |key| try established.put(a, key, {});
    for (request.transforms) |transform| {
        const entry = try established.getOrPut(a, transform.key);
        if (entry.found_existing) continue;
        try result.append(a, transform.key);
    }
    return result.toOwnedSlice(a);
}

const Witness = struct {
    physical_key: []u8,
    timestamp_key: []u8,
    revision_key: revisions.Key,
    revision: u64,
    version: u64,
    present: bool,
    bytes: usize,
    digest: [32]u8 = undefined,

    fn deinit(self: *Witness, a: A) void {
        a.free(self.physical_key);
        a.free(self.timestamp_key);
    }

    fn timestamp(txn: anytype, key: []const u8) !u64 {
        const raw = txn.get(key) catch |err| {
            if (err == error.NotFound) return 0;
            return err;
        };
        if (raw.len < 8) return error.InvalidTimestamp;
        return std.mem.readInt(u64, raw[0..8], .little);
    }

    /// A changed artifact/TTL revision is not proof that the primary body
    /// changed. Qualified content authority lets us compare its exact bytes
    /// without reading/hashing that body under writer admission. Legacy rows
    /// fall back to a direct value comparison at the current transaction cut.
    fn validate(self: *const Witness, txn: anytype) !void {
        if (try timestamp(txn, self.timestamp_key) != self.version) return error.OwnedPreparationStale;
        if (try revisions.load(txn, self.revision_key) == self.revision) return;
        if (self.present) {
            if (try revisions.loadContent(txn, self.physical_key)) |content| {
                if (content.bytes == self.bytes and content.kind == keys.primary_kind and std.mem.eql(u8, &content.digest, &self.digest)) return;
                return error.OwnedPreparationStale;
            }
        }
        const current = txn.get(self.physical_key) catch |err| {
            if (err == error.NotFound) {
                if (!self.present) return;
                return error.OwnedPreparationStale;
            }
            return err;
        };
        if (!self.present or current.len != self.bytes) return error.OwnedPreparationStale;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(current, &digest, .{});
        if (!std.mem.eql(u8, &digest, &self.digest)) return error.OwnedPreparationStale;
    }
};

pub const Inputs = struct {
    allocator: A,
    snapshot: effects.TransformReadSnapshot,
    witnesses: []Witness,
    sealed: bool = false,

    pub fn empty(a: A) Inputs {
        return .{ .allocator = a, .snapshot = .{}, .witnesses = &.{} };
    }

    /// Keys borrow OwnedRequest, which outlives this object. Values, physical
    /// lookup keys and metadata witnesses are owned. No read pin escapes.
    pub fn capture(a: A, store: *docstore.DocStore, base_keys: []const []const u8, maximum_bytes: usize) !Inputs {
        if (base_keys.len == 0) return empty(a);
        var snapshot: effects.TransformReadSnapshot = .{};
        errdefer snapshot.deinit(a);
        try snapshot.positions.ensureTotalCapacity(a, std.math.cast(u32, base_keys.len) orelse return error.InvalidBatchRequest);
        const entries = try a.alloc(effects.TransformReadSnapshot.Entry, base_keys.len);
        errdefer a.free(entries);
        const witnesses = try a.alloc(Witness, base_keys.len);
        errdefer a.free(witnesses);
        var initialized: usize = 0;
        errdefer for (0..initialized) |i| {
            if (entries[i].value) |value| a.free(value);
            witnesses[i].deinit(a);
        };
        if (store.portableImportPublicationInProgress()) return error.PortableImportPublicationInProgress;
        var read = try store.beginReadTxn();
        defer read.abort();
        var total: usize = 0;
        for (base_keys, 0..) |key, i| {
            if (keys.isInternalUserKey(key) or (key.len != 0 and key[0] == 0)) return error.Unsupported;
            const physical = try keys.documentKeyAlloc(a, key);
            errdefer a.free(physical);
            const timestamp_key = try keys.ttlKeyAlloc(a, key);
            errdefer a.free(timestamp_key);
            const revision_key = revisions.keyForPhysical(physical) orelse return error.Unsupported;
            if (try revisions.loadContent(&read, physical)) |content| {
                if (content.bytes > maximum_bytes -| total) return error.OwnedPreparationTooLarge;
            }
            const raw = read.get(physical) catch |err| blk: {
                if (err == error.NotFound) break :blk null;
                return err;
            };
            const length = if (raw) |value| value.len else 0;
            if (length > maximum_bytes -| total) return error.OwnedPreparationTooLarge;
            total += length;
            const body = if (raw) |value| try a.dupe(u8, value) else null;
            errdefer if (body) |value| a.free(value);
            const version = try Witness.timestamp(&read, timestamp_key);
            const revision = try revisions.load(&read, revision_key);
            entries[i] = .{ .key = key, .value = body, .expected_version = version };
            witnesses[i] = .{ .physical_key = physical, .timestamp_key = timestamp_key, .revision_key = revision_key, .revision = revision, .version = version, .present = body != null, .bytes = length };
            initialized += 1;
            snapshot.positions.putAssumeCapacity(key, i);
        }
        snapshot.entries = entries;
        return .{ .allocator = a, .snapshot = snapshot, .witnesses = witnesses };
    }

    /// Hash the independently copied bases after leaving capture admission.
    pub fn seal(self: *Inputs) void {
        if (self.sealed) return;
        for (self.snapshot.entries, self.witnesses) |entry, *witness| {
            if (entry.value) |body| std.crypto.hash.sha2.Sha256.hash(body, &witness.digest, .{});
        }
        self.sealed = true;
    }

    pub fn validate(self: *const Inputs, store: *docstore.DocStore) !void {
        if (!self.sealed) return error.InvalidDerivedApplyState;
        if (self.witnesses.len == 0) return;
        var read = try store.beginProbeTxn();
        defer read.abort();
        for (self.witnesses) |*witness| try witness.validate(&read);
    }

    pub fn deinit(self: *Inputs) void {
        for (self.witnesses) |*witness| witness.deinit(self.allocator);
        self.allocator.free(self.witnesses);
        self.snapshot.deinit(self.allocator);
        self.* = undefined;
    }
};
