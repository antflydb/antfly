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

//! Physical delayed-document reader. The storage owner retains its snapshot,
//! schema, policy, bounded range buffers and metadata directories.
const std = @import("std");
const Physical = @import("read_session_source_types").types;
const Values = @import("read_session_types").types.document_values;
const Window = @import("../read_session_source.zig").DocumentPrimaryReadWindow;
pub fn For(comptime DB: type) type {
    return struct {
        db: *DB,
        read: Physical.store.DocStore.Txn,
        schema: Physical.schema_registry.SchemaView,
        policy: Physical.row_policy_gate.Gate.Lease,
        window: Window,
        metadata: std.heap.ArenaAllocator,
        keys: [64][]const u8 = undefined,
        ids: [64][]const u8 = undefined,
        sizes: [64]?u64 = undefined,
        proofs: [64]?Physical.document_revision.Content = undefined,
        cached: usize = 0,
        next_cached: usize = 0,
        repair_requested: bool = false,
        directory_arena: std.heap.ArenaAllocator,
        directory_scope: ?Physical.backend_erased.ReadScope = null,
        directories: [64]?[]const u8 = @splat(null),
        directories_loaded: [64]bool = @splat(false),
        values: [64]?[]const u8 = undefined,
        sources: [64]Source = undefined,
        pub const Batch = Values.Batch;
        pub const Source = Values.Source;
        pub const ProjectedBatch = Values.ProjectedBatch;

        fn retireDirectories(self: *@This()) void {
            if (self.directory_scope) |*scope| scope.close();
            self.directory_scope = null;
            _ = self.directory_arena.reset(.free_all);
            @memset(&self.directories_loaded, false);
            @memset(&self.directories, null);
        }

        // Keep keys and scalar size proofs separate from body scratch: sorted
        // reads retire body scratch, and a small byte target can consume this
        // metadata window over several calls. Never reuse proofs across pages.
        fn prepare(self: *@This(), documents: []const []const u8) !void {
            const available = self.cached - self.next_cached;
            const common = @min(available, documents.len);
            if (common != 0) {
                const matches = for (documents[0..common], self.ids[self.next_cached..][0..common]) |wanted, cached_id| {
                    if (!std.mem.eql(u8, wanted, cached_id)) break false;
                } else true;
                if (matches) return;
            }
            self.cached = 0;
            self.next_cached = 0;
            self.retireDirectories();
            _ = self.metadata.reset(.free_all);
            const a = self.metadata.allocator();
            const count = @min(documents.len, self.keys.len);
            for (documents[0..count], 0..) |id, index| {
                self.ids[index] = try a.dupe(u8, id);
                self.keys[index] = try self.db.queryDocumentKeyOwned(a, id, self.schema);
            }
            var contents: [64]?Physical.document_revision.Content = undefined;
            var timestamps: [64]u64 = undefined;
            var scope = try self.read.openReadScope(a);
            defer scope.close();
            try Physical.document_revision.loadWindow(&scope, a, self.keys[0..count], self.ids[0..count], contents[0..count], timestamps[0..count]);
            for (contents[0..count], 0..) |content, index| self.sizes[index] = if (content) |proof| proof.bytes else null;
            @memcpy(self.proofs[0..count], contents[0..count]);
            self.cached = count;
        }

        /// Sorted physical reads restore caller order. Metadata lengths are
        /// revision-bound to this snapshot; unknown or oversized rows read in
        /// isolation. A legal single document may exceed the soft byte target.
        /// Results expire on the next lookup, batch or page release.
        pub fn lookupBatch(self: *@This(), documents: []const []const u8, max_bytes: usize) !Batch {
            const hydrated = try self.lookupBatchImpl(documents, &.{}, max_bytes);
            return .{ .values = self.values[0..hydrated.consumed], .consumed = hydrated.consumed };
        }
        pub fn lookupProjectedBatch(self: *@This(), documents: []const []const u8, fields: []const []const u8, max_bytes: usize) !ProjectedBatch {
            return self.lookupBatchImpl(documents, fields, max_bytes);
        }
        fn lookupBatchImpl(self: *@This(), documents: []const []const u8, fields: []const []const u8, max_bytes: usize) !ProjectedBatch {
            if (documents.len == 0 or documents.len > 64 or max_bytes == 0) return error.InvalidBatch;
            self.window.retire();
            errdefer self.window.retire();
            if (documents.len == 1 and fields.len == 0) {
                self.values[0] = try self.lookup(documents[0]);
                self.sources[0] = .{ .bytes = self.values[0].? };
                return .{ .values = self.sources[0..1], .consumed = 1 };
            }
            try self.prepare(documents);
            if (fields.len != 0) if (try self.projectBatch(documents.len, fields, max_bytes)) |projected| return projected;
            // A declined projection must not retain its metadata/range buffers
            // beside the full body. The proof cache has a separate owner.
            self.window.retire();
            const available = @min(documents.len, self.cached - self.next_cached);
            const limit = @min(max_bytes, Window.max_bytes);
            var count: usize = 0;
            var bytes: u64 = 0;
            while (count < available) : (count += 1) {
                const length = self.sizes[self.next_cached + count] orelse break;
                if (count != 0 and fields.len != 0 and length >= Physical.document_projection.minimum_bytes) break;
                if (length > limit -| bytes) break;
                bytes += length;
            }
            count = @max(count, 1);
            const keys = self.keys[self.next_cached..][0..count];
            if (count == 1) {
                self.values[0] = self.window.get(&self.read, keys[0], self.sizes[self.next_cached]) catch |err| switch (err) {
                    error.NotFound => return error.StorageReadTemporarilyUnavailable,
                    else => return err,
                };
            } else try self.window.getMany(&self.read, keys, self.values[0..count]);
            for (self.values[0..count]) |value| if (value == null) return error.StorageReadTemporarilyUnavailable;
            self.next_cached += count;
            for (self.sources[0..count], self.values[0..count]) |*source, value| source.* = .{ .bytes = value.? };
            return .{ .values = self.sources[0..count], .consumed = count };
        }

        fn requestRepair(self: *@This()) void {
            if (self.repair_requested) return;
            self.repair_requested = self.db.queryDocumentRequestRepairOwned();
        }
        fn projectBatch(self: *@This(), maximum: usize, fields: []const []const u8, max_bytes: usize) !?ProjectedBatch {
            const projection = Physical.document_projection;
            const revision = Physical.document_revision;
            const ranges_mod = Physical.value_ranges;
            const available = @min(maximum, self.cached - self.next_cached);
            const limit = @min(max_bytes, Window.max_bytes);
            const a = self.window.arena.allocator();
            self.window.scope = try self.read.openReadScope(a);
            const scope = &self.window.scope.?;
            // Directory metadata is separately bounded by 64 * 32 KiB. Only
            // projected payload cohorts enter the caller's soft byte window.
            var directory_keys: [64]projection.Key = undefined;
            var keys: [64][]const u8 = undefined;
            var pending: [64]usize = undefined;
            var pending_count: usize = 0;
            var fetched: [64]?[]const u8 = @splat(null);
            var candidates: usize = 0;
            for (0..available) |i| {
                const proof = self.proofs[self.next_cached + i] orelse {
                    self.requestRepair();
                    break;
                };
                if (proof.bytes < projection.minimum_bytes) break;
                if (!self.directories_loaded[self.next_cached + i]) {
                    directory_keys[pending_count] = projection.keyForRevision(revision.keyForPhysical(self.keys[self.next_cached + i]).?);
                    keys[pending_count] = &directory_keys[pending_count];
                    pending[pending_count] = self.next_cached + i;
                    pending_count += 1;
                }
                candidates += 1;
            }
            if (candidates == 0) return null;
            if (pending_count != 0) {
                // Keep this bounded metadata lease across payload windows. A
                // one-row byte limit must not re-read all remaining directories.
                errdefer self.retireDirectories();
                if (self.directory_scope == null) self.directory_scope = try self.read.openReadScope(self.directory_arena.allocator());
                _ = try Window.readMany(&self.directory_scope.?, keys[0..pending_count], fetched[0..pending_count]);
                for (pending[0..pending_count], fetched[0..pending_count]) |index, raw| {
                    self.directories[index] = raw;
                    self.directories_loaded[index] = true;
                }
            }
            const Prepared = struct { fields: []projection.FieldValue, ranges: []ranges_mod.Range, values: [][]const u8, hashes: [][32]u8, duplicate: bool = false };
            var prepared: [64]Prepared = undefined;
            var requests: [64]ranges_mod.Request = undefined;
            var request_count: usize = 0;
            var count: usize = 0;
            var retained: usize = 0;
            while (count < candidates) : (count += 1) {
                const physical = self.keys[self.next_cached + count];
                const duplicate = for (0..count) |prior| {
                    if (std.mem.eql(u8, physical, self.keys[self.next_cached + prior])) break prior;
                } else null;
                if (duplicate) |prior| {
                    prepared[count] = prepared[prior];
                    prepared[count].duplicate = true;
                    continue;
                }
                const proof = self.proofs[self.next_cached + count].?;
                const raw = self.directories[self.next_cached + count] orelse {
                    self.requestRepair();
                    break;
                };
                const directory = (try projection.Directory.open(a, raw, proof)) orelse {
                    if (!try projection.isCurrent(a, raw, proof)) self.requestRepair();
                    break;
                };
                var selected_bytes: u64 = 0;
                var selected_count: usize = 0;
                for (fields) |name| if (directory.find(name)) |field| {
                    selected_bytes +|= field.length;
                    selected_count += 1;
                };
                if (selected_bytes >= proof.bytes / 2 or selected_count > ranges_mod.maximum_ranges) break;
                const ranges = try a.alloc(ranges_mod.Range, selected_count);
                var slot: usize = 0;
                for (fields) |name| if (directory.find(name)) |field| {
                    ranges[slot] = .{ .offset = field.offset, .length = std.math.cast(usize, field.length) orelse return error.InvalidDocumentContentAuthority };
                    slot += 1;
                };
                const cost = if (scope.supportsRange()) try ranges_mod.retainedBytes(std.math.cast(usize, proof.bytes) orelse return error.InvalidDocumentContentAuthority, ranges) else if (selected_count == 0) 0 else proof.bytes;
                if (count != 0 and cost > limit -| retained) break;
                retained +|= std.math.cast(usize, cost) orelse return error.InvalidDocumentContentAuthority;
                const output = try a.alloc(projection.FieldValue, selected_count);
                const values = try a.alloc([]const u8, selected_count);
                const hashes = try a.alloc([32]u8, selected_count);
                slot = 0;
                for (fields) |name| if (directory.find(name)) |field| {
                    output[slot] = .{ .name = field.name, .value = undefined };
                    hashes[slot] = field.digest;
                    slot += 1;
                };
                prepared[count] = .{ .fields = output, .ranges = ranges, .values = values, .hashes = hashes };
                if (selected_count != 0) {
                    requests[request_count] = .{ .key = physical, .ranges = ranges, .output = values };
                    request_count += 1;
                }
            }
            if (count == 0) return null;
            std.mem.sort(ranges_mod.Request, requests[0..request_count], {}, struct {
                fn less(_: void, left: ranges_mod.Request, right: ranges_mod.Request) bool {
                    return std.mem.order(u8, left.key, right.key) == .lt;
                }
            }.less);
            scope.getRangesMany(requests[0..request_count]) catch |err| return if (err == error.NotFound) error.StorageReadTemporarilyUnavailable else err;
            for (prepared[0..count], self.sources[0..count]) |item, *source| {
                if (!item.duplicate) for (item.values, item.hashes, item.fields) |value, hash, *field| {
                    var digest: [32]u8 = undefined;
                    std.crypto.hash.sha2.Sha256.hash(value, &digest, .{});
                    if (!std.mem.eql(u8, &digest, &hash)) return error.InvalidDocumentContentAuthority;
                    field.value = value;
                };
                source.* = .{ .fields = item.fields };
            }
            self.window.bytes = retained;
            self.next_cached += count;
            return .{ .values = self.sources[0..count], .consumed = count };
        }

        pub fn lookup(self: *@This(), key: []const u8) ![]const u8 {
            self.window.retire();
            const a = self.window.arena.allocator();
            const physical = try self.db.queryDocumentKeyOwned(a, key, self.schema);
            // Ranking already applied visibility/TTL at the captured cut.
            // Never fall back to a fresh read if the captured row is absent.
            return self.window.get(&self.read, physical, 0) catch |err| switch (err) {
                error.NotFound => error.StorageReadTemporarilyUnavailable,
                else => err,
            };
        }
        /// Page cells already own their selected payloads. Retire the last
        /// source scope before a portal suspends, keeping only the snapshot.
        pub fn releasePage(self: *@This()) void {
            self.window.retire();
            self.retireDirectories();
            _ = self.metadata.reset(.free_all);
            self.cached = 0;
            self.next_cached = 0;
        }
        pub fn deinit(self: *@This()) void {
            self.window.deinit();
            self.retireDirectories();
            self.directory_arena.deinit();
            self.metadata.deinit();
            self.read.abort();
            self.schema.release();
            self.policy.release();
            self.* = undefined;
        }
    };
}
