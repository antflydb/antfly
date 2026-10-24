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

//! Bounded immutable page mutations shared by publication metadata and row maps.
const std = @import("std");
const local = @import("antfly_local_sources");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const pages_mod = @import("../serverless/graph_segment/page_store.zig");
const A = std.mem.Allocator;
const Id = [32]u8;
const max_blob_bytes = 4 * 1024 * 1024;
const chunk_bytes = 16 * 1024;
pub const Mutations = struct {
    a: A,
    sort: local.sql_spill.Sort,
    forest: @import("lake_index_delta_runs.zig").Forest,
    destination: tree.Store,
    private_pages: pages_mod.PrivatePages,
    reduction: @FieldType(tree.Store, "reduce") = null,
    ordinal: u64 = 0,
    addition_first: bool = false,
    pub fn init(a: A, manager: *local.sql_spill.Manager, storage: tree.Store) Mutations {
        var sort = local.sql_spill.Sort.init(a, manager, &.{.{}}, 1024 * 1024);
        sort.shared_runs = true;
        sort.encoded_bytes = true;
        return .{ .a = a, .sort = sort, .forest = .{ .a = a, .store = storage, .policy = .latest }, .destination = storage, .private_pages = .{ .manager = manager, .domain = storage.domain, .attempt = storage.attempt, .prefix_keys = storage.prefix_keys } };
    }
    pub fn deinit(self: *Mutations) void {
        self.private_pages.deinit();
        self.sort.deinit();
    }
    pub fn add(self: *Mutations, id: Id, value: ?[]const u8, previous_length: usize) !void {
        return self.addPrefix(&id, value, previous_length);
    }
    pub fn addPrefix(self: *Mutations, prefix: []const u8, value: ?[]const u8, previous_length: usize) !void {
        const length = if (value) |bytes| bytes.len else previous_length;
        if (length == 0 or length > max_blob_bytes) return error.InvalidNativeLakeTextCorpus;
        var offset: usize = 0;
        while (offset < length) : (offset += chunk_bytes) {
            var buffer: [68]u8 = undefined;
            if (prefix.len > 64) return error.InvalidNativeLakeTextCorpus;
            @memcpy(buffer[0..prefix.len], prefix);
            std.mem.writeInt(u32, buffer[prefix.len..][0..4], @intCast(offset), .big);
            const k = buffer[0 .. prefix.len + 4];
            try self.sort.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = k })}, .values = &.{local.sql_scalar.Datum.json(if (value) |bytes| .{ .string = bytes[offset..@min(length, offset + chunk_bytes)] } else .null)}, .ordinal = self.ordinal });
            self.ordinal += 1;
            if (self.sort.total >= 1024) try self.flush();
        }
    }
    /// Raw authenticated page records share the same bounded private reducer;
    /// chunked blob consumers use addPrefix without changing their wire keys.
    pub fn addRecord(self: *Mutations, key: []const u8, value: ?[]const u8) !void {
        if (key.len == 0 or key.len > tree.max_key_bytes or (if (value) |bytes| bytes.len > tree.max_record_bytes else false)) return error.InvalidGraphPage;
        try self.sort.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = key })}, .values = &.{local.sql_scalar.Datum.json(if (value) |bytes| .{ .string = bytes } else .null)}, .ordinal = self.ordinal });
        self.ordinal += 1;
        if (self.sort.total >= 1024) try self.flush();
    }
    pub fn flush(self: *Mutations) !void {
        if (self.sort.total == 0) return;
        try self.sort.finish();
        const Source = struct {
            sort: *local.sql_spill.Sort,
            scratch: std.heap.ArenaAllocator,
            owned: std.heap.ArenaAllocator,
            addition_first: bool,
            pending: ?local.sql_operators.Row = null,
            pub fn next(self_: *@This()) !?tree.Cursor.Record {
                _ = self_.owned.reset(.retain_capacity);
                const a = self_.owned.allocator();
                if (self_.pending == null) self_.pending = try self_.sort.next(self_.scratch.allocator());
                const row = self_.pending orelse return null;
                const k = try a.dupe(u8, row.keys[0].value.string);
                var encoded: []const u8 = "";
                while (self_.pending) |next_| {
                    if (!std.mem.eql(u8, k, next_.keys[0].value.string)) break;
                    const value = next_.values[0];
                    if (!self_.addition_first or encoded.len == 0 or encoded[0] == 0 or (!value.sql_null and value.value != .null))
                        encoded = if (value.sql_null or value.value == .null) &.{0} else try std.mem.concat(a, u8, &.{ &.{1}, value.value.string });
                    _ = self_.scratch.reset(.retain_capacity);
                    self_.pending = try self_.sort.next(self_.scratch.allocator());
                }
                return .{ .key = k, .value = encoded };
            }
        };
        var source: Source = .{ .sort = &self.sort, .scratch = .init(self.a), .owned = .init(self.a), .addition_first = self.addition_first };
        defer source.scratch.deinit();
        defer source.owned.deinit();
        self.forest.runs = .{ .ptr = &self.private_pages, .begin = pages_mod.PrivatePages.beginRun, .retain = pages_mod.PrivatePages.retain };
        try self.forest.add(&source);
        const manager = self.sort.manager;
        self.sort.deinit();
        self.sort = local.sql_spill.Sort.init(self.a, manager, &.{.{}}, 1024 * 1024);
        self.sort.shared_runs = true;
        self.sort.encoded_bytes = true;
    }
    pub fn finish(self: *Mutations, prior: ?tree.Ref) !?tree.Ref {
        try self.flush();
        const changes = try self.forest.finish() orelse return prior;
        var storage = self.destination;
        if (self.reduction) |reduction| storage.reduce = reduction;
        var delta = try tree.Cursor.init(self.a, self.forest.store, changes, "", null);
        defer delta.deinit();
        const Source = struct {
            cursor: *tree.Cursor,
            pub fn next(self_: *@This()) !?tree.Mutation {
                return if (try self_.cursor.next()) |record| try decode(record) else null;
            }
        };
        var source: Source = .{ .cursor = &delta };
        return tree.applySorted(self.a, storage, prior, &source);
    }

    fn decode(record: tree.Cursor.Record) !tree.Mutation {
        if (record.value.len == 0 or record.value[0] > 1 or (record.value[0] == 0 and record.value.len != 1)) return error.InvalidNativeLakeTextCorpus;
        return .{ .key = record.key, .value = if (record.value[0] == 0) null else record.value[1..] };
    }
};
