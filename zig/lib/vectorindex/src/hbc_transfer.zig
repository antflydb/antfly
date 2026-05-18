// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const Allocator = std.mem.Allocator;
const hbc_runtime = @import("hbc_runtime.zig");

pub fn ingestMembersFrom(self: anytype, src: anytype, member_ids: []const u64, batch_size: usize) !void {
    if (member_ids.len == 0) return;

    var src_txn = try src.beginRuntimeReadTxn();
    defer src_txn.abort();

    const dims: usize = @intCast(src.config.dims);
    const effective_batch = @max(@as(usize, 1), batch_size);
    var items = try self.alloc.alloc(hbc_runtime.BatchInsertItem, effective_batch);
    defer self.alloc.free(items);
    const vectors = try self.alloc.alloc(f32, effective_batch * dims);
    defer self.alloc.free(vectors);
    const transformed_vectors = try self.alloc.alloc(f32, effective_batch * dims);
    defer self.alloc.free(transformed_vectors);
    const scratch = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(scratch);

    var cursor: usize = 0;
    while (cursor < member_ids.len) {
        const batch_end = @min(cursor + effective_batch, member_ids.len);
        var item_count: usize = 0;
        for (member_ids[cursor..batch_end], 0..) |member_id, batch_idx| {
            const raw = try src.getVectorInto(&src_txn, member_id, scratch);
            const vector_slot = vectors[batch_idx * dims ..][0..dims];
            @memcpy(vector_slot, raw);
            const transformed_slot = transformed_vectors[batch_idx * dims ..][0..dims];
            _ = src.transformVector(raw, transformed_slot);
            items[item_count] = .{
                .vector_id = member_id,
                .vector = vector_slot,
                .transformed = transformed_slot,
                .metadata = (try src.getMetadataInTxn(&src_txn, member_id)) orelse "",
            };
            item_count += 1;
        }
        try self.batchInsertWithMetadata(items[0..item_count]);
        cursor = batch_end;
    }
}

pub fn bulkBuildMembersFrom(self: anytype, src: anytype, member_ids: []const u64) !void {
    if (member_ids.len == 0) return;
    const empty_metadata: []const u8 = &.{};

    var src_txn = try src.beginRuntimeReadTxn();
    defer src_txn.abort();

    const dims: usize = @intCast(src.config.dims);
    const items = try self.alloc.alloc(hbc_runtime.BatchInsertItem, member_ids.len);
    defer self.alloc.free(items);
    const vectors = try self.alloc.alloc(f32, member_ids.len * dims);
    defer self.alloc.free(vectors);
    const transformed = try self.alloc.alloc(f32, member_ids.len * dims);
    defer self.alloc.free(transformed);
    const scratch = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(scratch);

    var total_metadata_len: usize = 0;
    for (member_ids) |member_id| {
        total_metadata_len += ((try src.getMetadataInTxn(&src_txn, member_id)) orelse empty_metadata).len;
    }
    const metadata_storage = try self.alloc.alloc(u8, total_metadata_len);
    defer self.alloc.free(metadata_storage);

    var metadata_cursor: usize = 0;
    for (member_ids, 0..) |member_id, i| {
        const raw = try src.getVectorInto(&src_txn, member_id, scratch);
        const vector_slot = vectors[i * dims ..][0..dims];
        @memcpy(vector_slot, raw);
        const transformed_slot = transformed[i * dims ..][0..dims];
        _ = src.transformVector(raw, transformed_slot);

        const metadata = (try src.getMetadataInTxn(&src_txn, member_id)) orelse empty_metadata;
        const metadata_slot = metadata_storage[metadata_cursor..][0..metadata.len];
        @memcpy(metadata_slot, metadata);
        metadata_cursor += metadata.len;

        items[i] = .{
            .vector_id = member_id,
            .vector = vector_slot,
            .transformed = transformed_slot,
            .metadata = metadata_slot,
        };
    }

    try self.bulkBuildWithMetadata(items);
}

pub fn streamSplitMembers(
    self: anytype,
    split_key: []const u8,
    batch_size: usize,
    ctx: anytype,
    comptime consume: fn (@TypeOf(ctx), []const hbc_runtime.BatchInsertItem) anyerror!void,
) !usize {
    var plan = try self.buildSplitReusePlan(split_key);
    defer plan.deinit(self.alloc);

    var src_txn = try self.beginRuntimeReadTxn();
    defer src_txn.abort();

    const dims: usize = @intCast(self.config.dims);
    const effective_batch = @max(@as(usize, 1), batch_size);
    var items = try self.alloc.alloc(hbc_runtime.BatchInsertItem, effective_batch);
    defer self.alloc.free(items);
    const vectors = try self.alloc.alloc(f32, effective_batch * dims);
    defer self.alloc.free(vectors);
    const transformed_vectors = try self.alloc.alloc(f32, effective_batch * dims);
    defer self.alloc.free(transformed_vectors);
    const scratch = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(scratch);

    var item_count: usize = 0;
    var transferred: usize = 0;

    for (plan.right_only_roots) |node_id| {
        try streamSubtreeMembers(self, &src_txn, node_id, scratch, vectors, transformed_vectors, items, &item_count, &transferred, ctx, consume);
    }
    for (plan.mixed_leaves) |node_id| {
        try streamMixedLeafRightMembers(self, &src_txn, node_id, split_key, scratch, vectors, transformed_vectors, items, &item_count, &transferred, ctx, consume);
    }

    if (item_count > 0) {
        try consume(ctx, items[0..item_count]);
        transferred += item_count;
    }

    return transferred;
}

fn streamSubtreeMembers(
    self: anytype,
    txn: anytype,
    node_id: u64,
    scratch: []f32,
    vectors: []f32,
    transformed_vectors: []f32,
    items: []hbc_runtime.BatchInsertItem,
    item_count: *usize,
    transferred: *usize,
    ctx: anytype,
    comptime consume: fn (@TypeOf(ctx), []const hbc_runtime.BatchInsertItem) anyerror!void,
) !void {
    var node = try self.loadNode(txn, node_id);
    defer node.deinit(self.alloc);
    if (node.is_leaf) {
        for (node.members) |member_id| {
            try appendSplitBatchItem(self, txn, member_id, scratch, vectors, transformed_vectors, items, item_count, transferred, ctx, consume);
        }
        return;
    }
    for (node.children) |child_id| {
        try streamSubtreeMembers(self, txn, child_id, scratch, vectors, transformed_vectors, items, item_count, transferred, ctx, consume);
    }
}

fn streamMixedLeafRightMembers(
    self: anytype,
    txn: anytype,
    node_id: u64,
    split_key: []const u8,
    scratch: []f32,
    vectors: []f32,
    transformed_vectors: []f32,
    items: []hbc_runtime.BatchInsertItem,
    item_count: *usize,
    transferred: *usize,
    ctx: anytype,
    comptime consume: fn (@TypeOf(ctx), []const hbc_runtime.BatchInsertItem) anyerror!void,
) !void {
    var node = try self.loadNode(txn, node_id);
    defer node.deinit(self.alloc);
    if (!node.is_leaf) return error.ExpectedLeaf;

    for (node.members) |member_id| {
        const metadata = (try self.loadMetadataRaw(txn, member_id)) orelse continue;
        if (std.mem.order(u8, metadata, split_key) != .lt) {
            try appendSplitBatchItem(self, txn, member_id, scratch, vectors, transformed_vectors, items, item_count, transferred, ctx, consume);
        }
    }
}

fn appendSplitBatchItem(
    self: anytype,
    txn: anytype,
    member_id: u64,
    scratch: []f32,
    vectors: []f32,
    transformed_vectors: []f32,
    items: []hbc_runtime.BatchInsertItem,
    item_count: *usize,
    transferred: *usize,
    ctx: anytype,
    comptime consume: fn (@TypeOf(ctx), []const hbc_runtime.BatchInsertItem) anyerror!void,
) !void {
    const metadata = (try self.getMetadataInTxn(txn, member_id)) orelse return;
    const dims: usize = @intCast(self.config.dims);
    const raw = try self.getVectorInto(txn, member_id, scratch);
    const vector_slot = vectors[item_count.* * dims ..][0..dims];
    @memcpy(vector_slot, raw);
    const transformed_slot = transformed_vectors[item_count.* * dims ..][0..dims];
    _ = self.transformVector(raw, transformed_slot);
    items[item_count.*] = .{
        .vector_id = member_id,
        .vector = vector_slot,
        .transformed = transformed_slot,
        .metadata = metadata,
    };
    item_count.* += 1;

    if (item_count.* == items.len) {
        try consume(ctx, items[0..item_count.*]);
        transferred.* += item_count.*;
        item_count.* = 0;
    }
}
