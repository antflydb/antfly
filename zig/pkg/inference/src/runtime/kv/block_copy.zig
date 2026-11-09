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

//! Cold, zero-growth copies into independently owned KV pages. Every byte
//! layout/capacity is validated before submission. Success is a checked fence;
//! errors must drain attempted work or fail closed before the caller recycles
//! destination IDs. No float conversion, hidden allocation, or graph capture.
const std = @import("std");
const block = @import("block.zig");
pub const Item = extern struct {
    source: block.KvBlockId,
    destination: block.KvBlockId,
    prefix_tokens: u16,
    /// Owner publication metadata, never authority for a provider byte span.
    table_index: usize,
};
pub const Request = struct {
    /// Synchronous actual caller cleanup; never retained by a provider.
    cleanup: @import("../../execution/cleanup_context.zig").Context = .{},
    sequence: u32,
    page_size: u16,
    layers: u16,
    items: []const Item,
    pub fn validate(self: Request) !void {
        if (self.page_size == 0 or self.layers == 0 or self.layers > 256 or self.items.len == 0) return error.InvalidKvBlockCopy;
        for (self.items, 0..) |item, index| {
            if (item.source == block.invalid_block_id or item.destination == block.invalid_block_id or item.source == item.destination or item.prefix_tokens > self.page_size) return error.InvalidKvBlockCopy;
            for (self.items) |other| if (item.destination == other.source) return error.InvalidKvBlockCopy;
            for (self.items[0..index]) |previous| if (item.destination == previous.destination or item.table_index == previous.table_index) return error.InvalidKvBlockCopy;
        }
    }
    pub fn byteRange(self: Request, item: Item, row_bytes: usize) !struct { source: usize, destination: usize, bytes: usize } {
        if (row_bytes == 0) return error.InvalidKvBlockCopy;
        const stride = try std.math.mul(usize, self.page_size, row_bytes);
        return .{ .source = try std.math.mul(usize, item.source, stride), .destination = try std.math.mul(usize, item.destination, stride), .bytes = try std.math.mul(usize, item.prefix_tokens, row_bytes) };
    }
};
pub const Provider = struct {
    cleanup: @import("../../execution/cleanup_context.zig").Context = .{},
    context: *anyopaque,
    implementation: *const anyopaque,
    /// Borrows request metadata synchronously. May not retain a stack pointer.
    /// Complete immutable K/V planes are copied, with exactly one checked cold
    /// completion boundary; numerical buffers and capacity stay unchanged.
    copy_and_wait: *const fn (*anyopaque, *const anyopaque, Request) anyerror!void,
};

test "KV block copy validates all destination and byte ranges before submission" {
    const valid = [_]Item{ .{ .source = 2, .destination = 5, .prefix_tokens = 3, .table_index = 0 }, .{ .source = 3, .destination = 6, .prefix_tokens = 1, .table_index = 1 } };
    var request = Request{ .sequence = 1, .page_size = 4, .layers = 2, .items = &valid };
    try request.validate();
    const bytes = try request.byteRange(valid[0], 18);
    try std.testing.expectEqual(@as(usize, 144), bytes.source);
    try std.testing.expectEqual(@as(usize, 360), bytes.destination);
    try std.testing.expectEqual(@as(usize, 54), bytes.bytes);
    var invalid = valid;
    invalid[1].destination = valid[0].source;
    request.items = &invalid;
    try std.testing.expectError(error.InvalidKvBlockCopy, request.validate());
    request.items = &valid;
    try std.testing.expectError(error.Overflow, request.byteRange(valid[0], std.math.maxInt(usize)));
}
