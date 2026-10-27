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

//! Compact selected indexed afterimages. One bounded metadata page shares an
//! actual publication lease and primary query owner. Handles contain one pointer;
//! legacy hits allocate nothing. No page contains source bodies or wire grants.
const std = @import("std");
const claims = @import("../index_source_claim.zig");
const types = @import("types.zig");
const A = std.mem.Allocator;

pub const ReadRequest = union(enum) { projection: types.LookupOptions, logical_paths: []const []const []const u8 };
pub const Owner = struct {
    ptr: *anyopaque,
    retain: *const fn (*anyopaque) void,
    release: *const fn (*anyopaque) void,
    lookup: *const fn (*anyopaque, A, *const claims.CapturedSource, ReadRequest) anyerror!types.LookupResult,
    conditional: ?*const fn (*anyopaque, A, *const claims.CapturedSource, []const u8, types.LookupOptions, @import("../../api/source_projection.zig").Prepared) anyerror!?@import("indexed_conditional.zig").Result = null,
};
const Entry = struct { page: *Page, source: claims.SnapshotEntry };
pub const Row = struct {
    entry: *const Entry,
    /// Live native evidence must be consumed before any wire boundary. Generic
    /// hit serializers may handle an absent optional row, never export a lease.
    pub fn jsonStringify(_: Row, _: anytype) error{WriteFailed}!void {
        return error.WriteFailed;
    }
    pub fn jsonParse(_: A, _: anytype, _: std.json.ParseOptions) error{UnexpectedToken}!Row {
        return error.UnexpectedToken;
    }
    pub fn jsonParseFromValue(_: A, _: std.json.Value, _: std.json.ParseOptions) error{UnexpectedToken}!Row {
        return error.UnexpectedToken;
    }
    pub fn clone(self: Row) Row {
        self.entry.page.retain();
        return self;
    }
    pub fn deinit(self: Row) void {
        self.entry.page.release();
    }
    pub fn lookupLogicalPaths(self: Row, a: A, paths: []const []const []const u8) !types.LookupResult {
        const page = self.entry.page;
        const captured: claims.CapturedSource = .{ .reference = self.entry.source.reference(page.root.decision.binding), .decision = page.root.decision, .lease = page.root.lease };
        return page.owner.lookup(page.owner.ptr, a, &captured, .{ .logical_paths = paths });
    }
    pub fn lookupConditional(self: Row, a: A, id: []const u8, options: types.LookupOptions, prepared: @import("../../api/source_projection.zig").Prepared) !?@import("indexed_conditional.zig").Result {
        const page = self.entry.page;
        const run = page.owner.conditional orelse return null;
        const captured: claims.CapturedSource = .{ .reference = self.entry.source.reference(page.root.decision.binding), .decision = page.root.decision, .lease = page.root.lease };
        return run(page.owner.ptr, a, &captured, id, options, prepared);
    }
    pub fn lookup(self: Row, a: A, options: types.LookupOptions) !types.LookupResult {
        const page = self.entry.page;
        // Borrow the page's actual root lease through this synchronous lookup.
        const captured: claims.CapturedSource = .{ .reference = self.entry.source.reference(page.root.decision.binding), .decision = page.root.decision, .lease = page.root.lease };
        return page.owner.lookup(page.owner.ptr, a, &captured, .{ .projection = options });
    }
};
pub const Page = struct {
    allocator: A,
    block: []align(@alignOf(u64)) u8,
    refs: std.atomic.Value(usize) = .init(1),
    root: claims.CapturedRoot,
    owner: Owner,
    entries: []Entry,
    /// Consumes the captured root on every path. Owner is borrowed until the
    /// constructor retains it; all metadata fits in one allocation, <=64 rows.
    pub fn create(a: A, root_value: claims.CapturedRoot, owner: Owner, references: []const claims.Reference) !*Page {
        var root = root_value;
        errdefer root.deinit();
        if (references.len == 0 or references.len > 64) return error.InvalidArgument;
        try root.decision.require(root.decision.binding, .published);
        try root.decision.requireSourceRole();
        try root.lease.inner.require(root.decision.binding);
        for (references) |reference| if (!std.meta.eql(reference.binding, root.decision.binding)) return error.IndexPublicationFenceMismatch;
        const offset = std.mem.alignForward(usize, @sizeOf(Page), @alignOf(Entry));
        const size = try std.math.add(usize, offset, try std.math.mul(usize, references.len, @sizeOf(Entry)));
        const block = try a.alignedAlloc(u8, .of(u64), size);
        const self: *Page = @ptrCast(block.ptr);
        const entries = @as([*]Entry, @ptrCast(@alignCast(block.ptr + offset)))[0..references.len];
        owner.retain(owner.ptr);
        self.* = .{ .allocator = a, .block = block, .root = root, .owner = owner, .entries = entries };
        for (entries, references) |*entry, reference| entry.* = .{ .page = self, .source = .fromReference(reference) };
        return self;
    }
    pub fn row(self: *Page, index: usize) Row {
        std.debug.assert(index < self.entries.len);
        self.retain();
        return .{ .entry = &self.entries[index] };
    }
    pub fn retain(self: *Page) void {
        const before = self.refs.fetchAdd(1, .monotonic);
        std.debug.assert(before != 0 and before != std.math.maxInt(usize));
    }
    pub fn release(self: *Page) void {
        const before = self.refs.fetchSub(1, .acq_rel);
        std.debug.assert(before != 0);
        if (before != 1) return;
        const a = self.allocator;
        const block = self.block;
        self.root.deinit();
        self.owner.release(self.owner.ptr);
        a.free(block);
    }
};

test "api query contract indexed source handles cannot be reconstructed from JSON" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(Row, a, "{}", .{}));
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(Row, a, "null", .{}));
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromValue(Row, a, .{ .object = .empty }, .{}));
    var absent = try std.json.parseFromSlice(?Row, a, "null", .{});
    defer absent.deinit();
    try std.testing.expect(absent.value == null);
    const encoded = try std.json.Stringify.valueAlloc(a, @as(?Row, null), .{});
    defer a.free(encoded);
    try std.testing.expectEqualStrings("null", encoded);
}
