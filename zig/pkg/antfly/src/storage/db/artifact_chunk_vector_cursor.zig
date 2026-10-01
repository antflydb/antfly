// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Merge selected logical chunks with existing vector scopes. Including old
//! outputs makes empty/shrinking generations retire vectors instead of merely
//! ceasing to produce them. Only one key per input is retained.
const std = @import("std");
const keys = @import("../internal_keys.zig");
const logical = @import("artifact_chunk_cursor.zig");

pub const Budget = @import("artifact_scan_budget.zig").Budget;

pub const Step = union(enum) {
    member: []const u8,
    /// Inclusive physical resume position. No accepted member was skipped.
    yielded: []const u8,
    /// Encoded merge-input positions, including empty/ignored logical scopes.
    logical_yielded: []const u8,
    end,
};

pub fn Cursor(comptime Txn: type) type {
    return struct {
        const Self = @This();
        alloc: std.mem.Allocator,
        chunks: logical.Cursor(Txn),
        outputs: Txn.CursorAdapter,
        prefix: []u8,
        embedding: []const u8,
        chunk: ?[]u8 = null,
        output: ?[]u8 = null,
        output_entry: ?@import("../backend_erased.zig").Entry = null,
        started: bool = false,
        result: ?[]u8 = null,
        logical_resume: ?[]u8 = null,

        pub fn open(alloc: std.mem.Allocator, txn: *Txn, document: []const u8, producer: []const u8, embedding: []const u8) !Self {
            var chunks = try logical.Cursor(Txn).openNamed(alloc, txn, document, producer);
            errdefer chunks.close();
            var outputs = try txn.openPhysicalCursorAdapter();
            errdefer outputs.close();
            const prefix = try keys.artifactNamedPrefixAlloc(alloc, document, "chunk", producer);
            return .{ .alloc = alloc, .chunks = chunks, .outputs = outputs, .prefix = prefix, .embedding = embedding };
        }

        pub fn close(self: *Self) void {
            self.chunks.close();
            self.outputs.close();
            self.alloc.free(self.prefix);
            if (self.chunk) |value| self.alloc.free(value);
            if (self.output) |value| self.alloc.free(value);
            if (self.result) |value| self.alloc.free(value);
            if (self.logical_resume) |value| self.alloc.free(value);
            self.* = undefined;
        }

        pub fn seekAfter(self: *Self, key: []const u8) !void {
            try self.chunks.seekAfter(key);
            // The prefix successor skips the whole member subtree, including
            // binary embedding names and all consumer outputs.
            const upper = try keys.nextPrefixAlloc(self.alloc, key);
            defer if (upper) |value| self.alloc.free(value);
            self.output_entry = if (upper) |value| try self.outputs.seekAtOrAfter(value) else null;
            self.started = true;
            if (self.chunk) |value| self.alloc.free(value);
            self.chunk = null;
            if (self.output) |value| self.alloc.free(value);
            self.output = null;
        }

        pub fn resumePhysical(self: *Self, floor: []const u8) !void {
            if (!std.mem.startsWith(u8, floor, self.prefix) or floor.len == self.prefix.len) return error.InvalidBatchRequest;
            if (!self.started) {
                self.output_entry = try self.outputs.seekAtOrAfter(floor);
                self.started = true;
            } else if (self.output_entry) |row| if (std.mem.order(u8, row.key, floor) == .lt) {
                self.output_entry = try self.outputs.seekAtOrAfter(floor);
            };
        }

        pub fn resumeLogical(self: *Self, raw: []const u8) !void {
            try self.chunks.resumeFrom(raw);
        }

        pub fn checkpointLogicalAlloc(self: *const Self, alloc: std.mem.Allocator) ![]u8 {
            if (self.chunk != null) return self.chunks.checkpointBeforeReturnedAlloc(alloc);
            return self.chunks.checkpointAlloc(alloc);
        }

        /// Returned key is borrowed until the next call. The caller owns and
        /// closes the shared snapshot before invoking any external provider.
        pub fn next(self: *Self) !?[]const u8 {
            var budget: Budget = .{ .max_visits = std.math.maxInt(usize), .max_bytes = std.math.maxInt(usize) };
            return switch (try self.poll(&budget)) {
                .member => |key| key,
                .end => null,
                .yielded, .logical_yielded => unreachable,
            };
        }

        /// Physical tail inspection shares a page budget across calls. The
        /// returned resume position includes ignored/unrelated rows, so a
        /// page with no members still makes restartable forward progress.
        pub fn poll(self: *Self, budget: *Budget) !Step {
            if (self.result) |value| self.alloc.free(value);
            self.result = null;
            if (self.logical_resume) |value| self.alloc.free(value);
            self.logical_resume = null;
            if (!self.started) {
                self.output_entry = try self.outputs.seekAtOrAfter(self.prefix);
                self.started = true;
            }
            if (self.chunk == null) switch (try self.chunks.poll(budget)) {
                .row => |row| {
                    self.chunk = try self.alloc.dupe(u8, row.key);
                },
                .end => {},
                .yielded => {
                    self.logical_resume = try self.chunks.checkpointAlloc(self.alloc);
                    return .{ .logical_yielded = self.logical_resume.? };
                },
            };
            var physical_visits: usize = 0;
            while (self.output == null) {
                const row = self.output_entry orelse break;
                if (!std.mem.startsWith(u8, row.key, self.prefix)) {
                    self.output_entry = null;
                    break;
                }
                // A selected chunk already contributes this scope. Do not
                // scan the entire remaining physical tail looking for an
                // output greater than it; that would repeat at every page.
                if (self.chunk) |chunk| if (std.mem.order(u8, row.key, chunk) != .lt) break;
                // One physical step remains admissible after logical prefetch
                // consumed the budget. Otherwise every retry would refetch
                // the same member and yield at the same output forever.
                if (budget.exhausted() and physical_visits != 0) return .{ .yielded = row.key };
                physical_visits += 1;
                budget.visits +|= 1;
                budget.bytes +|= row.key.len +| row.value.len;
                if (keys.isDerivedEmbeddingArtifactKey(row.key) and keys.matchesDerivedEmbeddingArtifactName(row.key, self.embedding))
                    self.output = try keys.derivedEmbeddingBaseKeyAlloc(self.alloc, row.key);
                self.output_entry = try self.outputs.next();
            }
            if (self.chunk == null and self.output == null) return .end;
            const order = if (self.chunk == null) std.math.Order.gt else if (self.output == null) std.math.Order.lt else std.mem.order(u8, self.chunk.?, self.output.?);
            if (order != .gt) {
                self.result = self.chunk;
                self.chunk = null;
                if (order == .eq) {
                    self.alloc.free(self.output.?);
                    self.output = null;
                }
                if (self.output_entry) |row| if (std.mem.startsWith(u8, row.key, self.result.?)) {
                    const upper = try keys.nextPrefixAlloc(self.alloc, self.result.?);
                    defer if (upper) |value| self.alloc.free(value);
                    self.output_entry = if (upper) |value| try self.outputs.seekAtOrAfter(value) else null;
                };
            } else {
                self.result = self.output;
                self.output = null;
            }
            budget.visits +|= 1;
            budget.bytes +|= self.result.?.len;
            return .{ .member = self.result.? };
        }
    };
}
