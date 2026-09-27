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

//! Query-scoped edge streams. Consumers request bounded batches for vectorized
//! admission and stop without retaining the unconsumed adjacency tail.
const std = @import("std");
const graph = @import("graph.zig");
const A = std.mem.Allocator;
pub const batch_records = 64;
pub const batch_bytes = 256 * 1024;

pub const Stream = struct {
    alloc: A,
    ptr: *anyopaque,
    next_fn: *const fn (*anyopaque, A, usize, usize) anyerror!?[]graph.Edge,
    destroy_fn: *const fn (*anyopaque, A) void,

    /// Takes ownership on success only.
    pub fn init(alloc: A, value: anytype) !Stream {
        const T = @TypeOf(value);
        const box = try alloc.create(T);
        box.* = value;
        return .{ .alloc = alloc, .ptr = box, .next_fn = struct {
            fn next(ptr: *anyopaque, a: A, count: usize, bytes: usize) !?[]graph.Edge {
                const self: *T = @ptrCast(@alignCast(ptr));
                return self.nextPage(a, count, bytes);
            }
        }.next, .destroy_fn = struct {
            fn destroy(ptr: *anyopaque, a: A) void {
                const self: *T = @ptrCast(@alignCast(ptr));
                self.deinit(a);
                a.destroy(self);
            }
        }.destroy };
    }

    pub fn next(self: *Stream, count: usize, bytes: usize) !?[]graph.Edge {
        return self.next_fn(self.ptr, self.alloc, @min(batch_records, @max(1, count)), @max(1, bytes));
    }
    pub fn nextBudget(self: *Stream, budget: *@import("work_budget.zig").WorkBudget, demand: usize) !?[]graph.Edge {
        return self.next(@min(demand, budget.edgeLimit()), budget.edgeByteLimit()) catch |err| switch (err) {
            error.GraphExploredEdgesBudgetExceeded => budget.exhaust(.explored_edges, budget.max_edges),
            // An admitted source may have already recorded retained-memory
            // exhaustion. Do not overwrite its more precise diagnostic.
            error.QueryCandidateBudgetExceeded => if (budget.exhaustion() != null) error.GraphWorkBudgetExceeded else budget.exhaust(.explored_edges, budget.max_edges),
            error.GraphExploredEdgeBytesBudgetExceeded => budget.exhaust(.explored_edge_bytes, budget.max_edge_bytes),
            else => err,
        };
    }
    pub fn deinit(self: *Stream) void {
        self.destroy_fn(self.ptr, self.alloc);
        self.* = undefined;
    }

    pub fn empty(alloc: A) !Stream {
        return init(alloc, struct {
            fn nextPage(_: *@This(), _: A, _: usize, _: usize) !?[]graph.Edge {
                return null;
            }
            fn deinit(_: *@This(), _: A) void {}
        }{});
    }

    pub fn fromOwned(alloc: A, reader: anytype, edges: []graph.Edge) !Stream {
        return init(alloc, struct {
            reader: @TypeOf(reader),
            edges: ?[]graph.Edge,
            fn nextPage(self: *@This(), _: A, _: usize, _: usize) !?[]graph.Edge {
                const out = self.edges;
                self.edges = null;
                return out;
            }
            fn deinit(self: *@This(), a: A) void {
                if (self.edges) |items| self.reader.freeEdges(a, items);
            }
        }{ .reader = reader, .edges = edges });
    }
};

test "graph maintenance edge streams preserve source admission diagnostics" {
    const work = @import("work_budget.zig");
    var budget = work.WorkBudget.init(10, 10);
    var stream = try Stream.init(std.testing.allocator, struct {
        budget: *work.WorkBudget,
        fn nextPage(self: *@This(), _: A, _: usize, _: usize) !?[]graph.Edge {
            self.budget.retainStateBytes(self.budget.max_retained_state_bytes + 1) catch {};
            return error.QueryCandidateBudgetExceeded;
        }
        fn deinit(_: *@This(), _: A) void {}
    }{ .budget = &budget });
    defer stream.deinit();
    try std.testing.expectError(error.GraphWorkBudgetExceeded, stream.nextBudget(&budget, 1));
    try std.testing.expectEqual(work.Dimension.retained_state_bytes, budget.exhaustion().?.dimension);
}

pub fn openGraph(alloc: A, index: *graph.GraphIndex, key: []const u8, kinds: []const []const u8, direction: graph.EdgeDirection) !Stream {
    return Stream.init(alloc, index.nativeEdgeScan(key, kinds, direction));
}
