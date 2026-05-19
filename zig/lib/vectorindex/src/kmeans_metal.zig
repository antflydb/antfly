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
const builtin = @import("builtin");

pub const enabled = builtin.os.tag == .macos and !builtin.is_test;

fn heapAllocator() std.mem.Allocator {
    if (comptime builtin.link_libc) return std.heap.c_allocator;
    if (comptime builtin.single_threaded) return std.heap.page_allocator;
    return std.heap.smp_allocator;
}

pub fn available() bool {
    if (!enabled) return false;
    return native.antfly_kmeans_metal_available() != 0;
}

pub const Context = struct {
    handle: *anyopaque,
    assignment_u32: []u32,
    counts_u32: []u32,
    point_count: usize,
    max_cluster_count: usize,
    dims: usize,

    pub fn init(
        points: []const f32,
        point_count: usize,
        max_cluster_count: usize,
        dims: usize,
    ) !Context {
        if (!enabled) return error.MetalUnavailable;
        if (point_count > std.math.maxInt(u32) or max_cluster_count > std.math.maxInt(u32) or dims > std.math.maxInt(u32)) {
            return error.MetalUnsupportedShape;
        }
        if (points.len != point_count * dims) return error.InvalidDimensions;
        if (native.antfly_kmeans_metal_available() == 0) return error.MetalUnavailable;

        const alloc = heapAllocator();
        const assignment_u32 = try alloc.alloc(u32, point_count);
        errdefer alloc.free(assignment_u32);
        const counts_u32 = try alloc.alloc(u32, max_cluster_count);
        errdefer alloc.free(counts_u32);

        const handle = native.antfly_kmeans_metal_context_create(
            points.ptr,
            @intCast(point_count),
            @intCast(max_cluster_count),
            @intCast(dims),
        ) orelse return error.MetalUnavailable;

        return .{
            .handle = handle,
            .assignment_u32 = assignment_u32,
            .counts_u32 = counts_u32,
            .point_count = point_count,
            .max_cluster_count = max_cluster_count,
            .dims = dims,
        };
    }

    pub fn deinit(self: *Context) void {
        if (enabled) native.antfly_kmeans_metal_context_destroy(self.handle);
        const alloc = heapAllocator();
        alloc.free(self.counts_u32);
        alloc.free(self.assignment_u32);
        self.* = undefined;
    }

    pub fn assign(
        self: *Context,
        centroids: []const f32,
        dims: usize,
        metric: i32,
        assignments: []usize,
        distances: []f32,
    ) !void {
        if (!enabled) return error.MetalUnavailable;
        if (dims != self.dims) return error.InvalidDimensions;
        if (centroids.len % self.dims != 0) return error.InvalidDimensions;
        const cluster_count = centroids.len / self.dims;
        if (cluster_count == 0 or cluster_count > self.max_cluster_count) return error.InvalidDimensions;
        if (assignments.len != self.point_count or distances.len != self.point_count) return error.InvalidDimensions;

        const rc = native.antfly_kmeans_metal_context_assign(
            self.handle,
            centroids.ptr,
            @intCast(cluster_count),
            metric,
            self.assignment_u32.ptr,
            distances.ptr,
        );
        if (rc != 0) return error.MetalAssignFailed;

        for (assignments, self.assignment_u32) |*dst, value| dst.* = value;
    }

    pub fn updateCentroids(
        self: *Context,
        old_centroids: []const f32,
        next_centroids: []f32,
        counts: []usize,
        dims: usize,
        metric: i32,
    ) !void {
        if (!enabled) return error.MetalUnavailable;
        if (dims != self.dims) return error.InvalidDimensions;
        if (old_centroids.len != next_centroids.len) return error.InvalidDimensions;
        if (old_centroids.len % self.dims != 0) return error.InvalidDimensions;
        const cluster_count = old_centroids.len / self.dims;
        if (cluster_count == 0 or cluster_count > self.max_cluster_count or counts.len != cluster_count) return error.InvalidDimensions;

        const rc = native.antfly_kmeans_metal_context_update_centroids(
            self.handle,
            old_centroids.ptr,
            @intCast(cluster_count),
            metric,
            next_centroids.ptr,
            self.counts_u32.ptr,
        );
        if (rc != 0) return error.MetalAssignFailed;

        for (counts, self.counts_u32[0..cluster_count]) |*dst, value| dst.* = value;
    }
};

pub fn assign(
    points: []const f32,
    centroids: []const f32,
    point_count: usize,
    cluster_count: usize,
    dims: usize,
    metric: i32,
    assignments: []usize,
    distances: []f32,
) !void {
    if (!enabled) return error.MetalUnavailable;
    if (point_count > std.math.maxInt(u32) or cluster_count > std.math.maxInt(u32) or dims > std.math.maxInt(u32)) {
        return error.MetalUnsupportedShape;
    }
    if (points.len != point_count * dims or centroids.len != cluster_count * dims) return error.InvalidDimensions;
    if (assignments.len != point_count or distances.len != point_count) return error.InvalidDimensions;

    var context = try Context.init(points, point_count, cluster_count, dims);
    defer context.deinit();
    try context.assign(centroids, dims, metric, assignments, distances);
}

pub fn assignL2(
    points: []const f32,
    centroids: []const f32,
    point_count: usize,
    cluster_count: usize,
    dims: usize,
    assignments: []usize,
    distances: []f32,
) !void {
    try assign(points, centroids, point_count, cluster_count, dims, 0, assignments, distances);
}

const native = if (enabled) struct {
    extern fn antfly_kmeans_metal_available() callconv(.c) c_int;
    extern fn antfly_kmeans_metal_context_create(
        points: [*]const f32,
        point_count: u32,
        max_cluster_count: u32,
        dims: u32,
    ) callconv(.c) ?*anyopaque;
    extern fn antfly_kmeans_metal_context_destroy(handle: *anyopaque) callconv(.c) void;
    extern fn antfly_kmeans_metal_context_assign(
        handle: *anyopaque,
        centroids: [*]const f32,
        cluster_count: u32,
        metric: i32,
        assignments: [*]u32,
        distances: [*]f32,
    ) callconv(.c) c_int;
    extern fn antfly_kmeans_metal_context_update_centroids(
        handle: *anyopaque,
        old_centroids: [*]const f32,
        cluster_count: u32,
        metric: i32,
        next_centroids: [*]f32,
        counts: [*]u32,
    ) callconv(.c) c_int;
    extern fn antfly_kmeans_metal_assign(
        points: [*]const f32,
        centroids: [*]const f32,
        point_count: u32,
        cluster_count: u32,
        dims: u32,
        metric: i32,
        assignments: [*]u32,
        distances: [*]f32,
    ) callconv(.c) c_int;
} else struct {};
