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

//! Direct-storage operator for independently retained immutable datasets.
//! This administrative surface does not change a source table's TTL or claim
//! that an externally supplied snapshot was transactionally captured.
const std = @import("std");
const datasets = @import("../serverless/datasets/mod.zig");
const objects = @import("../serverless/artifacts/object_store.zig");
const t = datasets.types;

pub const Binding = struct { id: t.Id, uri: []const u8, access: t.Access };
pub const Request = struct {
    format: u8 = 1,
    action: enum { create, evolve, status, list, plan, publish, migrate, restore, reclaim_local, reclaim_upload, read, hold, release_hold, retire, collect, compact },
    catalog_uri: []const u8,
    dataset: t.Id,
    definition: ?t.Definition = null,
    segment: ?t.Segment = null,
    segment_id: ?t.Id = null,
    expected_revision: ?u64 = null,
    generation: ?u64 = null,
    partition: ?t.Id = null,
    incarnation: ?u64 = null,
    range: ?t.Range = null,
    expected_generation: ?u64 = null,
    source: ?Binding = null,
    destination: ?Binding = null,
    dependency: ?datasets.catalog.Dependency = null,
    input: ?[]const u8 = null,
    output: ?[]const u8 = null,
    after: []const u8 = "",
    limit: usize = 64,
};

pub const usage = "usage: antfly storage dataset --request FILE\nRun a bounded retained-dataset operation described by JSON. See docs/guides/retained-datasets.mdx.\n";

pub fn runFromIterator(init: std.process.Init, iterator: *std.process.Args.Iterator) !void {
    const option = iterator.next() orelse {
        try std.Io.File.stdout().writeStreamingAll(init.io, usage);
        return;
    };
    if (std.mem.eql(u8, option, "--help") or std.mem.eql(u8, option, "-h")) {
        try std.Io.File.stdout().writeStreamingAll(init.io, usage);
        return;
    }
    if (!std.mem.eql(u8, option, "--request")) return error.InvalidArguments;
    const path = iterator.next() orelse return error.InvalidArguments;
    if (iterator.next() != null) return error.InvalidArguments;
    const a = init.arena.allocator();
    const raw = try std.Io.Dir.cwd().readFileAlloc(init.io, path, a, .limited(32768));
    const request = try std.json.parseFromSlice(Request, a, raw, .{ .allocate = .alloc_always });
    defer request.deinit();
    try run(init, request.value);
}

fn binding(store: *objects.ObjectStore, value: Binding) datasets.Location {
    var identity: t.Id = undefined;
    std.crypto.hash.sha2.Sha256.hash(value.uri, &identity, .{});
    return .{ .id = value.id, .storage_identity = identity, .client = store.client, .bucket = store.bucket, .prefix = if (store.prefix.len == 0) "retained-segments" else store.prefix, .access = value.access };
}

fn emit(init: std.process.Init, value: anytype) !void {
    const json = try std.json.Stringify.valueAlloc(init.arena.allocator(), value, .{});
    try std.Io.File.stdout().writeStreamingAll(init.io, json);
    try std.Io.File.stdout().writeStreamingAll(init.io, "\n");
}

pub fn run(init: std.process.Init, request: Request) !void {
    if (request.format != 1 or t.zero(request.dataset)) return error.InvalidDatasetBinding;
    const a = init.arena.allocator();
    var metadata = try objects.ObjectStore.initRemoteUri(a, request.catalog_uri);
    defer metadata.deinit();
    var owner: [16]u8 = undefined;
    init.io.random(&owner);
    const namespace = std.fmt.bytesToHex(request.dataset, .lower);
    const catalog_prefix = try std.fmt.allocPrint(a, "{s}{s}retained-catalog", .{ metadata.prefix, if (metadata.prefix.len == 0) "" else "/" });
    const catalog = datasets.Catalog{ .context = .{ .alloc = a, .client = metadata.client, .bucket = metadata.bucket, .prefix = catalog_prefix, .namespace = &namespace, .io = init.io }, .dataset = request.dataset, .owner = owner };
    switch (request.action) {
        .create => try catalog.create(request.definition orelse return error.InvalidArguments),
        .evolve => try catalog.evolve(request.expected_generation orelse return error.InvalidArguments, request.definition orelse return error.InvalidArguments),
        .status => {
            if (request.segment_id) |id| {
                const value = try catalog.getSegment(id);
                defer value.deinit();
                return emit(init, value.value);
            }
            const value = try catalog.getDefinition();
            defer value.deinit();
            return emit(init, value.value);
        },
        .list => {
            const page = try catalog.listSegments(request.after, request.limit);
            defer page.deinit();
            return emit(init, page.value);
        },
        .plan => {
            var plan = try catalog.planRange(request.generation orelse return error.InvalidArguments, request.partition orelse return error.InvalidArguments, request.incarnation orelse return error.InvalidArguments, request.range orelse return error.InvalidArguments, request.limit);
            defer plan.deinit();
            return emit(init, .{ .segments = plan.segments });
        },
        .hold => try catalog.hold(request.segment_id orelse return error.InvalidArguments, request.dependency orelse return error.InvalidArguments),
        .release_hold => try catalog.releaseHold(request.segment_id orelse return error.InvalidArguments, request.dependency orelse return error.InvalidArguments),
        .compact => try catalog.compact(request.segment_id orelse return error.InvalidArguments),
        .retire => try catalog.retire(request.segment_id orelse return error.InvalidArguments, request.expected_revision orelse return error.InvalidArguments),
        else => {
            const source = request.source orelse return error.InvalidArguments;
            var storage = try objects.ObjectStore.initRemoteUri(a, source.uri);
            defer storage.deinit();
            const location = binding(&storage, source);
            switch (request.action) {
                .publish => {
                    const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, request.input orelse return error.InvalidArguments, a, .limited(datasets.catalog.max_segment_bytes));
                    try catalog.publish(request.segment orelse return error.InvalidArguments, location, bytes);
                },
                .read => {
                    const output = request.output orelse return error.InvalidArguments;
                    const lease = try catalog.acquire(request.segment_id orelse return error.InvalidArguments);
                    defer catalog.release(lease) catch {};
                    const bytes = try catalog.read(lease, location);
                    defer a.free(bytes);
                    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = output, .data = bytes });
                },
                .migrate, .restore => {
                    const target = request.destination orelse return error.InvalidArguments;
                    var destination = try objects.ObjectStore.initRemoteUri(a, target.uri);
                    defer destination.deinit();
                    if (request.action == .migrate) try catalog.migrate(request.segment_id orelse return error.InvalidArguments, request.expected_revision orelse return error.InvalidArguments, location, binding(&destination, target)) else try catalog.restore(request.segment_id orelse return error.InvalidArguments, request.expected_revision orelse return error.InvalidArguments, location, binding(&destination, target));
                },
                .reclaim_upload => return emit(init, .{ .complete = try catalog.reclaimUpload(location) }),
                .reclaim_local => return emit(init, .{ .complete = try catalog.reclaimLocal(request.segment_id orelse return error.InvalidArguments, location) }),
                .collect => try catalog.collect(request.segment_id orelse return error.InvalidArguments, location),
                else => unreachable,
            }
        },
    }
    try emit(init, .{ .ok = true });
}
