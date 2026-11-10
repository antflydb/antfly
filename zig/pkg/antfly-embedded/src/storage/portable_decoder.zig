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

//! Bounded, restartable physical materialization for disposable restore decoders.
const std = @import("std");
const engine = @import("engine_dispatch.zig").implementation;
const portable = @import("portable_backup.zig");
const store_mod = @import("docstore.zig");
const cancellation_mod = @import("antfly_cancellation");
pub const Proof = union(enum) { cohort: portable.CohortProof, source_copy: portable.SourceCopyProof };
pub const Plan = struct {
    proof: Proof,
    artifact_size: u64,
    scope: [32]u8,
    owner_range: @import("byte_range.zig").ByteRange,
    root: []const u8,
};

// All backend work, including its close/drain, finishes inside this call.
// The receiver for caller-owned I/O therefore lives for the entire operation.
pub fn step(alloc: std.mem.Allocator, io: std.Io, artifact: std.Io.File, plan: Plan, cancellation: cancellation_mod.CancellationToken) !bool {
    if (comptime engine.enabled) return engine.call("portableDecoderStep", .{ alloc, io, artifact, plan, cancellation });
    try cancellation.check();
    const directory = try std.fs.path.join(alloc, &.{ plan.root, "files" });
    defer alloc.free(directory);
    var configuration = @import("db/config.zig").portable_decoder_lsm_options_default;
    configuration.read_runtime = @import("lsm_backend/storage_io.zig").ReadRuntime.init(io);
    var backend = try @import("lsm_backend.zig").Backend.open(alloc, directory, configuration);
    defer backend.close();
    var store = try store_mod.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{ .name = "docs" }));
    defer store.close();
    const started = std.Io.Clock.awake.now(io);
    var complete = false;
    var pages: usize = 0;
    while (pages < 16) : (pages += 1) {
        try cancellation.check();
        complete = switch (plan.proof) {
            .cohort => |proof| try portable.importCohortFilePage(alloc, &store, io, artifact, plan.artifact_size, proof, plan.scope, 128, cancellation),
            .source_copy => |proof| try portable.importSourceCopyFilePage(alloc, &store, io, artifact, plan.artifact_size, proof, plan.scope, 128, cancellation),
        };
        if (complete or started.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds() >= 100 * std.time.ns_per_ms) break;
    }
    if (complete) try bindRange(alloc, &store, plan.owner_range);
    try @import("antfly_runtime_fs").fs_paths.syncDirPortable(io, directory);
    try @import("antfly_runtime_fs").fs_paths.syncDirPortable(io, plan.root);
    return complete;
}

// Routing metadata is absent from portable archives. A disposable decoder may
// persist its admitted range once or confirm the identical range on a retry.
pub fn bindRange(alloc: std.mem.Allocator, store: *store_mod.DocStore, range: @import("byte_range.zig").ByteRange) !void {
    if (comptime engine.enabled) return engine.call("portableDecoderBindRange", .{ alloc, store, range });
    if (range.end.len != 0 and std.mem.order(u8, range.start, range.end) != .lt) return error.RestoreStagingScopeChanged;
    const range_state = @import("db/range_state.zig");
    const encoded = try range_state.encodeRangeAlloc(alloc, range);
    defer alloc.free(encoded);
    var transaction = try store.beginWriteTxn();
    var transaction_open = true;
    defer if (transaction_open) transaction.abort();
    const prior = transaction.get(range_state.range_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (prior) |bytes| {
        if (!std.mem.eql(u8, bytes, encoded)) return error.RestoreStagingScopeChanged;
        transaction.abort();
        transaction_open = false;
    } else {
        try transaction.put(range_state.range_key, encoded);
        try transaction.commit();
        transaction_open = false;
    }
    try store.sync(true);
}
