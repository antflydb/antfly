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

//! Prior context is a bounded causal dependency. The target unit's epoch is
//! advanced only by original source events inside the configured horizon.
//! Inheriting prior producers' transitive proofs would defeat that bound and
//! eventually make every new segment depend on the entire conversation.
const std = @import("std");
const groups = @import("../group_sources.zig");
const state = @import("../group_source_state.zig");
const identity = @import("../group_source_identity.zig");
const prior = @import("../group_prior_context.zig");
const context = @import("../artifact/producer_context.zig");
const keys = @import("../../internal_keys.zig");
const requests = @import("enrichment_types.zig");
const Store = @import("../../docstore.zig").DocStore;
const A = std.mem.Allocator;
pub const Input = struct {
    arena: std.heap.ArenaAllocator,
    text: []const u8,
    fingerprint: groups.Digest,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
pub fn capture(a: A, store: *Store, request: requests.GeneratedEnrichmentRequest, raw: []const u8, token: ?*context.Token, renderer: prior.Renderer) !Input {
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const config = try std.json.parseFromSliceLeaky(groups.PriorContext, owned, request.prior_context_json, .{});
    try config.validate();
    const source = try std.json.parseFromSliceLeaky(std.json.Value, owned, raw, .{});
    if (source != .object) return error.GroupedSourceCorrupt;
    const group_key = (source.object.get("_group_identity") orelse source.object.get("_group_key")) orelse return error.GroupedSourceCorrupt;
    const segment = (source.object.get("_segment_identity") orelse source.object.get("_segment_key")) orelse return error.GroupedSourceCorrupt;
    const ordinal = source.object.get("_ordinal_start") orelse return error.GroupedSourceCorrupt;
    if (group_key != .string or segment != .string or ordinal != .integer) return error.GroupedSourceCorrupt;
    const table = identity.tableOf(request.doc_key) orelse return error.GroupedSourceCorrupt;
    const group = state.Group{ .definition = .{ .table_incarnation = table, .configuration = request.group_configuration }, .key = group_key.string };
    var read = try store.beginReadTxn();
    defer read.abort();
    if (try state.nextPending(owned, &read, group)) |pending| {
        if (!std.mem.eql(u8, pending.partition.segment, segment.string) and prior.Position.before(.{ .ordinal = pending.ordinal, .segment = pending.partition.segment }, .{ .ordinal = ordinal.integer, .segment = segment.string })) return error.ArtifactPublicationPending;
    }
    const epoch_key = try identity.unitHorizonEpochKey(owned, request.doc_key, config.horizon_segments);
    const epoch = read.getLeased(epoch_key) catch |err| if (err == error.NotFound) null else return err;
    if (token) |value| try value.observe(epoch_key, epoch, try @import("../artifact/publication.zig").artifactRevision(&read, value.namespace, epoch_key));
    const Read = struct {
        txn: *@TypeOf(read),
        a: A,
        token: ?*context.Token,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.txn.get(key);
        }
        pub fn openPhysicalCursorAdapter(self: *@This()) @TypeOf(read.openPhysicalCursorAdapter()) {
            return self.txn.openPhysicalCursorAdapter();
        }
        pub fn getArtifact(self: *@This(), unit: []const u8, name: []const u8) !?[]const u8 {
            const selected = try keys.artifactNamedPrefixAlloc(self.a, unit, "asset", name);
            const artifact = self.txn.getLeased(selected) catch |err| if (err == error.NotFound) return null else return err;
            if (self.token != null) {
                var proof = (try @import("../artifact/producer_provenance.zig").readCurrentForArtifact(self.a, self.txn, selected, artifact)) orelse return error.ArtifactPublicationPending;
                proof.deinit();
            }
            return artifact;
        }
    };
    var reader = Read{ .txn = &read, .a = owned, .token = token };
    const current = prior.Position{ .ordinal = ordinal.integer, .segment = segment.string };
    const candidates = try @import("../group_prior_scan.zig").collect(owned, &reader, group, current, config);
    const selected = try prior.select(owned, config, group.key, current, candidates, renderer);
    return .{ .arena = arena, .text = selected.text, .fingerprint = selected.fingerprint };
}
