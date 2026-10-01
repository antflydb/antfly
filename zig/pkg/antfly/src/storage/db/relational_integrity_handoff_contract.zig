// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Data-only relational lifecycle contract. Physical execution stays in its owner.
const std = @import("std");
pub const PruneProgress = struct { fence: @import("relational_integrity_topology_contract.zig").Fence, lower: []const u8, upper: []const u8, cursor: []const u8 = "", complete: bool = false };
const Allocator = std.mem.Allocator;
const integrity = @import("relational_integrity_contract.zig");
const topology = @import("relational_integrity_topology_contract.zig");

pub const MergeCopyAttempt = struct {
    donor_term: u64 = 0,
    sequence: u64 = 0,

    pub fn order(a: MergeCopyAttempt, b: MergeCopyAttempt) std.math.Order {
        const term_order = std.math.order(a.donor_term, b.donor_term);
        return if (term_order == .eq) std.math.order(a.sequence, b.sequence) else term_order;
    }
};

pub const manifest_key = "\x00\x00__metadata__:relational_integrity_handoff_manifest";

pub const progress_key = "\x00\x00__metadata__:relational_integrity_handoff_progress";

pub const prune_key = "\x00\x00__metadata__:relational_integrity_handoff_prune";

pub const max_records = 256;
// Binary records use numeric-array JSON on the private transport. Keep the
// encoded control comfortably below the existing HTTP/journal frame limit.

pub const max_bytes = 1024 * 1024;

pub const Manifest = struct {
    source: topology.Fence,
    destination: topology.Fence,
    lower: []const u8,
    upper: []const u8,
    source_range_start: []const u8,
    source_range_end: []const u8,
    catalog_bytes: []const u8,
    activation_bytes: []const u8,
    primary_sequence: u64,
    /// Immutable lower bound on primary-copy attempts for this frozen source.
    /// The ordinary merge receipt fences each later elected-leader attempt;
    /// every attempt still reads the same quiesced data and routed metadata.
    merge_copy_attempt: MergeCopyAttempt = .{},
};

pub const Record = struct { key: []const u8, value: []const u8 };

pub const Page = struct {
    sequence: u64,
    previous_digest: integrity.Digest,
    after: []const u8,
    next_cursor: []const u8,
    records: []const Record,
    exhausted: bool,
};

pub const Command = union(enum) { begin: Manifest, page: Page, finish: struct { sequence: u64, digest: integrity.Digest } };

pub const Progress = struct {
    manifest_digest: integrity.Digest,
    sequence: u64 = 0,
    digest: integrity.Digest = @splat(0),
    cursor: []const u8 = "",
    exhausted: bool = false,
    ready: bool = false,
    verification_cursor: []const u8 = "",
};

pub fn encode(alloc: Allocator, value: anytype) ![]u8 {
    var output: std.Io.Writer.Allocating = .init(alloc);
    errdefer output.deinit();
    var stream: std.json.Stringify = .{ .writer = &output.writer, .options = .{} };
    try @import("relational_integrity_json.zig").write(value, &stream);
    return output.toOwnedSlice();
}

pub fn hash(bytes: []const u8) integrity.Digest {
    var result: integrity.Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}
