// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Authenticated forward and inverse coordinates for cross-file serving packs.
const std = @import("std");
const state = @import("lake_index_native_state.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
pub const Span = struct {
    file: state.File,
    physical: artifacts.ChunkRef,
    file_rows: u32,
    file_lower: u32,
    pack_lower: u32,
    count: u32,
};
pub fn validate(spans: []const Span, count: u32, domain: [32]u8) !void {
    if (spans.len == 0 or spans.len > 64) return error.InvalidNativeLakeTextCorpus;
    var lower: u32 = 0;
    for (spans) |span| {
        try state.validate(&.{span.file}, domain);
        if (span.count == 0 or span.pack_lower != lower or @as(u64, span.file_lower) + span.count > span.file_rows or span.physical.byte_len == 0 or span.physical.byte_len > 4 * 1024 * 1024) return error.InvalidNativeLakeTextCorpus;
        try stores.validateSha256ArtifactIdentity(span.physical.artifact_id, span.physical.checksum);
        const scope = (try stores.uploadScopeFromArtifactId(span.physical.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
        if (!std.mem.eql(u8, &scope.domain, &domain)) return error.InvalidNativeLakeTextCorpus;
        lower = try std.math.add(u32, lower, span.count);
    }
    if (lower != count) return error.InvalidNativeLakeTextCorpus;
}
