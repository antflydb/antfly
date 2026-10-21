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

//! Print native memory wire schemas for the public OpenAPI source generator.
const std = @import("std");
const t = @import("types.zig");
const schema = @import("schema.zig");
pub fn main(init: std.process.Init) !void {
    var arena = std.heap.ArenaAllocator.init(init.gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var schemas: std.json.ObjectMap = .empty;
    inline for (.{
        .{ "ProvenanceDecision", t.provenance.Decision },
        .{ "ProvenanceEnvelope", t.provenance.Envelope },
        .{ "MemoryExplanation", t.provenance.Explanation },
        .{ "TraceEvent", t.provenance.TraceEvent },
        .{ "ResourceSelector", t.provenance.Selector },
        .{ "ResourceReference", t.provenance.Resource },
        .{ "ProvenanceOrigin", t.provenance.Origin },
        .{ "MemoryExplainRequest", t.provenance.ExplainRequest },
        .{ "MemorySpaceConfig", t.Config },
        .{ "MemoryConfigureRequest", @import("operations.zig").Configure },
        .{ "MemoryEvidence", t.Evidence },
        .{ "MemoryAssertion", t.Assertion },
        .{ "MemoryObservationRequest", t.ObservationRequest },
        .{ "MemoryAssertionRequest", t.AssertionRequest },
        .{ "MemoryCorrection", t.Correction },
        .{ "MemoryForgetRequest", t.Forget },
        .{ "MemoryOutcome", t.Outcome },
        .{ "MemoryRecallRequest", t.Recall },
        .{ "MemoryOperation", t.OperationStatus },
        .{ "MemoryRecord", t.StoredAssertion },
        .{ "MemoryEntity", t.Entity },
        .{ "MemoryEntityCorrection", t.EntityCorrection },
        .{ "MemoryEntityCandidateRequest", t.EntityCandidateRequest },
        .{ "MemoryEntityProposalRequest", t.EntityProposalRequest },
        .{ "MemoryEntityVerdict", t.EntityVerdict },
        .{ "MemoryEntityViewRequest", t.EntityViewRequest },
        .{ "MemoryEntityView", t.EntityView },
        .{ "MemoryEntityMention", t.EntityMention },
        .{ "MemoryEpisodeRequest", t.EpisodeRequest },
        .{ "MemoryEpisode", t.Episode },
        .{ "MemoryEpisodePage", t.EpisodePage },
        .{ "MemoryRelationshipProposalRequest", t.RelationshipProposalRequest },
        .{ "MemoryRelationshipProposal", t.RelationshipProposal },
        .{ "MemoryEmbeddingBatch", t.EmbeddingBatch },
        .{ "MemoryRecallResult", t.RecallResult },
        .{ "MemoryRecordPage", t.RecordPage },
        .{ "MemoryEntityPage", t.EntityPage },
        .{ "MemoryInspection", t.Inspection },
    }) |entry| try schemas.put(a, entry[0], try schema.wire(entry[1], a));
    try schemas.put(a, "_memory_mcp_tools", try @import("operations.zig").mcpTools(a));
    const bytes = try std.json.Stringify.valueAlloc(a, std.json.Value{ .object = schemas }, .{ .whitespace = .indent_2 });
    var buffer: [4096]u8 = undefined;
    var output = std.Io.File.stdout().writer(init.io, &buffer);
    try output.interface.writeAll(bytes);
    try output.interface.flush();
}
