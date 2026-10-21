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

//! Stage-specific extraction ownership, shared by model execution and publication.
const std = @import("std");
const t = @import("types.zig");
pub const Stage = enum { entities, distillation, summaries };
pub const Plan = struct {
    config: t.Config,
    sources: []const t.StoredEvidence,

    pub fn enabled(self: Plan, stage: Stage) bool {
        return switch (stage) {
            .entities => self.config.features.entity_extraction,
            .distillation => self.config.features.assertion_distillation,
            .summaries => self.config.features.summaries,
        };
    }
    fn current(source: t.StoredEvidence, stage: Stage) bool {
        return !source.context_only and (stage != .distillation or source.evidence.managed);
    }
    pub fn hasCurrent(self: Plan, stage: Stage) bool {
        for (self.sources) |source| if (current(source, stage)) return true;
        return false;
    }
    // Keep contextual inputs visible and causally attributable, but never
    // present client-owned evidence as a distillation root to the generator.
    pub fn inputs(self: Plan, a: std.mem.Allocator, stage: Stage) ![]const t.StoredEvidence {
        const result = try a.dupe(t.StoredEvidence, self.sources);
        for (result) |*source| source.context_only = !current(source.*, stage);
        return result;
    }
    fn support(self: Plan, stage: Stage, refs: []const t.EvidenceRef) !void {
        var rooted = false;
        for (refs) |ref| {
            var found = false;
            for (self.sources) |source| if (std.mem.eql(u8, ref.id, source.evidence.id)) {
                found = true;
                rooted = rooted or current(source, stage);
                break;
            };
            if (!found) return error.MemoryUngroundedOutput;
        }
        // Alternatives are independent support. Each branch must retain the
        // stage's own current observation, not borrow another branch's root.
        if (!rooted) return error.MemoryUngroundedOutput;
    }
    pub fn assertionStage(claim: t.Assertion) Stage {
        return if (claim.kind == .summary or claim.kind == .procedural) .summaries else .distillation;
    }
    pub fn validateAssertion(self: Plan, stage: Stage, claim: t.Assertion) !void {
        if (!self.enabled(stage) or stage == .entities or assertionStage(claim) != stage) return error.MemoryUngroundedOutput;
        try self.support(stage, claim.evidence);
        for (claim.support_sets) |branch| try self.support(stage, branch.evidence);
    }
    pub fn validateOutput(self: Plan, stage: Stage, output: t.Generated) !void {
        if (!self.enabled(stage)) return error.MemoryUngroundedOutput;
        if (output.assertions.len + output.entities.len + output.mentions.len > self.config.max_output_items) return error.MemoryInputTooLarge;
        if (stage == .entities) {
            if (output.assertions.len != 0) return error.MemoryUngroundedOutput;
            for (output.entities) |entity| try self.support(stage, entity.evidence);
            for (output.mentions) |mention| try self.support(stage, mention.evidence);
        } else {
            if (output.entities.len != 0 or output.mentions.len != 0) return error.MemoryUngroundedOutput;
            for (output.assertions) |claim| {
                if (claim.embedding != null) return error.MemoryUngroundedOutput;
                try self.validateAssertion(stage, claim);
            }
        }
    }
    pub fn validatePublication(self: Plan, output: t.Generated) !void {
        if (output.assertions.len + output.entities.len + output.mentions.len > self.config.max_output_items) return error.MemoryInputTooLarge;
        if (output.entities.len + output.mentions.len > 0) try self.validateOutput(.entities, .{ .entities = output.entities, .mentions = output.mentions });
        for (output.assertions) |claim| {
            if (claim.embedding != null) return error.MemoryUngroundedOutput;
            try self.validateAssertion(assertionStage(claim), claim);
        }
    }
};
