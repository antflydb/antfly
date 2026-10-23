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

const std = @import("std");
const graph = @import("graph_request_diagnostics.zig");
const reranking = @import("antfly_reranking");

/// Owns structured diagnostics produced while executing one public query.
/// Storage lives on the request stack so reused worker threads and nested
/// internal queries cannot leak details between requests.
pub const Context = struct {
    graph: graph.Context = .{},
    reranker_candidate_limit: reranking.CandidateLimitDiagnosticStorage = .{},
};

/// Binds request-owned diagnostic storage for synchronous query execution.
/// Bindings are stackable and restore the caller when an internal query exits.
pub const Scope = struct {
    graph: graph.Scope,
    reranker_candidate_limit: reranking.CandidateLimitDiagnosticBinding,

    pub fn init(context: *Context) Scope {
        return .{
            .graph = graph.Scope.init(&context.graph),
            .reranker_candidate_limit = reranking.bindCandidateLimitDiagnostic(&context.reranker_candidate_limit),
        };
    }

    pub fn deinit(self: Scope) void {
        self.reranker_candidate_limit.deinit();
        self.graph.deinit();
    }
};

pub fn reset() void {
    graph.reset();
    reranking.resetCandidateLimitDiagnostic();
}

test "nested query scopes restore their caller reranker diagnostic" {
    var outer: Context = .{};
    const outer_scope = Scope.init(&outer);
    defer outer_scope.deinit();
    outer.reranker_candidate_limit.diagnostic = .{ .provider = .cohere, .maximum = 1000 };

    var inner: Context = .{};
    const inner_scope = Scope.init(&inner);
    inner.reranker_candidate_limit.diagnostic = .{ .provider = .vertex, .maximum = 200 };
    try std.testing.expectEqual(reranking.Provider.vertex, reranking.takeCandidateLimitDiagnostic().?.provider);
    inner_scope.deinit();

    try std.testing.expectEqual(reranking.Provider.cohere, reranking.takeCandidateLimitDiagnostic().?.provider);
}
