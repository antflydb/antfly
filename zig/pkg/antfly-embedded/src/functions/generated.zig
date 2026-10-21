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

//! Generated decision schemas and typed response construction. Generation
//! execution and validation/repair are shared with ordinary generator assets.
const std = @import("std");
const d = @import("decisions.zig");
const c = @import("antfly_structured");
const evidence = @import("antfly_structured").token_evidence;
const A = std.mem.Allocator;
const V = std.json.Value;

const shared = @import("antfly_decisions").generated;
pub const questionSchema = shared.questionSchema;

pub fn response(a: A, cfg: d.DeciderConfig, questions: V, result: c.Result, index: usize) !V {
    const value = result.values[index] orelse return error.InvalidDecisionOutput;
    return responseValue(a, cfg, questions, value, result.token_evidence[index], index, if (index == 0) result.input_tokens else 0, if (index == 0) result.output_tokens else 0);
}
pub fn responseValue(a: A, cfg: d.DeciderConfig, questions: V, value: V, proof: ?c.TokenEvidence, index: usize, input_tokens: u64, output_tokens: u64) !V {
    const root = try shared.responseValue(a, .{ .model = cfg.modelName(), .probabilities = cfg.probabilities }, questions, value, proof, index, input_tokens, output_tokens);
    return d.normalizeResponseWithConfig(a, questions, root, cfg);
}
