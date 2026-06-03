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
const backends = @import("../backends/backends.zig");
const document_prep = @import("document_preprocessing.zig");
const impl = @import("document_classification_head_impl.zig");
const layoutlmv3 = @import("layoutlmv3_session.zig");

pub const ClassificationResult = impl.ClassificationResult;
pub const SequenceResult = layoutlmv3.SequenceResult;
pub const ExampleInput = impl.SequenceExampleInput;
pub const Features = impl.SequenceFeatures;
pub const Head = impl.SequenceHead;

pub const default_checkpoint_name = impl.default_checkpoint_name;
pub const default_prefix = impl.default_prefix;

pub fn resolveCheckpointPath(allocator: std.mem.Allocator, model_input: []const u8) ![]const u8 {
    return impl.resolveCheckpointPath(allocator, model_input);
}

pub fn extractFeatures(allocator: std.mem.Allocator, input: ExampleInput) !Features {
    return impl.extractFeatures(allocator, input);
}

pub fn classifyWithHead(
    allocator: std.mem.Allocator,
    head: *const Head,
    labels: []const []const u8,
    input: ExampleInput,
) ![]ClassificationResult {
    return impl.classify(allocator, head, labels, input);
}

pub fn classifyPrepared(
    allocator: std.mem.Allocator,
    session: backends.Session,
    prepared: *const document_prep.PreparedInputs,
    labels: []const []const u8,
) ![]SequenceResult {
    return layoutlmv3.classifySequencePrepared(allocator, session, prepared, labels);
}

pub fn summarizeEncoderPrepared(
    allocator: std.mem.Allocator,
    session: backends.Session,
    prepared: *const document_prep.PreparedInputs,
) !layoutlmv3.EncoderOutputSummary {
    return layoutlmv3.summarizeEncoderPrepared(allocator, session, prepared);
}

test {
    _ = impl;
    _ = layoutlmv3;
}
