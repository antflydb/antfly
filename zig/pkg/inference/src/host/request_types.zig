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

const RequestContext = @import("antfly_inference_execution_control").RequestContext;

pub const EmbeddingTaskType = enum {
    retrieval_query,
    retrieval_document,

    pub fn canonical(self: EmbeddingTaskType) []const u8 {
        return switch (self) {
            .retrieval_query => "RETRIEVAL_QUERY",
            .retrieval_document => "RETRIEVAL_DOCUMENT",
        };
    }
};

pub const EmbeddingRequestContext = struct {
    request: RequestContext,
    task_type: EmbeddingTaskType = .retrieval_document,
    instruction: ?[]const u8 = null,
    model_identity: ?[]const u8 = null,

    pub fn check(self: EmbeddingRequestContext) !void {
        return self.request.check();
    }
};

pub const ClassificationRequest = struct {
    texts: []const []const u8,
    labels: []const []const u8,
    hypothesis_template: ?[]const u8 = null,
    multi_label: bool = false,
};

pub const ClassificationScore = struct {
    label: []const u8,
    score: f32,
};
