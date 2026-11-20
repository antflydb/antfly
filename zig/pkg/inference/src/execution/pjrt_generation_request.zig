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

//! One stable execution owner for HTTP and direct PJRT generation. Callers
//! retain the model, generation slot and prepaid media lease until destroy.
const std = @import("std");
const provider = @import("../backends/compute_provider.zig");
const factory = @import("../architectures/session_factory.zig");
const generation = @import("../pipelines/generation.zig");
const Cache = @import("../graph/cache.zig").GraphCache;
const Model = @import("../models/loaded_runtime.zig").Runtime;
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Request = struct {
    allocator: std.mem.Allocator,
    compute: provider.ManagedComputeBackend,
    cache: Cache,
    pipeline: generation.NativeGenerationPipeline,

    pub const Policy = struct {
        media_host_bytes: usize,
        media_bucket_rows: usize = 0,
        decoder_cache: ?bool = null,
    };

    pub fn create(a: std.mem.Allocator, io: std.Io, model: *Model, path: []const u8, policy: Policy, control: Control) !*Request {
        if (model.session.backend() != .pjrt) return error.GenerationBackendSelectionMismatch;
        const client = factory.getPjrtClientPtr(model.session) orelse return error.GenerationBackendSelectionMismatch;
        const config = factory.getGptConfig(model.session) orelse return error.MissingDecoderConfig;
        const self = try a.create(Request);
        errdefer a.destroy(self);
        self.allocator = a;
        self.compute = try provider.acquireWithControl(model.session, a, control);
        errdefer self.compute.deinit();
        var borrowed = control;
        borrowed.hard_cancellation = null;
        borrowed.native_call_policy = .in_process;
        self.compute.backend.execution_control = borrowed;
        self.cache = Cache.init(a);
        self.pipeline = .{
            .allocator = a,
            .io = io,
            .cb = self.compute.backend,
            .execution_control = borrowed,
            .gpt_config = config,
            .session = model.session,
            .tokenizer = model.getTokenizer(),
            .add_bos_token = model.manifest.add_bos_token,
            .bos_token = model.manifest.bos_token,
            .chat_template = model.chat_tmpl,
            .model_dir = path,
            .gguf_projector_path = model.manifest.gguf_projector_path,
            .graph_cache = &self.cache,
            .compiled_partition_backend = .pjrt,
            .compiled_attachment_target = .whole_model,
            .pjrt_client = client,
            .generation_admission = &model.generation_admission,
            .media_prepaid = .{ .process_bytes = policy.media_host_bytes },
            .pjrt_media_bucket_rows = policy.media_bucket_rows,
            .pjrt_decoder_cache = policy.decoder_cache,
        };
        return self;
    }
    pub fn destroy(self: *Request) void {
        self.pipeline.deinit();
        self.cache.deinitWithBackend(&self.compute.backend);
        self.compute.deinit();
        self.allocator.destroy(self);
    }
};
