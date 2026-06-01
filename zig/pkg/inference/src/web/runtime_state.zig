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

const bert_arch = @import("../architectures/bert.zig");
const gguf_mod = @import("../gguf/root.zig");
const clap_config = @import("../models/clap.zig");
const clip_config = @import("../models/clip.zig");
const deberta_config = @import("../models/deberta.zig");
const florence_config = @import("../models/florence.zig");
const gpt_config = @import("../models/gpt.zig");
const projector_format_mod = @import("../architectures/projector_format.zig");
const t5_config = @import("../models/t5.zig");
const projector_store_mod = @import("projector_store.zig");
const whisper_config = @import("../models/whisper.zig");
const wasm_compute = @import("../ops/wasm_compute.zig");
const hf_tokenizer_mod = @import("inference_hf_tokenizer");

pub const max_models = 16;
pub const max_projectors = 16;
pub const max_tokenizers = 16;

pub const ModelConfig = union(enum) {
    bert: bert_arch.Config,
    clap: clap_config.Config,
    clip: clip_config.Config,
    deberta: deberta_config.Config,
    florence: florence_config.Config,
    gpt: gpt_config.Config,
    t5: t5_config.Config,
    whisper: whisper_config.Config,
};

pub const Model = struct {
    compute: wasm_compute.WasmCompute,
    config: ModelConfig,
};

pub const Projector = struct {
    store: *projector_store_mod.ProjectorStore,
    kind: projector_format_mod.Kind,
};

pub const Runtime = struct {
    models: [max_models]?Model = [_]?Model{null} ** max_models,
    projectors: [max_projectors]?Projector = [_]?Projector{null} ** max_projectors,
    tokenizers: [max_tokenizers]?*hf_tokenizer_mod.HfTokenizer = [_]?*hf_tokenizer_mod.HfTokenizer{null} ** max_tokenizers,

    pub fn storeModel(self: *Runtime, compute: wasm_compute.WasmCompute, config: ModelConfig) !u32 {
        for (&self.models, 1..) |*slot, handle| {
            if (slot.* == null) {
                slot.* = .{ .compute = compute, .config = config };
                return @intCast(handle);
            }
        }
        return error.TooManyModels;
    }

    pub fn getModel(self: *Runtime, handle: u32) !*Model {
        if (handle == 0 or handle > max_models) return error.InvalidHandle;
        return &(self.models[handle - 1] orelse return error.InvalidHandle);
    }

    pub fn unloadModel(self: *Runtime, handle: u32) void {
        if (handle == 0 or handle > max_models) return;
        if (self.models[handle - 1]) |*model| {
            var cb = model.compute.computeBackend();
            cb.deinit();
            self.models[handle - 1] = null;
        }
    }

    pub fn storeProjector(self: *Runtime, store: *projector_store_mod.ProjectorStore, kind: projector_format_mod.Kind) !u32 {
        for (&self.projectors, 1..) |*slot, handle| {
            if (slot.* == null) {
                slot.* = .{ .store = store, .kind = kind };
                return @intCast(handle);
            }
        }
        return error.TooManyProjectors;
    }

    pub fn getProjector(self: *Runtime, handle: u32) !*Projector {
        if (handle == 0 or handle > max_projectors) return error.InvalidHandle;
        return &(self.projectors[handle - 1] orelse return error.InvalidHandle);
    }

    pub fn unloadProjector(self: *Runtime, handle: u32) void {
        if (handle == 0 or handle > max_projectors) return;
        if (self.projectors[handle - 1]) |*projector| {
            projector.store.deinit();
            self.projectors[handle - 1] = null;
        }
    }

    pub fn storeTokenizer(self: *Runtime, tokenizer: *hf_tokenizer_mod.HfTokenizer) !u32 {
        for (&self.tokenizers, 1..) |*slot, handle| {
            if (slot.* == null) {
                slot.* = tokenizer;
                return @intCast(handle);
            }
        }
        return error.TooManyTokenizers;
    }

    pub fn getTokenizer(self: *Runtime, handle: u32) !*hf_tokenizer_mod.HfTokenizer {
        if (handle == 0 or handle > max_tokenizers) return error.InvalidHandle;
        return self.tokenizers[handle - 1] orelse return error.InvalidHandle;
    }

    pub fn unloadTokenizer(self: *Runtime, handle: u32) void {
        if (handle == 0 or handle > max_tokenizers) return;
        if (self.tokenizers[handle - 1]) |tokenizer| {
            tokenizer.deinitSelf();
            self.tokenizers[handle - 1] = null;
        }
    }
};

fn projectorFixtureBytes(allocator: @import("std").mem.Allocator) ![]u8 {
    const metadata = &[_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.vision.projector_type", .value = .{ .string = "gemma4v" } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, metadata, &.{});
    defer layout.deinit(allocator);
    return allocator.dupe(u8, layout.header_bytes);
}

test "runtime stores and unloads projector handles" {
    const allocator = @import("std").testing.allocator;
    const bytes = try projectorFixtureBytes(allocator);
    defer allocator.free(bytes);

    var runtime = Runtime{};
    const store = try projector_store_mod.ProjectorStore.initOwnedBytes(allocator, "fixture.mmproj.gguf", bytes);
    const handle = try runtime.storeProjector(store, store.kind);
    try @import("std").testing.expectEqual(@as(u32, 1), handle);
    try @import("std").testing.expectEqual(projector_format_mod.Kind.clip_gemma4_image, (try runtime.getProjector(handle)).kind);

    runtime.unloadProjector(handle);
    try @import("std").testing.expectError(error.InvalidHandle, runtime.getProjector(handle));
}
