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

// Model weight loading and architecture implementations.
//
// Provides:
// - SafeTensors file parser (header + mmap reader)
// - WeightSource abstraction over storage formats
// - Model architecture implementations (BERT, etc.)

pub const safetensors = @import("safetensors.zig");
pub const weight_source = @import("weight_source.zig");
pub const tensor_access = @import("tensor_access.zig");
pub const tensor_store = @import("tensor_store.zig");
pub const gguf = @import("../gguf/root.zig");
pub const bert = @import("bert.zig");
pub const t5 = @import("t5.zig");
pub const gpt = @import("gpt.zig");
pub const whisper = @import("whisper.zig");
pub const florence = @import("florence.zig");
pub const clip = @import("clip.zig");
pub const clap = @import("clap.zig");
pub const manifest = @import("manifest.zig");

test {
    _ = safetensors;
    _ = weight_source;
    _ = tensor_access;
    _ = tensor_store;
    _ = gguf;
    _ = bert;
    _ = t5;
    _ = gpt;
    _ = whisper;
    _ = florence;
    _ = clip;
    _ = clap;
    _ = manifest;
}
