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

//! Backend-neutral identity of the one Gemma 4 A4B geometry qualified for
//! bounded Metal inference. Artifact layout and quantization are additional
//! gates enforced by the GGUF inspector and session factory.

pub const architecture = "gemma4";
pub const gguf_file_type: u32 = 2;
pub const moe_layer_count: u16 = 30;
pub const expert_count: u16 = 128;
pub const top_k: u8 = 8;
pub const hidden_size: u32 = 2816;
pub const expert_intermediate_size: u32 = 704;
pub const encoded_expert_bytes: u64 = 3_345_408;

pub fn matchesGeometry(
    layers: u32,
    experts: u32,
    experts_per_token: u32,
    hidden: u32,
    expert_intermediate: u32,
) bool {
    return layers == @as(u32, moe_layer_count) and
        experts == @as(u32, expert_count) and
        experts_per_token == @as(u32, top_k) and
        hidden == hidden_size and
        expert_intermediate == expert_intermediate_size;
}
