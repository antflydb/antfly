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

//! Offline checkpoint and adapter operations. This root imports neither the
//! inference runtime facade nor its backend links and generated identities.
//! The combined CLI reuses these declarations within its existing module.
pub const io = struct {
    pub const compat = @import("io/compat.zig");
};
pub const finetune = struct {
    pub const peft = @import("finetune/peft.zig");
    pub const gliner2 = @import("finetune/gliner2.zig");
    pub const gemma4 = @import("finetune/gemma4.zig");
    pub const colqwen2 = @import("finetune/colqwen2.zig");
    pub const layoutlmv3 = @import("finetune/layoutlmv3.zig");
    pub const reranker_head = @import("finetune/reranker_head.zig");
    pub const reranker_lora = @import("finetune/reranker_lora.zig");
    pub const gliner2_run_validation = @import("finetune/gliner2_run_validation.zig");
};
