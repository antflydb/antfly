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

//! Focused dependencies for the embedded inference host. Keep this surface
//! independent of standalone, data, metadata, Raft, and their command roots.

pub const common = struct {
    pub const config = @import("../common/config.zig");
};
pub const db = struct {
    pub const embedder = @import("../storage/db/enrichment/embedder.zig");
};
pub const extracting = @import("antfly_extracting");
pub const inference = @import("../inference/mod.zig");
pub const inference_runtime = @import("../inference_runtime/runtime.zig");
pub const readers = @import("antfly_readers");
pub const resource_manager = @import("../storage/resource_manager.zig");
pub const template = @import("../template.zig");
pub const transcribing = @import("antfly_transcribing");
