// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

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
