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

//! Focused native attention-matching tests, independent of full model loading.
test {
    _ = @import("runtime/kv/attention_match.zig");
    _ = @import("runtime/kv/attention_match_study.zig");
    _ = @import("backends/context_attention_match.zig");
    _ = @import("backends/context_native_packed_parity_test.zig");
    _ = @import("backends/packed_context_session.zig");
    _ = @import("backends/compaction_coordinator.zig");
    _ = @import("context_native_head_qualify.zig");
    _ = @import("ops/metal_attention_match_fit.zig");
    _ = @import("ops/metal_attention_match_storage.zig");
    _ = @import("runtime/kv/attention_match_engine.zig");
    _ = @import("runtime/kv/attention_match_checkpoint.zig");
    _ = @import("runtime/kv/attention_match_resume.zig");
    if (@import("build_options").enable_metal) _ = @import("ops/metal_compute.zig");
}
