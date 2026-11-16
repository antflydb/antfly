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

//! Focused ownership and arithmetic-routing tests; no device or model load.
test {
    _ = @import("pipelines/generation.zig");
    _ = @import("runtime/scheduler/native_generate.zig");
    _ = @import("ops/execution_batch.zig");
    _ = @import("ops/execution_scope.zig");
    _ = @import("ops/cuda/cuda_compute.zig");
}
