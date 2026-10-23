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

//! Linked storage fixture with one inline inference provider.
const fixture = @import("standalone_runtime_test_root.zig");
pub const runtime = fixture.runtime;
pub const inference_host = fixture.inference_host;
pub const inference_client = fixture.inference_client;
pub const storage_backend_erased = fixture.storage_backend_erased;
pub const lsm_backend = fixture.lsm_backend;
pub const antfly_sources = fixture.antfly_sources;
pub const local_test_sources = fixture.local_test_sources;

comptime {
    if (@import("antfly_inference_engine").enabled) _ = @import("antfly_inference_native_exports");
}

test {
    _ = fixture;
}
