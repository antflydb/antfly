// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
