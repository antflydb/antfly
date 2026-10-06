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

pub const runtime = @import("standalone/runtime.zig");
pub const inference_host = @import("antfly_inference_host");
pub const inference_client = @import("standalone/inference_client.zig");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend_mod;

test {
    _ = @import("antfly_inference_worker_rpc");
    _ = @import("antfly_inference_worker_wire");
    _ = @import("antfly_inference_host").worker_module;
    _ = @import("antfly_inference_provider_failure");
    _ = runtime;
    _ = inference_host;
    _ = inference_client;
    _ = storage_backend_erased;
    _ = lsm_backend;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
