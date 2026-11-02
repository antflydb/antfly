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

//! Host discovery root: API production code is supplied by the linked archive.
pub const antfly_test_use_compiled_api_kernel = true;
pub const antfly_sources = @import("source_owner_control.zig");
pub const local_test_sources = @import("local_test_sources.zig");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;

test {
    _ = @import("api/kernel_bridge.zig");
    _ = @import("api/compiled_api_pgwire_test.zig");
}

test "API kernel linked consumer selects the opaque production bridge" {
    comptime {
        if (@import("api/kernel_bridge.zig").ApiHttpServer == @import("api/http_server.zig").ApiHttpServer)
            @compileError("linked API consumer must use opaque handles and the external table");
    }
}
