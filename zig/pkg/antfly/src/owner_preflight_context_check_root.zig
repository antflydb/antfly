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

pub const antfly_sources = @import("source_owner_physical.zig");
pub const consumer_tests_only = true;
pub const capi_dependencies = @import("capi_dependencies.zig");
test {
    _ = @import("api/kernel_owner_source.zig");
    _ = @import("api/http_server.zig");
    _ = @import("capi/server_owner.zig");
    _ = @import("storage/server_transaction_dispatch.zig");
}
