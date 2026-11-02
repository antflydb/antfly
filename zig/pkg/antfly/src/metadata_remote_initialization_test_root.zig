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

//! Durable first-use grants and their coordinated metadata decoder barrier.
pub const antfly_sources = @import("source_owner_physical.zig");
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
test {
    _ = @import("metadata/remote_attempt_initialization.zig");
    _ = @import("api/remote_initialization.zig");
    _ = @import("metadata/storage/raft_apply_store.zig");
    _ = @import("metadata/service.zig");
    _ = @import("metadata/http_client.zig");
    _ = @import("metadata/http_server.zig");
    _ = @import("system_catalog/operations.zig");
}
