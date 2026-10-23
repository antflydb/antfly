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

const indexes = @import("api/indexes.zig");
const managed_embedder = @import("antfly_local_sources").inference_managed_embedder;
const coverage_policy = @import("antfly_local_sources").api_coverage_policy;
const runtime_status = @import("antfly_local_sources").api_runtime_status;
const http_server = @import("api/http_server.zig");
const backups = @import("api/backups.zig");

test {
    _ = indexes;
    _ = managed_embedder;
    _ = coverage_policy;
    _ = runtime_status;
    _ = http_server;
    _ = backups;
}

/// Implementation source choices for this compilation root.
pub const antfly_sources = @import("source_owner_physical.zig");

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
