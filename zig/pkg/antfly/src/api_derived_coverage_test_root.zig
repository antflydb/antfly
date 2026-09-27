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

const indexes = @import("api/indexes.zig");
const managed_embedder = @import("inference/managed_embedder.zig");
const coverage_policy = @import("api/coverage_policy.zig");
const runtime_status = @import("api/runtime_status.zig");
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
