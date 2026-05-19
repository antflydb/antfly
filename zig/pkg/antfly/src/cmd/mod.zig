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

pub const data = @import("data.zig");
pub const metadata = @import("metadata.zig");
pub const serverless = @import("serverless.zig");
pub const serverless_api = @import("serverless_api.zig");
pub const serverless_maintenance = @import("serverless_maintenance.zig");
pub const serverless_query = @import("serverless_query.zig");
pub const serverless_swarm = @import("serverless_swarm.zig");
pub const swarm = @import("swarm.zig");
pub const termite = @import("termite.zig");
pub const cli = @import("cli/mod.zig");

test "cmd module compiles" {
    _ = data;
    _ = metadata;
    _ = serverless;
    _ = serverless_api;
    _ = serverless_maintenance;
    _ = serverless_query;
    _ = serverless_swarm;
    _ = swarm;
    _ = termite;
    _ = cli;
}
