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

pub const data = @import("data.zig");
pub const metadata = @import("metadata.zig");
pub const serverless = @import("serverless.zig");
pub const serverless_api = @import("serverless_api.zig");
pub const serverless_maintenance = @import("serverless_maintenance.zig");
pub const serverless_query = @import("serverless_query.zig");
pub const serverless_combined = @import("serverless_combined.zig");
pub const standalone = @import("standalone.zig");
pub const inference = @import("inference.zig");
pub const standby = @import("standby.zig");
pub const cli = @import("cli/mod.zig");
pub const lite = @import("lite.zig");

test "cmd module compiles" {
    _ = data;
    _ = metadata;
    _ = serverless;
    _ = serverless_api;
    _ = serverless_maintenance;
    _ = serverless_query;
    _ = serverless_combined;
    _ = standalone;
    _ = inference;
    _ = standby;
    _ = cli;
    _ = lite;
}
