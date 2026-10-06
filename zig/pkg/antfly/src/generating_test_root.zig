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

//! Generating implementation and its server-owned agent conversation contracts.
pub const antfly = @import("root.zig");
pub const antfly_sources = antfly.antfly_sources;
pub const local_test_sources = @import("local_test_sources.zig");
const agent_tools = @import("api/agent_tools.zig");
test {
    _ = antfly;
    _ = agent_tools;
}
