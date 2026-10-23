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

//! Generating implementation and its server-owned agent conversation contracts.
pub const antfly = @import("root.zig");
pub const antfly_sources = antfly.antfly_sources;
pub const local_test_sources = @import("local_test_sources.zig");
const agent_tools = @import("api/agent_tools.zig");
test {
    _ = antfly;
    _ = agent_tools;
}
