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

test {
    _ = @import("data/history_control_entry.zig");
    _ = @import("data/source_plan_entry.zig");
    _ = @import("data/source_scope_entry.zig");
    _ = @import("data/source_retirement_entry.zig");
    _ = @import("data/source_producer_closure_entry.zig");
    _ = @import("data/source_scope_http_wire.zig");
    _ = @import("api/source_scope_control_operations.zig");
    _ = @import("data/history_control_wire.zig");
    _ = @import("data/raft_batch.zig").consumer_tests;
}
pub const antfly_sources = @import("source_owner_physical.zig");
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};

test "history root control outcome verifies exact accepted scope before metadata settlement" {
    try @import("metadata/root_control_outcome.zig").test_support.contracts();
}

test "history root control driver recovers physical metadata without duplicate submission" {
    try @import("metadata/root_control_driver_test.zig").run();
}

test "history root control observation reconciles only stable scoped predicates" {
    try @import("metadata/root_control_observation.zig").test_support.contracts();
}

test "history root discovery captures authoritative immutable namespaces and complete coverage" {
    try @import("api/history_roots.zig").test_support.contracts();
}

test "history feed metadata port restricts maintenance and recovers durable unknown mutations" {
    try @import("api/changefeed_metadata.zig").test_support.contracts();
}

test "source scope retirement authority obtains fresh original coordinator evidence" {
    try @import("api/source_retirement_authority_test.zig").run();
}
