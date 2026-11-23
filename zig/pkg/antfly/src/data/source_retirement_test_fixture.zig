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

//! Modeled positive coordinator evidence for codec/apply tests only. Production
//! proposal construction must obtain this through an independent qualified read.
const std = @import("std");
const local = @import("antfly_local_sources");
const completion = local.storage_transaction_completion;
const retirement = local.storage_source_scope_retirement;
pub const Fixture = struct {
    members: [1]completion.Participant = undefined,
    command: retirement.Command = undefined,
    pub fn init(self: *Fixture, owner: completion.RoutedOwner) !void {
        self.members = .{.{ .namespace = owner.namespace, .owner_fence = owner.digest(), .roles = completion.Roles.checkpoint, .input_digest = @splat(5), .locator = "checkpoint", .routed_owner = owner }};
        const admission: completion.Admission = .{ .version = 3, .transaction_id = @splat(9), .admission_digest = @splat(11), .coordinator_namespace = owner.namespace, .coordinator_fence = owner.digest(), .participants = &self.members };
        const query: @FieldType(retirement.Intent, "coordinator") = .{ .owner = owner, .transaction_id = admission.transaction_id, .minimum_applied_index = 1 };
        const reference: @FieldType(retirement.Intent, "reference") = .{ .domain = @splat(71), .binding = @splat(72), .generation = 1, .token = 10, .sequence = 1 };
        self.command = .{ .version = 1, .intent = .{ .version = 1, .owner = owner, .coordinator = query, .reference = reference, .source_purpose = @splat(12), .admission_digest = admission.digest() }, .observation = .{ .request_digest = query.digest(), .cut = .{ .term = 1, .index = 6 }, .decision = .{ .record = .{ .status = .aborted, .begin_timestamp = 10, .created_at_ns = 11, .commit_version = 0, .finalized_at_ns = 20, .prepared = true, .intents_resolved = true, .digest = @splat(13), .source_purpose = @splat(12), .source_scope = reference }, .admission = .{ .admission = admission, .digest = admission.digest() } } } };
        _ = try self.command.proof();
    }
};
