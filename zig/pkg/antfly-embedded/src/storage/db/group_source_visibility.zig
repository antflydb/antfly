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

//! A visibility waiter drives only snapshots belonging to an original source
//! cause. Publication can append past its source sequence, but an unrelated
//! later append must never become the waiter's target.
const std = @import("std");
const index = @import("group_source_index.zig");
const state = @import("group_source_state.zig");
pub fn producerSequence(a: std.mem.Allocator, txn: anytype, owner: state.Owner, event: index.Invalidation) !u64 {
    const group = state.Group{ .definition = event.definition, .key = event.partition.group };
    const propagation = @import("group_source_propagation.zig");
    _ = (try propagation.currentProgress(a, txn, owner, event)) orelse return 0;
    var sequence: u64 = 0;
    for (try @import("group_source_causes.zig").page(a, txn, group, try propagation.causeId(a, owner, event))) |entry| {
        const dependency = entry.dependency;
        if (try state.settled(a, txn, group, dependency.partition, dependency.revision)) continue;
        if (try state.readSnapshot(a, txn, group, dependency.partition)) |snapshot| {
            if (snapshot.revision < dependency.revision) continue;
            const pending = (try state.pendingFor(a, txn, group, dependency.partition)) orelse continue;
            if (snapshot.revision == pending.revision) sequence = @max(sequence, snapshot.sequence);
        }
    }
    return sequence;
}
