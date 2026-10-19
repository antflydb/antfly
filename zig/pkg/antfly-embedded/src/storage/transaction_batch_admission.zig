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

const admission = @import("../common/read_admission.zig");

/// The caller aborts its owned batch on rejection. Every check precedes commit;
/// backend callback internals and accepted commit retain their existing policy.
pub fn apply(batch: anytype, writes: anytype, deletes: anytype, replay: anytype, control: ?admission.Port) !void {
    if (control) |owner| try owner.checkpoint();
    for (deletes) |key| {
        if (control) |owner| try owner.checkpoint();
        batch.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }
    for (writes) |kv| {
        if (control) |owner| try owner.checkpoint();
        try batch.put(kv.key, kv.value);
    }
    if (replay) |entry| {
        if (control) |owner| try owner.checkpoint();
        try batch.setReplayOpaque(entry.sequence, entry.payload);
    }
    if (control) |owner| try owner.checkpoint();
    try batch.commit();
}
