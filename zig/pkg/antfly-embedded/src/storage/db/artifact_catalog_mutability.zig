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

//! Shared catalog mutation fence for ordinary DDL and grouped activation.
const std = @import("std");
pub fn requireMutable(alloc: std.mem.Allocator, txn: anytype, namespace: [24]u8, context: ?@import("artifact_reconcile_intent.zig").Context) !void {
    if (context) |value| try @import("artifact_reconcile_intent.zig").requireContext(alloc, txn, value) else try @import("artifact_reconcile_intent.zig").requireAbsent(txn);
    try @import("../source_pin_state.zig").requireNoPrepared(txn, namespace);
    if (try @import("../retained_effects.zig").load(txn)) |retention| {
        if (retention.active() and std.mem.eql(u8, &retention.namespace, &namespace))
            return error.IntegrityTopologyBusy;
    }
    // A receiver has no retained source record. Its immutable copy/tail
    // contract is instead owned by the durable merge checkpoint. This
    // also closes ordinary merge DDL races, without scanning any rows.
    const merge = @import("merge_state.zig");
    const merge_raw = txn.get(merge.key) catch |err| switch (err) {
        error.NotFound => txn.get(merge.legacy_key) catch |legacy_err| switch (legacy_err) {
            error.NotFound => null,
            else => return legacy_err,
        },
        else => return err,
    };
    if (merge_raw) |raw| {
        var state = try merge.decodeAlloc(alloc, raw);
        defer state.deinit(alloc);
        if (state.phase != .none and state.phase != .finalized and state.phase != .rolled_back)
            return error.IntegrityTopologyBusy;
    }
}
