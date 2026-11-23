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

//! WS5: a minimal consumer-registry gate for hinted replay lanes.
//!
//! A durable, catalog-backed consumer registry is WS7's job. Until it
//! lands, presence is derived on demand from the same consumer-existence
//! checks retirement safety already walks (managed indexes, generated
//! enrichment, resolution/promotion runtimes, the replication outbox, hot
//! standby, repair pins) and cached here as a bitmask that gates which
//! hinted lanes an append actually writes. See
//! `mutation_commit.truncateReplayJournalIfSafeContext`, which recomputes it
//! on every retirement-safety pass and publishes it onto the `DocStore` /
//! lite `Store` the writer is about to charge.
const change_journal_mod = @import("change_journal.zig");
const TargetHint = change_journal_mod.TargetHint;

/// Every hint has a consumer: the conservative, always-correct default. A
/// writer that has never been told otherwise (fresh store, or the DB layer
/// not wired to call `Presence.toMask` yet) must keep writing every lane --
/// silently dropping a lane a consumer still needs is a correctness bug, not
/// a performance one.
pub const all_hints_mask: u8 = blk: {
    var mask: u8 = 0;
    for (@typeInfo(TargetHint).@"enum".field_values) |field_value| {
        const hint: TargetHint = @fromBackingInt(@intCast(field_value));
        mask |= change_journal_mod.singleHintMask(hint);
    }
    break :blk mask;
};

/// Per-hint consumer existence, derived from the DB layer's catalog. Fields
/// default to `true` so a caller that only knows about some hints cannot
/// accidentally silence the rest.
pub const Presence = struct {
    enrichment: bool = true,
    full_text: bool = true,
    dense_vector: bool = true,
    sparse_vector: bool = true,
    graph: bool = true,
    algebraic: bool = true,
    resolution: bool = true,
    promotion: bool = true,

    pub fn toMask(self: Presence) u8 {
        var mask: u8 = 0;
        if (self.enrichment) mask |= change_journal_mod.singleHintMask(.enrichment);
        if (self.full_text) mask |= change_journal_mod.singleHintMask(.full_text);
        if (self.dense_vector) mask |= change_journal_mod.singleHintMask(.dense_vector);
        if (self.sparse_vector) mask |= change_journal_mod.singleHintMask(.sparse_vector);
        if (self.graph) mask |= change_journal_mod.singleHintMask(.graph);
        if (self.algebraic) mask |= change_journal_mod.singleHintMask(.algebraic);
        if (self.resolution) mask |= change_journal_mod.singleHintMask(.resolution);
        if (self.promotion) mask |= change_journal_mod.singleHintMask(.promotion);
        return mask;
    }
};

const std = @import("std");

test "all_hints_mask covers every TargetHint exactly once" {
    try std.testing.expectEqual(@as(u8, 0xff), all_hints_mask);
}

test "Presence.toMask reflects only the hints marked present" {
    var presence = Presence{
        .enrichment = false,
        .full_text = true,
        .dense_vector = false,
        .sparse_vector = false,
        .graph = false,
        .algebraic = false,
        .resolution = false,
        .promotion = false,
    };
    try std.testing.expectEqual(change_journal_mod.singleHintMask(.full_text), presence.toMask());

    presence.resolution = true;
    try std.testing.expectEqual(
        change_journal_mod.singleHintMask(.full_text) | change_journal_mod.singleHintMask(.resolution),
        presence.toMask(),
    );

    try std.testing.expectEqual(all_hints_mask, (Presence{}).toMask());
}
