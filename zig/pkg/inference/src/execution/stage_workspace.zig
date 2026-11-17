// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Checked sequential peaks shared by semantic encoder and decoder stages.
const std = @import("std");

pub fn attentionFfn(batch: usize, queries: usize, keys: usize, hidden: usize, heads: usize, ffn: usize) !usize {
    // Attention score/probability storage, FFN intermediates, and K/V/frontend
    // storage are sequential peaks, not one copy per transformer layer.
    const mul = std.math.mul;
    const add = std.math.add;
    const scores = try mul(usize, try mul(usize, try mul(usize, queries, keys), heads), 3);
    const activations = try mul(usize, queries, try add(usize, try mul(usize, hidden, 6), try mul(usize, ffn, 3)));
    const context = try mul(usize, try mul(usize, keys, hidden), 6);
    return mul(usize, try mul(usize, batch, @max(scores, @max(activations, context))), @sizeOf(f32)) catch error.ResourceLimitExceeded;
}
