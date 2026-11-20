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
