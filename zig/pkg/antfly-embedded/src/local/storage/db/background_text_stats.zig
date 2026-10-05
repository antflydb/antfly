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

const std = @import("std");
const distributed_stats = @import("../../search/distributed_stats.zig");

pub const DistributedBackgroundTextStats = struct {
    aggregation_name: []const u8,
    field: []const u8,
    background_doc_count: u32 = 0,
    term_doc_freqs: []const distributed_stats.TermDocFreq = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.aggregation_name);
        alloc.free(self.field);
        for (self.term_doc_freqs) |item| alloc.free(item.term);
        if (self.term_doc_freqs.len > 0) alloc.free(self.term_doc_freqs);
        self.* = undefined;
    }
};

pub fn deinitAll(alloc: std.mem.Allocator, items: []const DistributedBackgroundTextStats) void {
    for (items) |item| {
        alloc.free(item.aggregation_name);
        alloc.free(item.field);
        for (item.term_doc_freqs) |term_doc_freq| alloc.free(term_doc_freq.term);
        if (item.term_doc_freqs.len > 0) alloc.free(item.term_doc_freqs);
    }
    if (items.len > 0) alloc.free(items);
}
