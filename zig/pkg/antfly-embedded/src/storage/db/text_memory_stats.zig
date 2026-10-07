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

//! Value-only text memory metrics shared with control-plane observability.

const std = @import("std");

pub const TextMemoryAttributionStats = struct {
    text_indexes: u64 = 0,
    text_segments: u64 = 0,
    text_segment_bytes: u64 = 0,
    text_mmap_segment_bytes: u64 = 0,
    text_heap_segment_bytes: u64 = 0,
    text_native_segment_bytes: u64 = 0,
    text_native_navigation_bytes: u64 = 0,
    text_max_segment_bytes: u64 = 0,
    stored_fields_bytes: u64 = 0,
    inverted_text_bytes: u64 = 0,
    inverted_header_bytes: u64 = 0,
    inverted_norm_bytes: u64 = 0,
    inverted_term_dict_bytes: u64 = 0,
    inverted_term_block_bytes: u64 = 0,
    inverted_term_index_bytes: u64 = 0,
    inverted_fst_bytes: u64 = 0,
    inverted_bloom_bytes: u64 = 0,
    inverted_postings_bytes: u64 = 0,
    inverted_postings_header_bytes: u64 = 0,
    inverted_block_max_bytes: u64 = 0,
    inverted_chunk_meta_bytes: u64 = 0,
    inverted_postings_payload_bytes: u64 = 0,
    inverted_positions_bytes: u64 = 0,
    inverted_skip_bytes: u64 = 0,
    inverted_one_hit_terms: u64 = 0,
    inverted_single_doc_postings_terms: u64 = 0,
    inverted_postings_terms: u64 = 0,
    inverted_postings_doc_frequency_total: u64 = 0,
    inverted_projected_posting_count_blocks_64: u64 = 0,
    inverted_projected_posting_count_blocks_128: u64 = 0,
    inverted_projected_posting_count_blocks_256: u64 = 0,
    typed_doc_values_bytes: u64 = 0,
    doc_ordinals_bytes: u64 = 0,
    section_index_bytes: u64 = 0,
    text_segment_estimated_resident_bytes: u64 = 0,
    text_segment_recently_touched_bytes: u64 = 0,
    text_segment_cold_mapped_bytes: u64 = 0,
    text_segment_residency_evictions: u64 = 0,

    pub fn accumulate(self: *@This(), other: @This()) void {
        inline for (comptime std.meta.fieldNames(@This())) |reflected_name| {
            if (comptime std.mem.eql(u8, reflected_name, "text_max_segment_bytes")) {
                @field(self, reflected_name) = @max(@field(self, reflected_name), @field(other, reflected_name));
            } else {
                @field(self, reflected_name) +|= @field(other, reflected_name);
            }
        }
    }
};

test "text memory attribution aggregation preserves totals and maximums" {
    var stats: TextMemoryAttributionStats = .{
        .text_segments = 2,
        .text_segment_bytes = 100,
        .text_max_segment_bytes = 80,
        .inverted_norm_bytes = 11,
        .text_segment_residency_evictions = 3,
    };
    stats.accumulate(.{
        .text_segments = 3,
        .text_segment_bytes = 120,
        .text_max_segment_bytes = 60,
        .inverted_norm_bytes = 13,
        .text_segment_residency_evictions = 5,
    });

    try std.testing.expectEqual(@as(u64, 5), stats.text_segments);
    try std.testing.expectEqual(@as(u64, 220), stats.text_segment_bytes);
    try std.testing.expectEqual(@as(u64, 80), stats.text_max_segment_bytes);
    try std.testing.expectEqual(@as(u64, 24), stats.inverted_norm_bytes);
    try std.testing.expectEqual(@as(u64, 8), stats.text_segment_residency_evictions);
}
