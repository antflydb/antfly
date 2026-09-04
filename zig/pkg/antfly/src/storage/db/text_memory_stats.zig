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

//! Value-only text memory metrics shared with control-plane observability.

const std = @import("std");

pub const TextMemoryAttributionStats = struct {
    text_indexes: u64 = 0,
    text_segments: u64 = 0,
    text_segment_bytes: u64 = 0,
    text_mmap_segment_bytes: u64 = 0,
    text_heap_segment_bytes: u64 = 0,
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
    configured_lmdb_main_map_bytes: u64 = 0,
    configured_lmdb_wal_map_bytes: u64 = 0,
    text_segment_estimated_resident_bytes: u64 = 0,
    text_segment_recently_touched_bytes: u64 = 0,
    text_segment_cold_mapped_bytes: u64 = 0,
    text_segment_residency_evictions: u64 = 0,

    pub fn accumulate(self: *@This(), other: @This()) void {
        inline for (@typeInfo(@This()).@"struct".fields) |field| {
            if (comptime std.mem.eql(u8, field.name, "text_max_segment_bytes")) {
                @field(self, field.name) = @max(@field(self, field.name), @field(other, field.name));
            } else {
                @field(self, field.name) +|= @field(other, field.name);
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
