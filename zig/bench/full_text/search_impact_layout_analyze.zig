// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const antfly = @import("antfly-zig");

const Totals = struct {
    segments: u64 = 0,
    segment_bytes: u64 = 0,
    documents: u64 = 0,
    stored_fields_bytes: u64 = 0,
    inverted_text_bytes: u64 = 0,
    inverted_header_bytes: u64 = 0,
    inverted_norm_bytes: u64 = 0,
    inverted_term_dict_bytes: u64 = 0,
    inverted_bloom_bytes: u64 = 0,
    inverted_postings_bytes: u64 = 0,
    inverted_positions_bytes: u64 = 0,
    typed_doc_values_bytes: u64 = 0,
    doc_ordinals_bytes: u64 = 0,
    index_sort_bytes: u64 = 0,
    index_sort_bounds_bytes: u64 = 0,
    other_section_bytes: u64 = 0,
    section_index_bytes: u64 = 0,
    postings_terms: u64 = 0,
    current_header_bytes: u64 = 0,
    projected_header_bytes: u64 = 0,
    impact_records: u64 = 0,
    current_impact_bytes: u64 = 0,
    impact_range_id_bytes: u64 = 0,
    projected_impact_bytes: u64 = 0,
    adaptive_terms: u64 = 0,
    raw_terms: u64 = 0,
    descriptor_header_delta: i64 = 0,
};

fn fileLength(fd: std.posix.fd_t) !usize {
    const size = std.posix.system.lseek(fd, 0, std.posix.SEEK.END);
    if (size < 0) return error.Unexpected;
    return std.math.cast(usize, size) orelse error.SizeOverflow;
}

fn analyzeSegment(alloc: std.mem.Allocator, path: []const u8, totals: *Totals) !void {
    const fd = try std.posix.openat(std.posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    defer _ = std.posix.system.close(fd);
    const len = try fileLength(fd);
    if (len == 0) return error.EmptySegment;
    const mapped = try std.posix.mmap(null, len, .{ .READ = true }, .{ .TYPE = .SHARED }, fd, 0);
    defer std.posix.munmap(mapped);
    try std.posix.madvise(mapped.ptr, mapped.len, std.posix.MADV.SEQUENTIAL);

    var segment = try antfly.segment.SegmentReader.init(alloc, mapped);
    defer segment.deinit();

    const layout = segment.layoutStats();
    totals.segments += 1;
    totals.segment_bytes +|= len;
    totals.documents +|= segment.doc_count;
    totals.stored_fields_bytes +|= layout.stored_fields_bytes;
    totals.inverted_text_bytes +|= layout.inverted_text_bytes;
    totals.inverted_header_bytes +|= layout.inverted_header_bytes;
    totals.inverted_norm_bytes +|= layout.inverted_norm_bytes;
    totals.inverted_term_dict_bytes +|= layout.inverted_term_dict_bytes;
    totals.inverted_bloom_bytes +|= layout.inverted_bloom_bytes;
    totals.inverted_postings_bytes +|= layout.inverted_postings_bytes;
    totals.inverted_positions_bytes +|= layout.inverted_positions_bytes;
    totals.typed_doc_values_bytes +|= layout.typed_doc_values_bytes;
    totals.doc_ordinals_bytes +|= layout.doc_ordinals_bytes;
    totals.index_sort_bytes +|= layout.index_sort_bytes;
    totals.index_sort_bounds_bytes +|= layout.index_sort_bounds_bytes;
    totals.other_section_bytes +|= layout.other_section_bytes;
    totals.section_index_bytes +|= layout.section_index_bytes;

    // Kernel artifacts name the compared field `text`; the production server
    // fixture names the same logical field `body`. Keep the detailed impact
    // projection focused on one declared benchmark field while always
    // reporting the format-neutral section totals above.
    const inverted = (try segment.invertedIndex("body")) orelse
        (try segment.invertedIndex("text")) orelse return;
    const stats = try inverted.detailedLayoutStats();
    totals.inverted_positions_bytes +|= stats.positions_bytes;
    totals.postings_terms +|= stats.postings_terms;
    totals.current_header_bytes +|= stats.postings_header_bytes;
    totals.projected_header_bytes +|= stats.projected_compact_postings_header_bytes;
    totals.impact_records +|= stats.impact_record_count;
    totals.current_impact_bytes +|= stats.block_max_bytes;
    totals.impact_range_id_bytes +|= stats.impact_range_id_bytes;
    totals.projected_impact_bytes +|= stats.projected_adaptive_impact_bytes;
    totals.adaptive_terms +|= stats.projected_adaptive_impact_terms;
    totals.raw_terms +|= stats.projected_raw_impact_terms;
    totals.descriptor_header_delta += stats.projected_impact_descriptor_header_delta;
}

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    var totals = Totals{};
    while (args.next()) |path| try analyzeSegment(alloc, path, &totals);
    if (totals.segments == 0) return error.MissingSegmentPath;

    const projected_total = @as(i128, totals.projected_impact_bytes) + totals.descriptor_header_delta;
    const impact_saving = @as(i128, totals.current_impact_bytes) - projected_total;
    const header_saving = totals.current_header_bytes -| totals.projected_header_bytes;
    const saving = impact_saving + @as(i128, header_saving);
    const accounted_bytes = totals.stored_fields_bytes +| totals.inverted_text_bytes +|
        totals.typed_doc_values_bytes +| totals.doc_ordinals_bytes +|
        totals.index_sort_bytes +| totals.index_sort_bounds_bytes +|
        totals.other_section_bytes +| totals.section_index_bytes;
    const unattributed_bytes = totals.segment_bytes -| accounted_bytes;
    var stdout_buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(init.io, &stdout_buffer);
    defer stdout.interface.flush() catch {};
    try stdout.interface.print(
        "{{\"segments\":{d},\"segment_bytes\":{d},\"documents\":{d},\"stored_fields_bytes\":{d},\"inverted_text_bytes\":{d},\"inverted_header_bytes\":{d},\"inverted_norm_bytes\":{d},\"inverted_term_dict_bytes\":{d},\"inverted_bloom_bytes\":{d},\"inverted_postings_bytes\":{d},\"inverted_positions_bytes\":{d},\"typed_doc_values_bytes\":{d},\"doc_ordinals_bytes\":{d},\"index_sort_bytes\":{d},\"index_sort_bounds_bytes\":{d},\"other_section_bytes\":{d},\"section_index_bytes\":{d},\"accounted_bytes\":{d},\"unattributed_bytes\":{d},\"postings_terms\":{d},\"current_header_bytes\":{d},\"projected_header_bytes\":{d},\"projected_header_saving_bytes\":{d},\"impact_records\":{d},\"current_impact_bytes\":{d},\"impact_range_id_bytes\":{d},\"projected_impact_bytes\":{d},\"adaptive_terms\":{d},\"raw_terms\":{d},\"descriptor_header_delta\":{d},\"projected_net_saving_bytes\":{d}}}\n",
        .{ totals.segments, totals.segment_bytes, totals.documents, totals.stored_fields_bytes, totals.inverted_text_bytes, totals.inverted_header_bytes, totals.inverted_norm_bytes, totals.inverted_term_dict_bytes, totals.inverted_bloom_bytes, totals.inverted_postings_bytes, totals.inverted_positions_bytes, totals.typed_doc_values_bytes, totals.doc_ordinals_bytes, totals.index_sort_bytes, totals.index_sort_bounds_bytes, totals.other_section_bytes, totals.section_index_bytes, accounted_bytes, unattributed_bytes, totals.postings_terms, totals.current_header_bytes, totals.projected_header_bytes, header_saving, totals.impact_records, totals.current_impact_bytes, totals.impact_range_id_bytes, totals.projected_impact_bytes, totals.adaptive_terms, totals.raw_terms, totals.descriptor_header_delta, saving },
    );
}
