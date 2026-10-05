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

pub const TermDocFreq = struct {
    term: []const u8,
    doc_freq: u32 = 0,

    pub fn clone(self: TermDocFreq, alloc: std.mem.Allocator) !TermDocFreq {
        return .{
            .term = try alloc.dupe(u8, self.term),
            .doc_freq = self.doc_freq,
        };
    }

    pub fn deinit(self: *TermDocFreq, alloc: std.mem.Allocator) void {
        alloc.free(self.term);
        self.* = undefined;
    }
};

pub const TextFieldStats = struct {
    field: []const u8,
    global_doc_count: u32 = 0,
    global_total_field_len: u64 = 0,
    term_doc_freqs: []const TermDocFreq = &.{},

    pub fn clone(self: TextFieldStats, alloc: std.mem.Allocator) !TextFieldStats {
        const cloned_term_doc_freqs = try alloc.alloc(TermDocFreq, self.term_doc_freqs.len);
        var initialized: usize = 0;
        errdefer {
            for (cloned_term_doc_freqs[0..initialized]) |*item| item.deinit(alloc);
            if (cloned_term_doc_freqs.len > 0) alloc.free(cloned_term_doc_freqs);
        }
        for (self.term_doc_freqs, 0..) |item, i| {
            cloned_term_doc_freqs[i] = try item.clone(alloc);
            initialized += 1;
        }

        return .{
            .field = try alloc.dupe(u8, self.field),
            .global_doc_count = self.global_doc_count,
            .global_total_field_len = self.global_total_field_len,
            .term_doc_freqs = cloned_term_doc_freqs,
        };
    }

    pub fn deinit(self: *TextFieldStats, alloc: std.mem.Allocator) void {
        alloc.free(self.field);
        for (self.term_doc_freqs) |tdf| alloc.free(tdf.term);
        if (self.term_doc_freqs.len > 0) alloc.free(self.term_doc_freqs);
        self.* = undefined;
    }

    pub fn avgDocLen(self: TextFieldStats) f32 {
        if (self.global_doc_count == 0) return 0;
        return @as(f32, @floatFromInt(self.global_total_field_len)) /
            @as(f32, @floatFromInt(self.global_doc_count));
    }

    pub fn termDocFreq(self: TextFieldStats, term: []const u8) ?u32 {
        for (self.term_doc_freqs) |item| {
            if (std.mem.eql(u8, item.term, term)) return item.doc_freq;
        }
        return null;
    }
};

pub fn deinitTextFieldStats(alloc: std.mem.Allocator, items: []const TextFieldStats) void {
    for (items) |item| {
        alloc.free(item.field);
        for (item.term_doc_freqs) |tdf| alloc.free(tdf.term);
        if (item.term_doc_freqs.len > 0) alloc.free(item.term_doc_freqs);
    }
    if (items.len > 0) alloc.free(items);
}

pub fn cloneTextFieldStatsSlice(alloc: std.mem.Allocator, items: []const TextFieldStats) ![]const TextFieldStats {
    const out = try alloc.alloc(TextFieldStats, items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    for (items, 0..) |item, i| {
        out[i] = try item.clone(alloc);
        initialized += 1;
    }
    return out;
}
