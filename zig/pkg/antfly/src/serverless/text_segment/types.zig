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
const Allocator = std.mem.Allocator;

pub const Posting = struct {
    doc_index: u32,
    term_freq: u32,
};

pub const DocumentEntry = struct {
    doc_id: []u8,
    normalized_text: []u8,
    token_count: u32,

    pub fn deinit(self: *DocumentEntry, alloc: Allocator) void {
        alloc.free(self.doc_id);
        alloc.free(self.normalized_text);
        self.* = undefined;
    }
};

pub const TermEntry = struct {
    term: []u8,
    postings: []Posting,

    pub fn deinit(self: *TermEntry, alloc: Allocator) void {
        alloc.free(self.term);
        alloc.free(self.postings);
        self.* = undefined;
    }
};

pub const Segment = struct {
    index_name: []u8 = &.{},
    source_name: []u8 = &.{},
    config_json: []u8 = &.{},
    docs: []DocumentEntry,
    terms: []TermEntry,

    pub fn deinit(self: *Segment, alloc: Allocator) void {
        if (self.index_name.len > 0) alloc.free(self.index_name);
        if (self.source_name.len > 0) alloc.free(self.source_name);
        if (self.config_json.len > 0) alloc.free(self.config_json);
        for (self.docs) |*doc| doc.deinit(alloc);
        alloc.free(self.docs);
        for (self.terms) |*term| term.deinit(alloc);
        alloc.free(self.terms);
        self.* = undefined;
    }
};

pub fn freeSegment(alloc: Allocator, segment: *Segment) void {
    segment.deinit(alloc);
}

test "freeSegment releases owned text segment data" {
    const alloc = std.testing.allocator;
    var segment = Segment{
        .index_name = try alloc.dupe(u8, "full_text_index_v0"),
        .source_name = try alloc.dupe(u8, "text"),
        .config_json = try alloc.dupe(u8, "{}"),
        .docs = try alloc.alloc(DocumentEntry, 1),
        .terms = try alloc.alloc(TermEntry, 1),
    };
    segment.docs[0] = .{
        .doc_id = try alloc.dupe(u8, "doc-a"),
        .normalized_text = try alloc.dupe(u8, "alpha bravo"),
        .token_count = 2,
    };
    segment.terms[0] = .{
        .term = try alloc.dupe(u8, "alpha"),
        .postings = try alloc.alloc(Posting, 1),
    };
    segment.terms[0].postings[0] = .{ .doc_index = 0, .term_freq = 1 };
    freeSegment(alloc, &segment);
}
