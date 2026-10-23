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

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Posting = struct {
    doc_index: u32,
    weight: f32,
};

pub const DocumentEntry = struct {
    doc_id: []u8,
    feature_count: u32,

    pub fn deinit(self: *DocumentEntry, alloc: Allocator) void {
        alloc.free(self.doc_id);
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
    docs: []DocumentEntry,
    terms: []TermEntry,

    pub fn deinit(self: *Segment, alloc: Allocator) void {
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

test "freeSegment releases owned sparse segment data" {
    const alloc = std.testing.allocator;
    var segment = Segment{
        .docs = try alloc.alloc(DocumentEntry, 1),
        .terms = try alloc.alloc(TermEntry, 1),
    };
    segment.docs[0] = .{
        .doc_id = try alloc.dupe(u8, "doc-a"),
        .feature_count = 2,
    };
    segment.terms[0] = .{
        .term = try alloc.dupe(u8, "alpha"),
        .postings = try alloc.alloc(Posting, 1),
    };
    segment.terms[0].postings[0] = .{ .doc_index = 0, .weight = 1.25 };
    freeSegment(alloc, &segment);
}
