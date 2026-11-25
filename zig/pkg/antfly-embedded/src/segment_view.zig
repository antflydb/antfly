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

//! What the index reads from one segment: queries, deletions and merge
//! planning go through `SegmentView` rather than the encoded reader. An
//! encoded segment (`segment.SegmentReader`) is the only implementation;
//! another kind of segment, such as an in-memory level-0 buffer, would be
//! another `Impl` variant. Code that works on the encoded bytes themselves
//! (merges, native navigation, integrity checks) asks for `encoded()`.

const std = @import("std");
const Allocator = std.mem.Allocator;
const segment = @import("segment.zig");
const inverted = @import("section/inverted.zig");
const typed_dv = @import("section/typed_doc_values.zig");

pub const SegmentView = struct {
    /// Immutable for the segment's life, so it is kept beside the
    /// implementation: most callers need only this.
    doc_count: u32,
    impl: Impl,

    pub const Impl = union(enum) {
        encoded: segment.SegmentReader,
    };

    pub fn fromEncoded(reader: segment.SegmentReader) SegmentView {
        return .{ .doc_count = reader.doc_count, .impl = .{ .encoded = reader } };
    }

    /// The encoded reader, for code that works on segment bytes.
    pub fn encoded(self: *const SegmentView) *const segment.SegmentReader {
        return switch (self.impl) {
            .encoded => |*reader| reader,
        };
    }

    pub fn encodedMut(self: *SegmentView) *segment.SegmentReader {
        return switch (self.impl) {
            .encoded => |*reader| reader,
        };
    }

    pub fn deinit(self: *SegmentView) void {
        switch (self.impl) {
            .encoded => |*reader| reader.deinit(),
        }
    }

    pub fn invertedIndex(self: *const SegmentView, field_name: []const u8) !?inverted.InvertedIndexReader {
        return switch (self.impl) {
            .encoded => |*reader| reader.invertedIndex(field_name),
        };
    }

    pub fn invertedIndexScoped(self: *const SegmentView, allocator: Allocator, field_name: []const u8) !?inverted.ScopedInvertedIndexReader {
        return switch (self.impl) {
            .encoded => |*reader| reader.invertedIndexScoped(allocator, field_name),
        };
    }

    pub fn invertedIndexScopedWithOptions(self: *const SegmentView, allocator: Allocator, field_name: []const u8, options: inverted.ScopedInvertedIndexReader.Options) !?inverted.ScopedInvertedIndexReader {
        return switch (self.impl) {
            .encoded => |*reader| reader.invertedIndexScopedWithOptions(allocator, field_name, options),
        };
    }

    pub fn invertedFieldStats(self: *const SegmentView, field: []const u8) !?segment.SegmentReader.InvertedFieldStats {
        return switch (self.impl) {
            .encoded => |*reader| reader.invertedFieldStats(field),
        };
    }

    pub fn typedDocValuesScoped(self: *const SegmentView, allocator: Allocator, field: []const u8) !?typed_dv.TypedDocValuesReader {
        return switch (self.impl) {
            .encoded => |*reader| reader.typedDocValuesScoped(allocator, field),
        };
    }

    pub fn hasCheapTypedNavigation(self: *const SegmentView, field: []const u8) bool {
        return switch (self.impl) {
            .encoded => |*reader| reader.hasCheapTypedNavigation(field),
        };
    }

    pub fn hasSection(self: *const SegmentView, name: []const u8, kind: segment.SectionType) bool {
        return switch (self.impl) {
            .encoded => |*reader| reader.hasSection(name, kind),
        };
    }

    pub fn getSection(self: *const SegmentView, field_name: []const u8, section_type: segment.SectionType) !?[]const u8 {
        return switch (self.impl) {
            .encoded => |*reader| reader.getSection(field_name, section_type),
        };
    }

    pub fn storedDoc(self: *const SegmentView, doc_idx: u32) !?segment.SegmentReader.StoredDocRef {
        return switch (self.impl) {
            .encoded => |*reader| reader.storedDoc(doc_idx),
        };
    }

    pub fn storedIdScoped(self: *const SegmentView, alloc: Allocator, doc: u32) !?[]const u8 {
        return switch (self.impl) {
            .encoded => |*reader| reader.storedIdScoped(alloc, doc),
        };
    }

    pub fn storedIdAlloc(self: *const SegmentView, alloc: Allocator, doc: u32) !?[]u8 {
        return switch (self.impl) {
            .encoded => |*reader| reader.storedIdAlloc(alloc, doc),
        };
    }

    pub fn docOrdinal(self: *const SegmentView, doc_idx: u32) !?u32 {
        return switch (self.impl) {
            .encoded => |*reader| reader.docOrdinal(doc_idx),
        };
    }

    pub fn docVersionsAlloc(self: *const SegmentView, alloc: Allocator) !?[]u64 {
        return switch (self.impl) {
            .encoded => |*reader| reader.docVersionsAlloc(alloc),
        };
    }

    pub fn numericRangeCoverage(self: *const SegmentView, field: []const u8) !?segment.NumericRangeCoverage {
        return switch (self.impl) {
            .encoded => |*reader| reader.numericRangeCoverage(field),
        };
    }

    pub fn provesNullIndexSortField(self: *const SegmentView, name: []const u8) !bool {
        return switch (self.impl) {
            .encoded => |*reader| reader.provesNullIndexSortField(name),
        };
    }

    pub fn indexSortBoundsAlloc(self: *const SegmentView, alloc: Allocator) !?segment.SegmentIndexSortBounds {
        return switch (self.impl) {
            .encoded => |*reader| reader.indexSortBoundsAlloc(alloc),
        };
    }

    pub fn typedMergeWorkingSetBytes(self: *const SegmentView) !u64 {
        return switch (self.impl) {
            .encoded => |*reader| reader.typedMergeWorkingSetBytes(),
        };
    }

    pub fn nativeNavigationBytes(self: *const SegmentView) usize {
        return switch (self.impl) {
            .encoded => |*reader| reader.nativeNavigationBytes(),
        };
    }
};

test "segment view forwards to its encoded reader" {
    const alloc = std.testing.allocator;
    var writer = segment.SegmentWriter.init(alloc);
    defer writer.deinit();
    try writer.addStoredDocFromInput("doc-1", "{}");
    var sink_impl = segment.MemorySegmentSink.init(alloc);
    var sink = sink_impl.sink();
    try writer.writeToSink(&sink);
    const bytes = try sink_impl.finishOwned();
    defer alloc.free(bytes);
    var view = SegmentView.fromEncoded(try segment.SegmentReader.init(alloc, bytes));
    defer view.deinit();
    try std.testing.expectEqual(@as(u32, 1), view.doc_count);
    try std.testing.expectEqual(view.encoded().doc_count, view.doc_count);
    const id = (try view.storedIdAlloc(alloc, 0)).?;
    defer alloc.free(id);
    try std.testing.expectEqualStrings("doc-1", id);
}
