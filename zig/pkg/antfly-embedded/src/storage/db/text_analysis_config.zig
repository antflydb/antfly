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

//! Schema/config composition shared by control and physical query units. This
//! module deliberately owns no index implementation or storage backend.

const std = @import("std");
const introducer = @import("../../introducer.zig");
const schema_mod = @import("../schema.zig");
const FieldAnalyzer = std.meta.Child(@FieldType(introducer.TextAnalysisConfig, "field_analyzers"));

pub fn parseForIndexConfig(
    alloc: std.mem.Allocator,
    raw: []const u8,
    runtime_schema: ?schema_mod.TableSchema,
) !introducer.TextAnalysisConfig {
    var cfg = try introducer.parseTextAnalysisConfig(alloc, raw);
    errdefer introducer.freeTextAnalysisConfig(alloc, cfg);

    if (runtime_schema) |schema| try appendSchemaFieldAnalyzers(alloc, &cfg, schema);
    return cfg;
}

/// Keep explicit configuration authoritative. Inherited schema/observed
/// mappings may retain ambiguity across document types, but cannot contradict
/// the configured analyzer used by physical tokenization. Commit ownership
/// only after every allocation succeeds.
fn appendFallbacks(alloc: std.mem.Allocator, cfg: *introducer.TextAnalysisConfig, source: anytype) !void {
    var overrides: std.StringHashMapUnmanaged(void) = .empty;
    defer overrides.deinit(alloc);
    for (cfg.field_analyzers) |entry| try overrides.put(alloc, entry.field_name, {});
    var added: std.ArrayList(FieldAnalyzer) = .empty;
    defer added.deinit(alloc);
    var transferred = false;
    defer if (!transferred) {
        for (added.items) |entry| {
            alloc.free(entry.field_name);
            alloc.free(entry.analyzer_name);
        }
    };
    var cursor = source;
    while (cursor.next()) |entry| {
        if (overrides.contains(entry.field_name)) continue;
        const field = try alloc.dupe(u8, entry.field_name);
        errdefer alloc.free(field);
        const analyzer = try alloc.dupe(u8, entry.analyzer_name);
        errdefer alloc.free(analyzer);
        try added.append(alloc, .{ .field_name = field, .analyzer_name = analyzer });
    }
    if (added.items.len == 0) return;
    const combined = try alloc.alloc(FieldAnalyzer, cfg.field_analyzers.len + added.items.len);
    @memcpy(combined[0..cfg.field_analyzers.len], cfg.field_analyzers);
    @memcpy(combined[cfg.field_analyzers.len..], added.items);
    if (cfg.field_analyzers.len != 0) alloc.free(cfg.field_analyzers);
    cfg.field_analyzers = combined;
    transferred = true;
}

fn appendSchemaFieldAnalyzers(alloc: std.mem.Allocator, cfg: *introducer.TextAnalysisConfig, schema: schema_mod.TableSchema) !void {
    const Cursor = struct {
        docs: @TypeOf(schema.full_text_documents),
        document: usize = 0,
        field: usize = 0,
        fn next(self: *@This()) ?FieldAnalyzer {
            while (self.document < self.docs.len) {
                const fields = self.docs[self.document].fields;
                if (self.field == fields.len) {
                    self.document += 1;
                    self.field = 0;
                    continue;
                }
                const field = fields[self.field];
                self.field += 1;
                if (std.mem.eql(u8, field.emitted_name, "_all")) continue;
                return .{ .field_name = field.emitted_name, .analyzer_name = field.analyzer };
            }
            return null;
        }
    };
    try appendFallbacks(alloc, cfg, Cursor{ .docs = schema.full_text_documents });
}

pub fn appendObservedFieldAnalyzers(alloc: std.mem.Allocator, cfg: *introducer.TextAnalysisConfig, observed: anytype) !void {
    const Cursor = struct {
        entries: @TypeOf(observed),
        index: usize = 0,
        fn next(self: *@This()) ?FieldAnalyzer {
            if (self.index == self.entries.len) return null;
            const entry = self.entries[self.index];
            self.index += 1;
            return .{ .field_name = entry.field_name, .analyzer_name = entry.analyzer_name };
        }
    };
    try appendFallbacks(alloc, cfg, Cursor{ .entries = observed });
}

test "SQL native index analyzer overrides remain authoritative across schema observed fields and allocation failures" {
    const T = struct {
        fn run(a: std.mem.Allocator) !void {
            const schema: schema_mod.TableSchema = .{ .version = 0, .full_text_documents = &.{
                .{ .name = "row", .fields = &.{.{ .path = "status", .emitted_name = "status", .analyzer = "keyword" }} },
                .{ .name = "other", .fields = &.{.{ .path = "status", .emitted_name = "status", .analyzer = "simple" }} },
            } };
            var config = try parseForIndexConfig(a, "{\"analysis_config\":{\"field_analyzers\":{\"status\":\"standard\"}}}", schema);
            defer introducer.freeTextAnalysisConfig(a, config);
            try std.testing.expectEqual(@as(usize, 1), config.field_analyzers.len);
            const observed = [_]FieldAnalyzer{ .{ .field_name = "status", .analyzer_name = "keyword" }, .{ .field_name = "body", .analyzer_name = "standard" } };
            try appendObservedFieldAnalyzers(a, &config, &observed);
            try std.testing.expectEqual(@as(usize, 2), config.field_analyzers.len);
            try std.testing.expectEqualStrings("standard", config.field_analyzers[0].analyzer_name);
            try std.testing.expectEqualStrings("body", config.field_analyzers[1].field_name);
            const inherited = try parseForIndexConfig(a, "{}", schema);
            defer introducer.freeTextAnalysisConfig(a, inherited);
            try std.testing.expectEqual(@as(usize, 2), inherited.field_analyzers.len);
            try std.testing.expectEqualStrings("keyword", inherited.field_analyzers[0].analyzer_name);
            try std.testing.expectEqualStrings("simple", inherited.field_analyzers[1].analyzer_name);
        }
    };
    try T.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, T.run, .{});
}
