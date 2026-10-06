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

//! Schema/config composition shared by control and physical query units. This
//! module deliberately owns no index implementation or storage backend.

const std = @import("std");
const introducer = @import("../../introducer.zig");
const schema_mod = @import("../schema.zig");

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

fn appendSchemaFieldAnalyzers(
    alloc: std.mem.Allocator,
    cfg: *introducer.TextAnalysisConfig,
    schema: schema_mod.TableSchema,
) !void {
    const FieldAnalyzer = std.meta.Child(@TypeOf(cfg.field_analyzers));
    var extra_count: usize = 0;
    for (schema.full_text_documents) |doc| {
        for (doc.fields) |field| {
            if (std.mem.eql(u8, field.emitted_name, "_all")) continue;
            extra_count += 1;
        }
    }
    if (extra_count == 0) return;

    const original_len = cfg.field_analyzers.len;
    const combined = try alloc.alloc(FieldAnalyzer, original_len + extra_count);
    var initialized: usize = 0;
    errdefer {
        for (combined[original_len..initialized]) |item| {
            alloc.free(item.field_name);
            alloc.free(item.analyzer_name);
        }
        alloc.free(combined);
    }

    for (cfg.field_analyzers, 0..) |item, i| combined[i] = item;
    initialized = original_len;
    for (schema.full_text_documents) |doc| {
        for (doc.fields) |field| {
            if (std.mem.eql(u8, field.emitted_name, "_all")) continue;
            combined[initialized] = .{
                .field_name = try alloc.dupe(u8, field.emitted_name),
                .analyzer_name = try alloc.dupe(u8, field.analyzer),
            };
            initialized += 1;
        }
    }

    if (cfg.field_analyzers.len > 0) alloc.free(cfg.field_analyzers);
    cfg.field_analyzers = combined;
}
