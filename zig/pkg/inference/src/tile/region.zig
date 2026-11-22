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

//! A generated region inside a hand-maintained source file: the text
//! between the begin and end marker lines is owned by the tile generator.

pub const ProgramUse = struct {
    operation: []const u8,
    source: []const u8,
    scope: enum { phase, body },
    precision: []const u8,
    schedule: []const u8,
    source_index: ?usize = null,
    export_name: ?[]const u8 = null,
    threads: ?[3]u32 = null,
    static_scratch_bytes: ?usize = null,
};
pub const Region = struct { path: []const u8, begin: []const u8, end: []const u8, data: []const u8, programs: []const ProgramUse = &.{} };

pub const metal_kernels_m = "src/backends/metal_kernels.m";
pub const cuda_bundle = "src/ops/cuda/artifacts/inference_cuda_kernels.cu";
/// Indent of the C-string lines inside metal_kernels.m.
pub const metal_indent = "           ";

pub fn region(comptime path: []const u8, comptime indent: []const u8, comptime family: []const u8, comptime data: []const u8) Region {
    return .{
        .path = path,
        .begin = indent ++ "// tile-codegen:begin " ++ family ++ " family (do not edit; run: zig build tile-kernel-codegen -- --write)\n",
        .end = indent ++ "// tile-codegen:end " ++ family ++ " family\n",
        .data = data,
    };
}

/// A Metal region in metal_kernels.m.
pub fn metal(comptime family: []const u8, comptime data: []const u8) Region {
    return region(metal_kernels_m, metal_indent, family, data);
}

/// A CUDA region in inference_cuda_kernels.cu.
pub fn cuda(comptime family: []const u8, comptime data: []const u8) Region {
    return region(cuda_bundle, "", family, data);
}

/// Encode shader lines once at the source boundary. Native owners keep readable
/// MSL; the checked-in Objective-C translation unit retains its string ABI.
pub fn metalLines(comptime indent: []const u8, comptime src: []const u8) []const u8 {
    comptime {
        @setEvalBranchQuota(100_000_000);
        const encoding = @import("source_boundary.zig");
        const size = encoding.metalEncodedSize(src, indent, .{});
        var buffer: [size]u8 = undefined;
        encoding.encodeMetal(&buffer, src, indent, .{});
        const result = buffer;
        return &result;
    }
}

const std = @import("std");

/// A native owner exports source_0, source_1, ... in lexical program order.
/// One region has the family name; multiple regions retain numbered suffixes.
fn sourceCount(comptime source: type) usize {
    comptime {
        if (@hasDecl(source, "sources")) {
            for (std.meta.declarations(source)) |decl| {
                if (std.mem.startsWith(u8, decl, "source_"))
                    @compileError("native family cannot mix file exports and source_N declarations");
            }
            if (source.sources.len == 0) @compileError("native family has no source regions");
            return source.sources.len;
        }
        var count: usize = 0;
        for (std.meta.declarations(source)) |decl| {
            if (std.mem.startsWith(u8, decl, "source_")) count += 1;
        }
        if (count == 0) @compileError("native family has no source regions");
        for (0..count) |i| {
            if (!@hasDecl(source, std.fmt.comptimePrint("source_{d}", .{i})))
                @compileError("native family source indices must be contiguous from zero");
        }
        return count;
    }
}

/// Compile-time ownership metadata, separate from emitted device programs.
pub const Family = struct {
    path: []const u8,
    indent: []const u8,
    name: []const u8,
    source: type,
    programs: []const ProgramUse = &.{},
};

/// Provenance belongs to the existing family registry, not a parallel catalog.
/// Select individual source indices when a family contains native ABI shells.
pub fn shared(comptime family: Family, comptime programs: []const ProgramUse) Family {
    @setEvalBranchQuota(100_000);
    var result = family;
    for (programs) |use| {
        if (!operationFamilyName(use.operation)) @compileError("shared program operation must not name a model");
        if (use.source_index) |index| if (index >= sourceCount(family.source)) @compileError("shared program source index outside family");
    }
    result.programs = family.programs ++ programs;
    return result;
}

pub fn nativeFamily(comptime path: []const u8, comptime indent: []const u8, comptime name: []const u8, comptime source: type) Family {
    return .{ .path = path, .indent = indent, .name = name, .source = source };
}

pub fn metalFamily(comptime name: []const u8, comptime source: type) Family {
    return nativeFamily(metal_kernels_m, metal_indent, name, source);
}

pub fn cudaFamily(comptime name: []const u8, comptime source: type) Family {
    return nativeFamily(cuda_bundle, "", name, source);
}

fn sourceFamily(comptime path: []const u8, comptime indent: []const u8, comptime name: []const u8, comptime text: []const u8, comptime encoding: @import("native_sources.zig").Encoding) Family {
    return nativeFamily(path, indent, name, struct {
        pub const sources = @import("native_sources.zig").sources(text, encoding);
    });
}

pub fn cudaSourceFamily(comptime name: []const u8, comptime text: []const u8) Family {
    return sourceFamily(cuda_bundle, "", name, text, .cuda);
}

pub fn metalSourceFamily(comptime name: []const u8, comptime text: []const u8) Family {
    return sourceFamily(metal_kernels_m, metal_indent, name, text, .metal);
}

/// Host ABI macro splices retain their existing Objective-C encoding.
pub fn encodedMetalSourceFamily(comptime name: []const u8, comptime text: []const u8) Family {
    return sourceFamily(metal_kernels_m, metal_indent, name, text, .encoded_metal);
}

fn regionCount(comptime families: []const Family) usize {
    comptime {
        @setEvalBranchQuota(1_000_000);
        var count: usize = 0;
        for (families) |item| count += sourceCount(item.source);
        return count;
    }
}

/// Numerical source owners describe operations/layouts, not checkpoint families.
/// Historical exported symbols remain compatibility names inside these owners.
pub fn operationFamilyName(name: []const u8) bool {
    var tokens = std.mem.tokenizeAny(u8, name, "._-/");
    while (tokens.next()) |token| {
        inline for (.{ "bert", "modernbert", "roberta", "gemma", "embeddinggemma", "qwen", "lighton", "lightonocr", "florence", "whisper", "gliner", "clip", "laya", "deberta", "nomic", "siglip", "e2b", "e4b", "a4b" }) |model| {
            if (std.ascii.eqlIgnoreCase(token, model)) return false;
            if (token.len > model.len and std.ascii.isDigit(token[model.len]) and std.ascii.eqlIgnoreCase(token[0..model.len], model)) return false;
        }
    }
    return true;
}

/// Flatten family descriptions once; do not repeatedly concatenate arrays.
pub fn regions(comptime families: []const Family) [regionCount(families)]Region {
    comptime {
        @setEvalBranchQuota(1_000_000);
        var result: [regionCount(families)]Region = undefined;
        var cursor: usize = 0;
        for (families) |item| {
            if (!operationFamilyName(item.name)) @compileError("native numerical family must use an operation/layout name: " ++ item.name);
            const count = sourceCount(item.source);
            for (0..count) |i| {
                const key = if (count == 1) item.name else std.fmt.comptimePrint("{s}.{d}", .{ item.name, i });
                const data = if (@hasDecl(item.source, "sources")) item.source.sources[i] else @field(item.source, std.fmt.comptimePrint("source_{d}", .{i}));
                result[cursor] = region(item.path, item.indent, key, data);
                var uses: []const ProgramUse = &.{};
                for (item.programs) |use| {
                    if (use.source_index == null or use.source_index.? == i) {
                        if (use.export_name) |name| {
                            if (std.mem.indexOf(u8, data, name ++ "(") == null and std.mem.indexOf(u8, data, "(" ++ name ++ ")") == null) @compileError("shared program provenance does not match its export: " ++ name);
                        }
                        uses = uses ++ [_]ProgramUse{use};
                    }
                }
                result[cursor].programs = uses;
                cursor += 1;
            }
        }
        return result;
    }
}

test "Metal source encoding preserves quotes backslashes blanks and line boundaries" {
    const text = "kernel void k() {\n  // \"quoted\" \\n\n}\n";
    try std.testing.expectEqualStrings("  \"kernel void k() {\\n\"\n  \"  // \\\"quoted\\\" \\\\n\\n\"\n  \"}\\n\"\n", comptime metalLines("  ", text));
    try std.testing.expectEqualStrings("", comptime metalLines("", "unterminated"));
    try std.testing.expectEqualStrings("\"a\\n\"\n", comptime metalLines("", "a\nunterminated"));
}

test "native family descriptors retain numbered keys and custom source paths" {
    const sources = struct {
        pub const source_0 = "first";
        pub const source_1 = "second";
    };
    const entries = comptime regions(&.{nativeFamily("custom.cuh", "", "row_program", sources)});
    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqualStrings("custom.cuh", entries[0].path);
    try std.testing.expectEqualStrings("first", entries[0].data);
    try std.testing.expectEqualStrings("second", entries[1].data);
    try std.testing.expect(std.mem.indexOf(u8, entries[1].begin, "row_program.1 family") != null);
    const single = comptime regions(&.{metalFamily("single", struct {
        pub const source_0 = "kernel";
    })});
    try std.testing.expectEqualStrings(metal_indent, single[0].begin[0..metal_indent.len]);
    try std.testing.expect(std.mem.indexOf(u8, single[0].begin, "single family") != null);
}

test "numerical family names reject model owners and retain operation vocabulary" {
    for ([_][]const u8{ "attention.f16_hd64_prefill", "projection.q4q8.rows", "elementwise.clipping", "vision.flash" }) |name| try std.testing.expect(operationFamilyName(name));
    for ([_][]const u8{ "attention.bert_prefill", "gemma4.projection", "cuda_qwen3vl_rope", "e4b_ffn", "attention.block_laya", "reduce.deberta_embeddings", "normalization.nomic_pool", "projection.siglip2", "attention.LightOnOCR3", "EmbeddingGemma2.projection", "cuda_Gemma4_attention" }) |name| try std.testing.expect(!operationFamilyName(name));
}

test "shared program provenance applies only to its selected production region" {
    const family = shared(cudaFamily("projection.test", struct {
        pub const source_0 = "kernel first()";
        pub const source_1 = "kernel second()";
    }), &.{.{ .operation = "projection.dot", .source = "shared.zig", .scope = .phase, .precision = "F32", .schedule = "serial", .source_index = 1, .export_name = "second" }});
    const entries = comptime regions(&.{family});
    try std.testing.expectEqual(@as(usize, 0), entries[0].programs.len);
    try std.testing.expectEqual(@as(usize, 1), entries[1].programs.len);
    try std.testing.expectEqualStrings("projection.dot", entries[1].programs[0].operation);
}
