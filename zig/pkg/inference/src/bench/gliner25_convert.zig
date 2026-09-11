// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const inference = @import("inference_internal");
const exporter = inference.gliner_boundary_export;
const Precision = inference.models.gliner_boundary_artifact.Precision;

pub fn main(init: std.process.Init) !void {
    const a = std.heap.c_allocator;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    var source: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    var verify: ?[]const u8 = null;
    var precision: ?Precision = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help")) {
            try std.Io.File.stdout().writeStreamingAll(init.io, "gliner25-convert --model-dir SOURCE --output-dir NEW_DIRECTORY --precision fp32|fp16_encoder|q8_0|q4_k|q4_0\n" ++
                "gliner25-convert --verify-dir DIRECTORY\n" ++
                "The output parent must exist. Existing destinations are never overwritten.\n" ++
                "A verified bundle is an integrity check; it does not establish quality qualification.\n");
            return;
        }
        const value = args.next() orelse return error.MissingConversionArgument;
        if (std.mem.eql(u8, arg, "--model-dir")) {
            if (source != null) return error.DuplicateConversionArgument;
            source = value;
        } else if (std.mem.eql(u8, arg, "--output-dir")) {
            if (output != null) return error.DuplicateConversionArgument;
            output = value;
        } else if (std.mem.eql(u8, arg, "--verify-dir")) {
            if (verify != null) return error.DuplicateConversionArgument;
            verify = value;
        } else if (std.mem.eql(u8, arg, "--precision")) {
            if (precision != null) return error.DuplicateConversionArgument;
            precision = std.meta.stringToEnum(Precision, value) orelse return error.UnsupportedGlinerBoundaryPrecision;
        } else return error.UnknownConversionArgument;
    }
    if (verify) |path| {
        if (source != null or output != null or precision != null) return error.ConflictingConversionArguments;
        const absolute = try std.Io.Dir.cwd().realPathFileAlloc(init.io, path, a);
        defer a.free(absolute);
        const summary = try exporter.verifyDirectory(a, absolute, null);
        const bytes = try std.json.Stringify.valueAlloc(a, summary, .{});
        defer a.free(bytes);
        try std.Io.File.stdout().writeStreamingAll(init.io, bytes);
        try std.Io.File.stdout().writeStreamingAll(init.io, "\n");
        return;
    }
    const absolute = try std.Io.Dir.cwd().realPathFileAlloc(init.io, source orelse return error.MissingConversionArgument, a);
    defer a.free(absolute);
    var result = try exporter.exportBundle(a, init.io, absolute, output orelse return error.MissingConversionArgument, .{ .precision = precision orelse return error.MissingConversionArgument });
    defer result.deinit();
    try std.Io.File.stdout().writeStreamingAll(init.io, result.receipt_json);
    try std.Io.File.stdout().writeStreamingAll(init.io, "\n");
}
