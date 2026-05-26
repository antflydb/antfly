// Copyright 2026 Antfly, Inc.
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
const termite = @import("termite_internal");

const finetune = termite.finetune.gemma4;
const gemma4_real = termite.finetune.gemma4_real_autodiff;
const gemma4_mm_real = termite.finetune.gemma4_multimodal_real_autodiff;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const base_model_dir = args.next() orelse return usageError();
    const prepared_inputs_path = args.next() orelse return usageError();
    const out_path = args.next() orelse return usageError();

    var opts = gemma4_real.TeacherTopKOptions{};
    var backend: gemma4_real.BackendKind = .native;
    var gguf_projector_path: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--top-k")) {
            const val = args.next() orelse return usageError();
            opts.top_k = try std.fmt.parseUnsigned(usize, val, 10);
        } else if (std.mem.eql(u8, arg, "--temperature")) {
            const val = args.next() orelse return usageError();
            opts.temperature = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            const val = args.next() orelse return usageError();
            opts.max_examples = try std.fmt.parseUnsigned(usize, val, 10);
        } else if (std.mem.eql(u8, arg, "--backend")) {
            const val = args.next() orelse return usageError();
            backend = parseBackend(val) orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--gguf-projector")) {
            gguf_projector_path = args.next() orelse return usageError();
        } else {
            return usageError();
        }
    }

    var prepared = try finetune.loadPreparedInputsSummary(allocator, prepared_inputs_path);
    defer finetune.freePreparedInputsSummary(allocator, &prepared);

    const has_multimodal = prepared.examples_with_images > 0 or prepared.examples_with_audio > 0;
    var maybe_projector_fingerprint: ?finetune.ProjectorFingerprint = null;
    defer if (maybe_projector_fingerprint) |*fp| finetune.freeProjectorFingerprint(allocator, fp);
    const summary = if (has_multimodal) blk: {
        const projector_path = gguf_projector_path orelse prepared.gguf_projector_path orelse return error.MissingGgufProjector;
        maybe_projector_fingerprint = try finetune.fingerprintProjectorFile(allocator, projector_path);
        if (prepared.gguf_projector_sha256) |expected_sha| {
            if (!std.mem.eql(u8, expected_sha, maybe_projector_fingerprint.?.sha256)) return error.PreparedProjectorFingerprintMismatch;
        }
        break :blk try gemma4_mm_real.materializeTeacherTopKTargets(
            allocator,
            base_model_dir,
            projector_path,
            maybe_projector_fingerprint.?.sha256,
            &prepared,
            backend,
            opts,
        );
    } else try gemma4_real.materializeTeacherTopKTargets(
        allocator,
        base_model_dir,
        &prepared,
        backend,
        opts,
    );
    try finetune.savePreparedInputsSummary(allocator, out_path, prepared);

    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(.{
        .task = "gemma4_teacher_top_k_materialize",
        .base_model_dir = base_model_dir,
        .prepared_inputs_path = prepared_inputs_path,
        .out_path = out_path,
        .backend = @tagName(backend),
        .multimodal = has_multimodal,
        .gguf_projector_path = if (has_multimodal) (gguf_projector_path orelse prepared.gguf_projector_path) else null,
        .gguf_projector_sha256 = if (maybe_projector_fingerprint) |fp| fp.sha256 else null,
        .summary = summary,
    }, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn parseBackend(value: []const u8) ?gemma4_real.BackendKind {
    if (std.mem.eql(u8, value, "native")) return .native;
    return null;
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: materialize-gemma4-teacher-targets <base_model_dir> <prepared_inputs_json> <out_summary_json> [options]
        \\
        \\  <base_model_dir>       Path to the full Gemma4 teacher model directory.
        \\  <prepared_inputs_json> Prepared inputs from prepare-gemma4-lora-inputs.
        \\  <out_summary_json>     Output prepared inputs with teacher top-k targets.
        \\
        \\Options:
        \\  --top-k N              Teacher tokens per row (default: 8)
        \\  --temperature F        Temperature applied before top-k softmax (default: 1.0)
        \\  --max-examples N       Maximum examples to materialize (default: 0 = all)
        \\  --backend native   Teacher inference backend (default: native)
        \\  --gguf-projector P     Required for multimodal inputs unless recorded in the prepared summary
        \\
        \\example: materialize-gemma4-teacher-targets /tmp/gemma4-base /tmp/prepared.json /tmp/prepared.teacher.json --top-k 8 --temperature 2.0
        \\example: materialize-gemma4-teacher-targets /tmp/gemma4-base /tmp/mm-prepared.json /tmp/mm-prepared.teacher.json --gguf-projector /tmp/mmproj.gguf
        \\
    , .{});
    return error.InvalidArguments;
}
