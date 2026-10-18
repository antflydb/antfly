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

//! Selection metadata is part of each atomic optimizer checkpoint. Best
//! checkpoints are immutable, sibling files pinned by digest, so a later best
//! cannot invalidate an earlier resume checkpoint after a crash.
const std = @import("std");
const controller = @import("../seeded_gradient_trainer.zig");
const safetensors = @import("../../models/safetensors.zig");
const file = @import("../../util/c_file.zig");
const publication = @import("../../gliner_boundary_export.zig");
const Best = @import("../early_stopping.zig").EarlyStoppingState;
const Control = @import("../../execution_control.zig").InferenceExecutionControl;
const A = std.mem.Allocator;
const extension_name = "__extension.gliner_span.selection";
const Metadata = struct {
    version: u32 = 1,
    run: [32]u8,
    score: f32,
    epoch: usize,
    best_name: []const u8,
    best_sha256: [32]u8,
};

pub const Selection = struct {
    a: A,
    io: std.Io,
    best: Best,
    epoch: usize,
    best_name: []const u8,
    best_sha256: [32]u8,

    pub fn deinit(self: *Selection) void {
        self.a.free(self.best_name);
    }

    fn pin(a: A, name: []const u8) ![32]u8 {
        var mapped = try file.MmapRegion.init(a, name);
        defer mapped.deinit();
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(mapped.data, &hash, .{});
        return hash;
    }

    pub fn init(a: A, io: std.Io, trainer: *controller.Trainer, root: []const u8, run: [32]u8, score: f32, control: Control) !Selection {
        var result = Selection{ .a = a, .io = io, .best = Best.init(.{ .direction = .maximize, .patience = std.math.maxInt(u32) }), .epoch = 0, .best_name = try a.dupe(u8, ""), .best_sha256 = undefined };
        errdefer result.deinit();
        try result.observe(trainer, root, run, score, 0, control);
        return result;
    }

    pub fn observe(self: *Selection, trainer: *controller.Trainer, root: []const u8, run: [32]u8, score: f32, epoch: usize, control: Control) !void {
        if (!std.math.isFinite(score) or score < 0 or score > 1) return error.InvalidSpanSelection;
        if (score <= self.best.best) return;
        const name = try std.fmt.allocPrint(self.a, "best-{d}.safetensors", .{trainer.identity().microbatch_step});
        errdefer self.a.free(name);
        const full = try std.fs.path.join(self.a, &.{ root, name });
        defer self.a.free(full);
        try trainer.save(full, run, control);
        const sha = try pin(self.a, full);
        try publication.syncDirectory(self.io, root);
        self.a.free(self.best_name);
        self.best_name = name;
        self.best_sha256 = sha;
        _ = self.best.observe(score);
        self.epoch = epoch;
    }

    pub fn save(self: *const Selection, trainer: *controller.Trainer, destination: []const u8, run: [32]u8, control: Control) !void {
        const bytes = try std.json.Stringify.valueAlloc(self.a, Metadata{ .run = run, .score = self.best.best, .epoch = self.epoch, .best_name = self.best_name, .best_sha256 = self.best_sha256 }, .{});
        defer self.a.free(bytes);
        // The generic trainer fingerprints its owned state, not task-owned
        // extensions. Protect selection metadata against payload corruption.
        var sha: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &sha, .{});
        const encoded = try self.a.alloc(f32, sha.len + bytes.len);
        defer self.a.free(encoded);
        for (sha, encoded[0..32]) |byte, *value| value.* = @floatFromInt(byte);
        for (bytes, encoded[32..]) |byte, *value| value.* = @floatFromInt(byte);
        try trainer.saveWithExtensions(destination, run, &.{.{ .name = extension_name, .shape = &.{encoded.len}, .data = encoded }}, control);
    }

    pub fn restore(a: A, io: std.Io, trainer: *controller.Trainer, source: []const u8, root: []const u8, run: [32]u8, control: Control) !Selection {
        try control.check();
        // Bind the metadata read to the exact snapshot the optimizer restored.
        const receipt = trainer.last_restore_receipt orelse return error.MissingSpanSelection;
        var reader = try safetensors.MMapReader.openFileAbsoluteLimited(a, source, 8 * 1024 * 1024 * 1024, 4 * 1024 * 1024);
        defer reader.deinit();
        var snapshot_sha256: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(reader.file_bytes, &snapshot_sha256, .{});
        if (!std.mem.eql(u8, &snapshot_sha256, &receipt.checkpoint.sha256)) return error.SpanResumeCheckpointChanged;
        if (!reader.header.tensors.contains(extension_name)) return error.MissingSpanSelection;
        var tensor = try reader.readTensor(extension_name);
        defer tensor.deinit();
        if (tensor.dtype != .f32 or tensor.shape.len != 1 or tensor.asFloat32().len <= 32 or tensor.asFloat32().len > 4096) return error.InvalidSpanSelection;
        const bytes = try a.alloc(u8, tensor.asFloat32().len);
        defer a.free(bytes);
        for (tensor.asFloat32(), bytes) |value, *byte| {
            if (!std.math.isFinite(value) or value < 0 or value > 255 or @trunc(value) != value) return error.InvalidSpanSelection;
            byte.* = @intFromFloat(value);
        }
        var payload_sha256: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes[32..], &payload_sha256, .{});
        if (!std.mem.eql(u8, &payload_sha256, bytes[0..32])) return error.InvalidSpanSelection;
        const parsed = try std.json.parseFromSlice(Metadata, a, bytes[32..], .{});
        defer parsed.deinit();
        const meta = parsed.value;
        if (meta.version != 1 or !std.mem.eql(u8, &meta.run, &run) or !std.math.isFinite(meta.score) or meta.score < 0 or meta.score > 1 or !std.mem.startsWith(u8, meta.best_name, "best-") or !std.mem.endsWith(u8, meta.best_name, ".safetensors") or std.mem.indexOfAny(u8, meta.best_name, "/\\") != null) return error.InvalidSpanSelection;
        const origin = try std.fs.path.join(a, &.{ std.fs.path.dirname(source).?, meta.best_name });
        defer a.free(origin);
        const destination = try std.fs.path.join(a, &.{ root, meta.best_name });
        defer a.free(destination);
        if (!std.mem.eql(u8, &try pin(a, origin), &meta.best_sha256)) return error.SpanBestCheckpointChanged;
        if (!std.mem.eql(u8, origin, destination)) {
            try std.Io.Dir.copyFile(std.Io.Dir.cwd(), origin, std.Io.Dir.cwd(), destination, io, .{ .replace = false });
            var copied = try std.Io.Dir.cwd().openFile(io, destination, .{});
            defer copied.close(io);
            try copied.sync(io);
            try publication.syncDirectory(io, root);
        }
        if (!std.mem.eql(u8, &try pin(a, destination), &meta.best_sha256)) return error.SpanBestCheckpointChanged;
        var best = Best.init(.{ .direction = .maximize, .patience = std.math.maxInt(u32) });
        _ = best.observe(meta.score);
        return .{ .a = a, .io = io, .best = best, .epoch = meta.epoch, .best_name = try a.dupe(u8, meta.best_name), .best_sha256 = meta.best_sha256 };
    }

    pub fn restoreBest(self: *const Selection, trainer: *controller.Trainer, root: []const u8, run: [32]u8, control: Control) !void {
        const name = try std.fs.path.join(self.a, &.{ root, self.best_name });
        defer self.a.free(name);
        try trainer.restore(name, run, control);
        if (!std.mem.eql(u8, &trainer.last_restore_receipt.?.checkpoint.sha256, &self.best_sha256)) return error.SpanBestCheckpointChanged;
    }
};
