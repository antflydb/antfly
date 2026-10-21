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

//! Durable, fail-closed EmbeddingGemma 2 training checkpoints.
//!
//! The CUDA trainer drains its streams and supplies an immutable host snapshot.
//! Load validates the entire directory into independent owning allocations;
//! callers swap it into a live engine only after this function returns.

const std = @import("std");
const compat = @import("../io/compat.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const safetensors = @import("../models/safetensors.zig");
const checkpoint = @import("safetensors_checkpoint.zig");
const fs_support = @import("embedding_gemma2_fs.zig");

pub const format = "antfly.embeddinggemma2.checkpoint/v1";
pub const tensor_file_name = "training.safetensors";
pub const state_file_name = "state.json";
const max_state_bytes = 64 * 1024;

pub const Tensor = checkpoint.NamedTensor;

pub const State = struct {
    version: u32 = 1,
    optimizer_step: u64,
    generation: u64,
    epoch: u64,
    cursor: u64,
    total_steps: u64,
    job_fingerprint: []const u8,
    base_sha256: []const u8,
    dataset_sha256: []const u8,
    tokenizer_sha256: []const u8,
    preprocessing_sha256: []const u8,
    seed: u64,
    shuffle_identity: []const u8,
};

pub const Expected = struct {
    job_fingerprint: []const u8,
    base_sha256: []const u8,
    dataset_sha256: []const u8,
    tokenizer_sha256: []const u8,
    preprocessing_sha256: []const u8,
    seed: u64,
    shuffle_identity: []const u8,
    tensors: []const TensorSpec,
};

pub const TensorSpec = struct { name: []const u8, shape: []const usize };

const StoredState = struct {
    format: []const u8,
    version: u32,
    optimizer_step: u64,
    generation: u64,
    epoch: u64,
    cursor: u64,
    total_steps: u64,
    job_fingerprint: []const u8,
    base_sha256: []const u8,
    dataset_sha256: []const u8,
    tokenizer_sha256: []const u8,
    preprocessing_sha256: []const u8,
    seed: u64,
    shuffle_identity: []const u8,
    tensor_file_sha256: []const u8,
};

pub const OwnedTensor = struct { name: []u8, data: []f32, shape: []usize };
pub const Loaded = struct {
    allocator: std.mem.Allocator,
    state: State,
    tensors: []OwnedTensor,
    tensor_file_sha256: [64]u8,

    pub fn deinit(self: *Loaded) void {
        for (self.tensors) |tensor| {
            self.allocator.free(tensor.name);
            self.allocator.free(tensor.data);
            self.allocator.free(tensor.shape);
        }
        self.allocator.free(self.tensors);
        inline for (.{ "job_fingerprint", "base_sha256", "dataset_sha256", "tokenizer_sha256", "preprocessing_sha256", "shuffle_identity" }) |field|
            self.allocator.free(@field(self.state, field));
        self.* = undefined;
    }
};

pub fn saveAtomic(allocator: std.mem.Allocator, output_dir: []const u8, state: State, tensors: []const Tensor, max_host_bytes: u64, control: ?Control) !void {
    try validateState(state);
    try validateTensorSnapshot(tensors, max_host_bytes);
    if (control) |active| try active.check();
    const io = compat.io();
    if (std.Io.Dir.cwd().access(io, output_dir, .{})) |_| return error.CheckpointAlreadyExists else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }
    var staging = try fs_support.OwnedStagingDir.create(allocator, io, output_dir, "tmp");
    defer staging.deinit();

    const tensor_path = try std.fs.path.join(allocator, &.{ staging.path, tensor_file_name });
    defer allocator.free(tensor_path);
    try checkpoint.saveControlled(allocator, tensor_path, tensors, control, true);
    const tensor_sha = try sha256FileHex(allocator, tensor_path, control);
    defer allocator.free(tensor_sha);
    const stored = StoredState{
        .format = format,
        .version = state.version,
        .optimizer_step = state.optimizer_step,
        .generation = state.generation,
        .epoch = state.epoch,
        .cursor = state.cursor,
        .total_steps = state.total_steps,
        .job_fingerprint = state.job_fingerprint,
        .base_sha256 = state.base_sha256,
        .dataset_sha256 = state.dataset_sha256,
        .tokenizer_sha256 = state.tokenizer_sha256,
        .preprocessing_sha256 = state.preprocessing_sha256,
        .seed = state.seed,
        .shuffle_identity = state.shuffle_identity,
        .tensor_file_sha256 = tensor_sha,
    };
    var json: std.Io.Writer.Allocating = .init(allocator);
    defer json.deinit();
    try std.json.Stringify.value(stored, .{}, &json.writer);
    try json.writer.writeByte('\n');
    const state_path = try std.fs.path.join(allocator, &.{ staging.path, state_file_name });
    defer allocator.free(state_path);
    try writeSynced(state_path, json.written());
    try syncDir(staging.path);
    if (control) |active| try active.check();
    try staging.publish(output_dir);
    try syncDir(std.fs.path.dirname(output_dir) orelse ".");
}

pub fn loadValidated(allocator: std.mem.Allocator, dir: []const u8, expected: Expected, max_host_bytes: u64, control: ?Control) !Loaded {
    if (control) |active| try active.check();
    const state_path = try std.fs.path.join(allocator, &.{ dir, state_file_name });
    defer allocator.free(state_path);
    const bytes = try compat.cwd().readFileAlloc(compat.io(), state_path, allocator, .limited(max_state_bytes));
    defer allocator.free(bytes);
    var parsed = try std.json.parseFromSlice(StoredState, allocator, bytes, .{ .ignore_unknown_fields = false });
    defer parsed.deinit();
    const stored = parsed.value;
    try validateStored(stored, expected);

    const tensor_path = try std.fs.path.join(allocator, &.{ dir, tensor_file_name });
    defer allocator.free(tensor_path);
    const actual_sha = try sha256FileHex(allocator, tensor_path, control);
    defer allocator.free(actual_sha);
    if (!std.mem.eql(u8, stored.tensor_file_sha256, actual_sha)) return error.CheckpointChecksumMismatch;
    var reader = try safetensors.MMapReader.openFileAbsoluteLimited(allocator, tensor_path, @intCast(max_host_bytes), 4 * 1024 * 1024);
    defer reader.deinit();
    try safetensors.validateReader(allocator, &reader);
    if (reader.header.tensors.count() != expected.tensors.len) return error.CheckpointTensorSetMismatch;
    for (expected.tensors, 0..) |spec, i| {
        if (!validTensorName(spec.name)) return error.InvalidCheckpointTensor;
        for (expected.tensors[0..i]) |previous| if (std.mem.eql(u8, previous.name, spec.name)) return error.DuplicateCheckpointTensor;
    }
    var total: u64 = 0;
    const owned = try allocator.alloc(OwnedTensor, expected.tensors.len);
    var initialized: usize = 0;
    errdefer {
        for (owned[0..initialized]) |tensor| {
            allocator.free(tensor.name);
            allocator.free(tensor.data);
            allocator.free(tensor.shape);
        }
        allocator.free(owned);
    }
    for (expected.tensors, 0..) |spec, i| {
        if (control) |active| try active.check();
        const meta = reader.header.tensors.get(spec.name) orelse return error.CheckpointTensorSetMismatch;
        if (meta.dtype != .f32 or meta.shape.len != spec.shape.len) return error.CheckpointTensorShapeMismatch;
        var count: usize = 1;
        for (spec.shape, 0..) |dim, d| {
            if (meta.shape[d] != @as(i64, @intCast(dim))) return error.CheckpointTensorShapeMismatch;
            count = std.math.mul(usize, count, dim) catch return error.CheckpointBudgetExceeded;
        }
        total = std.math.add(u64, total, @as(u64, count) * 8) catch return error.CheckpointBudgetExceeded;
        if (total > max_host_bytes) return error.CheckpointBudgetExceeded; // mmap source + owning replacement
        var tensor = try reader.readTensor(spec.name);
        defer tensor.deinit();
        owned[i] = try ownTensor(allocator, spec, tensor.data, count);
        initialized += 1;
    }
    var validated_digest: [64]u8 = undefined;
    @memcpy(&validated_digest, actual_sha);
    return .{ .allocator = allocator, .state = try ownState(allocator, stored), .tensors = owned, .tensor_file_sha256 = validated_digest };
}

fn ownTensor(allocator: std.mem.Allocator, spec: TensorSpec, source: []const u8, count: usize) !OwnedTensor {
    const name = try allocator.dupe(u8, spec.name);
    errdefer allocator.free(name);
    const data = try allocator.alloc(f32, count);
    errdefer allocator.free(data);
    @memcpy(std.mem.sliceAsBytes(data), source);
    for (data) |value| if (!std.math.isFinite(value)) return error.NonFiniteCheckpointTensor;
    const shape = try allocator.dupe(usize, spec.shape);
    return .{ .name = name, .data = data, .shape = shape };
}

fn validateState(state: State) !void {
    if (state.version != 1 or state.total_steps == 0 or state.optimizer_step > state.total_steps or state.cursor > std.math.maxInt(u63)) return error.InvalidCheckpointState;
    for ([_][]const u8{ state.job_fingerprint, state.base_sha256, state.dataset_sha256, state.tokenizer_sha256, state.preprocessing_sha256 }) |digest|
        if (!validHexDigest(digest)) return error.InvalidCheckpointIdentity;
    if (state.shuffle_identity.len == 0 or state.shuffle_identity.len > 256) return error.InvalidCheckpointState;
}

fn validateTensorSnapshot(tensors: []const Tensor, max_host_bytes: u64) !void {
    if (tensors.len == 0) return error.EmptyCheckpoint;
    var total: u64 = 0;
    for (tensors, 0..) |tensor, i| {
        if (!validTensorName(tensor.name) or tensor.shape.len == 0 or tensor.data.len == 0) return error.InvalidCheckpointTensor;
        for (tensors[0..i]) |previous| if (std.mem.eql(u8, previous.name, tensor.name)) return error.DuplicateCheckpointTensor;
        var elements: usize = 1;
        for (tensor.shape) |dim| elements = std.math.mul(usize, elements, dim) catch return error.CheckpointBudgetExceeded;
        if (elements != tensor.data.len) return error.CheckpointTensorShapeMismatch;
        for (tensor.data) |value| if (!std.math.isFinite(value)) return error.NonFiniteCheckpointTensor;
        total = std.math.add(u64, total, @as(u64, tensor.data.len) * 4) catch return error.CheckpointBudgetExceeded;
        if (total > max_host_bytes) return error.CheckpointBudgetExceeded;
    }
}

fn validTensorName(name: []const u8) bool {
    if (!std.mem.startsWith(u8, name, "language_model.")) return false;
    const payloads = [_][]const u8{ ".lora_A.weight", ".lora_B.weight", ".lora_A.weight.exp_avg", ".lora_B.weight.exp_avg", ".lora_A.weight.exp_avg_sq", ".lora_B.weight.exp_avg_sq", ".lora_A.weight.grad_accum", ".lora_B.weight.grad_accum" };
    for (payloads) |suffix| if (std.mem.endsWith(u8, name, suffix)) return true;
    return false;
}

fn validateStored(stored: StoredState, expected: Expected) !void {
    if (!std.mem.eql(u8, stored.format, format) or stored.version != 1) return error.UnsupportedCheckpointVersion;
    const state = State{ .version = stored.version, .optimizer_step = stored.optimizer_step, .generation = stored.generation, .epoch = stored.epoch, .cursor = stored.cursor, .total_steps = stored.total_steps, .job_fingerprint = stored.job_fingerprint, .base_sha256 = stored.base_sha256, .dataset_sha256 = stored.dataset_sha256, .tokenizer_sha256 = stored.tokenizer_sha256, .preprocessing_sha256 = stored.preprocessing_sha256, .seed = stored.seed, .shuffle_identity = stored.shuffle_identity };
    try validateState(state);
    if (!std.mem.eql(u8, stored.job_fingerprint, expected.job_fingerprint) or !std.mem.eql(u8, stored.base_sha256, expected.base_sha256) or !std.mem.eql(u8, stored.dataset_sha256, expected.dataset_sha256) or !std.mem.eql(u8, stored.tokenizer_sha256, expected.tokenizer_sha256) or !std.mem.eql(u8, stored.preprocessing_sha256, expected.preprocessing_sha256) or stored.seed != expected.seed or !std.mem.eql(u8, stored.shuffle_identity, expected.shuffle_identity)) return error.CheckpointIdentityMismatch;
    if (!validHexDigest(stored.tensor_file_sha256)) return error.InvalidCheckpointIdentity;
}

fn ownState(allocator: std.mem.Allocator, stored: StoredState) !State {
    const job = try allocator.dupe(u8, stored.job_fingerprint);
    errdefer allocator.free(job);
    const base = try allocator.dupe(u8, stored.base_sha256);
    errdefer allocator.free(base);
    const dataset = try allocator.dupe(u8, stored.dataset_sha256);
    errdefer allocator.free(dataset);
    const tokenizer = try allocator.dupe(u8, stored.tokenizer_sha256);
    errdefer allocator.free(tokenizer);
    const preprocessing = try allocator.dupe(u8, stored.preprocessing_sha256);
    errdefer allocator.free(preprocessing);
    const shuffle = try allocator.dupe(u8, stored.shuffle_identity);
    errdefer allocator.free(shuffle);
    return .{ .version = stored.version, .optimizer_step = stored.optimizer_step, .generation = stored.generation, .epoch = stored.epoch, .cursor = stored.cursor, .total_steps = stored.total_steps, .job_fingerprint = job, .base_sha256 = base, .dataset_sha256 = dataset, .tokenizer_sha256 = tokenizer, .preprocessing_sha256 = preprocessing, .seed = stored.seed, .shuffle_identity = shuffle };
}

fn validHexDigest(value: []const u8) bool {
    if (value.len != 64) return false;
    for (value) |c| if (!std.ascii.isHex(c)) return false;
    return true;
}
fn writeSynced(path: []const u8, data: []const u8) !void {
    const io = compat.io();
    try compat.cwd().writeFile(io, .{ .sub_path = path, .data = data });
    var file = try compat.cwd().openFile(io, path, .{});
    defer file.close(io);
    try file.sync(io);
}
fn syncDir(path: []const u8) !void {
    var dir = try compat.cwd().openDir(compat.io(), path, .{ .iterate = true });
    defer dir.close(compat.io());
    const file = std.Io.File{ .handle = dir.handle, .flags = .{ .nonblocking = false } };
    try file.sync(compat.io());
}
fn sha256FileHex(allocator: std.mem.Allocator, path: []const u8, control: ?Control) ![]u8 {
    var file = try compat.cwd().openFile(compat.io(), path, .{});
    defer file.close(compat.io());
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var buffer: [256 * 1024]u8 = undefined;
    while (true) {
        if (control) |active| try active.check();
        const n = file.readStreaming(compat.io(), &.{buffer[0..]}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (n == 0) break;
        hash.update(buffer[0..n]);
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return std.fmt.allocPrint(allocator, "{s}", .{std.fmt.bytesToHex(digest, .lower)});
}

fn testState() State {
    const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    return .{ .optimizer_step = 2, .generation = 3, .epoch = 1, .cursor = 7, .total_steps = 10, .job_fingerprint = digest, .base_sha256 = digest, .dataset_sha256 = digest, .tokenizer_sha256 = digest, .preprocessing_sha256 = digest, .seed = 42, .shuffle_identity = "splitmix64-v1" };
}
fn expectedFor(state: State, specs: []const TensorSpec) Expected {
    return .{ .job_fingerprint = state.job_fingerprint, .base_sha256 = state.base_sha256, .dataset_sha256 = state.dataset_sha256, .tokenizer_sha256 = state.tokenizer_sha256, .preprocessing_sha256 = state.preprocessing_sha256, .seed = state.seed, .shuffle_identity = state.shuffle_identity, .tensors = specs };
}

test "EG2 checkpoint round trips and refuses overwrite" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", allocator);
    defer allocator.free(root);
    const path = try std.fs.path.join(allocator, &.{ root, "checkpoint-2" });
    defer allocator.free(path);
    const shape = [_]usize{ 2, 2 };
    const data = [_]f32{ 1, 2, 3, 4 };
    const tensors = [_]Tensor{.{ .name = "language_model.layers.0.q_proj.lora_A.weight", .data = &data, .shape = &shape }};
    const specs = [_]TensorSpec{.{ .name = tensors[0].name, .shape = &shape }};
    const state = testState();
    try saveAtomic(allocator, path, state, &tensors, 1024, null);
    try std.testing.expectError(error.CheckpointAlreadyExists, saveAtomic(allocator, path, state, &tensors, 1024, null));
    var loaded = try loadValidated(allocator, path, expectedFor(state, &specs), 1024, null);
    defer loaded.deinit();
    try std.testing.expectEqualSlices(f32, &data, loaded.tensors[0].data);
}

test "EG2 checkpoint rejects checksum corruption and partial directory" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", allocator);
    defer allocator.free(root);
    const partial = try std.fs.path.join(allocator, &.{ root, "partial" });
    defer allocator.free(partial);
    try compat.cwd().createDirPath(compat.io(), partial);
    const shape = [_]usize{1};
    const specs = [_]TensorSpec{.{ .name = "language_model.x.lora_A.weight", .shape = &shape }};
    const state = testState();
    try std.testing.expectError(error.FileNotFound, loadValidated(allocator, partial, expectedFor(state, &specs), 1024, null));
    const path = try std.fs.path.join(allocator, &.{ root, "corrupt" });
    defer allocator.free(path);
    const data = [_]f32{1};
    const tensors = [_]Tensor{.{ .name = specs[0].name, .data = &data, .shape = &shape }};
    try saveAtomic(allocator, path, state, &tensors, 1024, null);
    const tensor_path = try std.fs.path.join(allocator, &.{ path, tensor_file_name });
    defer allocator.free(tensor_path);
    var file = try compat.cwd().openFile(compat.io(), tensor_path, .{ .mode = .read_write });
    defer file.close(compat.io());
    try file.setLength(compat.io(), 8);
    try file.sync(compat.io());
    try std.testing.expectError(error.CheckpointChecksumMismatch, loadValidated(allocator, path, expectedFor(state, &specs), 1024, null));
}

test "EG2 checkpoint validates snapshot before publication" {
    const shape = [_]usize{1};
    const nan = [_]f32{std.math.nan(f32)};
    const tensors = [_]Tensor{.{ .name = "language_model.x.lora_A.weight", .data = &nan, .shape = &shape }};
    try std.testing.expectError(error.NonFiniteCheckpointTensor, validateTensorSnapshot(&tensors, 1024));
    const finite = [_]f32{1};
    const too_big = [_]Tensor{.{ .name = "language_model.x.lora_A.weight", .data = &finite, .shape = &shape }};
    try std.testing.expectError(error.CheckpointBudgetExceeded, validateTensorSnapshot(&too_big, 3));
}

test "EG2 checkpoint cancellation publishes nothing" {
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", allocator);
    defer allocator.free(root);
    const path = try std.fs.path.join(allocator, &.{ root, "cancelled" });
    defer allocator.free(path);
    const shape = [_]usize{1};
    const data = [_]f32{1};
    const tensors = [_]Tensor{.{ .name = "language_model.x.lora_A.weight", .data = &data, .shape = &shape }};
    try std.testing.expectError(error.Cancelled, saveAtomic(allocator, path, testState(), &tensors, 1024, .{ .check_fn = Cancel.check }));
    try std.testing.expectError(error.FileNotFound, compat.cwd().access(compat.io(), path, .{}));
}
