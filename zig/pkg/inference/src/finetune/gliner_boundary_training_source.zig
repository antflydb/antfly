// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Immutable original-FP32 source for a boundary training job. All five files
//! are read, hashed and consumed through the same opened descriptors. Tensor
//! views borrow this owner's aligned snapshot; paths and mmap contents cannot
//! change a running job. Drain every trainer/tokenizer user before deinit.
//!
//! Identity records consumed bytes; an optional expected identity authenticates
//! a caller-selected snapshot. Neither one is a publisher signature or a model
//! quality qualification. This initial format admits the published 334-tensor
//! architecture, independently of the serving runtime's qualification gate.
const std = @import("std");
const builtin = @import("builtin");
const model = @import("../models/gliner_boundary.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const artifact = @import("../models/gliner_boundary_artifact.zig");
const safetensors = @import("../models/safetensors.zig");
const access = @import("../models/tensor_access.zig");
const native = @import("../ops/native_compute.zig");
const run = @import("gliner_boundary_run.zig");
const HfTokenizer = @import("inference_hf_tokenizer").HfTokenizer;
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const Budget = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

pub const file_names = [_][]const u8{"model.safetensors"} ++ bundle.sidecar_names;
pub const Limits = struct {
    /// Aggregate source snapshot, decoded tokenizer, parser/validation scratch,
    /// names and shapes. The actual reservation is file bytes plus auxiliary
    /// allowance, capped by this ceiling, rather than this ceiling alone.
    max_source_bytes: usize = 2 * 1024 * 1024 * 1024,
    max_auxiliary_bytes: usize = 384 * 1024 * 1024,
    max_weight_bytes: usize = 1536 * 1024 * 1024,
    max_tokenizer_bytes: usize = 32 * 1024 * 1024,
    max_config_bytes: usize = 1024 * 1024,
    max_header_bytes: usize = 4 * 1024 * 1024,
    max_json_depth: usize = 64,
};
pub const Options = struct {
    limits: Limits = .{},
    expected_identity: ?bundle.Identity = null,
};
pub const Usage = struct { reserved_bytes: usize, live_bytes: usize, peak_bytes: usize };

pub const Source = struct {
    backing: Allocator,
    budget: Budget,
    gate: AllocationGate,
    reserved_source_bytes: usize,
    identity: bundle.Identity = undefined,
    config: model.Config = undefined,
    store: native.WeightStore,
    /// Stable native names, original upstream names, dimensions and FP32 views.
    /// Values are borrowed and must never be updated in place. The optimizer
    /// makes its own mutable copies of selected trainables.
    parameters: []run.Parameter = &.{},
    parameter_count: usize = 0,
    blobs: [5]?Blob = .{null} ** 5,
    header: ?safetensors.Header = null,
    tokenizer_owner: ?*HfTokenizer = null,

    /// The caller reserves `reserved_source_bytes` with process admission.
    /// `reservation()` supports admission before this operation; supplying its
    /// returned value as max_source_bytes closes a size-growth race. Open still
    /// validates all descriptor sizes, bytes and optional pins independently.
    pub fn open(a: Allocator, io: std.Io, path: []const u8, options: Options, control: ?Control) !*Source {
        try validateOptions(options);
        try check(control);
        var opened = try Opened.init(io, path, options.limits, control);
        defer opened.deinit(io);
        const reserved = try calculateReservation(opened.sizes, options.limits);
        const self = try a.create(Source);
        self.* = .{
            .backing = a,
            .budget = .{ .backing = a, .limit = reserved - @sizeOf(Source) },
            .gate = undefined,
            .reserved_source_bytes = reserved,
            .store = undefined,
        };
        self.gate = .{ .budget = &self.budget, .control = control };
        self.store = .{ .allocator = self.gate.allocator(), .resident_weights = .{}, .lazy_weights = .{}, .allow_direct_quant = false };
        errdefer self.deinit();
        self.load(io, &opened, options, control) catch |err| {
            if (self.gate.failure) |interrupted| return interrupted;
            if (err == error.OutOfMemory and self.budget.denied) return error.BoundaryTrainingSourceLimitExceeded;
            return err;
        };
        if (self.gate.failure) |interrupted| return interrupted;
        // The load control can point into a caller's stack. Persistent tokenizer
        // allocations retain only this stable allocator, never that control.
        self.gate.control = null;
        return self;
    }

    pub fn tokenizer(self: *Source) Tokenizer {
        return self.tokenizer_owner.?.tokenizer();
    }

    pub fn usage(self: *const Source) Usage {
        return .{ .reserved_bytes = self.reserved_source_bytes, .live_bytes = @sizeOf(Source) + self.budget.live, .peak_bytes = @sizeOf(Source) + self.budget.peak };
    }

    pub fn reservedBytes(self: *const Source) usize {
        return self.reserved_source_bytes;
    }

    /// Exact original bytes, useful when publishing a newly trained model's
    /// unchanged sidecars. The caller must not retain them beyond this source.
    pub fn sidecar(self: *const Source, index: usize) ![]const u8 {
        if (index >= bundle.sidecar_names.len) return error.InvalidBoundaryTrainingSource;
        return self.blobs[index + 1].?.bytes;
    }

    pub fn deinit(self: *Source) void {
        const a = self.gate.allocator();
        self.gate.control = null;
        if (self.tokenizer_owner) |tokenizer_owner| tokenizer_owner.deinitSelf();
        self.store.deinitOwned();
        for (self.parameters[0..self.parameter_count]) |parameter| a.free(parameter.dimensions);
        a.free(self.parameters);
        if (self.header) |*header| header.deinit();
        for (&self.blobs) |*blob| if (blob.*) |*value| value.deinit(a);
        std.debug.assert(self.budget.live == 0);
        self.backing.destroy(self);
    }

    fn load(self: *Source, io: std.Io, opened: *const Opened, options: Options, control: ?Control) !void {
        const a = self.gate.allocator();
        // Validate small configuration sidecars before reading the large model.
        for ([_]usize{ 1, 2, 4, 3 }) |index| {
            self.blobs[index] = try snapshot(a, io, opened.files[index].?, opened.stats[index], options.limits, false, control);
            try validateJsonDepth(self.blobs[index].?.bytes, options.limits.max_json_depth, control);
        }
        self.config = try model.parseConfig(a, self.blobs[1].?.bytes, self.blobs[2].?.bytes);
        try validatePublishedShapeConfig(self.config);
        if (options.expected_identity) |expected| {
            if (expected.backbone != self.config.backbone) return error.GlinerBoundaryArtifactMismatch;
            for (expected.sidecars, 1..) |pin, index| try verifyDigest(self.blobs[index].?.digest, pin);
        }
        try validateTokenizer(a, self.blobs[3].?.bytes, self.blobs[4].?.bytes, self.config, control);
        self.blobs[0] = try snapshot(a, io, opened.files[0].?, opened.stats[0], options.limits, true, control);
        if (options.expected_identity) |expected| try verifyDigest(self.blobs[0].?.digest, expected.weight);
        const weights = self.blobs[0].?.bytes;
        const data_offset = try validateHeader(a, weights, options.limits, control);
        const parsed = try safetensors.parseHeader(a, weights);
        self.header = parsed.header;
        std.debug.assert(parsed.data_offset == data_offset);
        try validateRanges(a, &self.header.?, weights, data_offset, control);
        const descriptors = try a.alloc(access.Descriptor, self.header.?.tensors.count());
        defer a.free(descriptors);
        var iterator = self.header.?.tensors.iterator();
        var index: usize = 0;
        while (iterator.next()) |entry| : (index += 1) {
            const meta = entry.value_ptr.*;
            descriptors[index] = .{ .name = entry.key_ptr.*, .shape = meta.shape, .encoding = .{ .dense = meta.dtype }, .byte_len = @intCast(meta.data_end - meta.data_start), .quantized = false };
        }
        _ = try artifact.validate(a, self.config.backbone, .fp32, descriptors, control);
        try self.buildStore(data_offset, control);
        try check(control);
        self.tokenizer_owner = try HfTokenizer.loadFromBytesWithOptions(a, self.blobs[3].?.bytes, .{ .strict_unigram_normalizer = true });
        try validateLoadedTokenizer(a, self.tokenizer_owner.?, self.config, control);
        self.identity = .{ .backbone = self.config.backbone, .precision = .fp32, .weight = self.blobs[0].?.digest, .sidecars = undefined };
        for (&self.identity.sidecars, 1..) |*digest, file_index| digest.* = self.blobs[file_index].?.digest;
        try check(control);
    }

    fn buildStore(self: *Source, data_offset: usize, control: ?Control) !void {
        const a = self.gate.allocator();
        const header = &self.header.?;
        const names = try a.alloc([]const u8, header.tensors.count());
        defer a.free(names);
        var keys = header.tensors.keyIterator();
        var key_index: usize = 0;
        while (keys.next()) |key| : (key_index += 1) names[key_index] = key.*;
        std.mem.sort([]const u8, names, {}, struct {
            fn less(_: void, lhs: []const u8, rhs: []const u8) bool {
                return std.mem.order(u8, lhs, rhs) == .lt;
            }
        }.less);
        self.parameters = try a.alloc(run.Parameter, names.len);
        for (names) |canonical_name| {
            try check(control);
            const meta = header.tensors.get(canonical_name).?;
            const native_name = if (std.mem.startsWith(u8, canonical_name, "encoder.")) canonical_name["encoder.".len..] else canonical_name;
            if (native_name.len == 0 or self.store.resident_weights.contains(native_name)) return error.DuplicateTrainingParameter;
            const owned_name = try a.dupe(u8, native_name);
            errdefer a.free(owned_name);
            const dimensions = try a.alloc(i32, meta.shape.len);
            errdefer a.free(dimensions);
            for (meta.shape, dimensions) |dimension, *destination| destination.* = std.math.cast(i32, dimension) orelse return error.InvalidGlinerBoundaryTensorShape;
            const bytes = self.blobs[0].?.bytes[data_offset + @as(usize, @intCast(meta.data_start)) .. data_offset + @as(usize, @intCast(meta.data_end))];
            const values = std.mem.bytesAsSlice(f32, @as([]align(@alignOf(f32)) u8, @alignCast(bytes)));
            try validateFinite(values, control);
            try self.store.resident_weights.put(a, owned_name, .{ .tensor = .{ .data = bytes, .dtype = .f32, .shape = meta.shape, .name = owned_name, .allocator = a, .owns_data = false, .owns_shape = false, .shared_storage = self.blobs[0].?.bytes } });
            self.parameters[self.parameter_count] = .{ .name = owned_name, .canonical_name = canonical_name, .dimensions = dimensions, .values = values, .kind = .original };
            self.parameter_count += 1;
        }
    }
};

/// Read-only admission probe. Open holds and rechecks its own five descriptors;
/// this function does not create a durable model lease or certify their bytes.
pub fn reservation(io: std.Io, path: []const u8, limits: Limits, control: ?Control) !usize {
    try validateOptions(.{ .limits = limits });
    var opened = try Opened.init(io, path, limits, control);
    defer opened.deinit(io);
    return calculateReservation(opened.sizes, limits);
}

fn calculateReservation(sizes: [5]usize, limits: Limits) !usize {
    var bytes = std.math.add(usize, @sizeOf(Source) + 3, limits.max_auxiliary_bytes) catch return error.BoundaryTrainingSourceLimitExceeded;
    for (sizes, 0..) |size, index| {
        const limit = if (index == 0) limits.max_weight_bytes else if (index == 3) limits.max_tokenizer_bytes else limits.max_config_bytes;
        if (size == 0 or size > limit) return error.BoundaryTrainingSourceLimitExceeded;
        bytes = std.math.add(usize, bytes, size) catch return error.BoundaryTrainingSourceLimitExceeded;
    }
    if (bytes > limits.max_source_bytes) return error.BoundaryTrainingSourceLimitExceeded;
    return bytes;
}

fn validateOptions(options: Options) !void {
    if (builtin.cpu.arch.endian() != .little) return error.UnsupportedBoundaryTrainingSourceEndian;
    const limits = options.limits;
    if (limits.max_source_bytes <= @sizeOf(Source) or limits.max_auxiliary_bytes == 0 or limits.max_weight_bytes == 0 or
        limits.max_tokenizer_bytes == 0 or limits.max_config_bytes == 0 or limits.max_header_bytes == 0 or limits.max_json_depth == 0 or limits.max_json_depth > 128)
        return error.InvalidBoundaryTrainingSourceLimits;
    if (options.expected_identity) |expected| {
        if (expected.precision != .fp32) return error.QuantizedBoundaryTrainingUnsupported;
        try validateDigest(expected.weight);
        for (expected.sidecars) |pin| try validateDigest(pin);
    }
}

const Opened = struct {
    files: [5]?std.Io.File = .{null} ** 5,
    sizes: [5]usize = undefined,
    stats: [5]std.Io.File.Stat = undefined,

    fn init(io: std.Io, path: []const u8, limits: Limits, control: ?Control) !Opened {
        try check(control);
        const directory = try std.Io.Dir.cwd().openDir(io, path, .{});
        defer directory.close(io);
        var self = Opened{};
        errdefer self.deinit(io);
        // All descriptors are pinned before consuming any bytes. Safe HF cache
        // symlinks are allowed; regular-file and content checks apply to their
        // actual targets, with no path-based hash performed later.
        for (file_names, 0..) |name, index| {
            try check(control);
            self.files[index] = try openFileNonblocking(directory, name, control);
            self.stats[index] = try self.files[index].?.stat(io);
            if (self.stats[index].kind != .file) return error.InvalidBoundaryTrainingSourceFile;
            self.sizes[index] = std.math.cast(usize, self.stats[index].size) orelse return error.BoundaryTrainingSourceLimitExceeded;
        }
        _ = try calculateReservation(self.sizes, limits);
        return self;
    }
    fn deinit(self: *Opened, io: std.Io) void {
        for (self.files) |file| if (file) |opened| opened.close(io);
    }
};

fn openFileNonblocking(directory: std.Io.Dir, name: []const u8, control: ?Control) !std.Io.File {
    try check(control);
    // Checking kind only after an ordinary read-only open can block forever
    // on a FIFO. O_NONBLOCK is harmless for positional reads of regular files;
    // non-file targets are rejected by the descriptor stat immediately after.
    // The current native training source profile is Linux/macOS.
    if (builtin.os.tag != .linux and builtin.os.tag != .macos) return error.UnsupportedBoundaryTrainingSourcePlatform;
    var buffer: [1024]u8 = undefined;
    const name_z = try std.fmt.bufPrintZ(&buffer, "{s}", .{name});
    const fd = try std.posix.openatZ(directory.handle, name_z, .{ .ACCMODE = .RDONLY, .CLOEXEC = true, .NONBLOCK = true, .NOCTTY = true }, 0);
    return .{ .handle = fd, .flags = .{ .nonblocking = true } };
}

const Blob = struct {
    storage: []align(64) u8,
    bytes: []u8,
    digest: bundle.Digest,
    fn deinit(self: *Blob, a: Allocator) void {
        a.free(self.storage);
    }
};

fn snapshot(a: Allocator, io: std.Io, file: std.Io.File, initial: std.Io.File.Stat, limits: Limits, weight: bool, control: ?Control) !Blob {
    try check(control);
    if (initial.kind != .file) return error.InvalidBoundaryTrainingSourceFile;
    const size = std.math.cast(usize, initial.size) orelse return error.BoundaryTrainingSourceLimitExceeded;
    if (size == 0 or size > if (weight) limits.max_weight_bytes else @max(limits.max_config_bytes, limits.max_tokenizer_bytes)) return error.BoundaryTrainingSourceLimitExceeded;
    var prefix: [8]u8 = undefined;
    var displacement: usize = 0;
    if (weight) {
        if (size < 8 or try file.readPositionalAll(io, &prefix, 0) != prefix.len) return error.InvalidBoundaryTrainingSource;
        const header_size = std.mem.readInt(u64, &prefix, .little);
        if (header_size == 0 or header_size > limits.max_header_bytes or header_size > size - 8) return error.InvalidBoundaryTrainingSourceHeader;
        // SafeTensors may have an unpadded JSON header. Place the *file copy*
        // within three spare bytes so its payload is aligned without allocating
        // another model-sized copy. Metadata remains byte-addressable.
        displacement = @intCast((4 - header_size % 4) % 4);
    }
    const storage_size = std.math.add(usize, size, displacement) catch return error.BoundaryTrainingSourceLimitExceeded;
    const storage = try a.alignedAlloc(u8, .@"64", storage_size);
    errdefer a.free(storage);
    const bytes = storage[displacement..];
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var offset: usize = 0;
    if (weight) {
        @memcpy(bytes[0..8], &prefix);
        hash.update(&prefix);
        offset = 8;
    }
    while (offset < size) {
        try check(control);
        const end = @min(size, offset +| (256 * 1024));
        if (try file.readPositionalAll(io, bytes[offset..end], offset) != end - offset) return error.BoundaryTrainingSourceChanged;
        hash.update(bytes[offset..end]);
        offset = end;
    }
    var extra: [1]u8 = undefined;
    if (try file.readPositionalAll(io, &extra, size) != 0) return error.BoundaryTrainingSourceChanged;
    const final = try file.stat(io);
    if (final.size != initial.size or final.inode != initial.inode or final.kind != .file or !std.meta.eql(final.mtime, initial.mtime) or !std.meta.eql(final.ctime, initial.ctime)) return error.BoundaryTrainingSourceChanged;
    try check(control);
    return .{ .storage = storage, .bytes = bytes, .digest = .{ .size_bytes = size, .sha256 = std.fmt.bytesToHex(hash.finalResult(), .lower) } };
}

fn validateJsonDepth(bytes: []const u8, maximum: usize, control: ?Control) !void {
    var depth: usize = 0;
    var quoted = false;
    var escaped = false;
    for (bytes, 0..) |byte, index| {
        if (index % 4096 == 0) try check(control);
        if (quoted) {
            if (escaped) escaped = false else if (byte == '\\') escaped = true else if (byte == '"') quoted = false;
            continue;
        }
        switch (byte) {
            '"' => quoted = true,
            '{', '[' => {
                if (depth >= maximum) return error.BoundaryTrainingSourceLimitExceeded;
                depth += 1;
            },
            '}', ']' => {
                if (depth == 0) return error.InvalidBoundaryTrainingSourceJSON;
                depth -= 1;
            },
            else => {},
        }
    }
    if (quoted or depth != 0) return error.InvalidBoundaryTrainingSourceJSON;
    try check(control);
}

fn validateHeader(a: Allocator, bytes: []const u8, limits: Limits, control: ?Control) !usize {
    if (bytes.len < 8) return error.InvalidBoundaryTrainingSourceHeader;
    const length = std.mem.readInt(u64, bytes[0..8], .little);
    if (length == 0 or length > limits.max_header_bytes or length > bytes.len - 8) return error.InvalidBoundaryTrainingSourceHeader;
    const offset: usize = @intCast(length + 8);
    const json = bytes[8..offset];
    try validateJsonDepth(json, limits.max_json_depth, control);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, json, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidBoundaryTrainingSourceHeader;
    var entries = parsed.value.object.iterator();
    while (entries.next()) |entry| {
        try check(control);
        if (entry.value_ptr.* != .object or entry.key_ptr.len == 0 or entry.key_ptr.len > 1024) return error.InvalidBoundaryTrainingSourceHeader;
        if (std.mem.eql(u8, entry.key_ptr.*, "__metadata__")) {
            var metadata = entry.value_ptr.object.iterator();
            while (metadata.next()) |field| if (field.value_ptr.* != .string) return error.InvalidBoundaryTrainingSourceHeader;
            continue;
        }
        const obj = entry.value_ptr.object;
        if (obj.count() != 3 or !obj.contains("dtype") or !obj.contains("shape") or !obj.contains("data_offsets")) return error.InvalidBoundaryTrainingSourceHeader;
        const dtype = obj.get("dtype").?;
        if (dtype != .string or !std.mem.eql(u8, dtype.string, "F32")) return error.QuantizedBoundaryTrainingUnsupported;
    }
    return offset;
}

fn validateRanges(a: Allocator, header: *const safetensors.Header, bytes: []const u8, data_offset: usize, control: ?Control) !void {
    if (data_offset > bytes.len) return error.InvalidBoundaryTrainingSourceHeader;
    const Range = struct { start: usize, end: usize };
    const ranges = try a.alloc(Range, header.tensors.count());
    defer a.free(ranges);
    var entries = header.tensors.iterator();
    var index: usize = 0;
    while (entries.next()) |entry| : (index += 1) {
        try check(control);
        const meta = entry.value_ptr.*;
        if (meta.dtype != .f32) return error.QuantizedBoundaryTrainingUnsupported;
        if (meta.shape.len == 0 or meta.shape.len > 4) return error.InvalidGlinerBoundaryTensorShape;
        var count: usize = 1;
        for (meta.shape) |dimension| {
            if (dimension <= 0 or dimension > std.math.maxInt(i32)) return error.InvalidGlinerBoundaryTensorShape;
            count = std.math.mul(usize, count, @intCast(dimension)) catch return error.InvalidGlinerBoundaryTensorShape;
        }
        const expected = std.math.mul(usize, count, @sizeOf(f32)) catch return error.InvalidGlinerBoundaryTensorShape;
        const start = std.math.cast(usize, meta.data_start) orelse return error.InvalidBoundaryTrainingTensorRange;
        const end = std.math.cast(usize, meta.data_end) orelse return error.InvalidBoundaryTrainingTensorRange;
        if (end < start or end > bytes.len - data_offset or end - start != expected or start % 4 != 0 or (@intFromPtr(bytes.ptr) + data_offset + start) % @alignOf(f32) != 0) return error.InvalidBoundaryTrainingTensorRange;
        ranges[index] = .{ .start = start, .end = end };
    }
    std.mem.sort(Range, ranges, {}, struct {
        fn less(_: void, lhs: Range, rhs: Range) bool {
            return lhs.start < rhs.start;
        }
    }.less);
    var end: usize = 0;
    for (ranges) |range| {
        if (range.start != end) return error.InvalidBoundaryTrainingTensorRange;
        end = range.end;
    }
    if (end != bytes.len - data_offset) return error.InvalidBoundaryTrainingTensorRange;
    try check(control);
}

fn validateFinite(values: []const f32, control: ?Control) !void {
    for (values, 0..) |value, index| {
        if (index % 65536 == 0) try check(control);
        if (!std.math.isFinite(value)) return error.NonFiniteBoundaryTrainingSource;
    }
}

fn validatePublishedShapeConfig(config: model.Config) !void {
    const h = config.head;
    // Tensor inventory v1 fixes these architecture switches. Numerical loss,
    // threshold and sampling settings are interpreted from the verified config.
    if (h.boundary_dim != 128 or h.pair_dim != 128 or h.content_dim != 64 or h.record_dim != 128 or h.record_instance_queries != 32 or
        h.boundary_refinement_layers != 1 or h.boundary_ffn_multiplier != 2 or h.boundary_attention_layers != 2 or h.boundary_attention_heads != 4 or
        h.candidate_attention_layers != 0 or h.query_attention_layers != 0 or h.candidate_pool != .shared or h.content_soft_max_pool or h.dropout == 0 or
        !h.enable_span_content or !h.enable_rotary_endpoints or !h.query_conditioned_inside_weight or !h.endpoint_difference_features or
        !h.reranker_endpoint_compat or h.multihead_pair_compat_heads != 8 or !h.enable_abstention or !h.enable_count_head or
        !h.enable_records or !h.enable_relations or !h.directional_relation_states or !h.relation_biaffine_content)
        return error.UnsupportedBoundaryTrainingSourceConfiguration;
}

const markers = [_][]const u8{ "[SEP_STRUCT]", "[SEP_TEXT]", "[P]", "[C]", "[E]", "[R]", "[L]", "[EXAMPLE]", "[OUTPUT]", "[DESCRIPTION]" };

fn validateTokenizer(a: Allocator, bytes: []const u8, config_bytes: []const u8, config: model.Config, control: ?Control) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    const root = try object(parsed.value);
    try requireKeys(root, &.{ "version", "truncation", "padding", "model", "normalizer", "pre_tokenizer", "decoder", "post_processor", "added_tokens" });
    try requireString(root, "version", "1.0");
    if ((try item(root, "padding")) != .null or (try item(root, "truncation")) != .null) return error.UnsupportedBoundaryTrainingTokenizer;
    const unigram = try object(try item(root, "model"));
    try requireKeys(unigram, &.{ "type", "byte_fallback", "unk_id", "vocab" });
    try requireString(unigram, "type", "Unigram");
    try requireBool(unigram, "byte_fallback", false);
    try requireInteger(unigram, "unk_id", 3);
    const vocabulary = try item(unigram, "vocab");
    const base_vocab: usize = if (config.backbone == .multi) 250101 else 128000;
    if (vocabulary != .array or vocabulary.array.items.len != base_vocab) return error.InvalidBoundaryTrainingTokenizer;
    for (vocabulary.array.items, 0..) |piece, index| {
        if (index % 1024 == 0) try check(control);
        if (piece != .array or piece.array.items.len != 2 or piece.array.items[0] != .string or piece.array.items[0].string.len == 0) return error.InvalidBoundaryTrainingTokenizer;
        const score = switch (piece.array.items[1]) {
            .float => |value| value,
            .integer => |value| @as(f64, @floatFromInt(value)),
            else => return error.InvalidBoundaryTrainingTokenizer,
        };
        if (!std.math.isFinite(score)) return error.InvalidBoundaryTrainingTokenizer;
    }
    // These exact executable profiles have independently captured Unicode and
    // preprocessing parity. Unknown pre/post processing cannot default to a
    // nearby tokenizer family or silently alter a training example.
    try requireJson(a, try item(root, "normalizer"),
        \\{"type":"Sequence","normalizers":[{"type":"Replace","pattern":{"Regex":"\\s{2,}|[\\n\\r\\t]"},"content":" "},{"type":"NFC"},{"type":"Strip","strip_left":false,"strip_right":true}]}
    );
    try requireJson(a, try item(root, "pre_tokenizer"),
        \\{"type":"Sequence","pretokenizers":[{"type":"Metaspace","replacement":"▁","prepend_scheme":"always","split":true}]}
    );
    try requireJson(a, try item(root, "decoder"),
        \\{"type":"Metaspace","replacement":"▁","prepend_scheme":"always","split":true}
    );
    try requireJson(a, try item(root, "post_processor"),
        \\{"type":"TemplateProcessing","single":[{"SpecialToken":{"id":"[CLS]","type_id":0}},{"Sequence":{"id":"A","type_id":0}},{"SpecialToken":{"id":"[SEP]","type_id":0}}],"pair":[{"SpecialToken":{"id":"[CLS]","type_id":0}},{"Sequence":{"id":"A","type_id":0}},{"SpecialToken":{"id":"[SEP]","type_id":0}},{"Sequence":{"id":"B","type_id":1}},{"SpecialToken":{"id":"[SEP]","type_id":1}}],"special_tokens":{"[CLS]":{"id":"[CLS]","ids":[1],"tokens":["[CLS]"]},"[SEP]":{"id":"[SEP]","ids":[2],"tokens":["[SEP]"]}}}
    );
    const added = try item(root, "added_tokens");
    if (added != .array or added.array.items.len != if (config.backbone == .multi) @as(usize, 115) else 15) return error.InvalidBoundaryTrainingTokenizer;
    var seen = std.AutoHashMapUnmanaged(i64, void){};
    defer seen.deinit(a);
    for (added.array.items) |value| {
        try check(control);
        const token = try object(value);
        try requireKeys(token, &.{ "id", "content", "single_word", "lstrip", "rstrip", "normalized", "special" });
        const id = try item(token, "id");
        const content = try item(token, "content");
        if (id != .integer or id.integer < 0 or id.integer >= config.encoder.vocab_size or content != .string or content.string.len == 0) return error.InvalidBoundaryTrainingTokenizer;
        const entry = try seen.getOrPut(a, id.integer);
        if (entry.found_existing) return error.InvalidBoundaryTrainingTokenizer;
        for ([_][]const u8{ "single_word", "lstrip", "rstrip", "normalized" }) |name| try requireBool(token, name, false);
        if (id.integer < base_vocab and !std.mem.eql(u8, vocabulary.array.items[@intCast(id.integer)].array.items[0].string, content.string)) return error.InvalidBoundaryTrainingTokenizer;
        const special = try item(token, "special");
        if (special != .bool) return error.InvalidBoundaryTrainingTokenizer;
        if (id.integer >= base_vocab or id.integer <= 3) {
            const expected: []const u8 = if (id.integer == 0) "[PAD]" else if (id.integer == 1) "[CLS]" else if (id.integer == 2) "[SEP]" else if (id.integer == 3) "[UNK]" else if (id.integer == base_vocab) "[MASK]" else markers[@intCast(id.integer - @as(i64, @intCast(base_vocab)) - 1)];
            if (!special.bool or !std.mem.eql(u8, content.string, expected)) return error.InvalidBoundaryTrainingTokenizer;
        } else if (special.bool or config.backbone != .multi or id.integer < 250001) return error.InvalidBoundaryTrainingTokenizer;
    }
    for ([_]i64{ 0, 1, 2, 3 }) |id| if (!seen.contains(id)) return error.InvalidBoundaryTrainingTokenizer;
    for (base_vocab..config.encoder.vocab_size) |id| if (!seen.contains(@intCast(id))) return error.InvalidBoundaryTrainingTokenizer;
    const tokenizer_config = try std.json.parseFromSlice(std.json.Value, a, config_bytes, .{ .duplicate_field_behavior = .@"error" });
    defer tokenizer_config.deinit();
    const metadata = try object(tokenizer_config.value);
    try requireKeys(metadata, &.{ "add_prefix_space", "backend", "bos_token", "cls_token", "do_lower_case", "eos_token", "extra_special_tokens", "is_local", "local_files_only", "mask_token", "model_max_length", "pad_token", "sep_token", "split_by_punct", "tokenizer_class", "unk_id", "unk_token", "vocab_type" });
    try requireString(metadata, "tokenizer_class", "DebertaV2Tokenizer");
    try requireString(metadata, "backend", "tokenizers");
    try requireString(metadata, "vocab_type", "spm");
    try requireBool(metadata, "add_prefix_space", true);
    try requireBool(metadata, "do_lower_case", false);
    try requireBool(metadata, "split_by_punct", false);
    try requireInteger(metadata, "unk_id", 3);
    for ([_][]const u8{ "bos_token", "cls_token", "eos_token", "sep_token", "pad_token", "unk_token", "mask_token" }, [_][]const u8{ "[CLS]", "[CLS]", "[SEP]", "[SEP]", "[PAD]", "[UNK]", "[MASK]" }) |key, name| try requireString(metadata, key, name);
    const extra = try item(metadata, "extra_special_tokens");
    if (extra != .array or extra.array.items.len != markers.len) return error.InvalidBoundaryTrainingTokenizer;
    for (extra.array.items, markers) |value, name| if (value != .string or !std.mem.eql(u8, value.string, name)) return error.InvalidBoundaryTrainingTokenizer;
    // HF's very large model_max_length sentinel is intentionally not converted
    // to an integer or used for admission. The extraction config and processor
    // ceilings remain the authoritative whole-document/token limits.
    try check(control);
}

fn validateLoadedTokenizer(a: Allocator, loaded: *HfTokenizer, config: model.Config, control: ?Control) !void {
    const tokenizer = loaded.tokenizer();
    const special = tokenizer.specialTokens();
    const base_vocab: i32 = if (config.backbone == .multi) 250101 else 128000;
    if (loaded.model_type != .unigram or loaded.pre_tokenizer_type != .metaspace or tokenizer.vocabSize() != config.encoder.vocab_size or
        special.pad_id != config.encoder.pad_token_id or special.pad_id != 0 or special.cls_id != 1 or special.sep_id != 2 or special.unk_id != 3 or special.mask_id != base_vocab)
        return error.InvalidBoundaryTrainingTokenizer;
    for (0..config.encoder.vocab_size) |index| {
        if (index % 1024 == 0) try check(control);
        if (!loaded.id_to_token.contains(@intCast(index))) return error.InvalidBoundaryTrainingTokenizer;
    }
    for (markers, 0..) |name, index| {
        try check(control);
        const ids = try tokenizer.encode(a, name);
        defer a.free(ids);
        if (ids.len != 1 or ids[0] != base_vocab + 1 + @as(i32, @intCast(index)) or !loaded.special_token_ids.contains(ids[0])) return error.InvalidBoundaryTrainingTokenizer;
    }
}

fn item(obj: std.json.ObjectMap, key: []const u8) !std.json.Value {
    return obj.get(key) orelse error.InvalidBoundaryTrainingTokenizer;
}
fn object(value: std.json.Value) !std.json.ObjectMap {
    if (value != .object) return error.InvalidBoundaryTrainingTokenizer;
    return value.object;
}
fn requireString(obj: std.json.ObjectMap, key: []const u8, expected: []const u8) !void {
    const value = try item(obj, key);
    if (value != .string or !std.mem.eql(u8, value.string, expected)) return error.UnsupportedBoundaryTrainingTokenizer;
}
fn requireBool(obj: std.json.ObjectMap, key: []const u8, expected: bool) !void {
    const value = try item(obj, key);
    if (value != .bool or value.bool != expected) return error.UnsupportedBoundaryTrainingTokenizer;
}
fn requireInteger(obj: std.json.ObjectMap, key: []const u8, expected: i64) !void {
    const value = try item(obj, key);
    if (value != .integer or value.integer != expected) return error.UnsupportedBoundaryTrainingTokenizer;
}
fn requireKeys(obj: std.json.ObjectMap, allowed: []const []const u8) !void {
    var keys = obj.iterator();
    while (keys.next()) |entry| {
        var found = false;
        for (allowed) |key| if (std.mem.eql(u8, key, entry.key_ptr.*)) {
            found = true;
            break;
        };
        if (!found) return error.UnsupportedBoundaryTrainingTokenizer;
    }
}
fn requireJson(a: Allocator, actual: std.json.Value, expected: []const u8) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, a, expected, .{});
    defer parsed.deinit();
    if (!sameJson(actual, parsed.value)) return error.UnsupportedBoundaryTrainingTokenizer;
}
fn sameJson(lhs: std.json.Value, rhs: std.json.Value) bool {
    if (std.meta.activeTag(lhs) != std.meta.activeTag(rhs)) return false;
    return switch (lhs) {
        .null => true,
        .bool => |value| value == rhs.bool,
        .integer => |value| value == rhs.integer,
        .float => |value| value == rhs.float,
        .number_string => |value| std.mem.eql(u8, value, rhs.number_string),
        .string => |value| std.mem.eql(u8, value, rhs.string),
        .array => |values| blk: {
            if (values.items.len != rhs.array.items.len) break :blk false;
            for (values.items, rhs.array.items) |a, b| if (!sameJson(a, b)) break :blk false;
            break :blk true;
        },
        .object => |values| blk: {
            if (values.count() != rhs.object.count()) break :blk false;
            var it = values.iterator();
            while (it.next()) |entry| if (!sameJson(entry.value_ptr.*, rhs.object.get(entry.key_ptr.*) orelse break :blk false)) break :blk false;
            break :blk true;
        },
    };
}

fn validateDigest(digest: bundle.Digest) !void {
    if (digest.size_bytes == 0) return error.InvalidBoundaryTrainingSource;
    for (digest.sha256) |byte| if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return error.InvalidBoundaryTrainingSource;
}
fn verifyDigest(actual: bundle.Digest, expected: bundle.Digest) !void {
    if (actual.size_bytes != expected.size_bytes or !std.mem.eql(u8, &actual.sha256, &expected.sha256)) return error.GlinerBoundaryArtifactMismatch;
}
fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}

/// HfTokenizer's parser has no control argument. Checking every allocation
/// keeps its large vocabulary/normalizer construction cooperative, and records
/// cancellation separately from real memory exhaustion. Frees always proceed.
const AllocationGate = struct {
    budget: *Budget,
    control: ?Control = null,
    failure: ?anyerror = null,
    fn allocator(self: *@This()) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn allowed(self: *@This()) bool {
        if (self.failure != null) return false;
        check(self.control) catch |err| {
            self.failure = err;
            return false;
        };
        return true;
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (!self.allowed()) return null;
        return self.budget.allocator().rawAlloc(len, alignment, ret);
    }
    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (len > bytes.len and !self.allowed()) return false;
        return self.budget.allocator().rawResize(bytes, alignment, len, ret);
    }
    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (len > bytes.len and !self.allowed()) return null;
        return self.budget.allocator().rawRemap(bytes, alignment, len, ret);
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.budget.allocator().rawFree(bytes, alignment, ret);
    }
};

const test_header =
    \\{"encoder.encoder.layer.0.bias":{"dtype":"F32","shape":[2],"data_offsets":[0,8]},"boundary_head.count_head.bias":{"dtype":"F32","shape":[2],"data_offsets":[8,16]},"__metadata__":{"format":"pt"}}
;

fn testBlob(a: Allocator, json: []const u8, values: []const f32) !Blob {
    const data_offset = 8 + json.len;
    const displacement = (4 - data_offset % 4) % 4;
    const storage = try a.alignedAlloc(u8, .@"64", displacement + data_offset + values.len * 4);
    const bytes = storage[displacement..];
    std.mem.writeInt(u64, bytes[0..8], json.len, .little);
    @memcpy(bytes[8..data_offset], json);
    @memcpy(bytes[data_offset..], std.mem.sliceAsBytes(values));
    return .{ .storage = storage, .bytes = bytes, .digest = bundle.Digest.of(bytes) };
}

/// Exercises the ownership primitive using two small tensors. Production open
/// always validates the complete published inventory before this same builder.
fn exerciseSourceParts(a: Allocator) !void {
    const self = try a.create(Source);
    self.* = .{ .backing = a, .budget = .{ .backing = a, .limit = 16 * 1024 * 1024 }, .gate = undefined, .reserved_source_bytes = 16 * 1024 * 1024 + @sizeOf(Source), .store = undefined };
    self.gate = .{ .budget = &self.budget };
    const owned = self.gate.allocator();
    self.store = .{ .allocator = owned, .resident_weights = .{}, .lazy_weights = .{} };
    defer self.deinit();
    self.blobs[0] = try testBlob(owned, test_header, &.{ 1, -2, 3, -4 });
    const data_offset = try validateHeader(owned, self.blobs[0].?.bytes, .{}, null);
    const parsed = try safetensors.parseHeader(owned, self.blobs[0].?.bytes);
    self.header = parsed.header;
    try validateRanges(owned, &self.header.?, self.blobs[0].?.bytes, data_offset, null);
    try self.buildStore(data_offset, null);
    try std.testing.expectEqual(@as(usize, 2), self.parameters.len);
    const encoder = self.parameters[1];
    try std.testing.expectEqualStrings("encoder.layer.0.bias", encoder.name);
    try std.testing.expectEqualStrings("encoder.encoder.layer.0.bias", encoder.canonical_name);
    try std.testing.expectEqualSlices(f32, &.{ 1, -2 }, encoder.values);
    try std.testing.expectEqualSlices(i32, &.{2}, encoder.dimensions);
    const view = self.store.resident_weights.get(encoder.name).?.tensor;
    try std.testing.expect(!view.owns_data and !view.owns_shape);
    try std.testing.expectEqual(@intFromPtr(encoder.values.ptr), @intFromPtr(view.data.ptr));
    try std.testing.expectEqual(@as(usize, 0), @intFromPtr(view.data.ptr) % @alignOf(f32));
    try std.testing.expect(self.usage().live_bytes <= self.reservedBytes());
}

test "boundary training source immutable canonical views share aligned bytes with exact ownership" {
    try exerciseSourceParts(std.testing.allocator);
}

test "boundary training source allocation failures release tensor views metadata and stable budget" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseSourceParts, .{});
}

test "boundary training source rejects non-FP32 metadata invalid ranges nonfinite values and excessive JSON depth" {
    const a = std.testing.allocator;
    var blob = try testBlob(a, test_header, &.{ 1, 2, 3, 4 });
    defer blob.deinit(a);
    const data_offset = try validateHeader(a, blob.bytes, .{}, null);
    var parsed = try safetensors.parseHeader(a, blob.bytes);
    defer parsed.header.deinit();
    try validateRanges(a, &parsed.header, blob.bytes, data_offset, null);
    const entry = parsed.header.tensors.getPtr("boundary_head.count_head.bias").?;
    entry.data_start = 0;
    entry.data_end = 8;
    try std.testing.expectError(error.InvalidBoundaryTrainingTensorRange, validateRanges(a, &parsed.header, blob.bytes, data_offset, null));
    entry.data_start = 12;
    entry.data_end = 20;
    try std.testing.expectError(error.InvalidBoundaryTrainingTensorRange, validateRanges(a, &parsed.header, blob.bytes, data_offset, null));
    entry.data_start = 8;
    entry.data_end = 16;
    entry.dtype = .f16;
    try std.testing.expectError(error.QuantizedBoundaryTrainingUnsupported, validateRanges(a, &parsed.header, blob.bytes, data_offset, null));
    var half = try testBlob(a,
        \\{"a":{"dtype":"F16","shape":[2],"data_offsets":[0,4]}}
    , &.{1});
    defer half.deinit(a);
    try std.testing.expectError(error.QuantizedBoundaryTrainingUnsupported, validateHeader(a, half.bytes, .{}, null));
    var ignored = try testBlob(a,
        \\{"unexpected":7}
    , &.{});
    defer ignored.deinit(a);
    try std.testing.expectError(error.InvalidBoundaryTrainingSourceHeader, validateHeader(a, ignored.bytes, .{}, null));
    var duplicate = try testBlob(a,
        \\{"a":{},"a":{}}
    , &.{});
    defer duplicate.deinit(a);
    try std.testing.expectError(error.DuplicateField, validateHeader(a, duplicate.bytes, .{}, null));
    try std.testing.expectError(error.NonFiniteBoundaryTrainingSource, validateFinite(&.{ 0, std.math.nan(f32) }, null));
    try std.testing.expectError(error.NonFiniteBoundaryTrainingSource, validateFinite(&.{std.math.inf(f32)}, null));
    try std.testing.expectError(error.BoundaryTrainingSourceLimitExceeded, validateJsonDepth("[[[[]]]]", 3, null));
    try validateJsonDepth("{\"quoted\":\"[[[[]]]]\",\"escaped\":\"\\\"\"}", 1, null);
    try std.testing.expectError(error.InvalidBoundaryTrainingSourceJSON, validateJsonDepth("{\"bad\":[]", 64, null));
}

test "boundary training source snapshot handles unpadded headers and survives same-path replacement" {
    const a = std.testing.allocator;
    const compat = @import("../io/compat.zig");
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/source.safetensors", .{temporary.sub_path});
    defer a.free(path);
    // A legal but deliberately unpadded JSON header exercises the displacement.
    const json = if (test_header.len % 4 == 0) test_header ++ " " else test_header;
    var input = try testBlob(a, json, &.{ 1, 2, 3, 4 });
    defer input.deinit(a);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = input.bytes });
    const file = try compat.cwd().openFile(compat.io(), path, .{});
    defer file.close(compat.io());
    var copy = try snapshot(a, compat.io(), file, try file.stat(compat.io()), .{}, true, null);
    defer copy.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), (@intFromPtr(copy.bytes.ptr) + 8 + json.len) % @alignOf(f32));
    try std.testing.expectEqual(input.digest, copy.digest);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = "changed file contents" });
    try std.testing.expectEqualSlices(u8, input.bytes, copy.bytes);
    try std.testing.expectEqual(input.digest, bundle.Digest.of(copy.bytes));
}

test "boundary training source cancellation distinguishes allocator denial and frees prior storage" {
    const Cancel = struct {
        fn checkCancelled(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    const control = Control{ .check_fn = Cancel.checkCancelled };
    var budget = Budget{ .backing = std.testing.allocator, .limit = 32 };
    var gate = AllocationGate{ .budget = &budget };
    const a = gate.allocator();
    const bytes = try a.alloc(u8, 16);
    gate.control = control;
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 1));
    try std.testing.expectEqual(error.Cancelled, gate.failure.?);
    a.free(bytes);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expect(!budget.denied);
    try std.testing.expectError(error.Cancelled, Source.open(std.testing.allocator, @import("../io/compat.zig").io(), "does-not-exist", .{}, control));
    try std.testing.expectError(error.Cancelled, validateFinite(&.{1}, control));
    try std.testing.expectError(error.Cancelled, validateJsonDepth("{}", 64, control));
}

test "boundary training source admission includes tokenizer overhead and validates expected identity before IO" {
    const a = std.testing.allocator;
    const pin = bundle.Digest.of("source");
    var identity = bundle.Identity{ .backbone = .small, .precision = .q4_0, .weight = pin, .sidecars = .{pin} ** 4 };
    try std.testing.expectError(error.QuantizedBoundaryTrainingUnsupported, Source.open(a, @import("../io/compat.zig").io(), "does-not-exist", .{ .expected_identity = identity }, null));
    identity.precision = .fp32;
    identity.weight.sha256[0] = 'z';
    try std.testing.expectError(error.InvalidBoundaryTrainingSource, validateOptions(.{ .expected_identity = identity }));
    const sizes = [5]usize{ 1149461028, 3151, 857, 16035853, 645 };
    const bytes = try calculateReservation(sizes, .{});
    try std.testing.expect(bytes > sizes[0] + sizes[3] + 384 * 1024 * 1024);
    try std.testing.expect(bytes + 6 * 1024 * 1024 * 1024 + 4 * 1024 * 1024 * 1024 + 512 * 1024 * 1024 <= 12 * 1024 * 1024 * 1024);
    try std.testing.expectError(error.BoundaryTrainingSourceLimitExceeded, calculateReservation(sizes, .{ .max_source_bytes = bytes - 1 }));
    try std.testing.expectError(error.BoundaryTrainingSourceLimitExceeded, calculateReservation(sizes, .{ .max_tokenizer_bytes = 8 }));
    try std.testing.expectError(error.GlinerBoundaryArtifactMismatch, verifyDigest(bundle.Digest.of("abc"), bundle.Digest.of("abd")));
}

test "boundary training source rejects FIFO artifacts without waiting for a writer" {
    if ((builtin.os.tag != .linux and builtin.os.tag != .macos) or !@import("build_options").link_libc) return error.SkipZigTest;
    const Posix = struct {
        extern "c" fn mkfifo(path: [*:0]const u8, mode: std.c.mode_t) c_int;
    };
    const compat = @import("../io/compat.zig");
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    var directory_buffer: [256]u8 = undefined;
    const directory = try std.fmt.bufPrint(&directory_buffer, ".zig-cache/tmp/{s}", .{temporary.sub_path});
    var path_buffer: [512]u8 = undefined;
    const path = try std.fmt.bufPrintZ(&path_buffer, "{s}/model.safetensors", .{directory});
    if (Posix.mkfifo(path, 0o600) != 0) return error.TestFifoCreationFailed;
    try std.testing.expectError(error.InvalidBoundaryTrainingSourceFile, reservation(compat.io(), directory, .{}, null));
}

test "boundary training source accepts pinned architecture configs and preflights unsupported head layouts" {
    const a = std.testing.allocator;
    const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
    for ([_][]const u8{ "base", "small", "multi" }) |name| {
        const config_path = try std.fmt.allocPrint(a, "models/{s}/config.json", .{name});
        defer a.free(config_path);
        const encoder_path = try std.fmt.allocPrint(a, "models/{s}/encoder_config.json", .{name});
        defer a.free(encoder_path);
        const config_bytes = try fixtures.fixtureBytes(a, config_path);
        defer a.free(config_bytes);
        const encoder_bytes = try fixtures.fixtureBytes(a, encoder_path);
        defer a.free(encoder_bytes);
        var config = try model.parseConfig(a, config_bytes, encoder_bytes);
        try validatePublishedShapeConfig(config);
        config.head.content_soft_max_pool = true;
        try std.testing.expectError(error.UnsupportedBoundaryTrainingSourceConfiguration, validatePublishedShapeConfig(config));
        config.head.content_soft_max_pool = false;
        config.head.dropout = 0;
        try std.testing.expectError(error.UnsupportedBoundaryTrainingSourceConfiguration, validatePublishedShapeConfig(config));
    }
}

test {
    _ = @import("gliner_boundary_training_source_test.zig");
}
