// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Additive, input-only execution contract for a materialized trained model.
//! This deliberately does not reinterpret any frozen bundle/evaluation fixture.
const std = @import("std");
const inference = @import("inference_internal");
const bundle = inference.models.gliner_boundary_bundle;
const files = inference.file_snapshot;
const Budget = inference.runtime.bounded_allocator.BoundedAllocator;
const Allocator = std.mem.Allocator;
const Control = inference.InferenceExecutionControl;
const mib = 1024 * 1024;

pub const scope = "gliner25_trained_artifact_execution/v1";
pub const input_scope = "gliner25_trained_artifact_inputs/v1";
pub const source_commit = "3c913c7369301133d3b7699252074c4303ada50e";
pub const math_policy = "strict_f32_activations_v1";
pub const receipt_name = "antfly_gliner25_merge.json";
pub const setup_bytes = 8 * mib;
pub const max_fixture_bytes = 64 * 1024;
pub const max_receipt_bytes = 64 * 1024;
pub const max_weight_bytes = 2 * 1024 * mib;
pub const input_pin = bundle.Digest{ .size_bytes = 5965, .sha256 = "d9414f1d7a2138ed0a9211285bcee5f662667747a31b255ca1708f48a7966abd".* };
pub const request_pin = bundle.Digest{ .size_bytes = 3467, .sha256 = "030c979419cee5fd209ba6a6f23f2e0cc7744e396d4bf3357925651293f5fe5f".* };
pub const Backend = enum { native, metal };

// All fields are mandatory: resource-profile changes create explicit evidence.
// They do not change inference equations, input limits, or comparison tolerance.
pub const Limits = struct {
    version: u32,
    loader_host_bytes: usize,
    request_scratch_bytes: usize,
    encoder_device_bytes: usize,
    head_device_bytes: usize,
    proposal_download_bytes: usize,
    result_download_bytes: usize,
    combined_bytes: usize,
    event_bytes: usize,
    total_output_bytes: usize,
    startup_timeout_ms: u64,
    request_timeout_ms: u64,
    total_timeout_ms: u64,

    pub fn validate(self: Limits) !void {
        if (self.version != 1 or self.loader_host_bytes < mib or self.loader_host_bytes > 1024 * mib or
            self.request_scratch_bytes < mib or self.request_scratch_bytes > 2 * 1024 * mib or
            self.encoder_device_bytes < mib or self.encoder_device_bytes > 4 * 1024 * mib or
            self.head_device_bytes < mib or self.head_device_bytes > 1024 * mib or
            self.proposal_download_bytes == 0 or self.proposal_download_bytes > 64 * mib or
            self.result_download_bytes == 0 or self.result_download_bytes > 64 * mib or
            self.combined_bytes < mib or self.combined_bytes > 12 * 1024 * mib or
            self.event_bytes == 0 or self.event_bytes > 4 * mib or
            self.total_output_bytes < self.event_bytes or self.total_output_bytes > 64 * mib or
            self.startup_timeout_ms == 0 or self.startup_timeout_ms > 180000 or
            self.request_timeout_ms == 0 or self.request_timeout_ms > 120000 or
            self.total_timeout_ms < self.startup_timeout_ms or self.total_timeout_ms < self.request_timeout_ms or
            self.total_timeout_ms > 1800000) return error.InvalidTrainedExecutionLimits;
    }
};

pub const default_limits = Limits{
    .version = 1,
    .loader_host_bytes = 128 * mib,
    .request_scratch_bytes = 128 * mib,
    .encoder_device_bytes = 512 * mib,
    .head_device_bytes = 128 * mib,
    .proposal_download_bytes = 8 * mib,
    .result_download_bytes = 8 * mib,
    .combined_bytes = 3 * 1024 * mib,
    .event_bytes = 4 * mib,
    .total_output_bytes = 64 * mib,
    .startup_timeout_ms = 180000,
    .request_timeout_ms = 120000,
    .total_timeout_ms = 600000,
};

// Required-field wire equivalent of the production merge receipt. Keeping the
// v1 consumer local avoids importing training implementations into this bench.
pub const Receipt = struct {
    family: []const u8,
    version: u32,
    architecture_version: u32,
    config_version: u32,
    tensor_policy_version: u32,
    math_policy: []const u8,
    source: bundle.Identity,
    provenance: struct {
        configuration: bundle.Digest,
        adapter_files: struct { config: bundle.Digest, weights: bundle.Digest, receipt: ?bundle.Digest },
        schema_sha256: [32]u8,
    },
    target_sha256: [32]u8,
    parameter_sha256: [32]u8,
    merged: bundle.Identity,
    tensor_count: usize,
    merged_tensor_count: usize,
};
pub const Envelope = struct {
    version: u32,
    scope: []const u8,
    qualification: bool,
    backend: Backend,
    inputs: bundle.Digest,
    oracle_report: bundle.Digest,
    merge_receipt: bundle.Digest,
    merge: Receipt,
    limits: Limits,
};
pub const Case = struct {
    id: []const u8,
    kind: enum { extract, classification, joint_ie },
    text: []const u8,
    schema: std.json.Value,
};
pub const Inputs = struct {
    version: u32,
    scope: []const u8,
    source_commit: []const u8,
    requests: bundle.Digest,
    offset_unit: enum { unicode_codepoints },
    word_splitter: enum { whitespace },
    threshold: f32,
    cases: []const Case,
};

pub const Admission = struct {
    setup_bytes: usize,
    mapped_weight_bytes: usize,
    loader_host_bytes: usize,
    request_weight_copy_bytes: usize,
    request_scratch_bytes: usize,
    request_host_bytes: usize,
    device_context_bytes: usize,
    host_total_bytes: usize,
    backend_total_bytes: usize,
    combined_total_bytes: usize,
};
fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.TrainedExecutionLimitExceeded;
}
pub fn admission(envelope: Envelope) !Admission {
    try envelope.limits.validate();
    const weight_bytes = std.math.cast(usize, envelope.merge.merged.weight.size_bytes) orelse return error.TrainedExecutionLimitExceeded;
    if (weight_bytes == 0 or weight_bytes > max_weight_bytes) return error.TrainedExecutionLimitExceeded;
    const weight_copy = if (envelope.backend == .metal) weight_bytes else 0;
    const device_bytes = if (envelope.backend == .metal) try add(envelope.limits.encoder_device_bytes, envelope.limits.head_device_bytes) else 0;
    const request_bytes = try add(weight_copy, envelope.limits.request_scratch_bytes);
    const host = try add(try add(setup_bytes, weight_bytes), try add(envelope.limits.loader_host_bytes, request_bytes));
    const combined = try add(host, device_bytes);
    if (combined > envelope.limits.combined_bytes) return error.TrainedExecutionLimitExceeded;
    return .{ .setup_bytes = setup_bytes, .mapped_weight_bytes = weight_bytes, .loader_host_bytes = envelope.limits.loader_host_bytes, .request_weight_copy_bytes = weight_copy, .request_scratch_bytes = envelope.limits.request_scratch_bytes, .request_host_bytes = request_bytes, .device_context_bytes = device_bytes, .host_total_bytes = host, .backend_total_bytes = device_bytes, .combined_total_bytes = combined };
}

fn same(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}
pub fn equalIdentity(a: bundle.Identity, b: bundle.Identity) bool {
    return std.meta.eql(a, b);
}
fn validDigest(value: bundle.Digest, maximum: u64) !void {
    if (value.size_bytes == 0 or value.size_bytes > maximum) return error.InvalidTrainedExecutionFixture;
    for (value.sha256) |c| if (!((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'))) return error.InvalidTrainedExecutionFixture;
}
fn validateIdentity(value: bundle.Identity) !void {
    if (value.precision != .fp32) return error.InvalidTrainedExecutionFixture;
    try validDigest(value.weight, max_weight_bytes);
    for (value.sidecars, 0..) |sidecar, i| try validDigest(sidecar, if (i == 2) 32 * mib else mib);
}
fn validateReceipt(value: Receipt) !void {
    if (!same(value.family, "gliner_boundary_merge/v1") or value.version != 1 or value.architecture_version != 1 or
        value.config_version != 3 or value.tensor_policy_version != 1 or
        !same(value.math_policy, "f32_lora_delta_f64_dora_row_norm_v1") or value.tensor_count != 334 or
        value.merged_tensor_count == 0 or value.merged_tensor_count > 131) return error.InvalidTrainedExecutionFixture;
    try validateIdentity(value.source);
    try validateIdentity(value.merged);
    if (value.source.backbone != value.merged.backbone or !std.meta.eql(value.source.sidecars, value.merged.sidecars))
        return error.InvalidTrainedExecutionFixture;
    try validDigest(value.provenance.configuration, max_fixture_bytes);
    try validDigest(value.provenance.adapter_files.config, mib);
    try validDigest(value.provenance.adapter_files.weights, 1024 * mib);
    // This proof requires a completed Antfly training export, as does the
    // independent PEFT checker. The general merge API also accepts no receipt.
    try validDigest(value.provenance.adapter_files.receipt orelse return error.InvalidTrainedExecutionFixture, mib);
}
fn equalReceipt(a: Receipt, b: Receipt) bool {
    return same(a.family, b.family) and a.version == b.version and a.architecture_version == b.architecture_version and
        a.config_version == b.config_version and a.tensor_policy_version == b.tensor_policy_version and same(a.math_policy, b.math_policy) and
        equalIdentity(a.source, b.source) and equalIdentity(a.merged, b.merged) and std.meta.eql(a.provenance, b.provenance) and
        std.meta.eql(a.target_sha256, b.target_sha256) and std.meta.eql(a.parameter_sha256, b.parameter_sha256) and
        a.tensor_count == b.tensor_count and a.merged_tensor_count == b.merged_tensor_count;
}
pub fn validate(envelope: Envelope, backend: Backend) !void {
    if (envelope.version != 1 or !same(envelope.scope, scope) or envelope.qualification or envelope.backend != backend or
        !std.meta.eql(envelope.inputs, input_pin)) return error.InvalidTrainedExecutionFixture;
    try validDigest(envelope.oracle_report, 8 * mib);
    try validDigest(envelope.merge_receipt, max_receipt_bytes);
    try validateReceipt(envelope.merge);
    _ = try admission(envelope);
}

fn jsonDepth(bytes: []const u8) !void {
    var quoted = false;
    var escaped = false;
    var depth: usize = 0;
    for (bytes) |c| {
        if (quoted) {
            if (escaped) escaped = false else if (c == '\\') escaped = true else if (c == '"') quoted = false;
        } else switch (c) {
            '"' => quoted = true,
            '{', '[' => {
                depth += 1;
                if (depth > 24) return error.TrainedExecutionLimitExceeded;
            },
            '}', ']' => {
                if (depth == 0) return error.InvalidTrainedExecutionFixture;
                depth -= 1;
            },
            else => {},
        }
    }
    if (quoted or depth != 0) return error.InvalidTrainedExecutionFixture;
}
fn strictTypes(comptime T: type, value: std.json.Value) !void {
    if (T == std.json.Value) return;
    switch (@typeInfo(T)) {
        .int => if (value != .integer or value.integer < 0) return error.InvalidTrainedExecutionFixture,
        .float => if (value != .integer and value != .float) return error.InvalidTrainedExecutionFixture,
        .@"enum" => if (value != .string) return error.InvalidTrainedExecutionFixture,
        .bool => if (value != .bool) return error.InvalidTrainedExecutionFixture,
        .optional => |info| if (value != .null) try strictTypes(info.child, value),
        .@"struct" => |info| {
            if (value != .object) return error.InvalidTrainedExecutionFixture;
            inline for (info.fields) |field| try strictTypes(field.type, value.object.get(field.name) orelse return error.InvalidTrainedExecutionFixture);
        },
        .array => |info| {
            if (info.child == u8 and value == .string) return;
            if (value != .array or value.array.items.len != info.len) return error.InvalidTrainedExecutionFixture;
            for (value.array.items) |item| try strictTypes(info.child, item);
        },
        .pointer => |info| {
            if (info.size != .slice) @compileError("unsupported execution wire pointer");
            if (info.child == u8) {
                if (value != .string) return error.InvalidTrainedExecutionFixture;
            } else {
                if (value != .array) return error.InvalidTrainedExecutionFixture;
                for (value.array.items) |item| try strictTypes(info.child, item);
            }
        },
        else => @compileError("unsupported execution wire type"),
    }
}
fn parse(comptime T: type, a: Allocator, bytes: []const u8, maximum: usize) !std.json.Parsed(T) {
    if (bytes.len == 0 or bytes.len > maximum) return error.TrainedExecutionLimitExceeded;
    try jsonDepth(bytes);
    const raw = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .duplicate_field_behavior = .@"error" });
    defer raw.deinit();
    try strictTypes(T, raw.value);
    return std.json.parseFromSlice(T, a, bytes, .{ .allocate = .alloc_always, .duplicate_field_behavior = .@"error" });
}

/// The parser heap is itself bounded and remains alive through all execution.
/// Terminal allocation attribution never mistakes an allocator resize fallback
/// for a declared-cap denial.
pub const Owner = struct {
    backing: Allocator,
    budget: Budget,
    failure: ?Budget.AllocationFailure = null,

    pub fn create(a: Allocator, limit: usize) !*Owner {
        if (limit <= @sizeOf(Owner)) return error.TrainedExecutionLimitExceeded;
        const self = try a.create(Owner);
        self.* = .{ .backing = a, .budget = .{ .backing = a, .limit = limit - @sizeOf(Owner), .failure_context = self, .allocation_failed = failed } };
        return self;
    }
    fn failed(raw: ?*anyopaque, failure: Budget.AllocationFailure) void {
        const self: *Owner = @ptrCast(@alignCast(raw.?));
        self.failure = failure;
    }
    pub fn allocator(self: *Owner) Allocator {
        return self.budget.allocator();
    }
    pub fn mapError(self: *const Owner, err: anyerror) anyerror {
        if ((err == error.OutOfMemory or err == error.WriteFailed) and self.failure != null and self.failure.?.kind == .declared_limit)
            return error.TrainedExecutionLimitExceeded;
        return err;
    }
    pub fn destroy(self: *Owner) void {
        std.debug.assert(self.budget.live == 0);
        self.backing.destroy(self);
    }
};

pub const Verified = struct {
    owner: *Owner,
    envelope: std.json.Parsed(Envelope),
    inputs: std.json.Parsed(Inputs),
    fixture_digest: bundle.Digest,

    pub fn deinit(self: *Verified) void {
        self.inputs.deinit();
        self.envelope.deinit();
        self.owner.destroy();
        self.* = undefined;
    }
    pub fn fromBytes(a: Allocator, fixture: []const u8, inputs: []const u8, receipt: []const u8, backend: Backend) !Verified {
        const owner = try Owner.create(a, setup_bytes);
        errdefer owner.destroy();
        return init(owner, fixture, inputs, receipt, backend) catch |err| return owner.mapError(err);
    }
    fn init(owner: *Owner, fixture: []const u8, inputs: []const u8, receipt: []const u8, backend: Backend) !Verified {
        const a = owner.allocator();
        const envelope = try parse(Envelope, a, fixture, max_fixture_bytes);
        errdefer envelope.deinit();
        try validate(envelope.value, backend);
        if (!std.meta.eql(bundle.Digest.of(inputs), input_pin) or !std.meta.eql(bundle.Digest.of(receipt), envelope.value.merge_receipt))
            return error.TrainedExecutionArtifactMismatch;
        const parsed_receipt = try parse(Receipt, a, receipt, max_receipt_bytes);
        defer parsed_receipt.deinit();
        if (!equalReceipt(parsed_receipt.value, envelope.value.merge)) return error.TrainedExecutionArtifactMismatch;
        const parsed_inputs = try parse(Inputs, a, inputs, @intCast(input_pin.size_bytes));
        errdefer parsed_inputs.deinit();
        const source = parsed_inputs.value;
        if (source.version != 1 or !same(source.scope, input_scope) or !same(source.source_commit, source_commit) or
            !std.meta.eql(source.requests, request_pin) or source.threshold != 0.5 or source.cases.len != 10) return error.InvalidTrainedExecutionFixture;
        for (source.cases, 0..) |case, index| {
            if (case.id.len == 0 or case.schema != .object or case.text.len > mib or !std.unicode.utf8ValidateSlice(case.text)) return error.InvalidTrainedExecutionFixture;
            for (source.cases[0..index]) |prior| if (same(prior.id, case.id)) return error.InvalidTrainedExecutionFixture;
        }
        return .{ .owner = owner, .envelope = envelope, .inputs = parsed_inputs, .fixture_digest = bundle.Digest.of(fixture) };
    }
    pub fn open(a: Allocator, io: std.Io, directory: []const u8, fixture_path: []const u8, inputs_path: []const u8, backend: Backend, control: ?Control) !Verified {
        const owner = try Owner.create(a, setup_bytes);
        errdefer owner.destroy();
        return openOwned(owner, io, directory, fixture_path, inputs_path, backend, control) catch |err| return owner.mapError(err);
    }
    fn openOwned(owner: *Owner, io: std.Io, directory: []const u8, fixture_path: []const u8, inputs_path: []const u8, backend: Backend, control: ?Control) !Verified {
        const a = owner.allocator();
        const fixture = try files.read(a, io, .cwd(), fixture_path, max_fixture_bytes, control);
        defer a.free(fixture);
        const input_bytes = try files.read(a, io, .cwd(), inputs_path, @intCast(input_pin.size_bytes), control);
        defer a.free(input_bytes);
        const receipt_path = try std.fs.path.join(a, &.{ directory, receipt_name });
        defer a.free(receipt_path);
        const receipt = try files.read(a, io, .cwd(), receipt_path, max_receipt_bytes, control);
        defer a.free(receipt);
        return init(owner, fixture, input_bytes, receipt, backend);
    }
};

fn digestFile(a: Allocator, io: std.Io, directory: []const u8, name: []const u8, expected: bundle.Digest, control: ?Control) !void {
    const path = try std.fs.path.join(a, &.{ directory, name });
    defer a.free(path);
    const actual = try files.digest(io, .cwd(), path, expected.size_bytes, control);
    if (actual.size_bytes != expected.size_bytes or !same(&std.fmt.bytesToHex(actual.sha256, .lower), &expected.sha256))
        return error.TrainedExecutionArtifactMismatch;
}
/// The manifest must see only the six files bound by the execution receipt.
/// In particular, an unbound GGUF or shard index must never select a different
/// loader or acquire additional mapped weights before the identity comparison.
/// Iteration has constant storage and rejects a seventh entry immediately.
fn verifyTree(io: std.Io, directory: []const u8, control: ?Control) !void {
    if (control) |active| try active.check();
    if ((try std.Io.Dir.cwd().statFile(io, directory, .{ .follow_symlinks = false })).kind != .directory)
        return error.InvalidTrainedExecutionArtifact;
    const root = try std.Io.Dir.cwd().openDir(io, directory, .{ .iterate = true, .follow_symlinks = false });
    defer root.close(io);
    const names = [_][]const u8{ "model.safetensors", "config.json", "tokenizer.json", "tokenizer_config.json", receipt_name, "encoder_config" };
    var seen = [_]bool{false} ** names.len;
    var iterator = root.iterate();
    while (try iterator.next(io)) |entry| {
        if (control) |active| try active.check();
        const index = for (names, 0..) |name, i| {
            if (same(name, entry.name)) break i;
        } else return error.InvalidTrainedExecutionArtifact;
        if (seen[index]) return error.InvalidTrainedExecutionArtifact;
        seen[index] = true;
        const expected: std.Io.File.Kind = if (index == names.len - 1) .directory else .file;
        if ((try root.statFile(io, entry.name, .{ .follow_symlinks = false })).kind != expected)
            return error.InvalidTrainedExecutionArtifact;
    }
    for (seen) |present| if (!present) return error.InvalidTrainedExecutionArtifact;
    const encoder = try root.openDir(io, "encoder_config", .{ .iterate = true, .follow_symlinks = false });
    defer encoder.close(io);
    var children = encoder.iterate();
    var found = false;
    while (try children.next(io)) |entry| {
        if (control) |active| try active.check();
        if (found or !same(entry.name, "config.json") or
            (try encoder.statFile(io, entry.name, .{ .follow_symlinks = false })).kind != .file)
            return error.InvalidTrainedExecutionArtifact;
        found = true;
    }
    if (!found) return error.InvalidTrainedExecutionArtifact;
}
/// Reused before the first model loader and after the model has been closed.
/// A replacement path, truncated data, or changed same-size payload fails.
pub fn verifyArtifacts(a: Allocator, io: std.Io, directory: []const u8, envelope: Envelope, control: ?Control) !void {
    try validate(envelope, envelope.backend);
    try verifyTree(io, directory, control);
    try digestFile(a, io, directory, receipt_name, envelope.merge_receipt, control);
    try digestFile(a, io, directory, "model.safetensors", envelope.merge.merged.weight, control);
    for (bundle.sidecar_names, envelope.merge.merged.sidecars) |name, pin| try digestFile(a, io, directory, name, pin, control);
    // The materializer writes an aligned header and F32 contiguous payloads.
    // Require alignment before creating a borrowing model session, so its
    // loader owner cannot silently acquire a second full weight-file copy.
    const path = try std.fs.path.join(a, &.{ directory, "model.safetensors" });
    defer a.free(path);
    const file = try files.openRegular(io, .cwd(), path, control);
    defer file.close(io);
    var header: [8]u8 = undefined;
    if (try file.readPositionalAll(io, &header, 0) != 8) return error.InvalidTrainedExecutionArtifact;
    const header_bytes = std.mem.readInt(u64, &header, .little);
    if (header_bytes == 0 or header_bytes > 4 * mib or header_bytes % 8 != 0 or header_bytes > envelope.merge.merged.weight.size_bytes -| 8)
        return error.InvalidTrainedExecutionArtifact;
}
pub fn verifyInputs(a: Allocator, io: std.Io, fixture_path: []const u8, inputs_path: []const u8, fixture: bundle.Digest, control: ?Control) !void {
    try digestFile(a, io, "", fixture_path, fixture, control);
    try digestFile(a, io, "", inputs_path, input_pin, control);
}
pub fn canonicalRequestDigest(a: Allocator, case: Case) !bundle.Digest {
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .text = case.text, .schema = case.schema }, .{});
    defer a.free(bytes);
    return bundle.Digest.of(bytes);
}

fn testInputs(a: Allocator) ![]u8 {
    // The dedicated test target runs from the inference package directory.
    return files.read(a, std.testing.io, .cwd(), "testdata/gliner25/trained_execution_inputs_v1.json", @intCast(input_pin.size_bytes), null);
}
fn testReceipt(weight: bundle.Digest, sidecars: [4]bundle.Digest) Receipt {
    return .{ .family = "gliner_boundary_merge/v1", .version = 1, .architecture_version = 1, .config_version = 3, .tensor_policy_version = 1, .math_policy = "f32_lora_delta_f64_dora_row_norm_v1", .source = .{ .backbone = .small, .precision = .fp32, .weight = bundle.Digest.of("source weights"), .sidecars = sidecars }, .provenance = .{ .configuration = bundle.Digest.of("job bytes"), .schema_sha256 = [_]u8{5} ** 32, .adapter_files = .{ .config = bundle.Digest.of("adapter config"), .weights = bundle.Digest.of("adapter weights"), .receipt = bundle.Digest.of("adapter receipt") } }, .target_sha256 = [_]u8{6} ** 32, .parameter_sha256 = [_]u8{7} ** 32, .merged = .{ .backbone = .small, .precision = .fp32, .weight = weight, .sidecars = sidecars }, .tensor_count = 334, .merged_tensor_count = 131 };
}
fn testEnvelope(receipt: Receipt, raw: []const u8) Envelope {
    return .{ .version = 1, .scope = scope, .qualification = false, .backend = .native, .inputs = input_pin, .oracle_report = bundle.Digest.of("completed independently verified Python report"), .merge_receipt = bundle.Digest.of(raw), .merge = receipt, .limits = default_limits };
}
const test_weight = "\x08\x00\x00\x00\x00\x00\x00\x00{}      \x00\x00\x80\x3f";
const test_sidecars = [_][]const u8{ "config", "encoder config", "tokenizer", "tokenizer config" };
fn testSidecarPins() [4]bundle.Digest {
    var result: [4]bundle.Digest = undefined;
    for (test_sidecars, &result) |bytes, *pin| pin.* = bundle.Digest.of(bytes);
    return result;
}

test "trained execution exact input-only contract pins all ten schemas with owned parsing" {
    const a = std.testing.allocator;
    const inputs = try testInputs(a);
    defer a.free(inputs);
    const receipt = testReceipt(bundle.Digest.of(test_weight), testSidecarPins());
    const receipt_bytes = try std.json.Stringify.valueAlloc(a, receipt, .{});
    defer a.free(receipt_bytes);
    const fixture = try std.json.Stringify.valueAlloc(a, testEnvelope(receipt, receipt_bytes), .{});
    defer a.free(fixture);
    var verified = try Verified.fromBytes(a, fixture, inputs, receipt_bytes, .native);
    defer verified.deinit();
    try std.testing.expectEqual(@as(usize, 10), verified.inputs.value.cases.len);
    try std.testing.expectEqualStrings("c2150f5ac1064099e1c6ccbd2acccababee11e4c182dc668cb4f79eed6c87b26", &(try canonicalRequestDigest(a, verified.inputs.value.cases[0])).sha256);
    try std.testing.expectEqualStrings("eb4254eafa770fda81468c81874ddd9d5a2f9219d62f5a9a211bb8d65b5a10e6", &(try canonicalRequestDigest(a, verified.inputs.value.cases[1])).sha256);
    // Parsers retain independent strings, maps, and receipt fields. Mutating
    // the caller's source bytes cannot alter an admitted execution contract.
    @memset(fixture, ' ');
    @memset(inputs, ' ');
    @memset(receipt_bytes, ' ');
    try std.testing.expectEqualStrings("mixed_tasks", verified.inputs.value.cases[0].id);
    try std.testing.expectEqualStrings("gliner_boundary_merge/v1", verified.envelope.value.merge.family);
}

test "trained execution rejects changed source receipt inputs malformed and truncated envelopes" {
    const a = std.testing.allocator;
    const inputs = try testInputs(a);
    defer a.free(inputs);
    const receipt = testReceipt(bundle.Digest.of(test_weight), testSidecarPins());
    const raw = try std.json.Stringify.valueAlloc(a, receipt, .{});
    defer a.free(raw);
    const envelope = testEnvelope(receipt, raw);
    const fixture = try std.json.Stringify.valueAlloc(a, envelope, .{});
    defer a.free(fixture);
    try std.testing.expectError(error.InvalidTrainedExecutionFixture, Verified.fromBytes(a, fixture[0 .. fixture.len - 1], inputs, raw, .native));
    try std.testing.expectError(error.InvalidTrainedExecutionFixture, Verified.fromBytes(a, fixture, inputs, raw, .metal));
    const changed_inputs = try a.dupe(u8, inputs);
    defer a.free(changed_inputs);
    changed_inputs[changed_inputs.len - 2] ^= 1;
    try std.testing.expectError(error.TrainedExecutionArtifactMismatch, Verified.fromBytes(a, fixture, changed_inputs, raw, .native));
    try std.testing.expectError(error.TrainedExecutionArtifactMismatch, Verified.fromBytes(a, fixture, inputs, raw[0 .. raw.len - 1], .native));
    for (0..7) |kind| {
        var changed = envelope;
        switch (kind) {
            0 => changed.merge.source.weight = bundle.Digest.of("substitute source"),
            1 => changed.merge.provenance.configuration = bundle.Digest.of("substitute job"),
            2 => changed.merge.provenance.adapter_files.weights = bundle.Digest.of("substitute adapter"),
            3 => changed.merge.provenance.schema_sha256[0] ^= 1,
            4 => changed.merge.target_sha256[0] ^= 1,
            5 => changed.merge.parameter_sha256[0] ^= 1,
            6 => changed.merge.merged.weight = bundle.Digest.of("substitute trained model"),
            else => unreachable,
        }
        const bad = try std.json.Stringify.valueAlloc(a, changed, .{});
        defer a.free(bad);
        try std.testing.expectError(error.TrainedExecutionArtifactMismatch, Verified.fromBytes(a, bad, inputs, raw, .native));
    }
    const quoted_number = try std.mem.replaceOwned(u8, a, fixture, "\"version\":1", "\"version\":\"1\"");
    defer a.free(quoted_number);
    try std.testing.expectError(error.InvalidTrainedExecutionFixture, Verified.fromBytes(a, quoted_number, inputs, raw, .native));
    const duplicate = try std.mem.replaceOwned(u8, a, fixture, "\"version\":1", "\"version\":1,\"version\":1");
    defer a.free(duplicate);
    try std.testing.expectError(error.DuplicateField, Verified.fromBytes(a, duplicate, inputs, raw, .native));
}

test "trained execution admission distinguishes borrowed CPU and owned Metal weight copies" {
    const receipt = testReceipt(.{ .size_bytes = 295567748, .sha256 = [_]u8{'0'} ** 64 }, testSidecarPins());
    var envelope = testEnvelope(receipt, "receipt");
    const cpu = try admission(envelope);
    try std.testing.expectEqual(@as(usize, 0), cpu.request_weight_copy_bytes);
    try std.testing.expectEqual(@as(usize, 0), cpu.backend_total_bytes);
    try std.testing.expectEqual(setup_bytes + 295567748 + 2 * 128 * mib, cpu.combined_total_bytes);
    envelope.backend = .metal;
    const gpu = try admission(envelope);
    try std.testing.expectEqual(@as(usize, 295567748), gpu.request_weight_copy_bytes);
    try std.testing.expectEqual(@as(usize, 640 * mib), gpu.device_context_bytes);
    try std.testing.expectEqual(cpu.combined_total_bytes + 295567748 + 640 * mib, gpu.combined_total_bytes);
    envelope.limits.combined_bytes = gpu.combined_total_bytes - 1;
    try std.testing.expectError(error.TrainedExecutionLimitExceeded, admission(envelope));
    envelope.limits = default_limits;
    envelope.limits.request_scratch_bytes = std.math.maxInt(usize);
    try std.testing.expectError(error.InvalidTrainedExecutionLimits, admission(envelope));
    try std.testing.expectError(error.TrainedExecutionLimitExceeded, add(std.math.maxInt(usize), 1));
    inline for (std.meta.fields(Limits)) |field| {
        var changed = default_limits;
        @field(changed, field.name) = 0;
        try std.testing.expectError(error.InvalidTrainedExecutionLimits, changed.validate());
    }
}

test "trained execution parser reclaims every allocation failure and admits a retry" {
    const a = std.testing.allocator;
    const inputs = try testInputs(a);
    defer a.free(inputs);
    const receipt = testReceipt(bundle.Digest.of(test_weight), testSidecarPins());
    const raw = try std.json.Stringify.valueAlloc(a, receipt, .{});
    defer a.free(raw);
    const fixture = try std.json.Stringify.valueAlloc(a, testEnvelope(receipt, raw), .{});
    defer a.free(fixture);
    const Test = struct {
        fn run(allocator: Allocator, envelope: []const u8, data: []const u8, actual: []const u8) !void {
            var verified = try Verified.fromBytes(allocator, envelope, data, actual, .native);
            defer verified.deinit();
            try std.testing.expectEqual(@as(usize, 10), verified.inputs.value.cases.len);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Test.run, .{ fixture, inputs, raw });
    const owner = try Owner.create(a, 4096);
    defer owner.destroy();
    const owned = owner.allocator();
    try std.testing.expectError(error.OutOfMemory, owned.alloc(u8, 4096));
    try std.testing.expectEqual(error.TrainedExecutionLimitExceeded, owner.mapError(error.OutOfMemory));
    owner.failure = null;
    const retry = try owned.alloc(u8, 16);
    owned.free(retry);
    try std.testing.expectEqual(@as(?Budget.AllocationFailure, null), owner.failure);
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    owner.budget.backing = failing.allocator();
    try std.testing.expectError(error.OutOfMemory, owned.alloc(u8, 16));
    try std.testing.expectEqual(error.OutOfMemory, owner.mapError(error.OutOfMemory));
}

fn writeTestFile(io: std.Io, directory: std.Io.Dir, name: []const u8, bytes: []const u8) !void {
    const file = try directory.createFile(io, name, .{});
    defer file.close(io);
    try file.writeStreamingAll(io, bytes);
}
test "trained execution preloader and final verification reject same size substitutions and cancellation" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    try temporary.dir.createDir(io, "encoder_config", .default_dir);
    const directory = try temporary.dir.realPathFileAlloc(io, ".", a);
    defer a.free(directory);
    const receipt = testReceipt(bundle.Digest.of(test_weight), testSidecarPins());
    const raw = try std.json.Stringify.valueAlloc(a, receipt, .{});
    defer a.free(raw);
    const envelope = testEnvelope(receipt, raw);
    try writeTestFile(io, temporary.dir, receipt_name, raw);
    try writeTestFile(io, temporary.dir, "model.safetensors", test_weight);
    for (bundle.sidecar_names, test_sidecars) |name, bytes| try writeTestFile(io, temporary.dir, name, bytes);
    try verifyArtifacts(a, io, directory, envelope, null);
    // All expected bytes can be correct while an extra manifest carrier would
    // change loader selection. Reject these before reading any model tensor.
    for ([_][]const u8{ "model.gguf", "model.safetensors.index.json", "encoder_config/extra.json" }) |extra| {
        try writeTestFile(io, temporary.dir, extra, "unbound artifact");
        try std.testing.expectError(error.InvalidTrainedExecutionArtifact, verifyArtifacts(a, io, directory, envelope, null));
        try temporary.dir.deleteFile(io, extra);
        try verifyArtifacts(a, io, directory, envelope, null);
    }
    try temporary.dir.deleteFile(io, "tokenizer.json");
    try std.testing.expectError(error.InvalidTrainedExecutionArtifact, verifyArtifacts(a, io, directory, envelope, null));
    try temporary.dir.symLink(io, "config.json", "tokenizer.json", .{});
    try std.testing.expectError(error.InvalidTrainedExecutionArtifact, verifyArtifacts(a, io, directory, envelope, null));
    try temporary.dir.deleteFile(io, "tokenizer.json");
    try temporary.dir.createDir(io, "tokenizer.json", .default_dir);
    try std.testing.expectError(error.InvalidTrainedExecutionArtifact, verifyArtifacts(a, io, directory, envelope, null));
    try temporary.dir.deleteDir(io, "tokenizer.json");
    try writeTestFile(io, temporary.dir, "tokenizer.json", "tokenizer");
    try temporary.dir.deleteFile(io, "encoder_config/config.json");
    try std.testing.expectError(error.InvalidTrainedExecutionArtifact, verifyArtifacts(a, io, directory, envelope, null));
    try temporary.dir.symLink(io, "../config.json", "encoder_config/config.json", .{});
    try std.testing.expectError(error.InvalidTrainedExecutionArtifact, verifyArtifacts(a, io, directory, envelope, null));
    try temporary.dir.deleteFile(io, "encoder_config/config.json");
    try writeTestFile(io, temporary.dir, "encoder_config/config.json", "encoder config");
    // This exact verifier is called before createNative/MetalSession and again
    // after those owners close; no model or expected-output fixture is needed.
    const changed = try a.dupe(u8, test_weight);
    defer a.free(changed);
    changed[changed.len - 1] ^= 1;
    try writeTestFile(io, temporary.dir, "model.safetensors", changed);
    try std.testing.expectError(error.TrainedExecutionArtifactMismatch, verifyArtifacts(a, io, directory, envelope, null));
    try writeTestFile(io, temporary.dir, "model.safetensors", test_weight);
    try writeTestFile(io, temporary.dir, "tokenizer.json", "Tokenizer");
    try std.testing.expectError(error.TrainedExecutionArtifactMismatch, verifyArtifacts(a, io, directory, envelope, null));
    try writeTestFile(io, temporary.dir, "tokenizer.json", "tokenizer");
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, verifyArtifacts(a, io, directory, envelope, .{ .check_fn = Cancel.check }));
    try verifyArtifacts(a, io, directory, envelope, null);
    const Test = struct {
        fn run(allocator: Allocator, test_io: std.Io, path: []const u8, expected: Envelope) !void {
            try verifyArtifacts(allocator, test_io, path, expected, null);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Test.run, .{ io, directory, envelope });
}
