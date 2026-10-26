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

//! Strict loader for renderer-produced numerical capture inputs. Paths only
//! locate campaign files; every byte is independently hashed and all prompt,
//! question, study, loaded-model, executable, and source-receipt bindings are
//! checked before a model session can consume a token.
const std = @import("std");
const studies = @import("backends/context_study_templates.zig");
const release_build = @import("context_release_build_binding.zig");

const max_metadata_bytes = 16 * 1024 * 1024;
const max_prompt_tokens = 131072;
const max_study_tokens = 8192;
pub const required_prefill_chunk_rows: u32 = 64;

const FileRef = struct { path: []const u8, sha256: []const u8 };
const TokensRef = struct { path: []const u8, sha256: []const u8, count: u32 };
const Request = struct {
    session_id: []const u8,
    request_index: u32,
    question_sha256: []const u8,
    request_content_sha256: []const u8,
};
const Question = struct { tokens: TokensRef, source_start: u32, source_end: u32 };
const Study = struct {
    tokens: TokensRef,
    prompt_tokens_sha256: []const u8,
    rendered_suffix_sha256: []const u8,
    template: FileRef,
    generation_contract: FileRef,
    response_tokens: u32,
    generation_mode: []const u8,
};
const IdentityJson = struct {
    load_generation: u64,
    artifact_sha256: []const u8,
    tokenizer_sha256: []const u8,
    template_sha256: []const u8,
    layout_sha256: []const u8,
    implementation_sha256: []const u8,
};
const ProvenanceJson = struct { binary_sha256: []const u8, source_build_sha256: []const u8 };
const Manifest = struct {
    schema_version: u32,
    stage: []const u8,
    request: Request,
    coverage_receipt: FileRef,
    rendered_prompt: FileRef,
    prompt: TokensRef,
    prompt_token_offsets: TokensRef,
    question: Question,
    fit: Study,
    gate: Study,
    loaded_identity: IdentityJson,
    provenance: ProvenanceJson,
    completed: bool,
    diagnostic_only: bool,
    admitted_prefill_chunk_rows: u32,
};
const CoverageQuestion = struct {
    message_index: u32,
    part_index: u32,
    source_start: u32,
    source_end: u32,
    token_count: u32,
    tokens_sha256: []const u8,
};
const CoverageDocument = struct { id: []const u8, sha256: []const u8, bucket: []const u8 };
const CoverageBlock = struct { message_index: u32, part_index: u32, label: u32, start: u32, end: u32 };
const CoverageReceipt = struct {
    schema_version: u32,
    stage: []const u8,
    request: Request,
    document: CoverageDocument,
    qualified_context_tokens: u32,
    prompt_tokens: u32,
    reserved_output_tokens: u32,
    prompt_tokens_sha256: []const u8,
    prompt_token_offsets_sha256: []const u8,
    rendered_prompt_sha256: []const u8,
    question: CoverageQuestion,
    eligible_blocks: []const CoverageBlock,
    loaded_identity: IdentityJson,
    provenance: ProvenanceJson,
    completed: bool,
    cancelled: bool,
    admitted_prefill_chunk_rows: u32,
};

pub const LoadedIdentity = struct {
    load_generation: u64,
    artifact_sha256: [32]u8,
    tokenizer_sha256: [32]u8,
    template_sha256: [32]u8,
    layout_sha256: [32]u8,
    implementation_sha256: [32]u8,
};

pub const Inputs = struct {
    allocator: std.mem.Allocator,
    prompt: []i64,
    prompt_token_offsets: [][2]u32,
    question: []i64,
    fit: []i64,
    gate: []i64,
    fit_response_tokens: u32,
    gate_response_tokens: u32,
    qualified_context_tokens: u32,
    reserved_output_tokens: u32,
    prompt_tokens_sha256: [32]u8,
    question_source_start: u32,
    question_source_end: u32,
    fit_template_sha256: [32]u8,
    gate_template_sha256: [32]u8,
    generation_contract_sha256: [32]u8,
    coverage_receipt_sha256: [32]u8,
    request_content_sha256: [32]u8,
    binary_sha256: [32]u8,
    source_build_sha256: [32]u8,
    admitted_prefill_chunk_rows: u32,
    document_id: []u8,
    document_sha256: [32]u8,
    document_bucket: []u8,
    eligible_block_starts: []u32,

    pub fn deinit(self: *Inputs) void {
        self.allocator.free(self.prompt);
        self.allocator.free(self.prompt_token_offsets);
        self.allocator.free(self.question);
        self.allocator.free(self.fit);
        self.allocator.free(self.gate);
        self.allocator.free(self.document_id);
        self.allocator.free(self.document_bucket);
        self.allocator.free(self.eligible_block_starts);
        self.* = undefined;
    }
};

pub fn load(
    allocator: std.mem.Allocator,
    io: std.Io,
    campaign_dir: std.Io.Dir,
    manifest_file: []const u8,
    expected_manifest_sha256: [32]u8,
    source_receipt_file: []const u8,
    expected_loaded: LoadedIdentity,
) !Inputs {
    const manifest_bytes = try readFile(allocator, io, campaign_dir, manifest_file, max_metadata_bytes);
    defer allocator.free(manifest_bytes);
    if (!std.mem.eql(u8, &digest(manifest_bytes), &expected_manifest_sha256)) return error.CaptureManifestDigestMismatch;
    var parsed = try std.json.parseFromSlice(Manifest, allocator, manifest_bytes, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    });
    defer parsed.deinit();
    const manifest = parsed.value;
    if (manifest.schema_version != 1 or !std.mem.eql(u8, manifest.stage, "context_numerical_capture_inputs") or
        !manifest.completed or manifest.diagnostic_only or manifest.request.request_index >= std.math.maxInt(u32) or
        manifest.admitted_prefill_chunk_rows != required_prefill_chunk_rows or
        manifest.prompt.count < 1025 or manifest.prompt.count > max_prompt_tokens or
        manifest.prompt_token_offsets.count != manifest.prompt.count or
        manifest.question.tokens.count == 0 or manifest.question.tokens.count > max_study_tokens or
        manifest.question.source_start >= manifest.question.source_end or
        manifest.question.source_end > manifest.prompt.count or
        manifest.question.source_end - manifest.question.source_start != manifest.question.tokens.count)
        return error.InvalidCaptureManifest;
    _ = try digest32(manifest.request.session_id);
    _ = try digest32(manifest.request.question_sha256);
    const request_content_sha256 = try digest32(manifest.request.request_content_sha256);
    const loaded = try parseIdentity(manifest.loaded_identity);
    if (!std.meta.eql(loaded, expected_loaded)) return error.CaptureLoadedIdentityMismatch;

    const source_receipt = try readFile(allocator, io, campaign_dir, source_receipt_file, max_metadata_bytes);
    defer allocator.free(source_receipt);
    const source_sha256 = digest(source_receipt);
    if (!std.mem.eql(u8, &source_sha256, &(try digest32(manifest.provenance.source_build_sha256))))
        return error.CaptureSourceReceiptMismatch;
    const build = try release_build.parse(allocator, source_receipt);
    const binary_sha256 = try selfExecutableDigest(allocator, io);
    if (!std.mem.eql(u8, &binary_sha256, &build.binary_sha256) or
        !std.mem.eql(u8, &binary_sha256, &(try digest32(manifest.provenance.binary_sha256))))
        return error.CaptureBinaryMismatch;

    const coverage_bytes = try readBound(allocator, io, campaign_dir, manifest.coverage_receipt, max_metadata_bytes);
    defer allocator.free(coverage_bytes);
    const coverage_receipt_sha256 = digest(coverage_bytes);
    const coverage_binding = try validateCoverageReceipt(allocator, coverage_bytes, manifest, loaded);
    errdefer coverage_binding.deinit(allocator);
    const rendered_prompt = try readBound(allocator, io, campaign_dir, manifest.rendered_prompt, max_metadata_bytes);
    defer allocator.free(rendered_prompt);
    const prompt = try readTokens(allocator, io, campaign_dir, manifest.prompt, max_prompt_tokens);
    errdefer allocator.free(prompt);
    const prompt_token_offsets = try readOffsets(
        allocator,
        io,
        campaign_dir,
        manifest.prompt_token_offsets,
        manifest.prompt.count,
        rendered_prompt.len,
    );
    errdefer allocator.free(prompt_token_offsets);
    const question = try readTokens(allocator, io, campaign_dir, manifest.question.tokens, max_study_tokens);
    errdefer allocator.free(question);
    if (!std.mem.eql(i64, question, prompt[manifest.question.source_start..manifest.question.source_end]))
        return error.CaptureQuestionPromptMismatch;
    const fit = try readStudy(allocator, io, campaign_dir, .fit, manifest.fit, manifest.prompt.sha256);
    errdefer allocator.free(fit);
    const gate = try readStudy(allocator, io, campaign_dir, .gate, manifest.gate, manifest.prompt.sha256);
    errdefer allocator.free(gate);
    const fit_contract = try digest32(manifest.fit.generation_contract.sha256);
    const gate_contract = try digest32(manifest.gate.generation_contract.sha256);
    if (!std.mem.eql(u8, &fit_contract, &gate_contract) or
        std.mem.eql(u8, manifest.fit.template.sha256, manifest.gate.template.sha256) or
        manifest.fit.response_tokens < 8 or manifest.fit.response_tokens > max_study_tokens or
        manifest.gate.response_tokens < 8 or manifest.gate.response_tokens > max_study_tokens or
        !std.mem.eql(u8, manifest.fit.generation_mode, "greedy_fixed_length_ignore_eos") or
        !std.mem.eql(u8, manifest.gate.generation_mode, "greedy_fixed_length_ignore_eos"))
        return error.InvalidCaptureStudy;
    return .{
        .allocator = allocator,
        .prompt = prompt,
        .prompt_token_offsets = prompt_token_offsets,
        .question = question,
        .fit = fit,
        .gate = gate,
        .fit_response_tokens = manifest.fit.response_tokens,
        .gate_response_tokens = manifest.gate.response_tokens,
        .qualified_context_tokens = coverage_binding.qualified_context_tokens,
        .reserved_output_tokens = coverage_binding.reserved_output_tokens,
        .prompt_tokens_sha256 = try digest32(manifest.prompt.sha256),
        .question_source_start = manifest.question.source_start,
        .question_source_end = manifest.question.source_end,
        .fit_template_sha256 = try digest32(manifest.fit.template.sha256),
        .gate_template_sha256 = try digest32(manifest.gate.template.sha256),
        .generation_contract_sha256 = fit_contract,
        .coverage_receipt_sha256 = coverage_receipt_sha256,
        .request_content_sha256 = request_content_sha256,
        .binary_sha256 = binary_sha256,
        .source_build_sha256 = source_sha256,
        .admitted_prefill_chunk_rows = manifest.admitted_prefill_chunk_rows,
        .document_id = coverage_binding.document_id,
        .document_sha256 = coverage_binding.document_sha256,
        .document_bucket = coverage_binding.document_bucket,
        .eligible_block_starts = coverage_binding.eligible_block_starts,
    };
}

fn readStudy(allocator: std.mem.Allocator, io: std.Io, root: std.Io.Dir, kind: studies.Kind, study: Study, prompt_sha256: []const u8) ![]i64 {
    if (!std.mem.eql(u8, study.prompt_tokens_sha256, prompt_sha256)) return error.CaptureStudyPromptMismatch;
    _ = try digest32(study.rendered_suffix_sha256);
    const template = try readBound(allocator, io, root, study.template, max_metadata_bytes);
    defer allocator.free(template);
    const contract = try readBound(allocator, io, root, study.generation_contract, max_metadata_bytes);
    defer allocator.free(contract);
    try validateStudyAuthority(kind, study, template, contract);
    return readTokens(allocator, io, root, study.tokens, max_study_tokens);
}

fn validateStudyAuthority(kind: studies.Kind, study: Study, template: []const u8, contract: []const u8) !void {
    if (!std.mem.eql(u8, template, studies.template(kind)) or
        !std.mem.eql(u8, contract, studies.generation_contract) or
        study.response_tokens != studies.response_tokens or
        !std.mem.eql(u8, study.generation_mode, "greedy_fixed_length_ignore_eos"))
        return error.CaptureStudyAuthorityMismatch;
    try studies.validate(kind, .{
        .template_sha256 = try digest32(study.template.sha256),
        .generation_contract_sha256 = try digest32(study.generation_contract.sha256),
        .response_tokens = study.response_tokens,
        .generation_mode = .greedy_fixed_length_ignore_eos,
    });
}

const CoverageBinding = struct {
    qualified_context_tokens: u32,
    reserved_output_tokens: u32,
    document_id: []u8,
    document_sha256: [32]u8,
    document_bucket: []u8,
    eligible_block_starts: []u32,

    fn deinit(self: CoverageBinding, allocator: std.mem.Allocator) void {
        allocator.free(self.document_id);
        allocator.free(self.document_bucket);
        allocator.free(self.eligible_block_starts);
    }
};

fn validateCoverageReceipt(
    allocator: std.mem.Allocator,
    bytes: []const u8,
    manifest: Manifest,
    loaded: LoadedIdentity,
) !CoverageBinding {
    var parsed = try std.json.parseFromSlice(CoverageReceipt, allocator, bytes, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    const receipt = parsed.value;
    const qualified = std.math.add(u32, receipt.prompt_tokens, receipt.reserved_output_tokens) catch
        return error.CaptureCoverageMismatch;
    if (receipt.schema_version != 2 or !std.mem.eql(u8, receipt.stage, "context_payload_block_coverage") or
        !receipt.completed or receipt.cancelled or receipt.reserved_output_tokens == 0 or
        receipt.admitted_prefill_chunk_rows != manifest.admitted_prefill_chunk_rows or
        receipt.prompt_tokens != manifest.prompt.count or
        receipt.qualified_context_tokens != qualified or
        !std.mem.eql(u8, receipt.prompt_tokens_sha256, manifest.prompt.sha256) or
        !std.mem.eql(u8, receipt.prompt_token_offsets_sha256, manifest.prompt_token_offsets.sha256) or
        !std.mem.eql(u8, receipt.rendered_prompt_sha256, manifest.rendered_prompt.sha256) or
        receipt.request.request_index != manifest.request.request_index or
        !std.mem.eql(u8, receipt.request.session_id, manifest.request.session_id) or
        !std.mem.eql(u8, receipt.request.question_sha256, manifest.request.question_sha256) or
        !std.mem.eql(u8, receipt.request.request_content_sha256, manifest.request.request_content_sha256) or
        receipt.question.message_index != 0 or receipt.question.part_index != 1 or
        receipt.question.source_start != manifest.question.source_start or
        receipt.question.source_end != manifest.question.source_end or
        receipt.question.token_count != manifest.question.tokens.count or
        !std.mem.eql(u8, receipt.question.tokens_sha256, manifest.question.tokens.sha256) or
        !std.meta.eql(try parseIdentity(receipt.loaded_identity), loaded) or
        !std.mem.eql(u8, receipt.provenance.binary_sha256, manifest.provenance.binary_sha256) or
        !std.mem.eql(u8, receipt.provenance.source_build_sha256, manifest.provenance.source_build_sha256))
        return error.CaptureCoverageMismatch;
    if (receipt.document.id.len == 0 or receipt.document.bucket.len == 0 or receipt.eligible_blocks.len == 0 or
        receipt.eligible_blocks.len > max_prompt_tokens / 512)
        return error.CaptureCoverageMismatch;
    const document_sha256 = try digest32(receipt.document.sha256);
    const block_starts = try allocator.alloc(u32, receipt.eligible_blocks.len);
    errdefer allocator.free(block_starts);
    var previous_end: u32 = 0;
    for (receipt.eligible_blocks, block_starts, 0..) |block, *out, index| {
        if (block.message_index != 0 or block.part_index != 0 or block.label == 0 or
            block.end -| block.start != 512 or block.end > receipt.prompt_tokens or
            (index > 0 and block.start < previous_end))
            return error.CaptureCoverageMismatch;
        out.* = block.start;
        previous_end = block.end;
    }
    const document_id = try allocator.dupe(u8, receipt.document.id);
    errdefer allocator.free(document_id);
    const document_bucket = try allocator.dupe(u8, receipt.document.bucket);
    return .{
        .qualified_context_tokens = receipt.qualified_context_tokens,
        .reserved_output_tokens = receipt.reserved_output_tokens,
        .document_id = document_id,
        .document_sha256 = document_sha256,
        .document_bucket = document_bucket,
        .eligible_block_starts = block_starts,
    };
}

fn readOffsets(
    allocator: std.mem.Allocator,
    io: std.Io,
    root: std.Io.Dir,
    reference: TokensRef,
    expected_count: u32,
    rendered_bytes: usize,
) ![][2]u32 {
    if (reference.count != expected_count or reference.count == 0 or reference.count > max_prompt_tokens)
        return error.InvalidCaptureOffsets;
    const bytes = try readBound(
        allocator,
        io,
        root,
        .{ .path = reference.path, .sha256 = reference.sha256 },
        @as(u64, max_prompt_tokens) * 8,
    );
    defer allocator.free(bytes);
    if (bytes.len != @as(usize, reference.count) * 8) return error.InvalidCaptureOffsets;
    const offsets = try allocator.alloc([2]u32, reference.count);
    errdefer allocator.free(offsets);
    var previous_mapped_start: u32 = 0;
    var previous_mapped_end: u32 = 0;
    var saw_mapped = false;
    var saw_nonempty = false;
    for (offsets, 0..) |*span, index| {
        span.* = .{
            std.mem.readInt(u32, bytes[index * 8 ..][0..4], .little),
            std.mem.readInt(u32, bytes[index * 8 + 4 ..][0..4], .little),
        };
        if (span[0] > span[1] or span[1] > rendered_bytes) return error.InvalidCaptureOffsets;
        if (!std.meta.eql(span.*, .{ 0, 0 })) {
            if (saw_mapped and
                (span[0] < previous_mapped_start or span[1] < previous_mapped_end))
                return error.InvalidCaptureOffsets;
            saw_mapped = true;
            previous_mapped_start = span[0];
            previous_mapped_end = span[1];
        }
        if (span[0] != span[1]) saw_nonempty = true;
    }
    if (!saw_nonempty) return error.InvalidCaptureOffsets;
    return offsets;
}

fn readTokens(allocator: std.mem.Allocator, io: std.Io, root: std.Io.Dir, reference: TokensRef, limit: u32) ![]i64 {
    if (reference.count == 0 or reference.count > limit) return error.InvalidCaptureTokens;
    const bytes = try readBound(allocator, io, root, .{ .path = reference.path, .sha256 = reference.sha256 }, @as(u64, limit) * 8);
    defer allocator.free(bytes);
    if (bytes.len != @as(usize, reference.count) * 8) return error.InvalidCaptureTokens;
    const tokens = try allocator.alloc(i64, reference.count);
    errdefer allocator.free(tokens);
    for (tokens, 0..) |*token, index| {
        token.* = std.mem.readInt(i64, bytes[index * 8 ..][0..8], .little);
        if (token.* < 0) return error.InvalidCaptureTokens;
    }
    return tokens;
}

fn readBound(allocator: std.mem.Allocator, io: std.Io, root: std.Io.Dir, reference: FileRef, limit: u64) ![]u8 {
    const expected = try digest32(reference.sha256);
    const bytes = try readFile(allocator, io, root, reference.path, limit);
    errdefer allocator.free(bytes);
    if (!std.mem.eql(u8, &digest(bytes), &expected)) return error.CaptureFileDigestMismatch;
    return bytes;
}

fn parseIdentity(value: IdentityJson) !LoadedIdentity {
    if (value.load_generation == 0) return error.InvalidCaptureIdentity;
    return .{
        .load_generation = value.load_generation,
        .artifact_sha256 = try digest32(value.artifact_sha256),
        .tokenizer_sha256 = try digest32(value.tokenizer_sha256),
        .template_sha256 = try digest32(value.template_sha256),
        .layout_sha256 = try digest32(value.layout_sha256),
        .implementation_sha256 = try digest32(value.implementation_sha256),
    };
}

fn digest(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}

fn digest32(text: []const u8) ![32]u8 {
    if (text.len != 64) return error.InvalidCaptureDigest;
    var result: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&result, text) catch return error.InvalidCaptureDigest;
    return result;
}

fn validRelative(path: []const u8) bool {
    if (path.len == 0 or std.fs.path.isAbsolute(path)) return false;
    var parts = std.mem.splitScalar(u8, path, '/');
    while (parts.next()) |part| if (part.len == 0 or std.mem.eql(u8, part, ".") or std.mem.eql(u8, part, "..")) return false;
    return true;
}

fn openFile(io: std.Io, root: std.Io.Dir, path: []const u8) !std.Io.File {
    if (!validRelative(path)) return error.InvalidCapturePath;
    var current = root;
    var owned = false;
    defer if (owned) current.close(io);
    var parts = std.mem.splitScalar(u8, path, '/');
    var part = parts.next() orelse return error.InvalidCapturePath;
    while (parts.next()) |next| {
        const child = try current.openDir(io, part, .{ .follow_symlinks = false });
        if (owned) current.close(io);
        current = child;
        owned = true;
        part = next;
    }
    return current.openFile(io, part, .{ .mode = .read_only, .allow_directory = false, .follow_symlinks = false, .resolve_beneath = true });
}

fn readFile(allocator: std.mem.Allocator, io: std.Io, root: std.Io.Dir, path: []const u8, limit: u64) ![]u8 {
    const file = try openFile(io, root, path);
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.kind != .file or stat.size == 0 or stat.size > limit) return error.InvalidCapturePath;
    const size = std.math.cast(usize, stat.size) orelse return error.InvalidCapturePath;
    const bytes = try allocator.alloc(u8, size);
    errdefer allocator.free(bytes);
    if (try file.readPositionalAll(io, bytes, 0) != size) return error.InvalidCapturePath;
    return bytes;
}

fn selfExecutableDigest(allocator: std.mem.Allocator, io: std.Io) ![32]u8 {
    const path = try std.process.executablePathAlloc(io, allocator);
    defer allocator.free(path);
    const file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only, .allow_directory = false, .follow_symlinks = false });
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.kind != .file) return error.CaptureBinaryMismatch;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var buffer: [64 * 1024]u8 = undefined;
    var offset: u64 = 0;
    while (offset < stat.size) {
        const count: usize = @intCast(@min(stat.size - offset, buffer.len));
        const read = try file.readPositionalAll(io, buffer[0..count], offset);
        if (read == 0) return error.CaptureBinaryMismatch;
        hash.update(buffer[0..read]);
        offset += read;
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

test "context numerical capture rejects rehashed noncompiled study authority" {
    const fit = studies.authority(.fit);
    const fit_hex = std.fmt.bytesToHex(fit.template_sha256, .lower);
    const contract_hex = std.fmt.bytesToHex(fit.generation_contract_sha256, .lower);
    const study = Study{
        .tokens = .{ .path = "fit.i64", .sha256 = "1111111111111111111111111111111111111111111111111111111111111111", .count = 8 },
        .prompt_tokens_sha256 = "2222222222222222222222222222222222222222222222222222222222222222",
        .rendered_suffix_sha256 = "3333333333333333333333333333333333333333333333333333333333333333",
        .template = .{ .path = "fit.txt", .sha256 = fit_hex[0..] },
        .generation_contract = .{ .path = "contract.json", .sha256 = contract_hex[0..] },
        .response_tokens = studies.response_tokens,
        .generation_mode = "greedy_fixed_length_ignore_eos",
    };
    try validateStudyAuthority(.fit, study, studies.template(.fit), studies.generation_contract);
    try std.testing.expectError(error.CaptureStudyAuthorityMismatch, validateStudyAuthority(.fit, study, "rehashed attacker template", studies.generation_contract));
    var changed = study;
    changed.response_tokens -= 1;
    try std.testing.expectError(error.CaptureStudyAuthorityMismatch, validateStudyAuthority(.fit, changed, studies.template(.fit), studies.generation_contract));
}

test "context numerical capture rejects rehashed coverage receipt binding drift" {
    const h11 = "1111111111111111111111111111111111111111111111111111111111111111";
    const h22 = "2222222222222222222222222222222222222222222222222222222222222222";
    const h33 = "3333333333333333333333333333333333333333333333333333333333333333";
    const h44 = "4444444444444444444444444444444444444444444444444444444444444444";
    const h55 = "5555555555555555555555555555555555555555555555555555555555555555";
    const h66 = "6666666666666666666666666666666666666666666666666666666666666666";
    const h77 = "7777777777777777777777777777777777777777777777777777777777777777";
    const h88 = "8888888888888888888888888888888888888888888888888888888888888888";
    const request = Request{ .session_id = h66, .request_index = 7, .question_sha256 = h22, .request_content_sha256 = h33 };
    const identity = IdentityJson{ .load_generation = 1, .artifact_sha256 = h11, .tokenizer_sha256 = h11, .template_sha256 = h11, .layout_sha256 = h11, .implementation_sha256 = h11 };
    const tokens = TokensRef{ .path = "unused.i64", .sha256 = h77, .count = 8 };
    const file = FileRef{ .path = "unused", .sha256 = h11 };
    const study = Study{ .tokens = tokens, .prompt_tokens_sha256 = h88, .rendered_suffix_sha256 = h11, .template = file, .generation_contract = file, .response_tokens = 256, .generation_mode = "greedy_fixed_length_ignore_eos" };
    const manifest = Manifest{
        .schema_version = 1,
        .stage = "context_numerical_capture_inputs",
        .request = request,
        .coverage_receipt = file,
        .rendered_prompt = file,
        .prompt = .{ .path = "prompt.i64", .sha256 = h88, .count = 1025 },
        .prompt_token_offsets = .{ .path = "prompt-offsets.u32le", .sha256 = h77, .count = 1025 },
        .question = .{ .tokens = tokens, .source_start = 1000, .source_end = 1008 },
        .fit = study,
        .gate = study,
        .loaded_identity = identity,
        .provenance = .{ .binary_sha256 = h44, .source_build_sha256 = h55 },
        .completed = true,
        .diagnostic_only = false,
        .admitted_prefill_chunk_rows = required_prefill_chunk_rows,
    };
    const loaded = LoadedIdentity{ .load_generation = 1, .artifact_sha256 = @splat(0x11), .tokenizer_sha256 = @splat(0x11), .template_sha256 = @splat(0x11), .layout_sha256 = @splat(0x11), .implementation_sha256 = @splat(0x11) };
    const allocator = std.testing.allocator;
    const valid = try std.json.Stringify.valueAlloc(allocator, .{
        .schema_version = @as(u32, 2),
        .stage = "context_payload_block_coverage",
        .request = request,
        .document = .{ .id = "document-1", .sha256 = h11, .bucket = "document_qa" },
        .qualified_context_tokens = @as(u32, 1041),
        .prompt_tokens = @as(u32, 1025),
        .reserved_output_tokens = @as(u32, 16),
        .prompt_tokens_sha256 = h88,
        .prompt_token_offsets_sha256 = h77,
        .rendered_prompt_sha256 = h11,
        .question = .{ .message_index = @as(u32, 0), .part_index = @as(u32, 1), .source_start = @as(u32, 1000), .source_end = @as(u32, 1008), .token_count = @as(u32, 8), .tokens_sha256 = h77 },
        .eligible_blocks = &.{.{ .message_index = @as(u32, 0), .part_index = @as(u32, 0), .label = @as(u32, 1), .start = @as(u32, 0), .end = @as(u32, 512) }},
        .loaded_identity = identity,
        .provenance = manifest.provenance,
        .completed = true,
        .cancelled = false,
        .admitted_prefill_chunk_rows = required_prefill_chunk_rows,
    }, .{});
    defer allocator.free(valid);
    var binding = try validateCoverageReceipt(allocator, valid, manifest, loaded);
    binding.deinit(allocator);
    const drifted = try std.json.Stringify.valueAlloc(allocator, .{
        .schema_version = @as(u32, 2),
        .stage = "context_payload_block_coverage",
        .request = Request{ .session_id = h66, .request_index = 8, .question_sha256 = h22, .request_content_sha256 = h33 },
        .document = .{ .id = "document-1", .sha256 = h11, .bucket = "document_qa" },
        .qualified_context_tokens = @as(u32, 1041),
        .prompt_tokens = @as(u32, 1025),
        .reserved_output_tokens = @as(u32, 16),
        .prompt_tokens_sha256 = h88,
        .prompt_token_offsets_sha256 = h77,
        .rendered_prompt_sha256 = h11,
        .question = .{ .message_index = @as(u32, 0), .part_index = @as(u32, 1), .source_start = @as(u32, 1000), .source_end = @as(u32, 1008), .token_count = @as(u32, 8), .tokens_sha256 = h77 },
        .eligible_blocks = &.{.{ .message_index = @as(u32, 0), .part_index = @as(u32, 0), .label = @as(u32, 1), .start = @as(u32, 0), .end = @as(u32, 512) }},
        .loaded_identity = identity,
        .provenance = manifest.provenance,
        .completed = true,
        .cancelled = false,
        .admitted_prefill_chunk_rows = required_prefill_chunk_rows,
    }, .{});
    defer allocator.free(drifted);
    try std.testing.expectError(error.CaptureCoverageMismatch, validateCoverageReceipt(allocator, drifted, manifest, loaded));
}
