// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Native whole-head release authority bound to the exact qualified destination
//! and execution settings. Numerical validity alone cannot authorize serving.
const std = @import("std");
const checkpoint = @import("attention_match_checkpoint.zig");
const compaction_identity = @import("compaction_identity");
const Sha256 = std.crypto.hash.sha2.Sha256;
pub const minimum_prefix_tokens = 7936;
pub const maximum_context_tokens = 32768;
pub const maximum_output_tokens = 256;

pub const Execution = struct {
    forward_frame: bool = false,
    precomputed_tail: bool = false,
    retain_hidden: bool = false,
    matrix_stage: bool = false,
    coalesced_stage: bool = false,
    ple_tail: bool = false,
    resource_barriers: bool = false,
    concurrent_frame: bool = false,

    pub fn validate(self: Execution) !void {
        if ((!self.forward_frame and (self.precomputed_tail or self.retain_hidden or self.matrix_stage or
            self.coalesced_stage or self.ple_tail or self.resource_barriers or self.concurrent_frame)) or
            (self.ple_tail and !self.precomputed_tail) or (self.concurrent_frame and !self.resource_barriers))
            return error.UnqualifiedCompactionArtifact;
    }

    pub fn diagnosticFromEnvironment() Execution {
        if (comptime !@import("builtin").is_test) return .{};
        var result: Execution = .{};
        inline for (@typeInfo(Execution).@"struct".field_names, 0..) |field, i|
            @field(result, field) = @import("antfly_platform").env.getenvBool(std.mem.span(execution_environment_names[i]));
        return result;
    }

    pub fn fingerprint(self: Execution) [32]u8 {
        var hash = Sha256.init(.{});
        hash.update("antfly.native-whole-head.execution.v2\n");
        inline for (@typeInfo(Execution).@"struct".field_names) |field| hash.update(&.{@intFromBool(@field(self, field))});
        return hash.finalResult();
    }
};

pub const execution_environment_names = [_][*:0]const u8{
    "ANTFLY_METAL_ATTENTION_MATCH_FORWARD_FRAME",
    "ANTFLY_METAL_ATTENTION_MATCH_PRECOMPUTED_TAIL",
    "ANTFLY_METAL_ATTENTION_MATCH_RETAIN_HIDDEN",
    "ANTFLY_METAL_ATTENTION_MATCH_MATRIX_STAGE",
    "ANTFLY_METAL_ATTENTION_MATCH_COALESCED_STAGE",
    "ANTFLY_METAL_ATTENTION_MATCH_PLE_TAIL",
    "ANTFLY_METAL_ATTENTION_MATCH_RESOURCE_BARRIERS",
    "ANTFLY_METAL_ATTENTION_MATCH_CONCURRENT_FRAME",
};
comptime {
    if (execution_environment_names.len != @typeInfo(Execution).@"struct".field_names.len) @compileError("native execution field map changed");
}

pub const Destination = struct {
    device_registry_id: u64,
    physical_memory_bytes: u64,
    device_name_sha256: [32]u8,
    compiler_identity_sha256: [32]u8,

    pub fn validate(self: Destination) !void {
        if (self.device_registry_id == 0 or self.physical_memory_bytes == 0 or
            std.mem.allEqual(u8, &self.device_name_sha256, 0) or
            std.mem.allEqual(u8, &self.compiler_identity_sha256, 0))
            return error.UnqualifiedCompactionArtifact;
    }

    pub fn fingerprint(self: Destination) [32]u8 {
        var hash = Sha256.init(.{});
        hash.update("antfly.native-whole-head.destination.v2\n");
        var encoded: [16]u8 = undefined;
        std.mem.writeInt(u64, encoded[0..8], self.device_registry_id, .little);
        std.mem.writeInt(u64, encoded[8..16], self.physical_memory_bytes, .little);
        hash.update(&encoded);
        hash.update(&self.device_name_sha256);
        hash.update(&self.compiler_identity_sha256);
        return hash.finalResult();
    }
};

pub const Observed = struct {
    destination: Destination,
    backend_environment_sha256: [32]u8,
};

pub fn configurationFingerprint(execution: Execution, backend_environment: [32]u8) [32]u8 {
    var hash = Sha256.init(.{});
    hash.update("antfly.native-whole-head.configuration.v2\n");
    hash.update(&execution.fingerprint());
    hash.update(&backend_environment);
    return hash.finalResult();
}

pub const Certificate = struct {
    identity: checkpoint.Identity,
    encoding: checkpoint.Encoding,
    implementation_sha256: [32]u8,
    report_sha256: [32]u8,
    evidence_contract_version: u32,
    destination: Destination,
    execution: Execution,
    backend_environment_sha256: [32]u8,
    configuration_sha256: [32]u8,

    pub fn admitCheckpoint(self: Certificate, prefix_tokens: usize) !void {
        if (self.evidence_contract_version != 2 or self.encoding == .f32 or
            prefix_tokens < minimum_prefix_tokens or prefix_tokens >= maximum_context_tokens)
            return error.UnqualifiedCompactionArtifact;
    }

    pub fn admitRequest(self: Certificate, prefix_tokens: usize, output_tokens: usize) !void {
        try self.admitCheckpoint(prefix_tokens);
        if (output_tokens == 0 or output_tokens > maximum_output_tokens or
            prefix_tokens > maximum_context_tokens - output_tokens)
            return error.UnqualifiedCompactionArtifact;
    }

    pub fn validate(self: Certificate, observed: Observed) !void {
        if (self.encoding == .f32 or self.evidence_contract_version != 2 or
            std.mem.allEqual(u8, &self.identity.weights, 0) or
            std.mem.allEqual(u8, &self.identity.tokenizer, 0) or
            std.mem.allEqual(u8, &self.identity.template, 0) or
            std.mem.allEqual(u8, &self.identity.layout, 0) or
            std.mem.allEqual(u8, &self.report_sha256, 0) or
            std.mem.allEqual(u8, &self.backend_environment_sha256, 0) or
            !std.mem.eql(u8, &self.implementation_sha256, &compaction_identity.digest) or
            !std.meta.eql(self.destination, observed.destination) or
            !std.mem.eql(u8, &self.backend_environment_sha256, &observed.backend_environment_sha256) or
            !std.mem.eql(u8, &self.configuration_sha256, &configurationFingerprint(self.execution, self.backend_environment_sha256)))
            return error.UnqualifiedCompactionArtifact;
        try self.destination.validate();
        try self.execution.validate();
    }
};

// BEGIN METAL CHECKPOINT RELEASE REGISTRY
// Only complete native whole-head v2 evidence for this destination and exact
// execution configuration may enroll a certificate. Other backends stay closed.
const certificates: []const Certificate = &.{};
// END METAL CHECKPOINT RELEASE REGISTRY

pub fn select(identity: checkpoint.Identity, encoding: checkpoint.Encoding, observed: Observed) !*const Certificate {
    for (certificates) |*certificate| {
        if (certificate.encoding == encoding and certificate.identity.eql(identity)) {
            try certificate.validate(observed);
            return certificate;
        }
    }
    return error.UnqualifiedCompactionArtifact;
}

test "attention_match Metal certificate binds native v2 destination configuration source and report" {
    const identity: checkpoint.Identity = .{ .weights = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4) };
    const observed: Observed = .{ .destination = .{
        .device_registry_id = 1,
        .physical_memory_bytes = 24 * 1024 * 1024 * 1024,
        .device_name_sha256 = @splat(6),
        .compiler_identity_sha256 = @splat(7),
    }, .backend_environment_sha256 = @splat(8) };
    var certificate: Certificate = .{
        .identity = identity,
        .encoding = .fp8,
        .implementation_sha256 = compaction_identity.digest,
        .report_sha256 = @splat(5),
        .evidence_contract_version = 2,
        .destination = observed.destination,
        .execution = .{ .forward_frame = true, .precomputed_tail = true, .retain_hidden = true, .matrix_stage = true },
        .backend_environment_sha256 = observed.backend_environment_sha256,
        .configuration_sha256 = @splat(0),
    };
    certificate.configuration_sha256 = configurationFingerprint(certificate.execution, certificate.backend_environment_sha256);
    try certificate.validate(observed);
    try certificate.admitRequest(7936, 256);
    try certificate.admitRequest(8128, 64);
    try certificate.admitRequest(32512, 256);
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, certificate.admitRequest(7935, 256));
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, certificate.admitRequest(32513, 256));
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, certificate.admitRequest(8128, 257));
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, certificate.admitRequest(8128, 0));
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, certificate.admitCheckpoint(32768));
    inline for (.{ "device_registry_id", "physical_memory_bytes" }) |field| {
        var changed = observed;
        @field(changed.destination, field) += 1;
        try std.testing.expectError(error.UnqualifiedCompactionArtifact, certificate.validate(changed));
    }
    inline for (.{ "device_name_sha256", "compiler_identity_sha256" }) |field| {
        var changed = observed;
        @field(changed.destination, field)[0] ^= 1;
        try std.testing.expectError(error.UnqualifiedCompactionArtifact, certificate.validate(changed));
    }
    var changed = observed;
    changed.backend_environment_sha256[0] ^= 1;
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, certificate.validate(changed));
    certificate.execution.coalesced_stage = true;
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, certificate.validate(observed));
    certificate.execution.coalesced_stage = false;
    certificate.evidence_contract_version = 1;
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, certificate.validate(observed));
    certificate.evidence_contract_version = 2;
    certificate.implementation_sha256[0] ^= 1;
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, certificate.validate(observed));
    certificate.implementation_sha256 = compaction_identity.digest;
    certificate.report_sha256 = @splat(0);
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, certificate.validate(observed));
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, select(identity, .fp8, observed));
}

/// The build emits the sorted environment names found in canonical runtime
/// sources. Native execution switches are represented by Execution instead.
/// No values are returned or logged. A missing key differs from an empty value.
pub fn backendEnvironmentFingerprint() ![32]u8 {
    var hash = Sha256.init(.{});
    hash.update("antfly.native-whole-head.backend-environment.v2\n");
    var previous: []const u8 = "";
    for (compaction_identity.backend_environment_keys) |name| {
        const key = std.mem.span(name);
        if (key.len == 0 or key.len > 512 or std.mem.order(u8, previous, key) != .lt)
            return error.UnqualifiedCompactionArtifact;
        previous = key;
        const value = if (std.c.getenv(name)) |raw| std.mem.span(raw) else null;
        if (value) |raw| if (raw.len > 4096) return error.UnqualifiedCompactionArtifact;
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, key.len, .little);
        hash.update(&length);
        hash.update(key);
        std.mem.writeInt(u64, &length, if (value) |raw| raw.len else std.math.maxInt(u64), .little);
        hash.update(&length);
        if (value) |raw| hash.update(raw);
    }
    return hash.finalResult();
}
