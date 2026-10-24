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

//! Transport-independent document contracts. Definitions contain no executable
//! expressions or credentials. Every allocation belongs to the caller's arena.
const std = @import("std");
const A = std.mem.Allocator;

pub const Status = enum { complete, unresolved, truncated };
pub const Action = enum { accept, accurate, table, fallback, unresolved };
pub const Origin = enum { native, transcription, interpretation, derived, estimated };
pub const Operation = enum { read, decide, extract };
pub const Provider = enum { antfly, cloudflare };
pub const Binding = struct {
    name: []const u8,
    model: []const u8,
    operation: Operation,
    output_origin: Origin = .transcription,
    provider: Provider = .antfly,
    /// Deployment-owned credential reference, never a credential value.
    account_id: ?[]const u8 = null,
    token_env: ?[]const u8 = null,
    gateway_id: ?[]const u8 = null,
    model_identity: ?[]const u8 = null,
};
/// Captured before inference. Paths never appear in public run metadata.
pub const Snapshot = struct { binding: Binding, path: []const u8, identity: [32]u8 };
pub const Limits = struct {
    max_sources: usize = 64,
    max_pages: usize = 1024,
    max_source_bytes: usize = 64 * 1024 * 1024,
    max_output_bytes: usize = 16 * 1024 * 1024,
    max_model_calls: usize = 256,
    max_total_tokens: u64 = 1_000_000,
    max_region_attempts: usize = 2,
    max_regions: usize = 4096,
    max_pixels: u64 = 40_000_000,
    max_dimension: u32 = 16384,
    max_total_pixels: u64 = 400_000_000,
    deadline_ms: u64 = 60_000,

    pub fn validate(self: Limits) !void {
        inline for (@typeInfo(Limits).@"struct".field_names) |name| {
            if (@field(self, name) == 0) return error.InvalidDocumentLimits;
        }
        if (self.max_sources > 128 or self.max_pages > 10000 or self.max_regions > 65536 or
            self.max_region_attempts > 8 or self.max_model_calls > 10000 or
            self.deadline_ms > 1_800_000 or self.max_output_bytes > 256 * 1024 * 1024 or
            self.max_source_bytes > 256 * 1024 * 1024 or self.max_pixels > 100_000_000 or self.max_dimension > 32768)
            return error.InvalidDocumentLimits;
    }
};
pub const Read = struct {
    reader: []const u8 = "",
    steps: []const ReadStep = &.{},
    router: ?[]const u8 = null,
    table_reader: ?[]const u8 = null,
    accurate_reader: ?[]const u8 = null,
    fallback_reader: ?[]const u8 = null,
    verification_reader: ?[]const u8 = null,
    /// Native text remains evidence even when a policy requests OCR.
    native_text: enum { inspect, always_ocr } = .inspect,
    min_confidence: f64 = 0.8,
};
pub const ReadStep = struct {
    id: []const u8,
    operation: enum { inspect_layout, read_region, refine, assemble },
    input: []const u8 = "source",
    model: ?[]const u8 = null,
    router: ?[]const u8 = null,
    accurate_reader: ?[]const u8 = null,
    table_reader: ?[]const u8 = null,
    fallback_reader: ?[]const u8 = null,
    verification_reader: ?[]const u8 = null,
};
pub const Extract = struct {
    model: []const u8,
    verification_model: ?[]const u8 = null,
    fallback_model: ?[]const u8 = null,
    require_source_support: bool = false,
    allow_derived_values: bool = true,
};
pub const Definition = struct {
    name: ?[]const u8 = null,
    version: u32 = 1,
    kind: []const u8 = "document_agent",
    models: []const Binding,
    budget: Limits = .{},
    allow_remote: bool = false,
    read: ?Read = null,
    extract: ?Extract = null,
    decide: ?Extract = null,

    /// Compile the constrained recipe into task bindings once. References are
    /// typed and backward-only; a definition cannot introduce scripts or an
    /// unbounded graph. The short form and explicit step form are equivalent.
    pub fn compile(self: Definition) !Definition {
        var result = self;
        if (self.read) |read| if (read.steps.len > 0) {
            if (read.steps.len > 4 or read.reader.len > 0) return error.InvalidDocumentDefinition;
            var compiled = read;
            var initial: ?usize = null;
            var refined = false;
            var assembled = false;
            for (read.steps, 0..) |step, i| {
                if (step.id.len == 0 or assembled) return error.InvalidDocumentDataflow;
                for (read.steps[0..i]) |prior| if (std.mem.eql(u8, step.id, prior.id)) return error.InvalidDocumentDataflow;
                if (!std.mem.eql(u8, step.input, "source")) {
                    var found = false;
                    for (read.steps[0..i]) |prior| if (std.mem.eql(u8, step.input, prior.id)) {
                        found = true;
                    };
                    if (!found) return error.InvalidDocumentDataflow;
                }
                if (step.operation != .read_region and step.model != null) return error.InvalidDocumentDataflow;
                if (step.operation != .refine and (step.router != null or step.accurate_reader != null or step.table_reader != null or step.fallback_reader != null or step.verification_reader != null)) return error.InvalidDocumentDataflow;
                switch (step.operation) {
                    .inspect_layout => {
                        if (initial != null or step.model != null or !std.mem.eql(u8, step.input, "source")) return error.InvalidDocumentDataflow;
                    },
                    .read_region => {
                        if (initial != null or !std.mem.eql(u8, step.input, "source")) return error.InvalidDocumentDataflow;
                        compiled.reader = step.model orelse return error.UnknownDocumentBinding;
                        initial = i;
                    },
                    .refine => {
                        const prior = initial orelse return error.InvalidDocumentDataflow;
                        if (refined or !std.mem.eql(u8, step.input, read.steps[prior].id)) return error.InvalidDocumentDataflow;
                        refined = true;
                        compiled.router = step.router;
                        compiled.accurate_reader = step.accurate_reader;
                        compiled.table_reader = step.table_reader;
                        compiled.fallback_reader = step.fallback_reader;
                        compiled.verification_reader = step.verification_reader;
                    },
                    .assemble => {
                        if (initial == null or i == 0 or !std.mem.eql(u8, step.input, read.steps[i - 1].id)) return error.InvalidDocumentDataflow;
                        assembled = true;
                    },
                }
            }
            if (initial == null or !assembled) return error.InvalidDocumentDataflow;
            result.read = compiled;
        };
        try result.validate();
        return result;
    }

    pub fn binding(self: Definition, name: []const u8, operation: Operation) !Binding {
        for (self.models) |b| if (std.mem.eql(u8, b.name, name)) {
            if (b.operation != operation) return error.DocumentCapabilityMismatch;
            return b;
        };
        return error.UnknownDocumentBinding;
    }

    pub fn validate(self: Definition) !void {
        if (self.version != 1 or !std.mem.eql(u8, self.kind, "document_agent") or
            self.models.len == 0 or self.models.len > 32 or (self.read == null and self.extract == null and self.decide == null))
            return error.InvalidDocumentDefinition;
        try self.budget.validate();
        for (self.models, 0..) |b, i| {
            if (b.name.len == 0 or b.model.len == 0) return error.InvalidDocumentDefinition;
            if ((b.output_origin != .transcription and b.output_origin != .interpretation) or (b.operation != .read and b.output_origin != .transcription)) return error.InvalidDocumentDefinition;
            for (self.models[0..i]) |prior| if (std.mem.eql(u8, b.name, prior.name))
                return error.DuplicateDocumentBinding;
            if (b.provider == .cloudflare) {
                if (!self.allow_remote) return error.RemoteDocumentModelDenied;
                if (b.account_id == null or b.token_env == null or b.operation == .read or b.model_identity != null) return error.InvalidDocumentDefinition;
                if ((b.operation == .extract and !std.mem.eql(u8, b.model, "@cf/cloudflare/clairic")) or
                    (b.operation == .decide and !std.mem.eql(u8, b.model, "@cf/cloudflare/clef") and !std.mem.eql(u8, b.model, "@cf/cloudflare/clef-flash"))) return error.DocumentCapabilityMismatch;
                const env = b.token_env.?;
                if (env.len == 0 or env.len > 256) return error.InvalidDocumentDefinition;
                for (env) |ch| if (!std.ascii.isAlphanumeric(ch) and ch != '_') return error.InvalidDocumentDefinition;
            }
        }
        if (self.read) |r| {
            _ = try self.binding(r.reader, .read);
            if (!std.math.isFinite(r.min_confidence) or r.min_confidence < 0 or r.min_confidence > 1)
                return error.InvalidDocumentDefinition;
            if (r.router) |name| _ = try self.binding(name, .decide);
            for ([_]?[]const u8{ r.table_reader, r.accurate_reader, r.fallback_reader, r.verification_reader }) |name|
                if (name) |n| {
                    _ = try self.binding(n, .read);
                };
        }
        if (self.extract) |e| {
            _ = try self.binding(e.model, .extract);
            if (e.verification_model) |name| {
                const verifier = try self.binding(name, .extract);
                if (std.mem.eql(u8, verifier.model, (try self.binding(e.model, .extract)).model)) return error.InvalidDocumentDefinition;
            }
            if (e.fallback_model) |name| _ = try self.binding(name, .extract);
        }
        if (self.decide) |e| {
            if (e.verification_model != null or e.fallback_model != null or e.require_source_support or !e.allow_derived_values) return error.InvalidDocumentDefinition;
            _ = try self.binding(e.model, .decide);
        }
    }

    pub fn parse(a: A, bytes: []const u8) !std.json.Parsed(Definition) {
        if (bytes.len > 1024 * 1024) return error.InvalidDocumentDefinition;
        var parsed = try std.json.parseFromSlice(Definition, a, bytes, .{ .duplicate_field_behavior = .@"error" });
        errdefer parsed.deinit();
        parsed.value = try parsed.value.compile();
        return parsed;
    }
};

/// Original-source coordinates. A full projective transform supports crops,
/// scale, orientation and perspective correction without losing evidence.
pub const Transform = struct {
    matrix: [9]f64 = .{ 1, 0, 0, 0, 1, 0, 0, 0, 1 },
    pub fn point(self: Transform, p: [2]f64) ![2]f64 {
        const m = self.matrix;
        const w = m[6] * p[0] + m[7] * p[1] + m[8];
        if (!std.math.isFinite(w) or @abs(w) < 1e-12) return error.InvalidDocumentTransform;
        const result = [2]f64{ (m[0] * p[0] + m[1] * p[1] + m[2]) / w, (m[3] * p[0] + m[4] * p[1] + m[5]) / w };
        for (result) |v| if (!std.math.isFinite(v)) return error.InvalidDocumentTransform;
        return result;
    }
    pub fn inverse(self: Transform) !Transform {
        const m = self.matrix;
        const cofactors = [9]f64{
            m[4] * m[8] - m[5] * m[7], m[2] * m[7] - m[1] * m[8], m[1] * m[5] - m[2] * m[4],
            m[5] * m[6] - m[3] * m[8], m[0] * m[8] - m[2] * m[6], m[2] * m[3] - m[0] * m[5],
            m[3] * m[7] - m[4] * m[6], m[1] * m[6] - m[0] * m[7], m[0] * m[4] - m[1] * m[3],
        };
        const determinant = m[0] * cofactors[0] + m[1] * cofactors[3] + m[2] * cofactors[6];
        if (!std.math.isFinite(determinant) or @abs(determinant) < 1e-12) return error.InvalidDocumentTransform;
        var result: Transform = .{};
        for (&result.matrix, cofactors) |*v, cofactor| v.* = cofactor / determinant;
        return result;
    }
    pub fn box(self: Transform, bbox: [4]f64) ![4]f64 {
        for (bbox) |v| if (!std.math.isFinite(v)) return error.InvalidDocumentRegion;
        if (bbox[0] >= bbox[2] or bbox[1] >= bbox[3]) return error.InvalidDocumentRegion;
        var result: [4]f64 = .{ std.math.inf(f64), std.math.inf(f64), -std.math.inf(f64), -std.math.inf(f64) };
        for ([_][2]f64{ .{ bbox[0], bbox[1] }, .{ bbox[2], bbox[1] }, .{ bbox[0], bbox[3] }, .{ bbox[2], bbox[3] } }) |corner| {
            const p = try self.point(corner);
            result[0] = @min(result[0], p[0]);
            result[1] = @min(result[1], p[1]);
            result[2] = @max(result[2], p[0]);
            result[3] = @max(result[3], p[1]);
        }
        return result;
    }
};
pub const Region = struct {
    id: usize,
    text: []const u8,
    bbox: ?[4]f64 = null,
    label: ?[]const u8 = null,
    confidence: ?f64 = null,
    origin: Origin = .transcription,
    model: ?[]const u8 = null,
    validated: bool = false,
};
pub const Cell = struct {
    row: usize,
    column: usize,
    row_span: usize = 1,
    column_span: usize = 1,
    text: []const u8,
    bbox: ?[4]f64 = null,
};
pub const Table = struct { id: usize, rows: usize, columns: usize, cells: []const Cell, origin: Origin = .native };
pub const Page = struct {
    object: []const u8 = "read",
    index: usize = 0,
    source_id: []const u8,
    number: usize,
    kind: enum { page, slide, image },
    native_text: []const u8 = "",
    notes: []const u8 = "",
    text: []const u8 = "",
    regions: []const Region = &.{},
    tables: []const Table = &.{},
    fields: ?std.json.Value = null,
    status: Status = .unresolved,
    origin: Origin = .native,
    coordinate_space: enum { pdf_points, slide_emu, image_pixels } = .image_pixels,
    transform: Transform = .{},
    raster_to_source: ?Transform = null,
    findings: []const []const u8 = &.{},
    model: ?[]const u8 = null,
};
pub const Usage = struct {
    prompt_tokens: u64 = 0,
    completion_tokens: u64 = 0,
    total_tokens: u64 = 0,
    model_calls: usize = 0,
    physical_model_calls: usize = 0,
    rendered_pixels: u64 = 0,
    output_bytes: usize = 0,
    completed_pages: usize = 0,
    visited_pages: usize = 0,
};
pub const Event = struct {
    phase: enum { source, native_text, reading, decision, verification, extracting, accepted, unresolved, complete },
    source_id: []const u8,
    page_number: usize,
    model: ?[]const u8 = null,
    region_id: ?usize = null,
    action: ?Action = null,
    status: ?Status = null,
    finding: ?[]const u8 = null,
};
pub const Control = struct {
    ptr: ?*anyopaque = null,
    check_fn: ?*const fn (?*anyopaque) anyerror!void = null,
    event_fn: ?*const fn (?*anyopaque, Event) anyerror!void = null,
    pub fn check(self: Control) !void {
        if (self.check_fn) |f| try f(self.ptr);
    }
    pub fn emit(self: Control, event: Event) !void {
        try self.check();
        if (self.event_fn) |f| try f(self.ptr, event);
    }
};
pub const Run = struct {
    limits: Limits,
    usage: Usage = .{},
    control: Control = .{},
    event_allocator: ?A = null,
    work_allocator: ?A = null,
    provider_state: ?*anyopaque = null,
    provider_deinit_fn: ?*const fn (?*anyopaque) void = null,
    events: std.ArrayList(Event) = .empty,
    pub fn deinit(self: *Run) void {
        if (self.provider_deinit_fn) |release| release(self.provider_state);
        self.provider_state = null;
        self.provider_deinit_fn = null;
    }
    pub fn emit(self: *Run, event: Event) !void {
        try self.control.emit(event);
        if (self.event_allocator) |a| {
            if (self.events.items.len >= 16384) return error.DocumentEventLimitExceeded;
            var retained = event;
            retained.source_id = try a.dupe(u8, event.source_id);
            if (event.model) |model| retained.model = try a.dupe(u8, model);
            if (event.finding) |finding| retained.finding = try a.dupe(u8, finding);
            try self.events.append(a, retained);
        }
    }
    pub fn call(self: *Run) !void {
        try self.control.check();
        if (self.usage.model_calls >= self.limits.max_model_calls) return error.DocumentBudgetExhausted;
        self.usage.model_calls += 1;
    }
    pub fn tokens(self: *Run, input: u64, output_tokens: u64) !void {
        try self.control.check();
        const count = std.math.add(u64, input, output_tokens) catch return error.DocumentTokenLimitExceeded;
        if (count > self.limits.max_total_tokens -| self.usage.total_tokens) return error.DocumentTokenLimitExceeded;
        self.usage.prompt_tokens += input;
        self.usage.completion_tokens += output_tokens;
        self.usage.total_tokens += count;
    }
    pub fn pixels(self: *Run, count: u64) !void {
        try self.control.check();
        if (count > self.limits.max_pixels or count > self.limits.max_total_pixels -| self.usage.rendered_pixels)
            return error.DocumentPixelLimitExceeded;
        self.usage.rendered_pixels += count;
    }
    pub fn output(self: *Run, count: usize) !void {
        if (count > self.limits.max_output_bytes -| self.usage.output_bytes) return error.DocumentOutputTooLarge;
        self.usage.output_bytes += count;
    }
};

test "document definition rejects duplicate bindings and cross-task calls before execution" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.DocumentCapabilityMismatch, Definition.parse(a,
        \\{"models":[{"name":"fast","model":"x","operation":"extract"}],"read":{"reader":"fast"}}
    ));
    try std.testing.expectError(error.DuplicateDocumentBinding, Definition.parse(a,
        \\{"models":[{"name":"x","model":"a","operation":"read"},{"name":"x","model":"b","operation":"read"}],"read":{"reader":"x"}}
    ));
    try std.testing.expectError(error.UnknownField, Definition.parse(a,
        \\{"models":[],"script":"exec"}
    ));
}
test "document nested work consumes one cumulative budget" {
    var run = Run{ .limits = .{ .max_model_calls = 1, .max_total_pixels = 10 } };
    try run.call();
    try std.testing.expectError(error.DocumentBudgetExhausted, run.call());
    try run.pixels(8);
    try std.testing.expectError(error.DocumentPixelLimitExceeded, run.pixels(3));
}
test "document projective evidence mapping retains crop and perspective" {
    const t = Transform{ .matrix = .{ 2, 0, 10, 0, 3, 20, 0, 0, 1 } };
    try std.testing.expectEqual([2]f64{ 14, 29 }, try t.point(.{ 2, 3 }));
    try std.testing.expectError(error.InvalidDocumentTransform, (Transform{ .matrix = @splat(0) }).point(.{ 1, 1 }));
}

test "document recipe compilation validates backward typed flow and model operations" {
    var valid = try Definition.parse(std.testing.allocator,
        \\{"models":[{"name":"fast","model":"reader","operation":"read"}],"read":{"steps":[{"id":"initial","operation":"read_region","model":"fast"},{"id":"result","operation":"assemble","input":"initial"}]}}
    );
    defer valid.deinit();
    try std.testing.expectEqualStrings("fast", valid.value.read.?.reader);
    try std.testing.expectError(error.InvalidDocumentDataflow, Definition.parse(std.testing.allocator,
        \\{"models":[{"name":"fast","model":"reader","operation":"read"}],"read":{"steps":[{"id":"result","operation":"assemble","input":"future"}]}}
    ));
}
