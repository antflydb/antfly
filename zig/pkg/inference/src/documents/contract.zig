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
pub const costs = @import("costs.zig");

pub const Status = @import("antfly_documents").types.Status;
pub const Action = @import("antfly_documents").types.Action;
pub const Origin = @import("antfly_documents").types.Origin;
pub const Operation = enum { read, decide, extract };
pub const Provider = enum { antfly, cloudflare };
pub const AudioReadOptions = @import("antfly_documents").types.AudioReadOptions;
pub const Binding = struct {
    audio_options: AudioReadOptions = .{},
    name: []const u8,
    model: []const u8,
    operation: Operation,
    /// Source modality consumed by a read binding; other operations consume text.
    input: enum { image, audio } = .image,
    output_origin: Origin = .transcription,
    provider: Provider = .antfly,
    /// Deployment-owned credential reference, never a credential value.
    account_id: ?[]const u8 = null,
    token_env: ?[]const u8 = null,
    gateway_id: ?[]const u8 = null,
    model_identity: ?[]const u8 = null,
    /// Provider-declared hard total-token ceiling for one opaque remote call.
    max_total_tokens: ?u64 = null,
};
/// Captured before inference. Paths never appear in public run metadata.
pub const Snapshot = struct { binding: Binding, path: []const u8, identity: [32]u8, max_total_tokens: u64 = 131072 };
pub const Limits = struct {
    max_sources: usize = 64,
    max_pages: usize = 1024,
    max_source_bytes: usize = 64 * 1024 * 1024,
    max_output_bytes: usize = 16 * 1024 * 1024,
    max_model_calls: usize = 256,
    max_total_tokens: u64 = 1_000_000,
    max_parallel_pages: usize = 4,
    max_parallel_calls: usize = 4,
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
            self.max_parallel_pages > 64 or self.max_parallel_calls > 64 or
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
            if (b.input == .audio and (b.operation != .read or b.provider != .antfly)) return error.DocumentCapabilityMismatch;
            if (b.max_total_tokens != null and b.max_total_tokens.? == 0) return error.InvalidDocumentDefinition;
            if (b.name.len == 0 or b.model.len == 0) return error.InvalidDocumentDefinition;
            if ((b.output_origin != .transcription and b.output_origin != .interpretation) or (b.operation != .read and b.output_origin != .transcription)) return error.InvalidDocumentDefinition;
            for (self.models[0..i]) |prior| if (std.mem.eql(u8, b.name, prior.name))
                return error.DuplicateDocumentBinding;
            if (b.provider == .cloudflare) {
                if (!self.allow_remote) return error.RemoteDocumentModelDenied;
                if (b.account_id == null or b.token_env == null or b.model_identity != null) return error.InvalidDocumentDefinition;
                if (b.operation == .read and !std.mem.eql(u8, b.model, "@cf/mistralai/mistral-small-3.1-24b-instruct")) return error.DocumentCapabilityMismatch;
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
            if ((try self.binding(r.reader, .read)).input == .audio and r.table_reader != null) return error.DocumentCapabilityMismatch;
            if (r.router) |name| _ = try self.binding(name, .decide);
            for ([_]?[]const u8{ r.table_reader, r.accurate_reader, r.fallback_reader, r.verification_reader }) |name|
                if (name) |n| {
                    if ((try self.binding(n, .read)).input != (try self.binding(r.reader, .read)).input) return error.DocumentCapabilityMismatch;
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
pub const Transform = @import("antfly_documents").types.Transform;
pub const Region = @import("antfly_documents").types.Region;
pub const Cell = @import("antfly_documents").types.Cell;
pub const Table = @import("antfly_documents").types.Table;
pub const InheritedPart = @import("antfly_documents").types.InheritedPart;
pub const Page = @import("antfly_documents").types.Page;
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
pub const Event = @import("antfly_documents").types.Event;
pub const Control = @import("antfly_documents").types.Control;
/// A wave's short critical sections coordinate cumulative admission. Workers
/// retain separate Runs for nested absolute ceilings and their own event arenas.
pub const SharedAccounting = struct {
    limits: Limits,
    usage: Usage,
    event_count: usize,
    active_calls: usize = 0,
    active_regions: usize = 0,
    capacity_changed: std.Io.Event = .unset,
    provider_state: ?*anyopaque = null,
    provider_deinit_fn: ?*const fn (?*anyopaque) void = null,
    mutex: std.atomic.Mutex = .unlocked,
    pub fn lock(self: *SharedAccounting) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
};
/// All siblings refining a nested agent share that agent's absolute ceilings.
/// Protected by the enclosing SharedAccounting mutex; scopes form a stack of
/// structured lifetimes and are destroyed only after every child has joined.
pub const ScopeAccounting = struct {
    parent: ?*ScopeAccounting = null,
    limits: Limits,
    usage: Usage,
    fn remaining(self: *ScopeAccounting, comptime field: []const u8, comptime limit: []const u8) u64 {
        var result: u64 = @field(self.limits, limit) -| @field(self.usage, field);
        if (self.parent) |parent| result = @min(result, parent.remaining(field, limit));
        return result;
    }
    fn add(self: *ScopeAccounting, comptime field: []const u8, count: u64) void {
        @field(self.usage, field) += @intCast(count);
        if (self.parent) |parent| parent.add(field, count);
    }
};
pub const Run = struct {
    durable: ?*@import("antfly_readers").document_runs.Session = null,
    pixel_meter: @import("antfly_readers").document_runs.PixelMeter = .{},
    durable_stage: ?@import("antfly_readers").document_runs.Digest = null,
    measured_costs: costs.Set = .{},
    cost_capture: ?*costs.Capture = null,
    limits: Limits,
    shared: ?*SharedAccounting = null,
    scope: ?*ScopeAccounting = null,
    usage: Usage = .{},
    source_count: usize = 0,
    source_bytes: usize = 0,
    control: Control = .{},
    event_allocator: ?A = null,
    work_allocator: ?A = null,
    provider_state: ?*anyopaque = null,
    provider_deinit_fn: ?*const fn (?*anyopaque) void = null,
    events: std.ArrayList(Event) = .empty,
    pub const Permit = struct {
        shared: ?*SharedAccounting = null,
        io: ?std.Io = null,
        region: bool = false,
        pub fn deinit(self: *Permit) void {
            const shared = self.shared orelse return;
            shared.lock();
            const count = if (self.region) &shared.active_regions else &shared.active_calls;
            std.debug.assert(count.* > 0);
            count.* -= 1;
            shared.mutex.unlock();
            if (self.io) |io| shared.capacity_changed.set(io);
            self.shared = null;
        }
    };
    /// Scheduler capacity is distinct from model admission. Acquire call
    /// capacity only at leaf dispatch; composite parents never hold it while
    /// invoking children. Region capacity bounds live refinement scratch.
    pub fn acquire(self: *Run, io: ?std.Io, region: bool) !Permit {
        const shared = self.shared orelse return .{};
        while (true) {
            try self.control.check();
            shared.lock();
            const count = if (region) &shared.active_regions else &shared.active_calls;
            if (count.* < shared.limits.max_parallel_calls) {
                count.* += 1;
                shared.mutex.unlock();
                return .{ .shared = shared, .io = io, .region = region };
            }
            shared.capacity_changed.reset();
            shared.mutex.unlock();
            const runtime = io orelse return error.DocumentBudgetExhausted;
            shared.capacity_changed.waitTimeout(runtime, .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => {},
                else => return err,
            };
        }
    }
    pub fn deinit(self: *Run) void {
        self.abandonPixels();
        self.measured_costs.deinit();
        if (self.provider_deinit_fn) |release| release(self.provider_state);
        self.provider_state = null;
        self.provider_deinit_fn = null;
    }
    pub fn emit(self: *Run, event: Event) !void {
        try self.control.check();
        if (self.shared) |shared| {
            shared.lock();
            defer shared.mutex.unlock();
            if (shared.event_count >= 16384) return error.DocumentEventLimitExceeded;
            shared.event_count += 1;
        }
        if (self.event_allocator != null) _ = try self.outputJson(event);
        try self.control.emit(event);
        try self.retainEvent(event);
    }
    /// Events merged in source order were already charged by the worker.
    pub fn retainEvent(self: *Run, event: Event) !void {
        if (self.event_allocator) |a| {
            if (self.events.items.len >= 16384) return error.DocumentEventLimitExceeded;
            var retained = event;
            retained.source_id = try a.dupe(u8, event.source_id);
            if (event.model) |model| retained.model = try a.dupe(u8, model);
            if (event.finding) |finding| retained.finding = try a.dupe(u8, finding);
            try self.events.append(a, retained);
        }
    }
    /// Absolute ceilings over shared counters. Entering a child never resets
    /// accounting; leaving it restores only policy, not consumed work.
    pub fn nestedLimits(self: *const Run, child: Limits) Limits {
        var result = self.limits;
        inline for (@typeInfo(Limits).@"struct".field_names) |name| {
            @field(result, name) = @min(@field(result, name), @field(child, name));
        }
        result.max_sources = @min(self.limits.max_sources, self.source_count +| child.max_sources);
        result.max_source_bytes = @min(self.limits.max_source_bytes, self.source_bytes +| child.max_source_bytes);
        result.max_pages = @min(self.limits.max_pages, self.usage.visited_pages +| child.max_pages);
        result.max_model_calls = @min(self.limits.max_model_calls, self.usage.model_calls +| child.max_model_calls);
        result.max_total_tokens = @min(self.limits.max_total_tokens, self.usage.total_tokens +| child.max_total_tokens);
        result.max_total_pixels = @min(self.limits.max_total_pixels, self.usage.rendered_pixels +| child.max_total_pixels);
        result.max_output_bytes = @min(self.limits.max_output_bytes, self.usage.output_bytes +| child.max_output_bytes);
        return result;
    }
    /// Borrowing avoids a second physical byte/pixel charge but does not
    /// exempt the child from its own input ceilings.
    pub fn nestedViewLimits(self: *const Run, child: Limits, bytes: usize, width: u32, height: u32) !Limits {
        if (bytes > child.max_source_bytes) return error.DocumentSourceLimitExceeded;
        const view_pixels = @as(u64, width) * height;
        if (width > child.max_dimension or height > child.max_dimension or view_pixels > child.max_pixels or view_pixels > child.max_total_pixels) return error.DocumentPixelLimitExceeded;
        var result = self.nestedLimits(child);
        result.max_total_pixels = @min(result.max_total_pixels, self.usage.rendered_pixels +| (child.max_total_pixels - view_pixels));
        return result;
    }
    pub fn call(self: *Run) !void {
        try self.requireOutput();
        if (self.usage.model_calls >= self.limits.max_model_calls) return error.DocumentBudgetExhausted;
        if (self.shared) |shared| {
            shared.lock();
            defer shared.mutex.unlock();
            if (shared.usage.output_bytes >= shared.limits.max_output_bytes) return error.DocumentOutputTooLarge;
            if (shared.usage.model_calls >= shared.limits.max_model_calls) return error.DocumentBudgetExhausted;
            if (self.scope) |scope| if (scope.remaining("model_calls", "max_model_calls") == 0) return error.DocumentBudgetExhausted;
            shared.usage.model_calls += 1;
            if (self.scope) |scope| scope.add("model_calls", 1);
        }
        self.usage.model_calls += 1;
    }
    pub fn remainingCalls(self: *const Run) usize {
        // Semantic replay still traverses committed calls when the durable
        // dispatch budget is exhausted. Session.reserve independently fences
        // every new physical call against cumulative, cross-attempt charges.
        const local = self.limits.max_model_calls -| self.usage.model_calls;
        if (self.shared) |shared| {
            shared.lock();
            defer shared.mutex.unlock();
            const scoped: usize = if (self.scope) |scope| @intCast(scope.remaining("model_calls", "max_model_calls")) else local;
            return @min(local, scoped, shared.limits.max_model_calls -| shared.usage.model_calls);
        }
        return local;
    }
    /// Restore acknowledged semantic work without issuing provider calls.
    /// Check every enclosing ceiling under the shared admission lock first.
    pub fn replayUsage(self: *Run, delta: Usage) !void {
        try self.control.check();
        return self.applyReplayUsage(delta);
    }
    fn applyReplayUsage(self: *Run, delta: Usage) !void {
        const Check = struct {
            fn next(usage: Usage, limits: Limits, increment: Usage) !Usage {
                var result = usage;
                inline for (@typeInfo(Usage).@"struct".field_names) |field| @field(result, field) = std.math.add(@TypeOf(@field(result, field)), @field(result, field), @field(increment, field)) catch return error.DocumentBudgetExhausted;
                if (result.model_calls > limits.max_model_calls) return error.DocumentBudgetExhausted;
                if (result.total_tokens > limits.max_total_tokens) return error.DocumentTokenLimitExceeded;
                if (result.rendered_pixels > limits.max_total_pixels) return error.DocumentPixelLimitExceeded;
                if (result.output_bytes > limits.max_output_bytes) return error.DocumentOutputTooLarge;
                return result;
            }
        };
        var increment = delta;
        increment.physical_model_calls = 0;
        increment.visited_pages = 0;
        increment.completed_pages = 0;
        const local = try Check.next(self.usage, self.limits, increment);
        if (self.shared) |shared| {
            shared.lock();
            defer shared.mutex.unlock();
            const total = try Check.next(shared.usage, shared.limits, increment);
            if (self.scope) |scope| {
                if (increment.model_calls > scope.remaining("model_calls", "max_model_calls")) return error.DocumentBudgetExhausted;
                if (increment.total_tokens > scope.remaining("total_tokens", "max_total_tokens")) return error.DocumentTokenLimitExceeded;
                if (increment.rendered_pixels > scope.remaining("rendered_pixels", "max_total_pixels")) return error.DocumentPixelLimitExceeded;
                if (increment.output_bytes > scope.remaining("output_bytes", "max_output_bytes")) return error.DocumentOutputTooLarge;
                inline for (@typeInfo(Usage).@"struct".field_names) |field| scope.add(field, @field(increment, field));
            }
            shared.usage = total;
        }
        self.usage = local;
    }
    pub fn touchCost(self: *Run, receipt: costs.Receipt) !void {
        const session = self.durable orelse return;
        try self.measured_costs.add(session.allocator, receipt, session.max_stages);
        var capture = self.cost_capture;
        while (capture) |current| : (capture = current.parent) try current.costs.add(session.allocator, receipt, session.max_stages);
    }
    pub fn mergeCosts(self: *Run, child: *Run, a: A) !void {
        if (self.durable == null) return;
        for (try child.measured_costs.snapshot(a)) |receipt| try self.touchCost(receipt);
    }
    pub fn restoreCosts(self: *Run, logical: Usage, receipts: []const costs.Receipt) !void {
        const session = self.durable orelse return self.replayUsage(logical);
        try self.control.check();
        const Context = struct {
            run: *Run,
            logical: Usage,
            fn apply(ctx: @This(), delta: costs.Measurement) !void {
                var usage = ctx.logical;
                usage.prompt_tokens = delta.prompt_tokens;
                usage.completion_tokens = delta.completion_tokens;
                usage.total_tokens = std.math.add(u64, delta.prompt_tokens, delta.completion_tokens) catch return error.InvalidDocumentCheckpoint;
                usage.rendered_pixels = delta.rendered_pixels;
                try ctx.run.applyReplayUsage(usage);
            }
        };
        try session.restoreMeasurements(receipts, Context{ .run = self, .logical = logical }, Context.apply);
        for (receipts) |receipt| try self.touchCost(receipt);
    }
    pub fn physicalCall(self: *Run) void {
        if (self.shared) |shared| {
            shared.lock();
            defer shared.mutex.unlock();
            shared.usage.physical_model_calls += 1;
            if (self.scope) |scope| scope.add("physical_model_calls", 1);
        }
        self.usage.physical_model_calls += 1;
    }
    pub fn tokens(self: *Run, input: u64, output_tokens: u64) !void {
        try self.control.check();
        const count = std.math.add(u64, input, output_tokens) catch return error.DocumentTokenLimitExceeded;
        if (count > self.limits.max_total_tokens -| self.usage.total_tokens) return error.DocumentTokenLimitExceeded;
        if (self.shared) |shared| {
            shared.lock();
            defer shared.mutex.unlock();
            if (count > shared.limits.max_total_tokens -| shared.usage.total_tokens) return error.DocumentTokenLimitExceeded;
            if (self.scope) |scope| if (count > scope.remaining("total_tokens", "max_total_tokens")) return error.DocumentTokenLimitExceeded;
            if (self.scope) |scope| {
                scope.add("prompt_tokens", input);
                scope.add("completion_tokens", output_tokens);
                scope.add("total_tokens", count);
            }
            shared.usage.prompt_tokens += input;
            shared.usage.completion_tokens += output_tokens;
            shared.usage.total_tokens += count;
        }
        self.usage.prompt_tokens += input;
        self.usage.completion_tokens += output_tokens;
        self.usage.total_tokens += count;
        if (self.durable) |session| {
            const key = self.durable_stage orelse return error.InvalidDocumentCheckpoint;
            try self.touchCost(try session.measureModelTokens(key, input, output_tokens));
        }
    }
    pub fn abandonPixels(self: *Run) void {
        if (self.durable) |session| self.pixel_meter.abandon(session);
    }
    pub fn flushPixels(self: *Run) !void {
        if (self.durable) |session| try self.pixel_meter.flush(session);
    }
    pub fn pixels(self: *Run, count: u64) !void {
        try self.control.check();
        if (count > self.limits.max_pixels or count > self.limits.max_total_pixels -| self.usage.rendered_pixels) return error.DocumentPixelLimitExceeded;
        if (self.durable) |session| try self.pixel_meter.charge(session, count);
        if (count > self.limits.max_pixels or count > self.limits.max_total_pixels -| self.usage.rendered_pixels)
            return error.DocumentPixelLimitExceeded;
        if (self.shared) |shared| {
            shared.lock();
            defer shared.mutex.unlock();
            if (count > shared.limits.max_total_pixels -| shared.usage.rendered_pixels) return error.DocumentPixelLimitExceeded;
            if (self.scope) |scope| if (count > scope.remaining("rendered_pixels", "max_total_pixels")) return error.DocumentPixelLimitExceeded;
            shared.usage.rendered_pixels += count;
            if (self.scope) |scope| scope.add("rendered_pixels", count);
        }
        self.usage.rendered_pixels += count;
        if (self.durable) |session| try self.touchCost(try session.measure(self.pixel_meter.key, .{ .rendered_pixels = count }));
    }
    pub fn output(self: *Run, count: usize) !void {
        if (count > self.limits.max_output_bytes -| self.usage.output_bytes) return error.DocumentOutputTooLarge;
        if (self.shared) |shared| {
            shared.lock();
            defer shared.mutex.unlock();
            if (count > shared.limits.max_output_bytes -| shared.usage.output_bytes) return error.DocumentOutputTooLarge;
            if (self.scope) |scope| if (count > scope.remaining("output_bytes", "max_output_bytes")) return error.DocumentOutputTooLarge;
            shared.usage.output_bytes += count;
            if (self.scope) |scope| scope.add("output_bytes", count);
        }
        self.usage.output_bytes += count;
    }
    pub fn remainingOutput(self: *const Run) usize {
        const local = self.limits.max_output_bytes -| self.usage.output_bytes;
        if (self.shared) |shared| {
            shared.lock();
            defer shared.mutex.unlock();
            const scoped: usize = if (self.scope) |scope| @intCast(scope.remaining("output_bytes", "max_output_bytes")) else local;
            return @min(local, scoped, shared.limits.max_output_bytes -| shared.usage.output_bytes);
        }
        return local;
    }
    pub fn requireOutput(self: *const Run) !void {
        try self.control.check();
        if (self.remainingOutput() == 0) return error.DocumentOutputTooLarge;
    }
    pub fn outputJson(self: *Run, value: anytype) !usize {
        const count = try @import("antfly_documents").output.measure(value, self.remainingOutput());
        try self.output(count);
        return count;
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

test "document nested ceilings preserve consumed work and all child resource limits" {
    var run = Run{ .limits = .{ .max_sources = 10, .max_source_bytes = 100, .max_pages = 10, .max_model_calls = 10, .max_output_bytes = 100, .max_total_tokens = 100, .max_total_pixels = 100 }, .source_count = 2, .source_bytes = 20, .usage = .{ .visited_pages = 2, .model_calls = 2, .output_bytes = 20, .total_tokens = 20, .rendered_pixels = 20 } };
    const parent = run.limits;
    run.limits = run.nestedLimits(.{ .max_sources = 1, .max_source_bytes = 5, .max_pages = 1, .max_model_calls = 1, .max_output_bytes = 5, .max_total_tokens = 5, .max_total_pixels = 5, .max_pixels = 4, .max_dimension = 20 });
    try std.testing.expectEqual(@as(usize, 3), run.limits.max_sources);
    try std.testing.expectEqual(@as(usize, 25), run.limits.max_source_bytes);
    try std.testing.expectEqual(@as(usize, 3), run.limits.max_pages);
    try std.testing.expectEqual(@as(u32, 20), run.limits.max_dimension);
    try run.call();
    try std.testing.expectError(error.DocumentBudgetExhausted, run.call());
    try run.tokens(4, 1);
    try std.testing.expectError(error.DocumentTokenLimitExceeded, run.tokens(1, 0));
    try run.pixels(4);
    try std.testing.expectError(error.DocumentPixelLimitExceeded, run.pixels(2));
    try run.output(5);
    try std.testing.expectError(error.DocumentOutputTooLarge, run.output(1));
    run.limits = parent;
    try std.testing.expectEqual(@as(usize, 3), run.usage.model_calls);
    try std.testing.expectEqual(@as(u64, 25), run.usage.total_tokens);
}

test "document borrowed views enforce child bytes and reserve pixels without double charges" {
    const run = Run{ .limits = .{}, .source_count = 2, .source_bytes = 100, .usage = .{ .rendered_pixels = 500 } };
    const child = Limits{ .max_source_bytes = 68, .max_pixels = 100, .max_total_pixels = 150 };
    const limits = try run.nestedViewLimits(child, 68, 10, 10);
    try std.testing.expectEqual(@as(u64, 550), limits.max_total_pixels);
    try std.testing.expectEqual(@as(usize, 100), run.source_bytes);
    try std.testing.expectEqual(@as(usize, 2), run.source_count);
    try std.testing.expectError(error.DocumentSourceLimitExceeded, run.nestedViewLimits(child, 69, 10, 10));
    try std.testing.expectError(error.DocumentPixelLimitExceeded, run.nestedViewLimits(child, 68, 11, 10));
}

test "document event output is admitted before copying or publishing progress" {
    const Sink = struct {
        called: bool = false,
        fn emit(raw: ?*anyopaque, _: Event) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.called = true;
        }
    };
    var sink = Sink{};
    var run = Run{ .limits = .{ .max_output_bytes = 16 }, .event_allocator = std.testing.failing_allocator, .control = .{ .ptr = &sink, .event_fn = Sink.emit } };
    defer run.deinit();
    try std.testing.expectError(error.DocumentOutputTooLarge, run.emit(.{ .phase = .source, .source_id = "source", .page_number = 1 }));
    try std.testing.expect(!sink.called);
    try std.testing.expectEqual(@as(usize, 0), run.events.items.len);
    try std.testing.expectEqual(@as(usize, 0), run.usage.output_bytes);
}

test "document exhausted output admission prevents another model call" {
    var run = Run{ .limits = .{ .max_output_bytes = 1 }, .usage = .{ .output_bytes = 1 } };
    try std.testing.expectError(error.DocumentOutputTooLarge, run.call());
    try std.testing.expectEqual(@as(usize, 0), run.usage.model_calls);
}

test "document table validation rejects invalid extents without allocation or overflow" {
    const valid = Table{ .id = 0, .rows = 2, .columns = 3, .cells = &.{.{ .row = 0, .column = 0, .row_span = 2, .column_span = 3, .text = "Merged" }} };
    try valid.validate();
    for ([_]Cell{
        .{ .row = 2, .column = 0, .text = "outside" },
        .{ .row = 0, .column = 3, .text = "outside" },
        .{ .row = 0, .column = 0, .row_span = 0, .text = "zero" },
        .{ .row = 0, .column = 0, .column_span = 0, .text = "zero" },
        .{ .row = 1, .column = 0, .row_span = 2, .text = "crossing" },
        .{ .row = 0, .column = 2, .column_span = 2, .text = "crossing" },
        .{ .row = std.math.maxInt(usize), .column = 0, .text = "overflow" },
        .{ .row = 1, .column = 0, .row_span = std.math.maxInt(usize), .text = "overflow" },
        .{ .row = 0, .column = 1, .column_span = std.math.maxInt(usize), .text = "overflow" },
    }) |cell| {
        var table = valid;
        table.cells = &.{cell};
        try std.testing.expectError(error.InvalidDocumentModelResult, table.validate());
    }
    for ([_][2]usize{ .{ 0, 1 }, .{ 1, 0 }, .{ 0, 0 } }) |dimensions| {
        const table = Table{ .id = 0, .rows = dimensions[0], .columns = dimensions[1], .cells = &.{} };
        try std.testing.expectError(error.InvalidDocumentModelResult, table.validate());
    }
    try (Table{ .id = 0, .rows = std.math.maxInt(usize), .columns = std.math.maxInt(usize), .cells = &.{.{ .row = std.math.maxInt(usize) - 1, .column = std.math.maxInt(usize) - 1, .text = "edge" }} }).validate();
}

test "document parallel accounting shares admission while child ceilings remain independent" {
    var accounting = SharedAccounting{ .limits = .{ .max_model_calls = 2, .max_total_tokens = 10, .max_output_bytes = 10, .max_total_pixels = 10 }, .usage = .{}, .event_count = 0 };
    var first = Run{ .limits = accounting.limits, .shared = &accounting };
    var second = Run{ .limits = accounting.limits, .shared = &accounting };
    try first.call();
    try second.call();
    try std.testing.expectError(error.DocumentBudgetExhausted, first.call());
    first.physicalCall();
    second.physicalCall();
    try first.tokens(3, 1);
    try second.tokens(4, 2);
    try std.testing.expectError(error.DocumentTokenLimitExceeded, second.tokens(1, 0));
    try first.pixels(6);
    try second.pixels(4);
    try std.testing.expectError(error.DocumentPixelLimitExceeded, first.pixels(1));
    try first.output(7);
    try std.testing.expectEqual(@as(usize, 3), second.remainingOutput());
    try std.testing.expectError(error.DocumentOutputTooLarge, second.output(4));
    try second.output(3);
    try std.testing.expectError(error.DocumentOutputTooLarge, first.requireOutput());
    try std.testing.expectEqual(@as(usize, 2), accounting.usage.physical_model_calls);
    try std.testing.expectEqual(@as(u64, 10), accounting.usage.total_tokens);
    accounting.usage.output_bytes = 0;
    var scoped = Run{ .limits = .{ .max_model_calls = 1 }, .shared = &accounting };
    scoped.usage.model_calls = 1;
    try std.testing.expectError(error.DocumentBudgetExhausted, scoped.call());
}

test "document concurrent siblings share nested ceilings without partially charging denied work" {
    var ledger = SharedAccounting{ .limits = .{}, .usage = .{}, .event_count = 0 };
    var parent = ScopeAccounting{ .limits = .{ .max_model_calls = 2, .max_total_tokens = 10, .max_total_pixels = 10, .max_output_bytes = 10 }, .usage = .{} };
    var left_scope = ScopeAccounting{ .parent = &parent, .limits = .{}, .usage = .{} };
    var right_scope = ScopeAccounting{ .parent = &parent, .limits = .{}, .usage = .{} };
    var left = Run{ .limits = .{}, .shared = &ledger, .scope = &left_scope };
    var right = Run{ .limits = .{}, .shared = &ledger, .scope = &right_scope };
    try left.call();
    try right.call();
    try std.testing.expectEqual(@as(usize, 0), left.remainingCalls());
    try std.testing.expectError(error.DocumentBudgetExhausted, left.call());
    try left.tokens(4, 2);
    try std.testing.expectError(error.DocumentTokenLimitExceeded, right.tokens(3, 2));
    try right.tokens(2, 2);
    try left.pixels(6);
    try std.testing.expectError(error.DocumentPixelLimitExceeded, right.pixels(5));
    try right.pixels(4);
    try left.output(6);
    try std.testing.expectEqual(@as(usize, 4), right.remainingOutput());
    try std.testing.expectError(error.DocumentOutputTooLarge, right.output(5));
    try right.output(4);
    try std.testing.expectEqual(@as(usize, 0), left.remainingOutput());
    try std.testing.expectEqual(@as(usize, 2), ledger.usage.model_calls);
    try std.testing.expectEqual(@as(u64, 10), ledger.usage.total_tokens);
    try std.testing.expectEqual(@as(u64, 10), ledger.usage.rendered_pixels);
    try std.testing.expectEqual(@as(usize, 10), ledger.usage.output_bytes);
    try std.testing.expectEqual(@as(u64, 6), left_scope.usage.total_tokens);
    try std.testing.expectEqual(@as(u64, 4), right_scope.usage.total_tokens);
    try std.testing.expectEqual(ledger.usage, parent.usage);
}

test "document durable cost replay is concurrent and retains logical and nested ceilings" {
    const readers = @import("antfly_readers");
    const runs = readers.document_runs;
    const a = std.testing.allocator;
    var memory = readers.testing.Store{ .a = a };
    defer memory.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const store = memory.interface();
    const identity: runs.Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    try store.create(.{ .run_id = "costs", .owner = "alice", .identity = identity, .limits = .{}, .inputs = &.{} });
    const Clock = struct {
        fn now(_: ?*anyopaque) u64 {
            return 1;
        }
    };
    const lease = try store.claim(work, "costs", "alice", identity, 1, 100);
    var session = try runs.Session.init(a, store, lease, identity, .{ .now_fn = Clock.now }, 16, 32768);
    defer session.deinit();
    const receipt = costs.Receipt{ .key = @splat(7), .measured = .{ .prompt_tokens = 7, .completion_tokens = 3 } };
    var ledger = SharedAccounting{ .limits = .{ .max_total_tokens = 10, .max_model_calls = 2, .max_output_bytes = 2 }, .usage = .{}, .event_count = 0 };
    var scope = ScopeAccounting{ .limits = .{ .max_total_tokens = 5, .max_model_calls = 2, .max_output_bytes = 2 }, .usage = .{} };
    var denied = Run{ .durable = &session, .limits = ledger.limits, .shared = &ledger, .scope = &scope };
    defer denied.deinit();
    try std.testing.expectError(error.DocumentTokenLimitExceeded, denied.restoreCosts(.{ .model_calls = 1, .output_bytes = 1 }, &.{receipt}));
    try std.testing.expect(session.measuredReceipt(receipt.key) == null);
    try std.testing.expectEqual(@as(u64, 0), ledger.usage.total_tokens);
    scope.limits.max_total_tokens = 10;
    var capture = costs.Capture{ .parent = null };
    defer capture.deinit();
    var children = [_]Run{
        .{ .durable = &session, .limits = ledger.limits, .shared = &ledger, .scope = &scope, .cost_capture = &capture },
        .{ .durable = &session, .limits = ledger.limits, .shared = &ledger, .scope = &scope, .cost_capture = &capture },
    };
    defer for (&children) |*child| child.deinit();
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    var ready: std.atomic.Value(usize) = .init(0);
    const Worker = struct {
        run: *Run,
        ready: *std.atomic.Value(usize),
        receipt: costs.Receipt,
        failure: ?anyerror = null,
        fn execute(self: *@This()) void {
            _ = self.ready.fetchAdd(1, .acq_rel);
            while (self.ready.load(.acquire) != 2) std.atomic.spinLoopHint();
            self.run.restoreCosts(.{ .model_calls = 1, .output_bytes = 1 }, &.{self.receipt}) catch |err| {
                self.failure = err;
            };
        }
    };
    var workers = [_]Worker{ .{ .run = &children[0], .ready = &ready, .receipt = receipt }, .{ .run = &children[1], .ready = &ready, .receipt = receipt } };
    var group: std.Io.Group = .init;
    defer group.cancel(io_impl.io());
    for (&workers) |*worker| try group.concurrent(io_impl.io(), Worker.execute, .{worker});
    try group.await(io_impl.io());
    for (workers) |worker| try std.testing.expect(worker.failure == null);
    try std.testing.expectEqual(@as(u64, 10), ledger.usage.total_tokens);
    try std.testing.expectEqual(@as(usize, 2), ledger.usage.model_calls);
    try std.testing.expectEqual(@as(usize, 2), ledger.usage.output_bytes);
    try std.testing.expectEqual(@as(u64, 10), scope.usage.total_tokens);
    try std.testing.expectEqual(@as(usize, 1), (try capture.costs.snapshot(work)).len);
    try std.testing.expectError(error.DocumentBudgetExhausted, denied.restoreCosts(.{ .model_calls = 1 }, &.{receipt}));
    ledger.limits.max_model_calls = 3;
    scope.limits.max_model_calls = 3;
    denied.limits.max_model_calls = 3;
    try std.testing.expectError(error.DocumentOutputTooLarge, denied.restoreCosts(.{ .output_bytes = 1 }, &.{receipt}));
}

test "document audio bindings reject incompatible refinement and visual specialists" {
    const audio = Binding{ .name = "speech", .model = "gemma4", .operation = .read, .input = .audio };
    const image = Binding{ .name = "ocr", .model = "ocr", .operation = .read };
    var definition = Definition{ .models = &.{ audio, image }, .read = .{ .reader = "speech", .accurate_reader = "ocr" } };
    try std.testing.expectError(error.DocumentCapabilityMismatch, definition.validate());
    definition.read = .{ .reader = "speech", .table_reader = "speech" };
    try std.testing.expectError(error.DocumentCapabilityMismatch, definition.validate());
    definition.read = .{ .reader = "speech" };
    try definition.validate();
}
