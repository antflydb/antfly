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

//! Shared, synchronous composition executor. A child invocation owns its model
//! admission only for the call: orchestration never holds a parent model permit.
const std = @import("std");
pub const c = @import("contract.zig");
const A = std.mem.Allocator;
const ir = @import("representation.zig");
const support = @import("source_support.zig");
pub const Action = c.Action;
pub const Candidate = struct {
    model: ?[]const u8 = null,
    status: ?c.Status = null,
    origin: c.Origin = .transcription,
    text: []const u8,
    regions: []const c.Region = &.{},
    tables: []const c.Table = &.{},
    fields: ?std.json.Value = null,
    /// Quality judgments are advisory, never source-evidence proofs.
    confidence: ?f64 = null,
    truncated: bool = false,
};
pub const Observation = struct {
    page: c.Page,
    region: ?c.Region,
    remaining_calls: usize,
    allowed: []const Action,
};
/// Unchanged content is reusable, but cannot authorize a provenance or
/// structure replacement. Only source-supported output can replace it.
pub const Validation = enum { unsupported, unchanged, source_supported };
pub const Driver = struct {
    ptr: *anyopaque,
    read_fn: *const fn (*anyopaque, A, c.Binding, usize, ?[4]f64, *c.Run) anyerror!Candidate,
    decide_fn: ?*const fn (*anyopaque, A, c.Binding, Observation, *c.Run) anyerror!Action = null,
    /// Validation must explicitly establish evidence support, not just ask a
    /// model if another model's answer sounds good.
    validate_fn: ?*const fn (*anyopaque, c.Page, ?c.Region, Candidate, *c.Run) anyerror!Validation = null,
    parallel: ?struct {
        io: std.Io,
        prepare_fn: *const fn (*anyopaque, *c.Run) anyerror!bool,
        fork_fn: *const fn (*anyopaque, A) anyerror!Driver,
    } = null,
};

fn meaningful(text: []const u8) bool {
    if (!std.unicode.utf8ValidateSlice(text)) return false;
    var count: usize = 0;
    for (text) |b| if (b > ' ' and b != 0x7f) {
        count += 1;
    };
    return count > 0;
}

fn selectedBinding(def: c.Definition, action: Action) !?c.Binding {
    const r = def.read orelse return error.DocumentCapabilityMismatch;
    const name = switch (action) {
        .accurate => r.accurate_reader,
        .table => r.table_reader,
        .fallback => r.fallback_reader,
        .accept, .unresolved => return null,
    } orelse return error.DocumentActionDenied;
    return try def.binding(name, .read);
}

/// Deterministic quality policy. Probabilities from different models are never
/// compared; the threshold only applies to the current reader's own output.
fn defaultAction(r: c.Read, page: c.Page, region: ?c.Region) Action {
    if (region) |item| {
        if (item.label) |label| if (std.mem.eql(u8, label, "table") and r.table_reader != null) return .table;
        if (!meaningful(item.text) or (item.confidence != null and item.confidence.? < r.min_confidence))
            return if (r.accurate_reader != null) .accurate else .unresolved;
        return .accept;
    }
    if (page.status != .complete or !meaningful(page.text) or ir.hasFinding(page, "native_text_coverage_incomplete")) return if (r.accurate_reader != null) .accurate else .unresolved;
    return .accept;
}

pub const Result = struct { page: c.Page, output_bytes: usize };
pub fn readPage(a: A, def: c.Definition, driver: Driver, page_index: usize, supplied: c.Page, run: *c.Run) !c.Page {
    return (try readPageReported(a, def, driver, page_index, supplied, run)).page;
}
pub fn readPageReported(a: A, def: c.Definition, driver: Driver, page_index: usize, supplied: c.Page, run: *c.Run) !Result {
    try def.validate();
    const r = def.read orelse return error.DocumentCapabilityMismatch;
    try ir.validatePage(supplied, run.limits.max_regions);
    var page = supplied;
    try run.emit(.{ .phase = .source, .source_id = page.source_id, .page_number = page.number });
    // Native content is retained and never replaced just because OCR is enabled.
    // Native adapters add a finding when coverage is uncertain (e.g. images).
    if (r.native_text == .inspect and meaningful(page.native_text) and page.findings.len == 0) {
        page.text = page.native_text;
        page.regions = try ir.cloneRegions(a, page.regions, run.limits.max_regions);
        page.status = .complete;
        try annotate(a, &page);
        const output_bytes = try run.outputJson(page);
        try run.emit(.{ .phase = .native_text, .source_id = page.source_id, .page_number = page.number });
        return .{ .page = page, .output_bytes = output_bytes };
    }
    const initial = try def.binding(r.reader, .read);
    try run.call();
    try run.emit(.{ .phase = .reading, .source_id = page.source_id, .page_number = page.number, .model = initial.model });
    const candidate = try driver.read_fn(driver.ptr, a, initial, page_index, null, run);
    // Initial model output is a candidate too. A non-empty string does not
    // demonstrate coverage of source text that the adapter already observed.
    const missing_native = !support.coversNative(page.native_text, candidate.text);
    var verified_repair = false;
    var verification_interruption: ?anyerror = null;
    if (missing_native and ir.hasFinding(page, "native_font_fallback")) {
        if (driver.validate_fn) |validate| verified_repair = (validate(driver.ptr, page, null, candidate, run) catch |err| switch (err) {
            error.DocumentBudgetExhausted, error.DocumentRendererUnavailable => blk: {
                verification_interruption = err;
                break :blk .unsupported;
            },
            else => return err,
        }) == .source_supported;
    }
    page = try applyPage(a, page, candidate, initial.model, run.limits.max_regions);
    if (verification_interruption) |err| page = try interrupted(a, page, err);
    try ir.finding(a, &page, "native_text_superseded_by_verification", verified_repair);
    try ir.finding(a, &page, "native_text_coverage_incomplete", missing_native and !verified_repair);
    if (missing_native and !verified_repair) page.status = .unresolved;
    var refined_whole = false;
    if ((missing_native and !verified_repair) or page.status == .truncated) {
        // A truncated region list cannot identify the missing page tail.
        page = try refine(a, def, driver, page_index, page, null, run);
        refined_whole = true;
    }
    if (page.regions.len == 0) {
        if (!refined_whole) page = try refine(a, def, driver, page_index, page, null, run);
    } else {
        const original_text = page.text;
        const original_regions = page.regions;
        const regions = try ir.cloneRegions(a, page.regions, run.limits.max_regions);
        const revisions = try a.alloc(bool, regions.len);
        @memset(revisions, false);
        if (driver.parallel != null and run.shared != null and run.limits.max_parallel_calls > 1 and regions.len > 1 and try driver.parallel.?.prepare_fn(driver.ptr, run)) {
            try refineParallel(a, def, driver, page_index, &page, regions, revisions, run);
        } else for (regions, revisions) |*region, *changed| {
            const revised = try refineTracked(a, def, driver, page_index, page, region.*, run, changed);
            if (revised.status != .complete) page.status = revised.status;
            if (revised.regions.len == 1) region.* = revised.regions[0];
            // Refinement owns findings as well as revisions. Once the shared
            // call budget is exhausted no later region can make model progress.
            page.findings = revised.findings;
            if (ir.hasFinding(page, "model_call_budget_exhausted")) break;
        }
        page.regions = regions;
        page.text = ir.assembleRevised(a, original_text, original_regions, regions, revisions) catch |err| switch (err) {
            error.InvalidDocumentRegion => blk: {
                page.status = .unresolved;
                page.regions = original_regions;
                @memset(revisions, false);
                try ir.finding(a, &page, "region_text_mapping_unresolved", true);
                break :blk original_text;
            },
            else => return err,
        };
        const projection = try ir.projectRevisedTables(a, page.tables, page.regions, revisions, run.limits.max_regions);
        page.tables = projection.tables;
        if (!projection.resolved) {
            page.status = .unresolved;
            try ir.finding(a, &page, "table_revision_scope_unresolved", true);
        }
    }
    try annotate(a, &page);
    if (!support.coversNative(page.native_text, page.text) and !ir.hasFinding(page, "native_text_superseded_by_verification")) {
        page.status = .unresolved;
        try ir.finding(a, &page, "native_text_coverage_incomplete", true);
    } else try ir.finding(a, &page, "native_text_coverage_incomplete", false);
    const output_bytes = try run.outputJson(page);
    try run.emit(.{ .phase = if (page.status == .complete) .accepted else .unresolved, .source_id = page.source_id, .page_number = page.number });
    return .{ .page = page, .output_bytes = output_bytes };
}

const RegionTask = struct {
    arena: std.heap.ArenaAllocator,
    definition: c.Definition,
    driver: Driver,
    page_index: usize,
    supplied: c.Page,
    region: c.Region,
    run: c.Run,
    permit: c.Run.Permit,
    changed: bool = false,
    result: ?c.Page = null,
    failure: ?anyerror = null,
    fn execute(self: *RegionTask) std.Io.Cancelable!void {
        defer self.permit.deinit();
        defer self.run.abandonPixels();
        self.process() catch |err| {
            self.failure = err;
        };
    }
    fn process(self: *RegionTask) !void {
        const a = self.arena.allocator();
        self.run.event_allocator = a;
        const driver = try self.driver.parallel.?.fork_fn(self.driver.ptr, a);
        self.result = try refineTracked(a, self.definition, driver, self.page_index, self.supplied, self.region, &self.run, &self.changed);
        try self.run.flushPixels();
    }
};

fn refineParallel(a: A, def: c.Definition, driver: Driver, page_index: usize, page: *c.Page, regions: []c.Region, revisions: []bool, run: *c.Run) !void {
    try run.flushPixels();
    const parallel = driver.parallel.?;
    const backing = run.work_allocator orelse a;
    const width = @min(regions.len, run.limits.max_parallel_calls);
    const snapshot = page.*;
    var scope: c.ScopeAccounting = .{ .parent = run.scope, .limits = run.limits, .usage = run.usage };
    const shared_limits = run.shared.?.limits;
    const narrower = run.limits.max_model_calls < shared_limits.max_model_calls or run.limits.max_total_tokens < shared_limits.max_total_tokens or run.limits.max_total_pixels < shared_limits.max_total_pixels or run.limits.max_output_bytes < shared_limits.max_output_bytes;
    const child_scope = if (narrower) &scope else run.scope;
    var next: usize = 0;
    while (next < regions.len) {
        const count = @min(width, regions.len - next);
        const tasks = try backing.alloc(RegionTask, count);
        defer backing.free(tasks);
        var initialized: usize = 0;
        defer for (tasks[0..initialized]) |*task| {
            task.permit.deinit();
            task.run.deinit();
            task.arena.deinit();
        };
        var group: std.Io.Group = .init;
        defer group.cancel(parallel.io);
        const baseline = run.usage;
        for (tasks, 0..) |*task, index| {
            // Start each admitted task immediately. Acquiring an entire wave
            // before starting children would deadlock competing page owners.
            const permit = try run.acquire(parallel.io, true);
            task.* = .{
                .arena = .init(backing),
                .definition = def,
                .driver = driver,
                .page_index = page_index,
                .supplied = snapshot,
                .region = regions[next + index],
                .permit = permit,
                .run = .{ .limits = run.limits, .usage = baseline, .durable = run.durable, .cost_capture = run.cost_capture, .shared = run.shared, .scope = child_scope, .source_count = run.source_count, .source_bytes = run.source_bytes, .work_allocator = backing, .control = .{ .ptr = run.control.ptr, .check_fn = run.control.check_fn } },
            };
            initialized += 1;
            group.concurrent(parallel.io, RegionTask.execute, .{task}) catch task.execute() catch unreachable;
        }
        try group.await(parallel.io);
        for (tasks) |*task| try run.mergeCosts(&task.run, a);
        for (tasks, 0..) |*task, index| {
            if (task.failure) |err| return err;
            const revised = task.result.?;
            if (revised.status != .complete and page.status != .truncated) page.status = revised.status;
            if (revised.regions.len == 1) regions[next + index] = (try ir.cloneRegions(a, revised.regions, run.limits.max_regions))[0];
            revisions[next + index] = task.changed;
            for (revised.findings) |finding| try ir.finding(a, page, finding, true);
            for (task.run.events.items) |event| {
                try run.control.emit(event);
                try run.retainEvent(event);
            }
            // Shared admission has already charged each worker. Accumulate
            // only its local delta so an enclosing child keeps its own ceiling.
            inline for (@typeInfo(c.Usage).@"struct".field_names) |field| {
                @field(run.usage, field) += @field(task.run.usage, field) - @field(baseline, field);
            }
        }
        next += count;
        if (ir.hasFinding(page.*, "model_call_budget_exhausted")) break;
    }
}

fn annotate(a: A, page: *c.Page) !void {
    if (!ir.annotate(page.text, @constCast(page.regions))) {
        if (page.status != .truncated) page.status = .unresolved;
        try ir.finding(a, page, "region_text_mapping_unresolved", true);
    }
}

fn applyPage(a: A, supplied: c.Page, candidate: Candidate, model: []const u8, limit: usize) !c.Page {
    var page = supplied;
    page.text = try a.dupe(u8, candidate.text);
    page.regions = try ir.cloneRegions(a, candidate.regions, limit);
    page.tables = try ir.cloneTables(a, candidate.tables, limit);
    page.fields = try ir.cloneValue(a, candidate.fields);
    page.model = model;
    page.origin = candidate.origin;
    page.status = if (candidate.truncated) .truncated else candidate.status orelse if (meaningful(page.text)) .complete else .unresolved;
    return page;
}

fn refine(a: A, def: c.Definition, driver: Driver, page_index: usize, supplied: c.Page, region: ?c.Region, run: *c.Run) !c.Page {
    var changed = false;
    return refineTracked(a, def, driver, page_index, supplied, region, run, &changed);
}
fn refineTracked(a: A, def: c.Definition, driver: Driver, page_index: usize, supplied: c.Page, region: ?c.Region, run: *c.Run, changed: *bool) !c.Page {
    var page = supplied;
    var current = region;
    refineInto(a, def, driver, page_index, &page, &current, run, changed) catch |err| {
        page = try interrupted(a, page, err);
    };
    if (current) |v| page.regions = try a.dupe(c.Region, &.{v});
    return page;
}

/// Recoverable orchestration interruptions retain the latest accepted data.
/// Cancellation, deadlines, memory and hard resource limits still propagate.
pub fn interrupted(a: A, supplied: c.Page, err: anyerror) !c.Page {
    const reason = switch (err) {
        error.DocumentBudgetExhausted => "model_call_budget_exhausted",
        error.DocumentRendererUnavailable => "renderer_unavailable_for_visual_content",
        else => return err,
    };
    var page = supplied;
    if (page.status != .truncated) page.status = .unresolved;
    try ir.finding(a, &page, reason, true);
    return page;
}

fn refineInto(a: A, def: c.Definition, driver: Driver, page_index: usize, page: *c.Page, current: *?c.Region, run: *c.Run, changed: *bool) !void {
    const r = def.read.?;
    var attempted: [3]Action = undefined;
    var attempts: usize = 0;
    while (attempts < @min(run.limits.max_region_attempts, attempted.len)) {
        var allowed: [5]Action = undefined;
        var count: usize = 0;
        for ([_]Action{ .accept, .accurate, .table, .fallback, .unresolved }) |action| {
            if ((action == .accurate and r.accurate_reader == null) or
                (action == .table and r.table_reader == null) or
                (action == .fallback and r.fallback_reader == null)) continue;
            var repeated = false;
            for (attempted[0..attempts]) |prior| if (action == prior) {
                repeated = true;
            };
            if (!repeated) {
                allowed[count] = action;
                count += 1;
            }
        }
        var action = defaultAction(r, page.*, current.*);
        if (r.router) |router| {
            const decide = driver.decide_fn orelse return error.DocumentDecisionUnavailable;
            try run.call();
            try run.emit(.{ .phase = .decision, .source_id = page.source_id, .page_number = page.number, .model = (try def.binding(router, .decide)).model, .region_id = if (current.*) |v| v.id else null });
            action = try decide(driver.ptr, a, try def.binding(router, .decide), .{
                .page = page.*,
                .region = current.*,
                .allowed = allowed[0..count],
                .remaining_calls = run.remainingCalls(),
            }, run);
        }
        try run.emit(.{ .phase = .decision, .source_id = page.source_id, .page_number = page.number, .model = if (r.router) |name| (try def.binding(name, .decide)).model else null, .region_id = if (current.*) |v| v.id else null, .action = action });
        var permitted = false;
        for (allowed[0..count]) |v| if (v == action) {
            permitted = true;
        };
        if (!permitted) {
            if (r.router != null) return error.DocumentActionDenied;
            break;
        }
        if (action == .accept) break;
        if (action == .unresolved) {
            if (page.status != .truncated) page.status = .unresolved;
            break;
        }
        const binding = (try selectedBinding(def, action)).?;
        attempted[attempts] = action;
        attempts += 1;
        try run.call();
        var scratch = std.heap.ArenaAllocator.init(run.work_allocator orelse a);
        defer scratch.deinit();
        const candidate = try driver.read_fn(driver.ptr, scratch.allocator(), binding, page_index, if (current.*) |v| v.bbox else null, run);
        try run.emit(.{ .phase = .verification, .source_id = page.source_id, .page_number = page.number, .model = binding.model, .region_id = if (current.*) |v| v.id else null });
        // A second answer is not evidence of improvement. Keep the original if
        // no validator can establish support for the replacement.
        const validation = if (driver.validate_fn) |validate| try validate(driver.ptr, page.*, current.*, candidate, run) else .unsupported;
        if (validation == .unchanged) {
            // Keep the complete accepted representation, including descendant
            // origins, fields, tables and completion. Equal text establishes no
            // new source support and must not set validated or erase metadata.
            break;
        }
        if (validation == .source_supported and meaningful(candidate.text) and !candidate.truncated and (candidate.status == null or candidate.status.? == .complete)) {
            changed.* = true;
            if (current.*) |*v| {
                v.text = try a.dupe(u8, candidate.text);
                v.model = binding.model;
                v.origin = candidate.origin;
                v.validated = true;
                v.confidence = candidate.confidence;
                v.children = try ir.cloneRegions(a, candidate.regions, run.limits.max_regions);
                v.tables = try ir.cloneTables(a, candidate.tables, run.limits.max_regions);
                v.fields = try ir.cloneValue(a, candidate.fields);
            } else {
                const repaired_native = ir.hasFinding(page.*, "native_font_fallback") and !std.mem.eql(u8, page.text, candidate.text);
                page.* = try applyPage(a, page.*, candidate, binding.model, run.limits.max_regions);
                if (repaired_native) try ir.finding(a, page, "native_text_superseded_by_verification", true);
                try ir.finding(a, page, "native_text_coverage_incomplete", !support.coversNative(page.native_text, page.text) and !ir.hasFinding(page.*, "native_text_superseded_by_verification"));
            }
            page.status = .complete;
            break;
        }
        page.status = .unresolved;
        try run.emit(.{ .phase = .unresolved, .source_id = page.source_id, .page_number = page.number, .region_id = if (current.*) |v| v.id else null, .model = binding.model, .status = .unresolved, .finding = "replacement_not_supported_by_source" });
    }
}

test "document independent regions overlap leaf calls and merge revisions events and usage in order" {
    const Shared = struct {
        entered: std.atomic.Value(usize) = .init(0),
        ready: std.Io.Event = .unset,
        fail: bool = false,
    };
    const Fake = struct {
        io: std.Io,
        state: *Shared,
        fn read(raw: *anyopaque, _: A, binding: c.Binding, _: usize, box: ?[4]f64, run: *c.Run) !Candidate {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var permit = try run.acquire(self.io, false);
            defer permit.deinit();
            run.physicalCall();
            if (std.mem.eql(u8, binding.model, "fast")) return .{ .text = "a b", .regions = &.{
                .{ .id = 0, .text = "a", .bbox = .{ 0, 0, 10, 10 }, .confidence = 0 },
                .{ .id = 1, .text = "b", .bbox = .{ 10, 0, 20, 10 }, .confidence = 0 },
            } };
            if (self.state.entered.fetchAdd(1, .acq_rel) == 1) self.state.ready.set(self.io);
            try self.state.ready.waitTimeout(self.io, .{ .duration = .{ .raw = .fromMilliseconds(5000), .clock = .awake } });
            if (self.state.fail and box.?[0] == 0) return error.ReadTransientFailure;
            try run.tokens(1, 1);
            return .{ .text = if (box.?[0] == 0) "alpha" else "beta" };
        }
        fn validate(_: *anyopaque, _: c.Page, _: ?c.Region, _: Candidate, _: *c.Run) !Validation {
            return .source_supported;
        }
        fn prepare(_: *anyopaque, _: *c.Run) !bool {
            return true;
        }
        fn fork(raw: *anyopaque, a: A) !Driver {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const child = try a.create(@This());
            child.* = self.*;
            return .{ .ptr = child, .read_fn = read, .validate_fn = validate };
        }
    };
    const backing = std.testing.allocator;
    var pool = std.Io.Threaded.init(backing, .{});
    defer pool.deinit();
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    for ([_]enum { normal, failure, nested_budget }{ .normal, .failure, .nested_budget }) |mode| {
        const fail = mode == .failure;
        var state = Shared{ .fail = fail };
        if (mode == .nested_budget) state.ready.set(pool.io());
        var fake = Fake{ .io = pool.io(), .state = &state };
        const limits: c.Limits = .{ .max_parallel_calls = 2, .max_model_calls = if (mode == .nested_budget) 2 else 50 };
        var ledger = c.SharedAccounting{ .limits = .{ .max_parallel_calls = 2, .max_model_calls = 50 }, .usage = .{}, .event_count = 0 };
        var run = c.Run{ .limits = limits, .shared = &ledger, .work_allocator = backing, .event_allocator = arena.allocator() };
        defer run.deinit();
        const definition: c.Definition = .{ .models = &.{ .{ .name = "fast", .model = "fast", .operation = .read }, .{ .name = "accurate", .model = "accurate", .operation = .read } }, .read = .{ .reader = "fast", .accurate_reader = "accurate" } };
        const invocation = readPage(arena.allocator(), definition, .{ .ptr = &fake, .read_fn = Fake.read, .validate_fn = Fake.validate, .parallel = .{ .io = pool.io(), .prepare_fn = Fake.prepare, .fork_fn = Fake.fork } }, 0, .{ .source_id = "receipt", .number = 1, .kind = .image }, &run);
        if (fail) {
            try std.testing.expectError(error.ReadTransientFailure, invocation);
            try std.testing.expectEqual(@as(usize, 0), ledger.active_calls);
            try std.testing.expectEqual(@as(usize, 0), ledger.active_regions);
            continue;
        }
        const page = try invocation;
        if (mode == .nested_budget) {
            try std.testing.expectEqual(c.Status.unresolved, page.status);
            try std.testing.expectEqual(@as(usize, 2), run.usage.model_calls);
            try std.testing.expectEqual(@as(usize, 2), ledger.usage.model_calls);
            try std.testing.expectEqual(@as(usize, 2), ledger.usage.physical_model_calls);
            try std.testing.expectEqual(@as(usize, 0), ledger.active_calls);
            try std.testing.expectEqual(@as(usize, 0), ledger.active_regions);
            continue;
        }
        try std.testing.expectEqualStrings("alpha beta", page.text);
        try std.testing.expectEqual(c.Status.complete, page.status);
        try std.testing.expectEqual(@as(usize, 3), run.usage.model_calls);
        try std.testing.expectEqual(@as(usize, 3), ledger.usage.physical_model_calls);
        try std.testing.expectEqual(@as(u64, 4), run.usage.total_tokens);
        try std.testing.expectEqual(@as(u64, 4), ledger.usage.total_tokens);
        try std.testing.expectEqual(@as(usize, 0), ledger.active_calls);
        try std.testing.expectEqual(@as(usize, 0), ledger.active_regions);
        var previous: usize = 0;
        for (run.events.items) |event| if (event.region_id) |id| {
            try std.testing.expect(id >= previous);
            previous = id;
        };
    }
}

test "document escalation cannot replace correct source with plausible unsupported text" {
    const Fake = struct {
        calls: usize = 0,
        fn read(raw: *anyopaque, _: A, _: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) !Candidate {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return if (self.calls == 1) .{ .text = "", .regions = &.{} } else .{ .text = "Invented total $100" };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const def = c.Definition{ .models = &.{ .{ .name = "fast", .model = "fast", .operation = .read }, .{ .name = "big", .model = "big", .operation = .read } }, .read = .{ .reader = "fast", .accurate_reader = "big" } };
    var fake = Fake{};
    var run = c.Run{ .limits = .{} };
    const page = try readPage(arena.allocator(), def, .{ .ptr = &fake, .read_fn = Fake.read }, 0, .{ .source_id = "receipt", .number = 1, .kind = .image }, &run);
    try std.testing.expectEqual(c.Status.unresolved, page.status);
    try std.testing.expectEqualStrings("", page.text);
    try std.testing.expectEqual(@as(usize, 2), run.usage.model_calls);
}

test "document native page fast path consumes no model slots" {
    const Fake = struct {
        fn read(_: *anyopaque, _: A, _: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) !Candidate {
            return error.UnexpectedCall;
        }
    };
    var marker: u8 = 0;
    var run = c.Run{ .limits = .{} };
    const def = c.Definition{ .models = &.{.{ .name = "fast", .model = "fast", .operation = .read }}, .read = .{ .reader = "fast" } };
    const page = try readPage(std.testing.allocator, def, .{ .ptr = &marker, .read_fn = Fake.read }, 0, .{ .source_id = "pdf", .number = 2, .kind = .page, .native_text = "Invoice 17" }, &run);
    try std.testing.expectEqual(c.Status.complete, page.status);
    try std.testing.expectEqual(@as(usize, 0), run.usage.model_calls);
}

test "document region evaluation retains fast text and selectively corroborates table and uncertain text" {
    const Fake = struct {
        calls: usize = 0,
        fn read(raw: *anyopaque, _: A, binding: c.Binding, _: usize, box: ?[4]f64, run: *c.Run) !Candidate {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            run.usage.physical_model_calls += 1;
            if (std.mem.eql(u8, binding.name, "fast")) return .{ .text = "Heading\n?\n12.OO", .regions = &.{
                .{ .id = 0, .text = "Heading", .bbox = .{ 0, 0, 10, 10 }, .confidence = 0.99 },
                .{ .id = 1, .text = "?", .bbox = .{ 0, 10, 10, 20 }, .confidence = 0.1 },
                .{ .id = 2, .text = "12.OO", .bbox = .{ 0, 20, 10, 30 }, .label = "table", .confidence = 0.9 },
            } };
            if (box == null) return error.TestUnexpectedResult;
            if (std.mem.eql(u8, binding.name, "accurate")) {
                try std.testing.expectEqual(@as(f64, 10), box.?[1]);
                return .{ .text = "Coffee" };
            }
            try std.testing.expectEqualStrings("table", binding.name);
            try std.testing.expectEqual(@as(f64, 20), box.?[1]);
            return .{ .text = "12.00" };
        }
        fn validate(_: *anyopaque, _: c.Page, region: ?c.Region, candidate: Candidate, _: *c.Run) !Validation {
            // This deterministic fixture is the source-evidence oracle, not a
            // model approving its own output.
            return if (if (region.?.id == 1) std.mem.eql(u8, candidate.text, "Coffee") else std.mem.eql(u8, candidate.text, "12.00")) .source_supported else .unsupported;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var fake = Fake{};
    var run = c.Run{ .limits = .{ .max_model_calls = 3 } };
    const def = c.Definition{ .models = &.{ .{ .name = "fast", .model = "fast", .operation = .read }, .{ .name = "accurate", .model = "accurate", .operation = .read }, .{ .name = "table", .model = "table", .operation = .read } }, .read = .{ .reader = "fast", .accurate_reader = "accurate", .table_reader = "table" } };
    const page = try readPage(arena.allocator(), def, .{ .ptr = &fake, .read_fn = Fake.read, .validate_fn = Fake.validate }, 0, .{ .source_id = "receipt", .number = 1, .kind = .image }, &run);
    try std.testing.expectEqualStrings("Heading\nCoffee\n12.00", page.text);
    try std.testing.expectEqual(c.Status.complete, page.status);
    try std.testing.expectEqual(@as(usize, 3), fake.calls);
    try std.testing.expect(!page.regions[0].validated);
    try std.testing.expect(page.regions[1].validated and page.regions[2].validated);
}

test "document initial OCR cannot silently discard known native content" {
    const Fake = struct {
        fn read(_: *anyopaque, _: A, _: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) !Candidate {
            return .{ .text = "Header" };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var run = c.Run{ .limits = .{} };
    const def = c.Definition{ .models = &.{.{ .name = "fast", .model = "fast", .operation = .read }}, .read = .{ .reader = "fast" } };
    const page = try readPage(arena.allocator(), def, .{ .ptr = undefined, .read_fn = Fake.read }, 0, .{ .source_id = "doc", .number = 1, .kind = .page, .native_text = "Header\nBody", .findings = &.{"visual_content_requires_inspection"} }, &run);
    try std.testing.expect(page.status != .complete or std.mem.indexOf(u8, page.text, "Body") != null);
}

test "document accepted specialist revision retains tables and regions" {
    const Fake = struct {
        fn read(_: *anyopaque, _: A, binding: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) !Candidate {
            if (std.mem.eql(u8, binding.name, "fast")) return .{ .text = "" };
            return .{ .text = "Correct", .regions = &.{.{ .id = 0, .text = "Correct", .bbox = .{ 0, 0, 10, 10 } }}, .tables = &.{.{ .id = 0, .rows = 1, .columns = 1, .cells = &.{.{ .row = 0, .column = 0, .text = "Correct" }} }} };
        }
        fn validate(_: *anyopaque, _: c.Page, _: ?c.Region, _: Candidate, _: *c.Run) !Validation {
            return .source_supported;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var run = c.Run{ .limits = .{} };
    const def = c.Definition{ .models = &.{ .{ .name = "fast", .model = "fast", .operation = .read }, .{ .name = "accurate", .model = "accurate", .operation = .read } }, .read = .{ .reader = "fast", .accurate_reader = "accurate" } };
    const page = try readPage(arena.allocator(), def, .{ .ptr = undefined, .read_fn = Fake.read, .validate_fn = Fake.validate }, 0, .{ .source_id = "doc", .number = 1, .kind = .page }, &run);
    try std.testing.expectEqual(@as(usize, 1), page.tables.len);
    try std.testing.expectEqual(@as(usize, 1), page.regions.len);
}

test "document mixed interpretation does not invalidate unrelated literal spans" {
    const Fake = struct {
        fn read(_: *anyopaque, _: A, _: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) !Candidate {
            return .{ .text = "Header\nCaption", .regions = &.{ .{ .id = 0, .text = "Header", .origin = .transcription }, .{ .id = 1, .text = "Caption", .origin = .interpretation } } };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = c.Run{ .limits = .{} };
    const def = c.Definition{ .models = &.{.{ .name = "fast", .model = "fast", .operation = .read }}, .read = .{ .reader = "fast" } };
    const page = try readPage(a, def, .{ .ptr = undefined, .read_fn = Fake.read }, 0, .{ .source_id = "doc", .number = 1, .kind = .image }, &run);
    const metadata = try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, .{ .source_id = "doc", .segments = .{.{ .number = 1, .start = 0, .end = page.text.len, .origin = page.origin, .regions = page.regions }} }, .{}), .{});
    var item = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"structures\":{\"r\":[{\"title\":{\"value\":\"Header\",\"start\":0,\"end\":6}}]}}", .{});
    const supported = try @import("evidence.zig").attach(a, &item, page.text, metadata, true, false);
    try std.testing.expect(supported);
    var caption = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"structures\":{\"r\":[{\"caption\":{\"value\":\"Caption\",\"start\":7,\"end\":14}}]}}", .{});
    try std.testing.expect(!try @import("evidence.zig").attach(a, &caption, page.text, metadata, true, false));
}

test "document regional revisions retain owned structure after scratch teardown" {
    const Fake = struct {
        fn read(_: *anyopaque, a: A, binding: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) !Candidate {
            if (std.mem.eql(u8, binding.name, "fast")) return .{ .text = "Bad", .regions = &.{.{ .id = 3, .text = "Bad", .label = "table", .bbox = .{ 0, 0, 10, 10 } }} };
            const text = try a.dupe(u8, "Correct");
            return .{
                .text = text,
                .regions = try a.dupe(c.Region, &.{.{ .id = 0, .text = text, .bbox = .{ 0, 0, 10, 10 } }}),
                .tables = try a.dupe(c.Table, &.{.{ .id = 0, .rows = 1, .columns = 1, .cells = try a.dupe(c.Cell, &.{.{ .row = 0, .column = 0, .text = text }}) }}),
                .fields = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"count\":7}", .{}),
            };
        }
        fn validate(_: *anyopaque, _: c.Page, _: ?c.Region, _: Candidate, _: *c.Run) !Validation {
            return .source_supported;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var run = c.Run{ .limits = .{}, .work_allocator = std.testing.allocator };
    const def = c.Definition{ .models = &.{ .{ .name = "fast", .model = "fast", .operation = .read }, .{ .name = "table", .model = "table", .operation = .read } }, .read = .{ .reader = "fast", .table_reader = "table" } };
    const page = try readPage(arena.allocator(), def, .{ .ptr = undefined, .read_fn = Fake.read, .validate_fn = Fake.validate }, 0, .{ .source_id = "doc", .number = 1, .kind = .image }, &run);
    try std.testing.expectEqual(c.Status.complete, page.status);
    try std.testing.expectEqualStrings("Correct", page.regions[0].children[0].text);
    try std.testing.expectEqualStrings("Correct", page.regions[0].tables[0].cells[0].text);
    try std.testing.expectEqual(@as(i64, 7), page.regions[0].fields.?.object.get("count").?.integer);
    try std.testing.expectEqual(@as(usize, 1), page.tables.len);
    try std.testing.expectEqual(@as(?usize, 3), page.tables[0].region_id);
}

test "document initial coverage preserves native words outside detected regions" {
    const Fake = struct {
        fn read(_: *anyopaque, _: A, _: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) !Candidate {
            return .{ .text = "Header\nBody\nVisual", .regions = &.{.{ .id = 0, .text = "Header", .bbox = .{ 0, 0, 10, 10 } }} };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var run = c.Run{ .limits = .{} };
    const def = c.Definition{ .models = &.{.{ .name = "fast", .model = "fast", .operation = .read }}, .read = .{ .reader = "fast" } };
    const page = try readPage(arena.allocator(), def, .{ .ptr = undefined, .read_fn = Fake.read }, 0, .{ .source_id = "doc", .number = 1, .kind = .page, .native_text = "Header\nBody", .findings = &.{"visual_content_requires_inspection"} }, &run);
    try std.testing.expectEqual(c.Status.complete, page.status);
    try std.testing.expectEqualStrings("Header\nBody\nVisual", page.text);
}

test "document missing native coverage routes a whole-page repair before acceptance" {
    const Fake = struct {
        calls: usize = 0,
        fn read(raw: *anyopaque, _: A, binding: c.Binding, _: usize, box: ?[4]f64, _: *c.Run) !Candidate {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expect(box == null);
            return .{ .text = if (std.mem.eql(u8, binding.name, "fast")) "Header" else "Header\nBody" };
        }
        fn validate(_: *anyopaque, page: c.Page, _: ?c.Region, candidate: Candidate, _: *c.Run) !Validation {
            return if (std.mem.eql(u8, page.native_text, candidate.text)) .source_supported else .unsupported;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var fake = Fake{};
    var run = c.Run{ .limits = .{ .max_model_calls = 2 }, .work_allocator = std.testing.allocator };
    const def = c.Definition{ .models = &.{ .{ .name = "fast", .model = "fast", .operation = .read }, .{ .name = "accurate", .model = "accurate", .operation = .read } }, .read = .{ .reader = "fast", .accurate_reader = "accurate" } };
    const page = try readPage(arena.allocator(), def, .{ .ptr = &fake, .read_fn = Fake.read, .validate_fn = Fake.validate }, 0, .{ .source_id = "doc", .number = 1, .kind = .page, .native_text = "Header\nBody", .findings = &.{"visual_content_requires_inspection"} }, &run);
    try std.testing.expectEqual(c.Status.complete, page.status);
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectEqualStrings("Header\nBody", page.text);
    try std.testing.expect(!ir.hasFinding(page, "native_text_coverage_incomplete"));
}

test "document accepted regional revision can remove a previously projected table" {
    const Fake = struct {
        fn read(_: *anyopaque, _: A, binding: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) !Candidate {
            if (std.mem.eql(u8, binding.name, "fast")) return .{ .text = "Incorrect", .regions = &.{.{ .id = 7, .text = "Incorrect", .label = "table", .bbox = .{ 0, 0, 10, 10 } }}, .tables = &.{.{ .id = 0, .region_id = 7, .rows = 1, .columns = 1, .cells = &.{.{ .row = 0, .column = 0, .text = "Incorrect" }} }} };
            return .{ .text = "Correct prose", .tables = &.{} };
        }
        fn validate(_: *anyopaque, _: c.Page, _: ?c.Region, _: Candidate, _: *c.Run) !Validation {
            return .source_supported;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var run = c.Run{ .limits = .{} };
    const def = c.Definition{ .models = &.{ .{ .name = "fast", .model = "fast", .operation = .read }, .{ .name = "table", .model = "table", .operation = .read } }, .read = .{ .reader = "fast", .table_reader = "table" } };
    const page = try readPage(arena.allocator(), def, .{ .ptr = undefined, .read_fn = Fake.read, .validate_fn = Fake.validate }, 0, .{ .source_id = "photo", .number = 1, .kind = .image }, &run);
    try std.testing.expectEqual(@as(usize, 0), page.tables.len);
}

test "document truncated region lists require whole-page repair" {
    const Fake = struct {
        calls: usize = 0,
        fn read(raw: *anyopaque, _: A, binding: c.Binding, _: usize, box: ?[4]f64, _: *c.Run) !Candidate {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expect(box == null);
            if (std.mem.eql(u8, binding.name, "fast")) return .{ .text = "Header", .truncated = true, .regions = &.{.{ .id = 0, .text = "Header" }} };
            return .{ .text = "Header\nTail" };
        }
        fn validate(_: *anyopaque, _: c.Page, _: ?c.Region, _: Candidate, _: *c.Run) !Validation {
            return .source_supported;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var fake = Fake{};
    var run = c.Run{ .limits = .{} };
    const def = c.Definition{ .models = &.{ .{ .name = "fast", .model = "fast", .operation = .read }, .{ .name = "accurate", .model = "accurate", .operation = .read } }, .read = .{ .reader = "fast", .accurate_reader = "accurate" } };
    const page = try readPage(arena.allocator(), def, .{ .ptr = &fake, .read_fn = Fake.read, .validate_fn = Fake.validate }, 0, .{ .source_id = "photo", .number = 1, .kind = .image }, &run);
    try std.testing.expectEqual(c.Status.complete, page.status);
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectEqualStrings("Header\nTail", page.text);
}

test "document interruptions retain accepted regional work and propagate cancellation" {
    const Fake = struct {
        calls: usize = 0,
        failure: anyerror = error.DocumentBudgetExhausted,
        fn read(raw: *anyopaque, a: A, _: c.Binding, _: usize, _: ?[4]f64, run: *c.Run) !Candidate {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            run.usage.physical_model_calls += 1;
            if (self.calls == 1) return .{ .text = "Bad\nLater", .regions = &.{
                .{ .id = 10, .text = "Bad", .label = "table", .bbox = .{ 0, 0, 10, 10 } },
                .{ .id = 11, .text = "Later", .label = "table", .bbox = .{ 0, 10, 10, 20 } },
            } };
            if (self.calls > 2) return self.failure;
            return .{ .text = try a.dupe(u8, "Correct"), .tables = try a.dupe(c.Table, &.{.{ .id = 0, .rows = 1, .columns = 1, .cells = try a.dupe(c.Cell, &.{.{ .row = 0, .column = 0, .text = try a.dupe(u8, "Correct") }}) }}), .fields = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"amount\":9007199254740993.01}", .{ .parse_numbers = false }) };
        }
        fn validate(_: *anyopaque, _: c.Page, _: ?c.Region, _: Candidate, _: *c.Run) !Validation {
            return .source_supported;
        }
    };
    const def = c.Definition{ .models = &.{ .{ .name = "fast", .model = "fast", .operation = .read }, .{ .name = "table", .model = "table", .operation = .read } }, .read = .{ .reader = "fast", .table_reader = "table" } };
    for ([_]anyerror{ error.DocumentBudgetExhausted, error.DocumentRendererUnavailable, error.Canceled }) |failure| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        var fake = Fake{ .failure = failure };
        var run = c.Run{ .limits = .{ .max_model_calls = if (failure == error.DocumentBudgetExhausted) 2 else 3 }, .work_allocator = std.testing.allocator };
        const result = readPage(arena.allocator(), def, .{ .ptr = &fake, .read_fn = Fake.read, .validate_fn = Fake.validate }, 0, .{ .source_id = "receipt", .number = 1, .kind = .image }, &run);
        if (failure == error.Canceled) {
            try std.testing.expectError(error.Canceled, result);
            continue;
        }
        const page = try result;
        try std.testing.expectEqual(c.Status.unresolved, page.status);
        try std.testing.expectEqualStrings("Correct\nLater", page.text);
        try std.testing.expectEqual(@as(usize, 2), page.regions.len);
        try std.testing.expectEqualStrings("9007199254740993.01", page.regions[0].fields.?.object.get("amount").?.number_string);
        try std.testing.expectEqualStrings("Correct", page.tables[0].cells[0].text);
        try std.testing.expectEqual(@as(?usize, 10), page.tables[0].region_id);
        try std.testing.expect(ir.hasFinding(page, if (failure == error.DocumentBudgetExhausted) "model_call_budget_exhausted" else "renderer_unavailable_for_visual_content"));
        try std.testing.expectEqual(@as(usize, if (failure == error.DocumentBudgetExhausted) 2 else 3), fake.calls);
    }
}

test "document child output budgets include structured page data before returning to a parent" {
    const Fake = struct {
        fn read(_: *anyopaque, a: A, _: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) !Candidate {
            const value: [2000]u8 = @splat('x');
            var fields: std.json.Value = .{ .object = .{} };
            try fields.object.put(a, "blob", .{ .string = try a.dupe(u8, &value) });
            return .{ .text = "Small", .fields = fields };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var dummy: u8 = 0;
    var run = c.Run{ .limits = .{} };
    const outer = run.limits;
    run.limits = run.nestedLimits(.{ .max_output_bytes = 1024 });
    defer run.limits = outer;
    const def = c.Definition{ .models = &.{.{ .name = "leaf", .model = "leaf", .operation = .read }}, .read = .{ .reader = "leaf" } };
    try std.testing.expectError(error.DocumentOutputTooLarge, readPage(arena.allocator(), def, .{ .ptr = &dummy, .read_fn = Fake.read }, 0, .{ .source_id = "image", .number = 1, .kind = .image }, &run));
}

test "document unchanged refinement preserves accepted structure and completion without allocation" {
    const Fake = struct {
        fn read(_: *anyopaque, _: A, _: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) !Candidate {
            return .{ .text = "caption", .origin = .transcription, .status = .complete, .fields = .{ .string = "replacement" } };
        }
        fn validate(_: *anyopaque, _: c.Page, _: ?c.Region, _: Candidate, _: *c.Run) !Validation {
            return .unchanged;
        }
    };
    const def = c.Definition{ .models = &.{ .{ .name = "fast", .model = "fast", .operation = .read }, .{ .name = "accurate", .model = "accurate", .operation = .read } }, .read = .{ .reader = "fast", .accurate_reader = "accurate" } };
    var dummy: u8 = 0;
    const driver = Driver{ .ptr = &dummy, .read_fn = Fake.read, .validate_fn = Fake.validate };
    var run = c.Run{ .limits = .{} };
    const cells = [_]c.Cell{.{ .row = 0, .column = 0, .text = "caption" }};
    const tables = [_]c.Table{.{ .id = 3, .rows = 1, .columns = 1, .cells = &cells }};
    const children = [_]c.Region{.{ .id = 9, .text = "caption", .origin = .estimated }};
    var page = c.Page{ .source_id = "receipt", .number = 1, .kind = .image, .text = "caption", .origin = .derived, .status = .truncated, .tables = &tables, .fields = .{ .string = "accepted" } };
    var region: ?c.Region = null;
    var changed = false;
    try refineInto(std.testing.failing_allocator, def, driver, 0, &page, &region, &run, &changed);
    try std.testing.expect(!changed);
    try std.testing.expectEqual(c.Status.truncated, page.status);
    try std.testing.expectEqual(c.Origin.derived, page.origin);
    try std.testing.expectEqualStrings("accepted", page.fields.?.string);
    try std.testing.expect(page.tables.ptr == &tables);
    page.status = .unresolved;
    region = .{ .id = 7, .text = "caption", .origin = .interpretation, .confidence = 0.1, .children = &children, .tables = &tables, .fields = .{ .string = "accepted" } };
    changed = false;
    try refineInto(std.testing.failing_allocator, def, driver, 0, &page, &region, &run, &changed);
    try std.testing.expect(!changed);
    try std.testing.expectEqual(c.Status.unresolved, page.status);
    try std.testing.expectEqual(c.Origin.interpretation, region.?.origin);
    try std.testing.expect(!region.?.validated);
    try std.testing.expect(region.?.children.ptr == &children);
    try std.testing.expect(region.?.tables.ptr == &tables);
    try std.testing.expectEqualStrings("accepted", region.?.fields.?.string);
}

test "document unchanged mixed table locations preserve authoritative tables" {
    const Fake = struct {
        fn read(_: *anyopaque, _: std.mem.Allocator, _: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) !Candidate {
            const cells = &[_]c.Cell{.{ .row = 0, .column = 0, .text = "nested" }};
            const flat_cells = &[_]c.Cell{.{ .row = 0, .column = 0, .text = "flat" }};
            return .{ .text = "caption", .regions = &.{.{ .id = 7, .text = "caption", .tables = &.{.{ .id = 2, .rows = 1, .columns = 1, .cells = cells }} }}, .tables = &.{.{ .id = 1, .region_id = 7, .rows = 1, .columns = 1, .cells = flat_cells }} };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var run = c.Run{ .limits = .{} };
    var marker: u8 = 0;
    const def = c.Definition{ .models = &.{.{ .name = "fast", .model = "fast", .operation = .read }}, .read = .{ .reader = "fast" } };
    const page = try readPage(arena.allocator(), def, .{ .ptr = &marker, .read_fn = Fake.read }, 0, .{ .source_id = "receipt", .number = 1, .kind = .image }, &run);
    std.debug.print("status={s}, flat tables={d}, surviving table={d}\n", .{ @tagName(page.status), page.tables.len, page.tables[0].id });
    var retained = false;
    for (page.tables) |t| if (t.id == 1) {
        retained = true;
    };
    try std.testing.expect(retained);
}

test "document read produced overlapping provenance cannot establish literal extraction" {
    const Fake = struct {
        fn read(_: *anyopaque, _: std.mem.Allocator, _: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) !Candidate {
            return .{ .text = "caption", .regions = &.{ .{ .id = 0, .text = "caption", .origin = .transcription }, .{ .id = 1, .text = "caption", .origin = .interpretation } } };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = c.Run{ .limits = .{} };
    var marker: u8 = 0;
    const def = c.Definition{ .models = &.{.{ .name = "fast", .model = "fast", .operation = .read }}, .read = .{ .reader = "fast" } };
    const page = try readPage(a, def, .{ .ptr = &marker, .read_fn = Fake.read }, 0, .{ .source_id = "receipt", .number = 1, .kind = .image }, &run);
    try std.testing.expectEqual(c.Status.unresolved, page.status);
    try std.testing.expect(page.regions[1].text_start == null);
    const wire = try std.json.Stringify.valueAlloc(a, .{ .source_id = page.source_id, .segments = .{.{ .number = page.number, .start = @as(usize, 0), .end = page.text.len, .origin = page.origin, .regions = page.regions }} }, .{});
    const metadata = try std.json.parseFromSliceLeaky(std.json.Value, a, wire, .{});
    var item = try std.json.parseFromSliceLeaky(std.json.Value, a,
        \\{"structures":{"receipt":[{"caption":{"value":"caption","start":0,"end":7}}]}}
    , .{});
    const supported = try @import("evidence.zig").attach(a, &item, page.text, metadata, true, false);
    std.debug.print("normal read status={s}, interpreted region start={?}, strict extraction accepted={}\n", .{ @tagName(page.status), page.regions[1].text_start, supported });
    try std.testing.expect(!supported);
}

test "document explicit repeated text offsets retain nonliteral extraction proof" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Fake = struct {
        fn read(_: *anyopaque, _: std.mem.Allocator, _: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) anyerror!Candidate {
            return .{ .text = "10 10", .origin = .transcription, .regions = &.{.{ .id = 1, .text = "10", .origin = .interpretation, .text_start = 3, .text_end = 5 }} };
        }
    };
    var dummy: u8 = 0;
    var run = c.Run{ .limits = .{} };
    const page = try readPage(a, .{ .models = &.{.{ .name = "leaf", .model = "leaf", .operation = .read }}, .read = .{ .reader = "leaf", .min_confidence = 0 } }, .{ .ptr = &dummy, .read_fn = Fake.read }, 0, .{ .source_id = "receipt", .number = 1, .kind = .image }, &run);
    const metadata = try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, .{ .source_id = "receipt", .segments = .{.{ .number = 1, .start = 0, .end = 5, .origin = page.origin, .regions = page.regions }} }, .{}), .{});
    var item = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"structures\":{\"record\":[{\"value\":{\"value\":\"10\",\"start\":3,\"end\":5}}]}}", .{});
    const supported = try @import("evidence.zig").attach(a, &item, page.text, metadata, true, false);
    try std.testing.expectEqual(@as(usize, 3), page.regions[0].text_start.?);
    try std.testing.expect(!supported);
}

test "document native shortcut leaves ambiguous placements unresolved without dispatch" {
    const Fake = struct {
        fn read(_: *anyopaque, _: A, _: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) !Candidate {
            return error.UnexpectedCall;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var marker: u8 = 0;
    var run = c.Run{ .limits = .{} };
    const page = try readPage(arena.allocator(), .{ .models = &.{.{ .name = "leaf", .model = "leaf", .operation = .read }}, .read = .{ .reader = "leaf" } }, .{ .ptr = &marker, .read_fn = Fake.read }, 0, .{ .source_id = "receipt", .number = 1, .kind = .page, .native_text = "10 10", .regions = &.{.{ .id = 1, .text = "10" }} }, &run);
    try std.testing.expectEqual(c.Status.unresolved, page.status);
    try std.testing.expect(ir.hasFinding(page, "region_text_mapping_unresolved"));
    try std.testing.expectEqual(@as(usize, 0), run.usage.model_calls);
}

test "document native crossing run cannot authorize amount loss through regional refinement" {
    const Fake = struct {
        calls: usize = 0,
        const runs = [_]c.Region{
            .{ .id = 0, .text = "Total", .bbox = .{ 1, 1, 4, 4 }, .origin = .native },
            .{ .id = 1, .text = "12.50", .bbox = .{ 5, 1, 15, 4 }, .origin = .native },
        };
        fn read(raw: *anyopaque, _: std.mem.Allocator, _: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) anyerror!Candidate {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return if (self.calls == 1) .{ .text = "Total 12.50 12.50", .regions = &.{.{ .id = 2, .text = "Total 12.50", .bbox = .{ 0, 0, 10, 10 }, .confidence = 0.1 }} } else .{ .text = "Total" };
        }
        fn validate(_: *anyopaque, page: c.Page, region: ?c.Region, candidate: Candidate, _: *c.Run) anyerror!Validation {
            return if (try @import("source_support.zig").nativeSupports(std.testing.allocator, page.native_text, &runs, region, candidate.text)) .source_supported else .unsupported;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var fake = Fake{};
    var run = c.Run{ .limits = .{} };
    const page = try readPage(arena.allocator(), .{ .models = &.{ .{ .name = "fast", .model = "fast", .operation = .read }, .{ .name = "accurate", .model = "accurate", .operation = .read } }, .read = .{ .reader = "fast", .accurate_reader = "accurate", .native_text = .always_ocr } }, .{ .ptr = &fake, .read_fn = Fake.read, .validate_fn = Fake.validate }, 0, .{ .source_id = "receipt", .number = 1, .kind = .page, .native_text = "Total 12.50" }, &run);
    try std.testing.expect(!page.regions[0].validated);
    try std.testing.expectEqualStrings("Total 12.50 12.50", page.text);
    try std.testing.expectEqual(c.Status.unresolved, page.status);
}

test "document pixel ownership ends when a region worker fails before the group join" {
    const readers = @import("antfly_readers");
    const runs = readers.document_runs;
    const a = std.testing.allocator;
    var memory = readers.testing.Store{ .a = a };
    defer memory.deinit();
    var io_impl = std.Io.Threaded.init(a, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const store = memory.interface();
    const identity: runs.Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    try store.create(.{ .run_id = "worker", .owner = "alice", .identity = identity, .limits = .{ .pixels = 3 }, .inputs = &.{} });
    const Clock = struct {
        fn now(_: ?*anyopaque) u64 {
            return 1;
        }
        fn unexpectedWait(_: ?*anyopaque) !void {
            return error.UnexpectedAdmissionWait;
        }
        fn unused(_: void, _: A) !runs.Session.Output {
            return error.UnexpectedCall;
        }
    };
    const lease = try store.claim(work, "worker", "alice", identity, 1, 100);
    var session = try runs.Session.init(a, store, lease, identity, .{ .now_fn = Clock.now }, 16, 32768);
    defer session.deinit();
    session.io = io;
    session.check_fn = Clock.unexpectedWait;
    var ledger = c.SharedAccounting{ .limits = .{}, .usage = .{}, .event_count = 0 };
    var parent = c.Run{ .limits = .{}, .shared = &ledger };
    const DriverState = struct {
        run: *c.Run = undefined,
        fn fork(raw: *anyopaque, _: A) !Driver {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.run.pixels(1);
            // Fail before the normal per-region flush, outside the crop helper.
            return error.DocumentSourceLimitExceeded;
        }
        fn prepare(_: *anyopaque, _: *c.Run) !bool {
            return true;
        }
        fn unused(_: *anyopaque, _: A, _: c.Binding, _: usize, _: ?[4]f64, _: *c.Run) !Candidate {
            return error.UnexpectedCall;
        }
    };
    var driver_state = DriverState{};
    var task = RegionTask{
        .arena = .init(a),
        .definition = .{ .models = &.{}, .read = .{} },
        .driver = .{ .ptr = &driver_state, .read_fn = DriverState.unused, .parallel = .{ .io = io, .fork_fn = DriverState.fork, .prepare_fn = DriverState.prepare } },
        .page_index = 0,
        .supplied = .{ .source_id = "source", .number = 1, .kind = .image },
        .region = .{ .id = 1, .text = "" },
        .run = .{ .durable = &session, .limits = .{}, .shared = &ledger },
        .permit = try parent.acquire(io, true),
    };
    defer task.arena.deinit();
    defer task.run.deinit();
    driver_state.run = &task.run;
    var finished: std.Io.Event = .unset;
    const Worker = struct {
        task: *RegionTask,
        io: std.Io,
        finished: *std.Io.Event,
        fn execute(self: *@This()) void {
            defer self.finished.set(self.io);
            self.task.execute() catch |err| {
                self.task.failure = err;
            };
        }
    };
    const Sibling = struct {
        session: *runs.Session,
        io: std.Io,
        finished: *std.Io.Event,
        failure: ?anyerror = null,
        fn execute(self: *@This()) void {
            self.finished.waitTimeout(self.io, .{ .duration = .{ .raw = .fromMilliseconds(5000), .clock = .awake } }) catch |err| {
                self.failure = err;
                return;
            };
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            _ = self.session.execute(scratch.allocator(), @splat(9), .{ .pixels = 1 }, 0, {}, Clock.unused) catch |err| {
                self.failure = err;
                return;
            };
            self.failure = error.TestUnexpectedResult;
        }
    };
    var worker = Worker{ .task = &task, .io = io, .finished = &finished };
    var sibling = Sibling{ .session = &session, .io = io, .finished = &finished };
    var group: std.Io.Group = .init;
    defer group.cancel(io);
    try group.concurrent(io, Worker.execute, .{&worker});
    try group.concurrent(io, Sibling.execute, .{&sibling});
    try group.await(io);
    try std.testing.expectEqual(error.DocumentSourceLimitExceeded, task.failure.?);
    try std.testing.expectEqual(error.DocumentBudgetExhausted, sibling.failure.?);
    try std.testing.expectEqual(@as(u64, 0), task.run.pixel_meter.upper);
    try std.testing.expectEqual(@as(usize, 0), ledger.active_regions);
    const record = try store.load(work, "worker", "alice");
    try std.testing.expectEqual(@as(usize, 0), record.pending.len);
    try std.testing.expectEqual(@as(u64, 3), record.used.pixels);
    try std.testing.expectEqual(@as(u64, 3), record.uncertain.pixels);
}
