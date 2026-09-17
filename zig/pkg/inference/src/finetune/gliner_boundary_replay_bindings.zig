// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Typed dropout leaves for encoder-region recomputation. Construct only after
//! the single PEFT rewrite and descriptor remapping. No encoder activations,
//! optimizer state, RNG cursor, or eager generated-mask payloads are retained.
//! Recipes own names and descriptor copies. Context borrows immutable explicit
//! masks and a request-local transfer owner; both outlive every replay tape.
const std = @import("std");
const ml = @import("ml").graph;
const encoder = @import("gliner_boundary_encoder_graph.zig");
const peft = @import("gliner_boundary_peft_graph.zig");
const transfer = @import("gliner_boundary_training_transfer.zig");
const recomputed = @import("../graph/recomputed_training.zig");
const seeded = @import("../graph/seeded_training.zig");
const ops = @import("../ops/ops.zig");
const Budget = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const Id = ml.NodeId;
const Shape = ml.Shape;
const nil = ml.null_node;
const Sha = std.crypto.hash.sha2.Sha256;

pub const abi = "gliner25_encoder_region_dropout_replay_v1";
pub const Mask = struct { name: []const u8, values: []const f32 };
pub const Limits = struct {
    max_source_nodes: usize = 1_000_000,
    max_recipes: usize = 16384,
    max_candidates: usize = 65536,
    max_name_bytes: usize = 1024,
    max_host_bytes: usize = 64 * 1024 * 1024,
    /// One generated host mask at a time; transfer/device admission is separate.
    max_mask_bytes: usize = 64 * 1024 * 1024,
    /// Explicit masks remain fully charged to their immutable caller owner.
    max_explicit_bytes: usize = 512 * 1024 * 1024,
    max_work_items: usize = 1 << 28,
};
pub const Options = struct { limits: Limits = .{}, control: ?Control = null };
pub const Recipe = union(enum) { encoder: encoder.DropoutDescriptor, peft: peft.Use };
pub const Entry = struct { node: Id, key: [32]u8, shape: Shape, name: []const u8, recipe: Recipe };

fn check(control: ?Control) !void {
    if (control) |value| try value.check();
}
fn add(left: usize, right: usize) !usize {
    return std.math.add(usize, left, right) catch error.BoundaryReplayLimitExceeded;
}
fn sameHash(a: [32]u8, b: [32]u8) bool {
    return std.mem.eql(u8, &a, &b);
}
fn integer(hash: *Sha, value: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    hash.update(&bytes);
}
fn float(hash: *Sha, value: f32) void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, @bitCast(value), .little);
    hash.update(&bytes);
}
fn string(hash: *Sha, value: []const u8) void {
    integer(hash, value.len);
    hash.update(value);
}
fn sameShape(a: Shape, b: Shape) bool {
    return a.dtype == b.dtype and a.rank_ == b.rank_ and a.rank_ <= 8 and
        std.mem.eql(i64, a.dims[0..a.rank_], b.dims[0..b.rank_]) and
        std.mem.eql(i64, a.bounds[0..a.rank_], b.bounds[0..b.rank_]);
}
fn maskBytes(shape: Shape, limits: Limits) !usize {
    if (shape.dtype != .f32 or shape.rank_ == 0 or shape.rank_ > 8) return error.InvalidBoundaryReplayShape;
    for (shape.dims[0..shape.rank_], shape.bounds[0..shape.rank_]) |dim, bound| {
        if (dim <= 0 or dim > std.math.maxInt(i32) or bound != 0) return error.InvalidBoundaryReplayShape;
    }
    const bytes = try transfer.shapeBytes(shape);
    if (bytes > limits.max_mask_bytes) return error.BoundaryReplayLimitExceeded;
    return bytes;
}
fn probability(entry: Entry) f32 {
    return switch (entry.recipe) {
        .encoder => |value| value.probability,
        .peft => |value| value.probability,
    };
}
fn recipeKey(entry: Entry, limits: Limits) ![32]u8 {
    _ = try maskBytes(entry.shape, limits);
    if (entry.name.len == 0 or entry.name.len > limits.max_name_bytes or !std.unicode.utf8ValidateSlice(entry.name)) return error.InvalidBoundaryReplayRecipe;
    const p = probability(entry);
    if (!std.math.isFinite(p) or p < 0 or p >= 1) return error.InvalidBoundaryReplayRecipe;
    var hash = Sha.init(.{});
    hash.update(abi);
    string(&hash, entry.name);
    integer(&hash, entry.shape.rank_);
    for (entry.shape.dims[0..entry.shape.rank_]) |dim| integer(&hash, @intCast(dim));
    float(&hash, p);
    switch (entry.recipe) {
        .encoder => |value| {
            if (value.node != entry.node or !sameShape(value.shape, entry.shape)) return error.InvalidBoundaryReplayRecipe;
            var name: [128]u8 = undefined;
            const expected = try std.fmt.bufPrint(&name, "__gliner25.encoder.dropout.{s}.{d}", .{ @tagName(value.site.kind), value.site.layer });
            if (!std.mem.eql(u8, expected, entry.name)) return error.InvalidBoundaryReplayRecipe;
            string(&hash, "encoder_site_element_v1");
            integer(&hash, @intFromEnum(value.site.kind));
            integer(&hash, value.site.layer);
            integer(&hash, value.streamId());
        },
        .peft => |value| {
            if (value.mask != entry.node or value.mask_name == null or !std.mem.eql(u8, value.mask_name.?, entry.name) or
                !std.mem.startsWith(u8, entry.name, "__boundary_peft_mask.") or !sameShape(value.mask_shape, entry.shape)) return error.InvalidBoundaryReplayRecipe;
            string(&hash, "boundary_peft_site_element_v1");
            integer(&hash, value.occurrence);
            integer(&hash, value.rows);
            integer(&hash, value.stream);
        },
    }
    return hash.finalResult();
}

const Work = struct {
    used: usize = 0,
    options: Options,
    fn charge(self: *Work, amount: usize) !void {
        self.used = try add(self.used, amount);
        if (self.used > self.options.limits.max_work_items) return error.BoundaryReplayLimitExceeded;
        try check(self.options.control);
    }
};

/// DFS over ordinary and alternate-VJP edges, independent of allocation order.
/// A typed mask used only by a head outside these encoder roots is excluded.
fn closure(a: Allocator, graph: *const ml.Graph, outputs: []const Id, work: *Work) ![]u8 {
    const marks = try a.alloc(u8, graph.nodeCount());
    errdefer a.free(marks);
    @memset(marks, 0);
    const Frame = struct { node: Id, next: u8 = 0 };
    var stack = std.ArrayListUnmanaged(Frame).empty;
    defer stack.deinit(a);
    for (outputs) |output| {
        if (output >= marks.len) return error.InvalidBoundaryReplayGraph;
        if (marks[output] == 2) continue;
        marks[output] = 1;
        try stack.append(a, .{ .node = output });
        while (stack.items.len != 0) {
            try work.charge(1);
            const frame = &stack.items[stack.items.len - 1];
            const node = graph.node(frame.node);
            if (node.num_inputs > 4) return error.InvalidBoundaryReplayGraph;
            const edge_count = node.num_inputs + @as(u8, @intFromBool(node.vjp_alternate != nil));
            if (frame.next == edge_count) {
                marks[frame.node] = 2;
                _ = stack.pop();
                continue;
            }
            const child = if (frame.next < node.num_inputs) node.inputs[frame.next] else node.vjp_alternate;
            frame.next += 1;
            if (child >= marks.len or marks[child] == 1) return error.InvalidBoundaryReplayGraph;
            if (marks[child] == 2) continue;
            marks[child] = 1;
            try stack.append(a, .{ .node = child });
        }
    }
    return marks;
}

const HostOwner = struct {
    budget: Budget,
    failure: ?Budget.AllocationFailure = null,
    fn failed(raw: ?*anyopaque, value: Budget.AllocationFailure) void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.failure = value;
    }
    fn translate(self: *const @This(), err: anyerror) anyerror {
        if (err == error.OutOfMemory) if (self.failure) |failure| {
            if (failure.kind == .declared_limit) return error.BoundaryReplayLimitExceeded;
        };
        return err;
    }
};

pub const Recipes = struct {
    backing: Allocator,
    host_owner: *HostOwner,
    budget: *Budget,
    entries: []Entry,
    replay_inputs: []recomputed.ReplayInput,
    options: Options,
    fingerprint: [32]u8,
    largest_mask_bytes: usize,
    all_mask_bytes: usize,

    pub fn init(backing: Allocator, graph: *const ml.Graph, encoder_outputs: []const Id, descriptors: []const encoder.DropoutDescriptor, uses: []const peft.Use, options: Options) !Recipes {
        const limits = options.limits;
        if (graph.nodeCount() == 0 or graph.nodeCount() > limits.max_source_nodes or limits.max_source_nodes > 1_000_000 or
            encoder_outputs.len == 0 or encoder_outputs.len > 256 or limits.max_recipes == 0 or limits.max_recipes > 16384 or
            limits.max_candidates == 0 or limits.max_candidates > 65536 or try add(descriptors.len, uses.len) > limits.max_candidates or
            limits.max_name_bytes == 0 or limits.max_name_bytes > 1024 or limits.max_host_bytes == 0 or limits.max_host_bytes > 1024 * 1024 * 1024 or
            limits.max_mask_bytes == 0 or limits.max_mask_bytes > 1024 * 1024 * 1024 or limits.max_explicit_bytes == 0 or
            limits.max_explicit_bytes > 1024 * 1024 * 1024 or limits.max_work_items == 0 or limits.max_work_items > 1 << 30) return error.BoundaryReplayLimitExceeded;
        try check(options.control);
        const owner = try backing.create(HostOwner);
        errdefer backing.destroy(owner);
        owner.* = .{ .budget = .{ .backing = backing, .limit = limits.max_host_bytes, .failure_context = owner, .allocation_failed = HostOwner.failed } };
        const budget = &owner.budget;
        return initialize(backing, owner, graph, encoder_outputs, descriptors, uses, options) catch |err| {
            std.debug.assert(budget.live == 0);
            return owner.translate(err);
        };
    }

    fn initialize(backing: Allocator, owner: *HostOwner, graph: *const ml.Graph, outputs: []const Id, descriptors: []const encoder.DropoutDescriptor, uses: []const peft.Use, options: Options) !Recipes {
        const budget = &owner.budget;
        const a = budget.allocator();
        var work = Work{ .options = options };
        try work.charge(graph.nodeCount());
        const reachable = try closure(a, graph, outputs, &work);
        defer a.free(reachable);
        var entries = std.ArrayListUnmanaged(Entry).empty;
        errdefer {
            for (entries.items) |entry| a.free(entry.name);
            entries.deinit(a);
        }
        var seen_nodes = std.AutoHashMapUnmanaged(Id, void).empty;
        defer seen_nodes.deinit(a);
        var seen_names = std.StringHashMapUnmanaged(void).empty;
        defer seen_names.deinit(a);
        var largest: usize = 0;
        var all: usize = 0;
        for (0..try add(descriptors.len, uses.len)) |index| {
            try work.charge(1);
            const recipe: Recipe = if (index < descriptors.len) .{ .encoder = descriptors[index] } else .{ .peft = uses[index - descriptors.len] };
            const id = switch (recipe) {
                .encoder => |value| value.node,
                .peft => |value| value.mask,
            };
            if (id == nil and recipe == .peft) continue;
            if (id >= reachable.len) return error.InvalidBoundaryReplayRecipe;
            if (reachable[id] == 0) continue;
            if (entries.items.len >= options.limits.max_recipes) return error.BoundaryReplayLimitExceeded;
            const node = graph.node(id);
            if (node.op != .parameter) return error.InvalidBoundaryReplayRecipe;
            const attrs = node.op.parameter;
            if (attrs.name_offset > graph.string_table.items.len or attrs.name_len > graph.string_table.items.len - attrs.name_offset) return error.InvalidBoundaryReplayGraph;
            const name = graph.parameterName(node);
            var entry = Entry{ .node = id, .key = undefined, .shape = node.output_shape, .name = name, .recipe = recipe };
            entry.key = try recipeKey(entry, options.limits);
            try work.charge(name.len);
            if ((try seen_nodes.getOrPut(a, id)).found_existing or (try seen_names.getOrPut(a, name)).found_existing) return error.DuplicateBoundaryReplayRecipe;
            const bytes = try maskBytes(entry.shape, options.limits);
            largest = @max(largest, bytes);
            all = try add(all, bytes);
            entry.name = try a.dupe(u8, name);
            errdefer a.free(entry.name);
            if (entry.recipe == .peft) entry.recipe.peft.mask_name = entry.name;
            try entries.append(a, entry);
        }
        // No recognized dropout leaf may silently become an eagerly retained
        // fixed input because its descriptor was omitted or remapped wrongly.
        for (reachable, 0..) |mark, index| {
            if (mark == 0) continue;
            const node = graph.node(@intCast(index));
            if (node.op != .parameter) continue;
            const attrs = node.op.parameter;
            if (attrs.name_offset > graph.string_table.items.len or attrs.name_len > graph.string_table.items.len - attrs.name_offset) return error.InvalidBoundaryReplayGraph;
            const name = graph.parameterName(node);
            if ((std.mem.startsWith(u8, name, "__gliner25.encoder.dropout.") or std.mem.startsWith(u8, name, "__boundary_peft_mask.")) and
                !seen_nodes.contains(@intCast(index))) return error.MissingBoundaryReplayRecipe;
        }
        const inputs = try a.alloc(recomputed.ReplayInput, entries.items.len);
        errdefer a.free(inputs);
        for (entries.items, inputs) |entry, *input| input.* = .{ .node = entry.node, .key = entry.key };
        const owned = try entries.toOwnedSlice(a);
        var result = Recipes{ .backing = backing, .host_owner = owner, .budget = budget, .entries = owned, .replay_inputs = inputs, .options = options, .fingerprint = undefined, .largest_mask_bytes = largest, .all_mask_bytes = all };
        errdefer {
            for (owned) |entry| a.free(entry.name);
            a.free(owned);
        }
        result.fingerprint = try result.currentFingerprint(options.control);
        // Plans are cached beyond this request. Construction control may own
        // stack-local callback state and must never survive successful setup.
        result.options.control = null;
        return result;
    }

    pub fn deinit(self: *Recipes) void {
        const a = self.budget.allocator();
        for (self.entries) |entry| a.free(entry.name);
        a.free(self.entries);
        a.free(self.replay_inputs);
        std.debug.assert(self.budget.live == 0);
        self.backing.destroy(self.host_owner);
        self.* = undefined;
    }
    /// Includes the fixed allocator/terminal-failure owner.
    pub fn metadataBytes(self: *const Recipes) usize {
        return self.budget.live + @sizeOf(HostOwner);
    }
    fn currentFingerprint(self: *const Recipes, control: ?Control) ![32]u8 {
        try check(control);
        if (self.entries.len > self.options.limits.max_recipes or self.entries.len != self.replay_inputs.len) return error.InvalidBoundaryReplayRecipe;
        var hash = Sha.init(.{});
        hash.update(abi ++ ".recipes");
        inline for (std.meta.fields(Limits)) |field| integer(&hash, @field(self.options.limits, field.name));
        integer(&hash, self.entries.len);
        var work = Work{ .options = .{ .limits = self.options.limits, .control = control } };
        var largest: usize = 0;
        var all: usize = 0;
        for (self.entries, self.replay_inputs) |entry, input| {
            try work.charge(try add(entry.name.len, 1));
            const key = try recipeKey(entry, self.options.limits);
            if (input.node != entry.node or !sameHash(input.key, key) or !sameHash(entry.key, key)) return error.InvalidBoundaryReplayRecipe;
            const bytes = try maskBytes(entry.shape, self.options.limits);
            largest = @max(largest, bytes);
            all = try add(all, bytes);
            integer(&hash, entry.node);
            hash.update(&key);
            if (entry.recipe == .peft) {
                const use = entry.recipe.peft;
                integer(&hash, use.adapter);
                integer(&hash, use.source);
                integer(&hash, use.output);
                integer(&hash, use.input);
            }
        }
        if (largest != self.largest_mask_bytes or all != self.all_mask_bytes) return error.InvalidBoundaryReplayRecipe;
        integer(&hash, largest);
        integer(&hash, all);
        return hash.finalResult();
    }
    pub fn validate(self: *const Recipes) !void {
        try self.validateWithControl(null);
    }
    fn validateWithControl(self: *const Recipes, control: ?Control) !void {
        if (!sameHash(try self.currentFingerprint(control), self.fingerprint)) return error.BoundaryReplayInputsMutated;
    }
};

const Supplied = struct { source_index: usize, values: []const f32 };
pub const Context = struct {
    allocator: Allocator,
    recipes: *const Recipes,
    replay: encoder.Replay,
    identity: seeded.StepIdentity,
    supplied: ?[]const Mask,
    ordered: []Supplied,
    explicit_bytes: usize,
    io: *transfer.IO,
    fingerprint: [32]u8,

    /// Supplied masks are the complete regional subset, already validated and
    /// charged as part of the caller's complete step override. Only this small
    /// index owner is allocated here; the F32 arrays remain borrowed immutable.
    pub fn init(a: Allocator, recipes: *const Recipes, replay: encoder.Replay, identity: seeded.StepIdentity, supplied: ?[]const Mask, io: *transfer.IO) !Context {
        try recipes.validateWithControl(io.control);
        if (identity.microbatch != replay.micro_batch) return error.TrainingTapeIdentityMismatch;
        const count = if (supplied != null) recipes.entries.len else 0;
        if (supplied) |values| if (values.len != count) return error.InvalidBoundaryReplayMaskSet;
        const ordered = try a.alloc(Supplied, count);
        errdefer a.free(ordered);
        var bytes: usize = 0;
        if (supplied) |values| {
            var names = std.StringHashMapUnmanaged(usize).empty;
            defer names.deinit(a);
            for (values, 0..) |value, index| {
                if (value.name.len == 0 or value.name.len > recipes.options.limits.max_name_bytes) return error.InvalidBoundaryReplayMaskSet;
                const slot = try names.getOrPut(a, value.name);
                if (slot.found_existing) return error.InvalidBoundaryReplayMaskSet;
                slot.value_ptr.* = index;
            }
            for (recipes.entries, ordered) |entry, *target| {
                const index = names.get(entry.name) orelse return error.InvalidBoundaryReplayMaskSet;
                target.* = .{ .source_index = index, .values = values[index].values };
                const size = try maskBytes(entry.shape, recipes.options.limits);
                if (values[index].values.len != size / 4) return error.InvalidBoundaryReplayMaskSet;
                bytes = try add(bytes, size);
                if (bytes > recipes.options.limits.max_explicit_bytes) return error.BoundaryReplayLimitExceeded;
            }
        }
        var result = Context{ .allocator = a, .recipes = recipes, .replay = replay, .identity = identity, .supplied = supplied, .ordered = ordered, .explicit_bytes = bytes, .io = io, .fingerprint = undefined };
        result.fingerprint = try result.currentFingerprint();
        return result;
    }
    pub fn deinit(self: *Context) void {
        self.allocator.free(self.ordered);
        self.* = undefined;
    }
    fn currentFingerprint(self: *const Context) ![32]u8 {
        try self.recipes.validateWithControl(self.io.control);
        try check(self.io.control);
        if (self.identity.microbatch != self.replay.micro_batch) return error.TrainingTapeIdentityMismatch;
        var hash = Sha.init(.{});
        hash.update(abi ++ ".context");
        hash.update(&self.recipes.fingerprint);
        hash.update(&self.identity.binding);
        integer(&hash, self.identity.optimizer_step);
        integer(&hash, self.identity.microbatch);
        integer(&hash, self.replay.seed);
        integer(&hash, self.replay.micro_batch);
        integer(&hash, self.replay.replica);
        integer(&hash, @intFromBool(self.supplied != null));
        var work = Work{ .options = .{ .limits = self.recipes.options.limits, .control = self.io.control } };
        var bytes: usize = 0;
        if (self.supplied) |masks| {
            if (masks.len != self.recipes.entries.len or masks.len != self.ordered.len) return error.InvalidBoundaryReplayMaskSet;
            for (self.recipes.entries, self.ordered) |entry, saved| {
                if (saved.source_index >= masks.len) return error.BoundaryReplayInputsMutated;
                const value = masks[saved.source_index];
                const size = try maskBytes(entry.shape, self.recipes.options.limits);
                if (!std.mem.eql(u8, value.name, entry.name) or value.values.len != size / 4 or
                    saved.values.len != value.values.len or saved.values.ptr != value.values.ptr) return error.BoundaryReplayInputsMutated;
                bytes = try add(bytes, size);
                if (bytes > self.recipes.options.limits.max_explicit_bytes) return error.BoundaryReplayLimitExceeded;
                const scale: f32 = 1 / (1 - probability(entry));
                for (value.values, 0..) |element, index| {
                    if (index % 1024 == 0) {
                        try work.charge(@min(@as(usize, 1024), value.values.len - index));
                        try check(self.io.control);
                    }
                    // Preserve the supplied bit pattern (including signed
                    // zero) in the seal; only source-admitted values survive.
                    if (element != 0 and element != scale) return error.InvalidBoundaryReplayMask;
                    float(&hash, element);
                }
            }
        } else if (self.ordered.len != 0) return error.BoundaryReplayInputsMutated;
        if (bytes != self.explicit_bytes) return error.BoundaryReplayInputsMutated;
        return hash.finalResult();
    }
    pub fn validate(self: *const Context) !void {
        if (!sameHash(try self.currentFingerprint(), self.fingerprint)) return error.BoundaryReplayInputsMutated;
    }
    /// The Context address and all borrowed owners must remain stable until
    /// the last Tape using this source has been destroyed.
    pub fn source(self: *const Context) !recomputed.ReplaySource {
        try self.validate();
        return .{ .fingerprint = self.fingerprint, .context = self, .validate = validateSource, .materialize = materialize };
    }
    fn validateSource(raw: ?*const anyopaque, expected: [32]u8) !void {
        const self: *const Context = @ptrCast(@alignCast(raw orelse return error.InvalidBoundaryReplayContext));
        if (!sameHash(expected, self.fingerprint)) return error.BoundaryReplayInputsMutated;
        try self.validate();
    }
    fn materialize(raw: ?*const anyopaque, a: Allocator, cb: *const ops.ComputeBackend, input: recomputed.ReplayInput, shape: Shape, identity: seeded.StepIdentity, control: ?Control) !ops.CT {
        const self: *const Context = @ptrCast(@alignCast(raw orelse return error.InvalidBoundaryReplayContext));
        try check(control);
        try cb.checkExecutionControl();
        try self.validate();
        if (!std.meta.eql(identity, self.identity)) return error.TrainingTapeIdentityMismatch;
        if (cb.ptr != self.io.cb.ptr or cb.vtable != self.io.cb.vtable) return error.InvalidBoundaryReplayBackend;
        for (self.recipes.entries, 0..) |entry, index| {
            if (entry.node != input.node) continue;
            if (!sameHash(input.key, entry.key) or !sameShape(shape, entry.shape)) return error.InvalidBoundaryReplayInput;
            const size = try maskBytes(shape, self.recipes.options.limits);
            if (self.supplied != null) {
                const output = try self.io.upload(shape, .{ .f32 = self.ordered[index].values });
                errdefer cb.free(output);
                try check(control);
                try cb.checkExecutionControl();
                try self.validate();
                return output;
            }
            const values = try a.alloc(f32, size / 4);
            defer a.free(values);
            // Existing site generators consume the complete logical shape;
            // restarting them for tiles would alter the counter stream.
            switch (entry.recipe) {
                .encoder => |descriptor| try encoder.fillDropout(descriptor, self.replay, values),
                .peft => |use| try peft.fillDropout(use, .{ .seed = self.replay.seed, .optimizer_step = identity.optimizer_step, .micro_batch = self.replay.micro_batch, .replica = self.replay.replica }, values),
            }
            try check(control);
            try cb.checkExecutionControl();
            const output = try self.io.upload(shape, .{ .f32 = values });
            errdefer cb.free(output);
            try check(control);
            try cb.checkExecutionControl();
            try self.validate();
            return output;
        }
        return error.InvalidBoundaryReplayInput;
    }
};

const Tiny = struct {
    graph: ml.Graph,
    output: Id,
    descriptor: encoder.DropoutDescriptor,
    uses: [2]peft.Use,

    fn init(a: Allocator) !Tiny {
        var graph = ml.Graph.init(a);
        errdefer graph.deinit();
        var b = ml.Builder.init(&graph);
        const shape = Shape.init(.f32, &.{ 4, 8 });
        const x = try b.parameter("input", shape);
        const enc = try b.parameter("__gliner25.encoder.dropout.embeddings.0", shape);
        const adapter = try b.parameter("__boundary_peft_mask.encoder.layer.0.attention.self.query_proj.0", shape);
        const head = try b.parameter("__boundary_peft_mask.classifier.0.0", shape);
        const output = try b.mul(x, enc);
        const alternate = try b.mul(x, adapter);
        graph.nodes.items[output].vjp_alternate = alternate;
        const use = peft.Use{ .adapter = 0, .source = output, .input = x, .output = alternate, .rows = 4, .occurrence = 0, .mask = adapter, .mask_name = "__boundary_peft_mask.encoder.layer.0.attention.self.query_proj.0", .mask_shape = shape, .probability = 0.125, .stream = 0xfedcba9876543210 };
        var outside = use;
        outside.mask = head;
        outside.mask_name = "__boundary_peft_mask.classifier.0.0";
        outside.stream ^= 1;
        return .{ .graph = graph, .output = output, .descriptor = .{ .node = enc, .site = .{ .kind = .embeddings }, .shape = shape, .probability = 0.125 }, .uses = .{ use, outside } };
    }
};
const tiny_replay = encoder.Replay{ .seed = 0xfedcba9876543210, .micro_batch = 0x100000002, .replica = 0x8000000000000003 };
const tiny_identity = seeded.StepIdentity{ .binding = @splat(7), .optimizer_step = 0x300000004, .microbatch = tiny_replay.micro_batch };

test "boundary regional replay recipes use encoder ordinary and alternate closure with copied identities" {
    const a = std.testing.allocator;
    var tiny = try Tiny.init(a);
    defer tiny.graph.deinit();
    var recipes = try Recipes.init(a, &tiny.graph, &.{tiny.output}, &.{tiny.descriptor}, &tiny.uses, .{});
    defer recipes.deinit();
    try std.testing.expectEqual(@as(usize, 2), recipes.entries.len);
    try std.testing.expectEqual(tiny.descriptor.node, recipes.entries[0].node);
    try std.testing.expectEqual(tiny.uses[0].mask, recipes.entries[1].node);
    try std.testing.expectEqual(@as(usize, 128), recipes.largest_mask_bytes);
    try std.testing.expectEqual(@as(usize, 256), recipes.all_mask_bytes);
    tiny.descriptor.probability = 0.5;
    tiny.uses[0].stream ^= 1;
    try recipes.validate();
    const original = recipes.entries[1].recipe.peft.stream;
    recipes.entries[1].recipe.peft.stream ^= 1;
    try std.testing.expectError(error.InvalidBoundaryReplayRecipe, recipes.validate());
    recipes.entries[1].recipe.peft.stream = original;
    try recipes.validate();
    recipes.replay_inputs[1].key[0] ^= 1;
    try std.testing.expectError(error.InvalidBoundaryReplayRecipe, recipes.validate());
    recipes.replay_inputs[1].key[0] ^= 1;
}

test "boundary regional replay refuses missing duplicate cyclic and over-budget recipes" {
    const a = std.testing.allocator;
    var tiny = try Tiny.init(a);
    defer tiny.graph.deinit();
    try std.testing.expectError(error.MissingBoundaryReplayRecipe, Recipes.init(a, &tiny.graph, &.{tiny.output}, &.{tiny.descriptor}, &.{}, .{}));
    try std.testing.expectError(error.DuplicateBoundaryReplayRecipe, Recipes.init(a, &tiny.graph, &.{tiny.output}, &.{ tiny.descriptor, tiny.descriptor }, &tiny.uses, .{}));
    try std.testing.expectError(error.BoundaryReplayLimitExceeded, Recipes.init(a, &tiny.graph, &.{tiny.output}, &.{tiny.descriptor}, &tiny.uses, .{ .limits = .{ .max_mask_bytes = 127 } }));
    try std.testing.expectError(error.BoundaryReplayLimitExceeded, Recipes.init(a, &tiny.graph, &.{tiny.output}, &.{tiny.descriptor}, &tiny.uses, .{ .limits = .{ .max_work_items = 1 } }));
    try std.testing.expectError(error.BoundaryReplayLimitExceeded, Recipes.init(a, &tiny.graph, &.{tiny.output}, &.{tiny.descriptor}, &tiny.uses, .{ .limits = .{ .max_host_bytes = 1 } }));
    tiny.graph.nodes.items[tiny.output].vjp_alternate = tiny.output;
    try std.testing.expectError(error.InvalidBoundaryReplayGraph, Recipes.init(a, &tiny.graph, &.{tiny.output}, &.{tiny.descriptor}, &tiny.uses, .{}));
}

fn exerciseGenerated(a: Allocator) !void {
    const native = @import("../ops/native_compute.zig");
    var tiny = try Tiny.init(a);
    defer tiny.graph.deinit();
    var recipes = try Recipes.init(a, &tiny.graph, &.{tiny.output}, &.{tiny.descriptor}, &tiny.uses, .{});
    defer recipes.deinit();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var io = transfer.IO{ .allocator = a, .cb = &cb, .execution = .native, .primitive = .{}, .admission = .{}, .control = null };
    var context = try Context.init(a, &recipes, tiny_replay, tiny_identity, null, &io);
    defer context.deinit();
    const source = try context.source();
    var temporary = Budget{ .backing = a, .limit = 128 };
    for (recipes.entries, recipes.replay_inputs) |entry, input| {
        var expected: [32]f32 = undefined;
        switch (entry.recipe) {
            .encoder => |value| try encoder.fillDropout(value, tiny_replay, &expected),
            .peft => |value| try peft.fillDropout(value, .{ .seed = tiny_replay.seed, .micro_batch = tiny_replay.micro_batch, .optimizer_step = tiny_identity.optimizer_step, .replica = tiny_replay.replica }, &expected),
        }
        for (0..2) |_| {
            const tensor = try source.materialize.?(source.context, temporary.allocator(), &cb, input, entry.shape, tiny_identity, null);
            defer cb.free(tensor);
            try std.testing.expectEqual(@as(usize, 0), temporary.live);
            const values = try cb.toFloat32(tensor, a);
            defer a.free(values);
            try std.testing.expectEqualSlices(f32, &expected, values);
        }
    }
    var wrong = tiny_identity;
    wrong.optimizer_step += 1;
    try std.testing.expectError(error.TrainingTapeIdentityMismatch, source.materialize.?(source.context, a, &cb, recipes.replay_inputs[0], recipes.entries[0].shape, wrong, null));
    var wrong_input = recipes.replay_inputs[0];
    wrong_input.key[0] ^= 1;
    try std.testing.expectError(error.InvalidBoundaryReplayInput, source.materialize.?(source.context, a, &cb, wrong_input, recipes.entries[0].shape, tiny_identity, null));
    context.replay.seed ^= 1;
    try std.testing.expectError(error.BoundaryReplayInputsMutated, source.validate.?(source.context, source.fingerprint));
    context.replay.seed ^= 1;
    try source.validate.?(source.context, source.fingerprint);
}

test "boundary regional replay generated masks preserve all counter bits and free host staging before return" {
    try exerciseGenerated(std.testing.allocator);
}

test "boundary regional replay generated setup and materialization unwind every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseGenerated, .{});
}

fn exerciseSupplied(a: Allocator) !void {
    const native = @import("../ops/native_compute.zig");
    var tiny = try Tiny.init(a);
    defer tiny.graph.deinit();
    var recipes = try Recipes.init(a, &tiny.graph, &.{tiny.output}, &.{tiny.descriptor}, &tiny.uses, .{});
    defer recipes.deinit();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var io = transfer.IO{ .allocator = a, .cb = &cb, .execution = .native, .primitive = .{}, .admission = .{}, .control = null };
    var left: [32]f32 = @splat(1 / @as(f32, 0.875));
    left[1] = 0;
    var right: [32]f32 = @splat(0);
    right[1] = left[0];
    var masks = [_]Mask{ .{ .name = recipes.entries[1].name, .values = &right }, .{ .name = recipes.entries[0].name, .values = &left } };
    var context = try Context.init(a, &recipes, tiny_replay, tiny_identity, &masks, &io);
    defer context.deinit();
    const ordered_masks = [_]Mask{ masks[1], masks[0] };
    var ordered = try Context.init(a, &recipes, tiny_replay, tiny_identity, &ordered_masks, &io);
    defer ordered.deinit();
    try std.testing.expectEqualSlices(u8, &context.fingerprint, &ordered.fingerprint);
    try std.testing.expectEqual(@as(usize, 256), context.explicit_bytes);
    const source = try context.source();
    const tensor = try source.materialize.?(source.context, a, &cb, recipes.replay_inputs[0], recipes.entries[0].shape, tiny_identity, null);
    defer cb.free(tensor);
    const actual = try cb.toFloat32(tensor, a);
    defer a.free(actual);
    try std.testing.expectEqualSlices(f32, &left, actual);
    left[1] = left[0];
    try std.testing.expectError(error.BoundaryReplayInputsMutated, source.validate.?(source.context, source.fingerprint));
    left[1] = 0;
    left[2] = 0.7;
    try std.testing.expectError(error.InvalidBoundaryReplayMask, source.validate.?(source.context, source.fingerprint));
    left[2] = left[0];
    const Cancel = struct {
        fn apply(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    io.control = .{ .check_fn = Cancel.apply };
    try std.testing.expectError(error.Cancelled, source.materialize.?(source.context, a, &cb, recipes.replay_inputs[0], recipes.entries[0].shape, tiny_identity, null));
    io.control = null;
    try source.validate.?(source.context, source.fingerprint);
    masks[0].name = "foreign mask";
    try std.testing.expectError(error.BoundaryReplayInputsMutated, source.validate.?(source.context, source.fingerprint));
    masks[0].name = recipes.entries[1].name;
    try invalidMaskSet(a, &recipes, &.{ masks[0], masks[0] }, &io);
    try std.testing.expectError(error.InvalidBoundaryReplayMaskSet, Context.init(a, &recipes, tiny_replay, tiny_identity, masks[0..1], &io));
}

fn invalidMaskSet(a: Allocator, recipes: *const Recipes, masks: []const Mask, io: *transfer.IO) !void {
    var unexpected = Context.init(a, recipes, tiny_replay, tiny_identity, masks, io) catch |err| {
        if (err == error.InvalidBoundaryReplayMaskSet) return;
        return err; // Preserve injected allocation failures for the OOM sweep.
    };
    unexpected.deinit();
    return error.ExpectedBoundaryReplayMaskSetError;
}

test "boundary regional replay explicit mask pins reject mutation cancellation missing and duplicate names" {
    try exerciseSupplied(std.testing.allocator);
}

test "boundary regional replay explicit mask metadata unwinds every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exerciseSupplied, .{});
}

test "boundary regional replay cached recipes forget construction control and honor current request cancellation" {
    const Gate = struct {
        calls: usize = 0,
        failure: ?anyerror = null,
        fn apply(raw: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.failure) |failure| return failure;
        }
        fn control(self: *@This()) Control {
            return .{ .ptr = self, .check_fn = apply };
        }
    };
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var tiny = try Tiny.init(a);
    defer tiny.graph.deinit();
    var construction = Gate{};
    var recipes = try Recipes.init(a, &tiny.graph, &.{tiny.output}, &.{tiny.descriptor}, &tiny.uses, .{ .control = construction.control() });
    defer recipes.deinit();
    try std.testing.expect(construction.calls > 0);
    try std.testing.expect(recipes.options.control == null);
    const construction_calls = construction.calls;
    construction.failure = error.ExpiredConstructionControl;
    try recipes.validate();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var request = Gate{};
    var io = transfer.IO{ .allocator = a, .cb = &cb, .execution = .native, .primitive = .{}, .admission = .{}, .control = request.control() };
    var context = try Context.init(a, &recipes, tiny_replay, tiny_identity, null, &io);
    defer context.deinit();
    const source = try context.source();
    const tensor = try source.materialize.?(source.context, a, &cb, recipes.replay_inputs[0], recipes.entries[0].shape, tiny_identity, null);
    cb.free(tensor);
    request.failure = error.Cancelled;
    try std.testing.expectError(error.Cancelled, source.validate.?(source.context, source.fingerprint));
    try std.testing.expectError(error.Cancelled, source.materialize.?(source.context, a, &cb, recipes.replay_inputs[0], recipes.entries[0].shape, tiny_identity, null));
    try std.testing.expectError(error.Cancelled, Context.init(a, &recipes, tiny_replay, tiny_identity, null, &io));
    request.failure = null;
    try source.validate.?(source.context, source.fingerprint);
    try std.testing.expectEqual(construction_calls, construction.calls);
}

test "boundary regional replay terminal backing OOM is independent of earlier resize limit miss" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var owner = HostOwner{ .budget = .{ .backing = failing.allocator(), .limit = 32 } };
    owner.budget.failure_context = &owner;
    owner.budget.allocation_failed = HostOwner.failed;
    const a = owner.budget.allocator();
    const first = try a.alloc(u8, 8);
    defer a.free(first);
    try std.testing.expect(!a.resize(first, 33));
    try std.testing.expect(owner.budget.denied);
    try std.testing.expect(owner.failure == null);
    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 1));
    try std.testing.expectEqual(error.OutOfMemory, owner.translate(error.OutOfMemory));
    try std.testing.expectEqual(.backing_allocator, owner.failure.?.kind);
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 33));
    try std.testing.expectEqual(error.BoundaryReplayLimitExceeded, owner.translate(error.OutOfMemory));
    try std.testing.expectEqual(.declared_limit, owner.failure.?.kind);
}
