// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded, deterministic joint entity/relation selection. Candidate utilities
//! are calibrated by the caller; presentation probabilities never substitute
//! for the optimization objective. All coordinates are document-global,
//! half-open offsets in one caller-chosen unit.
//!
//! The explicit native profile treats a derived symmetric reverse as the same
//! undirected fact and does not charge an absent slot. The separately versioned
//! fastino_v1 profile retains the pinned source optimizer and companion checks,
//! including its Python semantic identities and tie rules.
const std = @import("std");
const schema_mod = @import("extraction_schema.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const NodeSet = u256;
const InputSet = u256;
const GraphSet = u1024;
const max_nodes = 256;
const max_edges = 256;
const max_graph_edges = 1024;
const source_keys = @import("extraction_joint_source_keys.zig");
pub const SourceSpan = source_keys.Span;
pub const SourceIdentity = source_keys.Identity;
pub const Profile = enum { native, fastino_v1 };

pub const Node = struct {
    entity_type: usize,
    start: usize,
    end: usize,
    utility: f64,
    probability: f64,
    required: bool = false,
};
pub const Edge = struct {
    relation_type: usize,
    head: usize,
    tail: usize,
    utility: f64,
    probability: f64,
    slot: ?u64 = null,
    hypothesis: ?u64 = null,
    count_alternative: ?u64 = null,
    required: bool = false,
};
pub const DecodedEdge = struct {
    relation_type: usize,
    /// Indices into Result.nodes (or the nodes supplied to validateGlobal).
    head: usize,
    tail: usize,
    utility: f64,
    probability: f64,
    source_index: ?usize,
    derived_from: ?usize = null,
    derived: bool,
    slot: ?u64 = null,
    hypothesis: ?u64 = null,
    count_alternative: ?u64 = null,
};
pub const Algorithm = enum { exact, beam, auto };
pub const Status = enum { optimal, feasible, infeasible, search_exhausted };
pub const Options = struct {
    algorithm: Algorithm = .auto,
    /// Explicit native algorithms retain their existing contract. The pinned
    /// source profile requires .beam and never substitutes an exact witness.
    profile: Profile = .native,
    max_nodes: usize = 128,
    max_edges: usize = 256,
    max_graph_edges: usize = 512,
    exact_node_budget: usize = 200000,
    beam_node_budget: usize = 200000,
    beam_width: usize = 32,
    max_validation_steps: usize = 10000000,
    max_source_key_bytes: usize = 1024 * 1024,
    control: ?Control = null,
};
pub const Result = struct {
    allocator: Allocator,
    status: Status,
    nodes: []Node,
    node_source_indices: []usize,
    edges: []DecodedEdge,
    utility: f64,
    visited_nodes: usize,
    /// Final search method stopped at its node budget. A completed bounded
    /// beam may be feasible/approximate while this remains false.
    exhausted: bool = false,
    pub fn deinit(self: *Result) void {
        self.allocator.free(self.nodes);
        self.allocator.free(self.node_source_indices);
        self.allocator.free(self.edges);
        self.* = undefined;
    }
    pub fn valid(self: Result) bool {
        return self.status == .optimal or self.status == .feasible;
    }
};
const GraphEdge = struct { relation_type: usize, head: usize, tail: usize };
const Bundle = struct { graph: GraphSet, primary: usize };
const State = struct { nodes: NodeSet = 0, inputs: InputSet = 0, graph: GraphSet = 0, score: f64 = 0 };
const BeamOutcome = enum { complete, pruned, exhausted };
const Work = struct {
    options: Options,
    steps: usize = 0,
    fn tick(self: *Work) !void {
        if (self.steps >= self.options.max_validation_steps) return error.JointValidationLimitExceeded;
        self.steps += 1;
        if (self.steps % 128 == 1) if (self.options.control) |control| try control.check();
    }
};
const Problem = struct {
    schema: schema_mod.JointSchema,
    nodes: []const Node,
    edges: []const Edge,
    graph: []const GraphEdge,
    bundles: []const Bundle,
};

/// No candidate cap truncation occurs here: an oversized candidate set fails
/// before search. Exact search may traverse temporarily negative atomic gains,
/// allowing two edges to share the cost of a rescued low-confidence endpoint.
pub fn decode(allocator: Allocator, schema: schema_mod.JointSchema, nodes: []const Node, edges: []const Edge, options: Options) !Result {
    return decodeWithSourceIdentity(allocator, schema, nodes, edges, null, options);
}

pub fn decodeWithSourceIdentity(allocator: Allocator, schema: schema_mod.JointSchema, nodes: []const Node, edges: []const Edge, identity: ?SourceIdentity, options: Options) !Result {
    try validateOptions(options);
    if (options.control) |control| try control.check();
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    var work = Work{ .options = options };
    const problem = try buildProblem(arena.allocator(), schema, nodes, edges, &work);
    if (options.profile == .fastino_v1) {
        if (options.algorithm != .beam) return error.InvalidJointSourceOptions;
        const source_identity = identity orelse return error.MissingJointSourceIdentity;
        for (nodes) |node| if (node.required) return error.UnsupportedJointSourceCandidate;
        for (edges) |edge| if (edge.required) return error.UnsupportedJointSourceCandidate;
        const keys = try source_keys.prepare(arena.allocator(), schema, nodes, edges, problem.graph, source_identity, options.max_source_key_bytes);
        var source = SourceSearch{ .problem = problem, .work = &work, .identity = source_identity, .keys = keys };
        return source.run(allocator, arena.allocator());
    }
    var search = Search{ .problem = problem, .work = &work };
    search.initializeOrder();
    var initial = State{};
    for (nodes, 0..) |node, index| if (node.required) {
        initial.nodes |= nodeBit(index);
        initial.score = try finite(initial.score + node.utility);
    };
    if (!try validNodes(problem, initial.nodes, &work)) return emptyResult(allocator, .infeasible, 0);
    for (edges, 0..) |edge, index| if (edge.required) {
        initial = (try search.includeEdge(initial, index)) orelse return emptyResult(allocator, .infeasible, 0);
    };
    // The complete initial closure is already a valid assignment. Keep it as
    // a witness even if the search budget is zero.
    search.best = initial;
    var exhausted = false;
    var approximate = false;
    if (options.algorithm != .beam) search.exact(0, initial) catch |err| switch (err) {
        error.JointSearchExhausted => {
            exhausted = true;
            approximate = true;
        },
        else => return err,
    };
    if (options.algorithm == .beam or (options.algorithm == .auto and exhausted)) {
        const outcome = try search.beam(arena.allocator(), initial);
        approximate = outcome != .complete;
        exhausted = outcome == .exhausted;
    }
    const status: Status = if (search.best != null) if (approximate) .feasible else .optimal else if (exhausted) .search_exhausted else .infeasible;
    return if (search.best) |best| try materialize(allocator, problem, best, status, exhausted, search.visits, &work) else emptyResult(allocator, status, search.visits);
}

/// Revalidate every hard constraint after remapping, deduplication or window
/// merging. This uses the same checks as search, including derived companions.
/// Invalid indices/NaNs are errors; a well-formed violating graph returns false.
pub fn validateGlobal(allocator: Allocator, schema: schema_mod.JointSchema, nodes: []const Node, edges: []const DecodedEdge, options: Options) !bool {
    try validateOptions(options);
    if (nodes.len > options.max_nodes or edges.len > options.max_graph_edges) return error.JointCandidateLimitExceeded;
    if (options.control) |control| try control.check();
    var work = Work{ .options = options };
    try validateNodes(schema, nodes, &work);
    const graph = try allocator.alloc(GraphEdge, edges.len);
    defer allocator.free(graph);
    const explicit = try allocator.alloc(Edge, edges.len);
    defer allocator.free(explicit);
    var explicit_len: usize = 0;
    for (edges, graph, 0..) |edge, *entry, i| {
        if (edge.head >= nodes.len or edge.tail >= nodes.len or edge.relation_type >= schema.relations.len or
            !validProbability(edge.probability) or !std.math.isFinite(edge.utility) or
            (edge.count_alternative != null and edge.hypothesis == null)) return error.InvalidJointCandidate;
        entry.* = .{ .relation_type = edge.relation_type, .head = edge.head, .tail = edge.tail };
        for (graph[0..i]) |previous| {
            try work.tick();
            if (sameEdge(previous, entry.*)) return false;
        }
        if (!edge.derived) {
            if (explicit_len == max_edges or explicit_len >= options.max_edges) return error.JointCandidateLimitExceeded;
            explicit[explicit_len] = .{ .relation_type = edge.relation_type, .head = edge.head, .tail = edge.tail, .utility = edge.utility, .probability = edge.probability, .slot = edge.slot, .hypothesis = edge.hypothesis, .count_alternative = edge.count_alternative };
            explicit_len += 1;
        } else if (edge.slot != null or edge.hypothesis != null or edge.count_alternative != null or edge.utility != 0) return error.InvalidDerivedJointEdge;
    }
    const problem = Problem{ .schema = schema, .nodes = nodes, .edges = explicit[0..explicit_len], .graph = graph, .bundles = &.{} };
    const state = State{ .nodes = lowMask(NodeSet, nodes.len), .inputs = lowMask(InputSet, explicit_len), .graph = lowMask(GraphSet, graph.len) };
    if (!try validNodes(problem, state.nodes, &work) or !try validGraph(problem, state, &work)) return false;
    // A derived edge must be justified by another selected edge, not an
    // arbitrary edge labeled derived to bypass explicit resource uniqueness.
    var justified: GraphSet = 0;
    for (edges, 0..) |edge, i| if (!edge.derived) {
        justified |= graphBit(i);
    };
    var pending = justified;
    while (pending != 0) {
        const source = graph[takeFirst(GraphSet, &pending)];
        for (graph, 0..) |candidate, i| {
            try work.tick();
            if (justified & graphBit(i) == 0 and isCompanion(schema, source, candidate)) {
                justified |= graphBit(i);
                if (options.profile == .native) pending |= graphBit(i);
            }
        }
    }
    if (justified != lowMask(GraphSet, graph.len)) return false;
    if (options.control) |control| try control.check();
    return true;
}

fn validateOptions(options: Options) !void {
    if (options.max_nodes == 0 or options.max_nodes > max_nodes or options.max_edges > max_edges or
        options.max_graph_edges == 0 or options.max_graph_edges > max_graph_edges or options.beam_width == 0 or
        options.beam_width > 1024 or options.max_validation_steps == 0) return error.InvalidJointDecodeOptions;
}
fn validateNodes(schema: schema_mod.JointSchema, nodes: []const Node, work: *Work) !void {
    for (nodes, 0..) |node, i| {
        try work.tick();
        if (node.entity_type >= schema.entities.len or node.end <= node.start or
            !std.math.isFinite(node.utility) or !validProbability(node.probability)) return error.InvalidJointCandidate;
        for (nodes[0..i]) |other| {
            try work.tick();
            if (node.entity_type == other.entity_type and node.start == other.start and node.end == other.end) return error.DuplicateJointNode;
        }
    }
}
fn buildProblem(allocator: Allocator, schema: schema_mod.JointSchema, nodes: []const Node, edges: []const Edge, work: *Work) !Problem {
    if (nodes.len > work.options.max_nodes or edges.len > work.options.max_edges) return error.JointCandidateLimitExceeded;
    try validateNodes(schema, nodes, work);
    var graph = std.ArrayListUnmanaged(GraphEdge).empty;
    const bundles = try allocator.alloc(Bundle, edges.len);
    for (edges, bundles) |edge, *bundle| {
        if (edge.head >= nodes.len or edge.tail >= nodes.len or edge.relation_type >= schema.relations.len or
            !std.math.isFinite(edge.utility) or !validProbability(edge.probability) or
            (edge.count_alternative != null and edge.hypothesis == null)) return error.InvalidJointCandidate;
        bundle.primary = try internEdge(allocator, &graph, .{ .relation_type = edge.relation_type, .head = edge.head, .tail = edge.tail }, work);
        bundle.graph = graphBit(bundle.primary);
        // Native saturates implications, including chains. The source profile
        // adds only companions of this original explicit edge.
        var pending = bundle.graph;
        while (pending != 0) {
            const index = takeFirst(GraphSet, &pending);
            const source = graph.items[index];
            for (schema.relations, 0..) |_, relation| {
                try work.tick();
                const companion = GraphEdge{ .relation_type = relation, .head = source.tail, .tail = source.head };
                if (!isCompanion(schema, source, companion)) continue;
                const next = try internEdge(allocator, &graph, companion, work);
                if (bundle.graph & graphBit(next) == 0) {
                    bundle.graph |= graphBit(next);
                    // Fastino adds companions of explicit evidence only.
                    if (work.options.profile == .native) pending |= graphBit(next);
                }
            }
        }
    }
    return .{ .schema = schema, .nodes = nodes, .edges = edges, .graph = try graph.toOwnedSlice(allocator), .bundles = bundles };
}
fn internEdge(allocator: Allocator, graph: *std.ArrayListUnmanaged(GraphEdge), edge: GraphEdge, work: *Work) !usize {
    for (graph.items, 0..) |existing, i| {
        try work.tick();
        if (sameEdge(existing, edge)) return i;
    }
    if (graph.items.len >= work.options.max_graph_edges) return error.JointCandidateLimitExceeded;
    try graph.append(allocator, edge);
    return graph.items.len - 1;
}
fn sameEdge(a: GraphEdge, b: GraphEdge) bool {
    return a.relation_type == b.relation_type and a.head == b.head and a.tail == b.tail;
}
fn isCompanion(schema: schema_mod.JointSchema, source: GraphEdge, target: GraphEdge) bool {
    if (source.head != target.tail or source.tail != target.head) return false;
    if (source.relation_type == target.relation_type and schema.relations[source.relation_type].symmetric) return true;
    if (schema.relations[source.relation_type].inverse == target.relation_type or schema.relations[target.relation_type].inverse == source.relation_type) return true;
    for (schema.constraints) |constraint| switch (constraint) {
        .symmetric => |relation| if (source.relation_type == relation and target.relation_type == relation) return true,
        .inverse => |pair| if ((source.relation_type == pair.relation and target.relation_type == pair.inverse) or
            (source.relation_type == pair.inverse and target.relation_type == pair.relation)) return true,
        else => {},
    };
    return false;
}

const Overlap = @FieldType(schema_mod.JointConstraint, "entity_overlap");
fn validNodes(problem: Problem, selected: NodeSet, work: *Work) !bool {
    var any_nested = false;
    var all_nested = true;
    for (problem.schema.entities) |entity| {
        any_nested = any_nested or (entity.allow_nested orelse false);
        all_nested = all_nested and (entity.allow_nested orelse false);
    }
    const implicit: Overlap = if (all_nested) .allow else if (any_nested) .nested else .disallow;
    var remaining = selected;
    while (remaining != 0) {
        const i = takeFirst(NodeSet, &remaining);
        var others = remaining;
        while (others != 0) {
            try work.tick();
            const j = takeFirst(NodeSet, &others);
            if (!overlapAllowed(problem.nodes[i], problem.nodes[j], implicit)) return false;
            for (problem.schema.constraints) |constraint| switch (constraint) {
                .entity_overlap => |policy| if (!overlapAllowed(problem.nodes[i], problem.nodes[j], policy)) return false,
                else => {},
            };
        }
    }
    return true;
}
fn overlapAllowed(a: Node, b: Node, policy: Overlap) bool {
    if (policy == .allow or a.end <= b.start or b.end <= a.start) return true;
    if (policy == .disallow) return false;
    return (a.start <= b.start and b.end <= a.end) or (b.start <= a.start and a.end <= b.end);
}
fn applies(relation: ?usize, actual: usize) bool {
    return relation == null or relation.? == actual;
}
fn typed(problem: Problem, edge: GraphEdge, head: []const usize, tail: []const usize) bool {
    return (head.len == 0 or std.mem.indexOfScalar(usize, head, problem.nodes[edge.head].entity_type) != null) and
        (tail.len == 0 or std.mem.indexOfScalar(usize, tail, problem.nodes[edge.tail].entity_type) != null);
}
fn countEdges(problem: Problem, state: State, relation: ?usize, endpoint: usize, head: bool, work: *Work) !usize {
    var remaining = state.graph;
    var count: usize = 0;
    while (remaining != 0) {
        try work.tick();
        const edge = problem.graph[takeFirst(GraphSet, &remaining)];
        if (applies(relation, edge.relation_type) and (if (head) edge.head else edge.tail) == endpoint) count += 1;
    }
    return count;
}
fn hasGraph(problem: Problem, selected: GraphSet, target: GraphEdge, work: *Work) !bool {
    var remaining = selected;
    while (remaining != 0) {
        try work.tick();
        if (sameEdge(problem.graph[takeFirst(GraphSet, &remaining)], target)) return true;
    }
    return false;
}

fn validGraph(problem: Problem, state: State, work: *Work) !bool {
    return validGraphMode(problem, state, work, true);
}

fn evidenceIndex(problem: Problem, state: State, graph_index: usize) ?usize {
    var inputs = state.inputs;
    while (inputs != 0) {
        const index = takeFirst(InputSet, &inputs);
        const edge = problem.edges[index];
        if (sameEdge(problem.graph[graph_index], .{ .relation_type = edge.relation_type, .head = edge.head, .tail = edge.tail })) return index;
    }
    return null;
}

fn sourceSlot(problem: Problem, state: State, graph_index: usize) ?u64 {
    return if (evidenceIndex(problem, state, graph_index)) |index| problem.edges[index].slot else null;
}

fn validGraphMode(problem: Problem, state: State, work: *Work, companions: bool) !bool {
    const source_profile = work.options.profile == .fastino_v1;
    var remaining = state.graph;
    while (remaining != 0) {
        const graph_index = takeFirst(GraphSet, &remaining);
        const edge = problem.graph[graph_index];
        try work.tick();
        if (state.nodes & nodeBit(edge.head) == 0 or state.nodes & nodeBit(edge.tail) == 0) return false;
        const spec = problem.schema.relations[edge.relation_type];
        if (!typed(problem, edge, spec.head, spec.tail) or (!spec.allow_self and edge.head == edge.tail)) return false;
        if (spec.max_per_head) |limit| if (try countEdges(problem, state, edge.relation_type, edge.head, true, work) > limit) return false;
        if (spec.max_per_tail) |limit| if (try countEdges(problem, state, edge.relation_type, edge.tail, false, work) > limit) return false;
        for (problem.schema.constraints) |constraint| switch (constraint) {
            .typed_endpoints => |c| if (applies(c.relation, edge.relation_type) and !typed(problem, edge, c.head, c.tail)) return false,
            .no_self_loops => |relation| if (applies(relation, edge.relation_type) and edge.head == edge.tail) return false,
            .max_per_head => |c| if (applies(c.relation, edge.relation_type) and try countEdges(problem, state, c.relation, edge.head, true, work) > c.limit) return false,
            .max_per_tail => |c| if (applies(c.relation, edge.relation_type) and try countEdges(problem, state, c.relation, edge.tail, false, work) > c.limit) return false,
            else => {},
        };
        var others = remaining;
        while (others != 0) {
            try work.tick();
            const other_index = takeFirst(GraphSet, &others);
            const other = problem.graph[other_index];
            if (other.relation_type != edge.relation_type) continue;
            if (other.head == edge.head and other.tail == edge.tail) return false;
            const reverse = other.head == edge.tail and other.tail == edge.head;
            const semantic_companion = !source_profile and reverse and isCompanion(problem.schema, edge, other);
            if (reverse and !spec.directed and !semantic_companion) return false;
            // The pinned compiler inserts UniqueRelationSlot("slot") for
            // every relation, including None slots on derived companions.
            if (source_profile and sourceSlot(problem, state, graph_index) == sourceSlot(problem, state, other_index)) return false;
            for (problem.schema.constraints) |constraint| switch (constraint) {
                .unique_pair => |c| if (applies(c.relation, edge.relation_type) and !c.directed and reverse and !semantic_companion) return false,
                .unique_slot => |c| if (applies(c.relation, edge.relation_type)) {
                    if (c.slot == .head and edge.head == other.head) return false;
                    if (c.slot == .tail and edge.tail == other.tail) return false;
                },
                else => {},
            };
        }
        if (companions) for (problem.schema.relations, 0..) |_, relation| {
            try work.tick();
            const companion = GraphEdge{ .relation_type = relation, .head = edge.tail, .tail = edge.head };
            if (isCompanion(problem.schema, edge, companion) and !try hasGraph(problem, state.graph, companion, work)) return false;
        };
    }
    // Explicit evidence owns count/slot resources. Derived companions carry no
    // new count hypothesis or slot and never consume these resources twice.
    var explicit = state.inputs;
    while (explicit != 0) {
        const i = takeFirst(InputSet, &explicit);
        const edge = problem.edges[i];
        var others = explicit;
        while (others != 0) {
            try work.tick();
            const other = problem.edges[takeFirst(InputSet, &others)];
            if (source_profile and edge.hypothesis == other.hypothesis) {
                if (edge.hypothesis != null and edge.count_alternative != null and other.count_alternative != null and edge.count_alternative != other.count_alternative) return false;
                if (edge.slot != null and edge.slot == other.slot and edge.count_alternative == other.count_alternative) return false;
            }
            if (edge.relation_type != other.relation_type) continue;
            if (edge.head == other.head and edge.tail == other.tail) return false;
            if (edge.head == other.tail and edge.tail == other.head) {
                if (!problem.schema.relations[edge.relation_type].directed) return false;
                for (problem.schema.constraints) |constraint| switch (constraint) {
                    .unique_pair => |c| if (applies(c.relation, edge.relation_type) and !c.directed) return false,
                    else => {},
                };
            }
            if (edge.hypothesis != other.hypothesis) continue;
            if (edge.hypothesis != null and edge.count_alternative != null and other.count_alternative != null and edge.count_alternative != other.count_alternative) return false;
            if (edge.slot != null and edge.slot == other.slot and edge.count_alternative == other.count_alternative) return false;
        }
    }
    for (problem.schema.constraints) |constraint| switch (constraint) {
        .acyclic => |relation| if (!try acyclic(problem, state.graph, relation, work)) return false,
        else => {},
    };
    return true;
}

fn acyclic(problem: Problem, selected: GraphSet, relation: usize, work: *Work) !bool {
    var indegree = [_]usize{0} ** max_nodes;
    var used: NodeSet = 0;
    var remaining = selected;
    while (remaining != 0) {
        try work.tick();
        const edge = problem.graph[takeFirst(GraphSet, &remaining)];
        if (edge.relation_type != relation) continue;
        used |= nodeBit(edge.head) | nodeBit(edge.tail);
        indegree[edge.tail] += 1;
    }
    var queue: [max_nodes]usize = undefined;
    var queue_len: usize = 0;
    var nodes = used;
    while (nodes != 0) {
        const node = takeFirst(NodeSet, &nodes);
        if (indegree[node] == 0) {
            queue[queue_len] = node;
            queue_len += 1;
        }
    }
    var pos: usize = 0;
    while (pos < queue_len) : (pos += 1) {
        remaining = selected;
        while (remaining != 0) {
            try work.tick();
            const edge = problem.graph[takeFirst(GraphSet, &remaining)];
            if (edge.relation_type != relation or edge.head != queue[pos]) continue;
            indegree[edge.tail] -= 1;
            if (indegree[edge.tail] == 0) {
                queue[queue_len] = edge.tail;
                queue_len += 1;
            }
        }
    }
    return queue_len == @popCount(used);
}

/// Fastino 3c913c7 beam.py/greedy.py. Candidate identity and scoring order are
/// part of this versioned profile; the native optimizer below is unchanged.
const SourceSearch = struct {
    problem: Problem,
    work: *Work,
    identity: SourceIdentity,
    keys: source_keys.Keys,
    edge_order: [max_edges]usize = undefined,
    greedy_order: [max_edges]usize = undefined,
    node_order: [max_nodes]usize = undefined,
    node_key_order: [max_nodes]usize = undefined,
    graph_order: [max_graph_edges]usize = undefined,
    visits: usize = 0,

    const Choice = struct { state: State, greedy: bool = false, feasible: bool = true };

    fn visit(self: *SourceSearch) !void {
        if (self.visits >= self.work.options.beam_node_budget) return error.JointSearchExhausted;
        self.visits += 1;
        try self.work.tick();
    }

    fn edgeLess(self: *const SourceSearch, left_index: usize, right_index: usize, greedy: bool) bool {
        const a = self.problem.edges[left_index];
        const b = self.problem.edges[right_index];
        // Source beam includes both endpoint terms even for a self edge;
        // Greedy's initial rank counts that endpoint only once.
        const ag = a.utility + self.problem.nodes[a.head].utility + (if (greedy and a.head == a.tail) 0 else self.problem.nodes[a.tail].utility);
        const bg = b.utility + self.problem.nodes[b.head].utility + (if (greedy and b.head == b.tail) 0 else self.problem.nodes[b.tail].utility);
        if (ag != bg) return ag > bg;
        if (greedy and a.utility != b.utility) return a.utility > b.utility;
        const names = std.mem.order(u8, self.problem.schema.relations[a.relation_type].name, self.problem.schema.relations[b.relation_type].name);
        if (names != .eq) return names == .lt;
        inline for (.{ "hypotheses", "slots" }) |field| {
            const order = std.mem.order(u8, @field(self.keys, field)[left_index], @field(self.keys, field)[right_index]);
            if (order != .eq) return order == .lt;
        }
        const heads = std.mem.order(u8, self.keys.nodes[a.head], self.keys.nodes[b.head]);
        if (heads != .eq) return heads == .lt;
        const tails = std.mem.order(u8, self.keys.nodes[a.tail], self.keys.nodes[b.tail]);
        if (tails != .eq) return tails == .lt;
        return left_index < right_index; // Python stable sort.
    }

    fn initialize(self: *SourceSearch) !void {
        for (self.problem.edges, 0..) |edge, i| {
            _ = try finite(try finite(edge.utility + self.problem.nodes[edge.head].utility) + self.problem.nodes[edge.tail].utility);
            self.edge_order[i] = i;
            self.greedy_order[i] = i;
        }
        for (self.problem.nodes, 0..) |_, i| {
            self.node_order[i] = i;
            self.node_key_order[i] = i;
        }
        for (self.problem.graph, 0..) |_, i| self.graph_order[i] = i;
        const Sort = struct {
            fn edge(ctx: *const SourceSearch, a: usize, b: usize) bool {
                return ctx.edgeLess(a, b, false);
            }
            fn greedy(ctx: *const SourceSearch, a: usize, b: usize) bool {
                return ctx.edgeLess(a, b, true);
            }
            fn node(ctx: *const SourceSearch, a: usize, b: usize) bool {
                const x = ctx.problem.nodes[a];
                const y = ctx.problem.nodes[b];
                if (x.utility != y.utility) return x.utility > y.utility;
                const names = std.mem.order(u8, ctx.problem.schema.entities[x.entity_type].name, ctx.problem.schema.entities[y.entity_type].name);
                if (names != .eq) return names == .lt;
                const xs = ctx.identity.node_spans[a];
                const ys = ctx.identity.node_spans[b];
                return if (xs.start != ys.start) xs.start < ys.start else xs.end < ys.end;
            }
            fn key(ctx: *const SourceSearch, a: usize, b: usize) bool {
                return std.mem.lessThan(u8, ctx.keys.nodes[a], ctx.keys.nodes[b]);
            }
            fn graph(ctx: *const SourceSearch, a: usize, b: usize) bool {
                const x = ctx.problem.graph[a];
                const y = ctx.problem.graph[b];
                const names = std.mem.order(u8, ctx.problem.schema.relations[x.relation_type].name, ctx.problem.schema.relations[y.relation_type].name);
                if (names != .eq) return names == .lt;
                const heads = std.mem.order(u8, ctx.keys.nodes[x.head], ctx.keys.nodes[y.head]);
                if (heads != .eq) return heads == .lt;
                return std.mem.lessThan(u8, ctx.keys.nodes[x.tail], ctx.keys.nodes[y.tail]);
            }
        };
        std.mem.sort(usize, self.edge_order[0..self.problem.edges.len], self, Sort.edge);
        std.mem.sort(usize, self.greedy_order[0..self.problem.edges.len], self, Sort.greedy);
        std.mem.sort(usize, self.node_order[0..self.problem.nodes.len], self, Sort.node);
        std.mem.sort(usize, self.node_key_order[0..self.problem.nodes.len], self, Sort.key);
        std.mem.sort(usize, self.graph_order[0..self.problem.graph.len], self, Sort.graph);
        if (self.work.options.control) |control| try control.check();
    }

    fn include(self: *SourceSearch, state: State, index: usize) !?State {
        const edge = self.problem.edges[index];
        var next = state;
        next.inputs |= inputBit(index);
        next.graph |= graphBit(self.problem.bundles[index].primary);
        var node_gain: f64 = 0;
        // Keep the source list's duplicate endpoint behavior for allow_self.
        for ([_]usize{ edge.head, edge.tail }) |node| if (state.nodes & nodeBit(node) == 0) {
            node_gain = try finite(node_gain + self.problem.nodes[node].utility);
            next.nodes |= nodeBit(node);
        };
        const gain = try finite(edge.utility + node_gain);
        if (gain < 0) return null;
        if (!try validNodes(self.problem, next.nodes, self.work) or !try validGraphMode(self.problem, next, self.work, false)) return null;
        next.score = try finite(state.score + gain);
        return next;
    }

    fn finishNodes(self: *SourceSearch, state: State) !State {
        var next = state;
        for (self.node_order[0..self.problem.nodes.len]) |index| {
            try self.visit();
            if (next.nodes & nodeBit(index) != 0 or self.problem.nodes[index].utility <= 0) continue;
            const proposal = next.nodes | nodeBit(index);
            if (!try validNodes(self.problem, proposal, self.work)) continue;
            next.nodes = proposal;
            next.score = try finite(next.score + self.problem.nodes[index].utility);
        }
        return next;
    }

    fn close(self: *SourceSearch, state: State) State {
        var result = state;
        var inputs = state.inputs;
        while (inputs != 0) result.graph |= self.problem.bundles[takeFirst(InputSet, &inputs)].graph;
        return result;
    }

    fn nodeSignature(self: *SourceSearch, a: NodeSet, b: NodeSet) !std.math.Order {
        var ai: usize = 0;
        var bi: usize = 0;
        const ordered_nodes = self.node_key_order[0..self.problem.nodes.len];
        while (true) {
            while (ai < ordered_nodes.len and a & nodeBit(ordered_nodes[ai]) == 0) : (ai += 1) {}
            while (bi < ordered_nodes.len and b & nodeBit(ordered_nodes[bi]) == 0) : (bi += 1) {}
            try self.work.tick();
            if (ai == ordered_nodes.len or bi == ordered_nodes.len) return std.math.order(ordered_nodes.len - ai, ordered_nodes.len - bi);
            const order = std.mem.order(u8, self.keys.nodes[ordered_nodes[ai]], self.keys.nodes[ordered_nodes[bi]]);
            if (order != .eq) return order;
            ai += 1;
            bi += 1;
        }
    }

    fn partialSignature(self: *SourceSearch, a: State, b: State) !std.math.Order {
        const nodes = try self.nodeSignature(a.nodes, b.nodes);
        if (nodes != .eq) return nodes;
        var ai: usize = 0;
        var bi: usize = 0;
        const order = self.edge_order[0..self.problem.edges.len];
        while (true) {
            while (ai < order.len and a.inputs & inputBit(order[ai]) == 0) : (ai += 1) {}
            while (bi < order.len and b.inputs & inputBit(order[bi]) == 0) : (bi += 1) {}
            try self.work.tick();
            if (ai == order.len or bi == order.len) return std.math.order(order.len - ai, order.len - bi);
            const keys = std.mem.order(u8, self.keys.edges[order[ai]], self.keys.edges[order[bi]]);
            if (keys != .eq) return keys;
            ai += 1;
            bi += 1;
        }
    }

    fn finalKey(self: *const SourceSearch, state: State, index: usize) []const u8 {
        return if (evidenceIndex(self.problem, state, index)) |source| self.keys.edges[source] else self.keys.derived[index];
    }
    fn finalSignature(self: *SourceSearch, a: State, b: State) !std.math.Order {
        const nodes = try self.nodeSignature(a.nodes, b.nodes);
        if (nodes != .eq) return nodes;
        var ai: usize = 0;
        var bi: usize = 0;
        const order = self.graph_order[0..self.problem.graph.len];
        while (true) {
            while (ai < order.len and a.graph & graphBit(order[ai]) == 0) : (ai += 1) {}
            while (bi < order.len and b.graph & graphBit(order[bi]) == 0) : (bi += 1) {}
            try self.work.tick();
            if (ai == order.len or bi == order.len) return std.math.order(order.len - ai, order.len - bi);
            const keys = std.mem.order(u8, self.finalKey(a, order[ai]), self.finalKey(b, order[bi]));
            if (keys != .eq) return keys;
            ai += 1;
            bi += 1;
        }
    }

    fn insert(self: *SourceSearch, list: []State, len: *usize, state: State) !void {
        for (list[0..len.*], 0..) |existing, i| {
            try self.work.tick();
            if (existing.nodes == state.nodes and existing.inputs == state.inputs) {
                if (state.score <= existing.score) return;
                // Same semantic selection is retained only at its best score.
                std.mem.copyForwards(State, list[i .. len.* - 1], list[i + 1 .. len.*]);
                len.* -= 1;
                break;
            }
        }
        var index: usize = 0;
        while (index < len.*) : (index += 1) {
            try self.work.tick();
            if (state.score > list[index].score or (state.score == list[index].score and try self.partialSignature(state, list[index]) == .lt)) break;
        }
        const end = @min(list.len, len.* + 1);
        if (index == list.len) return;
        var cursor = end - 1;
        while (cursor > index) : (cursor -= 1) list[cursor] = list[cursor - 1];
        list[index] = state;
        len.* = end;
    }

    fn candidate(self: *SourceSearch, best: *?Choice, candidate_: Choice) !void {
        if (!try validNodes(self.problem, candidate_.state.nodes, self.work) or !try validGraph(self.problem, candidate_.state, self.work)) return;
        if (best.*) |old| {
            if (candidate_.state.score < old.state.score) return;
            if (candidate_.state.score == old.state.score and try self.finalSignature(candidate_.state, old.state) != .gt) return;
        }
        best.* = candidate_;
    }

    fn run(self: *SourceSearch, allocator: Allocator, scratch: Allocator) !Result {
        try self.initialize();
        const width = self.work.options.beam_width;
        var current = try scratch.alloc(State, width);
        var next = try scratch.alloc(State, width);
        current[0] = .{};
        var count: usize = 1;
        for (self.edge_order[0..self.problem.edges.len]) |index| {
            var length: usize = 0;
            // Source considers every skip before additions; stable ties retain
            // their first occurrence before the score/signature cut.
            for (current[0..count]) |state| try self.insert(next, &length, state);
            for (current[0..count]) |state| {
                try self.visit();
                if (try self.include(state, index)) |added| try self.insert(next, &length, added);
            }
            std.mem.swap([]State, &current, &next);
            count = length;
        }
        var best: ?Choice = null;
        for (current[0..count]) |state| try self.candidate(&best, .{ .state = self.close(try self.finishNodes(state)) });
        var greedy = State{};
        for (self.greedy_order[0..self.problem.edges.len]) |index| {
            try self.visit();
            if (try self.include(greedy, index)) |added| greedy = added;
        }
        greedy = self.close(try self.finishNodes(greedy));
        const greedy_valid = try validNodes(self.problem, greedy.nodes, self.work) and try validGraph(self.problem, greedy, self.work);
        try self.candidate(&best, .{ .state = if (greedy_valid) greedy else .{}, .greedy = true, .feasible = greedy_valid });
        if (self.work.options.control) |control| try control.check();
        const choice = best orelse return emptyResult(allocator, .infeasible, self.visits);
        return materializeOrdered(allocator, self.problem, choice.state, if (choice.feasible) .feasible else .infeasible, false, self.visits, self.work, if (choice.greedy) self.greedy_order[0..self.problem.edges.len] else self.edge_order[0..self.problem.edges.len], self.graph_order[0..self.problem.graph.len]);
    }
};

const Search = struct {
    problem: Problem,
    work: *Work,
    edge_order: [max_edges]usize = undefined,
    node_order: [max_nodes]usize = undefined,
    best: ?State = null,
    visits: usize = 0,

    fn initializeOrder(self: *Search) void {
        for (self.problem.edges, 0..) |_, i| self.edge_order[i] = i;
        for (self.problem.nodes, 0..) |_, i| self.node_order[i] = i;
        const Sort = struct {
            fn edge(problem: Problem, a: usize, b: usize) bool {
                const left = problem.edges[a];
                const right = problem.edges[b];
                const left_gain = left.utility + problem.nodes[left.head].utility + (if (left.head == left.tail) 0 else problem.nodes[left.tail].utility);
                const right_gain = right.utility + problem.nodes[right.head].utility + (if (right.head == right.tail) 0 else problem.nodes[right.tail].utility);
                if (left_gain != right_gain) return left_gain > right_gain;
                const names = std.mem.order(u8, problem.schema.relations[left.relation_type].name, problem.schema.relations[right.relation_type].name);
                if (names != .eq) return names == .lt;
                if (left.head != right.head) return left.head < right.head;
                if (left.tail != right.tail) return left.tail < right.tail;
                return a < b;
            }
            fn node(problem: Problem, a: usize, b: usize) bool {
                const left = problem.nodes[a];
                const right = problem.nodes[b];
                if (left.utility != right.utility) return left.utility > right.utility;
                const names = std.mem.order(u8, problem.schema.entities[left.entity_type].name, problem.schema.entities[right.entity_type].name);
                if (names != .eq) return names == .lt;
                if (left.start != right.start) return left.start < right.start;
                if (left.end != right.end) return left.end < right.end;
                return a < b;
            }
        };
        std.mem.sort(usize, self.edge_order[0..self.problem.edges.len], self.problem, Sort.edge);
        std.mem.sort(usize, self.node_order[0..self.problem.nodes.len], self.problem, Sort.node);
    }
    fn includeEdge(self: *Search, state: State, index: usize) !?State {
        if (state.inputs & inputBit(index) != 0) return state;
        const edge = self.problem.edges[index];
        var next = state;
        next.inputs |= inputBit(index);
        next.graph |= self.problem.bundles[index].graph;
        var added = (nodeBit(edge.head) | nodeBit(edge.tail)) & ~state.nodes;
        next.nodes |= added;
        next.score = try finite(next.score + edge.utility);
        while (added != 0) next.score = try finite(next.score + self.problem.nodes[takeFirst(NodeSet, &added)].utility);
        if (!try validNodes(self.problem, next.nodes, self.work) or !try validGraph(self.problem, next, self.work)) return null;
        return next;
    }
    fn includeNode(self: *Search, state: State, index: usize) !?State {
        if (state.nodes & nodeBit(index) != 0) return state;
        var next = state;
        next.nodes |= nodeBit(index);
        next.score = try finite(next.score + self.problem.nodes[index].utility);
        if (!try validNodes(self.problem, next.nodes, self.work)) return null;
        return next;
    }
    fn better(a: State, b: State) bool {
        if (a.score != b.score) return a.score > b.score;
        // Prefer a smaller explanation when evidence is exactly tied, then
        // declaration-stable bit sets. No hash iteration influences selection.
        const size_a = @as(usize, @popCount(a.nodes)) + @as(usize, @popCount(a.inputs));
        const size_b = @as(usize, @popCount(b.nodes)) + @as(usize, @popCount(b.inputs));
        if (size_a != size_b) return size_a < size_b;
        if (a.nodes != b.nodes) return a.nodes < b.nodes;
        return a.inputs < b.inputs;
    }
    fn consider(self: *Search, state: State) void {
        if (self.best == null or better(state, self.best.?)) self.best = state;
    }
    fn upper(self: *Search, depth: usize, state: State) !f64 {
        var bound = state.score;
        for (self.problem.nodes, 0..) |node, i| if (state.nodes & nodeBit(i) == 0 and node.utility > 0) {
            bound = try finite(bound + node.utility);
        };
        if (depth < self.problem.edges.len) for (self.edge_order[depth..self.problem.edges.len]) |i| {
            if (state.inputs & inputBit(i) == 0 and self.problem.edges[i].utility > 0) bound = try finite(bound + self.problem.edges[i].utility);
        };
        return bound;
    }
    fn exact(self: *Search, depth: usize, state: State) anyerror!void {
        if (self.visits >= self.work.options.exact_node_budget) return error.JointSearchExhausted;
        self.visits += 1;
        try self.work.tick();
        self.consider(state);
        if (depth == self.problem.edges.len + self.problem.nodes.len) return;
        if (self.best) |best| if (try self.upper(depth, state) < best.score) return;
        if (depth < self.problem.edges.len) {
            const index = self.edge_order[depth];
            if (state.inputs & inputBit(index) != 0) return self.exact(depth + 1, state);
            if (try self.includeEdge(state, index)) |next| try self.exact(depth + 1, next);
            if (!self.problem.edges[index].required) try self.exact(depth + 1, state);
        } else {
            const index = self.node_order[depth - self.problem.edges.len];
            if (state.nodes & nodeBit(index) != 0 or self.problem.nodes[index].utility <= 0) return self.exact(depth + 1, state);
            if (try self.includeNode(state, index)) |next| try self.exact(depth + 1, next);
            try self.exact(depth + 1, state);
        }
    }
    fn beam(self: *Search, allocator: Allocator, initial: State) !BeamOutcome {
        var current = try allocator.alloc(State, self.work.options.beam_width);
        var next = try allocator.alloc(State, self.work.options.beam_width);
        current[0] = initial;
        var current_len: usize = 1;
        var visits: usize = 0;
        var complete = true;
        for (0..self.problem.edges.len + self.problem.nodes.len) |depth| {
            var next_len: usize = 0;
            for (current[0..current_len]) |state| {
                if (visits >= self.work.options.beam_node_budget) return .exhausted;
                visits += 1;
                self.visits += 1;
                try self.work.tick();
                // Skip is always legal because required candidates were seeded.
                self.insertBeam(next, &next_len, state, &complete);
                const candidate = if (depth < self.problem.edges.len) try self.includeEdge(state, self.edge_order[depth]) else blk: {
                    const index = self.node_order[depth - self.problem.edges.len];
                    break :blk if (self.problem.nodes[index].utility > 0) try self.includeNode(state, index) else null;
                };
                if (candidate) |added| {
                    self.consider(added);
                    self.insertBeam(next, &next_len, added, &complete);
                }
            }
            std.mem.swap([]State, &current, &next);
            current_len = next_len;
        }
        return if (complete) .complete else .pruned;
    }
    fn insertBeam(_: *Search, list: []State, len: *usize, state: State, complete: *bool) void {
        for (list[0..len.*]) |existing| if (existing.nodes == state.nodes and existing.inputs == state.inputs) return;
        var index: usize = 0;
        while (index < len.* and !better(state, list[index])) index += 1;
        if (len.* == list.len) complete.* = false;
        if (index >= list.len) return;
        const new_len = @min(list.len, len.* + 1);
        var cursor = new_len - 1;
        while (cursor > index) : (cursor -= 1) list[cursor] = list[cursor - 1];
        list[index] = state;
        len.* = new_len;
    }
};

fn materialize(allocator: Allocator, problem: Problem, state: State, status: Status, exhausted: bool, visits: usize, work: *Work) !Result {
    return materializeOrdered(allocator, problem, state, status, exhausted, visits, work, null, null);
}

fn companionForConstraint(constraint: schema_mod.JointConstraint, source: GraphEdge, target: GraphEdge) bool {
    if (source.head != target.tail or source.tail != target.head) return false;
    return switch (constraint) {
        .symmetric => |relation| source.relation_type == relation and target.relation_type == relation,
        .inverse => |pair| (source.relation_type == pair.relation and target.relation_type == pair.inverse) or
            (source.relation_type == pair.inverse and target.relation_type == pair.relation),
        else => false,
    };
}

fn sourceCompanionWitness(problem: Problem, state: State, target: GraphEdge, order: []const usize, work: *Work) !?usize {
    // Source compile_schema retains authored constraints first, then appends
    // relation-spec constraints in declaration order. solution() iterates that
    // constraint order before each original selected edge; derived probability
    // provenance must retain the same first witness.
    const Find = struct {
        fn run(p: Problem, selected: InputSet, wanted: GraphEdge, ordered: []const usize, constraint: schema_mod.JointConstraint, w: *Work) !?usize {
            switch (constraint) {
                .symmetric, .inverse => {},
                else => return null,
            }
            for (ordered) |index| if (selected & inputBit(index) != 0) {
                try w.tick();
                if (companionForConstraint(constraint, p.graph[p.bundles[index].primary], wanted)) return index;
            };
            return null;
        }
    };
    for (problem.schema.constraints) |constraint| if (try Find.run(problem, state.inputs, target, order, constraint, work)) |index| return index;
    for (problem.schema.relations, 0..) |relation, index| {
        if (relation.symmetric) if (try Find.run(problem, state.inputs, target, order, .{ .symmetric = index }, work)) |source| return source;
        if (relation.inverse) |inverse| if (try Find.run(problem, state.inputs, target, order, .{ .inverse = .{ .relation = index, .inverse = inverse } }, work)) |source| return source;
    }
    return null;
}

fn materializeOrdered(allocator: Allocator, problem: Problem, state: State, status: Status, exhausted: bool, visits: usize, work: *Work, source_order: ?[]const usize, graph_order: ?[]const usize) !Result {
    const nodes = try allocator.alloc(Node, @popCount(state.nodes));
    errdefer allocator.free(nodes);
    const node_sources = try allocator.alloc(usize, nodes.len);
    errdefer allocator.free(node_sources);
    const edges = try allocator.alloc(DecodedEdge, @popCount(state.graph));
    errdefer allocator.free(edges);
    var map: [max_nodes]usize = undefined;
    var selected = state.nodes;
    for (nodes, node_sources, 0..) |*node, *source, i| {
        source.* = takeFirst(NodeSet, &selected);
        node.* = problem.nodes[source.*];
        map[source.*] = i;
    }
    var remaining = state.graph;
    var graph_position: usize = 0;
    for (edges) |*output| {
        try work.tick();
        const graph_index = if (graph_order) |order| blk: {
            while (state.graph & graphBit(order[graph_position]) == 0) : (graph_position += 1) {}
            const index = order[graph_position];
            graph_position += 1;
            break :blk index;
        } else takeFirst(GraphSet, &remaining);
        const edge = problem.graph[graph_index];
        var source: ?usize = null;
        var derived_from: ?usize = null;
        if (source_order) |order| {
            for (order) |i| if (state.inputs & inputBit(i) != 0) {
                if (problem.bundles[i].primary == graph_index) source = i;
            };
            if (source == null) derived_from = try sourceCompanionWitness(problem, state, edge, order, work);
        } else {
            var inputs = state.inputs;
            while (inputs != 0) {
                const i = takeFirst(InputSet, &inputs);
                if (problem.bundles[i].primary == graph_index) source = i;
                if (problem.bundles[i].graph & graphBit(graph_index) != 0 and (derived_from == null or
                    problem.edges[i].utility > problem.edges[derived_from.?].utility)) derived_from = i;
            }
        }
        const evidence = problem.edges[source orelse derived_from.?];
        output.* = .{
            .relation_type = edge.relation_type,
            .head = map[edge.head],
            .tail = map[edge.tail],
            .utility = if (source != null) evidence.utility else 0,
            .probability = evidence.probability,
            .source_index = source,
            .derived_from = if (source == null) derived_from else null,
            .derived = source == null,
            .slot = if (source != null) evidence.slot else null,
            .hypothesis = if (source != null) evidence.hypothesis else null,
            .count_alternative = if (source != null) evidence.count_alternative else null,
        };
    }
    const Sort = struct {
        fn less(_: void, a: DecodedEdge, b: DecodedEdge) bool {
            if (a.relation_type != b.relation_type) return a.relation_type < b.relation_type;
            if (a.head != b.head) return a.head < b.head;
            if (a.tail != b.tail) return a.tail < b.tail;
            return !a.derived and b.derived;
        }
    };
    if (graph_order == null) std.mem.sort(DecodedEdge, edges, {}, Sort.less);
    if (!try validateGlobal(allocator, problem.schema, nodes, edges, work.options)) return error.InvalidJointDecodeResult;
    return .{ .allocator = allocator, .status = status, .nodes = nodes, .node_source_indices = node_sources, .edges = edges, .utility = state.score, .visited_nodes = visits, .exhausted = exhausted };
}

/// ResultBuilder source presentation: globally source-order selected entities,
/// assign e1..eN, then order relations by (type, head ID string, tail ID string).
/// All edge provenance fields remain attached to their unchanged edge value;
/// source_index/derived_from refer to input candidates, not this output array.
pub fn sortSourcePresentation(schema: schema_mod.JointSchema, nodes: []const Node, edges: []DecodedEdge, control: ?Control) !void {
    if (nodes.len > max_nodes or edges.len > max_graph_edges) return error.JointCandidateLimitExceeded;
    if (control) |value| try value.check();
    var order: [max_nodes]usize = undefined;
    var identifiers: [max_nodes][4]u8 = undefined;
    var id_lengths: [max_nodes]usize = undefined;
    for (nodes, 0..) |node, i| {
        if (node.entity_type >= schema.entities.len or node.end <= node.start) return error.InvalidJointCandidate;
        order[i] = i;
    }
    for (edges) |edge| if (edge.relation_type >= schema.relations.len or edge.head >= nodes.len or edge.tail >= nodes.len) return error.InvalidJointCandidate;
    const NodeOrder = struct {
        schema: schema_mod.JointSchema,
        nodes: []const Node,
        fn less(self: @This(), a: usize, b: usize) bool {
            const x = self.nodes[a];
            const y = self.nodes[b];
            if (x.start != y.start) return x.start < y.start;
            if (x.end != y.end) return x.end < y.end;
            const names = std.mem.order(u8, self.schema.entities[x.entity_type].name, self.schema.entities[y.entity_type].name);
            // Equal source range implies equal text in the original document.
            return if (names != .eq) names == .lt else a < b;
        }
    };
    std.mem.sort(usize, order[0..nodes.len], NodeOrder{ .schema = schema, .nodes = nodes }, NodeOrder.less);
    for (order[0..nodes.len], 0..) |node, i| id_lengths[node] = (std.fmt.bufPrint(&identifiers[node], "e{d}", .{i + 1}) catch unreachable).len;
    const EdgeOrder = struct {
        schema: schema_mod.JointSchema,
        ids: []const [4]u8,
        lengths: []const usize,
        fn less(self: @This(), a: DecodedEdge, b: DecodedEdge) bool {
            const names = std.mem.order(u8, self.schema.relations[a.relation_type].name, self.schema.relations[b.relation_type].name);
            if (names != .eq) return names == .lt;
            const heads = std.mem.order(u8, self.ids[a.head][0..self.lengths[a.head]], self.ids[b.head][0..self.lengths[b.head]]);
            if (heads != .eq) return heads == .lt;
            return std.mem.lessThan(u8, self.ids[a.tail][0..self.lengths[a.tail]], self.ids[b.tail][0..self.lengths[b.tail]]);
        }
    };
    std.mem.sort(DecodedEdge, edges, EdgeOrder{ .schema = schema, .ids = identifiers[0..nodes.len], .lengths = id_lengths[0..nodes.len] }, EdgeOrder.less);
    if (control) |value| try value.check();
}
fn emptyResult(allocator: Allocator, status: Status, visits: usize) !Result {
    const nodes = try allocator.alloc(Node, 0);
    errdefer allocator.free(nodes);
    const sources = try allocator.alloc(usize, 0);
    errdefer allocator.free(sources);
    const edges = try allocator.alloc(DecodedEdge, 0);
    return .{ .allocator = allocator, .status = status, .nodes = nodes, .node_source_indices = sources, .edges = edges, .utility = -std.math.inf(f64), .visited_nodes = visits };
}
fn nodeBit(index: usize) NodeSet {
    return @as(NodeSet, 1) << @as(u8, @intCast(index));
}
fn inputBit(index: usize) InputSet {
    return @as(InputSet, 1) << @as(u8, @intCast(index));
}
fn graphBit(index: usize) GraphSet {
    return @as(GraphSet, 1) << @as(u10, @intCast(index));
}
fn takeFirst(comptime T: type, set: *T) usize {
    const index: usize = @intCast(@ctz(set.*));
    set.* &= set.* - 1;
    return index;
}
fn lowMask(comptime T: type, count: usize) T {
    return if (count == @bitSizeOf(T)) std.math.maxInt(T) else (@as(T, 1) << @as(std.math.Log2Int(T), @intCast(count))) - 1;
}
fn finite(value: f64) !f64 {
    return if (std.math.isFinite(value)) value else error.InvalidJointCandidate;
}
fn validProbability(value: f64) bool {
    return std.math.isFinite(value) and value >= 0 and value <= 1;
}

fn testNode(start: usize, utility: f64) Node {
    return .{ .entity_type = 0, .start = start, .end = start + 1, .utility = utility, .probability = 0.5 };
}

test "joint fastino profile edge beam differs from an exact witness" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, basic_schema, .{});
    defer compiled.deinit();
    var nodes: [12]Node = undefined;
    var spans: [12]SourceSpan = undefined;
    var edges: [6]Edge = undefined;
    for (&nodes, &spans, 0..) |*node, *span, i| {
        node.* = testNode(i * 10, 10);
        span.* = .{ .start = i, .end = i + 1 };
    }
    for (&edges, 0..) |*edge, i| edge.* = .{ .relation_type = 0, .head = i * 2, .tail = i * 2 + 1, .utility = -1, .probability = 0.25, .slot = i, .hypothesis = 0 };
    var source = try decodeWithSourceIdentity(a, compiled.schema.joint_ie.?, &nodes, &edges, .{ .node_spans = &spans }, .{ .profile = .fastino_v1, .algorithm = .beam });
    defer source.deinit();
    try std.testing.expectEqual(@as(usize, 12), source.nodes.len);
    try std.testing.expectEqual(@as(usize, 3), source.edges.len);
    try std.testing.expectEqual(@as(f64, 117), source.utility);
    try std.testing.expectEqual(Status.feasible, source.status);
    try std.testing.expect(!source.exhausted);
    var native = try decode(a, compiled.schema.joint_ie.?, &nodes, &edges, .{ .algorithm = .exact });
    defer native.deinit();
    try std.testing.expectEqual(@as(usize, 0), native.edges.len);
    try std.testing.expectEqual(@as(f64, 120), native.utility);
    try std.testing.expectEqual(Status.optimal, native.status);
}

test "joint fastino profile preserves zero gain and source self endpoint scoring" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, "{\"joint_ie\":{\"entities\":{\"person\":{}},\"relations\":{\"knows\":{\"head\":[\"person\"],\"tail\":[\"person\"],\"allow_self\":true}}}}", .{});
    defer compiled.deinit();
    const spans = [_]SourceSpan{ .{ .start = 2, .end = 3 }, .{ .start = 10, .end = 11 } };
    const nodes = [_]Node{ testNode(20, 0), testNode(100, 0) };
    const edges = [_]Edge{.{ .relation_type = 0, .head = 0, .tail = 1, .utility = 0, .probability = 0.5, .slot = 0, .hypothesis = 0 }};
    var zero = try decodeWithSourceIdentity(a, compiled.schema.joint_ie.?, &nodes, &edges, .{ .node_spans = &spans }, .{ .profile = .fastino_v1, .algorithm = .beam });
    defer zero.deinit();
    try std.testing.expectEqual(@as(usize, 1), zero.edges.len);
    try std.testing.expectEqual(@as(f64, 0), zero.utility);
    const self_nodes = [_]Node{testNode(20, 1)};
    var self_edges = edges;
    self_edges[0].tail = 0;
    var self_result = try decodeWithSourceIdentity(a, compiled.schema.joint_ie.?, &self_nodes, &self_edges, .{ .node_spans = spans[0..1] }, .{ .profile = .fastino_v1, .algorithm = .beam });
    defer self_result.deinit();
    try std.testing.expectEqual(@as(usize, 1), self_result.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), self_result.edges.len);
    // Actual source constructs a duplicate new_ids list before set union.
    try std.testing.expectEqual(@as(f64, 2), self_result.utility);
}

test "joint fastino profile validates derived candidates after node completion" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, "{\"joint_ie\":{\"entities\":{\"person\":{}},\"relations\":{\"knows\":{\"head\":[\"person\"],\"tail\":[\"person\"],\"symmetric\":true}},\"constraints\":[{\"type\":\"AcyclicRelation\",\"relation\":\"knows\"}]}}", .{});
    defer compiled.deinit();
    const nodes = [_]Node{ testNode(20, 1), testNode(100, 1) };
    const spans = [_]SourceSpan{ .{ .start = 2, .end = 3 }, .{ .start = 10, .end = 11 } };
    const edges = [_]Edge{.{ .relation_type = 0, .head = 0, .tail = 1, .utility = 3, .probability = 0.95, .slot = 0, .hypothesis = 0 }};
    var result = try decodeWithSourceIdentity(a, compiled.schema.joint_ie.?, &nodes, &edges, .{ .node_spans = &spans }, .{ .profile = .fastino_v1, .algorithm = .beam });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.nodes.len);
    try std.testing.expectEqual(@as(usize, 0), result.edges.len);
    try std.testing.expectEqual(@as(f64, 2), result.utility);
    try std.testing.expect(result.valid());
}

test "joint fastino profile derived provenance follows constraint order" {
    const a = std.testing.allocator;
    const entities = [_]schema_mod.JointEntity{.{ .name = "person" }};
    const relations = [_]schema_mod.JointRelation{
        .{ .name = "r", .head = &.{0}, .tail = &.{0} },
        .{ .name = "s", .head = &.{0}, .tail = &.{0} },
        .{ .name = "t", .head = &.{0}, .tail = &.{0} },
    };
    const constraints = [_]schema_mod.JointConstraint{
        .{ .inverse = .{ .relation = 0, .inverse = 2 } },
        .{ .inverse = .{ .relation = 1, .inverse = 2 } },
    };
    const schema = schema_mod.JointSchema{ .entities = &entities, .relations = &relations, .constraints = &constraints };
    const nodes = [_]Node{ testNode(20, 1), testNode(100, 1) };
    const spans = [_]SourceSpan{ .{ .start = 2, .end = 3 }, .{ .start = 10, .end = 11 } };
    const edges = [_]Edge{
        .{ .relation_type = 0, .head = 0, .tail = 1, .utility = 2, .probability = 0.6, .slot = 0, .hypothesis = 0 },
        .{ .relation_type = 1, .head = 0, .tail = 1, .utility = 3, .probability = 0.9, .slot = 0, .hypothesis = 1 },
    };
    var result = try decodeWithSourceIdentity(a, schema, &nodes, &edges, .{ .node_spans = &spans }, .{ .profile = .fastino_v1, .algorithm = .beam });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.edges.len);
    try std.testing.expectEqual(@as(?usize, 0), result.edges[2].derived_from);
    try std.testing.expectEqual(@as(f64, 0.6), result.edges[2].probability);
    try sortSourcePresentation(schema, result.nodes, result.edges, null);
    try std.testing.expectEqual(@as(?usize, 0), result.edges[2].derived_from);
}

test "joint fastino profile owns body token keys and bounded failure cleanup" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, basic_schema, .{});
    defer compiled.deinit();
    const Check = struct {
        fn run(allocator: Allocator, schema: schema_mod.JointSchema) !void {
            const nodes = [_]Node{ testNode(200, 1), testNode(1000, 1) };
            const spans = [_]SourceSpan{ .{ .start = 2, .end = 3 }, .{ .start = 10, .end = 11 } };
            const edges = [_]Edge{.{ .relation_type = 0, .head = 0, .tail = 1, .utility = 1, .probability = 0.75, .slot = 10, .hypothesis = 0 }};
            var result = try decodeWithSourceIdentity(allocator, schema, &nodes, &edges, .{ .node_spans = &spans }, .{ .profile = .fastino_v1, .algorithm = .beam });
            defer result.deinit();
            try std.testing.expectEqual(@as(f64, 3), result.utility);
            try std.testing.expectEqual(@as(usize, 1), result.edges.len);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{compiled.schema.joint_ie.?});
    const nodes = [_]Node{ testNode(200, 1), testNode(1000, 1) };
    const spans = [_]SourceSpan{ .{ .start = 2, .end = 3 }, .{ .start = 10, .end = 11 } };
    const edges = [_]Edge{.{ .relation_type = 0, .head = 0, .tail = 1, .utility = 1, .probability = 0.75, .slot = 10, .hypothesis = 0 }};
    const profile = Options{ .profile = .fastino_v1, .algorithm = .beam };
    try std.testing.expectError(error.MissingJointSourceIdentity, decode(a, compiled.schema.joint_ie.?, &nodes, &edges, profile));
    var bad = profile;
    bad.algorithm = .exact;
    try std.testing.expectError(error.InvalidJointSourceOptions, decodeWithSourceIdentity(a, compiled.schema.joint_ie.?, &nodes, &edges, .{ .node_spans = &spans }, bad));
    bad = profile;
    bad.max_source_key_bytes = 1;
    try std.testing.expectError(error.JointCandidateLimitExceeded, decodeWithSourceIdentity(a, compiled.schema.joint_ie.?, &nodes, &edges, .{ .node_spans = &spans }, bad));
    bad = profile;
    bad.beam_node_budget = 0;
    try std.testing.expectError(error.JointSearchExhausted, decodeWithSourceIdentity(a, compiled.schema.joint_ie.?, &nodes, &edges, .{ .node_spans = &spans }, bad));
    bad = profile;
    bad.max_validation_steps = 1;
    try std.testing.expectError(error.JointValidationLimitExceeded, decodeWithSourceIdentity(a, compiled.schema.joint_ie.?, &nodes, &edges, .{ .node_spans = &spans }, bad));
    const Cancel = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls >= 3) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    bad = profile;
    bad.control = .{ .ptr = &cancel, .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, decodeWithSourceIdentity(a, compiled.schema.joint_ie.?, &nodes, &edges, .{ .node_spans = &spans }, bad));
    try Check.run(a, compiled.schema.joint_ie.?);
}

test "joint source presentation uses lexical entity IDs and preserves provenance" {
    const a = std.testing.allocator;
    var compiled = try schema_mod.compile(a, "{\"joint_ie\":{\"entities\":{\"person\":{}},\"relations\":{\"z\":{\"head\":[\"person\"],\"tail\":[\"person\"]},\"a\":{\"head\":[\"person\"],\"tail\":[\"person\"]}}}}", .{});
    defer compiled.deinit();
    var nodes: [12]Node = undefined;
    for (&nodes, 0..) |*node, i| node.* = testNode((11 - i) * 10, @floatFromInt(i));
    var edges = [_]DecodedEdge{
        .{ .relation_type = 0, .head = 10, .tail = 0, .utility = 2, .probability = 0.6, .source_index = 8, .derived = false },
        .{ .relation_type = 0, .head = 2, .tail = 1, .utility = 0, .probability = 0.7, .source_index = null, .derived_from = 9, .derived = true },
        .{ .relation_type = 1, .head = 10, .tail = 0, .utility = 3, .probability = 0.8, .source_index = 10, .derived = false },
    };
    const original = edges;
    try sortSourcePresentation(compiled.schema.joint_ie.?, &nodes, &edges, null);
    try std.testing.expectEqualDeep(original[2], edges[0]);
    try std.testing.expectEqualDeep(original[1], edges[1]); // e10 sorts before e2.
    try std.testing.expectEqualDeep(original[0], edges[2]);
    var invalid = edges;
    invalid[0].head = nodes.len;
    try std.testing.expectError(error.InvalidJointCandidate, sortSourcePresentation(compiled.schema.joint_ie.?, &nodes, &invalid, null));
    try std.testing.expectEqualDeep(edges[1], invalid[1]);
}

test {
    _ = @import("extraction_joint_source_test.zig");
}
fn testEdge(head: usize, tail: usize, utility: f64) Edge {
    return .{ .relation_type = 0, .head = head, .tail = tail, .utility = utility, .probability = 0.8 };
}
fn testDecoded(edge: Edge, source: usize) DecodedEdge {
    return .{
        .relation_type = edge.relation_type,
        .head = edge.head,
        .tail = edge.tail,
        .utility = edge.utility,
        .probability = edge.probability,
        .source_index = source,
        .derived = false,
        .slot = edge.slot,
        .hypothesis = edge.hypothesis,
        .count_alternative = edge.count_alternative,
    };
}
const basic_schema = "{\"joint_ie\":{\"entities\":{\"person\":{}},\"relations\":{\"knows\":{\"head\":[\"person\"],\"tail\":[\"person\"]}}}}";

test "joint exact optimizer matches exhaustive acyclic graph selection" {
    const allocator = std.testing.allocator;
    var compiled = try schema_mod.compile(allocator, "{\"joint_ie\":{\"entities\":{\"person\":{}},\"relations\":{\"knows\":{\"head\":[\"person\"],\"tail\":[\"person\"]}},\"constraints\":[{\"type\":\"AcyclicRelation\",\"relation\":\"knows\"}]}}", .{});
    defer compiled.deinit();
    const schema = compiled.schema.joint_ie.?;
    const nodes = [_]Node{ testNode(0, 1), testNode(2, 1), testNode(4, 1) };
    const edges = [_]Edge{ testEdge(0, 1, 3), testEdge(1, 2, 2), testEdge(2, 0, 1) };
    var best: f64 = -std.math.inf(f64);
    for (0..8) |mask| {
        var candidate: [3]DecodedEdge = undefined;
        var count: usize = 0;
        var utility: f64 = 3;
        for (edges, 0..) |edge, i| if (mask & (@as(usize, 1) << @as(u6, @intCast(i))) != 0) {
            candidate[count] = testDecoded(edge, i);
            count += 1;
            utility += edge.utility;
        };
        if (try validateGlobal(allocator, schema, &nodes, candidate[0..count], .{})) best = @max(best, utility);
    }
    var result = try decode(allocator, schema, &nodes, &edges, .{ .algorithm = .exact });
    defer result.deinit();
    try std.testing.expectEqual(Status.optimal, result.status);
    try std.testing.expectEqual(best, result.utility);
    try std.testing.expectEqual(@as(f64, 8), result.utility);
    try std.testing.expectEqual(@as(usize, 2), result.edges.len);
    try std.testing.expect(try validateGlobal(allocator, schema, result.nodes, result.edges, .{}));
    var required = edges;
    for (&required) |*edge| edge.required = true;
    var impossible = try decode(allocator, schema, &nodes, &required, .{});
    defer impossible.deinit();
    try std.testing.expectEqual(Status.infeasible, impossible.status);
}

test "joint exact search shares rescued endpoint cost across negative atomic gains" {
    const allocator = std.testing.allocator;
    var compiled = try schema_mod.compile(allocator, basic_schema, .{});
    defer compiled.deinit();
    const nodes = [_]Node{ testNode(0, -3), testNode(2, -1), testNode(4, -1) };
    const edges = [_]Edge{ testEdge(0, 1, 3), testEdge(0, 2, 3) };
    var result = try decode(allocator, compiled.schema.joint_ie.?, &nodes, &edges, .{ .algorithm = .exact });
    defer result.deinit();
    try std.testing.expectEqual(Status.optimal, result.status);
    try std.testing.expectEqual(@as(f64, 1), result.utility);
    try std.testing.expectEqual(@as(usize, 3), result.nodes.len);
    try std.testing.expectEqual(@as(usize, 2), result.edges.len);
}

test "joint symmetric companions survive pair uniqueness and consume endpoint degrees" {
    const allocator = std.testing.allocator;
    var compiled = try schema_mod.compile(allocator, "{\"joint_ie\":{\"entities\":{\"person\":{}},\"relations\":{\"knows\":{\"head\":[\"person\"],\"tail\":[\"person\"],\"symmetric\":true,\"max_per_head\":1}}}}", .{});
    defer compiled.deinit();
    const nodes = [_]Node{ testNode(0, 0), testNode(2, 0), testNode(4, 0) };
    const edges = [_]Edge{ testEdge(0, 1, 3), testEdge(1, 0, 2), testEdge(1, 2, 1) };
    var result = try decode(allocator, compiled.schema.joint_ie.?, &nodes, &edges, .{});
    defer result.deinit();
    try std.testing.expectEqual(Status.optimal, result.status);
    try std.testing.expectEqual(@as(f64, 3), result.utility);
    try std.testing.expectEqual(@as(usize, 2), result.edges.len);
    try std.testing.expect(!result.edges[0].derived);
    try std.testing.expect(result.edges[1].derived);
    try std.testing.expectEqual(@as(f64, 0), result.edges[1].utility);
    try std.testing.expectEqual(@as(?u64, null), result.edges[1].slot);
    try std.testing.expect(!try validateGlobal(allocator, compiled.schema.joint_ie.?, result.nodes, result.edges[0..1], .{}));
    // A pair of invented derived edges cannot justify itself cyclically.
    const forged = try allocator.dupe(DecodedEdge, result.edges);
    defer allocator.free(forged);
    for (forged) |*edge| {
        edge.derived = true;
        edge.utility = 0;
        edge.source_index = null;
    }
    try std.testing.expect(!try validateGlobal(allocator, compiled.schema.joint_ie.?, result.nodes, forged, .{}));
}

test "joint inverse companions obey target relation limits and global merge validation" {
    const allocator = std.testing.allocator;
    var compiled = try schema_mod.compile(allocator, "{\"joint_ie\":{\"entities\":{\"person\":{}},\"relations\":{\"parent\":{\"head\":[\"person\"],\"tail\":[\"person\"],\"inverse\":\"child\"},\"child\":{\"head\":[\"person\"],\"tail\":[\"person\"],\"max_per_head\":1}}}}", .{});
    defer compiled.deinit();
    const nodes = [_]Node{ testNode(0, 0), testNode(2, 0), testNode(4, 0) };
    const edges = [_]Edge{ testEdge(0, 2, 3), testEdge(1, 2, 2) };
    var result = try decode(allocator, compiled.schema.joint_ie.?, &nodes, &edges, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(f64, 3), result.utility);
    try std.testing.expectEqual(@as(usize, 2), result.edges.len);
    try std.testing.expectEqual(@as(usize, 1), result.edges[1].relation_type);
    // Two independently valid windows can exceed the inverse head bound when
    // merged; the document-level validator must reject the combined graph.
    const merged = [_]DecodedEdge{
        testDecoded(edges[0], 0),                                                                                               testDecoded(edges[1], 1),
        .{ .relation_type = 1, .head = 2, .tail = 0, .utility = 0, .probability = 0.8, .source_index = null, .derived = true }, .{ .relation_type = 1, .head = 2, .tail = 1, .utility = 0, .probability = 0.8, .source_index = null, .derived = true },
    };
    try std.testing.expect(!try validateGlobal(allocator, compiled.schema.joint_ie.?, &nodes, &merged, .{}));
}

test "joint slots distinguish hypotheses counts and missing resources" {
    const allocator = std.testing.allocator;
    var compiled = try schema_mod.compile(allocator, basic_schema, .{});
    defer compiled.deinit();
    const nodes = [_]Node{ testNode(0, 0), testNode(2, 0), testNode(4, 0) };
    var edges = [_]Edge{ testEdge(0, 1, 3), testEdge(0, 2, 2) };
    inline for (0..4) |scenario| {
        for (&edges, 0..) |*edge, i| {
            edge.slot = if (scenario == 0) null else 0;
            edge.hypothesis = if (scenario == 2) i else if (scenario == 0) null else 0;
            edge.count_alternative = if (scenario == 3) i else null;
        }
        var result = try decode(allocator, compiled.schema.joint_ie.?, &nodes, &edges, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, if (scenario == 0 or scenario == 2) 2 else 1), result.edges.len);
    }
}

test "joint typed endpoints self loops overlap and bounded search never return invalid graphs" {
    const allocator = std.testing.allocator;
    var compiled = try schema_mod.compile(allocator, "{\"joint_ie\":{\"entities\":{\"person\":{},\"company\":{}},\"relations\":{\"works_for\":{\"head\":[\"person\"],\"tail\":[\"company\"]}}}}", .{});
    defer compiled.deinit();
    const nodes = [_]Node{ testNode(0, 1), .{ .entity_type = 1, .start = 2, .end = 3, .utility = 1, .probability = 0.8 }, testNode(0, 5) };
    // Duplicate typed spans are malformed, independently of graph selection.
    try std.testing.expectError(error.DuplicateJointNode, decode(allocator, compiled.schema.joint_ie.?, &nodes, &.{}, .{}));
    const distinct = nodes[0..2];
    const edges = [_]Edge{ testEdge(0, 1, 1), testEdge(1, 0, 50), testEdge(0, 0, 50) };
    var exact = try decode(allocator, compiled.schema.joint_ie.?, distinct, &edges, .{});
    defer exact.deinit();
    try std.testing.expectEqual(@as(usize, 1), exact.edges.len);
    var bounded = try decode(allocator, compiled.schema.joint_ie.?, distinct, &edges, .{ .algorithm = .exact, .exact_node_budget = 0 });
    defer bounded.deinit();
    try std.testing.expectEqual(Status.feasible, bounded.status);
    try std.testing.expect(bounded.exhausted);
    try std.testing.expect(try validateGlobal(allocator, compiled.schema.joint_ie.?, bounded.nodes, bounded.edges, .{}));
    var beam = try decode(allocator, compiled.schema.joint_ie.?, distinct, &edges, .{ .algorithm = .beam, .beam_width = 1 });
    defer beam.deinit();
    try std.testing.expect(beam.valid());
    try std.testing.expect(!beam.exhausted);
    try std.testing.expect(try validateGlobal(allocator, compiled.schema.joint_ie.?, beam.nodes, beam.edges, .{}));
    try std.testing.expectError(error.JointCandidateLimitExceeded, decode(allocator, compiled.schema.joint_ie.?, distinct, &edges, .{ .max_edges = 1 }));
    try std.testing.expectError(error.JointValidationLimitExceeded, decode(allocator, compiled.schema.joint_ie.?, distinct, &edges, .{ .max_validation_steps = 1 }));
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, decode(allocator, compiled.schema.joint_ie.?, distinct, &edges, .{ .control = .{ .check_fn = Cancel.check } }));
}

test "joint decoder allocation failures release results and scratch" {
    var compiled = try schema_mod.compile(std.testing.allocator, basic_schema, .{});
    defer compiled.deinit();
    const Check = struct {
        fn run(allocator: Allocator, schema: schema_mod.JointSchema) !void {
            const nodes = [_]Node{ testNode(0, -1), testNode(2, 1) };
            const edges = [_]Edge{testEdge(0, 1, 3)};
            var result = try decode(allocator, schema, &nodes, &edges, .{ .algorithm = .beam });
            defer result.deinit();
            try std.testing.expect(result.valid());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{compiled.schema.joint_ie.?});
}
