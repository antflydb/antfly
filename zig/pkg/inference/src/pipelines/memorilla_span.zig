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

//! Memorilla span copy half (docs/plans/memorilla-copy17-native.md 8.11/8.12;
//! `scripts/memorilla/qwen_span_copy.py`), pure host F32:
//!
//! 1. `questionPrefix` / `prefixDoubledTexts`: every self-context doubled
//!    document is encoded after `"Question: {q}\nPassage:"`
//!    (`question-passage-v1`); the kept tail ids must not change;
//! 2. `spanFeatures`: per document the second copy's own-token rows (special
//!    suffix rows dropped) of the final and layer-24 states, each
//!    L2-normalized, concatenated (`[n, 5120]`) and stored as FP16, aligned to
//!    the copy-source ids;
//! 3. `Head.forward`: projection + LayerNorm, document-segment and position
//!    embeddings, a learned no-answer row in front, two pre-norm transformer
//!    encoder layers (exact GELU), a final LayerNorm and start/end logits
//!    (index 0 = no answer), as PyTorch computes `SpanHead` at inference;
//! 4. `bestSpan`: the best in-document span of at most `max_span_tokens`;
//! 5. `evidenceWindow`: the exact source window around it (left-biased,
//!    word-aligned) whose ids are the record rows.
const std = @import("std");
const tok = @import("inference_tokenizer");
const qualified = @import("../models/memorilla_qualification.zig");
const bundle = @import("../models/memorilla.zig");
const safetensors = @import("../models/safetensors.zig");
const native = @import("../backends/native.zig");
const activations = @import("../backends/activations.zig");
const c_file = @import("../util/c_file.zig");
const copy = @import("memorilla_copy.zig");
const A = std.mem.Allocator;

pub const record_kind = "span-window";
pub const question_prefix_v1 = "question-passage-v1";
pub const weights_file_name = "span-head.safetensors";
/// Final-layer plus layer-24 encoder rows.
pub const feature_width: usize = 2 * qualified.width;
pub const layer_norm_eps: f32 = 1e-5;
pub const normalize_eps: f32 = 1e-12;
const protocol_max_bytes: usize = 256 * 1024;
const manifest_max_bytes: usize = 1024 * 1024;
const weights_max_bytes: usize = 512 * 1024 * 1024;

/// `always`: the window in every prompt. `learned`: open when the margin
/// clears `span_gate_offset` (plan 8.14). `routed`: the question routes to the
/// nearest profile centroid and opens when the margin clears that profile's
/// offset; with `fallback`, a question below its profile's familiarity floor
/// takes the `learned` offset instead (plans 8.16/8.17).
pub const GatePolicy = enum { always, learned, routed };
pub const routes_file_name = "span-gate-routes.safetensors";
pub const max_route_profiles: usize = 64;
pub const max_route_profile_bytes: usize = 64;
const routes_max_bytes: usize = 16 * 1024 * 1024;

/// Documents the narrow read takes (plan 8.27 R2): the first two of the
/// ranked candidate list.
pub const default_read_documents: usize = 2;
/// `span_read_cascade` bounds: the wide evidence window stays within the
/// decoder rows a span bundle may add (`span_window_tokens` has the same
/// bound); the read budget within one padded encoder batch.
pub const max_read_window_tokens: usize = 256;
pub const max_read_budget_tokens: usize = 16384;

/// `span_read_cascade` (plan 8.32, study F policy `replace@t`): with it,
/// a request's documents are its ranked candidate list. Every request reads
/// the narrow set first (the top `default_read_documents`, a
/// `span_window_tokens` window); when that record's margin is below
/// `threshold`, it also reads the wide set (the ranked list until
/// `read_tokens` encoder tokens, `readBudget`) with one `window_tokens`-row
/// window and uses that record instead. No benchmark labels are involved.
pub const ReadCascade = struct {
    /// Narrow margins at or above this keep the narrow record (the margin's
    /// own scale: `score - null_score - span_gate_threshold`).
    threshold: f64,
    read_tokens: usize,
    window_tokens: usize,
    /// Optional `floor`: narrow margins below it keep the narrow record too,
    /// so only `floor <= m_n < threshold` widens (a narrow read that failed
    /// preflight still goes wide). -inf (absent) is study F's C*.
    floor: f64 = -std.math.inf(f64),
    /// Optional `wide_gate_offset`: the gate offset for a wide record (it
    /// replaces `span_gate_offset`, the `learned` offset and the `routed`
    /// fallback offset); narrow records keep today's gate. Null (absent): the
    /// same offset as narrow records, C*.
    wide_gate_offset: ?f64 = null,
};

/// Per-request read of a cascade bundle: `narrow` (R2P1 always), `wide`
/// (R512P2 always) or `auto` (the cascade, the default).
pub const ReadMode = enum { narrow, wide, auto };
/// The two span passes a cascade request can run.
pub const ReadPass = enum { narrow, wide };

/// The span-window fields of a candidate protocol.
pub const Protocol = struct {
    window_tokens: usize,
    left_fraction: f64,
    max_span_tokens: usize,
    gate_threshold: f64,
    gate_policy: GatePolicy,
    width: usize,
    hidden: usize,
    layers: usize,
    heads: usize,
    max_positions: usize,
    max_documents: usize,
    /// `learned` opens at `margin >= gate_offset` (`span_gate_offset`).
    gate_offset: f64 = 0,
    /// Profiles in `span_gate_routes` (0: the bundle has no routes file).
    route_count: usize = 0,
    /// `span_gate_routes.fallback`: unfamiliar questions take `gate_offset`.
    route_fallback: bool = false,
    /// `span_read_cascade` (null: every request reads all its documents
    /// under `window_tokens`, as before).
    read_cascade: ?ReadCascade = null,

    /// The widest evidence window any request of this protocol can place.
    pub fn maxWindowTokens(self: *const Protocol) usize {
        const cascade = self.read_cascade orelse return self.window_tokens;
        return @max(self.window_tokens, cascade.window_tokens);
    }

    /// A request's read mode: null for a protocol without a cascade (which
    /// takes no mode), else the requested mode or `auto`.
    pub fn readMode(self: *const Protocol, requested: ?ReadMode) !?ReadMode {
        if (self.read_cascade == null) return if (requested == null) null else error.UnsupportedMemorillaReadMode;
        return requested orelse .auto;
    }
};

fn member(object: std.json.ObjectMap, name: []const u8) !std.json.Value {
    return object.get(name) orelse error.InvalidMemorillaSpanProtocol;
}

fn positive(value: std.json.Value, maximum: usize) !usize {
    if (value != .integer or value.integer < 1 or value.integer > maximum) return error.InvalidMemorillaSpanProtocol;
    return @intCast(value.integer);
}

fn number(value: std.json.Value) !f64 {
    return switch (value) {
        .float => |x| x,
        .integer => |x| @floatFromInt(x),
        else => error.InvalidMemorillaSpanProtocol,
    };
}

/// The span fields of `protocol.json`, or null for a copy17 (record kind
/// absent) protocol. `span_gate_policy` defaults to `always` (plan 8.11:
/// the window carries the binding context the decoder needs to abstain).
pub fn parseProtocol(a: A, bytes: []const u8) !?Protocol {
    var parsed = std.json.parseFromSlice(std.json.Value, a, bytes, .{}) catch return error.InvalidMemorillaSpanProtocol;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidMemorillaSpanProtocol;
    const object = parsed.value.object;
    const kind = object.get("record_kind") orelse return null;
    if (kind != .string or !std.mem.eql(u8, kind.string, record_kind)) return error.UnsupportedMemorillaProfile;
    const prefix = try member(object, "document_question_prefix");
    if (prefix != .string or !std.mem.eql(u8, prefix.string, question_prefix_v1)) return error.UnsupportedMemorillaProfile;
    const head = try member(object, "span_head");
    if (head != .object) return error.InvalidMemorillaSpanProtocol;
    const left = try number(try member(object, "span_window_left_fraction"));
    const threshold = try number(try member(object, "span_gate_threshold"));
    if (!(left >= 0 and left <= 1) or !std.math.isFinite(threshold)) return error.InvalidMemorillaSpanProtocol;
    var policy: GatePolicy = .always;
    if (object.get("span_gate_policy")) |value| {
        if (value != .string) return error.InvalidMemorillaSpanProtocol;
        policy = std.meta.stringToEnum(GatePolicy, value.string) orelse return error.InvalidMemorillaSpanProtocol;
    }
    var offset: f64 = 0;
    if (object.get("span_gate_offset")) |value| {
        offset = try number(value);
        if (!std.math.isFinite(offset)) return error.InvalidMemorillaSpanProtocol;
    }
    var route_count: usize = 0;
    var route_fallback = false;
    if (object.get("span_gate_routes")) |value| {
        var names = try routeNames(a, value);
        defer names.deinit(a);
        route_count = names.items.len;
        if (value.object.get("fallback")) |flag| {
            if (flag != .bool) return error.InvalidMemorillaSpanProtocol;
            route_fallback = flag.bool;
        }
    }
    if (policy == .routed and route_count == 0) return error.InvalidMemorillaSpanProtocol;
    // The per-route read budget (plan 8.27 D1) picked reads with a
    // nearest-centroid classifier over benchmark profiles; the read cascade
    // replaced it (plan 8.32). Refused rather than ignored, so such a bundle
    // never silently reads its whole candidate list.
    if (object.get("span_read_routes") != null) return error.UnsupportedMemorillaProfile;
    const read_cascade: ?ReadCascade = if (object.get("span_read_cascade")) |value| try parseReadCascade(value) else null;
    const out = Protocol{
        .gate_offset = offset,
        .route_count = route_count,
        .route_fallback = route_fallback,
        .read_cascade = read_cascade,
        .window_tokens = try positive(try member(object, "span_window_tokens"), 256),
        .left_fraction = left,
        .max_span_tokens = try positive(try member(object, "span_max_tokens"), 512),
        .gate_threshold = threshold,
        .gate_policy = policy,
        .width = try positive(try member(head.object, "width"), 1 << 16),
        .hidden = try positive(try member(head.object, "hidden"), 1 << 14),
        .layers = try positive(try member(head.object, "layers"), 16),
        .heads = try positive(try member(head.object, "heads"), 64),
        .max_positions = try positive(try member(head.object, "max_positions"), 1 << 16),
        .max_documents = try positive(try member(head.object, "max_documents"), 64),
    };
    if (out.width != feature_width or out.hidden % out.heads != 0) return error.UnsupportedMemorillaProfile;
    return out;
}

// --- routed gate (plans 8.16/8.17) -----------------------------------------------

/// `span_gate_routes.profiles`: 1-64 unique nonempty names of at most 64
/// bytes, borrowed from `value`. The object takes `profiles` and an optional
/// boolean `fallback`, nothing else.
fn routeNames(a: A, value: std.json.Value) !std.ArrayListUnmanaged([]const u8) {
    if (value != .object) return error.InvalidMemorillaSpanProtocol;
    var keys = value.object.iterator();
    while (keys.next()) |entry| {
        if (!std.mem.eql(u8, entry.key_ptr.*, "profiles") and !std.mem.eql(u8, entry.key_ptr.*, "fallback")) return error.InvalidMemorillaSpanProtocol;
    }
    const profiles = try member(value.object, "profiles");
    if (profiles != .array or profiles.array.items.len == 0 or profiles.array.items.len > max_route_profiles) return error.InvalidMemorillaSpanProtocol;
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer names.deinit(a);
    for (profiles.array.items) |item| {
        if (item != .string or item.string.len == 0 or item.string.len > max_route_profile_bytes) return error.InvalidMemorillaSpanProtocol;
        for (names.items) |seen| if (std.mem.eql(u8, seen, item.string)) return error.InvalidMemorillaSpanProtocol;
        try names.append(a, item.string);
    }
    return names;
}

fn exactInteger(value: std.json.Value, maximum: usize) !usize {
    if (value != .integer or value.integer < 1 or value.integer > maximum) return error.InvalidMemorillaSpanProtocol;
    return @intCast(value.integer);
}

fn finiteNumber(value: std.json.Value) !f64 {
    const x = try number(value);
    if (!std.math.isFinite(x)) return error.InvalidMemorillaSpanProtocol;
    return x;
}

/// `span_read_cascade` (`qwen_span_copy.span_read_cascade_config`):
/// `{"threshold": finite number, "read_tokens": 1..16384, "window_tokens":
/// 1..256}` (integers for the last two), optionally `"floor"` (finite, below
/// the threshold) and `"wide_gate_offset"` (finite); no other field.
fn parseReadCascade(value: std.json.Value) !ReadCascade {
    if (value != .object) return error.InvalidMemorillaSpanProtocol;
    var keys = value.object.iterator();
    while (keys.next()) |entry| {
        const known = for ([_][]const u8{ "threshold", "read_tokens", "window_tokens", "floor", "wide_gate_offset" }) |name| {
            if (std.mem.eql(u8, entry.key_ptr.*, name)) break true;
        } else false;
        if (!known) return error.InvalidMemorillaSpanProtocol;
    }
    var out = ReadCascade{
        .threshold = try finiteNumber(try member(value.object, "threshold")),
        .read_tokens = try exactInteger(try member(value.object, "read_tokens"), max_read_budget_tokens),
        .window_tokens = try exactInteger(try member(value.object, "window_tokens"), max_read_window_tokens),
    };
    if (value.object.get("floor")) |floor| {
        out.floor = try finiteNumber(floor);
        // A floor at or above the threshold would never widen.
        if (!(out.floor < out.threshold)) return error.InvalidMemorillaSpanProtocol;
    }
    if (value.object.get("wide_gate_offset")) |offset| out.wide_gate_offset = try finiteNumber(offset);
    return out;
}

/// One request's read set and window.
pub const ReadPlan = struct {
    /// Leading documents of the ranked candidate list that are read.
    documents: usize,
    /// Encoder tokens of the read documents when a budget applied (the sum
    /// the budget compared), else 0.
    budget_tokens: usize = 0,
    /// Evidence window rows.
    window_tokens: usize,
};

/// `read_budget_documents` over per-document token counts: the first
/// document always, then documents in order while the running total stays
/// within `budget`; reading stops at the first document that would exceed
/// it. `count` returns document `i`'s encoder tokens (no special tokens) and
/// is called only for the documents the rule inspects.
pub fn readBudget(context: anytype, comptime count: fn (@TypeOf(context), usize) anyerror!usize, documents: usize, budget: usize) !ReadPlan {
    if (budget == 0) return error.InvalidMemorillaReadBudget;
    if (documents == 0) return error.EmptyMemorillaCollection;
    var read: usize = 0;
    var total: usize = 0;
    while (read < documents) : (read += 1) {
        const tokens = try count(context, read);
        const next = std.math.add(usize, total, tokens) catch return error.MemorillaResourceOverflow;
        if (read > 0 and next > budget) break;
        total = next;
    }
    return .{ .documents = read, .budget_tokens = total, .window_tokens = 0 };
}

/// A protocol without a cascade reads every document it is given under
/// `span_window_tokens` (the path before read policies existed).
pub fn wholePlan(protocol: *const Protocol, documents: usize) !ReadPlan {
    if (documents == 0) return error.EmptyMemorillaCollection;
    return .{ .documents = documents, .window_tokens = protocol.window_tokens };
}

/// The narrow read (R2P1): the top `default_read_documents` candidates and
/// a `span_window_tokens` window.
pub fn narrowPlan(protocol: *const Protocol, documents: usize) !ReadPlan {
    if (documents == 0) return error.EmptyMemorillaCollection;
    return .{ .documents = @min(documents, default_read_documents), .window_tokens = protocol.window_tokens };
}

/// The wide read (R512P2 at the study F settings): `readBudget` over the
/// ranked candidates and the cascade's window.
pub fn widePlan(cascade: ReadCascade, context: anytype, comptime count: fn (@TypeOf(context), usize) anyerror!usize, documents: usize) !ReadPlan {
    var plan = try readBudget(context, count, documents, cascade.read_tokens);
    plan.window_tokens = cascade.window_tokens;
    return plan;
}

/// The first span pass of a cascade request, given which read sets passed
/// preflight: `narrow` and `wide` run their own pass (null: unavailable, the
/// request fails); `auto` starts narrow, or wide when the narrow set is
/// unavailable (study F counts a narrow record error as margin -inf).
pub fn firstPass(mode: ReadMode, narrow_available: bool, wide_available: bool) ?ReadPass {
    return switch (mode) {
        .narrow => if (narrow_available) .narrow else null,
        .wide => if (wide_available) .wide else null,
        .auto => if (narrow_available) .narrow else if (wide_available) .wide else null,
    };
}

/// Study F `replace@t`: after an `auto` request's narrow pass, the wide pass
/// runs (and its record replaces the narrow one) when the narrow margin is
/// below the threshold, not below the floor, and the wide set is available;
/// otherwise the narrow record stays. Other modes never run a second pass.
pub fn widens(cascade: ReadCascade, mode: ReadMode, narrow_margin: f64, wide_available: bool) bool {
    return mode == .auto and wide_available and !(narrow_margin >= cascade.threshold) and !(narrow_margin < cascade.floor);
}

/// The protocol the gate of a record of `pass` reads: a wide record of a
/// cascade with `wide_gate_offset` takes that offset as `gate_offset`;
/// every other record (and a bundle without a cascade, `pass` null) the
/// protocol unchanged.
pub fn gateProtocol(protocol: Protocol, pass: ?ReadPass) Protocol {
    const cascade = protocol.read_cascade orelse return protocol;
    const offset = cascade.wide_gate_offset orelse return protocol;
    if (pass != .wide) return protocol;
    var out = protocol;
    out.gate_offset = offset;
    return out;
}

/// The routed gate's table (`qwen_span_copy.py gate-routes`): one row per
/// profile, in `span_gate_routes.profiles` order.
pub const Routes = struct {
    allocator: A,
    names: [][]u8,
    dimension: usize,
    /// `[profiles, dimension]`, unit rows: the normalized mean question vector.
    centroids: []f32,
    /// Margin offset per profile: +inf never opens, -inf always opens.
    offsets: []f32,
    /// Familiarity floor per profile (cosine to its centroid); -inf never
    /// falls back.
    floors: []f32,
    /// sha256 of `span-gate-routes.safetensors`.
    sha256: [64]u8,

    pub const Route = struct { index: usize, similarity: f32 };

    pub fn deinit(self: *Routes) void {
        for (self.names) |name| self.allocator.free(name);
        self.allocator.free(self.names);
        self.allocator.free(self.centroids);
        self.allocator.free(self.offsets);
        self.allocator.free(self.floors);
        self.* = undefined;
    }

    pub fn index(self: *const Routes, name: []const u8) ?usize {
        for (self.names, 0..) |known, i| if (std.mem.eql(u8, known, name)) return i;
        return null;
    }

    /// Nearest centroid of the L2-normalized question (`F.normalize`, eps
    /// 1e-12); the first maximum wins ties, as `argmax`.
    pub fn route(self: *const Routes, question: []const f32) !Route {
        if (question.len != self.dimension) return error.InvalidMemorillaGateQuestion;
        var squares: f64 = 0;
        for (question) |x| {
            if (!std.math.isFinite(x)) return error.InvalidMemorillaGateQuestion;
            squares += @as(f64, x) * x;
        }
        const scale: f32 = @floatCast(1.0 / @max(@sqrt(squares), normalize_eps));
        var best = Route{ .index = 0, .similarity = -std.math.inf(f32) };
        for (0..self.names.len) |p| {
            const row = self.centroids[p * self.dimension ..][0..self.dimension];
            var dot: f32 = 0;
            for (row, question) |c, x| dot += c * (x * scale);
            if (dot > best.similarity) best = .{ .index = p, .similarity = dot };
        }
        return best;
    }

    fn tensor(a: A, reader: *const safetensors.MMapReader, name: []const u8, shape: []const i64) ![]f32 {
        var t = reader.readTensor(name) catch |err| return if (err == error.TensorNotFound) error.InvalidMemorillaGateRoutes else err;
        defer t.deinit();
        if (t.dtype != .f32 or !std.mem.eql(i64, t.shape, shape)) return error.InvalidMemorillaGateRoutes;
        const count = t.data.len / @sizeOf(f32);
        if (count * @sizeOf(f32) != t.data.len) return error.InvalidMemorillaGateRoutes;
        const out = try a.alloc(f32, count);
        @memcpy(std.mem.sliceAsBytes(out), t.data[0 .. count * @sizeOf(f32)]);
        return out;
    }

    /// Parses the table for `names` (copied) from safetensors `bytes`:
    /// exactly `centroids` `[P, dimension]` (finite unit rows), `offsets`
    /// `[P]` (no NaN) and `floors` `[P]` (finite or -inf).
    pub fn fromBytes(a: A, bytes: []const u8, names: []const []const u8, dimension: usize) !Routes {
        if (names.len == 0 or names.len > max_route_profiles or dimension == 0) return error.InvalidMemorillaGateRoutes;
        var reader = try safetensors.MMapReader.fromBorrowedBytesLimited(a, bytes, 1024 * 1024);
        defer reader.deinit();
        try safetensors.validateReader(a, &reader);
        if (reader.header.tensors.count() != 3) return error.InvalidMemorillaGateRoutes;
        const p: i64 = @intCast(names.len);
        const centroids = try tensor(a, &reader, "centroids", &.{ p, @intCast(dimension) });
        errdefer a.free(centroids);
        const offsets = try tensor(a, &reader, "offsets", &.{p});
        errdefer a.free(offsets);
        const floors = try tensor(a, &reader, "floors", &.{p});
        errdefer a.free(floors);
        for (0..names.len) |i| {
            var squares: f64 = 0;
            for (centroids[i * dimension ..][0..dimension]) |x| {
                if (!std.math.isFinite(x)) return error.InvalidMemorillaGateRoutes;
                squares += @as(f64, x) * x;
            }
            if (@abs(@sqrt(squares) - 1) > 1e-3) return error.InvalidMemorillaGateRoutes;
            if (std.math.isNan(offsets[i])) return error.InvalidMemorillaGateRoutes;
            if (!(std.math.isFinite(floors[i]) or floors[i] == -std.math.inf(f32))) return error.InvalidMemorillaGateRoutes;
        }
        const owned = try a.alloc([]u8, names.len);
        var made: usize = 0;
        errdefer {
            for (owned[0..made]) |name| a.free(name);
            a.free(owned);
        }
        for (names, owned) |name, *slot| {
            slot.* = try a.dupe(u8, name);
            made += 1;
        }
        return .{ .allocator = a, .names = owned, .dimension = dimension, .centroids = centroids, .offsets = offsets, .floors = floors, .sha256 = bundle.digest(bytes) };
    }

    /// Opens the pinned `span-gate-routes.safetensors` of a hybrid bundle with
    /// the profile names of its pinned `protocol.json` (both sha256 and size
    /// checked against `manifest.json`).
    pub fn loadBundle(a: A, directory: []const u8, protocol: Protocol) !Routes {
        if (protocol.route_count == 0) return error.MemorillaGateProfileUnavailable;
        const manifest_path = try std.fs.path.join(a, &.{ directory, "manifest.json" });
        defer a.free(manifest_path);
        const manifest_bytes = try c_file.readFileMax(a, manifest_path, manifest_max_bytes);
        defer a.free(manifest_bytes);
        var manifest = std.json.parseFromSlice(std.json.Value, a, manifest_bytes, .{}) catch return error.InvalidMemorillaManifest;
        defer manifest.deinit();
        const protocol_path = try std.fs.path.join(a, &.{ directory, "protocol.json" });
        defer a.free(protocol_path);
        const protocol_bytes = try c_file.readFileMax(a, protocol_path, protocol_max_bytes);
        defer a.free(protocol_bytes);
        try bundle.verifyFile(manifest.value, "protocol.json", protocol_bytes);
        var parsed = std.json.parseFromSlice(std.json.Value, a, protocol_bytes, .{}) catch return error.InvalidMemorillaSpanProtocol;
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidMemorillaSpanProtocol;
        var names = try routeNames(a, try member(parsed.value.object, "span_gate_routes"));
        defer names.deinit(a);
        if (names.items.len != protocol.route_count) return error.InvalidMemorillaSpanProtocol;
        const path = try std.fs.path.join(a, &.{ directory, routes_file_name });
        defer a.free(path);
        const bytes = try c_file.readFileMax(a, path, routes_max_bytes);
        defer a.free(bytes);
        try bundle.verifyFile(manifest.value, routes_file_name, bytes);
        return fromBytes(a, bytes, names.items, qualified.width);
    }
};

pub const GateDecision = struct {
    open: bool,
    /// The offset the margin was compared with.
    offset: f64,
    /// Routed or declared profile (row of the routes table).
    profile: ?usize = null,
    /// Cosine of the question to the routed centroid (routed only).
    similarity: f32 = 0,
    /// The routed question was below its profile's floor and took the
    /// protocol's `learned` offset.
    fallback: bool = false,
};

/// `qwen_span_copy.span_gate_decision`: the gate for one request. A declared
/// profile (a caller that knows its task type) takes that profile's offset
/// under any policy; otherwise `policy` decides. The window opens when
/// `margin >= offset`.
pub fn decideGate(policy: GatePolicy, protocol: Protocol, routes: ?*const Routes, question: []const f32, margin: f64, declared: ?[]const u8) !GateDecision {
    if (std.math.isNan(margin)) return error.InvalidMemorillaSpanMargin;
    if (declared) |name| {
        const table = routes orelse return error.MemorillaGateProfileUnavailable;
        const i = table.index(name) orelse return error.UnknownMemorillaGateProfile;
        const offset: f64 = table.offsets[i];
        return .{ .open = margin >= offset, .offset = offset, .profile = i };
    }
    switch (policy) {
        .always => return .{ .open = true, .offset = -std.math.inf(f64) },
        .learned => return .{ .open = margin >= protocol.gate_offset, .offset = protocol.gate_offset },
        .routed => {
            const table = routes orelse return error.MemorillaGateProfileUnavailable;
            const r = try table.route(question);
            const fallback = protocol.route_fallback and r.similarity < table.floors[r.index];
            const offset: f64 = if (fallback) protocol.gate_offset else table.offsets[r.index];
            return .{ .open = margin >= offset, .offset = offset, .profile = r.index, .similarity = r.similarity, .fallback = fallback };
        },
    }
}

// --- question prefix -------------------------------------------------------------

/// `question_prefix_text(q, "question-passage-v1")`: Python
/// `" ".join(q.split())` inside `"Question: {q}\nPassage:"`.
pub fn questionPrefix(a: A, question: []const u8) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(a);
    try out.appendSlice(a, "Question: ");
    const view = try std.unicode.Utf8View.init(question);
    var it = view.iterator();
    var pending_space = false;
    var wrote = false;
    while (it.nextCodepointSlice()) |slice| {
        const cp = std.unicode.utf8Decode(slice) catch unreachable;
        if (qualified.pythonWhitespace(cp)) {
            pending_space = wrote;
            continue;
        }
        if (pending_space) try out.append(a, ' ');
        pending_space = false;
        wrote = true;
        try out.appendSlice(a, slice);
    }
    try out.appendSlice(a, "\nPassage:");
    return out.toOwnedSlice(a);
}

/// `question_prefixed_documents`: `prefix ++ text` for every doubled text,
/// refusing a prefix that changes a text's last `counts[i]` ids.
pub fn prefixDoubledTexts(a: A, tokenizer: tok.Tokenizer, texts: []const []const u8, counts: []const usize, prefix: []const u8) ![][]u8 {
    if (texts.len != counts.len) return error.InvalidMemorillaDocumentCount;
    const out = try a.alloc([]u8, texts.len);
    var built: usize = 0;
    errdefer {
        for (out[0..built]) |text| a.free(text);
        a.free(out);
    }
    for (texts, counts, out) |text, count, *slot| {
        const plain = try tokenizer.encode(a, text);
        defer a.free(plain);
        const joined = try std.mem.concat(a, u8, &.{ prefix, text });
        errdefer a.free(joined);
        const full = try tokenizer.encode(a, joined);
        defer a.free(full);
        if (count == 0 or count > plain.len or count > full.len or
            !std.mem.eql(i32, full[full.len - count ..], plain[plain.len - count ..])) return error.MemorillaQuestionPrefixMismatch;
        slot.* = joined;
        built += 1;
    }
    return out;
}

// --- features ----------------------------------------------------------------------

fn normalizeInto(out: []f32, row: []const f32) !void {
    var sum: f64 = 0;
    for (row) |value| {
        if (!std.math.isFinite(value)) return error.NonFiniteMemorillaFeature;
        sum += @as(f64, value) * value;
    }
    const norm: f32 = @floatCast(@sqrt(sum));
    if (norm == 0) return error.ZeroMemorillaFeature;
    const denominator = @max(norm, normalize_eps);
    for (out, row) |*o, value| o.* = @as(f32, @as(f16, @floatCast(value / denominator)));
}

/// `span_features` over the two `[batch, seq_len, width]` host state tensors
/// of one encoder run: rows `0..counts.len` are the prefixed documents (each
/// ending in `suffix` special rows), any later rows are ignored. Returns
/// `[sum(counts), 2 * width]`: per document token, the L2-normalized final
/// row then the L2-normalized tap row, each rounded to FP16.
pub fn spanFeatures(a: A, final: []const f32, tap: []const f32, batch: usize, seq_len: usize, width: usize, active: []const usize, counts: []const usize, suffix: usize) ![]f32 {
    if (final.len != tap.len or final.len != batch * seq_len * width or active.len != batch or counts.len > batch or counts.len == 0)
        return error.InvalidMemorillaStates;
    var total: usize = 0;
    for (counts) |count| total = try std.math.add(usize, total, count);
    const out = try a.alloc(f32, try std.math.mul(usize, total, 2 * width));
    errdefer a.free(out);
    var cursor: usize = 0;
    for (counts, active[0..counts.len], 0..) |count, length, row| {
        if (count == 0 or length > seq_len or count + suffix > length) return error.InvalidMemorillaStates;
        const first = length - count - suffix;
        for (0..count) |offset| {
            const source = (row * seq_len + first + offset) * width;
            const target = out[cursor * 2 * width ..][0 .. 2 * width];
            try normalizeInto(target[0..width], final[source..][0..width]);
            try normalizeInto(target[width..], tap[source..][0..width]);
            cursor += 1;
        }
    }
    std.debug.assert(cursor == total);
    return out;
}

// --- head ------------------------------------------------------------------------

const Block = struct {
    in_w: []f32,
    in_b: []f32,
    out_w: []f32,
    out_b: []f32,
    l1_w: []f32,
    l1_b: []f32,
    l2_w: []f32,
    l2_b: []f32,
    n1_w: []f32,
    n1_b: []f32,
    n2_w: []f32,
    n2_b: []f32,
};

pub const Head = struct {
    allocator: A,
    width: usize,
    hidden: usize,
    heads: usize,
    max_positions: usize,
    max_documents: usize,
    project_w: []f32,
    project_b: []f32,
    project_ln_w: []f32,
    project_ln_b: []f32,
    segment: []f32,
    position: []f32,
    null_row: []f32,
    blocks: []Block,
    norm_w: []f32,
    norm_b: []f32,
    out_w: []f32,
    out_b: []f32,
    /// sha256 of `span-head.safetensors`.
    sha256: [64]u8,
    /// Owned tensor storage (every slice above points into it).
    storage: std.ArrayListUnmanaged([]f32) = .empty,

    pub fn deinit(self: *Head) void {
        for (self.storage.items) |values| self.allocator.free(values);
        self.storage.deinit(self.allocator);
        self.allocator.free(self.blocks);
        self.* = undefined;
    }

    fn read(self: *Head, reader: *const safetensors.MMapReader, name: []const u8, shape: []const i64) ![]f32 {
        var tensor = reader.readTensor(name) catch |err| return if (err == error.TensorNotFound) error.MissingSpanHeadTensor else err;
        defer tensor.deinit();
        if (tensor.dtype != .f32 or !std.mem.eql(i64, tensor.shape, shape)) return error.InvalidSpanHeadTensor;
        const count = tensor.data.len / @sizeOf(f32);
        if (count * @sizeOf(f32) != tensor.data.len) return error.InvalidSpanHeadTensor;
        const out = try self.allocator.alloc(f32, count);
        errdefer self.allocator.free(out);
        @memcpy(std.mem.sliceAsBytes(out), tensor.data[0 .. count * @sizeOf(f32)]);
        for (out) |value| if (!std.math.isFinite(value)) return error.InvalidSpanHeadTensor;
        try self.storage.append(self.allocator, out);
        return out;
    }

    /// Parses a span head from safetensors `bytes` (`state_dict` of
    /// `qwen_span_copy.SpanHead`) for `protocol`'s geometry.
    pub fn fromBytes(a: A, bytes: []const u8, protocol: Protocol) !Head {
        var reader = try safetensors.MMapReader.fromBorrowedBytesLimited(a, bytes, 1024 * 1024);
        defer reader.deinit();
        try safetensors.validateReader(a, &reader);
        const w: i64 = @intCast(protocol.width);
        const h: i64 = @intCast(protocol.hidden);
        var head = Head{
            .allocator = a,
            .width = protocol.width,
            .hidden = protocol.hidden,
            .heads = protocol.heads,
            .max_positions = protocol.max_positions,
            .max_documents = protocol.max_documents,
            .project_w = &.{},
            .project_b = &.{},
            .project_ln_w = &.{},
            .project_ln_b = &.{},
            .segment = &.{},
            .position = &.{},
            .null_row = &.{},
            .blocks = try a.alloc(Block, protocol.layers),
            .norm_w = &.{},
            .norm_b = &.{},
            .out_w = &.{},
            .out_b = &.{},
            .sha256 = bundle.digest(bytes),
        };
        errdefer head.deinit();
        head.project_w = try head.read(&reader, "project.0.weight", &.{ h, w });
        head.project_b = try head.read(&reader, "project.0.bias", &.{h});
        head.project_ln_w = try head.read(&reader, "project.1.weight", &.{h});
        head.project_ln_b = try head.read(&reader, "project.1.bias", &.{h});
        head.segment = try head.read(&reader, "segment.weight", &.{ @intCast(protocol.max_documents), h });
        head.position = try head.read(&reader, "position.weight", &.{ @intCast(protocol.max_positions + 1), h });
        head.null_row = try head.read(&reader, "null", &.{ 1, 1, h });
        var buf: [96]u8 = undefined;
        for (head.blocks, 0..) |*block, i| {
            const p = "encoder.layers.";
            block.in_w = try head.read(&reader, try std.fmt.bufPrint(&buf, "{s}{d}.self_attn.in_proj_weight", .{ p, i }), &.{ 3 * h, h });
            block.in_b = try head.read(&reader, try std.fmt.bufPrint(&buf, "{s}{d}.self_attn.in_proj_bias", .{ p, i }), &.{3 * h});
            block.out_w = try head.read(&reader, try std.fmt.bufPrint(&buf, "{s}{d}.self_attn.out_proj.weight", .{ p, i }), &.{ h, h });
            block.out_b = try head.read(&reader, try std.fmt.bufPrint(&buf, "{s}{d}.self_attn.out_proj.bias", .{ p, i }), &.{h});
            block.l1_w = try head.read(&reader, try std.fmt.bufPrint(&buf, "{s}{d}.linear1.weight", .{ p, i }), &.{ 4 * h, h });
            block.l1_b = try head.read(&reader, try std.fmt.bufPrint(&buf, "{s}{d}.linear1.bias", .{ p, i }), &.{4 * h});
            block.l2_w = try head.read(&reader, try std.fmt.bufPrint(&buf, "{s}{d}.linear2.weight", .{ p, i }), &.{ h, 4 * h });
            block.l2_b = try head.read(&reader, try std.fmt.bufPrint(&buf, "{s}{d}.linear2.bias", .{ p, i }), &.{h});
            block.n1_w = try head.read(&reader, try std.fmt.bufPrint(&buf, "{s}{d}.norm1.weight", .{ p, i }), &.{h});
            block.n1_b = try head.read(&reader, try std.fmt.bufPrint(&buf, "{s}{d}.norm1.bias", .{ p, i }), &.{h});
            block.n2_w = try head.read(&reader, try std.fmt.bufPrint(&buf, "{s}{d}.norm2.weight", .{ p, i }), &.{h});
            block.n2_b = try head.read(&reader, try std.fmt.bufPrint(&buf, "{s}{d}.norm2.bias", .{ p, i }), &.{h});
        }
        head.norm_w = try head.read(&reader, "norm.weight", &.{h});
        head.norm_b = try head.read(&reader, "norm.bias", &.{h});
        head.out_w = try head.read(&reader, "out.weight", &.{ 2, h });
        head.out_b = try head.read(&reader, "out.bias", &.{2});
        // Exactly the expected tensors: a renamed or extra tensor is a
        // different head.
        if (reader.header.tensors.count() != 11 + 12 * protocol.layers) return error.InvalidSpanHeadTensor;
        return head;
    }

    /// Opens the pinned `span-head.safetensors` of a hybrid bundle (its
    /// sha256 and size must match `manifest.json`).
    pub fn loadBundle(a: A, directory: []const u8, protocol: Protocol) !Head {
        const manifest_path = try std.fs.path.join(a, &.{ directory, "manifest.json" });
        defer a.free(manifest_path);
        const manifest_bytes = try c_file.readFileMax(a, manifest_path, manifest_max_bytes);
        defer a.free(manifest_bytes);
        var manifest = std.json.parseFromSlice(std.json.Value, a, manifest_bytes, .{}) catch return error.InvalidMemorillaManifest;
        defer manifest.deinit();
        const path = try std.fs.path.join(a, &.{ directory, weights_file_name });
        defer a.free(path);
        const bytes = try c_file.readFileMax(a, path, weights_max_bytes);
        defer a.free(bytes);
        try bundle.verifyFile(manifest.value, weights_file_name, bytes);
        return fromBytes(a, bytes, protocol);
    }

    /// Host bytes of the loaded weights (admission of the per-request load).
    pub fn weightBytes(protocol: Protocol) usize {
        const h = protocol.hidden;
        const per_layer = 3 * h * h + 3 * h + h * h + h + 8 * h * h + 4 * h + h + 4 * h;
        return @sizeOf(f32) * (h * protocol.width + 4 * h + protocol.max_documents * h + (protocol.max_positions + 1) * h + h +
            protocol.layers * per_layer + 2 * h + 2 * h + 2);
    }

    /// Host scratch of one forward over `rows` document tokens.
    pub fn forwardBytes(protocol: Protocol, rows: usize) usize {
        const n = rows + 1;
        const h = protocol.hidden;
        return @sizeOf(f32) * (rows * h + n * (3 * h + 4 * h + 4 * h) + n * n + 3 * n * (h / protocol.heads) + 4 * n);
    }
};

/// Start/end logits `[T + 1]` (index 0 = no answer).
pub const Logits = struct {
    allocator: A,
    start: []f32,
    end: []f32,

    pub fn deinit(self: *Logits) void {
        self.allocator.free(self.start);
        self.allocator.free(self.end);
        self.* = undefined;
    }
};

fn sgemmTransB(io: ?std.Io, m: usize, n: usize, k: usize, alpha: f32, x: []const f32, w: []const f32, out: []f32) !void {
    if (io) |runtime| try native.sgemmTransB(runtime, m, n, k, alpha, x, w, 0, out) else native.sgemmTransBSync(m, n, k, alpha, x, w, 0, out);
}

fn sgemm(io: ?std.Io, m: usize, n: usize, k: usize, x: []const f32, y: []const f32, out: []f32) !void {
    if (io) |runtime| try native.sgemm(runtime, m, n, k, 1, x, y, 0, out) else native.sgemmSync(m, n, k, 1, x, y, 0, out);
}

/// `y = x W^T + b` for row-major `x [rows, in]`, `W [out, in]`.
fn linear(io: ?std.Io, x: []const f32, rows: usize, in_dim: usize, w: []const f32, b: []const f32, out: []f32) !void {
    try sgemmTransB(io, rows, b.len, in_dim, 1, x, w, out);
    for (0..rows) |row| for (out[row * b.len ..][0..b.len], b) |*o, bias| {
        o.* += bias;
    };
}

/// `nn.LayerNorm` (biased variance, eps 1e-5) of each `width`-row of `x`
/// into `out` (may alias `x`).
fn layerNorm(x: []const f32, out: []f32, width: usize, weight: []const f32, bias: []const f32) void {
    var row: usize = 0;
    while (row * width < x.len) : (row += 1) {
        const input = x[row * width ..][0..width];
        var mean: f64 = 0;
        for (input) |v| mean += v;
        mean /= @floatFromInt(width);
        var variance: f64 = 0;
        for (input) |v| variance += (v - mean) * (v - mean);
        variance /= @floatFromInt(width);
        const inv: f32 = @floatCast(1.0 / @sqrt(variance + layer_norm_eps));
        const m: f32 = @floatCast(mean);
        const target = out[row * width ..][0..width];
        for (target, input, weight, bias) |*o, v, g, b| o.* = (v - m) * inv * g + b;
    }
}

fn softmaxRow(row: []f32) void {
    var maximum: f32 = -std.math.inf(f32);
    for (row) |v| maximum = @max(maximum, v);
    var sum: f64 = 0;
    for (row) |*v| {
        v.* = @exp(v.* - maximum);
        sum += v.*;
    }
    const inv: f32 = @floatCast(1.0 / sum);
    for (row) |*v| v.* *= inv;
}

/// One forward over `features [rows, width]` whose documents hold `counts`
/// tokens each (in order). `io` parallelizes the GEMMs (null: process pool).
pub fn forward(a: A, io: ?std.Io, head: *const Head, features: []const f32, counts: []const usize) !Logits {
    var rows: usize = 0;
    for (counts) |count| rows = try std.math.add(usize, rows, count);
    if (rows == 0 or features.len != rows * head.width) return error.InvalidMemorillaStates;
    const h = head.hidden;
    const n = rows + 1;
    const x = try a.alloc(f32, n * h);
    defer a.free(x);
    // Row 0: the no-answer row plus position 0; rows 1..: projected tokens
    // plus their document segment and position (both clamped to the tables).
    try linear(io, features, rows, head.width, head.project_w, head.project_b, x[h..]);
    layerNorm(x[h..], x[h..], h, head.project_ln_w, head.project_ln_b);
    for (x[0..h], head.null_row, head.position[0..h]) |*o, z, p| o.* = z + p;
    {
        var row: usize = 1;
        for (counts, 0..) |count, document| {
            const segment = head.segment[@min(document, head.max_documents - 1) * h ..][0..h];
            for (0..count) |_| {
                const position = head.position[@min(row, head.max_positions) * h ..][0..h];
                for (x[row * h ..][0..h], segment, position) |*o, s, p| o.* += s + p;
                row += 1;
            }
        }
    }
    const normed = try a.alloc(f32, n * h);
    defer a.free(normed);
    const qkv = try a.alloc(f32, n * 3 * h);
    defer a.free(qkv);
    const attended = try a.alloc(f32, n * h);
    defer a.free(attended);
    const projected = try a.alloc(f32, n * h);
    defer a.free(projected);
    const hidden = try a.alloc(f32, n * 4 * h);
    defer a.free(hidden);
    const dh = h / head.heads;
    const scores = try a.alloc(f32, n * n);
    defer a.free(scores);
    const q = try a.alloc(f32, n * dh);
    defer a.free(q);
    const k = try a.alloc(f32, n * dh);
    defer a.free(k);
    const v = try a.alloc(f32, n * dh);
    defer a.free(v);
    const o = try a.alloc(f32, n * dh);
    defer a.free(o);
    const scale: f32 = @floatCast(1.0 / @sqrt(@as(f64, @floatFromInt(dh))));
    for (head.blocks) |block| {
        // x += SelfAttention(LN1 x)
        layerNorm(x, normed, h, block.n1_w, block.n1_b);
        try linear(io, normed, n, h, block.in_w, block.in_b, qkv);
        for (0..head.heads) |index| {
            for (0..n) |row| {
                const base = row * 3 * h + index * dh;
                @memcpy(q[row * dh ..][0..dh], qkv[base..][0..dh]);
                @memcpy(k[row * dh ..][0..dh], qkv[base + h ..][0..dh]);
                @memcpy(v[row * dh ..][0..dh], qkv[base + 2 * h ..][0..dh]);
            }
            try sgemmTransB(io, n, n, dh, scale, q, k, scores);
            for (0..n) |row| softmaxRow(scores[row * n ..][0..n]);
            try sgemm(io, n, dh, n, scores, v, o);
            for (0..n) |row| @memcpy(attended[row * h + index * dh ..][0..dh], o[row * dh ..][0..dh]);
        }
        try linear(io, attended, n, h, block.out_w, block.out_b, projected);
        for (x, projected) |*value, delta| value.* += delta;
        // x += W2 gelu(W1 LN2 x)
        layerNorm(x, normed, h, block.n2_w, block.n2_b);
        try linear(io, normed, n, h, block.l1_w, block.l1_b, hidden);
        activations.geluExact(hidden);
        try linear(io, hidden, n, 4 * h, block.l2_w, block.l2_b, projected);
        for (x, projected) |*value, delta| value.* += delta;
    }
    layerNorm(x, normed, h, head.norm_w, head.norm_b);
    const logits = try a.alloc(f32, n * 2);
    defer a.free(logits);
    try linear(io, normed, n, h, head.out_w, head.out_b, logits);
    const start = try a.alloc(f32, n);
    errdefer a.free(start);
    const end = try a.alloc(f32, n);
    errdefer a.free(end);
    for (0..n) |row| {
        start[row] = logits[row * 2];
        end[row] = logits[row * 2 + 1];
        if (!std.math.isFinite(start[row]) or !std.math.isFinite(end[row])) return error.NonFiniteMemorillaFeature;
    }
    return .{ .allocator = a, .start = start, .end = end };
}

// --- decoding --------------------------------------------------------------------

pub const Bound = struct { lo: usize, hi: usize };

/// Per-document `[lo, hi)` ranges of the concatenated ids.
pub fn docBounds(a: A, counts: []const usize) ![]Bound {
    const out = try a.alloc(Bound, counts.len);
    var offset: usize = 0;
    for (counts, out) |count, *bound| {
        if (count == 0) {
            a.free(out);
            return error.InvalidMemorillaStates;
        }
        bound.* = .{ .lo = offset, .hi = offset + count };
        offset += count;
    }
    return out;
}

pub const Span = struct {
    /// Inclusive positions in the concatenated ids.
    start: usize,
    end: usize,
    score: f64,
    null_score: f64,
};

/// `best_span`: the best inclusive in-document span of at most `max_len`
/// tokens (ties keep the first in row-major order, earlier documents first)
/// and the no-answer score, both as F32 sums widened to F64.
pub fn bestSpan(start: []const f32, end: []const f32, bounds: []const Bound, max_len: usize) !Span {
    if (start.len != end.len or start.len == 0 or max_len == 0 or bounds.len == 0) return error.InvalidMemorillaStates;
    const s = start[1..];
    const e = end[1..];
    var best: ?Span = null;
    var best_value: f32 = -std.math.inf(f32);
    for (bounds) |bound| {
        if (bound.hi > s.len or bound.lo >= bound.hi) return error.InvalidMemorillaStates;
        var doc_best: f32 = -std.math.inf(f32);
        var doc_i: usize = bound.lo;
        var doc_j: usize = bound.lo;
        for (bound.lo..bound.hi) |i| {
            const last = @min(bound.hi, i + max_len);
            for (i..last) |j| {
                const value = s[i] + e[j];
                if (value > doc_best) {
                    doc_best = value;
                    doc_i = i;
                    doc_j = j;
                }
            }
        }
        if (best == null or doc_best > best_value) {
            best_value = doc_best;
            best = .{ .start = doc_i, .end = doc_j, .score = doc_best, .null_score = 0 };
        }
    }
    var out = best.?;
    out.null_score = @as(f32, start[0] + end[0]);
    return out;
}

/// Python 3 `round(x)` for the window budget: nearest, ties to even.
pub fn roundHalfEven(x: f64) i64 {
    const floor = @floor(x);
    const diff = x - floor;
    var r: f64 = if (diff > 0.5) floor + 1 else if (diff < 0.5) floor else if (@mod(floor, 2) == 0) floor else floor + 1;
    if (r == -0.0) r = 0;
    return @intFromFloat(r);
}

pub const Window = struct { lo: usize, hi: usize };

/// `evidence_window`: `[lo, hi)` around the inclusive span inside its
/// document, `width` tokens at most, about `left_fraction` of the free
/// budget before the span (unused room on one side goes to the other), then
/// trimmed so it neither starts nor ends inside a word (`continues[i]`: the
/// id at position i continues the word before it). A span longer than
/// `width` keeps its first `width` tokens.
pub fn evidenceWindow(continues: []const bool, start: usize, end: usize, bounds: []const Bound, width: usize, left_fraction: f64) !Window {
    if (!(left_fraction >= 0 and left_fraction <= 1) or width == 0 or end < start) return error.InvalidMemorillaStates;
    const bound = for (bounds) |b| {
        if (b.lo <= start and start < b.hi) break b;
    } else return error.InvalidMemorillaStates;
    if (end >= bound.hi or bound.hi > continues.len) return error.InvalidMemorillaStates;
    if (end - start + 1 >= width) return .{ .lo = start, .hi = start + width };
    const free = width - (end - start + 1);
    const wanted: usize = @intCast(@max(roundHalfEven(@as(f64, @floatFromInt(free)) * left_fraction), 0));
    var left = @min(wanted, start - bound.lo);
    const right = @min(free - left, bound.hi - end - 1);
    left = @min(free - right, start - bound.lo);
    var lo = start - left;
    var hi = end + 1 + right;
    while (lo < start and continues[lo]) lo += 1;
    while (hi > end + 1 and hi < bound.hi and continues[hi]) hi -= 1;
    return .{ .lo = lo, .hi = hi };
}

/// `window_ids`: the window's ids padded with `pad_id` to `width`.
pub fn windowIds(a: A, ids: []const u32, window: Window, width: usize, pad_id: i32) ![]i32 {
    if (window.hi < window.lo or window.hi > ids.len) return error.InvalidMemorillaStates;
    const out = try a.alloc(i32, width);
    @memset(out, pad_id);
    const taken = @min(window.hi - window.lo, width);
    for (ids[window.lo..][0..taken], out[0..taken]) |id, *o| o.* = @intCast(id);
    return out;
}

/// `token_continues_word` for every copy id (decoder pieces).
pub fn continuationFlags(a: A, tokenizer: tok.Tokenizer, ids: []const u32) ![]bool {
    const out = try a.alloc(bool, ids.len);
    errdefer a.free(out);
    for (ids, out) |id, *flag| {
        if (id > std.math.maxInt(i32)) return error.InvalidMemorillaTokens;
        flag.* = try copy.tokenContinuesWord(a, tokenizer, @intCast(id));
    }
    return out;
}

/// The window as one citable span (`copy.buildCitations` input).
pub fn windowSpan(window: Window) copy.CompletedSpan {
    return .{
        .span = .{ .begin = 0, .end = window.hi - window.lo, .start = window.lo },
        .first = window.lo,
        .last = window.hi,
        .slot_begin = 0,
        .slot_end = window.hi - window.lo,
    };
}

// --- tests -----------------------------------------------------------------------

test "Memorilla span window is left biased stays in its document and trims words" {
    const a = std.testing.allocator;
    const bounds = try docBounds(a, &.{ 40, 40 });
    defer a.free(bounds);
    var none: [80]bool = @splat(false);
    var w = try evidenceWindow(&none, 20, 21, bounds, 12, 0.65);
    try std.testing.expect(w.lo <= 20 and w.hi >= 22 and w.hi - w.lo == 12);
    try std.testing.expect(20 - w.lo > w.hi - 22);
    w = try evidenceWindow(&none, 41, 41, bounds, 12, 0.65);
    try std.testing.expectEqual(Window{ .lo = 40, .hi = 52 }, w);
    w = try evidenceWindow(&none, 38, 39, bounds, 12, 0.65);
    try std.testing.expect(w.hi == 40 and w.hi - w.lo == 12);
    try std.testing.expectError(error.InvalidMemorillaStates, evidenceWindow(&none, 39, 40, bounds, 12, 0.65));
    try std.testing.expectEqual(Window{ .lo = 2, .hi = 10 }, try evidenceWindow(&none, 2, 30, bounds, 8, 0.5));
    // Python test_evidence_window_trims_partial_words.
    const single = try docBounds(a, &.{30});
    defer a.free(single);
    var flags: [30]bool = @splat(false);
    flags[7] = true;
    flags[8] = true;
    flags[25] = true;
    w = try evidenceWindow(&flags, 15, 15, single, 18, 0.5);
    try std.testing.expectEqual(Window{ .lo = 9, .hi = 24 }, w);
    const ids = [_]u32{ 0, 1, 2, 3, 4, 5, 6 };
    const padded = try windowIds(a, &ids, .{ .lo = 3, .hi = 6 }, 5, 99);
    defer a.free(padded);
    try std.testing.expectEqualSlices(i32, &.{ 3, 4, 5, 99, 99 }, padded);
}

test "Memorilla span rounding follows Python round half to even" {
    try std.testing.expectEqual(@as(i64, 2), roundHalfEven(2.5));
    try std.testing.expectEqual(@as(i64, 4), roundHalfEven(3.5));
    try std.testing.expectEqual(@as(i64, 7), roundHalfEven(6.5000000001));
    try std.testing.expectEqual(@as(i64, 0), roundHalfEven(0.4));
    // 22 * 0.65 and 21 * 0.65 as F64.
    try std.testing.expectEqual(@as(i64, 14), roundHalfEven(22.0 * 0.65));
    try std.testing.expectEqual(@as(i64, 14), roundHalfEven(21.0 * 0.65));
}

test "Memorilla span best span respects documents length and ties" {
    const a = std.testing.allocator;
    const bounds = try docBounds(a, &.{ 3, 3 });
    defer a.free(bounds);
    const start = [_]f32{ 0, 0, 5, 0, 0, 0, 0 };
    const end = [_]f32{ 0, 0, 0, 0, 6, 0, 0 };
    const got = try bestSpan(&start, &end, bounds, 10);
    try std.testing.expect(got.start <= got.end);
    try std.testing.expect((got.start < 3 and got.end < 3) or (got.start >= 3 and got.end >= 3));
    try std.testing.expectEqual(@as(f64, 0), got.null_score);
    const one = try docBounds(a, &.{4});
    defer a.free(one);
    const s2 = [_]f32{ 3, 0, 4, 0, 0 };
    const e2 = [_]f32{ 3, 0, 0, 0, 4 };
    const limited = try bestSpan(&s2, &e2, one, 2);
    try std.testing.expect(limited.end - limited.start + 1 <= 2);
    try std.testing.expectEqual(@as(f64, 6), limited.null_score);
    // Ties keep the first span in row-major order: (0, 0).
    const three = try docBounds(a, &.{3});
    defer a.free(three);
    const flat = [_]f32{ 0, 1, 1, 1 };
    const tie = try bestSpan(&flat, &flat, three, 3);
    try std.testing.expectEqual(@as(usize, 0), tie.start);
    try std.testing.expectEqual(@as(usize, 0), tie.end);
}

test "Memorilla span question prefix collapses Python whitespace" {
    const a = std.testing.allocator;
    const prefix = try questionPrefix(a, "Who was  the Norse\nleader?\u{3000}");
    defer a.free(prefix);
    try std.testing.expectEqualStrings("Question: Who was the Norse leader?\nPassage:", prefix);
    const empty = try questionPrefix(a, "  ");
    defer a.free(empty);
    try std.testing.expectEqualStrings("Question: \nPassage:", empty);
}

test "Memorilla span protocol parses span fields and rejects others" {
    const a = std.testing.allocator;
    try std.testing.expect((try parseProtocol(a, "{\"system_prompt\":\"x\"}")) == null);
    const json =
        \\{"record_kind":"span-window","document_question_prefix":"question-passage-v1","span_head":{"width":5120,"hidden":1024,"layers":2,"heads":8,"dropout":0.1,"max_positions":2048,"max_documents":4},"span_window_tokens":32,"span_window_left_fraction":0.65,"span_max_tokens":30,"span_gate_threshold":-3.828125}
    ;
    const p = (try parseProtocol(a, json)).?;
    try std.testing.expectEqual(@as(usize, 32), p.window_tokens);
    try std.testing.expectEqual(GatePolicy.always, p.gate_policy);
    try std.testing.expectEqual(@as(f64, -3.828125), p.gate_threshold);
    try std.testing.expectError(error.UnsupportedMemorillaProfile, parseProtocol(a, "{\"record_kind\":\"other\"}"));
    try std.testing.expect(Head.weightBytes(p) > 4 * 1024 * 5120);
}

/// A tiny head with deterministic weights (the Python reference values in
/// the forward test come from the same formula).
fn tinyHead(a: A) !Head {
    const protocol = Protocol{ .window_tokens = 4, .left_fraction = 0.5, .max_span_tokens = 3, .gate_threshold = 0, .gate_policy = .always, .width = 6, .hidden = 4, .layers = 1, .heads = 2, .max_positions = 3, .max_documents = 2 };
    var head = Head{
        .allocator = a,
        .width = 6,
        .hidden = 4,
        .heads = 2,
        .max_positions = 3,
        .max_documents = 2,
        .project_w = &.{},
        .project_b = &.{},
        .project_ln_w = &.{},
        .project_ln_b = &.{},
        .segment = &.{},
        .position = &.{},
        .null_row = &.{},
        .blocks = try a.alloc(Block, 1),
        .norm_w = &.{},
        .norm_b = &.{},
        .out_w = &.{},
        .out_b = &.{},
        .sha256 = @splat('0'),
    };
    errdefer head.deinit();
    var counter: usize = 0;
    const fill = struct {
        fn make(h: *Head, n: usize, c: *usize) ![]f32 {
            const values = try h.allocator.alloc(f32, n);
            errdefer h.allocator.free(values);
            for (values) |*v| {
                c.* += 1;
                v.* = @as(f32, @floatFromInt(@as(i64, @intCast((c.* * 37) % 23)) - 11)) / 17.0;
            }
            try h.storage.append(h.allocator, values);
            return values;
        }
    }.make;
    head.project_w = try fill(&head, 4 * 6, &counter);
    head.project_b = try fill(&head, 4, &counter);
    head.project_ln_w = try fill(&head, 4, &counter);
    head.project_ln_b = try fill(&head, 4, &counter);
    head.segment = try fill(&head, 2 * 4, &counter);
    head.position = try fill(&head, 4 * 4, &counter);
    head.null_row = try fill(&head, 4, &counter);
    const b = &head.blocks[0];
    b.in_w = try fill(&head, 12 * 4, &counter);
    b.in_b = try fill(&head, 12, &counter);
    b.out_w = try fill(&head, 4 * 4, &counter);
    b.out_b = try fill(&head, 4, &counter);
    b.l1_w = try fill(&head, 16 * 4, &counter);
    b.l1_b = try fill(&head, 16, &counter);
    b.l2_w = try fill(&head, 4 * 16, &counter);
    b.l2_b = try fill(&head, 4, &counter);
    b.n1_w = try fill(&head, 4, &counter);
    b.n1_b = try fill(&head, 4, &counter);
    b.n2_w = try fill(&head, 4, &counter);
    b.n2_b = try fill(&head, 4, &counter);
    head.norm_w = try fill(&head, 4, &counter);
    head.norm_b = try fill(&head, 4, &counter);
    head.out_w = try fill(&head, 8, &counter);
    head.out_b = try fill(&head, 2, &counter);
    _ = protocol;
    return head;
}

test "Memorilla span head forward matches the PyTorch reference on a tiny head" {
    const a = std.testing.allocator;
    var head = try tinyHead(a);
    defer head.deinit();
    // Three document tokens in two documents plus one beyond the position
    // table (clamped), features i/7 - 0.4.
    var features: [4 * 6]f32 = undefined;
    for (&features, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i % 7)) / 7.0 - 0.4;
    var logits = try forward(a, null, &head, &features, &.{ 2, 2 });
    defer logits.deinit();
    // PyTorch reference: qwen_span_copy.SpanHead(width=6, hidden=4, layers=1,
    // heads=2, max_positions=3, max_documents=2) in eval mode, F32, with
    // these weights loaded in state_dict order.
    const want_start = [_]f32{ 0.51132, 0.40065, 0.63478, 0.43044, 0.32965 };
    const want_end = [_]f32{ 0.14552, 0.21032, 0.07271, 0.20211, 0.22415 };
    for (logits.start, want_start) |got, want| try std.testing.expectApproxEqAbs(want, got, 2e-4);
    for (logits.end, want_end) |got, want| try std.testing.expectApproxEqAbs(want, got, 2e-4);
}

const span_protocol_base =
    \\"record_kind":"span-window","document_question_prefix":"question-passage-v1","span_head":{"width":5120,"hidden":1024,"layers":2,"heads":8,"max_positions":2048,"max_documents":4},"span_window_tokens":32,"span_window_left_fraction":0.65,"span_max_tokens":30,"span_gate_threshold":-3.8
;

test "Memorilla span protocol parses the gate offset and routes" {
    const a = std.testing.allocator;
    const routed = (try parseProtocol(a, "{" ++ span_protocol_base ++
        \\,"span_gate_policy":"routed","span_gate_offset":-3,"span_gate_routes":{"profiles":["qa","persona"],"fallback":true}}
    )).?;
    try std.testing.expectEqual(GatePolicy.routed, routed.gate_policy);
    try std.testing.expectEqual(@as(f64, -3), routed.gate_offset);
    try std.testing.expectEqual(@as(usize, 2), routed.route_count);
    try std.testing.expect(routed.route_fallback);
    // Learned without an offset keeps the 8.11 behaviour (open at margin 0).
    const learned = (try parseProtocol(a, "{" ++ span_protocol_base ++ ",\"span_gate_policy\":\"learned\"}")).?;
    try std.testing.expectEqual(@as(f64, 0), learned.gate_offset);
    try std.testing.expectEqual(@as(usize, 0), learned.route_count);
    const bad = [_][]const u8{
        // routed needs routes
        "{" ++ span_protocol_base ++ ",\"span_gate_policy\":\"routed\"}",
        // duplicate, empty, or too many names; unknown key; non-bool fallback
        "{" ++ span_protocol_base ++ ",\"span_gate_routes\":{\"profiles\":[\"qa\",\"qa\"]}}",
        "{" ++ span_protocol_base ++ ",\"span_gate_routes\":{\"profiles\":[\"\"]}}",
        "{" ++ span_protocol_base ++ ",\"span_gate_routes\":{\"profiles\":[]}}",
        "{" ++ span_protocol_base ++ ",\"span_gate_routes\":{\"profiles\":[\"qa\"],\"floor\":0.5}}",
        "{" ++ span_protocol_base ++ ",\"span_gate_routes\":{\"profiles\":[\"qa\"],\"fallback\":1}}",
        "{" ++ span_protocol_base ++ ",\"span_gate_offset\":\"-3\"}",
    };
    for (bad) |json| try std.testing.expectError(error.InvalidMemorillaSpanProtocol, parseProtocol(a, json));
}

test "Memorilla span protocol parses the read cascade and refuses read routes" {
    const a = std.testing.allocator;
    const routes =
        \\,"span_gate_policy":"learned","span_gate_offset":-3,"span_gate_routes":{"profiles":["triviaqa","narrativeqa","factkg"]}
    ;
    // Study F's C* (replace@-4, R512P2), with or without the gate routes.
    const cascade = (try parseProtocol(a, "{" ++ span_protocol_base ++ routes ++
        \\,"span_read_cascade":{"window_tokens":64,"threshold":-4.0,"read_tokens":512}}
    )).?;
    try std.testing.expectEqual(ReadCascade{ .threshold = -4, .read_tokens = 512, .window_tokens = 64 }, cascade.read_cascade.?);
    try std.testing.expectEqual(@as(usize, 64), cascade.maxWindowTokens());
    try std.testing.expectEqual(GatePolicy.learned, cascade.gate_policy);
    const bare = (try parseProtocol(a, "{" ++ span_protocol_base ++
        \\,"span_read_cascade":{"threshold":-4,"read_tokens":512,"window_tokens":16}}
    )).?;
    try std.testing.expectEqual(@as(f64, -4), bare.read_cascade.?.threshold);
    // A window narrower than the narrow read's keeps the narrow bound.
    try std.testing.expectEqual(@as(usize, 32), bare.maxWindowTokens());
    // A bundle without the field keeps today's behaviour.
    const plain = (try parseProtocol(a, "{" ++ span_protocol_base ++ routes ++ "}")).?;
    try std.testing.expectEqual(@as(?ReadCascade, null), plain.read_cascade);
    try std.testing.expectEqual(@as(usize, 32), plain.maxWindowTokens());
    // Bounds are inclusive; the threshold is any finite number.
    const edge = (try parseProtocol(a, "{" ++ span_protocol_base ++
        \\,"span_read_cascade":{"threshold":12.5,"read_tokens":16384,"window_tokens":256}}
    )).?;
    try std.testing.expectEqual(ReadCascade{ .threshold = 12.5, .read_tokens = 16384, .window_tokens = 256 }, edge.read_cascade.?);
    const low = (try parseProtocol(a, "{" ++ span_protocol_base ++
        \\,"span_read_cascade":{"threshold":-100,"read_tokens":1,"window_tokens":1}}
    )).?;
    try std.testing.expectEqual(ReadCascade{ .threshold = -100, .read_tokens = 1, .window_tokens = 1 }, low.read_cascade.?);
    const bad = [_][]const u8{
        // not an object, empty, missing or extra fields
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":[]}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":-4}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":512}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":512,\"window\":64}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":512,\"window_tokens\":64,\"mode\":\"replace\"}}",
        // threshold: string, null, nonfinite literal
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":\"-4\",\"read_tokens\":512,\"window_tokens\":64}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":null,\"read_tokens\":512,\"window_tokens\":64}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":1e999,\"read_tokens\":512,\"window_tokens\":64}}",
        // nonpositive, fractional, string or oversized budgets and windows
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":0,\"window_tokens\":64}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":512,\"window_tokens\":0}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":-512,\"window_tokens\":64}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":512.5,\"window_tokens\":64}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":\"512\",\"window_tokens\":64}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":512,\"window_tokens\":257}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":16385,\"window_tokens\":64}}",
    };
    for (bad) |json| std.testing.expectError(error.InvalidMemorillaSpanProtocol, parseProtocol(a, json)) catch |err| {
        std.debug.print("accepted: {s}\n", .{json});
        return err;
    };
    // The per-route read budget (benchmark centroid routing) is refused,
    // alone or next to a cascade.
    try std.testing.expectError(error.UnsupportedMemorillaProfile, parseProtocol(a, "{" ++ span_protocol_base ++ routes ++
        \\,"span_read_routes":{"triviaqa":{"read_tokens":512,"window_tokens":64}}}
    ));
    try std.testing.expectError(error.UnsupportedMemorillaProfile, parseProtocol(a, "{" ++ span_protocol_base ++ routes ++
        \\,"span_read_routes":{"triviaqa":{"read_tokens":512,"window_tokens":64}},"span_read_cascade":{"threshold":-4,"read_tokens":512,"window_tokens":64}}
    ));
}

test "Memorilla span cascade floor and wide gate offset: parsing, boundaries, C* defaults" {
    const a = std.testing.allocator;
    const inf = std.math.inf(f64);
    // Absent fields are study F's C*: floor -inf, no wide gate offset.
    const cstar = (try parseProtocol(a, "{" ++ span_protocol_base ++
        \\,"span_gate_policy":"learned","span_gate_offset":-3,"span_read_cascade":{"threshold":-4,"read_tokens":512,"window_tokens":64}}
    )).?;
    try std.testing.expectEqual(-inf, cstar.read_cascade.?.floor);
    try std.testing.expectEqual(@as(?f64, null), cstar.read_cascade.?.wide_gate_offset);
    const g = (try parseProtocol(a, "{" ++ span_protocol_base ++
        \\,"span_gate_policy":"learned","span_gate_offset":-3,"span_read_cascade":{"wide_gate_offset":-5.5,"floor":-8,"threshold":-4,"read_tokens":512,"window_tokens":64}}
    )).?;
    try std.testing.expectEqual(ReadCascade{ .threshold = -4, .read_tokens = 512, .window_tokens = 64, .floor = -8, .wide_gate_offset = -5.5 }, g.read_cascade.?);
    const bad = [_][]const u8{
        // floor at or above the threshold, non-numbers, nonfinite, unknown keys
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":512,\"window_tokens\":64,\"floor\":-4}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":512,\"window_tokens\":64,\"floor\":-3}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":512,\"window_tokens\":64,\"floor\":\"-8\"}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":512,\"window_tokens\":64,\"floor\":null}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":512,\"window_tokens\":64,\"floor\":-1e999}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":512,\"window_tokens\":64,\"wide_gate_offset\":\"-5\"}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":512,\"window_tokens\":64,\"wide_gate_offset\":1e999}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":512,\"window_tokens\":64,\"wide_gate_offset\":true}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"threshold\":-4,\"read_tokens\":512,\"window_tokens\":64,\"floor\":-8,\"ceiling\":0}}",
        "{" ++ span_protocol_base ++ ",\"span_read_cascade\":{\"read_tokens\":512,\"window_tokens\":64,\"floor\":-8}}",
    };
    for (bad) |json| std.testing.expectError(error.InvalidMemorillaSpanProtocol, parseProtocol(a, json)) catch |err| {
        std.debug.print("accepted: {s}\n", .{json});
        return err;
    };

    // Default floor: widening is exactly C*'s `!(m >= t)` on every margin.
    const c = cstar.read_cascade.?;
    for ([_]f64{ -inf, -1000, -8.0001, -8, -7.9999, -4.0001, -4, -3.9999, 0, 9, inf, std.math.nan(f64) }) |m|
        try std.testing.expectEqual(!(m >= -4), widens(c, .auto, m, true));
    // Floor boundaries (floor -8, threshold -4): floor <= m < threshold widens.
    const f = g.read_cascade.?;
    try std.testing.expect(widens(f, .auto, -8, true));
    try std.testing.expect(widens(f, .auto, -7.9999, true));
    try std.testing.expect(widens(f, .auto, -4.0001, true));
    try std.testing.expect(!widens(f, .auto, -8.0001, true));
    try std.testing.expect(!widens(f, .auto, -100, true));
    try std.testing.expect(!widens(f, .auto, -inf, true));
    try std.testing.expect(!widens(f, .auto, -4, true));
    try std.testing.expect(!widens(f, .auto, 3, true));
    try std.testing.expect(!widens(f, .auto, -6, false));
    try std.testing.expect(!widens(f, .wide, -6, true));
    // A narrow read that failed preflight still goes wide, whatever the floor.
    try std.testing.expectEqual(@as(?ReadPass, .wide), firstPass(.auto, false, true));

    // Gate: without `wide_gate_offset` every record keeps the protocol (C*).
    for ([_]?ReadPass{ null, .narrow, .wide }) |pass| try std.testing.expectEqual(@as(f64, -3), gateProtocol(cstar, pass).gate_offset);
    // With it, only wide records take it; narrow records keep today's gate.
    try std.testing.expectEqual(@as(f64, -5.5), gateProtocol(g, .wide).gate_offset);
    try std.testing.expectEqual(@as(f64, -3), gateProtocol(g, .narrow).gate_offset);
    try std.testing.expectEqual(@as(f64, -3), gateProtocol(g, null).gate_offset);
    // learned: a wide record opens at margin >= -5.5 (boundary included), a
    // narrow one still at >= -3.
    try std.testing.expect((try decideGate(.learned, gateProtocol(g, .wide), null, &.{}, -5.5, null)).open);
    try std.testing.expect(!(try decideGate(.learned, gateProtocol(g, .wide), null, &.{}, -5.5001, null)).open);
    try std.testing.expect(!(try decideGate(.learned, gateProtocol(g, .narrow), null, &.{}, -4, null)).open);
    try std.testing.expect((try decideGate(.learned, gateProtocol(g, .narrow), null, &.{}, -3, null)).open);
    try std.testing.expect(!(try decideGate(.learned, gateProtocol(cstar, .wide), null, &.{}, -4, null)).open);
    // `always` stays open for any record.
    try std.testing.expect((try decideGate(.always, gateProtocol(g, .wide), null, &.{}, -50, null)).open);
    // A bundle without a cascade is untouched.
    const plain = (try parseProtocol(a, "{" ++ span_protocol_base ++ ",\"span_gate_policy\":\"learned\",\"span_gate_offset\":-3}")).?;
    try std.testing.expectEqual(@as(f64, -3), gateProtocol(plain, .wide).gate_offset);
}

test "Memorilla span read modes: no mode without a cascade, auto by default" {
    var protocol = Protocol{ .window_tokens = 32, .left_fraction = 0.65, .max_span_tokens = 30, .gate_threshold = -3.8, .gate_policy = .learned, .width = feature_width, .hidden = 1024, .layers = 2, .heads = 8, .max_positions = 2048, .max_documents = 4 };
    try std.testing.expectEqual(@as(?ReadMode, null), try protocol.readMode(null));
    for ([_]ReadMode{ .narrow, .wide, .auto }) |mode| try std.testing.expectError(error.UnsupportedMemorillaReadMode, protocol.readMode(mode));
    protocol.read_cascade = .{ .threshold = -4, .read_tokens = 512, .window_tokens = 64 };
    try std.testing.expectEqual(@as(?ReadMode, .auto), try protocol.readMode(null));
    for ([_]ReadMode{ .narrow, .wide, .auto }) |mode| try std.testing.expectEqual(@as(?ReadMode, mode), try protocol.readMode(mode));
}

test "Memorilla span cascade widens below the threshold only" {
    const cascade = ReadCascade{ .threshold = -4, .read_tokens = 512, .window_tokens = 64 };
    const inf = std.math.inf(f64);
    // auto: below the threshold widens; at or above keeps the narrow record.
    try std.testing.expect(widens(cascade, .auto, -4.0001, true));
    try std.testing.expect(widens(cascade, .auto, -12, true));
    try std.testing.expect(widens(cascade, .auto, -inf, true));
    try std.testing.expect(!widens(cascade, .auto, -4, true));
    try std.testing.expect(!widens(cascade, .auto, -3.9999, true));
    try std.testing.expect(!widens(cascade, .auto, 7, true));
    try std.testing.expect(!widens(cascade, .auto, inf, true));
    // A NaN margin is not >= t, as in the Python simulation.
    try std.testing.expect(widens(cascade, .auto, std.math.nan(f64), true));
    // The wide set unavailable (e.g. past the encoder bound): keep narrow.
    try std.testing.expect(!widens(cascade, .auto, -12, false));
    // Forced modes never run a second pass.
    for ([_]ReadMode{ .narrow, .wide }) |mode| {
        try std.testing.expect(!widens(cascade, mode, -12, true));
        try std.testing.expect(!widens(cascade, mode, 3, true));
    }
    // First passes.
    try std.testing.expectEqual(@as(?ReadPass, .narrow), firstPass(.auto, true, true));
    try std.testing.expectEqual(@as(?ReadPass, .narrow), firstPass(.auto, true, false));
    try std.testing.expectEqual(@as(?ReadPass, .wide), firstPass(.auto, false, true));
    try std.testing.expectEqual(@as(?ReadPass, null), firstPass(.auto, false, false));
    try std.testing.expectEqual(@as(?ReadPass, .narrow), firstPass(.narrow, true, true));
    try std.testing.expectEqual(@as(?ReadPass, .narrow), firstPass(.narrow, true, false));
    try std.testing.expectEqual(@as(?ReadPass, null), firstPass(.narrow, false, true));
    try std.testing.expectEqual(@as(?ReadPass, .wide), firstPass(.wide, true, true));
    try std.testing.expectEqual(@as(?ReadPass, .wide), firstPass(.wide, false, true));
    try std.testing.expectEqual(@as(?ReadPass, null), firstPass(.wide, true, false));
}

const TestCounts = struct {
    counts: []const usize,
    asked: *usize,
    fn count(self: TestCounts, i: usize) anyerror!usize {
        self.asked.* = @max(self.asked.*, i + 1);
        return self.counts[i];
    }
};

test "Memorilla span read plans follow the Python read budget vectors" {
    var asked: usize = 0;
    // `test_read_budget_reads_ranked_documents_until_the_budget` (one token
    // per character).
    const docs = [_]usize{ 300, 150, 100, 10 };
    const c = TestCounts{ .counts = &docs, .asked = &asked };
    try std.testing.expectEqual(ReadPlan{ .documents = 2, .budget_tokens = 450, .window_tokens = 0 }, try readBudget(c, TestCounts.count, 4, 512));
    try std.testing.expectEqual(@as(usize, 3), asked); // stops at the first document past the budget
    try std.testing.expectEqual(@as(usize, 3), (try readBudget(c, TestCounts.count, 4, 555)).documents);
    try std.testing.expectEqual(ReadPlan{ .documents = 4, .budget_tokens = 560, .window_tokens = 0 }, try readBudget(c, TestCounts.count, 4, 560));
    try std.testing.expectEqual(@as(usize, 4), (try readBudget(c, TestCounts.count, 4, 10_000)).documents);
    // The top document is always read, even past the budget, and nothing
    // after a document that does not fit is read.
    const long = TestCounts{ .counts = &.{ 900, 1 }, .asked = &asked };
    try std.testing.expectEqual(ReadPlan{ .documents = 1, .budget_tokens = 900, .window_tokens = 0 }, try readBudget(long, TestCounts.count, 2, 512));
    const gap = TestCounts{ .counts = &.{ 100, 500, 10 }, .asked = &asked };
    try std.testing.expectEqual(@as(usize, 1), (try readBudget(gap, TestCounts.count, 3, 512)).documents);
    try std.testing.expectError(error.InvalidMemorillaReadBudget, readBudget(c, TestCounts.count, 4, 0));
    try std.testing.expectError(error.EmptyMemorillaCollection, readBudget(c, TestCounts.count, 0, 512));

    // Plans: every document without a cascade; the narrow read takes the
    // top two; the wide read the budget and the cascade's window.
    const protocol = Protocol{ .window_tokens = 32, .left_fraction = 0.65, .max_span_tokens = 30, .gate_threshold = -3.8, .gate_policy = .learned, .width = feature_width, .hidden = 1024, .layers = 2, .heads = 8, .max_positions = 2048, .max_documents = 4 };
    try std.testing.expectEqual(ReadPlan{ .documents = 4, .window_tokens = 32 }, try wholePlan(&protocol, 4));
    try std.testing.expectEqual(ReadPlan{ .documents = 2, .window_tokens = 32 }, try narrowPlan(&protocol, 4));
    try std.testing.expectEqual(ReadPlan{ .documents = 1, .window_tokens = 32 }, try narrowPlan(&protocol, 1));
    const cascade = ReadCascade{ .threshold = -4, .read_tokens = 512, .window_tokens = 64 };
    try std.testing.expectEqual(ReadPlan{ .documents = 2, .budget_tokens = 450, .window_tokens = 64 }, try widePlan(cascade, c, TestCounts.count, 4));
    try std.testing.expectEqual(ReadPlan{ .documents = 1, .budget_tokens = 900, .window_tokens = 64 }, try widePlan(cascade, long, TestCounts.count, 2));
    try std.testing.expectError(error.EmptyMemorillaCollection, wholePlan(&protocol, 0));
    try std.testing.expectError(error.EmptyMemorillaCollection, narrowPlan(&protocol, 0));
    try std.testing.expectError(error.EmptyMemorillaCollection, widePlan(cascade, c, TestCounts.count, 0));
}

fn routesBytes(a: A, payload: []const f32, profiles: usize, dimension: usize) ![]u8 {
    var header: std.ArrayListUnmanaged(u8) = .empty;
    defer header.deinit(a);
    const c = profiles * dimension * 4;
    try header.print(a, "{{\"centroids\":{{\"dtype\":\"F32\",\"shape\":[{d},{d}],\"data_offsets\":[0,{d}]}},\"offsets\":{{\"dtype\":\"F32\",\"shape\":[{d}],\"data_offsets\":[{d},{d}]}},\"floors\":{{\"dtype\":\"F32\",\"shape\":[{d}],\"data_offsets\":[{d},{d}]}}}}", .{ profiles, dimension, c, profiles, c, c + 4 * profiles, profiles, c + 4 * profiles, c + 8 * profiles });
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(a);
    var size: [8]u8 = undefined;
    std.mem.writeInt(u64, &size, header.items.len, .little);
    try out.appendSlice(a, &size);
    try out.appendSlice(a, header.items);
    try out.appendSlice(a, std.mem.sliceAsBytes(payload));
    return out.toOwnedSlice(a);
}

test "Memorilla span routes load and route like the evaluator" {
    const a = std.testing.allocator;
    const inf = std.math.inf(f32);
    // Three profiles in 2-D: qa (1,0) opens always, persona (0,1) never,
    // facts (0.6,0.8) at margin -6; floors 0.9 / -inf / 0.5.
    const payload = [_]f32{ 1, 0, 0, 1, 0.6, 0.8, -inf, inf, -6, 0.9, -inf, 0.5 };
    const bytes = try routesBytes(a, &payload, 3, 2);
    defer a.free(bytes);
    var routes = try Routes.fromBytes(a, bytes, &.{ "qa", "persona", "facts" }, 2);
    defer routes.deinit();
    try std.testing.expectEqual(@as(?usize, 2), routes.index("facts"));
    try std.testing.expectEqual(@as(?usize, null), routes.index("other"));
    const protocol = Protocol{ .window_tokens = 32, .left_fraction = 0.65, .max_span_tokens = 30, .gate_threshold = -3.8, .gate_policy = .routed, .width = feature_width, .hidden = 1024, .layers = 2, .heads = 8, .max_positions = 2048, .max_documents = 4, .gate_offset = -3, .route_count = 3, .route_fallback = true };

    // Unnormalized question near qa: routes there, opens at any margin.
    var d = try decideGate(.routed, protocol, &routes, &.{ 5, 0.5 }, -40, null);
    try std.testing.expect(d.open and d.profile.? == 0 and !d.fallback);
    try std.testing.expectApproxEqAbs(@as(f32, 0.99504), d.similarity, 1e-4);
    // Near persona: never opens (offset +inf), and its floor is -inf.
    d = try decideGate(.routed, protocol, &routes, &.{ 0.1, 1 }, 40, null);
    try std.testing.expect(!d.open and d.profile.? == 1 and !d.fallback);
    // Facts at margin -6.5 stays closed, at -6 opens.
    try std.testing.expect(!(try decideGate(.routed, protocol, &routes, &.{ 0.6, 0.8 }, -6.5, null)).open);
    try std.testing.expect((try decideGate(.routed, protocol, &routes, &.{ 0.6, 0.8 }, -6, null)).open);
    // Between qa and facts, nearest is qa (cos 0.8971, facts 0.8918) but
    // below qa's floor 0.9: falls back to the learned offset -3.
    d = try decideGate(.routed, protocol, &routes, &.{ 2.03, 1 }, -3.5, null);
    try std.testing.expect(d.fallback and d.profile.? == 0 and d.offset == -3 and !d.open);
    try std.testing.expect((try decideGate(.routed, protocol, &routes, &.{ 2.03, 1 }, -3, null)).open);
    // Without fallback the same question takes qa's offset.
    var no_fallback = protocol;
    no_fallback.route_fallback = false;
    try std.testing.expect((try decideGate(.routed, no_fallback, &routes, &.{ 2.03, 1 }, -3.5, null)).open);
    // Ties go to the first profile (argmax): qa and persona at -0.7071.
    try std.testing.expectEqual(@as(?usize, 0), (try decideGate(.routed, no_fallback, &routes, &.{ -1, -1 }, 0, null)).profile);
    // A declared profile overrides any policy; unknown names are refused.
    d = try decideGate(.always, protocol, &routes, &.{ 1, 0 }, 40, "persona");
    try std.testing.expect(!d.open and d.profile.? == 1);
    try std.testing.expectError(error.UnknownMemorillaGateProfile, decideGate(.learned, protocol, &routes, &.{ 1, 0 }, 0, "other"));
    try std.testing.expectError(error.MemorillaGateProfileUnavailable, decideGate(.learned, protocol, null, &.{ 1, 0 }, 0, "qa"));
    // always and learned ignore the routes.
    try std.testing.expect((try decideGate(.always, protocol, null, &.{}, -100, null)).open);
    try std.testing.expect(!(try decideGate(.learned, protocol, null, &.{}, -3.5, null)).open);
    try std.testing.expect((try decideGate(.learned, protocol, null, &.{}, -3, null)).open);
    // Bad inputs.
    try std.testing.expectError(error.InvalidMemorillaSpanMargin, decideGate(.learned, protocol, null, &.{}, std.math.nan(f64), null));
    try std.testing.expectError(error.InvalidMemorillaGateQuestion, decideGate(.routed, protocol, &routes, &.{ 1, 0, 0 }, 0, null));
    try std.testing.expectError(error.InvalidMemorillaGateQuestion, decideGate(.routed, protocol, &routes, &.{ std.math.nan(f32), 0 }, 0, null));
}

test "Memorilla span routes reject malformed tables" {
    const a = std.testing.allocator;
    const inf = std.math.inf(f32);
    const cases = [_][6]f32{
        .{ 2, 0, 0, 0.5, 0, 0 }, // centroid not unit
        .{ 1, 0, std.math.nan(f32), 0.5, 0, 0 }, // NaN offset
        .{ 1, 0, 0, inf, 0, 0 }, // +inf floor
        .{ 1, 0, 0, std.math.nan(f32), 0, 0 }, // NaN floor
    };
    for (cases) |payload| {
        const bytes = try routesBytes(a, payload[0..4], 1, 2);
        defer a.free(bytes);
        try std.testing.expectError(error.InvalidMemorillaGateRoutes, Routes.fromBytes(a, bytes, &.{"qa"}, 2));
    }
    // Shape mismatch with the protocol's profile count.
    const good = [_]f32{ 1, 0, 0, 0.5 };
    const bytes = try routesBytes(a, &good, 1, 2);
    defer a.free(bytes);
    try std.testing.expectError(error.InvalidMemorillaGateRoutes, Routes.fromBytes(a, bytes, &.{ "qa", "persona" }, 2));
    try std.testing.expectError(error.InvalidMemorillaGateRoutes, Routes.fromBytes(a, bytes, &.{"qa"}, 3));
}
