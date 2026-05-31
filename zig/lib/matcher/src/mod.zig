// Copyright 2026 Antfly, Inc.
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

//! Record-matching scorer for entity resolution.
//!
//! Implements the comparison-levels ("Fellegi-Sunter") scoring model described
//! in zig/RESOLUTION.md. A resolver decides whether an extracted mention refers
//! to an existing canonical entity. The decision is made by a pure, declarative
//! scoring function:
//!
//!   * each `Comparison` pairs a field on the mention (`left`) with a field on
//!     the candidate (`right`) and lists weighted `Level`s,
//!   * each `Level` tests a comparator (exact / jaro_winkler / levenshtein /
//!     jaccard / cosine / prefix) against a threshold; the first matching level
//!     contributes its weight,
//!   * weights sum (with a bias) into a log-odds score, which a logistic link
//!     turns into a calibrated probability,
//!   * thresholds map the probability to MATCH / REVIEW / NO_MATCH.
//!
//! The same config drives a deterministic resolver (hand-written weights) and a
//! learned one (weights fit by EM / logistic regression over labelled pairs) --
//! only the weights change, never the structure. Scoring is intentionally
//! allocation-light, side-effect free, and deterministic so it is safe to run
//! inside replay workers: the same (mention, candidate) pair always scores the
//! same.

const std = @import("std");

/// Comparators usable in a level condition. Text comparators operate on
/// `Value.text`; `cosine` operates on `Value.vector`; `exact` works on either
/// text or numbers.
pub const Comparator = enum {
    exact,
    jaro_winkler,
    levenshtein,
    jaccard,
    cosine,
    prefix,

    pub fn parse(s: []const u8) ?Comparator {
        const map = .{
            .{ "exact", Comparator.exact },
            .{ "jaro_winkler", Comparator.jaro_winkler },
            .{ "levenshtein", Comparator.levenshtein },
            .{ "jaccard", Comparator.jaccard },
            .{ "cosine", Comparator.cosine },
            .{ "prefix", Comparator.prefix },
        };
        inline for (map) |entry| {
            if (std.mem.eql(u8, s, entry[0])) return entry[1];
        }
        return null;
    }
};

/// Comparison operator used in a `when` condition such as `cosine > 0.9`.
pub const Op = enum {
    gt,
    ge,
    lt,
    le,
    eq,

    pub fn parse(s: []const u8) ?Op {
        if (std.mem.eql(u8, s, ">")) return .gt;
        if (std.mem.eql(u8, s, ">=")) return .ge;
        if (std.mem.eql(u8, s, "<")) return .lt;
        if (std.mem.eql(u8, s, "<=")) return .le;
        if (std.mem.eql(u8, s, "==") or std.mem.eql(u8, s, "=")) return .eq;
        return null;
    }

    fn holds(self: Op, value: f64, threshold: f64) bool {
        return switch (self) {
            .gt => value > threshold,
            .ge => value >= threshold,
            .lt => value < threshold,
            .le => value <= threshold,
            .eq => value == threshold,
        };
    }
};

/// A typed field value pulled from a mention or candidate record.
pub const Value = union(enum) {
    text: []const u8,
    number: f64,
    vector: []const f32,
    none,

    pub fn asText(self: Value) ?[]const u8 {
        return switch (self) {
            .text => |t| t,
            else => null,
        };
    }

    pub fn asNumber(self: Value) ?f64 {
        return switch (self) {
            .number => |n| n,
            else => null,
        };
    }

    pub fn asVector(self: Value) ?[]const f32 {
        return switch (self) {
            .vector => |v| v,
            else => null,
        };
    }
};

/// A named field on a record.
pub const Field = struct {
    name: []const u8,
    value: Value,
};

/// A record is a flat bag of named fields. The mention (`left`) and candidate
/// (`right`) are both records; comparisons look up fields by name.
pub const Record = struct {
    fields: []const Field,

    pub fn get(self: Record, name: []const u8) Value {
        for (self.fields) |f| {
            if (std.mem.eql(u8, f.name, name)) return f.value;
        }
        return .none;
    }
};

/// A level condition. `exact` parses to `threshold{exact, ge, 1.0}` and the
/// catch-all `"else": true` parses to `otherwise`.
const Condition = union(enum) {
    threshold: struct {
        comparator: Comparator,
        op: Op,
        value: f64,
    },
    otherwise,

    fn holds(self: Condition, scratch: std.mem.Allocator, left: Value, right: Value) bool {
        return switch (self) {
            .otherwise => true,
            .threshold => |t| t.op.holds(similarity(scratch, t.comparator, left, right), t.value),
        };
    }
};

const Level = struct {
    condition: Condition,
    weight: f64,
};

const Comparison = struct {
    name: []const u8,
    left: []const u8,
    right: []const u8,
    levels: []const Level,

    /// First-match weight: walk levels in order, return the weight of the first
    /// level whose condition holds. No matching level contributes 0.
    fn weightFor(self: Comparison, scratch: std.mem.Allocator, left: Value, right: Value) f64 {
        for (self.levels) |level| {
            if (level.condition.holds(scratch, left, right)) return level.weight;
        }
        return 0;
    }
};

/// Final decision for a (mention, candidate) pair.
pub const Outcome = enum {
    match,
    review,
    no_match,
};

pub const ScoreResult = struct {
    /// Summed level weights plus bias, interpreted as log-odds.
    score: f64,
    /// Logistic of `score`, in [0, 1].
    probability: f64,
    outcome: Outcome,
};

/// Per-comparison breakdown of a score, for the review UI and for learning.
pub const Contribution = struct {
    comparison: []const u8,
    /// Index of the matched level, or null if no level matched.
    level: ?usize,
    weight: f64,
};

pub const ParseError = error{
    InvalidConfig,
    MissingComparisons,
    InvalidComparison,
    InvalidLevel,
    InvalidCondition,
} || std.mem.Allocator.Error || std.json.ParseError(std.json.Scanner);

/// A parsed, reusable scorer. Owns its config in an arena; `deinit` frees it.
pub const Scorer = struct {
    arena: std.heap.ArenaAllocator,
    comparisons: []const Comparison,
    bias: f64,
    match_threshold: f64,
    review_threshold: f64,

    pub fn parse(gpa: std.mem.Allocator, json_bytes: []const u8) ParseError!Scorer {
        var parsed = try std.json.parseFromSlice(std.json.Value, gpa, json_bytes, .{});
        defer parsed.deinit();
        return parseValue(gpa, parsed.value);
    }

    pub fn parseValue(gpa: std.mem.Allocator, root: std.json.Value) ParseError!Scorer {
        if (root != .object) return error.InvalidConfig;
        const obj = root.object;

        var arena = std.heap.ArenaAllocator.init(gpa);
        errdefer arena.deinit();
        const a = arena.allocator();

        const comps_v = obj.get("comparisons") orelse return error.MissingComparisons;
        if (comps_v != .array) return error.MissingComparisons;
        const comparisons = try a.alloc(Comparison, comps_v.array.items.len);
        for (comps_v.array.items, 0..) |cv, i| {
            comparisons[i] = try parseComparison(a, cv);
        }

        var bias: f64 = 0;
        if (obj.get("combine")) |cv| {
            if (cv == .object) {
                if (cv.object.get("bias")) |bv| bias = valueToF64(bv) orelse 0;
            }
        }

        var match_threshold: f64 = 0.5;
        var review_threshold: f64 = 0.5;
        if (obj.get("decision")) |dv| {
            if (dv == .object) {
                if (dv.object.get("match")) |mv| match_threshold = valueToF64(mv) orelse 0.5;
                // Review band exists in the data model; the human-in-the-loop
                // workflow that consumes it is phase 2 (see RESOLUTION.md).
                review_threshold = if (dv.object.get("review")) |rv|
                    (valueToF64(rv) orelse match_threshold)
                else
                    match_threshold;
            }
        }

        return .{
            .arena = arena,
            .comparisons = comparisons,
            .bias = bias,
            .match_threshold = match_threshold,
            .review_threshold = review_threshold,
        };
    }

    pub fn deinit(self: *Scorer) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Score a (mention, candidate) pair. `scratch` is used only for transient
    /// comparator working memory and may be freed immediately after; the result
    /// borrows nothing.
    pub fn score(self: *const Scorer, scratch: std.mem.Allocator, left: Record, right: Record) ScoreResult {
        var sum: f64 = self.bias;
        for (self.comparisons) |cmp| {
            sum += cmp.weightFor(scratch, left.get(cmp.left), right.get(cmp.right));
        }
        const probability = logistic(sum);
        const outcome: Outcome = if (probability >= self.match_threshold)
            .match
        else if (probability >= self.review_threshold)
            .review
        else
            .no_match;
        return .{ .score = sum, .probability = probability, .outcome = outcome };
    }

    /// Per-comparison breakdown of a score. The returned slice is owned by the
    /// caller (`allocator.free`); the `comparison` strings borrow the scorer and
    /// stay valid until the scorer is `deinit`ed.
    pub fn explain(
        self: *const Scorer,
        allocator: std.mem.Allocator,
        scratch: std.mem.Allocator,
        left: Record,
        right: Record,
    ) ![]Contribution {
        const out = try allocator.alloc(Contribution, self.comparisons.len);
        for (self.comparisons, 0..) |cmp, ci| {
            const lv = left.get(cmp.left);
            const rv = right.get(cmp.right);
            var matched: ?usize = null;
            var weight: f64 = 0;
            for (cmp.levels, 0..) |level, li| {
                if (level.condition.holds(scratch, lv, rv)) {
                    matched = li;
                    weight = level.weight;
                    break;
                }
            }
            out[ci] = .{ .comparison = cmp.name, .level = matched, .weight = weight };
        }
        return out;
    }
};

fn parseComparison(a: std.mem.Allocator, value: std.json.Value) ParseError!Comparison {
    if (value != .object) return error.InvalidComparison;
    const obj = value.object;

    const name = try dupString(a, obj.get("name") orelse return error.InvalidComparison);
    const left = try dupFieldName(a, obj.get("left") orelse return error.InvalidComparison);
    const right = try dupFieldName(a, obj.get("right") orelse return error.InvalidComparison);

    const levels_v = obj.get("levels") orelse return error.InvalidComparison;
    if (levels_v != .array) return error.InvalidComparison;
    const levels = try a.alloc(Level, levels_v.array.items.len);
    for (levels_v.array.items, 0..) |lv, i| {
        levels[i] = try parseLevel(lv);
    }

    return .{ .name = name, .left = left, .right = right, .levels = levels };
}

fn parseLevel(value: std.json.Value) ParseError!Level {
    if (value != .object) return error.InvalidLevel;
    const obj = value.object;

    const weight = valueToF64(obj.get("weight") orelse return error.InvalidLevel) orelse
        return error.InvalidLevel;

    if (obj.get("when")) |when_v| {
        const when = jsonString(when_v) orelse return error.InvalidCondition;
        return .{ .condition = try parseWhen(when), .weight = weight };
    }
    if (obj.get("else")) |_| {
        return .{ .condition = .otherwise, .weight = weight };
    }
    return error.InvalidLevel;
}

fn parseWhen(raw: []const u8) ParseError!Condition {
    const s = std.mem.trim(u8, raw, " \t");
    if (std.mem.eql(u8, s, "exact")) {
        return .{ .threshold = .{ .comparator = .exact, .op = .ge, .value = 1.0 } };
    }
    var it = std.mem.tokenizeAny(u8, s, " \t");
    const comparator_s = it.next() orelse return error.InvalidCondition;
    const op_s = it.next() orelse return error.InvalidCondition;
    const value_s = it.next() orelse return error.InvalidCondition;
    if (it.next() != null) return error.InvalidCondition;

    const comparator = Comparator.parse(comparator_s) orelse return error.InvalidCondition;
    const op = Op.parse(op_s) orelse return error.InvalidCondition;
    const value = std.fmt.parseFloat(f64, value_s) catch return error.InvalidCondition;
    return .{ .threshold = .{ .comparator = comparator, .op = op, .value = value } };
}

fn jsonString(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => |s| s,
        else => null,
    };
}

fn dupString(a: std.mem.Allocator, value: std.json.Value) ParseError![]const u8 {
    const s = jsonString(value) orelse return error.InvalidComparison;
    return a.dupe(u8, s);
}

/// Field accessors may be written `m.canonical_text` / `c.canonical_name`; the
/// leading `<ctx>.` is stripped so records can be keyed by bare field name.
fn dupFieldName(a: std.mem.Allocator, value: std.json.Value) ParseError![]const u8 {
    const raw = jsonString(value) orelse return error.InvalidComparison;
    const name = if (std.mem.indexOfScalar(u8, raw, '.')) |idx| raw[idx + 1 ..] else raw;
    return a.dupe(u8, name);
}

fn valueToF64(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        .number_string => |s| std.fmt.parseFloat(f64, s) catch null,
        else => null,
    };
}

fn logistic(x: f64) f64 {
    return 1.0 / (1.0 + std.math.exp(-x));
}

// --- Learned weights (logistic regression) ---------------------------------
//
// Phase-2 "learned mode": the scorer's structure (comparisons -> levels) is
// unchanged; only the per-level weights change, fit offline from labelled
// match/no-match pairs. A pair is encoded as a feature vector -- in the
// deterministic level encoding, `features[i]` is 1.0 when level i was the
// first-matching level of its comparison, else 0.0 -- and logistic regression
// fits a weight per feature (+ a bias). The fitted weights then populate the
// same `Level.weight`/`combine.bias` fields the deterministic scorer reads.

pub const TrainingExample = struct {
    features: []const f64,
    /// True when the pair is a real match (the regression target).
    label: bool,
};

pub const FitOptions = struct {
    iterations: usize = 1000,
    learning_rate: f64 = 0.3,
    /// L2 regularization strength applied to weights (not the bias).
    l2: f64 = 0.0,
};

/// Fit logistic-regression weights over labelled examples by batch gradient
/// descent. Returns `feature_count + 1` owned weights; the last element is the
/// bias. Deterministic for a given input so a learned scorer is reproducible.
pub fn fitLogisticRegression(
    alloc: std.mem.Allocator,
    examples: []const TrainingExample,
    feature_count: usize,
    opts: FitOptions,
) ![]f64 {
    const w = try alloc.alloc(f64, feature_count + 1);
    @memset(w, 0);
    errdefer alloc.free(w);
    if (examples.len == 0) return w;

    const grad = try alloc.alloc(f64, feature_count + 1);
    defer alloc.free(grad);
    const n: f64 = @floatFromInt(examples.len);

    var iter: usize = 0;
    while (iter < opts.iterations) : (iter += 1) {
        @memset(grad, 0);
        for (examples) |ex| {
            var z: f64 = w[feature_count]; // bias
            for (ex.features, 0..) |f, i| {
                if (i >= feature_count) break;
                z += w[i] * f;
            }
            const err = logistic(z) - (if (ex.label) @as(f64, 1.0) else 0.0);
            for (ex.features, 0..) |f, i| {
                if (i >= feature_count) break;
                grad[i] += err * f;
            }
            grad[feature_count] += err;
        }
        for (w, 0..) |*wi, i| {
            const reg = if (i < feature_count) opts.l2 * wi.* else 0.0;
            wi.* -= opts.learning_rate * (grad[i] / n + reg);
        }
    }
    return w;
}

/// Match probability for a feature vector under fitted weights
/// (`weights[len-1]` is the bias). Mirrors the scorer's logistic link.
pub fn predictLogistic(weights: []const f64, features: []const f64) f64 {
    if (weights.len == 0) return logistic(0);
    const fc = weights.len - 1;
    var z: f64 = weights[fc];
    for (features, 0..) |f, i| {
        if (i >= fc) break;
        z += weights[i] * f;
    }
    return logistic(z);
}

// --- Fusion across multiple extractors -------------------------------------
//
// Multiple extractors may assert the same edge with different confidences and
// trust. Fusion combines them (plus an optional graph-derived prior pinned to a
// config generation) into one calibrated confidence the graph stores as the
// edge weight.

pub const FusionStrategy = enum { noisy_or, max, mean };

pub const SourceConfidence = struct {
    /// Per-source asserted confidence in [0, 1].
    confidence: f64,
    /// Source trust in [0, 1]; scales the contribution.
    trust: f64 = 1.0,
};

fn clamp01(x: f64) f64 {
    return @max(0.0, @min(1.0, x));
}

/// Combine per-source `trust * confidence` contributions (and a `prior` scaled
/// by `prior_weight`) into one confidence in [0, 1]. `noisy_or` treats sources
/// as independent evidence (1 - prod(1 - c_i)); `max` takes the strongest;
/// `mean` averages. The prior is folded in as one more term so a graph-derived
/// belief nudges -- but cannot by itself certify -- a fused edge.
pub fn fuse(
    strategy: FusionStrategy,
    sources: []const SourceConfidence,
    prior: f64,
    prior_weight: f64,
) f64 {
    const prior_term = clamp01(prior) * clamp01(prior_weight);
    switch (strategy) {
        .noisy_or => {
            var complement: f64 = 1.0 - prior_term;
            for (sources) |s| complement *= (1.0 - clamp01(clamp01(s.trust) * clamp01(s.confidence)));
            return clamp01(1.0 - complement);
        },
        .max => {
            var best: f64 = prior_term;
            for (sources) |s| best = @max(best, clamp01(s.trust) * clamp01(s.confidence));
            return clamp01(best);
        },
        .mean => {
            var sum: f64 = prior_term;
            var count: f64 = if (prior_weight > 0) 1.0 else 0.0;
            for (sources) |s| {
                sum += clamp01(s.trust) * clamp01(s.confidence);
                count += 1.0;
            }
            return if (count == 0) 0.0 else clamp01(sum / count);
        },
    }
}

// --- Comparators ------------------------------------------------------------

/// Similarity in [0, 1] between two values under `comparator`. Type mismatches
/// (e.g. a text comparator on a vector field) and allocation failures degrade
/// to 0 so scoring stays total and deterministic.
pub fn similarity(scratch: std.mem.Allocator, comparator: Comparator, left: Value, right: Value) f64 {
    return switch (comparator) {
        .exact => exactSimilarity(left, right),
        .jaro_winkler => textSimilarity(scratch, left, right, jaroWinkler),
        .levenshtein => textSimilarity(scratch, left, right, levenshteinRatio),
        .jaccard => textSimilarity(scratch, left, right, jaccardSimilarity),
        .prefix => textSimilarity(scratch, left, right, prefixSimilarity),
        .cosine => cosineSimilarity(left, right),
    };
}

fn textSimilarity(
    scratch: std.mem.Allocator,
    left: Value,
    right: Value,
    comptime f: fn (std.mem.Allocator, []const u8, []const u8) f64,
) f64 {
    const a = left.asText() orelse return 0;
    const b = right.asText() orelse return 0;
    return f(scratch, a, b);
}

fn exactSimilarity(left: Value, right: Value) f64 {
    if (left.asText()) |a| {
        if (right.asText()) |b| return if (std.mem.eql(u8, a, b)) 1.0 else 0.0;
    }
    if (left.asNumber()) |a| {
        if (right.asNumber()) |b| return if (a == b) 1.0 else 0.0;
    }
    return 0;
}

pub fn cosineSimilarity(left: Value, right: Value) f64 {
    const a = left.asVector() orelse return 0;
    const b = right.asVector() orelse return 0;
    if (a.len == 0 or a.len != b.len) return 0;
    var dot: f64 = 0;
    var norm_a: f64 = 0;
    var norm_b: f64 = 0;
    for (a, b) |x, y| {
        const xf: f64 = x;
        const yf: f64 = y;
        dot += xf * yf;
        norm_a += xf * xf;
        norm_b += yf * yf;
    }
    if (norm_a == 0 or norm_b == 0) return 0;
    const cos = dot / (@sqrt(norm_a) * @sqrt(norm_b));
    return if (cos < 0) 0 else cos;
}

pub fn jaroWinkler(scratch: std.mem.Allocator, a: []const u8, b: []const u8) f64 {
    if (std.mem.eql(u8, a, b)) return 1.0;
    if (a.len == 0 or b.len == 0) return 0.0;

    const jaro = jaroSimilarity(scratch, a, b);
    var prefix: usize = 0;
    const max_prefix = @min(@min(a.len, b.len), 4);
    while (prefix < max_prefix and a[prefix] == b[prefix]) : (prefix += 1) {}
    return jaro + @as(f64, @floatFromInt(prefix)) * 0.1 * (1.0 - jaro);
}

fn jaroSimilarity(scratch: std.mem.Allocator, a: []const u8, b: []const u8) f64 {
    const a_matches = scratch.alloc(bool, a.len) catch return 0;
    defer scratch.free(a_matches);
    @memset(a_matches, false);
    const b_matches = scratch.alloc(bool, b.len) catch return 0;
    defer scratch.free(b_matches);
    @memset(b_matches, false);

    const max_len = @max(a.len, b.len);
    const max_dist: usize = if (max_len > 1) max_len / 2 - 1 else 0;

    var matches: usize = 0;
    for (a, 0..) |a_char, i| {
        const start = i -| max_dist;
        const end = @min(b.len, i + max_dist + 1);
        var j = start;
        while (j < end) : (j += 1) {
            if (b_matches[j] or a_char != b[j]) continue;
            a_matches[i] = true;
            b_matches[j] = true;
            matches += 1;
            break;
        }
    }
    if (matches == 0) return 0;

    var transpositions: usize = 0;
    var k: usize = 0;
    for (a, 0..) |a_char, i| {
        if (!a_matches[i]) continue;
        while (k < b.len and !b_matches[k]) : (k += 1) {}
        if (k < b.len and a_char != b[k]) transpositions += 1;
        k += 1;
    }

    const matches_f: f64 = @floatFromInt(matches);
    const a_len_f: f64 = @floatFromInt(a.len);
    const b_len_f: f64 = @floatFromInt(b.len);
    const transpositions_f = @as(f64, @floatFromInt(transpositions)) / 2.0;
    return ((matches_f / a_len_f) + (matches_f / b_len_f) + ((matches_f - transpositions_f) / matches_f)) / 3.0;
}

fn levenshteinRatio(scratch: std.mem.Allocator, a: []const u8, b: []const u8) f64 {
    const max_len = @max(a.len, b.len);
    if (max_len == 0) return 1.0;
    const dist = levenshtein(scratch, a, b) catch return 0;
    return 1.0 - @as(f64, @floatFromInt(dist)) / @as(f64, @floatFromInt(max_len));
}

fn levenshtein(scratch: std.mem.Allocator, a: []const u8, b: []const u8) !usize {
    var prev = try scratch.alloc(usize, b.len + 1);
    defer scratch.free(prev);
    var curr = try scratch.alloc(usize, b.len + 1);
    defer scratch.free(curr);

    for (0..b.len + 1) |j| prev[j] = j;
    for (a, 0..) |a_char, i| {
        curr[0] = i + 1;
        for (b, 0..) |b_char, j| {
            const cost: usize = if (a_char == b_char) 0 else 1;
            curr[j + 1] = @min(@min(curr[j] + 1, prev[j + 1] + 1), prev[j] + cost);
        }
        std.mem.swap([]usize, &prev, &curr);
    }
    return prev[b.len];
}

fn jaccardSimilarity(scratch: std.mem.Allocator, a: []const u8, b: []const u8) f64 {
    var set_a = tokenSet(scratch, a) catch return 0;
    defer set_a.deinit(scratch);
    var set_b = tokenSet(scratch, b) catch return 0;
    defer set_b.deinit(scratch);

    if (set_a.items.len == 0 and set_b.items.len == 0) return 1.0;

    var intersection: usize = 0;
    for (set_a.items) |x| {
        for (set_b.items) |y| {
            if (std.mem.eql(u8, x, y)) {
                intersection += 1;
                break;
            }
        }
    }
    const union_size = set_a.items.len + set_b.items.len - intersection;
    if (union_size == 0) return 0;
    return @as(f64, @floatFromInt(intersection)) / @as(f64, @floatFromInt(union_size));
}

fn tokenSet(scratch: std.mem.Allocator, s: []const u8) !std.ArrayListUnmanaged([]const u8) {
    var list = std.ArrayListUnmanaged([]const u8).empty;
    errdefer list.deinit(scratch);
    var it = std.mem.tokenizeAny(u8, s, " \t\r\n");
    outer: while (it.next()) |token| {
        for (list.items) |existing| {
            if (std.mem.eql(u8, existing, token)) continue :outer;
        }
        try list.append(scratch, token);
    }
    return list;
}

fn prefixSimilarity(_: std.mem.Allocator, a: []const u8, b: []const u8) f64 {
    if (a.len == 0 or b.len == 0) return 0;
    const shorter, const longer = if (a.len <= b.len) .{ a, b } else .{ b, a };
    if (std.mem.startsWith(u8, longer, shorter)) {
        return @as(f64, @floatFromInt(shorter.len)) / @as(f64, @floatFromInt(longer.len));
    }
    return 0;
}

// --- Tests ------------------------------------------------------------------

const testing = std.testing;

const name_only_config =
    \\{
    \\  "comparisons": [
    \\    { "name": "name", "left": "m.name", "right": "c.name",
    \\      "levels": [
    \\        { "when": "exact", "weight": 8.0 },
    \\        { "when": "jaro_winkler > 0.92", "weight": 5.0 },
    \\        { "when": "jaro_winkler > 0.85", "weight": 2.0 },
    \\        { "else": true, "weight": -6.0 }
    \\      ] }
    \\  ],
    \\  "combine": { "bias": -3.0 },
    \\  "decision": { "match": 0.9, "review": 0.6 }
    \\}
;

test "parse strips context prefix from field accessors" {
    var scorer = try Scorer.parse(testing.allocator, name_only_config);
    defer scorer.deinit();
    try testing.expectEqual(@as(usize, 1), scorer.comparisons.len);
    try testing.expectEqualStrings("name", scorer.comparisons[0].left);
    try testing.expectEqualStrings("name", scorer.comparisons[0].right);
    try testing.expectEqual(@as(usize, 4), scorer.comparisons[0].levels.len);
}

fn nameRecord(fields: *[1]Field, value: []const u8) Record {
    fields[0] = .{ .name = "name", .value = .{ .text = value } };
    return .{ .fields = fields };
}

test "exact name match scores as match with high probability" {
    var scorer = try Scorer.parse(testing.allocator, name_only_config);
    defer scorer.deinit();
    var lf: [1]Field = undefined;
    var rf: [1]Field = undefined;
    const r = scorer.score(
        testing.allocator,
        nameRecord(&lf, "Ada Lovelace"),
        nameRecord(&rf, "Ada Lovelace"),
    );
    try testing.expectEqual(Outcome.match, r.outcome);
    try testing.expect(r.probability > 0.99);
}

test "dissimilar names fall through to the else level and score no_match" {
    var scorer = try Scorer.parse(testing.allocator, name_only_config);
    defer scorer.deinit();
    var lf: [1]Field = undefined;
    var rf: [1]Field = undefined;
    const r = scorer.score(
        testing.allocator,
        nameRecord(&lf, "Ada Lovelace"),
        nameRecord(&rf, "Charles Babbage"),
    );
    try testing.expectEqual(Outcome.no_match, r.outcome);
    try testing.expect(r.probability < 0.01);
}

test "a near-miss typo lands in the review band, not match" {
    var scorer = try Scorer.parse(testing.allocator, name_only_config);
    defer scorer.deinit();
    var lf: [1]Field = undefined;
    var rf: [1]Field = undefined;
    // One deleted character: high jaro-winkler, but not exact.
    const r = scorer.score(
        testing.allocator,
        nameRecord(&lf, "Ada Lovelace"),
        nameRecord(&rf, "Ada Lovlace"),
    );
    try testing.expectEqual(Outcome.review, r.outcome);
    try testing.expect(r.probability > 0.6 and r.probability < 0.9);
}

test "cosine comparison scores vector fields" {
    const config =
        \\{
        \\  "comparisons": [
        \\    { "name": "vec", "left": "emb", "right": "emb",
        \\      "levels": [
        \\        { "when": "cosine > 0.9", "weight": 6.0 },
        \\        { "else": true, "weight": -4.0 }
        \\      ] }
        \\  ],
        \\  "combine": { "bias": 0.0 },
        \\  "decision": { "match": 0.9 }
        \\}
    ;
    var scorer = try Scorer.parse(testing.allocator, config);
    defer scorer.deinit();

    const v1 = [_]f32{ 0.1, 0.2, 0.3, 0.4 };
    const v2 = [_]f32{ 0.1, 0.2, 0.3, 0.4 };
    const v3 = [_]f32{ -0.4, -0.3, -0.2, -0.1 };
    const left = Record{ .fields = &.{.{ .name = "emb", .value = .{ .vector = &v1 } }} };
    const same = Record{ .fields = &.{.{ .name = "emb", .value = .{ .vector = &v2 } }} };
    const diff = Record{ .fields = &.{.{ .name = "emb", .value = .{ .vector = &v3 } }} };

    try testing.expectEqual(Outcome.match, scorer.score(testing.allocator, left, same).outcome);
    try testing.expectEqual(Outcome.no_match, scorer.score(testing.allocator, left, diff).outcome);
}

test "explain reports the matched level per comparison" {
    var scorer = try Scorer.parse(testing.allocator, name_only_config);
    defer scorer.deinit();
    var lf: [1]Field = undefined;
    var rf: [1]Field = undefined;
    const contributions = try scorer.explain(
        testing.allocator,
        testing.allocator,
        nameRecord(&lf, "Ada Lovelace"),
        nameRecord(&rf, "Ada Lovelace"),
    );
    defer testing.allocator.free(contributions);
    try testing.expectEqual(@as(usize, 1), contributions.len);
    try testing.expectEqualStrings("name", contributions[0].comparison);
    try testing.expectEqual(@as(?usize, 0), contributions[0].level);
    try testing.expectEqual(@as(f64, 8.0), contributions[0].weight);
}

test "comparator building blocks" {
    const a = testing.allocator;
    try testing.expectEqual(@as(f64, 1.0), jaroWinkler(a, "hello", "hello"));
    try testing.expectEqual(@as(f64, 0.0), jaroWinkler(a, "", "hello"));
    try testing.expect(levenshteinRatio(a, "kitten", "sitting") > 0.5);
    try testing.expectEqual(@as(f64, 1.0), levenshteinRatio(a, "same", "same"));
    // {ada, lovelace} vs {ada, babbage}: intersection 1, union 3.
    try testing.expectApproxEqAbs(@as(f64, 1.0 / 3.0), jaccardSimilarity(a, "ada lovelace", "ada babbage"), 1e-9);
    try testing.expectEqual(@as(f64, 1.0), exactSimilarity(.{ .text = "x" }, .{ .text = "x" }));
    try testing.expectEqual(@as(f64, 0.0), exactSimilarity(.{ .text = "x" }, .{ .text = "y" }));
}

test "missing fields contribute the else weight rather than crashing" {
    var scorer = try Scorer.parse(testing.allocator, name_only_config);
    defer scorer.deinit();
    const empty = Record{ .fields = &.{} };
    const r = scorer.score(testing.allocator, empty, empty);
    // No fields -> exact/jaro all fail -> else (-6) + bias (-3) = -9.
    try testing.expectEqual(Outcome.no_match, r.outcome);
    try testing.expectApproxEqAbs(@as(f64, -9.0), r.score, 1e-9);
}

test "invalid configs are rejected" {
    try testing.expectError(error.MissingComparisons, Scorer.parse(testing.allocator, "{}"));
    try testing.expectError(error.InvalidCondition, Scorer.parse(testing.allocator,
        \\{ "comparisons": [ { "name": "n", "left": "a", "right": "b",
        \\  "levels": [ { "when": "bogus_comparator > 0.5", "weight": 1.0 } ] } ] }
    ));
}

test "fitLogisticRegression learns separable level weights" {
    const alloc = testing.allocator;
    // Two features: f0 = "names match", f1 = "names differ". Matches activate
    // f0; non-matches activate f1.
    const examples = [_]TrainingExample{
        .{ .features = &.{ 1, 0 }, .label = true },
        .{ .features = &.{ 1, 0 }, .label = true },
        .{ .features = &.{ 0, 1 }, .label = false },
        .{ .features = &.{ 0, 1 }, .label = false },
    };
    const w = try fitLogisticRegression(alloc, &examples, 2, .{});
    defer alloc.free(w);
    try testing.expectEqual(@as(usize, 3), w.len); // 2 features + bias
    // The "match" feature ends positive, the "differ" feature negative.
    try testing.expect(w[0] > 0);
    try testing.expect(w[1] < 0);
    // And the fitted model classifies the two regimes correctly.
    try testing.expect(predictLogistic(w, &.{ 1, 0 }) > 0.5);
    try testing.expect(predictLogistic(w, &.{ 0, 1 }) < 0.5);
}

test "fitLogisticRegression on no examples returns zero weights" {
    const alloc = testing.allocator;
    const w = try fitLogisticRegression(alloc, &.{}, 3, .{});
    defer alloc.free(w);
    try testing.expectEqual(@as(usize, 4), w.len);
    for (w) |wi| try testing.expectEqual(@as(f64, 0), wi);
    // Zero weights => probability 0.5 (no signal).
    try testing.expectApproxEqAbs(@as(f64, 0.5), predictLogistic(w, &.{ 1, 1, 1 }), 1e-9);
}

test "fuse combines source confidences and a prior" {
    // Two strong, fully-trusted sources under noisy-or exceed either alone.
    const sources = [_]SourceConfidence{
        .{ .confidence = 0.8, .trust = 1.0 },
        .{ .confidence = 0.6, .trust = 1.0 },
    };
    const noisy = fuse(.noisy_or, &sources, 0.0, 0.0);
    try testing.expectApproxEqAbs(@as(f64, 0.92), noisy, 1e-9); // 1 - 0.2*0.4
    try testing.expect(noisy > 0.8);

    // Trust scales a source down.
    const low_trust = [_]SourceConfidence{.{ .confidence = 0.9, .trust = 0.5 }};
    try testing.expectApproxEqAbs(@as(f64, 0.45), fuse(.noisy_or, &low_trust, 0.0, 0.0), 1e-9);

    // max takes the strongest contribution (including the prior).
    try testing.expectApproxEqAbs(@as(f64, 0.8), fuse(.max, &sources, 0.0, 0.0), 1e-9);
    try testing.expectApproxEqAbs(@as(f64, 0.85), fuse(.max, &low_trust, 0.85, 1.0), 1e-9);

    // Results stay clamped to [0, 1].
    const many = [_]SourceConfidence{ .{ .confidence = 1, .trust = 1 }, .{ .confidence = 1, .trust = 1 } };
    try testing.expectEqual(@as(f64, 1.0), fuse(.noisy_or, &many, 1.0, 1.0));
    try testing.expectEqual(@as(f64, 0.0), fuse(.noisy_or, &.{}, 0.0, 0.0));
}
