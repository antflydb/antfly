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

const std = @import("std");
const regex = @import("antfly_regex").search;
const Options = @import("options.zig").Options;
const Allocator = std.mem.Allocator;
pub const Span = regex.Span;
pub const Match = struct { span: Span, captures: []const ?Span, names: []const regex.CaptureName, capture_text: ?[]const u8 = null };
const Literal = struct { bytes: []const u8, insensitive: bool, order: usize };
const Regular = struct { pattern: regex.Pattern, order: usize };
const Advanced = struct { pattern: @import("pcre2.zig").Pattern, order: usize };
/// Immutable compiled programs are shared; mutable VM/DFA scratch belongs to a
/// matcher. Atomic ownership makes worker and library-client lifetimes explicit.
const Program = struct {
    a: Allocator,
    references: std.atomic.Value(usize) = .init(1),
    prefilter: ?regex.prefilter.Set = null,
    absolute_start: bool = false,
    direct: std.ArrayList(Literal) = .empty,
    regular: std.ArrayList(Regular) = .empty,
    advanced: std.ArrayList(Advanced) = .empty,
    fn release(self: *Program) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        if (self.prefilter) |*set| set.deinit();
        self.clearPatterns();
        self.direct.deinit(self.a);
        self.regular.deinit(self.a);
        self.advanced.deinit(self.a);
        self.a.destroy(self);
    }
    fn clearPatterns(self: *Program) void {
        for (self.direct.items) |item| self.a.free(item.bytes);
        for (self.regular.items) |*item| item.pattern.deinit();
        for (self.advanced.items) |*item| item.pattern.deinit();
        self.direct.clearRetainingCapacity();
        self.regular.clearRetainingCapacity();
        self.advanced.clearRetainingCapacity();
    }
    fn compileAll(self: *Program, o: *const Options) !void {
        var order: usize = 0;
        for (o.patterns.items) |raw| {
            if (o.multiline) {
                try self.compile(raw, o, order);
                order += 1;
            } else {
                var split = std.mem.splitScalar(u8, raw, '\n');
                while (split.next()) |pattern| {
                    try self.compile(pattern, o, order);
                    order += 1;
                }
            }
        }
    }
    fn buildPrefilter(self: *Program, o: *const Options) !void {
        for (self.regular.items) |item| self.absolute_start = self.absolute_start or regex.hasAbsoluteStart(item.pattern.states);
        for (self.advanced.items) |item| self.absolute_start = self.absolute_start or regex.hasAbsoluteStart(item.pattern.pattern.states);
        if (self.advanced.items.len > 0 or self.direct.items.len + self.regular.items.len < 4) return;
        var needles: std.ArrayList(regex.prefilter.Needle) = .empty;
        defer needles.deinit(self.a);
        for (self.direct.items) |item| try needles.append(self.a, .{ .bytes = item.bytes, .insensitive = item.insensitive, .unicode = o.unicode });
        for (self.regular.items) |item| {
            if (item.pattern.prefilter.len == 0) return;
            try needles.append(self.a, .{ .bytes = item.pattern.prefilter });
        }
        self.prefilter = regex.prefilter.Set.init(self.a, needles.items, @min(o.regex_size_limit, 1024 * 1024)) catch |err| switch (err) {
            error.PrefilterTooLarge, error.UnsupportedPrefilter => null,
            else => return err,
        };
    }
    fn compile(self: *Program, raw: []const u8, o: *const Options, order: usize) !void {
        const direct = o.engine != .pcre2 and (o.fixed or std.mem.indexOfAny(u8, raw, "\\.*+?()[]{}^$|") == null) and !(o.multiline and o.whole_line);
        const insensitive = o.ignore_case or (o.smart_case and !regex.hasUpperLiteral(self.a, raw, o.fixed or direct, o.unicode));
        if (direct) {
            if (o.unicode and !std.unicode.utf8ValidateSlice(raw)) return error.InvalidRegex;
            const bytes = try self.a.dupe(u8, raw);
            errdefer self.a.free(bytes);
            try self.direct.append(self.a, .{ .bytes = bytes, .insensitive = insensitive, .order = order });
            return;
        }
        const pattern = if (o.fixed) try quote(self.a, raw) else try self.a.dupe(u8, raw);
        defer self.a.free(pattern);
        if (o.engine == .pcre2) {
            var advanced = try @import("pcre2.zig").Pattern.init(self.a, pattern, o, insensitive);
            errdefer advanced.deinit();
            try self.advanced.append(self.a, .{ .pattern = advanced, .order = order });
            return;
        }
        // Ripgrep parses each expression inside a noncapturing group. This also
        // preserves its rejection of an unterminated extended-mode comment.
        const grouped = try std.fmt.allocPrint(self.a, "(?:{s})", .{pattern});
        defer self.a.free(grouped);
        var regular = regex.Pattern.init(self.a, grouped, .{ .insensitive = insensitive, .unicode = o.unicode, .multiline = true, .dotall = o.multiline_dotall, .whole_word = o.word, .whole_line = o.whole_line, .crlf = o.crlf }) catch |err| {
            if (o.engine != .auto) return err;
            var advanced = try @import("pcre2.zig").Pattern.init(self.a, pattern, o, insensitive);
            errdefer advanced.deinit();
            try self.advanced.append(self.a, .{ .pattern = advanced, .order = order });
            return;
        };
        errdefer regular.deinit();
        if (regular.arena.queryCapacity() > o.regex_size_limit) {
            if (o.engine != .auto) return error.RegexTooLarge;
            var advanced = try @import("pcre2.zig").Pattern.init(self.a, pattern, o, insensitive);
            errdefer advanced.deinit();
            try self.advanced.append(self.a, .{ .pattern = advanced, .order = order });
            regular.deinit();
            return;
        }
        try self.regular.append(self.a, .{ .pattern = regular, .order = order });
    }
};
pub const Matcher = struct {
    a: Allocator,
    options: *const Options,
    program: *Program,
    failure: ?anyerror = null,
    runners: std.ArrayList(Scratch) = .empty,
    advanced: std.ArrayList(@import("pcre2.zig").Pattern) = .empty,
    literal_capture: [1]?Span = .{null},
    capture_mode: regex.Runner.CaptureMode = .full,
    advance_utf8: bool = false,
    const Scratch = struct { vm: regex.Runner, dfa: ?regex.Dfa = null, dfa_allowed: bool = true };
    pub fn init(a: Allocator, o: *const Options) !Matcher {
        return initMode(a, o, .full);
    }
    pub fn initForSearch(a: Allocator, o: *const Options) !Matcher {
        return initMode(a, o, if (o.replacement != null) .full else .span);
    }
    fn initMode(a: Allocator, o: *const Options, mode: regex.Runner.CaptureMode) !Matcher {
        const program = try a.create(Program);
        program.* = .{ .a = a };
        errdefer program.release();
        try program.compileAll(o);
        if (o.engine == .auto and program.advanced.items.len > 0 and program.regular.items.len + program.direct.items.len > 0) {
            // Upstream chooses one dialect for the entire pattern set. Mixing
            // byte and UTF empty-match semantics changes counts/replacements.
            program.clearPatterns();
            var selected = o.*;
            selected.engine = .pcre2;
            try program.compileAll(&selected);
        }
        try program.buildPrefilter(o);
        return initScratch(a, o, program, mode);
    }
    pub fn clone(self: *const Matcher, a: Allocator) !Matcher {
        _ = self.program.references.fetchAdd(1, .monotonic);
        errdefer self.program.release();
        return initScratch(a, self.options, self.program, self.capture_mode);
    }
    fn initScratch(a: Allocator, o: *const Options, program: *Program, mode: regex.Runner.CaptureMode) !Matcher {
        var self = Matcher{ .a = a, .options = o, .program = program, .capture_mode = mode };
        errdefer self.freeScratch();
        for (program.regular.items) |*item| {
            var vm = try regex.Runner.initMode(a, &item.pattern, mode);
            errdefer vm.deinit();
            try self.runners.append(a, .{ .vm = vm });
        }
        for (program.advanced.items) |*item| {
            var runner = try item.pattern.clone(a);
            errdefer runner.deinit();
            try self.advanced.append(a, runner);
        }
        return self;
    }
    fn freeScratch(self: *Matcher) void {
        for (self.runners.items) |*runner| {
            runner.vm.deinit();
            if (runner.dfa) |*dfa| dfa.deinit();
        }
        self.runners.deinit(self.a);
        for (self.advanced.items) |*runner| runner.deinit();
        self.advanced.deinit(self.a);
    }
    pub fn deinit(self: *Matcher) void {
        self.freeScratch();
        self.program.release();
    }
    pub fn find(self: *Matcher, text: []const u8, offset: usize) ?Match {
        var best: ?Match = null;
        var best_order: usize = std.math.maxInt(usize);
        for (self.program.direct.items) |item| if (regex.literal.find(text, item.bytes, offset, item.insensitive, self.options.unicode, self.options.word, self.options.whole_line)) |span| {
            if (best == null or span.start < best.?.span.start or (span.start == best.?.span.start and item.order < best_order)) {
                self.literal_capture[0] = .{ .start = span.start, .end = span.end };
                best = .{ .span = self.literal_capture[0].?, .captures = &self.literal_capture, .names = &.{} };
                best_order = item.order;
                self.advance_utf8 = false;
            }
        };
        for (self.runners.items, self.program.regular.items) |*runner, *item| {
            const found = runner.vm.find(text, offset);
            if (runner.vm.failure) |err| {
                self.failure = err;
                return null;
            }
            if (found) |match| if (best == null or match.span.start < best.?.span.start or (match.span.start == best.?.span.start and item.order < best_order)) {
                best = .{ .span = match.span, .captures = match.captures, .names = item.pattern.names };
                best_order = item.order;
                self.advance_utf8 = false;
            };
        }
        for (self.advanced.items, self.program.advanced.items) |*runner, item| {
            const found = runner.find(text, offset) catch |err| {
                self.failure = err;
                return null;
            };
            if (found) |match| if (best == null or match.span.start < best.?.span.start or (match.span.start == best.?.span.start and item.order < best_order)) {
                best = .{ .span = match.span, .captures = match.captures, .names = item.pattern.names };
                best_order = item.order;
                // ripgrep 15.2 / PCRE2 10.45 JIT on Linux x86_64 permits
                // intermediate byte offsets for empty UTF matches. Verified ARM64
                // references advance codepoints. Keep this output compatibility
                // separate from VM decoding and native JIT execution.
                const target = @import("builtin");
                self.advance_utf8 = item.pattern.pattern.flags.unicode and !(target.os.tag == .linux and target.cpu.arch == .x86_64);
            };
        }
        return best;
    }
    /// Mandatory literals prove absence; they never approximate relevance.
    pub fn canReject(self: *const Matcher, text: []const u8) bool {
        if (self.program.prefilter) |*set| return !set.mayMatch(text);
        if (self.program.advanced.items.len > 0 or self.program.direct.items.len + self.program.regular.items.len == 0) return false;
        for (self.program.direct.items) |item| {
            if (item.insensitive) {
                if (!regex.literal.isAscii(item.bytes) or !regex.literal.isAscii(text) or regex.literal.findAsciiFolded(text, item.bytes, 0) != null) return false;
            } else if (regex.literal.findExact(text, item.bytes, 0) != null) return false;
        }
        for (self.program.regular.items) |item| if (item.pattern.prefilter.len == 0 or regex.literal.findExact(text, item.pattern.prefilter, 0) != null) return false;
        return true;
    }
    pub fn jitStats(self: *const Matcher) struct { programs: usize, bytes: usize } {
        var programs: usize = 0;
        var bytes: usize = 0;
        for (self.program.advanced.items) |item| if (item.pattern.pattern.jit) |program| {
            programs += 1;
            bytes += program.memory.len;
        };
        return .{ .programs = programs, .bytes = bytes };
    }
    pub fn hasAbsoluteStart(self: *const Matcher) bool {
        return self.program.absolute_start;
    }
    pub fn allowAbsoluteStart(self: *Matcher, allowed: bool) void {
        for (self.runners.items) |*runner| runner.vm.absolute_start_allowed = allowed;
        for (self.advanced.items) |*runner| runner.runner.absolute_start_allowed = allowed;
    }
    /// Conservative sum covers nested assertions and subprograms. Admission is
    /// explicit; an accepted pattern never silently loses rendering context.
    pub fn renderContextBytes(self: *const Matcher) !usize {
        var bytes: usize = 0;
        for (self.program.advanced.items) |item| if (regex.hasLineAnchors(item.pattern.pattern.states)) {
            bytes = 1;
            break;
        };
        for (self.program.advanced.items) |item| for (item.pattern.pattern.states) |state| {
            if (state.tag == .look_shift) {
                bytes += @as(usize, state.second) * 4;
                if (bytes > 1024 * 1024) return error.RegexContextTooLarge;
            }
        };
        return bytes;
    }
    pub fn multilineUsesMatches(self: *const Matcher) bool {
        if (self.program.advanced.items.len > 0) return true;
        const terminator: u6 = if (self.options.null_data) 0 else 10;
        for (self.program.direct.items) |item| if (std.mem.indexOfScalar(u8, item.bytes, terminator) != null) return true;
        for (self.program.regular.items) |item| for (item.pattern.states) |state| {
            if ((state.tag == .consume or state.tag == .repeat_consume) and state.ascii_mask[0] & (@as(u64, 1) << terminator) != 0) return true;
        };
        return false;
    }
    pub fn joinsMultilineJson(self: *const Matcher) bool {
        if (self.multilineUsesMatches()) return true;
        for (self.program.regular.items) |item| if (regex.hasLineAnchors(item.pattern.states)) return true;
        for (self.program.advanced.items) |item| if (regex.hasLineAnchors(item.pattern.pattern.states)) return true;
        return false;
    }
    pub fn matches(self: *Matcher, text: []const u8) !bool {
        if (self.program.direct.items.len + self.program.regular.items.len + self.program.advanced.items.len == 0) return true;
        for (self.program.direct.items) |item| if (regex.literal.find(text, item.bytes, 0, item.insensitive, self.options.unicode, self.options.word, self.options.whole_line) != null) return true;
        const budget = self.options.dfa_size_limit / @max(1, self.options.jobs) / @max(1, self.program.regular.items.len);
        for (self.runners.items, self.program.regular.items) |*runner, *item| {
            if (item.pattern.prefilter.len > 0 and regex.literal.findExact(text, item.pattern.prefilter, 0) == null) continue;
            if (runner.dfa_allowed and runner.dfa == null) {
                runner.dfa = regex.Dfa.init(self.a, &item.pattern, budget) catch |err| switch (err) {
                    error.DfaCacheFull, error.UnsupportedDfa => blk: {
                        runner.dfa_allowed = false;
                        break :blk null;
                    },
                    else => return err,
                };
            }
            if (runner.dfa) |*dfa| {
                if (dfa.matches(text) catch |err| switch (err) {
                    error.DfaCacheFull => blk: {
                        dfa.deinit();
                        runner.dfa = null;
                        runner.dfa_allowed = false;
                        break :blk runner.vm.find(text, 0) != null;
                    },
                    else => return err,
                }) return true;
            } else if (runner.vm.find(text, 0) != null) return true;
            if (runner.vm.failure) |err| return err;
        }
        for (self.advanced.items) |*runner| if (try runner.find(text, 0) != null) return true;
        return false;
    }
    /// Counts retain only the current VM result, even on very dense lines.
    pub const Count = struct { total: usize, terminal_empty: bool = false };
    pub fn count(self: *Matcher, text: []const u8, limit: usize, terminated: bool) !Count {
        if (limit == std.math.maxInt(usize) and !self.options.word and !self.options.whole_line and self.program.direct.items.len == 1 and self.program.regular.items.len == 0 and self.program.advanced.items.len == 0) {
            const item = self.program.direct.items[0];
            if (item.bytes.len == 1) return .{ .total = regex.literal.countByte(text, item.bytes[0], item.insensitive, self.options.unicode) };
        }
        var result: usize = 0;
        var terminal_empty = false;
        var cursor: usize = 0;
        var previous_end: ?usize = null;
        while (self.find(text, cursor)) |hit| {
            if (!(hit.span.start == hit.span.end and previous_end != null and previous_end.? == hit.span.end)) {
                result += 1;
                terminal_empty = !terminated and hit.span.start == text.len and hit.span.end == text.len;
                previous_end = hit.span.end;
                if (result == limit) break;
            }
            cursor = if (hit.span.start == hit.span.end) self.advance(text, hit.span.end) else hit.span.end;
            if (cursor > text.len or (!terminated and cursor == text.len)) break;
        }
        if (self.failure) |err| return err;
        return .{ .total = result, .terminal_empty = terminal_empty };
    }
    pub fn advance(self: *Matcher, text: []const u8, pos: usize) usize {
        if (pos >= text.len) return text.len + 1;
        if (!self.advance_utf8) return pos + 1;
        // PCRE-style UTF matching advances a codepoint after an empty match;
        // the default byte search engine deliberately also visits byte offsets.
        const n = std.unicode.utf8ByteSequenceLength(text[pos]) catch return pos + 1;
        if (n > text.len - pos) return pos + 1;
        _ = std.unicode.utf8Decode(text[pos..][0..n]) catch return pos + 1;
        return pos + n;
    }
    pub fn replacement(a: Allocator, template: []const u8, text: []const u8, m: Match) ![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(a);
        var i: usize = 0;
        while (i < template.len) {
            if (template[i] != '$') {
                try out.append(a, template[i]);
                i += 1;
                continue;
            }
            i += 1;
            if (i == template.len) {
                try out.append(a, '$');
                break;
            }
            if (template[i] == '$') {
                try out.append(a, '$');
                i += 1;
                continue;
            }
            const braced = template[i] == '{';
            if (braced) i += 1;
            const start = i;
            while (i < template.len and (std.ascii.isAlphanumeric(template[i]) or template[i] == '_')) i += 1;
            if (start == i or (braced and (i == template.len or template[i] != '}'))) {
                try out.append(a, '$');
                continue;
            }
            const key = template[start..i];
            if (braced) i += 1;
            var group: ?usize = std.fmt.parseInt(usize, key, 10) catch null;
            if (group == null) for (m.names) |name| if (std.mem.eql(u8, key, name.name)) {
                // PCRE2 capture-name lookup exposes the final duplicate slot.
                group = name.index;
            };
            if (group) |g| if (g < m.captures.len) {
                if (m.captures[g]) |span| try out.appendSlice(a, (m.capture_text orelse text)[span.start..span.end]);
            };
        }
        return out.toOwnedSlice(a);
    }
};
fn quote(a: Allocator, raw: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    for (raw) |c| {
        if (std.mem.indexOfScalar(u8, "\\.*+?()[]{}^$|", c) != null) try out.append(a, '\\');
        try out.append(a, c);
    }
    return out.toOwnedSlice(a);
}
test "Unicode captures and replacement use source byte ranges" {
    const a = std.testing.allocator;
    var o = Options{ .ignore_case = true };
    defer o.deinit(a);
    try o.patterns.append(a, "(?P<word>\\p{L}+)=(\\d+)");
    var m = try Matcher.init(a, &o);
    defer m.deinit();
    const source = "!Été=42!";
    const found = m.find(source, 0).?;
    try std.testing.expectEqualStrings("Été=42", source[found.span.start..found.span.end]);
    const value = try Matcher.replacement(a, "${word}:$2:$$", source, found);
    defer a.free(value);
    try std.testing.expectEqualStrings("Été:42:$", value);
}

test "worker clones retain compiled programs after their original matcher is freed" {
    const a = std.testing.allocator;
    var o = Options{};
    defer o.deinit(a);
    try o.patterns.append(a, "literal");
    try o.patterns.append(a, "a[0-9]+b");
    var original = try Matcher.init(a, &o);
    var worker = try original.clone(a);
    original.deinit();
    defer worker.deinit();
    try std.testing.expect(try worker.matches("literal"));
    try std.testing.expect(try worker.matches("a123b"));
    try std.testing.expect(!try worker.matches("a123c"));
    try std.testing.expectEqualStrings("a123b", "a123b"[worker.find("a123b", 0).?.span.start..worker.find("a123b", 0).?.span.end]);
}
