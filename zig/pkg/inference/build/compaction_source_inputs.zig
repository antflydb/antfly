// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Extend the canonical manifest through relative Zig imports and embedded code.
const std = @import("std");

/// Module aliases are supplied by the build graph. Runtime binary/model receipts
/// bind those separately; only literal relative source dependencies belong here.
pub fn literalInputs(a: std.mem.Allocator, source: [:0]const u8) ![][]const u8 {
    var result: std.ArrayList([]const u8) = .empty;
    var tokens = std.zig.Tokenizer.init(source);
    while (true) {
        const token = tokens.next();
        if (token.tag == .eof) break;
        if (token.tag != .builtin) continue;
        const name = source[token.loc.start..token.loc.end];
        const imported = std.mem.eql(u8, name, "@import");
        const embedded = std.mem.eql(u8, name, "@embedFile");
        if (!imported and !embedded) continue;
        if (tokens.next().tag != .l_paren) return error.InvalidSourceDependency;
        const value = tokens.next();
        if (value.tag != .string_literal) return error.NonliteralSourceDependency;
        const path = try std.zig.string_literal.parseAlloc(a, source[value.loc.start..value.loc.end]);
        var ending = tokens.next();
        if (ending.tag == .comma) ending = tokens.next();
        if (ending.tag != .r_paren) {
            // Existing video tests choose data fixtures in an inline loop.
            // Recognize their actual test declaration rather than skipping a
            // runtime import or an arbitrary computed shader path.
            if (!embedded or !try testDataExpression(a, source, token.loc.start, path))
                return error.NonliteralSourceDependency;
            var depth: usize = 1;
            while (depth != 0) {
                const next = tokens.next();
                switch (next.tag) {
                    .l_paren => depth += 1,
                    .r_paren => depth -= 1,
                    .eof => return error.InvalidSourceDependency,
                    else => {},
                }
            }
            continue;
        }
        if (imported and !std.mem.endsWith(u8, path, ".zig")) continue;
        // Model fixtures and backend-specific compiled artifacts have separate
        // bindings. Do not make a Metal build require a CUDA fatbin download.
        if (embedded and !isCode(path)) continue;
        try result.append(a, path);
    }
    return result.toOwnedSlice(a);
}

fn testDataExpression(a: std.mem.Allocator, source: [:0]const u8, offset: usize, prefix: []const u8) !bool {
    const data_path = for ([_][]const u8{ "testdata/", "../testdata/", "../../testdata/" }) |base| {
        if (std.mem.startsWith(u8, prefix, base) and std.mem.indexOf(u8, prefix[base.len..], "../") == null)
            break true;
    } else false;
    if (!data_path or !std.mem.endsWith(u8, prefix, "/")) return false;
    var tree = try std.zig.Ast.parse(a, source, .{});
    defer tree.deinit(a);
    if (tree.errors.len != 0) return false;
    for (tree.rootDecls()) |node| {
        if (tree.nodeTag(node) != .test_decl) continue;
        if (tree.tokenStart(tree.firstToken(node)) <= offset and offset <= tree.tokenStart(tree.lastToken(node)))
            return true;
    }
    return false;
}

fn isCode(path: []const u8) bool {
    inline for (.{ ".zig", ".metal", ".m", ".c", ".h", ".cu", ".cuh", ".inc" }) |suffix|
        if (std.mem.endsWith(u8, path, suffix)) return true;
    return false;
}

/// Seeds include the two normalized registry inputs. Keeping them in `seen`
/// prevents a relative import from reintroducing their unnormalized bytes.
pub fn addTransitive(b: *std.Build, root: std.Build.LazyPath, run: *std.Build.Step.Run, seeds: []const []const u8) void {
    const base = switch (root) {
        .src_path => |source| source.owner.root.join(b.allocator, source.sub_path) catch @panic("OOM"),
        else => @panic("compaction source root must belong to a build package"),
    };
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    var queue: std.ArrayList([]const u8) = .empty;
    for (seeds) |seed| {
        const path = std.Io.Dir.path.resolvePosix(b.allocator, &.{seed}) catch @panic("OOM");
        if ((seen.getOrPut(b.allocator, path) catch @panic("OOM")).found_existing) continue;
        queue.append(b.allocator, path) catch @panic("OOM");
    }
    const seed_count = queue.items.len;
    var index: usize = 0;
    while (index < queue.items.len) : (index += 1) {
        const path = queue.items[index];
        if (!std.mem.endsWith(u8, path, ".zig")) continue;
        const lazy = root.path(b, path);
        // The configure cache must also invalidate when an existing source
        // imports a new helper; run-step file arguments alone cannot do that.
        b.dependOnFileContents(lazy);
        const resolved = base.join(b.allocator, path) catch @panic("OOM");
        const bytes = resolved.root_dir.handle.readFileAlloc(b.graph.io, resolved.sub_path, b.allocator, .limited(16 * 1024 * 1024)) catch
            @panic("unable to read compaction source dependency");
        const source = b.allocator.dupeSentinel(u8, bytes, 0) catch @panic("OOM");
        const inputs = literalInputs(b.allocator, source) catch |err| {
            std.debug.print("compaction source dependency {s}: {s}\n", .{ path, @errorName(err) });
            @panic("invalid compaction source dependency");
        };
        for (inputs) |input| {
            const child = std.Io.Dir.path.resolvePosix(b.allocator, &.{ std.fs.path.dirname(path) orelse ".", input }) catch @panic("OOM");
            if ((seen.getOrPut(b.allocator, child) catch @panic("OOM")).found_existing) continue;
            queue.append(b.allocator, child) catch @panic("OOM");
        }
    }
    const extra = queue.items[seed_count..];
    std.mem.sort([]const u8, extra, {}, struct {
        fn less(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.order(u8, lhs, rhs) == .lt;
        }
    }.less);
    for (extra) |path| {
        const logical = if (std.mem.startsWith(u8, path, "src/")) path[4..] else path;
        run.addArgs(&.{ "source", logical });
        run.addFileArg2(root.path(b, path), .{ .make_absolute = true });
    }
}

test "compaction source dependencies include nested Zig and embedded code without comments or data artifacts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const paths = try literalInputs(arena.allocator(),
        \\const alias = @import("std");
        \\const helper = @import("../runtime/helper.zig");
        \\const shader = @embedFile("attention.metal");
        \\const fixture = @embedFile("weights.fatbin");
        \\// @import("comment.zig")
        \\const text = "@import(\"string.zig\")";
    );
    try std.testing.expectEqual(@as(usize, 2), paths.len);
    try std.testing.expectEqualStrings("../runtime/helper.zig", paths[0]);
    try std.testing.expectEqualStrings("attention.metal", paths[1]);
}

test "compaction source dependencies reject nonliteral imports and embeds" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expectError(error.NonliteralSourceDependency, literalInputs(arena.allocator(), "const x = @import(selected_path);"));
    try std.testing.expectError(error.NonliteralSourceDependency, literalInputs(arena.allocator(), "const x = @import(\"part/\" ++ \"helper.zig\");"));
    try std.testing.expectError(error.NonliteralSourceDependency, literalInputs(arena.allocator(), "const x = @embedFile(selected_path);"));
}

test "compaction identity permits computed test data but rejects computed runtime or shader dependencies" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const paths = try literalInputs(arena.allocator(),
        \\const helper = @import("nested/helper.zig");
        \\test "video fixture" {
        \\    const bytes = @embedFile("../testdata/video/" ++ name);
        \\}
    );
    try std.testing.expectEqual(@as(usize, 1), paths.len);
    try std.testing.expectEqualStrings("nested/helper.zig", paths[0]);
    try std.testing.expectError(error.NonliteralSourceDependency, literalInputs(arena.allocator(),
        \\const bytes = @embedFile("../testdata/video/" ++ name);
    ));
    try std.testing.expectError(error.NonliteralSourceDependency, literalInputs(arena.allocator(),
        \\test "computed shader" {
        \\    const bytes = @embedFile("../ops/" ++ name);
        \\}
    ));
}
