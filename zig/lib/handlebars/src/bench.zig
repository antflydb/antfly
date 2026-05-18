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

const std = @import("std");
const hbs = @import("handlebars");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    std.debug.print("handlebars-zig benchmarks\n", .{});
    std.debug.print("========================\n\n", .{});

    benchSimple(allocator);
    benchEach(allocator);
    benchNested(allocator);
    benchParseOnly(allocator);
    benchCachedTemplate(allocator);
}

fn benchSimple(allocator: std.mem.Allocator) void {
    const template = "Hello {{name}}, you have {{count}} messages from {{sender}}.";
    const iterations = 100_000;

    const start = std.c.mach_absolute_time();

    for (0..iterations) |_| {
        var arena_state = std.heap.ArenaAllocator.init(allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        var m: hbs.ValueMap = .{};
        m.put(arena, "name", hbs.Value.str("User")) catch return;
        m.put(arena, "count", hbs.Value.int(42)) catch return;
        m.put(arena, "sender", hbs.Value.str("System")) catch return;

        _ = hbs.renderSimple(arena, template, .{ .map = m }) catch return;
    }

    const elapsed = std.c.mach_absolute_time() - start;
    const ns_per_op = elapsed / iterations;
    std.debug.print("simple render:     {d:>8} ns/op ({d} ops)\n", .{ ns_per_op, iterations });
}

fn benchEach(allocator: std.mem.Allocator) void {
    const template = "{{#each items}}{{@index}}: {{this}}\n{{/each}}";
    const iterations = 50_000;

    const start = std.c.mach_absolute_time();

    for (0..iterations) |_| {
        var arena_state = std.heap.ArenaAllocator.init(allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        const items = [_]hbs.Value{
            hbs.Value.str("alpha"),
            hbs.Value.str("beta"),
            hbs.Value.str("gamma"),
            hbs.Value.str("delta"),
            hbs.Value.str("epsilon"),
        };
        var m: hbs.ValueMap = .{};
        m.put(arena, "items", .{ .array = &items }) catch return;

        _ = hbs.renderSimple(arena, template, .{ .map = m }) catch return;
    }

    const elapsed = std.c.mach_absolute_time() - start;
    const ns_per_op = elapsed / iterations;
    std.debug.print("each (5 items):    {d:>8} ns/op ({d} ops)\n", .{ ns_per_op, iterations });
}

fn benchNested(allocator: std.mem.Allocator) void {
    const template =
        \\{{#if show}}
        \\  {{#each users}}
        \\    Name: {{name}}, Email: {{email}}
        \\  {{/each}}
        \\{{else}}
        \\  No users
        \\{{/if}}
    ;
    const iterations = 50_000;

    const start = std.c.mach_absolute_time();

    for (0..iterations) |_| {
        var arena_state = std.heap.ArenaAllocator.init(allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        var user_a: hbs.ValueMap = .{};
        user_a.put(arena, "name", hbs.Value.str("Alice")) catch return;
        user_a.put(arena, "email", hbs.Value.str("alice@example.com")) catch return;
        var user_b: hbs.ValueMap = .{};
        user_b.put(arena, "name", hbs.Value.str("Bob")) catch return;
        user_b.put(arena, "email", hbs.Value.str("bob@example.com")) catch return;

        const users = [_]hbs.Value{ .{ .map = user_a }, .{ .map = user_b } };
        var m: hbs.ValueMap = .{};
        m.put(arena, "show", hbs.Value.bln(true)) catch return;
        m.put(arena, "users", .{ .array = &users }) catch return;

        _ = hbs.renderSimple(arena, template, .{ .map = m }) catch return;
    }

    const elapsed = std.c.mach_absolute_time() - start;
    const ns_per_op = elapsed / iterations;
    std.debug.print("nested if+each:    {d:>8} ns/op ({d} ops)\n", .{ ns_per_op, iterations });
}

fn benchParseOnly(allocator: std.mem.Allocator) void {
    const template = "{{#each items}}{{@index}}: {{name}} ({{email}}){{/each}}";
    const iterations = 200_000;

    const start = std.c.mach_absolute_time();

    for (0..iterations) |_| {
        var arena_state = std.heap.ArenaAllocator.init(allocator);
        defer arena_state.deinit();
        _ = hbs.Parser.parse(template, arena_state.allocator()) catch return;
    }

    const elapsed = std.c.mach_absolute_time() - start;
    const ns_per_op = elapsed / iterations;
    std.debug.print("parse only:        {d:>8} ns/op ({d} ops)\n", .{ ns_per_op, iterations });
}

fn benchCachedTemplate(allocator: std.mem.Allocator) void {
    const source = "Hello {{name}}, you have {{count}} messages from {{sender}}.";
    const iterations = 200_000;

    var tpl = hbs.Template.init(allocator, source) catch return;
    defer tpl.deinit();

    const start = std.c.mach_absolute_time();

    for (0..iterations) |_| {
        var arena_state = std.heap.ArenaAllocator.init(allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        var m: hbs.ValueMap = .{};
        m.put(arena, "name", hbs.Value.str("User")) catch return;
        m.put(arena, "count", hbs.Value.int(42)) catch return;
        m.put(arena, "sender", hbs.Value.str("System")) catch return;

        _ = tpl.renderSimple(arena, .{ .map = m }) catch return;
    }

    const elapsed = std.c.mach_absolute_time() - start;
    const ns_per_op = elapsed / iterations;
    std.debug.print("cached template:   {d:>8} ns/op ({d} ops)\n", .{ ns_per_op, iterations });
}
