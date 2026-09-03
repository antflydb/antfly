// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

const std = @import("std");

pub const Route = enum {
    cli,
    data,
    ha,
    inference,
    metadata,
    serverless,
    standalone,
    cloud,
    completion,
    help,
    version,
};

pub const Command = struct {
    name: []const u8,
    description: []const u8,
    route: Route,
    subcommands: []const []const u8 = &.{},
};

const table_subcommands = [_][]const u8{ "create", "drop", "list", "get" };
const artifact_subcommands = [_][]const u8{ "list", "get", "put", "delete", "reprocess", "job" };
const agents_subcommands = [_][]const u8{ "retrieval", "query-builder" };
const auth_subcommands = [_][]const u8{ "me", "users", "permissions", "roles", "row-filters", "subjects", "api-keys" };
const inference_subcommands = [_][]const u8{
    "run",          "embed",            "classify",  "generate",
    "chat",         "compile-artifact", "export",    "quantize",
    "run-artifact", "transcribe",       "read",      "extract",
    "compare",      "finetune",         "smoke",     "list",
    "pull",         "convert",          "cuda-info",
};
const lite_subcommands = [_][]const u8{
    "init",           "status",  "info",   "batch",      "lookup",
    "scan",           "query",   "index",  "enrichment", "schema",
    "run-until-idle", "backup",  "export", "snapshot",   "restore",
    "import",         "promote", "check",  "compact",    "vacuum",
    "serve",
};
const serverless_subcommands = [_][]const u8{ "api", "query", "maintenance", "combined" };
const internal_subcommands = [_][]const u8{"metadata"};
const completion_subcommands = [_][]const u8{ "bash", "zsh", "fish" };

/// The command table is shared by top-level dispatch and completion rendering.
/// Adding a top-level command here therefore cannot silently leave packaged
/// completions behind.
pub const commands = [_]Command{
    .{ .name = "data", .description = "Run a data node", .route = .data },
    .{ .name = "metadata", .description = "Run a metadata node", .route = .metadata },
    .{ .name = "standalone", .description = "Run a standalone server", .route = .standalone },
    .{ .name = "swarm", .description = "Run a standalone server (legacy alias)", .route = .standalone },
    .{ .name = "inference", .description = "Manage the inference runtime", .route = .inference, .subcommands = &inference_subcommands },
    .{ .name = "serverless", .description = "Run serverless commands", .route = .serverless, .subcommands = &serverless_subcommands },
    .{ .name = "lite", .description = "Manage embedded Antfly Lite databases", .route = .standalone, .subcommands = &lite_subcommands },
    .{ .name = "ha", .description = "Manage local hot-standby HA", .route = .ha },
    .{ .name = "table", .description = "Manage tables", .route = .cli, .subcommands = &table_subcommands },
    .{ .name = "index", .description = "Manage indexes", .route = .cli, .subcommands = &table_subcommands },
    .{ .name = "artifact", .description = "Manage generated artifacts", .route = .cli, .subcommands = &artifact_subcommands },
    .{ .name = "query", .description = "Query table data", .route = .cli },
    .{ .name = "lookup", .description = "Look up a document by key", .route = .cli },
    .{ .name = "load", .description = "Bulk-load NDJSON data", .route = .cli },
    .{ .name = "insert", .description = "Insert a document", .route = .cli },
    .{ .name = "delete", .description = "Delete a document", .route = .cli },
    .{ .name = "agents", .description = "Run AI agents", .route = .cli, .subcommands = &agents_subcommands },
    .{ .name = "backup", .description = "Back up tables", .route = .cli },
    .{ .name = "restore", .description = "Restore tables", .route = .cli },
    .{ .name = "auth", .description = "Manage users and authorization", .route = .cli, .subcommands = &auth_subcommands },
    .{ .name = "internal", .description = "Run internal cluster commands", .route = .cli, .subcommands = &internal_subcommands },
    .{ .name = "cloud", .description = "Delegate to the Antfly Cloud CLI", .route = .cloud },
    .{ .name = "completion", .description = "Generate shell completion scripts", .route = .completion, .subcommands = &completion_subcommands },
    .{ .name = "help", .description = "Show command help", .route = .help },
    .{ .name = "version", .description = "Show the Antfly version", .route = .version },
};

pub fn findCommand(name: []const u8) ?Command {
    for (commands) |command| {
        if (std.mem.eql(u8, name, command.name)) return command;
    }
    return null;
}

pub const Shell = enum {
    bash,
    zsh,
    fish,

    pub fn parse(name: []const u8) !Shell {
        if (std.mem.eql(u8, name, "bash")) return .bash;
        if (std.mem.eql(u8, name, "zsh")) return .zsh;
        if (std.mem.eql(u8, name, "fish")) return .fish;
        return error.UnsupportedShell;
    }
};

pub fn write(shell: Shell, writer: *std.Io.Writer) !void {
    switch (shell) {
        .bash => try writeBash(writer),
        .zsh => try writeZsh(writer),
        .fish => try writeFish(writer),
    }
}

fn writeCommandNames(writer: *std.Io.Writer, command_list: []const Command) !void {
    for (command_list, 0..) |command, index| {
        if (index != 0) try writer.writeByte(' ');
        try writer.writeAll(command.name);
    }
}

fn writeSubcommandNames(writer: *std.Io.Writer, subcommands: []const []const u8) !void {
    for (subcommands, 0..) |subcommand, index| {
        if (index != 0) try writer.writeByte(' ');
        try writer.writeAll(subcommand);
    }
}

fn writeBash(writer: *std.Io.Writer) !void {
    try writer.writeAll(
        \\# Generated by `antfly completion bash`; do not edit.
        \\_antfly() {
        \\  local cur
        \\  COMPREPLY=()
        \\  cur="${COMP_WORDS[COMP_CWORD]}"
        \\  if (( COMP_CWORD == 1 )); then
        \\    COMPREPLY=($(compgen -W "
    );
    try writeCommandNames(writer, &commands);
    try writer.writeAll(
        \\" -- "$cur"))
        \\    return
        \\  fi
        \\  case "${COMP_WORDS[1]}" in
    );
    try writer.writeByte('\n');
    for (commands) |command| {
        if (command.subcommands.len == 0) continue;
        try writer.print("    {s}) COMPREPLY=($(compgen -W \"", .{command.name});
        try writeSubcommandNames(writer, command.subcommands);
        try writer.writeAll("\" -- \"$cur\")) ;;\n");
    }
    try writer.writeAll(
        \\  esac
        \\}
        \\complete -F _antfly antfly
        \\
    );
}

fn writeZsh(writer: *std.Io.Writer) !void {
    try writer.writeAll(
        \\#compdef antfly
        \\# Generated by `antfly completion zsh`; do not edit.
        \\
        \\_antfly() {
        \\  local -a commands subcommands
        \\  commands=(
    );
    try writer.writeByte('\n');
    for (commands) |command| {
        try writer.print("    '{s}:{s}'\n", .{ command.name, command.description });
    }
    try writer.writeAll(
        \\  )
        \\  if (( CURRENT == 2 )); then
        \\    _describe 'command' commands
        \\    return
        \\  fi
        \\
        \\  case "$words[2]" in
    );
    try writer.writeByte('\n');
    for (commands) |command| {
        if (command.subcommands.len == 0) continue;
        try writer.print("    {s}) subcommands=(", .{command.name});
        try writeSubcommandNames(writer, command.subcommands);
        try writer.writeAll(") ;;\n");
    }
    try writer.writeAll(
        \\  esac
        \\  if (( ${#subcommands[@]} )); then
        \\    _describe 'subcommand' subcommands
        \\  fi
        \\}
        \\
        \\compdef _antfly antfly
        \\
    );
}

fn writeFish(writer: *std.Io.Writer) !void {
    try writer.writeAll(
        \\# Generated by `antfly completion fish`; do not edit.
        \\complete -c antfly -f
        \\
    );
    for (commands) |command| {
        try writer.print("complete -c antfly -n '__fish_use_subcommand' -a '{s}' -d '{s}'\n", .{ command.name, command.description });
        if (command.subcommands.len == 0) continue;
        try writer.print("complete -c antfly -n '__fish_seen_subcommand_from {s}' -a '", .{command.name});
        try writeSubcommandNames(writer, command.subcommands);
        try writer.writeAll("'\n");
    }
}

test "command table drives routes and completion entries" {
    try std.testing.expectEqual(Route.standalone, findCommand("swarm").?.route);
    try std.testing.expectEqual(Route.cli, findCommand("table").?.route);
    try std.testing.expectEqual(Route.completion, findCommand("completion").?.route);
    try std.testing.expect(findCommand("termite") == null);
}

test "zsh completion contains nested inference and completion commands" {
    var output: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer output.deinit();
    try write(.zsh, &output.writer);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "inference) subcommands=(run embed classify") != null);
    try std.testing.expect(std.mem.indexOf(u8, output.written(), "completion) subcommands=(bash zsh fish)") != null);
}
