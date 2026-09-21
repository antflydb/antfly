// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Remote-maintenance vocabulary shared by parsing, help and completion.
pub const Action = enum { issues, repair, retry, replay, rebuild, refresh, pause, @"resume", delete, status, advance, cancel };
pub const Resource = enum { index, artifact };
pub const Description = struct {
    action: Action,
    description: []const u8,
    artifact: bool = false,
    artifact_description: ?[]const u8 = null,

    pub fn text(self: Description, resource: Resource) []const u8 {
        return if (resource == .artifact) self.artifact_description orelse self.description else self.description;
    }
};
pub const actions = [_]Description{
    .{ .action = .issues, .description = "List repair issues", .artifact = true },
    .{ .action = .repair, .description = "Start a durable repair job", .artifact = true },
    .{ .action = .retry, .description = "Retry failed relational index owners using exact status proofs" },
    .{ .action = .replay, .description = "Replay saved relational maintenance requests (--recovery-file)" },
    .{ .action = .rebuild, .description = "Force an index or graph metric rebuild" },
    .{ .action = .refresh, .description = "Refresh a graph metric (--metric)" },
    .{ .action = .pause, .description = "Pause automatic repair or graph maintenance" },
    .{ .action = .@"resume", .description = "Resume automatic repair or graph maintenance" },
    .{ .action = .delete, .description = "Clear graph metric materialization (--metric)" },
    .{ .action = .status, .description = "Get index status or repair job status (--job)", .artifact = true, .artifact_description = "Get repair job status (--job)" },
    .{ .action = .advance, .description = "Advance a repair job (--job)", .artifact = true },
    .{ .action = .cancel, .description = "Cancel the current index repair or a repair job (--job)", .artifact = true, .artifact_description = "Cancel a repair job (--job)" },
};
pub fn usage(comptime resource: Resource) []const u8 {
    comptime var text: []const u8 = "usage: antfly " ++ @tagName(resource) ++ " maintenance <action> --table <table> [options]\n\n";
    inline for (actions) |action| {
        if (resource == .index or action.artifact) text = text ++ "  " ++ @tagName(action.action) ++ "  " ++ (comptime action.text(resource)) ++ "\n";
    }
    text = text ++ "\n  --job <id>              Select a job for status/advance/cancel\n" ++
        "  --index <name>          Restrict work to one index\n" ++
        "  --once                  Run one bounded repair/control pass\n" ++
        "  --cursor <cursor>       Continue a bounded pass\n" ++
        "  --limit <n>             Per-pass bound (issues: 1..500; work: 1..1000)\n";
    if (resource == .index) text = text ++ "  --metric <name>         Select graph metric lifecycle actions\n  --repair-id <id>        Fence controls against a newer attempt\n\nRepair/rebuild starts a job and advances one pass. Controls run across all groups under the server owner. --once exposes bounded passes.\n";
    if (resource == .index) text = text ++ "  --recovery-file <path>  Saved JSONL requests for replay (requires --table and --index)\nRelational repair/rebuild/retry automatically selects eligible owners and submits at most 128 proofs per request. Admission is per owner, not globally atomic. Exact requests are synced to an exclusive 0600 .antfly-index-maintenance-*.jsonl recovery file before submission. Replay validates the complete file before submitting its original requests without refreshing proofs; a torn final line is ignored. Relational pause/resume/cancel and pass cursors are unsupported.\n";
    if (resource == .artifact) text = text ++ "  --kind <kind>           Filter artifact repair kind\n\nRepair starts a job and advances one pass. --once exposes a bounded pass.\n";
    return text;
}
