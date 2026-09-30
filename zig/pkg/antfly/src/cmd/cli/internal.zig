// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const antfly_client = @import("antfly-client");
const cli = @import("mod.zig");

pub fn run(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const subcommand = args.next() orelse {
        cli.fatal("internal requires a subcommand: metadata or store-root", .{});
    };

    if (std.mem.eql(u8, subcommand, "metadata")) return metadata(allocator, io, client, args);
    if (std.mem.eql(u8, subcommand, "store-root")) return storeRoot(allocator, io, client, args);

    cli.fatal("unknown internal subcommand: {s}", .{subcommand});
}

fn storeRoot(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const action = args.next() orelse cli.fatal("store-root requires proof, enroll, or status", .{});
    if (std.mem.eql(u8, action, "proof")) return storeRootProof(allocator, io, args);
    if (std.mem.eql(u8, action, "enroll")) return storeRootEnroll(allocator, io, client, args);
    if (std.mem.eql(u8, action, "status")) return storeRootStatus(allocator, io, client, args);
    cli.fatal("unknown store-root action: {s}", .{action});
}

/// Run on the data node with permission to read its private signing checkpoint.
/// The checkpoint is never printed or sent; only the signed, cluster-bound
/// proof is emitted for an administrator to inspect and approve separately.
fn storeRootProof(allocator: std.mem.Allocator, io: std.Io, args: *std.process.Args.Iterator) !void {
    var root_dir: ?[]const u8 = null;
    var metadata_incarnation: ?[]const u8 = null;
    var node_id_raw: ?[]const u8 = null;
    var store_id_raw: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--replica-root-dir")) cli.takeUniqueValue(args, &root_dir, arg) else if (std.mem.eql(u8, arg, "--metadata-incarnation")) cli.takeUniqueValue(args, &metadata_incarnation, arg) else if (std.mem.eql(u8, arg, "--node-id")) cli.takeUniqueValue(args, &node_id_raw, arg) else if (std.mem.eql(u8, arg, "--store-id")) cli.takeUniqueValue(args, &store_id_raw, arg) else cli.fatal("unknown store-root proof option: {s}", .{arg});
    }
    const incarnation = metadata_incarnation orelse cli.fatal("--metadata-incarnation is required; use the target metadata cluster's canonical identity", .{});
    if (incarnation.len != 32) return error.InvalidMetadataIncarnation;
    var incarnation_bytes: [32]u8 = undefined;
    @memcpy(&incarnation_bytes, incarnation);
    const node_id = try std.fmt.parseInt(u64, node_id_raw orelse cli.fatal("--node-id is required", .{}), 10);
    const store_id = try std.fmt.parseInt(u64, store_id_raw orelse cli.fatal("--store-id is required", .{}), 10);
    const signing = try @import("../../storage/db/root_signing_identity.zig").load(
        allocator,
        io,
        root_dir orelse cli.fatal("--replica-root-dir is required", .{}),
    );
    const request = try @import("../../metadata/store_root_enrollment.zig").Request.sign(.{
        .metadata_incarnation = incarnation_bytes,
        .node_id = node_id,
        .store_id = store_id,
        .root_incarnation = signing.root_incarnation,
        .public_key = signing.public_key,
    }, signing.seed);
    const encoded = try @import("../../api/store_root_enrollment_http.zig").encodeAlloc(allocator, request);
    defer allocator.free(encoded);
    cli.writeStdout(io, encoded);
    cli.writeStdout(io, "\n");
}

fn storeRootEnroll(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var path: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--file")) cli.takeUniqueValue(args, &path, arg) else cli.fatal("unknown store-root enroll option: {s}", .{arg});
    }
    const bytes = try cli.readFileAlloc(io, allocator, path orelse cli.fatal("--file is required", .{}), @import("../../api/store_root_enrollment_http.zig").max_body_bytes);
    defer allocator.free(bytes);
    const proof_http = @import("../../api/store_root_enrollment_http.zig");
    var parsed = try std.json.parseFromSlice(proof_http.Request, allocator, bytes, .{ .ignore_unknown_fields = false });
    defer parsed.deinit();
    _ = try parsed.value.toDomain();
    if (parsed.value.identity.node_id > std.math.maxInt(i64) or parsed.value.identity.store_id > std.math.maxInt(i64))
        return error.InvalidStoreRootEnrollment;
    var response = try client.inner.enrollStoreRoot(.{
        .identity = .{
            .metadata_incarnation = &parsed.value.identity.metadata_incarnation,
            .node_id = @intCast(parsed.value.identity.node_id),
            .store_id = @intCast(parsed.value.identity.store_id),
            .root_incarnation = parsed.value.identity.root_incarnation,
            .public_key = parsed.value.identity.public_key,
        },
        .signature = parsed.value.signature,
    });
    defer response.deinit();
    cli.expectHttpSuccess(&response);
    return cli.printResponse(allocator, io, &response);
}

fn storeRootStatus(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    var path: ?[]const u8 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--file")) cli.takeUniqueValue(args, &path, arg) else cli.fatal("unknown store-root status option: {s}", .{arg});
    }
    const proof_http = @import("../../api/store_root_enrollment_http.zig");
    const bytes = try cli.readFileAlloc(io, allocator, path orelse cli.fatal("--file is required", .{}), proof_http.max_body_bytes);
    defer allocator.free(bytes);
    var parsed = try std.json.parseFromSlice(proof_http.Request, allocator, bytes, .{ .ignore_unknown_fields = false });
    defer parsed.deinit();
    const proof = try parsed.value.toDomain();
    if (proof.identity.node_id > std.math.maxInt(i64) or proof.identity.store_id > std.math.maxInt(i64))
        return error.InvalidStoreRootEnrollment;
    var response = try client.inner.getStoreRootEnrollmentStatus(.{
        .metadata_incarnation = &proof.identity.metadata_incarnation,
        .node_id = @intCast(proof.identity.node_id),
        .store_id = @intCast(proof.identity.store_id),
        .root_incarnation = parsed.value.identity.root_incarnation,
        .public_key = parsed.value.identity.public_key,
    });
    defer response.deinit();
    cli.expectHttpSuccess(&response);
    return cli.printResponse(allocator, io, &response);
}

fn metadata(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient, args: *std.process.Args.Iterator) !void {
    const subcommand = args.next() orelse {
        return metadataStatus(allocator, io, client);
    };

    if (std.mem.eql(u8, subcommand, "status")) {
        cli.rejectRemainingArgs(args, "internal metadata status");
        return metadataStatus(allocator, io, client);
    }

    cli.fatal("unknown internal metadata subcommand: {s}", .{subcommand});
}

fn metadataStatus(allocator: std.mem.Allocator, io: std.Io, client: *antfly_client.AntflyClient) !void {
    var resp = try client.getStatus();
    defer resp.deinit();
    if (resp.data) |data| {
        try cli.writeJson(allocator, io, data.value);
    }
}
