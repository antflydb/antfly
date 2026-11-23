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

//! Executable trusted label providers and identity-attached catalog entries.
//! Native resource transactions own authorization, policy veto, durability and
//! retirement. The SQL/WASM package registry has no policy-hook authority.
const std = @import("std");
const A = std.mem.Allocator;
pub const max_providers = 32;
pub const max_label_bytes = 64 * 1024;
pub const Class = enum { table, namespace, procedure };
pub const Object = struct {
    incarnation: [16]u8,
    database_id: u64,
    class: Class,
    id: u64,
    pub fn validate(self: Object) !void {
        if (self.database_id == 0 or self.id == 0 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidSecurityLabelRecord;
    }
};
/// Policy authorization issued by a trusted ingress after checking its
/// socket peer against an exact catalog preimage. Raw peer labels never
/// enter the consensus log. Apply must resolve the verifier's public key
/// from trusted catalog authority and supply its own expected cut.
pub const KernelRelabel = struct {
    object: Object,
    catalog_revision: u64,
    command_nonce: [16]u8,
    actor: [32]u8,
    subject: [32]u8,
    policy: [32]u8,
    policy_sequence: u32,
    authority_epoch: u64 = 0,
    previous: ?[32]u8,
    proposed: ?[32]u8,

    pub const Digest = [32]u8;
    const Signature = std.crypto.sign.Ed25519.Signature;
    const PublicKey = std.crypto.sign.Ed25519.PublicKey;

    pub fn labelDigest(value: ?[]const u8) ?Digest {
        const label = value orelse return null;
        var h = std.crypto.hash.Blake3.init(.{});
        h.update("antfly/selinux-object-context/v1");
        h.update(label);
        var result: Digest = undefined;
        h.final(&result);
        return result;
    }

    fn digest(self: KernelRelabel) !Digest {
        try self.object.validate();
        if (self.catalog_revision == 0 or std.mem.allEqual(u8, &self.command_nonce, 0) or
            std.mem.allEqual(u8, &self.actor, 0) or std.mem.allEqual(u8, &self.subject, 0) or
            std.mem.allEqual(u8, &self.policy, 0)) return error.InvalidSecurityLabelRecord;

        var h = std.crypto.hash.Blake3.init(.{});
        h.update("antfly/selinux-relabel-authorization/v1");
        h.update(&self.object.incarnation);
        h.update(&.{switch (self.object.class) {
            .table => 1,
            .namespace => 2,
            .procedure => 3,
        }});
        var integer: [8]u8 = undefined;
        for ([_]u64{ self.object.database_id, self.object.id, self.catalog_revision, self.policy_sequence, self.authority_epoch }) |value| {
            std.mem.writeInt(u64, &integer, value, .big);
            h.update(&integer);
        }
        h.update(&self.command_nonce);
        h.update(&self.actor);
        h.update(&self.subject);
        h.update(&self.policy);
        for ([_]?Digest{ self.previous, self.proposed }) |value| {
            h.update(&.{@intFromBool(value != null)});
            if (value) |present| h.update(&present);
        }
        var result: Digest = undefined;
        h.final(&result);
        return result;
    }

    /// Trusted ingress supplies a policy adapter and an authenticated peer
    /// context. Recheck the policy cut after authorization before signing.
    pub const Policy = struct {
        ptr: *anyopaque,
        snapshot: *const fn (*anyopaque) anyerror!Snapshot,
        authorize: *const fn (*anyopaque, Object, []const u8, ?[]const u8, ?[]const u8) anyerror!void,

        pub const Snapshot = struct { digest: Digest, sequence: u32 };

        pub fn approve(self: Policy, cut: KernelRelabel, peer: []const u8, previous: ?[]const u8, proposed: ?[]const u8, key: std.crypto.sign.Ed25519.KeyPair) !Approval {
            if (peer.len == 0 or !std.meta.eql(labelDigest(peer), @as(?Digest, cut.subject)) or
                !std.meta.eql(labelDigest(previous), cut.previous) or !std.meta.eql(labelDigest(proposed), cut.proposed))
                return error.SelinuxAdmissionChanged;
            const expected: Snapshot = .{ .digest = cut.policy, .sequence = cut.policy_sequence };
            if (!std.meta.eql(try self.snapshot(self.ptr), expected)) return error.SelinuxAdmissionChanged;
            try self.authorize(self.ptr, cut.object, peer, previous, proposed);
            if (!std.meta.eql(try self.snapshot(self.ptr), expected)) return error.SelinuxAdmissionChanged;
            return Approval.sign(cut, key);
        }
    };

    /// Replicated apply supplies the trust root and the current catalog cut
    /// independently of the wire approval. The subject digest is the only
    /// cut field attested exclusively by the trusted socket ingress.
    pub const Authority = struct {
        verifier: [PublicKey.encoded_length]u8,
        policy: Policy.Snapshot,
        epoch: u64 = 0,

        pub const CatalogCut = struct {
            object: Object,
            revision: u64,
            nonce: [16]u8,
            actor: Digest,
            previous: ?[]const u8,
            proposed: ?[]const u8,
        };

        pub fn verify(self: Authority, catalog: CatalogCut, approval: Approval) !void {
            try approval.verify(.{
                .object = catalog.object,
                .catalog_revision = catalog.revision,
                .command_nonce = catalog.nonce,
                .actor = catalog.actor,
                .subject = approval.cut.subject,
                .policy = self.policy.digest,
                .policy_sequence = self.policy.sequence,
                .authority_epoch = self.epoch,
                .previous = labelDigest(catalog.previous),
                .proposed = labelDigest(catalog.proposed),
            }, self.verifier);
        }
    };

    /// Process-local capability issued by the transport for an accepted
    /// connection. This type must never be decoded from a client request.
    pub const Ingress = struct {
        ptr: *anyopaque,
        approve: *const fn (*anyopaque, A, Authority, Authority.CatalogCut) anyerror!Approval,
    };

    pub const Approval = struct {
        cut: KernelRelabel,
        signature: [Signature.encoded_length]u8,

        pub fn sign(cut: KernelRelabel, key: std.crypto.sign.Ed25519.KeyPair) !Approval {
            return .{ .cut = cut, .signature = (try key.sign(&try cut.digest(), null)).toBytes() };
        }

        pub fn verify(self: Approval, expected: KernelRelabel, verifier: [PublicKey.encoded_length]u8) !void {
            if (!std.meta.eql(self.cut, expected)) return error.SelinuxAdmissionChanged;
            const key = PublicKey.fromBytes(verifier) catch return error.InvalidSelinuxApproval;
            Signature.fromBytes(self.signature).verify(&try expected.digest(), key) catch return error.InvalidSelinuxApproval;
        }
    };
};

/// A default label computed at authenticated ingress for one exact CREATE.
/// Apply reconstructs the parent identity, catalog cut and command digest
/// before publishing the child and its label in the same transaction.
pub const KernelCreate = struct {
    parent: Object,
    child_class: Class,
    catalog_revision: u64,
    command_nonce: [16]u8,
    actor: [32]u8,
    subject: [32]u8,
    policy: [32]u8,
    policy_sequence: u32,
    command: [32]u8,
    parent_label: [32]u8,
    child_label: [32]u8,

    const Signature = std.crypto.sign.Ed25519.Signature;
    const PublicKey = std.crypto.sign.Ed25519.PublicKey;

    fn digest(self: KernelCreate) ![32]u8 {
        try self.parent.validate();
        if (self.parent.class != .namespace or self.child_class == .namespace or
            self.catalog_revision == 0 or std.mem.allEqual(u8, &self.command_nonce, 0)) return error.InvalidSecurityLabelRecord;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/selinux-create-authorization/v1");
        hash.update(&self.parent.incarnation);
        hash.update(&.{if (self.child_class == .table) 1 else 2});
        var integer: [8]u8 = undefined;
        for ([_]u64{ self.parent.database_id, self.parent.id, self.catalog_revision, self.policy_sequence }) |value| {
            std.mem.writeInt(u64, &integer, value, .big);
            hash.update(&integer);
        }
        hash.update(&self.command_nonce);
        for ([_][32]u8{ self.actor, self.subject, self.policy, self.command, self.parent_label, self.child_label }) |value| {
            if (std.mem.allEqual(u8, &value, 0)) return error.InvalidSecurityLabelRecord;
            hash.update(&value);
        }
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }

    pub const Approval = struct {
        cut: KernelCreate,
        signature: [Signature.encoded_length]u8,

        pub fn sign(cut: KernelCreate, key: std.crypto.sign.Ed25519.KeyPair) !Approval {
            return .{ .cut = cut, .signature = (try key.sign(&try cut.digest(), null)).toBytes() };
        }

        pub fn verify(self: Approval, expected: KernelCreate, verifier: [PublicKey.encoded_length]u8) !void {
            if (!std.meta.eql(self.cut, expected)) return error.SelinuxAdmissionChanged;
            const key = PublicKey.fromBytes(verifier) catch return error.InvalidSelinuxApproval;
            Signature.fromBytes(self.signature).verify(&try expected.digest(), key) catch return error.InvalidSelinuxApproval;
        }
    };
};

test "SELinux CREATE authorization binds parent, command and both labels" {
    const key = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(1));
    const cut: KernelCreate = .{
        .parent = .{ .incarnation = @splat(1), .database_id = 1, .class = .namespace, .id = 2 },
        .child_class = .table,
        .catalog_revision = 3,
        .command_nonce = @splat(4),
        .actor = @splat(5),
        .subject = @splat(6),
        .policy = @splat(7),
        .policy_sequence = 8,
        .command = @splat(9),
        .parent_label = KernelRelabel.labelDigest("parent").?,
        .child_label = KernelRelabel.labelDigest("child").?,
    };
    const approval = try KernelCreate.Approval.sign(cut, key);
    const verifier = key.public_key.toBytes();
    try approval.verify(cut, verifier);
    var changed = cut;
    changed.parent.id += 1;
    try std.testing.expectError(error.SelinuxAdmissionChanged, approval.verify(changed, verifier));
    changed = cut;
    changed.command[0] ^= 1;
    try std.testing.expectError(error.SelinuxAdmissionChanged, approval.verify(changed, verifier));
    changed = cut;
    changed.parent_label[0] ^= 1;
    try std.testing.expectError(error.SelinuxAdmissionChanged, approval.verify(changed, verifier));
    changed = cut;
    changed.child_label[0] ^= 1;
    try std.testing.expectError(error.SelinuxAdmissionChanged, approval.verify(changed, verifier));
    var forged = approval;
    forged.cut = changed;
    try std.testing.expectError(error.InvalidSelinuxApproval, forged.verify(changed, verifier));
}

pub fn validateProvider(name: []const u8) !void {
    if (name.len == 0 or name.len > 128 or !std.unicode.utf8ValidateSlice(name) or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidSecurityLabelProvider;
}
pub fn validateLabel(label: ?[]const u8) !void {
    if (label) |value| {
        if (value.len > max_label_bytes) return error.SecurityLabelTooLarge;
        if (!std.unicode.utf8ValidateSlice(value) or std.mem.indexOfScalar(u8, value, 0) != null) return error.InvalidSecurityLabel;
    }
}
/// The authenticated native transaction checks owner identity before the
/// provider receives control. Callbacks are trusted deterministic evaluators;
/// they may veto but cannot publish effects or perform external I/O.
pub const Relabel = struct {
    object: Object,
    previous: ?[]const u8,
    proposed: ?[]const u8,
    authority: *const anyopaque,
};
pub const Provider = struct {
    name: []const u8,
    /// Immutable policy implementation/configuration identity. Captured in
    /// admission; ordered apply rejects a different provider registration.
    digest: [32]u8,
    context: *anyopaque,
    check: *const fn (*anyopaque, Relabel) anyerror!void,
};

/// Constructed by trusted server bootstrap, immutable while a transaction
/// borrows it. It intentionally has no SQL/API register(name) operation.
pub const Registry = struct {
    providers: []const Provider = &.{},

    pub fn validate(self: Registry) !void {
        if (self.providers.len > max_providers) return error.InvalidSecurityLabelProvider;
        for (self.providers, 0..) |provider, i| {
            try validateProvider(provider.name);
            if (std.mem.allEqual(u8, &provider.digest, 0)) return error.InvalidSecurityLabelProvider;
            for (self.providers[0..i]) |prior| if (std.mem.eql(u8, prior.name, provider.name)) return error.DuplicateSecurityLabelProvider;
        }
    }
    pub fn select(self: Registry, requested: ?[]const u8) !Provider {
        try self.validate();
        if (requested) |name| {
            for (self.providers) |provider| if (std.mem.eql(u8, provider.name, name)) return provider;
            return error.SecurityLabelProviderNotLoaded;
        }
        if (self.providers.len == 0) return error.NoSecurityLabelProviders;
        if (self.providers.len != 1) return error.SecurityLabelProviderRequired;
        return self.providers[0];
    }
};

pub const Entry = struct {
    version: u16 = 1,
    provider: []const u8,
    policy_digest: [32]u8,
    label: []const u8,
};
/// Restoring dormant metadata is permitted, but an active callback must never
/// reinterpret a prior label written under another immutable policy contract.
pub fn previousLabel(provider: Provider, entries: []const Entry) !?[]const u8 {
    return labelForPolicy(provider.name, provider.digest, entries);
}

pub fn labelForPolicy(name: []const u8, digest: [32]u8, entries: []const Entry) !?[]const u8 {
    try validateEntries(entries);
    for (entries) |entry| if (std.mem.eql(u8, entry.provider, name)) {
        if (!std.mem.eql(u8, &entry.policy_digest, &digest)) return error.CatalogGenerationChanged;
        return entry.label;
    };
    return null;
}
/// The current kernel policy checks the old context. The stored digest
/// records the policy that last approved the label.
pub fn previousKernelLabel(entries: []const Entry) !?[]const u8 {
    try validateEntries(entries);
    for (entries) |entry| {
        if (std.mem.eql(u8, entry.provider, "selinux")) return entry.label;
    }
    return null;
}

pub fn validateEntries(entries: []const Entry) !void {
    if (entries.len > max_providers) return error.InvalidCatalogRecord;
    var bytes: usize = 0;
    for (entries, 0..) |entry, i| {
        if (entry.version != 1 or std.mem.allEqual(u8, &entry.policy_digest, 0)) return error.InvalidCatalogRecord;
        validateProvider(entry.provider) catch return error.InvalidCatalogRecord;
        validateLabel(entry.label) catch return error.InvalidCatalogRecord;
        // The trusted bootstrap knows this exact immutable contract. Preserve
        // unknown/dormant providers, but never restore an impossible v1 value.
        if (std.mem.eql(u8, entry.provider, "antfly") and std.mem.eql(u8, &entry.policy_digest, &native_policy_digest) and !nativeLabelAllowed(entry.label)) return error.InvalidCatalogRecord;
        bytes +|= entry.label.len;
        if (bytes > 256 * 1024) return error.InvalidCatalogRecord;
        if (i != 0 and std.mem.order(u8, entries[i - 1].provider, entry.provider) != .lt) return error.InvalidCatalogRecord;
    }
}
pub fn cloneEntries(a: A, entries: []const Entry) ![]Entry {
    const result = try a.alloc(Entry, entries.len);
    var initialized: usize = 0;
    errdefer {
        for (result[0..initialized]) |entry| {
            a.free(entry.provider);
            a.free(entry.label);
        }
        a.free(result);
    }
    // The slice allocation has full capacity; manually free partial fields on
    // error, then release the actual full slice allocation.
    for (entries, result) |entry, *out| {
        const provider = try a.dupe(u8, entry.provider);
        errdefer a.free(provider);
        out.* = .{ .version = entry.version, .provider = provider, .policy_digest = entry.policy_digest, .label = try a.dupe(u8, entry.label) };
        initialized += 1;
    }
    return result;
}
pub fn freeEntries(a: A, entries: []const Entry) void {
    for (entries) |entry| {
        a.free(entry.provider);
        a.free(entry.label);
    }
    a.free(entries);
}

/// Trusted compiled provider policy. `restricted` restricts relabeling only;
/// this provider does not claim to be a SELinux or row-access MAC module.
pub const NativeAuthority = struct { administrator: bool };
var native_provider_context: u8 = 0;
pub const native_policy_contract = "antfly:security-label-policy:antfly:v1:labels(public,internal,restricted);restricted-relabel-admin;null-removes";
pub const native_policy_digest: [32]u8 = .{ 0x5d, 0x29, 0x0d, 0xb1, 0x9c, 0xc8, 0xfb, 0x80, 0x49, 0x4a, 0x16, 0x22, 0xca, 0x8a, 0xcc, 0x6c, 0xe9, 0x07, 0x4b, 0xce, 0x81, 0xeb, 0x0a, 0x12, 0xaf, 0x8e, 0xe7, 0xac, 0xdf, 0xc2, 0xb6, 0xd3 };
fn nativeLabelAllowed(label: []const u8) bool {
    return std.mem.eql(u8, label, "restricted") or std.mem.eql(u8, label, "internal") or std.mem.eql(u8, label, "public");
}
fn checkNativeProvider(_: *anyopaque, input: Relabel) !void {
    const authority_value: *const NativeAuthority = @ptrCast(@alignCast(input.authority));
    if (input.previous) |old| if (std.mem.eql(u8, old, "restricted") and !authority_value.administrator) return error.Forbidden;
    if (input.proposed) |label| {
        if (!nativeLabelAllowed(label)) return error.SqlInvalidParameterValue;
        if (std.mem.eql(u8, label, "restricted") and !authority_value.administrator) return error.Forbidden;
    }
}
const native_providers = [_]Provider{.{ .name = "antfly", .digest = native_policy_digest, .context = &native_provider_context, .check = checkNativeProvider }};
/// Registry initialization is trusted compiled bootstrap. SQL packages and
/// request bodies cannot add hooks, rename this provider, or enable `selinux`.
pub fn nativeRegistry() Registry {
    return .{ .providers = &native_providers };
}

const fixture_object: Object = .{ .incarnation = @splat(1), .database_id = 7, .class = .table, .id = 11 };
test "security label trusted provider selection handles zero one many exact names and duplicate registration" {
    try std.testing.expectError(error.NoSecurityLabelProviders, (Registry{}).select(null));
    try std.testing.expectError(error.SecurityLabelProviderNotLoaded, (Registry{}).select("selinux"));
    const registry = nativeRegistry();
    try std.testing.expectEqualStrings("antfly", (try registry.select(null)).name);
    try std.testing.expectError(error.SecurityLabelProviderNotLoaded, registry.select("ANTFLY"));
    var first = registry.providers[0];
    var second = registry.providers[0];
    first.name = "first";
    second.name = "second";
    const two = [_]Provider{ first, second };
    try std.testing.expectError(error.SecurityLabelProviderRequired, (Registry{ .providers = &two }).select(null));
    try std.testing.expectEqualStrings("second", (try (Registry{ .providers = &two }).select("second")).name);
    const duplicates = [_]Provider{ first, first };
    try std.testing.expectError(error.DuplicateSecurityLabelProvider, (Registry{ .providers = &duplicates }).validate());
}
test "security label native provider vetoes invalid labels and restricted NULL transitions" {
    const registry = nativeRegistry();
    try std.testing.expectError(error.SecurityLabelProviderNotLoaded, registry.select("selinux"));
    const provider = try registry.select(null);
    var authority_value: NativeAuthority = .{ .administrator = false };
    try provider.check(provider.context, .{ .object = fixture_object, .previous = null, .proposed = "internal", .authority = &authority_value });
    try std.testing.expectError(error.SqlInvalidParameterValue, provider.check(provider.context, .{ .object = fixture_object, .previous = null, .proposed = "arbitrary", .authority = &authority_value }));
    try std.testing.expectError(error.SqlInvalidParameterValue, provider.check(provider.context, .{ .object = fixture_object, .previous = null, .proposed = "", .authority = &authority_value }));
    try std.testing.expectError(error.Forbidden, provider.check(provider.context, .{ .object = fixture_object, .previous = "restricted", .proposed = null, .authority = &authority_value }));
    authority_value.administrator = true;
    try provider.check(provider.context, .{ .object = fixture_object, .previous = "restricted", .proposed = null, .authority = &authority_value });
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(native_policy_contract, &digest, .{});
    try std.testing.expectEqualSlices(u8, &digest, &provider.digest);
}
test "security label entries restore dormant providers but reject duplicate noncanonical unsupported and corrupt state" {
    const entry: Entry = .{ .provider = "dormant", .policy_digest = @splat(9), .label = "" };
    try validateEntries(&.{entry}); // no runtime provider required by restore.
    try std.testing.expectError(error.InvalidCatalogRecord, validateEntries(&.{ entry, entry }));
    var invalid = entry;
    invalid.version = 2;
    try std.testing.expectError(error.InvalidCatalogRecord, validateEntries(&.{invalid}));
    invalid = entry;
    invalid.policy_digest = @splat(0);
    try std.testing.expectError(error.InvalidCatalogRecord, validateEntries(&.{invalid}));
    invalid = entry;
    invalid.label = "nul\x00label";
    try std.testing.expectError(error.InvalidCatalogRecord, validateEntries(&.{invalid}));
}
test "security label owned entry cloning has exhaustive allocation failure cleanup" {
    const Failure = struct {
        fn run(a: A) !void {
            const entries = [_]Entry{ .{ .provider = "first", .policy_digest = @splat(1), .label = "internal" }, .{ .provider = "second", .policy_digest = @splat(2), .label = "" } };
            const owned = try cloneEntries(a, &entries);
            defer freeEntries(a, owned);
            try validateEntries(owned);
            try std.testing.expectEqualStrings("internal", owned[0].label);
            try std.testing.expectEqualStrings("", owned[1].label);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Failure.run, .{});
}

test "security label active policy refuses dormant entries written under another contract" {
    const provider = try nativeRegistry().select(null);
    const entry: Entry = .{ .provider = provider.name, .policy_digest = provider.digest, .label = "internal" };
    try std.testing.expectEqualStrings("internal", (try previousLabel(provider, &.{entry})).?);
    var old = entry;
    old.policy_digest = @splat(3);
    try validateEntries(&.{old});
    try std.testing.expectError(error.CatalogGenerationChanged, previousLabel(provider, &.{old}));
    old.provider = "dormant";
    try std.testing.expect(try previousLabel(provider, &.{old}) == null);
}

test "security label restore enforces the known policy without interpreting dormant providers" {
    var entry: Entry = .{ .provider = "antfly", .policy_digest = native_policy_digest, .label = "arbitrary" };
    try std.testing.expectError(error.InvalidCatalogRecord, validateEntries(&.{entry}));
    entry.label = "";
    try std.testing.expectError(error.InvalidCatalogRecord, validateEntries(&.{entry}));
    entry.label = "restricted";
    try validateEntries(&.{entry});
    entry.policy_digest = @splat(7);
    entry.label = "opaque-dormant-value";
    try validateEntries(&.{entry});
    try std.testing.expectError(error.CatalogGenerationChanged, previousLabel(try nativeRegistry().select(null), &.{entry}));
}

test "SELinux approval binds issuer principal labels and catalog cut" {
    const Fixture = struct {
        sequence: u32 = 19,
        reload: bool = false,
        deny: bool = false,
        fn snapshot(ptr: *anyopaque) !KernelRelabel.Policy.Snapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .digest = @splat(8), .sequence = self.sequence };
        }
        fn authorize(ptr: *anyopaque, _: Object, _: []const u8, _: ?[]const u8, _: ?[]const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.deny) return error.Forbidden;
            if (self.reload) self.sequence += 1;
        }
    };
    var fixture: Fixture = .{};
    const policy: KernelRelabel.Policy = .{ .ptr = &fixture, .snapshot = Fixture.snapshot, .authorize = Fixture.authorize };
    const KeyPair = std.crypto.sign.Ed25519.KeyPair;
    const key = try KeyPair.generateDeterministic(@splat(4));
    const other = try KeyPair.generateDeterministic(@splat(5));
    const cut: KernelRelabel = .{
        .object = fixture_object,
        .catalog_revision = 3,
        .command_nonce = @splat(2),
        .actor = @splat(6),
        .subject = KernelRelabel.labelDigest("peer").?,
        .policy = @splat(8),
        .policy_sequence = 19,
        .previous = KernelRelabel.labelDigest("original"),
        .proposed = KernelRelabel.labelDigest("replacement"),
    };
    const approval = try policy.approve(cut, "peer", "original", "replacement", key);
    try std.testing.expectError(error.SelinuxAdmissionChanged, policy.approve(cut, "other-peer", "original", "replacement", key));
    fixture.deny = true;
    try std.testing.expectError(error.Forbidden, policy.approve(cut, "peer", "original", "replacement", key));
    fixture.deny = false;
    fixture.reload = true;
    try std.testing.expectError(error.SelinuxAdmissionChanged, policy.approve(cut, "peer", "original", "replacement", key));
    try approval.verify(cut, key.public_key.toBytes());
    try std.testing.expectError(error.InvalidSelinuxApproval, approval.verify(cut, other.public_key.toBytes()));
    var stale = cut;
    stale.catalog_revision += 1;
    try std.testing.expectError(error.SelinuxAdmissionChanged, approval.verify(stale, key.public_key.toBytes()));
    stale = cut;
    stale.proposed = null;
    try std.testing.expectError(error.SelinuxAdmissionChanged, approval.verify(stale, key.public_key.toBytes()));
    stale = cut;
    stale.subject[0] ^= 1;
    try std.testing.expectError(error.SelinuxAdmissionChanged, approval.verify(stale, key.public_key.toBytes()));
}

test "SELinux catalog authority reconstructs the approval cut independently" {
    const key = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(61));
    const other_key = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(62));
    const cut: KernelRelabel = .{
        .object = fixture_object,
        .catalog_revision = 55,
        .command_nonce = @splat(1),
        .actor = @splat(2),
        .subject = KernelRelabel.labelDigest("peer").?,
        .policy = @splat(3),
        .policy_sequence = 8,
        .previous = KernelRelabel.labelDigest("old"),
        .proposed = null,
    };
    const approval = try KernelRelabel.Approval.sign(cut, key);
    const authority: KernelRelabel.Authority = .{
        .verifier = key.public_key.toBytes(),
        .policy = .{ .digest = cut.policy, .sequence = cut.policy_sequence },
    };
    const catalog: KernelRelabel.Authority.CatalogCut = .{
        .object = fixture_object,
        .revision = 55,
        .nonce = @splat(1),
        .actor = @splat(2),
        .previous = "old",
        .proposed = null,
    };
    try authority.verify(catalog, approval);
    var changed = catalog;
    changed.object.id += 1;
    try std.testing.expectError(error.SelinuxAdmissionChanged, authority.verify(changed, approval));
    changed = catalog;
    changed.revision += 1;
    try std.testing.expectError(error.SelinuxAdmissionChanged, authority.verify(changed, approval));
    changed = catalog;
    changed.nonce[0] ^= 1;
    try std.testing.expectError(error.SelinuxAdmissionChanged, authority.verify(changed, approval));
    changed = catalog;
    changed.actor[0] ^= 1;
    try std.testing.expectError(error.SelinuxAdmissionChanged, authority.verify(changed, approval));
    changed = catalog;
    changed.previous = null;
    try std.testing.expectError(error.SelinuxAdmissionChanged, authority.verify(changed, approval));
    changed = catalog;
    changed.proposed = "new";
    try std.testing.expectError(error.SelinuxAdmissionChanged, authority.verify(changed, approval));
    var stale = authority;
    stale.policy.sequence += 1;
    try std.testing.expectError(error.SelinuxAdmissionChanged, stale.verify(catalog, approval));
    stale = authority;
    stale.epoch += 1;
    try std.testing.expectError(error.SelinuxAdmissionChanged, stale.verify(catalog, approval));
    stale = authority;
    stale.verifier = other_key.public_key.toBytes();
    try std.testing.expectError(error.InvalidSelinuxApproval, stale.verify(catalog, approval));
    var forged = approval;
    forged.cut.subject[0] ^= 1;
    try std.testing.expectError(error.InvalidSelinuxApproval, authority.verify(catalog, forged));
}
