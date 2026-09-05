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

//! Stable, non-owning identities for provider authentication sources.
//!
//! Literal credentials are represented only by a one-way digest; named secret,
//! ADC, and AWS sources retain their non-secret locators. These identities are
//! suitable for execution-entry comparison, cache partitioning, and rate-limit
//! scopes. Durable model/vector provenance must remain credential-free.

const std = @import("std");

pub const CredentialSourceIdentity = struct {
    pub const Kind = enum(u8) {
        none,
        literal_secret,
        secret_ref,
        env_var,
        google_adc_default,
        google_adc_file,
        aws_default_chain,
        aws_profile,
        aws_web_identity,
    };

    kind: Kind,
    present_components: u8 = 0,
    components: [4][]const u8 = .{ "", "", "", "" },
    literal_digest: ?[std.crypto.hash.sha2.Sha256.digest_length]u8 = null,

    pub fn none() CredentialSourceIdentity {
        return .{ .kind = .none };
    }

    pub fn literalSecret(value: []const u8) CredentialSourceIdentity {
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(value, &digest, .{});
        return .{ .kind = .literal_secret, .literal_digest = digest };
    }

    pub fn secretReference(reference: []const u8) CredentialSourceIdentity {
        return one(.secret_ref, reference);
    }

    pub fn environmentVariable(name: []const u8) CredentialSourceIdentity {
        return one(.env_var, name);
    }

    pub fn googleAdc(credentials_path: ?[]const u8) CredentialSourceIdentity {
        const path = credentials_path orelse return .{ .kind = .google_adc_default };
        return one(.google_adc_file, path);
    }

    pub fn awsDefaultChain() CredentialSourceIdentity {
        return .{ .kind = .aws_default_chain };
    }

    pub fn awsProfile(profile: []const u8, shared_credentials_file: ?[]const u8) CredentialSourceIdentity {
        var identity = one(.aws_profile, profile);
        if (shared_credentials_file) |path| identity.setComponent(1, path);
        return identity;
    }

    pub fn awsWebIdentity(
        role_arn: []const u8,
        token_file: []const u8,
        session_name: []const u8,
        sts_endpoint: ?[]const u8,
    ) CredentialSourceIdentity {
        var identity = one(.aws_web_identity, role_arn);
        identity.setComponent(1, token_file);
        identity.setComponent(2, session_name);
        if (sts_endpoint) |endpoint| identity.setComponent(3, endpoint);
        return identity;
    }

    pub fn eql(lhs: CredentialSourceIdentity, rhs: CredentialSourceIdentity) bool {
        if (lhs.kind != rhs.kind or lhs.present_components != rhs.present_components) return false;
        if ((lhs.literal_digest == null) != (rhs.literal_digest == null)) return false;
        if (lhs.literal_digest) |lhs_digest| {
            const rhs_digest = rhs.literal_digest.?;
            if (!std.mem.eql(u8, &lhs_digest, &rhs_digest)) return false;
        }
        for (lhs.components, rhs.components, 0..) |lhs_component, rhs_component, index| {
            const mask = @as(u8, 1) << @intCast(index);
            if (lhs.present_components & mask == 0) continue;
            if (!std.mem.eql(u8, lhs_component, rhs_component)) return false;
        }
        return true;
    }

    /// Add the typed source identity to a streaming hash. Component lengths
    /// prevent ambiguous concatenations, and the presence mask distinguishes
    /// an omitted optional locator from an explicitly empty one.
    pub fn updateHash(self: CredentialSourceIdentity, hasher: anytype) void {
        const kind = [_]u8{@intFromEnum(self.kind)};
        hasher.update(&kind);
        hasher.update(&.{self.present_components});
        hasher.update(&.{@intFromBool(self.literal_digest != null)});
        if (self.literal_digest) |digest| hasher.update(&digest);
        for (self.components, 0..) |component, index| {
            const mask = @as(u8, 1) << @intCast(index);
            if (self.present_components & mask == 0) continue;
            updateField(hasher, component);
        }
    }

    fn one(kind: Kind, component: []const u8) CredentialSourceIdentity {
        var identity = CredentialSourceIdentity{ .kind = kind };
        identity.setComponent(0, component);
        return identity;
    }

    fn setComponent(self: *CredentialSourceIdentity, index: usize, component: []const u8) void {
        std.debug.assert(index < self.components.len);
        self.components[index] = component;
        self.present_components |= @as(u8, 1) << @intCast(index);
    }
};

pub fn updateField(hasher: anytype, value: []const u8) void {
    var encoded_len = std.mem.nativeToLittle(u64, @intCast(value.len));
    hasher.update(std.mem.asBytes(&encoded_len));
    hasher.update(value);
}

test "credential source identities preserve type locator and presence boundaries" {
    const Identity = CredentialSourceIdentity;
    try std.testing.expect(!Identity.googleAdc(null).eql(Identity.googleAdc("<default-adc>")));
    try std.testing.expect(!Identity.awsDefaultChain().eql(Identity.none()));
    try std.testing.expect(!Identity.awsProfile("default", null).eql(Identity.awsProfile("default", "")));
    try std.testing.expect(!Identity.literalSecret("same").eql(Identity.secretReference("same")));
}
