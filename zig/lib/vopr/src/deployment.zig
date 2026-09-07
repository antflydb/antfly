// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Registered in-process deployment topology and quiet-suffix obligations.
//!
//! The composer owns only pointer-free orchestration evidence. Production
//! fixtures remain responsible for starting services, applying faults, and
//! measuring resources, then acknowledge those facts here at safe boundaries.

const std = @import("std");
const ids = @import("id.zig");

pub const StableId = ids.StableId;
pub const DomainKind = enum { process, storage, resource, link };
pub const FaultKind = enum { network, node_pause, storage, resource, clock, service_rate, custom };
pub const InstanceState = enum { stopped, starting, ready, failed };

pub const ResourcePolicy = struct {
    memory_limit_bytes: u64 = 0,
    disk_limit_bytes: u64 = 0,
    max_tasks: u32 = 0,
    max_sockets: u32 = 0,
};

pub const ResourceUsage = struct {
    memory_bytes: u64 = 0,
    disk_bytes: u64 = 0,
    active_tasks: u32 = 0,
    open_sockets: u32 = 0,

    pub fn within(self: ResourceUsage, policy: ResourcePolicy) bool {
        return (policy.memory_limit_bytes == 0 or self.memory_bytes <= policy.memory_limit_bytes) and
            (policy.disk_limit_bytes == 0 or self.disk_bytes <= policy.disk_limit_bytes) and
            (policy.max_tasks == 0 or self.active_tasks <= policy.max_tasks) and
            (policy.max_sockets == 0 or self.open_sockets <= policy.max_sockets);
    }

    pub fn quiet(self: ResourceUsage) bool {
        return self.active_tasks == 0 and self.open_sockets == 0;
    }
};

pub const Role = struct {
    id: StableId,
    name: []const u8,
    depends_on: []const StableId = &.{},
};

pub const Node = struct {
    id: StableId,
    name: []const u8,
    process_domain: StableId,
    storage_domain: StableId,
    resource_domain: StableId,
    resources: ResourcePolicy = .{},
};

pub const Instance = struct {
    id: StableId,
    node_id: StableId,
    role_id: StableId,
    required_for_quiet: bool = true,
};

/// Links are directional. A bidirectional connection is two registered links,
/// allowing asymmetric partitions and delay to retain distinct identities.
pub const Link = struct {
    id: StableId,
    name: []const u8,
    from_node: StableId,
    to_node: StableId,
};

pub const Manifest = struct {
    roles: []const Role,
    nodes: []const Node,
    instances: []const Instance,
    links: []const Link = &.{},

    pub fn validate(self: Manifest) !void {
        if (self.roles.len == 0) return error.DeploymentHasNoRoles;
        if (self.nodes.len == 0) return error.DeploymentHasNoNodes;
        if (self.instances.len == 0) return error.DeploymentHasNoInstances;
        for (self.roles, 0..) |role, index| {
            try validateIdentity(role.id, role.name);
            if (findDuplicateRole(self.roles, index, role.id)) return error.DuplicateDeploymentRole;
            for (role.depends_on, 0..) |dependency, dependency_index| {
                if (dependency == role.id) return error.SelfDependentDeploymentRole;
                if (self.roleIndex(dependency) == null) return error.UnknownDeploymentRoleDependency;
                for (role.depends_on[0..dependency_index]) |prior|
                    if (prior == dependency) return error.DuplicateDeploymentRoleDependency;
            }
            if (self.roleDependsOn(role.id, role.id, 0)) return error.CyclicDeploymentRoleDependency;
        }
        for (self.nodes, 0..) |node, index| {
            try validateIdentity(node.id, node.name);
            if (findDuplicateNode(self.nodes, index, node.id)) return error.DuplicateDeploymentNode;
            if (node.process_domain == 0 or node.storage_domain == 0 or node.resource_domain == 0)
                return error.InvalidDeploymentDomain;
            if (node.process_domain == node.storage_domain or node.process_domain == node.resource_domain or
                node.storage_domain == node.resource_domain)
                return error.DuplicateDeploymentDomain;
            try self.validateUniqueNodeDomains(index, node);
        }
        for (self.instances, 0..) |instance, index| {
            if (instance.id == 0) return error.InvalidDeploymentInstance;
            if (findDuplicateInstance(self.instances, index, instance.id)) return error.DuplicateDeploymentInstance;
            if (self.nodeIndex(instance.node_id) == null) return error.UnknownDeploymentInstanceNode;
            if (self.roleIndex(instance.role_id) == null) return error.UnknownDeploymentInstanceRole;
        }
        for (self.roles) |role| {
            var found = false;
            for (self.instances) |instance| found = found or instance.role_id == role.id;
            if (!found) return error.DeploymentRoleHasNoInstance;
        }
        for (self.links, 0..) |link, index| {
            try validateIdentity(link.id, link.name);
            if (findDuplicateLink(self.links, index, link.id)) return error.DuplicateDeploymentLink;
            if (self.nodeIndex(link.from_node) == null or self.nodeIndex(link.to_node) == null)
                return error.UnknownDeploymentLinkNode;
            if (link.from_node == link.to_node) return error.DeploymentSelfLink;
            if (self.domainKind(link.id) != .link) return error.DuplicateDeploymentDomain;
        }
    }

    fn validateUniqueNodeDomains(self: Manifest, node_index: usize, node: Node) !void {
        const domains = [_]StableId{ node.process_domain, node.storage_domain, node.resource_domain };
        for (self.nodes[0..node_index]) |prior| for (domains) |domain| {
            if (domain == prior.process_domain or domain == prior.storage_domain or domain == prior.resource_domain)
                return error.DuplicateDeploymentDomain;
        };
        for (self.links) |link| for (domains) |domain|
            if (domain == link.id) return error.DuplicateDeploymentDomain;
    }

    pub fn roleIndex(self: Manifest, id: StableId) ?usize {
        for (self.roles, 0..) |role, index| if (role.id == id) return index;
        return null;
    }

    pub fn nodeIndex(self: Manifest, id: StableId) ?usize {
        for (self.nodes, 0..) |node, index| if (node.id == id) return index;
        return null;
    }

    pub fn instanceIndex(self: Manifest, id: StableId) ?usize {
        for (self.instances, 0..) |instance, index| if (instance.id == id) return index;
        return null;
    }

    pub fn domainKind(self: Manifest, id: StableId) ?DomainKind {
        var found: ?DomainKind = null;
        for (self.nodes) |node| {
            if (node.process_domain == id) found = mergeDomain(found, .process);
            if (node.storage_domain == id) found = mergeDomain(found, .storage);
            if (node.resource_domain == id) found = mergeDomain(found, .resource);
        }
        for (self.links) |link| {
            if (link.id == id) found = mergeDomain(found, .link);
        }
        return found;
    }

    fn roleDependsOn(self: Manifest, role_id: StableId, target: StableId, depth: usize) bool {
        if (depth >= self.roles.len) return true;
        const role = self.roles[self.roleIndex(role_id) orelse return false];
        for (role.depends_on) |dependency| {
            if (dependency == target) return true;
            if (self.roleDependsOn(dependency, target, depth + 1)) return true;
        }
        return false;
    }
};

const ActiveFault = struct { id: StableId, kind: FaultKind, domain_id: StableId };

pub const Composer = struct {
    allocator: std.mem.Allocator,
    manifest: Manifest,
    nodes_started: []bool,
    instances: []InstanceState,
    resources: []?ResourceUsage,
    quiet_ack_epoch: []u64,
    active_faults: std.ArrayListUnmanaged(ActiveFault) = .empty,
    quiet_requested: bool = false,
    quiet_epoch: u64 = 0,

    pub fn init(allocator: std.mem.Allocator, manifest: Manifest) !Composer {
        try manifest.validate();
        const nodes_started = try allocator.alloc(bool, manifest.nodes.len);
        errdefer allocator.free(nodes_started);
        const instances = try allocator.alloc(InstanceState, manifest.instances.len);
        errdefer allocator.free(instances);
        const resources = try allocator.alloc(?ResourceUsage, manifest.nodes.len);
        errdefer allocator.free(resources);
        const quiet_ack_epoch = try allocator.alloc(u64, manifest.nodes.len);
        @memset(nodes_started, false);
        @memset(instances, .stopped);
        @memset(resources, null);
        @memset(quiet_ack_epoch, 0);
        return .{
            .allocator = allocator,
            .manifest = manifest,
            .nodes_started = nodes_started,
            .instances = instances,
            .resources = resources,
            .quiet_ack_epoch = quiet_ack_epoch,
        };
    }

    pub fn deinit(self: *Composer) void {
        self.active_faults.deinit(self.allocator);
        self.allocator.free(self.nodes_started);
        self.allocator.free(self.instances);
        self.allocator.free(self.resources);
        self.allocator.free(self.quiet_ack_epoch);
        self.* = undefined;
    }

    pub fn startNode(self: *Composer, node_id: StableId) !void {
        if (self.quiet_requested) return error.DeploymentQuietSuffixStarted;
        const index = self.manifest.nodeIndex(node_id) orelse return error.UnknownDeploymentNode;
        if (self.nodes_started[index]) return error.DeploymentNodeAlreadyStarted;
        self.nodes_started[index] = true;
        self.invalidateQuiet(index);
        for (self.manifest.instances, 0..) |instance, instance_index| {
            if (instance.node_id == node_id) self.instances[instance_index] = .starting;
        }
    }

    pub fn restartNode(self: *Composer, node_id: StableId) !void {
        if (self.quiet_requested) return error.DeploymentQuietSuffixStarted;
        const index = self.manifest.nodeIndex(node_id) orelse return error.UnknownDeploymentNode;
        self.nodes_started[index] = true;
        self.resources[index] = null;
        self.invalidateQuiet(index);
        for (self.manifest.instances, 0..) |instance, instance_index| {
            if (instance.node_id == node_id) self.instances[instance_index] = .starting;
        }
    }

    pub fn publishReady(self: *Composer, instance_id: StableId) !void {
        const index = self.manifest.instanceIndex(instance_id) orelse return error.UnknownDeploymentInstance;
        const instance = self.manifest.instances[index];
        const node_index = self.manifest.nodeIndex(instance.node_id).?;
        if (!self.nodes_started[node_index]) return error.DeploymentNodeNotStarted;
        const role = self.manifest.roles[self.manifest.roleIndex(instance.role_id).?];
        for (role.depends_on) |dependency| if (!self.roleReady(dependency))
            return error.DeploymentRoleDependencyNotReady;
        self.instances[index] = .ready;
        self.invalidateQuiet(node_index);
    }

    pub fn failInstance(self: *Composer, instance_id: StableId) !void {
        const index = self.manifest.instanceIndex(instance_id) orelse return error.UnknownDeploymentInstance;
        self.instances[index] = .failed;
        self.invalidateQuiet(self.manifest.nodeIndex(self.manifest.instances[index].node_id).?);
    }

    pub fn observeResources(self: *Composer, node_id: StableId, usage: ResourceUsage) !void {
        const index = self.manifest.nodeIndex(node_id) orelse return error.UnknownDeploymentNode;
        self.resources[index] = usage;
        self.invalidateQuiet(index);
    }

    pub fn activateFault(self: *Composer, fault_id: StableId, kind: FaultKind, domain_id: StableId) !void {
        if (self.quiet_requested) return error.DeploymentQuietSuffixStarted;
        if (fault_id == 0) return error.InvalidDeploymentFault;
        for (self.active_faults.items) |fault| if (fault.id == fault_id) return error.DuplicateDeploymentFault;
        const domain_kind = self.manifest.domainKind(domain_id) orelse return error.UnknownDeploymentFaultDomain;
        if (!faultCompatible(kind, domain_kind)) return error.IncompatibleDeploymentFaultDomain;
        try self.active_faults.append(self.allocator, .{ .id = fault_id, .kind = kind, .domain_id = domain_id });
        self.invalidateAffectedNodes(domain_id);
    }

    pub fn healFault(self: *Composer, fault_id: StableId) !void {
        for (self.active_faults.items, 0..) |fault, index| if (fault.id == fault_id) {
            const domain_id = fault.domain_id;
            _ = self.active_faults.swapRemove(index);
            self.invalidateAffectedNodes(domain_id);
            return;
        };
        return error.UnknownDeploymentFault;
    }

    pub fn healAll(self: *Composer) void {
        self.active_faults.clearRetainingCapacity();
        @memset(self.quiet_ack_epoch, 0);
    }

    pub fn requestQuietSuffix(self: *Composer) !u64 {
        if (self.quiet_requested) return error.DeploymentQuietSuffixAlreadyRequested;
        self.quiet_requested = true;
        self.quiet_epoch +|= 1;
        if (self.quiet_epoch == 0) self.quiet_epoch = 1;
        @memset(self.quiet_ack_epoch, 0);
        return self.quiet_epoch;
    }

    pub fn acknowledgeNodeQuiet(self: *Composer, node_id: StableId) !void {
        if (!self.quiet_requested) return error.DeploymentQuietSuffixNotRequested;
        const index = self.manifest.nodeIndex(node_id) orelse return error.UnknownDeploymentNode;
        if (!self.nodeReady(node_id)) return error.DeploymentNodeNotReady;
        if (self.nodeFaulted(node_id)) return error.DeploymentNodeFaulted;
        const usage = self.resources[index] orelse return error.DeploymentResourceEvidenceMissing;
        if (!usage.within(self.manifest.nodes[index].resources)) return error.DeploymentResourcePolicyExceeded;
        if (!usage.quiet()) return error.DeploymentNodeResourcesNotQuiet;
        self.quiet_ack_epoch[index] = self.quiet_epoch;
    }

    pub fn quietComplete(self: *const Composer) bool {
        if (!self.quiet_requested or self.active_faults.items.len != 0) return false;
        for (self.manifest.nodes, 0..) |node, index| {
            if (!self.nodeRequiredForQuiet(node.id)) continue;
            if (self.quiet_ack_epoch[index] != self.quiet_epoch) return false;
        }
        return true;
    }

    pub fn activeFaultCount(self: *const Composer) usize {
        return self.active_faults.items.len;
    }

    fn roleReady(self: *const Composer, role_id: StableId) bool {
        for (self.manifest.instances, self.instances) |instance, state|
            if (instance.role_id == role_id and state == .ready) return true;
        return false;
    }

    fn nodeReady(self: *const Composer, node_id: StableId) bool {
        var required = false;
        for (self.manifest.instances, self.instances) |instance, state| if (instance.node_id == node_id and instance.required_for_quiet) {
            required = true;
            if (state != .ready) return false;
        };
        return required;
    }

    fn nodeRequiredForQuiet(self: *const Composer, node_id: StableId) bool {
        for (self.manifest.instances) |instance|
            if (instance.node_id == node_id and instance.required_for_quiet) return true;
        return false;
    }

    fn nodeFaulted(self: *const Composer, node_id: StableId) bool {
        for (self.active_faults.items) |fault| if (self.domainAffectsNode(fault.domain_id, node_id)) return true;
        return false;
    }

    fn domainAffectsNode(self: *const Composer, domain_id: StableId, node_id: StableId) bool {
        const node = self.manifest.nodes[self.manifest.nodeIndex(node_id) orelse return false];
        if (domain_id == node.process_domain or domain_id == node.storage_domain or domain_id == node.resource_domain) return true;
        for (self.manifest.links) |link|
            if (link.id == domain_id) return link.from_node == node_id or link.to_node == node_id;
        return false;
    }

    fn invalidateAffectedNodes(self: *Composer, domain_id: StableId) void {
        for (self.manifest.nodes, 0..) |node, index|
            if (self.domainAffectsNode(domain_id, node.id)) self.invalidateQuiet(index);
    }

    fn invalidateQuiet(self: *Composer, node_index: usize) void {
        self.quiet_ack_epoch[node_index] = 0;
    }
};

fn validateIdentity(id: StableId, name: []const u8) !void {
    if (id == 0) return error.InvalidDeploymentIdentity;
    if (name.len == 0) return error.EmptyDeploymentName;
}

fn mergeDomain(existing: ?DomainKind, incoming: DomainKind) ?DomainKind {
    if (existing != null) return null;
    return incoming;
}

fn faultCompatible(fault: FaultKind, domain: DomainKind) bool {
    return switch (fault) {
        .network => domain == .link,
        .node_pause, .clock => domain == .process,
        .service_rate => domain == .process or domain == .resource,
        .custom => true,
        .storage => domain == .storage,
        .resource => domain == .resource,
    };
}

fn findDuplicateRole(values: []const Role, before: usize, id: StableId) bool {
    for (values[0..before]) |value| if (value.id == id) return true;
    return false;
}
fn findDuplicateNode(values: []const Node, before: usize, id: StableId) bool {
    for (values[0..before]) |value| if (value.id == id) return true;
    return false;
}
fn findDuplicateInstance(values: []const Instance, before: usize, id: StableId) bool {
    for (values[0..before]) |value| if (value.id == id) return true;
    return false;
}
fn findDuplicateLink(values: []const Link, before: usize, id: StableId) bool {
    for (values[0..before]) |value| if (value.id == id) return true;
    return false;
}

const test_roles = [_]Role{
    .{ .id = 1, .name = "metadata" },
    .{ .id = 2, .name = "data", .depends_on = &.{1} },
    .{ .id = 3, .name = "public", .depends_on = &.{2} },
};
const test_nodes = [_]Node{
    .{ .id = 10, .name = "node-a", .process_domain = 101, .storage_domain = 102, .resource_domain = 103, .resources = .{ .memory_limit_bytes = 100, .disk_limit_bytes = 200, .max_tasks = 4, .max_sockets = 4 } },
    .{ .id = 20, .name = "node-b", .process_domain = 201, .storage_domain = 202, .resource_domain = 203, .resources = .{ .memory_limit_bytes = 100, .disk_limit_bytes = 200, .max_tasks = 4, .max_sockets = 4 } },
};
const test_instances = [_]Instance{
    .{ .id = 1001, .node_id = 10, .role_id = 1 },
    .{ .id = 1002, .node_id = 10, .role_id = 2 },
    .{ .id = 1003, .node_id = 10, .role_id = 3 },
    .{ .id = 2001, .node_id = 20, .role_id = 1 },
    .{ .id = 2002, .node_id = 20, .role_id = 2 },
    .{ .id = 2003, .node_id = 20, .role_id = 3 },
};
const test_links = [_]Link{
    .{ .id = 301, .name = "a-to-b", .from_node = 10, .to_node = 20 },
    .{ .id = 302, .name = "b-to-a", .from_node = 20, .to_node = 10 },
};
const test_manifest: Manifest = .{ .roles = &test_roles, .nodes = &test_nodes, .instances = &test_instances, .links = &test_links };

test "deployment composer enforces readiness fault scopes resources and quiet suffix" {
    var composer = try Composer.init(std.testing.allocator, test_manifest);
    defer composer.deinit();
    try composer.startNode(10);
    try composer.startNode(20);
    try std.testing.expectError(error.DeploymentRoleDependencyNotReady, composer.publishReady(1002));
    try composer.publishReady(1001);
    try composer.publishReady(2001);
    try composer.publishReady(1002);
    try composer.publishReady(2002);
    try composer.publishReady(1003);
    try composer.publishReady(2003);

    try composer.activateFault(8999, .service_rate, 101);
    try composer.activateFault(9000, .service_rate, 103);
    try composer.healFault(8999);
    try composer.healFault(9000);
    try composer.activateFault(9001, .network, 301);
    const epoch = try composer.requestQuietSuffix();
    try std.testing.expectEqual(@as(u64, 1), epoch);
    try std.testing.expectError(error.DeploymentNodeFaulted, composer.acknowledgeNodeQuiet(10));
    try composer.healFault(9001);
    try composer.observeResources(10, .{ .memory_bytes = 50, .disk_bytes = 100 });
    try composer.observeResources(20, .{ .memory_bytes = 50, .disk_bytes = 100, .active_tasks = 1 });
    try composer.acknowledgeNodeQuiet(10);
    try std.testing.expectError(error.DeploymentNodeResourcesNotQuiet, composer.acknowledgeNodeQuiet(20));
    try composer.observeResources(20, .{ .memory_bytes = 50, .disk_bytes = 100 });
    try composer.acknowledgeNodeQuiet(20);
    try std.testing.expect(composer.quietComplete());
    try std.testing.expectError(error.DeploymentQuietSuffixStarted, composer.activateFault(9002, .storage, 102));
}

test "deployment manifest rejects cycles duplicate domains and incompatible fault scope" {
    const cyclic_roles = [_]Role{
        .{ .id = 1, .name = "a", .depends_on = &.{2} },
        .{ .id = 2, .name = "b", .depends_on = &.{1} },
    };
    const cyclic_instances = [_]Instance{
        .{ .id = 1, .node_id = 10, .role_id = 1 },
        .{ .id = 2, .node_id = 10, .role_id = 2 },
    };
    try std.testing.expectError(error.CyclicDeploymentRoleDependency, (Manifest{
        .roles = &cyclic_roles,
        .nodes = test_nodes[0..1],
        .instances = &cyclic_instances,
    }).validate());

    var composer = try Composer.init(std.testing.allocator, test_manifest);
    defer composer.deinit();
    try std.testing.expectError(error.IncompatibleDeploymentFaultDomain, composer.activateFault(1, .storage, 301));
    try std.testing.expectError(error.UnknownDeploymentFaultDomain, composer.activateFault(1, .network, 999));
}
