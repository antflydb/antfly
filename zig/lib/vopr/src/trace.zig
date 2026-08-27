// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const event = @import("event.zig");
const health = @import("health.zig");
const ids = @import("id.zig");
const observation = @import("observation.zig");
const property = @import("property.zig");
const transition = @import("transition.zig");

pub const format = "vopr-trace-v1";
pub const simulator_abi = 1;

pub const Header = struct {
    system: []const u8 = "generic",
    scenario: []const u8,
    scenario_version: u32,
    source_revision: []const u8 = "unknown",
    target: []const u8 = "native",
    optimize: []const u8 = "unknown",
};

pub const Parameter = struct {
    id: ids.StableId,
    name: []const u8,
    value: i64,

    pub fn named(name: []const u8, value: i64) Parameter {
        return .{ .id = ids.stable("parameter", name), .name = name, .value = value };
    }
};

pub const Config = struct {
    seed: ?u64 = null,
    transition_budget: u64,
    resource_budget: u64 = std.math.maxInt(u64),
    fixture_hashes: []const u64 = &.{},
    feature_flags: []const ids.StableId = &.{},
    backend_ids: []const ids.StableId = &.{},
    scenario_parameters: []const Parameter = &.{},
};

pub const ChoiceRecord = struct {
    site_id: ids.StableId,
    site_name: []const u8,
    occurrence: u64,
    enabled_ids: []const ids.StableId,
    selected_id: ids.StableId,
};

pub const TransitionRecord = struct {
    index: u64,
    id: ids.StableId,
    name: []const u8,
    kind: transition.Kind,
    actor_id: ?ids.StableId = null,
    resource_id: ?ids.StableId = null,
    parameter: i64 = 0,
    payload_digest: u64 = 0,
};

pub const FaultPhase = transition.FaultPhase;
pub const FaultRecord = struct {
    index: u64,
    id: ids.StableId,
    name: []const u8,
    phase: FaultPhase,
};

pub const EventRecord = struct {
    index: u64,
    ordinal: u64,
    id: ids.StableId,
    name: []const u8,
    kind: event.Kind,
    actor_id: ?ids.StableId,
    resource_id: ?ids.StableId,
    payload_digest: u64,
};

pub const ObservationRecord = struct {
    index: u64,
    digest: u64,
    features: []const observation.Feature,
};

pub const PropertyRecord = struct {
    index: u64,
    property_id: ids.StableId,
    name: []const u8,
    kind: property.Kind,
    condition: bool,
    details: []const u8,
};

pub const FailureClass = enum { property, liveness, harness, panic, process, allocator, differential, replay_divergence };
pub const FailureRecord = struct {
    index: u64,
    class: FailureClass,
    property_id: ?ids.StableId,
    identity: []const u8,
    fingerprint: u64,
    observation_digest: ?u64,
};

pub const Summary = struct {
    transitions: u64,
    final_observation_digest: u64,
    property_failures: u64,
};

pub const Trace = struct {
    allocator: std.mem.Allocator,
    header: Header,
    config: Config,
    choices: std.ArrayListUnmanaged(ChoiceRecord) = .empty,
    transitions: std.ArrayListUnmanaged(TransitionRecord) = .empty,
    faults: std.ArrayListUnmanaged(FaultRecord) = .empty,
    events: std.ArrayListUnmanaged(EventRecord) = .empty,
    observations: std.ArrayListUnmanaged(ObservationRecord) = .empty,
    properties: std.ArrayListUnmanaged(PropertyRecord) = .empty,
    failures: std.ArrayListUnmanaged(FailureRecord) = .empty,
    summary: ?Summary = null,
    /// Diagnostic-only evidence sampled by the runner. It is intentionally
    /// omitted from render/parse and therefore cannot affect replay truth.
    health_evidence: ?health.Evidence = null,

    pub fn init(allocator: std.mem.Allocator, header: Header, config: Config) !Trace {
        if (header.system.len == 0 or header.scenario.len == 0 or header.scenario_version == 0) return error.InvalidTraceHeader;
        if (config.transition_budget == 0) return error.InvalidTransitionBudget;
        const system = try allocator.dupe(u8, header.system);
        errdefer allocator.free(system);
        const scenario = try allocator.dupe(u8, header.scenario);
        errdefer allocator.free(scenario);
        const source_revision = try allocator.dupe(u8, header.source_revision);
        errdefer allocator.free(source_revision);
        const target = try allocator.dupe(u8, header.target);
        errdefer allocator.free(target);
        const optimize = try allocator.dupe(u8, header.optimize);
        errdefer allocator.free(optimize);
        const fixture_hashes = try allocator.dupe(u64, config.fixture_hashes);
        errdefer allocator.free(fixture_hashes);
        const feature_flags = try allocator.dupe(ids.StableId, config.feature_flags);
        errdefer allocator.free(feature_flags);
        const backend_ids = try allocator.dupe(ids.StableId, config.backend_ids);
        errdefer allocator.free(backend_ids);
        const scenario_parameters = try allocator.alloc(Parameter, config.scenario_parameters.len);
        errdefer allocator.free(scenario_parameters);
        var initialized_parameters: usize = 0;
        errdefer for (scenario_parameters[0..initialized_parameters]) |parameter| allocator.free(parameter.name);
        for (config.scenario_parameters, 0..) |parameter, index| {
            scenario_parameters[index] = .{
                .id = parameter.id,
                .name = try allocator.dupe(u8, parameter.name),
                .value = parameter.value,
            };
            initialized_parameters += 1;
        }
        return .{
            .allocator = allocator,
            .header = .{
                .system = system,
                .scenario = scenario,
                .scenario_version = header.scenario_version,
                .source_revision = source_revision,
                .target = target,
                .optimize = optimize,
            },
            .config = .{
                .seed = config.seed,
                .transition_budget = config.transition_budget,
                .resource_budget = config.resource_budget,
                .fixture_hashes = fixture_hashes,
                .feature_flags = feature_flags,
                .backend_ids = backend_ids,
                .scenario_parameters = scenario_parameters,
            },
        };
    }

    pub fn deinit(self: *Trace) void {
        self.allocator.free(self.header.system);
        self.allocator.free(self.header.scenario);
        self.allocator.free(self.header.source_revision);
        self.allocator.free(self.header.target);
        self.allocator.free(self.header.optimize);
        self.allocator.free(self.config.fixture_hashes);
        self.allocator.free(self.config.feature_flags);
        self.allocator.free(self.config.backend_ids);
        for (self.config.scenario_parameters) |parameter| self.allocator.free(parameter.name);
        self.allocator.free(self.config.scenario_parameters);
        for (self.choices.items) |record| {
            self.allocator.free(record.site_name);
            self.allocator.free(record.enabled_ids);
        }
        for (self.transitions.items) |record| self.allocator.free(record.name);
        for (self.faults.items) |record| self.allocator.free(record.name);
        for (self.events.items) |record| self.allocator.free(record.name);
        for (self.observations.items) |record| {
            for (record.features) |feature| self.allocator.free(feature.name);
            self.allocator.free(record.features);
        }
        for (self.properties.items) |record| {
            self.allocator.free(record.name);
            self.allocator.free(record.details);
        }
        for (self.failures.items) |record| self.allocator.free(record.identity);
        self.choices.deinit(self.allocator);
        self.transitions.deinit(self.allocator);
        self.faults.deinit(self.allocator);
        self.events.deinit(self.allocator);
        self.observations.deinit(self.allocator);
        self.properties.deinit(self.allocator);
        self.failures.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn addChoice(self: *Trace, record: ChoiceRecord) !void {
        const site_name = try self.allocator.dupe(u8, record.site_name);
        errdefer self.allocator.free(site_name);
        const enabled_ids = try self.allocator.dupe(ids.StableId, record.enabled_ids);
        errdefer self.allocator.free(enabled_ids);
        try self.choices.append(self.allocator, .{
            .site_id = record.site_id,
            .site_name = site_name,
            .occurrence = record.occurrence,
            .enabled_ids = enabled_ids,
            .selected_id = record.selected_id,
        });
    }

    pub fn addTransition(self: *Trace, record: TransitionRecord) !void {
        const name = try self.allocator.dupe(u8, record.name);
        errdefer self.allocator.free(name);
        try self.transitions.append(self.allocator, .{
            .index = record.index,
            .id = record.id,
            .name = name,
            .kind = record.kind,
            .actor_id = record.actor_id,
            .resource_id = record.resource_id,
            .parameter = record.parameter,
            .payload_digest = record.payload_digest,
        });
    }

    pub fn addFault(self: *Trace, record: FaultRecord) !void {
        const name = try self.allocator.dupe(u8, record.name);
        errdefer self.allocator.free(name);
        try self.faults.append(self.allocator, .{ .index = record.index, .id = record.id, .name = name, .phase = record.phase });
    }

    pub fn addEvent(self: *Trace, record: EventRecord) !void {
        const name = try self.allocator.dupe(u8, record.name);
        errdefer self.allocator.free(name);
        try self.events.append(self.allocator, .{
            .index = record.index,
            .ordinal = record.ordinal,
            .id = record.id,
            .name = name,
            .kind = record.kind,
            .actor_id = record.actor_id,
            .resource_id = record.resource_id,
            .payload_digest = record.payload_digest,
        });
    }

    pub fn addObservation(self: *Trace, record: ObservationRecord) !void {
        const features = try self.allocator.alloc(observation.Feature, record.features.len);
        errdefer self.allocator.free(features);
        var initialized: usize = 0;
        errdefer for (features[0..initialized]) |feature| self.allocator.free(feature.name);
        for (record.features, 0..) |feature, index| {
            features[index] = .{ .id = feature.id, .name = try self.allocator.dupe(u8, feature.name), .value = feature.value };
            initialized += 1;
        }
        try self.observations.append(self.allocator, .{ .index = record.index, .digest = record.digest, .features = features });
    }

    pub fn addProperty(self: *Trace, record: PropertyRecord) !void {
        const name = try self.allocator.dupe(u8, record.name);
        errdefer self.allocator.free(name);
        const details = try self.allocator.dupe(u8, record.details);
        errdefer self.allocator.free(details);
        try self.properties.append(self.allocator, .{
            .index = record.index,
            .property_id = record.property_id,
            .name = name,
            .kind = record.kind,
            .condition = record.condition,
            .details = details,
        });
    }

    pub fn addFailure(self: *Trace, record: FailureRecord) !void {
        const identity = try self.allocator.dupe(u8, record.identity);
        errdefer self.allocator.free(identity);
        try self.failures.append(self.allocator, .{
            .index = record.index,
            .class = record.class,
            .property_id = record.property_id,
            .identity = identity,
            .fingerprint = record.fingerprint,
            .observation_digest = record.observation_digest,
        });
    }

    pub fn validate(self: *const Trace) !void {
        if (self.summary == null) return error.MissingTraceSummary;
        if (self.summary.?.transitions != self.transitions.items.len) return error.InvalidTraceSummary;
        if (self.choices.items.len != self.transitions.items.len) return error.InvalidChoiceCount;
        try ids.validateCanonical(self.config.fixture_hashes);
        try ids.validateCanonical(self.config.feature_flags);
        try ids.validateCanonical(self.config.backend_ids);
        var prior_parameter_id: ids.StableId = 0;
        for (self.config.scenario_parameters, 0..) |parameter, index| {
            if (parameter.id == 0 or parameter.name.len == 0) return error.InvalidScenarioParameter;
            if (index > 0 and parameter.id <= prior_parameter_id) return error.NonCanonicalScenarioParameters;
            prior_parameter_id = parameter.id;
        }
        var expected_index: u64 = 1;
        for (self.transitions.items) |record| {
            if (record.index != expected_index) return error.NonCanonicalTransitionIndex;
            if (record.id == 0 or record.name.len == 0) return error.InvalidTransitionRecord;
            expected_index += 1;
        }
        for (self.choices.items, 0..) |record, index| {
            if (record.site_id == 0 or record.site_name.len == 0) return error.InvalidChoiceSite;
            if (record.occurrence != index) return error.NonCanonicalChoiceOccurrence;
            try ids.validateCanonical(record.enabled_ids);
            if (std.mem.indexOfScalar(ids.StableId, record.enabled_ids, record.selected_id) == null) return error.SelectedChoiceNotEnabled;
        }
        var prior_fault_index: u64 = 0;
        for (self.faults.items) |record| {
            if (record.index == 0 or record.index > self.transitions.items.len) return error.InvalidFaultIndex;
            if (record.index < prior_fault_index) return error.NonCanonicalFaultOrder;
            if (record.id == 0 or record.name.len == 0) return error.InvalidFaultRecord;
            prior_fault_index = record.index;
        }
        var prior_event_index: u64 = 0;
        var expected_ordinal: u64 = 0;
        for (self.events.items) |record| {
            if (record.index == 0 or record.index > self.transitions.items.len) return error.InvalidEventIndex;
            if (record.index != prior_event_index) {
                if (record.index < prior_event_index) return error.NonCanonicalEventOrder;
                prior_event_index = record.index;
                expected_ordinal = 0;
            }
            if (record.ordinal != expected_ordinal) return error.NonCanonicalEventOrder;
            if (record.id == 0 or record.name.len == 0) return error.InvalidEventRecord;
            expected_ordinal += 1;
        }
        if (self.observations.items.len != self.transitions.items.len + 1) return error.InvalidObservationCount;
        for (self.observations.items, 0..) |record, index| {
            if (record.index != index) return error.NonCanonicalObservationIndex;
            var prior: ids.StableId = 0;
            for (record.features, 0..) |feature, feature_index| {
                if (feature.id == 0 or feature.name.len == 0) return error.InvalidObservationFeature;
                if (feature_index > 0 and prior >= feature.id) return error.NonCanonicalObservationFeatures;
                prior = feature.id;
            }
            if (record.digest != observation.digestFeatures(record.features)) return error.InvalidObservationDigest;
        }
        if (self.summary.?.final_observation_digest != self.observations.items[self.observations.items.len - 1].digest) return error.InvalidTraceSummary;
        var prior_property_index: u64 = 0;
        var prior_property_id: u64 = 0;
        for (self.properties.items) |record| {
            if (record.index == 0 or record.index > self.transitions.items.len) return error.InvalidPropertyIndex;
            if (record.property_id == 0 or record.name.len == 0) return error.InvalidPropertyRecord;
            if (record.index < prior_property_index or (record.index == prior_property_index and record.property_id <= prior_property_id)) return error.NonCanonicalPropertyOrder;
            prior_property_index = record.index;
            prior_property_id = record.property_id;
        }
        var property_failure_count: u64 = 0;
        var prior_failure_index: u64 = 0;
        var prior_failure_fingerprint: u64 = 0;
        for (self.failures.items) |record| {
            if (record.index > self.transitions.items.len) return error.InvalidFailureIndex;
            if (record.identity.len == 0 or record.fingerprint == 0) return error.InvalidFailureRecord;
            if (record.class == .property and record.property_id == null) return error.InvalidFailureRecord;
            if (record.index < prior_failure_index or (record.index == prior_failure_index and record.fingerprint <= prior_failure_fingerprint)) return error.NonCanonicalFailureOrder;
            prior_failure_index = record.index;
            prior_failure_fingerprint = record.fingerprint;
            property_failure_count += @intFromBool(record.class == .property);
        }
        if (self.summary.?.property_failures != property_failure_count) return error.InvalidTraceSummary;
    }

    pub fn renderAlloc(self: *const Trace, allocator: std.mem.Allocator) ![]u8 {
        try self.validate();
        var out: std.ArrayListUnmanaged(u8) = .empty;
        errdefer out.deinit(allocator);
        try appendJsonLine(allocator, &out, HeaderWire{
            .system = self.header.system,
            .scenario = self.header.scenario,
            .scenario_version = self.header.scenario_version,
            .source_revision = self.header.source_revision,
            .target = self.header.target,
            .optimize = self.header.optimize,
        });
        try appendJsonLine(allocator, &out, ConfigWire{
            .seed = self.config.seed,
            .transition_budget = self.config.transition_budget,
            .resource_budget = self.config.resource_budget,
            .fixture_hashes = self.config.fixture_hashes,
            .feature_flags = self.config.feature_flags,
            .backend_ids = self.config.backend_ids,
            .scenario_parameters = self.config.scenario_parameters,
        });
        for (self.choices.items) |record| try appendJsonLine(allocator, &out, ChoiceWire.from(record));
        for (self.transitions.items) |record| try appendJsonLine(allocator, &out, TransitionWire.from(record));
        for (self.faults.items) |record| try appendJsonLine(allocator, &out, FaultWire.from(record));
        for (self.events.items) |record| try appendJsonLine(allocator, &out, EventWire.from(record));
        for (self.observations.items) |record| try appendJsonLine(allocator, &out, ObservationWire.from(record));
        for (self.properties.items) |record| try appendJsonLine(allocator, &out, PropertyWire.from(record));
        for (self.failures.items) |record| try appendJsonLine(allocator, &out, FailureWire.from(record));
        try appendJsonLine(allocator, &out, SummaryWire.from(self.summary.?));
        return try out.toOwnedSlice(allocator);
    }

    /// Compares the canonical wire artifact without materializing either full
    /// trace. The success path compares wire values directly. A mismatch
    /// renders only the first unequal JSONL record into two reusable buffers,
    /// preserving diagnostics while bounding memory to one record.
    pub fn canonicalEqual(
        self: *const Trace,
        other: *const Trace,
        allocator: std.mem.Allocator,
    ) !bool {
        try self.validate();
        try other.validate();

        var expected_line: std.ArrayListUnmanaged(u8) = .empty;
        defer expected_line.deinit(allocator);
        var actual_line: std.ArrayListUnmanaged(u8) = .empty;
        defer actual_line.deinit(allocator);

        if (!try canonicalLineEqual(
            allocator,
            &expected_line,
            &actual_line,
            "header",
            0,
            HeaderWire{
                .system = self.header.system,
                .scenario = self.header.scenario,
                .scenario_version = self.header.scenario_version,
                .source_revision = self.header.source_revision,
                .target = self.header.target,
                .optimize = self.header.optimize,
            },
            HeaderWire{
                .system = other.header.system,
                .scenario = other.header.scenario,
                .scenario_version = other.header.scenario_version,
                .source_revision = other.header.source_revision,
                .target = other.header.target,
                .optimize = other.header.optimize,
            },
        )) return false;
        if (!try canonicalLineEqual(
            allocator,
            &expected_line,
            &actual_line,
            "config",
            0,
            ConfigWire{
                .seed = self.config.seed,
                .transition_budget = self.config.transition_budget,
                .resource_budget = self.config.resource_budget,
                .fixture_hashes = self.config.fixture_hashes,
                .feature_flags = self.config.feature_flags,
                .backend_ids = self.config.backend_ids,
                .scenario_parameters = self.config.scenario_parameters,
            },
            ConfigWire{
                .seed = other.config.seed,
                .transition_budget = other.config.transition_budget,
                .resource_budget = other.config.resource_budget,
                .fixture_hashes = other.config.fixture_hashes,
                .feature_flags = other.config.feature_flags,
                .backend_ids = other.config.backend_ids,
                .scenario_parameters = other.config.scenario_parameters,
            },
        )) return false;

        if (!canonicalLengthEqual("choice", self.choices.items.len, other.choices.items.len)) return false;
        for (self.choices.items, other.choices.items, 0..) |expected, actual, index|
            if (!try canonicalLineEqual(allocator, &expected_line, &actual_line, "choice", index, ChoiceWire.from(expected), ChoiceWire.from(actual))) return false;

        if (!canonicalLengthEqual("transition", self.transitions.items.len, other.transitions.items.len)) return false;
        for (self.transitions.items, other.transitions.items, 0..) |expected, actual, index|
            if (!try canonicalLineEqual(allocator, &expected_line, &actual_line, "transition", index, TransitionWire.from(expected), TransitionWire.from(actual))) return false;

        if (!canonicalLengthEqual("fault", self.faults.items.len, other.faults.items.len)) return false;
        for (self.faults.items, other.faults.items, 0..) |expected, actual, index|
            if (!try canonicalLineEqual(allocator, &expected_line, &actual_line, "fault", index, FaultWire.from(expected), FaultWire.from(actual))) return false;

        if (!canonicalLengthEqual("event", self.events.items.len, other.events.items.len)) return false;
        for (self.events.items, other.events.items, 0..) |expected, actual, index|
            if (!try canonicalLineEqual(allocator, &expected_line, &actual_line, "event", index, EventWire.from(expected), EventWire.from(actual))) return false;

        if (!canonicalLengthEqual("observation", self.observations.items.len, other.observations.items.len)) return false;
        for (self.observations.items, other.observations.items, 0..) |expected, actual, index|
            if (!try canonicalLineEqual(allocator, &expected_line, &actual_line, "observation", index, ObservationWire.from(expected), ObservationWire.from(actual))) return false;

        if (!canonicalLengthEqual("property", self.properties.items.len, other.properties.items.len)) return false;
        for (self.properties.items, other.properties.items, 0..) |expected, actual, index|
            if (!try canonicalLineEqual(allocator, &expected_line, &actual_line, "property", index, PropertyWire.from(expected), PropertyWire.from(actual))) return false;

        if (!canonicalLengthEqual("failure", self.failures.items.len, other.failures.items.len)) return false;
        for (self.failures.items, other.failures.items, 0..) |expected, actual, index|
            if (!try canonicalLineEqual(allocator, &expected_line, &actual_line, "failure", index, FailureWire.from(expected), FailureWire.from(actual))) return false;

        return try canonicalLineEqual(
            allocator,
            &expected_line,
            &actual_line,
            "summary",
            0,
            SummaryWire.from(self.summary.?),
            SummaryWire.from(other.summary.?),
        );
    }
};

const HeaderWire = struct {
    type: []const u8 = "header",
    format: []const u8 = format,
    simulator_abi: u32 = simulator_abi,
    system: []const u8,
    scenario: []const u8,
    scenario_version: u32,
    source_revision: []const u8,
    target: []const u8,
    optimize: []const u8,
};
const ConfigWire = struct {
    type: []const u8 = "config",
    seed: ?u64,
    transition_budget: u64,
    resource_budget: u64,
    fixture_hashes: []const u64,
    feature_flags: []const u64,
    backend_ids: []const u64,
    scenario_parameters: []const Parameter,
};
const ChoiceWire = struct {
    type: []const u8 = "choice",
    site_id: u64,
    site_name: []const u8,
    occurrence: u64,
    enabled_ids: []const u64,
    selected_id: u64,
    fn from(record: ChoiceRecord) ChoiceWire {
        return .{ .site_id = record.site_id, .site_name = record.site_name, .occurrence = record.occurrence, .enabled_ids = record.enabled_ids, .selected_id = record.selected_id };
    }
};
const TransitionWire = struct {
    type: []const u8 = "transition",
    index: u64,
    id: u64,
    name: []const u8,
    kind: transition.Kind,
    actor_id: ?u64,
    resource_id: ?u64,
    parameter: i64,
    payload_digest: u64,
    fn from(record: TransitionRecord) TransitionWire {
        return .{
            .index = record.index,
            .id = record.id,
            .name = record.name,
            .kind = record.kind,
            .actor_id = record.actor_id,
            .resource_id = record.resource_id,
            .parameter = record.parameter,
            .payload_digest = record.payload_digest,
        };
    }
};
const FaultWire = struct {
    type: []const u8 = "fault",
    index: u64,
    id: u64,
    name: []const u8,
    phase: FaultPhase,
    fn from(record: FaultRecord) FaultWire {
        return .{ .index = record.index, .id = record.id, .name = record.name, .phase = record.phase };
    }
};
const EventWire = struct {
    type: []const u8 = "event",
    index: u64,
    ordinal: u64,
    id: u64,
    name: []const u8,
    kind: event.Kind,
    actor_id: ?u64,
    resource_id: ?u64,
    payload_digest: u64,
    fn from(record: EventRecord) EventWire {
        return .{ .index = record.index, .ordinal = record.ordinal, .id = record.id, .name = record.name, .kind = record.kind, .actor_id = record.actor_id, .resource_id = record.resource_id, .payload_digest = record.payload_digest };
    }
};
const ObservationWire = struct {
    type: []const u8 = "observation",
    index: u64,
    digest: u64,
    features: []const observation.Feature,
    fn from(record: ObservationRecord) ObservationWire {
        return .{ .index = record.index, .digest = record.digest, .features = record.features };
    }
};
const PropertyWire = struct {
    type: []const u8 = "property",
    index: u64,
    property_id: u64,
    name: []const u8,
    kind: property.Kind,
    condition: bool,
    details: []const u8,
    fn from(record: PropertyRecord) PropertyWire {
        return .{ .index = record.index, .property_id = record.property_id, .name = record.name, .kind = record.kind, .condition = record.condition, .details = record.details };
    }
};
const FailureWire = struct {
    type: []const u8 = "failure",
    index: u64,
    class: FailureClass,
    property_id: ?u64,
    identity: []const u8,
    fingerprint: u64,
    observation_digest: ?u64,
    fn from(record: FailureRecord) FailureWire {
        return .{ .index = record.index, .class = record.class, .property_id = record.property_id, .identity = record.identity, .fingerprint = record.fingerprint, .observation_digest = record.observation_digest };
    }
};
const SummaryWire = struct {
    type: []const u8 = "summary",
    transitions: u64,
    final_observation_digest: u64,
    property_failures: u64,
    fn from(summary: Summary) SummaryWire {
        return .{ .transitions = summary.transitions, .final_observation_digest = summary.final_observation_digest, .property_failures = summary.property_failures };
    }
};

fn appendJsonLine(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: anytype) !void {
    const encoded = try std.json.Stringify.valueAlloc(allocator, value, .{});
    defer allocator.free(encoded);
    try out.appendSlice(allocator, encoded);
    try out.append(allocator, '\n');
}

fn canonicalLengthEqual(section: []const u8, expected: usize, actual: usize) bool {
    if (expected == actual) return true;
    std.log.err("exact replay canonical {s} count diverged expected={d} actual={d}", .{
        section,
        expected,
        actual,
    });
    return false;
}

fn canonicalLineEqual(
    allocator: std.mem.Allocator,
    expected_line: *std.ArrayListUnmanaged(u8),
    actual_line: *std.ArrayListUnmanaged(u8),
    section: []const u8,
    index: usize,
    expected: anytype,
    actual: @TypeOf(expected),
) !bool {
    if (canonicalValueEqual(expected, actual)) return true;

    expected_line.clearRetainingCapacity();
    actual_line.clearRetainingCapacity();
    try appendJsonLine(allocator, expected_line, expected);
    try appendJsonLine(allocator, actual_line, actual);

    const max_diagnostic_bytes = 1024;
    std.log.err(
        "exact replay canonical {s}[{d}] diverged\nexpected: {s}\nactual:   {s}",
        .{
            section,
            index,
            expected_line.items[0..@min(expected_line.items.len, max_diagnostic_bytes)],
            actual_line.items[0..@min(actual_line.items.len, max_diagnostic_bytes)],
        },
    );
    return false;
}

/// `std.meta.eql` deliberately compares slices by address. Canonical trace
/// equality instead follows slices because independently constructed record and
/// replay artifacts must compare by value.
fn canonicalValueEqual(expected: anytype, actual: @TypeOf(expected)) bool {
    const T = @TypeOf(expected);
    return switch (@typeInfo(T)) {
        .@"struct" => |info| blk: {
            inline for (info.fields) |field| {
                if (!canonicalValueEqual(@field(expected, field.name), @field(actual, field.name))) break :blk false;
            }
            break :blk true;
        },
        .array => blk: {
            for (expected, actual) |expected_item, actual_item| {
                if (!canonicalValueEqual(expected_item, actual_item)) break :blk false;
            }
            break :blk true;
        },
        .pointer => |info| switch (info.size) {
            .slice => blk: {
                if (expected.len != actual.len) break :blk false;
                for (expected, actual) |expected_item, actual_item| {
                    if (!canonicalValueEqual(expected_item, actual_item)) break :blk false;
                }
                break :blk true;
            },
            .one, .many, .c => expected == actual,
        },
        .optional => if (expected) |expected_value|
            if (actual) |actual_value| canonicalValueEqual(expected_value, actual_value) else false
        else
            actual == null,
        .@"union" => |info| blk: {
            if (info.tag_type == null) @compileError("cannot compare untagged canonical wire union " ++ @typeName(T));
            const expected_tag = std.meta.activeTag(expected);
            if (expected_tag != std.meta.activeTag(actual)) break :blk false;
            switch (expected) {
                inline else => |value, tag| break :blk canonicalValueEqual(value, @field(actual, @tagName(tag))),
            }
        },
        .error_union => if (expected) |expected_value|
            if (actual) |actual_value| canonicalValueEqual(expected_value, actual_value) else |_| false
        else |expected_error| if (actual) |_| false else |actual_error| expected_error == actual_error,
        .vector => @reduce(.And, expected == actual),
        else => expected == actual,
    };
}

pub fn parseAlloc(allocator: std.mem.Allocator, encoded: []const u8) !Trace {
    var lines = std.mem.splitScalar(u8, encoded, '\n');
    const header_line = lines.next() orelse return error.MissingTraceHeader;
    if (header_line.len == 0) return error.MissingTraceHeader;
    var parsed_header = try std.json.parseFromSlice(HeaderWire, allocator, header_line, .{ .ignore_unknown_fields = false });
    defer parsed_header.deinit();
    if (!std.mem.eql(u8, parsed_header.value.type, "header") or !std.mem.eql(u8, parsed_header.value.format, format) or parsed_header.value.simulator_abi != simulator_abi) return error.IncompatibleTrace;

    const config_line = lines.next() orelse return error.MissingTraceConfig;
    var parsed_config = try std.json.parseFromSlice(ConfigWire, allocator, config_line, .{ .ignore_unknown_fields = false });
    defer parsed_config.deinit();
    if (!std.mem.eql(u8, parsed_config.value.type, "config")) return error.InvalidTraceRecord;

    var result = try Trace.init(allocator, .{
        .system = parsed_header.value.system,
        .scenario = parsed_header.value.scenario,
        .scenario_version = parsed_header.value.scenario_version,
        .source_revision = parsed_header.value.source_revision,
        .target = parsed_header.value.target,
        .optimize = parsed_header.value.optimize,
    }, .{
        .seed = parsed_config.value.seed,
        .transition_budget = parsed_config.value.transition_budget,
        .resource_budget = parsed_config.value.resource_budget,
        .fixture_hashes = parsed_config.value.fixture_hashes,
        .feature_flags = parsed_config.value.feature_flags,
        .backend_ids = parsed_config.value.backend_ids,
        .scenario_parameters = parsed_config.value.scenario_parameters,
    });
    errdefer result.deinit();

    var saw_summary = false;
    var last_record_rank: u8 = 2;
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        if (saw_summary) return error.RecordAfterSummary;
        var discriminator = try std.json.parseFromSlice(struct { type: []const u8 }, allocator, line, .{ .ignore_unknown_fields = true });
        defer discriminator.deinit();
        const record_type = discriminator.value.type;
        const rank = recordRank(record_type) orelse return error.UnknownTraceRecord;
        if (rank < last_record_rank) return error.NonCanonicalRecordOrder;
        last_record_rank = rank;
        if (std.mem.eql(u8, record_type, "choice")) {
            var parsed = try std.json.parseFromSlice(ChoiceWire, allocator, line, .{ .ignore_unknown_fields = false });
            defer parsed.deinit();
            try result.addChoice(.{ .site_id = parsed.value.site_id, .site_name = parsed.value.site_name, .occurrence = parsed.value.occurrence, .enabled_ids = parsed.value.enabled_ids, .selected_id = parsed.value.selected_id });
        } else if (std.mem.eql(u8, record_type, "transition")) {
            var parsed = try std.json.parseFromSlice(TransitionWire, allocator, line, .{ .ignore_unknown_fields = false });
            defer parsed.deinit();
            try result.addTransition(.{
                .index = parsed.value.index,
                .id = parsed.value.id,
                .name = parsed.value.name,
                .kind = parsed.value.kind,
                .actor_id = parsed.value.actor_id,
                .resource_id = parsed.value.resource_id,
                .parameter = parsed.value.parameter,
                .payload_digest = parsed.value.payload_digest,
            });
        } else if (std.mem.eql(u8, record_type, "fault")) {
            var parsed = try std.json.parseFromSlice(FaultWire, allocator, line, .{ .ignore_unknown_fields = false });
            defer parsed.deinit();
            try result.addFault(.{ .index = parsed.value.index, .id = parsed.value.id, .name = parsed.value.name, .phase = parsed.value.phase });
        } else if (std.mem.eql(u8, record_type, "event")) {
            var parsed = try std.json.parseFromSlice(EventWire, allocator, line, .{ .ignore_unknown_fields = false });
            defer parsed.deinit();
            try result.addEvent(.{ .index = parsed.value.index, .ordinal = parsed.value.ordinal, .id = parsed.value.id, .name = parsed.value.name, .kind = parsed.value.kind, .actor_id = parsed.value.actor_id, .resource_id = parsed.value.resource_id, .payload_digest = parsed.value.payload_digest });
        } else if (std.mem.eql(u8, record_type, "observation")) {
            var parsed = try std.json.parseFromSlice(ObservationWire, allocator, line, .{ .ignore_unknown_fields = false });
            defer parsed.deinit();
            try result.addObservation(.{ .index = parsed.value.index, .digest = parsed.value.digest, .features = parsed.value.features });
        } else if (std.mem.eql(u8, record_type, "property")) {
            var parsed = try std.json.parseFromSlice(PropertyWire, allocator, line, .{ .ignore_unknown_fields = false });
            defer parsed.deinit();
            try result.addProperty(.{ .index = parsed.value.index, .property_id = parsed.value.property_id, .name = parsed.value.name, .kind = parsed.value.kind, .condition = parsed.value.condition, .details = parsed.value.details });
        } else if (std.mem.eql(u8, record_type, "failure")) {
            var parsed = try std.json.parseFromSlice(FailureWire, allocator, line, .{ .ignore_unknown_fields = false });
            defer parsed.deinit();
            try result.addFailure(.{ .index = parsed.value.index, .class = parsed.value.class, .property_id = parsed.value.property_id, .identity = parsed.value.identity, .fingerprint = parsed.value.fingerprint, .observation_digest = parsed.value.observation_digest });
        } else if (std.mem.eql(u8, record_type, "summary")) {
            var parsed = try std.json.parseFromSlice(SummaryWire, allocator, line, .{ .ignore_unknown_fields = false });
            defer parsed.deinit();
            result.summary = .{ .transitions = parsed.value.transitions, .final_observation_digest = parsed.value.final_observation_digest, .property_failures = parsed.value.property_failures };
            saw_summary = true;
        } else return error.UnknownTraceRecord;
    }
    try result.validate();
    return result;
}

fn recordRank(record_type: []const u8) ?u8 {
    if (std.mem.eql(u8, record_type, "choice")) return 2;
    if (std.mem.eql(u8, record_type, "transition")) return 3;
    if (std.mem.eql(u8, record_type, "fault")) return 4;
    if (std.mem.eql(u8, record_type, "event")) return 5;
    if (std.mem.eql(u8, record_type, "observation")) return 6;
    if (std.mem.eql(u8, record_type, "property")) return 7;
    if (std.mem.eql(u8, record_type, "failure")) return 8;
    if (std.mem.eql(u8, record_type, "summary")) return 9;
    return null;
}

test "canonical value equality follows nested slice contents" {
    const expected_ids = try std.testing.allocator.dupe(u64, &.{ 11, 22 });
    defer std.testing.allocator.free(expected_ids);
    const actual_ids = try std.testing.allocator.dupe(u64, &.{ 11, 22 });
    defer std.testing.allocator.free(actual_ids);
    const different_ids = try std.testing.allocator.dupe(u64, &.{ 11, 23 });
    defer std.testing.allocator.free(different_ids);

    const expected_name = try std.testing.allocator.dupe(u8, "mode");
    defer std.testing.allocator.free(expected_name);
    const actual_name = try std.testing.allocator.dupe(u8, "mode");
    defer std.testing.allocator.free(actual_name);
    const expected_parameters = [_]Parameter{.{ .id = 7, .name = expected_name, .value = 3 }};
    const actual_parameters = [_]Parameter{.{ .id = 7, .name = actual_name, .value = 3 }};
    const expected_parameter_slice: []const Parameter = &expected_parameters;
    const actual_parameter_slice: []const Parameter = &actual_parameters;

    try std.testing.expect(canonicalValueEqual(expected_ids, actual_ids));
    try std.testing.expect(!canonicalValueEqual(expected_ids, different_ids));
    try std.testing.expect(canonicalValueEqual(expected_parameter_slice, actual_parameter_slice));
}
