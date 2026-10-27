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
const scoped = @import("scoped_producer_inputs.zig");
const publication = @import("artifact_publication.zig");
const provenance = @import("artifact_producer_provenance.zig");
const transport = @import("artifact_publication_transport_codec.zig");
const keys = @import("../internal_keys.zig");
const inventory = @import("artifact_inventory.zig");
const A = std.mem.Allocator;

test "scoped producer APF4 APB2 and JSON preserve inputs and legacy wire identities" {
    const Fixture = struct {
        fn run(a: A) !void {
            const output = try keys.embeddingArtifactKeyForDocumentAlloc(a, "doc", "model");
            defer a.free(output);
            const source: publication.Source = .{ .document_key = "doc", .content_digest = @splat(1), .timestamp = 1, .input_position = .{ .raft = .{ .term = 1, .index = 1 } } };
            const term: scoped.Term = .{ .producer_name = "idx", .artifact_name = "model", .definition = @splat(7), .reprocess_generation = 0, .value_identity = @splat(8), .input_revision = @splat(9) };
            const guard: scoped.Guard = .{ .source_index = 0, .canonical_document = 1, .created_generation = 1, .observed_root = @splat(10), .terms = (&term)[0..1] };
            var command: publication.Command = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "idx", .producer_generation = 1, .producer_artifact_name = "model", .sources = (&source)[0..1], .input_scopes = (&guard)[0..1], .mutations = &.{.{ .family = .base_vector, .key = output, .value = null, .source_index = 0 }}, .publication_digest = @splat(0) };
            command.publication_digest = command.digest();
            try command.validate(a);
            var proof = try provenance.fromCommand(a, command);
            defer proof.deinit();
            const bytes = try provenance.encodeAlloc(a, proof.proof);
            defer a.free(bytes);
            try std.testing.expectEqualStrings("APF4", bytes[0..4]);
            var decoded = try provenance.decodeAlloc(a, bytes);
            defer decoded.deinit();
            try std.testing.expectEqualDeep(proof.proof, decoded.proof);
            const encoded = try transport.encodeAlloc(a, command);
            defer a.free(encoded);
            try std.testing.expectEqualStrings("APB2", encoded[0..4]);
            var imported = try transport.decodeBorrowed(a, encoded);
            defer imported.deinit();
            try std.testing.expectEqualDeep(command, imported.command);
            const json = try std.json.Stringify.valueAlloc(a, command, .{});
            defer a.free(json);
            var parsed = try std.json.parseFromSlice(publication.Command, a, json, .{});
            defer parsed.deinit();
            try std.testing.expectEqualDeep(command, parsed.value);
            var next_source = source;
            next_source.timestamp = 2;
            next_source.input_position = .{ .raft = .{ .term = 1, .index = 2 } };
            next_source.content_digest = @splat(3);
            var next_guard = guard;
            next_guard.observed_root = @splat(11);
            var next = command;
            next.sources = (&next_source)[0..1];
            next.input_scopes = (&next_guard)[0..1];
            next.publication_digest = next.digest();
            try next.validate(a);
            try std.testing.expectEqualDeep(command.inputDigest(), next.inputDigest());
            try std.testing.expect(!std.mem.eql(u8, &command.publication_digest, &next.publication_digest));
            try proof.proof.requireInheritedBy(next);
            var legacy = command;
            legacy.input_scopes = &.{};
            legacy.publication_digest = legacy.digest();
            var old_proof = try provenance.fromCommand(a, legacy);
            defer old_proof.deinit();
            const old_bytes = try provenance.encodeAlloc(a, old_proof.proof);
            defer a.free(old_bytes);
            try std.testing.expectEqualStrings("APF3", old_bytes[0..4]);
            const old_json = try std.json.Stringify.valueAlloc(a, legacy, .{});
            defer a.free(old_json);
            try std.testing.expect(std.mem.indexOf(u8, old_json, "input_scopes") == null);
            const old_transport = try transport.encodeAlloc(a, legacy);
            defer a.free(old_transport);
            try std.testing.expectEqualStrings("APB1", old_transport[0..4]);
            try std.testing.expectError(error.EnrichmentSourceChanged, old_proof.proof.requireInheritedBy(command));
            try std.testing.expectError(error.EnrichmentSourceChanged, proof.proof.requireInheritedBy(legacy));
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

fn indexCatalog(a: A) ![]u8 {
    const types = @import("types.zig");
    const configs = [_]types.IndexConfig{
        .{ .name = "idx", .kind = .dense_vector, .config_json = "{\"field\":\"vector\",\"dims\":2,\"embedding_name\":\"model\"}", .coverage_generation = 7 },
        .{ .name = "other", .kind = .dense_vector, .config_json = "{\"field\":\"vector\",\"dims\":2,\"embedding_name\":\"other_model\"}", .coverage_generation = 9 },
    };
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    try out.appendSlice(a, "AIDX\x02\x00\x00\x00\x02\x00\x00\x00");
    for (configs) |config| {
        var length: [4]u8 = undefined;
        std.mem.writeInt(u32, &length, @intCast(config.name.len), .little);
        try out.appendSlice(a, &length);
        try out.appendSlice(a, config.name);
        try out.append(a, @backingInt(config.kind));
        std.mem.writeInt(u32, &length, @intCast(config.config_json.len), .little);
        try out.appendSlice(a, &length);
        try out.appendSlice(a, config.config_json);
        var generation: [8]u8 = undefined;
        std.mem.writeInt(u64, &generation, config.coverage_generation, .little);
        try out.appendSlice(a, &generation);
    }
    return out.toOwnedSlice(a);
}

test "scoped producer native publication preserves unrelated edits and rejects forged scopes ABA and reprocess" {
    const a = std.testing.allocator;
    const ds = @import("../docstore.zig");
    const lsm = @import("../lsm_backend.zig");
    const identity = @import("doc_identity.zig");
    var backend = lsm.Backend.init(a, .{});
    defer backend.close();
    var store = try ds.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    store.document_payload_prototype = true;
    const schema = @import("../schema.zig");
    const encoded_schema = try schema.serializeSchema(a, .{});
    defer a.free(encoded_schema);
    try store.put(schema.schema_key, encoded_schema);
    const physical = try keys.documentKeyAlloc(a, "doc");
    defer a.free(physical);
    const ttl = try keys.ttlKeyAlloc(a, "doc");
    defer a.free(ttl);
    var timestamp: [8]u8 = undefined;
    std.mem.writeInt(u64, &timestamp, 1, .little);
    try store.put(ttl, &timestamp);
    const body = try a.alloc(u8, 128 * 1024);
    defer a.free(body);
    @memset(body, 'x');
    const first = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":1}}", .{body});
    defer a.free(first);
    const second = try std.fmt.allocPrint(a, "{{\"body\":\"{s}\",\"metadata\":2}}", .{body});
    defer a.free(second);
    try store.put(physical, first);
    try store.put(physical, second);
    const id_namespace: identity.Namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 };
    var namespace: [24]u8 = undefined;
    identity.encodeNamespace(&namespace, id_namespace);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try identity.writeNamespaceTxn(&txn, id_namespace);
        _ = try identity.ensureOrdinalForNamespaceTxn(a, &txn, id_namespace, 11, "doc");
        try txn.commit();
    }
    const source_authority = @import("../source_authority.zig");
    try store.put(source_authority.key, &(source_authority.State{ .kind = .native, .namespace = namespace }).encode());
    const indexes = try indexCatalog(a);
    defer a.free(indexes);
    const catalogs: inventory.Catalogs = .{ .indexes = indexes, .enrichments = "[{\"name\":\"model\",\"kind\":\"embedding\",\"source_field\":\"body\",\"expected_dims\":2},{\"name\":\"other_model\",\"kind\":\"embedding\",\"source_field\":\"metadata\",\"expected_dims\":2}]" };
    const ordered: inventory.Command = .{ .namespace = namespace, .binding = .{ .epoch = 1, .digest = catalogs.digest(), .semantic_digest = try catalogs.semanticDigest(a), .effect_protocol = 17 }, .catalogs = catalogs };
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(inventory.index_key, catalogs.indexes);
        try txn.put(inventory.enrichment_key, catalogs.enrichments);
        try inventory.stageOrdered(a, &txn, ordered, 1);
        try txn.commit();
    }
    var definitions = try scoped.Catalog.init(a, catalogs);
    defer definitions.deinit();
    const definition = definitions.producer("idx", "model").?;
    const other = definitions.producer("other", "other_model").?;
    var source: publication.Source = .{ .document_key = "doc", .content_digest = undefined, .timestamp = 1, .input_position = null };
    std.crypto.hash.sha2.Sha256.hash(second, &source.content_digest, .{});
    var term: scoped.Term = undefined;
    var guard: scoped.Guard = undefined;
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const current = try scoped.current(a, &read, "doc");
        defer current.deinit(a);
        source.input_position = try publication.inputRevision(&read, namespace, "doc");
        term = .{ .producer_name = "idx", .artifact_name = "model", .definition = definition.digest, .reprocess_generation = 0, .value_identity = current.value("body"), .input_revision = try scoped.inputRevision(definition.*, namespace, "doc", current, 0) };
        guard = .{ .source_index = 0, .canonical_document = current.canonical_document, .created_generation = current.created_generation, .observed_root = current.root.digest, .terms = (&term)[0..1] };
    }
    const output = try keys.embeddingArtifactKeyForDocumentAlloc(a, "doc", "model");
    defer a.free(output);
    var command: publication.Command = .{ .namespace = namespace, .authority_epoch = 1, .catalog_digest = catalogs.digest(), .producer_name = "idx", .producer_generation = definition.generation, .producer_artifact_name = "model", .sources = (&source)[0..1], .input_scopes = (&guard)[0..1], .mutations = &.{.{ .family = .base_vector, .key = output, .value = null, .source_index = 0 }}, .publication_digest = @splat(0) };
    command.publication_digest = command.digest();
    try command.validate(a);
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var activation: publication.Command = .{ .mode = .activate, .namespace = namespace, .authority_epoch = 1, .catalog_digest = catalogs.digest(), .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
        activation.publication_digest = activation.digest();
        try publication.stageAuthority(&txn, activation);
        try @import("artifact_producer_validation.zig").begin(a, &txn, (try publication.authority(&txn)).?);
        const materialized: publication.Materialization = .{ .position = .{ .raft = .{ .term = 1, .index = 40 } }, .replay_sequence = 40 };
        try txn.put(&publication.materializationRevisionKey(namespace, "doc"), &try materialized.encode());
        try txn.commit();
    }
    const owners = try command.outputSources();
    const Observation = @import("artifact_stream_observation.zig").Observation;
    var original_observation: Observation = undefined;
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try publication.validateCommandSources(a, &read, command, owners);
        original_observation = try Observation.capture(&read, "doc");
        const admitted = try schema.deserializeSchema(a, try read.get(schema.schema_key));
        defer schema.freeSchema(a, admitted);
        try std.testing.expectEqual(scoped.minimum_reader_format, admitted.decoded_format_version);
    }
    // Install the real immutable proof, receipt and output revision witnesses.
    // Subsequent acceptance must follow these persisted records, rather than
    // validating an in-memory proof only.
    {
        var proof = try provenance.fromCommand(a, command);
        defer proof.deinit();
        const bytes = try provenance.encodeAlloc(a, proof.proof);
        defer a.free(bytes);
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        const position: publication.Position = .{ .raft = .{ .term = 1, .index = 41 } };
        try publication.stageReceipts(&txn, command, 41);
        try publication.stageArtifactRevisions(&txn, command, position);
        try provenance.stage(&txn, command, bytes, position);
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.patchMetadata(physical, &.{.{ .name = "metadata", .value_json = "3" }});
        std.mem.writeInt(u64, &timestamp, 2, .little);
        try txn.put(ttl, &timestamp);
        const materialized: publication.Materialization = .{ .position = .{ .raft = .{ .term = 1, .index = 42 } }, .replay_sequence = 42 };
        try txn.put(&publication.materializationRevisionKey(namespace, "doc"), &try materialized.encode());
        try txn.commit();
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        const MetadataOnly = struct {
            txn: *ds.DocStore.Txn,
            physical: []const u8,
            pub fn get(self: *@This(), name: []const u8) anyerror![]const u8 {
                if (std.mem.eql(u8, name, self.physical) or std.mem.startsWith(u8, name, @import("../document_payload.zig").prefix)) return error.UnexpectedDocumentBodyRead;
                return self.txn.get(name);
            }
            pub fn getPhysical(self: *@This(), name: []const u8) anyerror![]const u8 {
                if (std.mem.startsWith(u8, name, @import("../document_payload.zig").prefix)) return error.UnexpectedDocumentBodyRead;
                return self.txn.getPhysical(name);
            }
        };
        var metadata: MetadataOnly = .{ .txn = &read, .physical = physical };
        try publication.validateCommandSources(a, &metadata, command, owners);
        var metadata_accepted = (try provenance.readCurrentScopedForDocument(a, &metadata, command, "doc")) orelse return error.TestExpectedMetadataAcceptance;
        defer metadata_accepted.deinit();
        try std.testing.expect((try provenance.readCurrentScopedForDocument(a, &metadata, command, "missing")) == null);
        var proof = try provenance.fromCommand(a, command);
        defer proof.deinit();
        try proof.proof.validateInputs(a, &read);
        const current_source = try publication.capturePrimarySource(a, &read, namespace, "doc");
        defer a.free(current_source.document_key);
        try std.testing.expect(current_source.timestamp != source.timestamp);
        var accepted = (try provenance.readCurrentForSource(a, &read, command, current_source)) orelse return error.TestExpectedAcceptedProof;
        defer accepted.deinit();
        try std.testing.expectEqual(@as(u8, 4), accepted.owned.proof.version);
        // Completion binds the current obligation independently of the model
        // result's original observation. The old cut cannot advance it.
        try std.testing.expectError(error.EnrichmentSourceChanged, original_observation.requireCurrent(&read, "doc"));
        var observation = try Observation.capture(&read, "doc");
        try observation.observeProof("doc", accepted.owned.proof);
        try observation.requireCurrent(&read, "doc");
        try std.testing.expectEqualDeep(try publication.materializationRevision(&read, namespace, "doc"), observation.revision);
        try std.testing.expect(!std.meta.eql(original_observation.revision, observation.revision));
        try std.testing.expectEqual(@as(u64, 41), accepted.receipt.sequence);
        var forged = guard;
        const current = try scoped.current(a, &read, "doc");
        defer current.deinit(a);
        const wrong: scoped.Term = .{ .producer_name = "other", .artifact_name = "other_model", .definition = other.digest, .reprocess_generation = 0, .value_identity = current.value("metadata"), .input_revision = try scoped.inputRevision(other.*, namespace, "doc", current, 0) };
        forged.terms = (&wrong)[0..1];
        var malicious = command;
        malicious.input_scopes = (&forged)[0..1];
        malicious.publication_digest = malicious.digest();
        try malicious.validate(a);
        try std.testing.expectError(error.InvalidBatchRequest, publication.validateCommandSources(a, &read, malicious, owners));
        forged = guard;
        forged.require_whole_source = true;
        malicious.input_scopes = (&forged)[0..1];
        try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateCommandSources(a, &read, malicious, owners));
    }
    {
        const batch = @import("source_proof_batch.zig");
        const donor_namespace: publication.Namespace = @splat(9);
        const semantic = try catalogs.semanticDigest(a);
        const donor_binding: inventory.Binding = .{ .epoch = 2, .digest = catalogs.digest(), .semantic_digest = semantic, .effect_protocol = 17 };
        const receiver_binding: inventory.Binding = .{ .epoch = 1, .digest = catalogs.digest(), .semantic_digest = semantic, .effect_protocol = 17 };
        var plan = try batch.ReceiverProducerPlan.init(a, catalogs, donor_binding, catalogs, receiver_binding);
        defer plan.deinit();
        var donor_guard = guard;
        var donor_term = term;
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            var donor_source = try scoped.current(a, &read, "doc");
            defer donor_source.deinit(a);
            donor_source.canonical_document = 91;
            donor_source.created_generation = 12;
            donor_guard.canonical_document = 91;
            donor_guard.created_generation = 12;
            donor_term.input_revision = try scoped.inputRevision(definition.*, donor_namespace, "doc", donor_source, 0);
        }
        donor_guard.terms = (&donor_term)[0..1];
        var donor_command = command;
        donor_command.namespace = donor_namespace;
        donor_command.authority_epoch = 2;
        donor_command.input_scopes = (&donor_guard)[0..1];
        donor_command.publication_digest = donor_command.digest();
        var donor_proof = try provenance.fromCommand(a, donor_command);
        defer donor_proof.deinit();
        const bytes = try provenance.encodeAlloc(a, donor_proof.proof);
        defer a.free(bytes);
        const value = try batch.encodeValueAlloc(a, &.{1}, bytes);
        defer a.free(value);
        var decoded = try batch.decodeValue(a, donor_namespace, donor_command.publication_digest, value);
        defer decoded.deinit();
        const pin: publication.Digest = @splat(8);
        try store.put(&batch.witnessKey(donor_namespace, pin, donor_command.publication_digest), &decoded.record_digest);
        var read = try store.beginReadTxn();
        defer read.abort();
        var candidate = (try batch.prepareReceiverCandidate(a, &read, namespace, .{ .start = "doc", .end = "dop" }, pin, &plan, decoded)) orelse return error.TestExpectedAdoptionCandidate;
        defer candidate.deinit();
        try std.testing.expect(try batch.revalidateReceiverCandidate(a, &read, namespace, candidate));
        try std.testing.expect(candidate.sources[0].timestamp != donor_command.sources[0].timestamp);
        try std.testing.expectEqual(guard.canonical_document, candidate.input_scopes[0].canonical_document);
        try std.testing.expect(!std.mem.eql(u8, &donor_term.input_revision, &candidate.input_scopes[0].terms[0].input_revision));
        var adopted = try batch.buildAdoptedProofAlloc(a, &candidate);
        defer adopted.deinit(a);
        try std.testing.expectEqual(@as(u8, 4), adopted.proof.version);
        try adopted.proof.validateInputs(a, &read);
        var roundtrip = try provenance.decodeAlloc(a, adopted.encoded);
        defer roundtrip.deinit();
        try std.testing.expectEqualDeep(adopted.proof, roundtrip.proof);
        var different_observation = adopted.proof;
        var observed = candidate.sources[0];
        observed.timestamp += 1;
        different_observation.sources = (&observed)[0..1];
        try std.testing.expectEqualDeep(adopted.proof.input_digest, different_observation.inputCommand().inputDigest());
        try std.testing.expect(!std.mem.eql(u8, &adopted.proof.publication_digest, &different_observation.adoptionDigest()));
        const CheckAllocations = struct {
            fn run(alloc: A, source_catalogs: inventory.Catalogs, source_binding: inventory.Binding, target_binding: inventory.Binding) !void {
                var compiled = try batch.ReceiverProducerPlan.init(alloc, source_catalogs, source_binding, source_catalogs, target_binding);
                defer compiled.deinit();
            }
        };
        try @import("antfly_platform").allocator.checkAllAllocationFailures(a, CheckAllocations.run, .{ catalogs, donor_binding, receiver_binding });
    }
    const intent = @import("artifact_reprocess_intent.zig");
    const intent_key = try intent.embeddingKeyAlloc(a, "doc", "model");
    defer a.free(intent_key);
    try store.put(intent_key, &intent.encode("", 1));
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateCommandSources(a, &read, command, owners));
    }
    try store.delete(intent_key);
    try store.put(physical, "{\"body\":\"changed\",\"metadata\":3}");
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateCommandSources(a, &read, command, owners));
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try identity.markDeletedTxn(a, &txn, 12, "doc");
        try txn.delete(physical);
        _ = try identity.ensureOrdinalForNamespaceTxn(a, &txn, id_namespace, 13, "doc");
        try txn.put(physical, second);
        try txn.commit();
    }
    {
        var read = try store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, publication.validateCommandSources(a, &read, command, owners));
    }
}

test "scoped producer definition catalog copies borrowed metadata before subsequent reads" {
    const Reader = struct {
        scratch: [128]u8 = undefined,
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            const value = if (std.mem.eql(u8, key, inventory.index_key)) "indexes" else if (std.mem.eql(u8, key, inventory.enrichment_key)) "enrichments" else if (std.mem.eql(u8, key, inventory.resolver_key)) "resolvers" else return error.NotFound;
            @memset(&self.scratch, 0);
            @memcpy(self.scratch[0..value.len], value);
            return self.scratch[0..value.len];
        }
    };
    var reader: Reader = .{};
    var copy = try inventory.copyCatalogs(std.testing.allocator, &reader);
    defer copy.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("indexes", copy.indexes);
    try std.testing.expectEqualStrings("enrichments", copy.enrichments);
    try std.testing.expectEqualStrings("resolvers", copy.resolvers);
}

test "scoped producer inheritance conjoins definitions remaps ordinals and retains whole dependencies" {
    const Fixture = struct {
        fn run(a: A) !void {
            const context = @import("artifact_producer_context.zig");
            const first: publication.Source = .{ .document_key = "b", .content_digest = @splat(1), .timestamp = 1, .input_position = null };
            var token: context.Token = .{ .arena = .init(a), .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2), .producer_name = "idx", .producer_kind = .index, .producer_generation = 1, .artifact_name = "model", .source = first };
            defer token.deinit();
            const one: scoped.Term = .{ .producer_name = "idx", .artifact_name = "model", .definition = @splat(3), .reprocess_generation = 0, .value_identity = @splat(4), .input_revision = @splat(5) };
            var two = one;
            two.definition = @splat(6);
            two.producer_name = "other";
            var guard: scoped.Guard = .{ .source_index = 0, .canonical_document = 1, .created_generation = 1, .observed_root = @splat(7), .terms = (&one)[0..1] };
            token.input_scopes = try scoped.cloneIntoArena(token.arena.allocator(), (&guard)[0..1]);
            var incoming = first;
            incoming.timestamp = 2;
            incoming.content_digest = @splat(8);
            guard.terms = (&two)[0..1];
            try token.inheritScopedSources((&incoming)[0..1], (&guard)[0..1]);
            try std.testing.expectEqual(@as(usize, 2), token.input_scopes[0].terms.len);
            try std.testing.expect(!token.input_scopes[0].require_whole_source);
            const before_repeat = token.arena.state.used_list.?.end_index;
            try token.inheritScopedSources((&incoming)[0..1], (&guard)[0..1]);
            try std.testing.expectEqual(before_repeat, token.arena.state.used_list.?.end_index);
            try token.inheritSources((&incoming)[0..1]);
            try std.testing.expect(token.input_scopes[0].require_whole_source);
            try std.testing.expectEqual(@as(u64, 2), token.sources()[0].timestamp);
            try token.observePrecondition("zz", null, null);
            const earlier: publication.Source = .{ .document_key = "a", .content_digest = @splat(1), .timestamp = 1, .input_position = null };
            try token.inheritSources((&earlier)[0..1]);
            try std.testing.expectEqual(@as(u32, 1), token.input_scopes[0].source_index);
            try std.testing.expectEqual(@as(u32, 1), token.preconditions.items[0].source_index);
            var conflicting = incoming;
            conflicting.timestamp = 3;
            if (token.inheritSources((&conflicting)[0..1])) |_| return error.TestExpectedConflict else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.EnrichmentSourceChanged, err);
            }
            guard.created_generation = 2;
            if (token.inheritScopedSources((&incoming)[0..1], (&guard)[0..1])) |_| return error.TestExpectedConflict else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.EnrichmentSourceChanged, err);
            }
            guard.created_generation = 1;
            two.input_revision = @splat(9);
            if (token.inheritScopedSources((&incoming)[0..1], (&guard)[0..1])) |_| return error.TestExpectedConflict else |err| {
                if (err == error.OutOfMemory) return err;
                try std.testing.expectEqual(error.EnrichmentSourceChanged, err);
            }
            // Failed merges cannot partially remap or replace the live read set.
            try std.testing.expectEqual(@as(usize, 2), token.sources().len);
            try std.testing.expectEqual(@as(u32, 1), token.preconditions.items[0].source_index);
            try std.testing.expectEqual(@as(u64, 2), token.sources()[1].timestamp);
            const result = try token.command(&.{});
            try std.testing.expectEqualDeep(token.input_scopes, result.input_scopes);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
