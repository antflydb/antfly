// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Closed, allocation-free runtime qualification policy. Bundle integrity,
//! architecture recognition, and a qualified request are separate contracts.
//! Only the identity of bytes consumed by the live session may be checked here;
//! a pathname, listing manifest, or parsed bundle receipt is not a substitute.
//!
//! The production table holds exactly the artifacts that passed a reviewed
//! release qualification: exact weight/sidecar digests, backend, feature
//! set, and safe LengthContract bounds measured on the real artifact (see
//! zig/pkg/inference/models/gliner2/GLINER25.md for the record). This module
//! does not consume JSON evidence, expose a runtime override, or
//! authenticate release evidence. Adding a row is a reviewed release
//! decision, not an inference from successful loading or from the
//! architecture-wide runtime_available flag.
const std = @import("std");
const bundle = @import("gliner_boundary_bundle.zig");
const model = @import("gliner_boundary.zig");
const artifact = @import("gliner_boundary_artifact.zig");

pub const policy_version: u32 = 1;
pub const max_entries: usize = 64;
pub const Backend = enum { native, metal, cuda };

/// Required features come from the complete compiled request, never from
/// capability strings supplied by a model or client. A row declares support
/// for every combination of its listed features within that row's lengths.
/// Separate rows must not be combined to manufacture support for a mixed task.
/// Normal schema, option, resource, and tokenizer validation still applies.
pub const Feature = enum {
    entities,
    entity_attributes,
    classification_single,
    classification_multi,
    classification_ordinal,
    classification_structured,
    classification_constraints,
    legacy_structures,
    records_natural,
    records_latent,
    records_anchorless,
    field_choices,
    field_rules,
    record_occurrence_policy,
    relations,
    relation_endpoints,
    joint_ie,
    joint_constraints,
    /// The omitted single-window JointIE decoder selects the source beam contract.
    /// It is distinct from every explicit native algorithm, including auto.
    joint_fastino_v1,
    regex_validation,
    schema_descriptions,
    classification_context,
    word_whitespace,
    word_char,
    overlap_flat,
    overlap_allow,
    overlap_nested,
    overlap_longest,
    offset_utf8,
    offset_codepoints,
    offset_utf16,
    decoder_auto,
    decoder_exact,
    decoder_beam,
    best_effort,
    single_window,
    long_document,
    record_identity_occurrence,
    record_identity_semantic,
    confidence,
    spans,
};
pub const Features = std.EnumSet(Feature);

/// Both endpoints are inclusive. Comparisons do not add or multiply counts,
/// so an admitted upper endpoint of maxInt(u64) cannot wrap during matching.
pub const Range = struct {
    min: u64,
    max: u64,

    pub fn exact(value: u64) Range {
        return .{ .min = value, .max = value };
    }

    pub fn valid(self: Range) bool {
        return self.min <= self.max;
    }

    pub fn contains(self: Range, observed: Range) bool {
        return self.valid() and observed.valid() and self.min <= observed.min and observed.max <= self.max;
    }
};

/// A complete observed contract, or a row's explicitly admitted ranges.
/// All fields are required: an unknown token count must not default to zero.
/// For multiple observations, retain their minima AND maxima. Taking only
/// maxima could admit a short input through a row qualified only at long sizes.
pub const LengthContract = struct {
    /// Total input items in the public request, not the current execution batch.
    request_items: Range,
    /// Whole original document bytes, before any window slicing.
    document_bytes: Range,
    /// Whole original source words from the selected splitter. Excludes enum
    /// prefixes and synthetic terminal words; UTF-8 bytes are a different unit.
    document_words: Range,
    /// Number of windows for the whole document; one for the single-window path.
    window_count: Range,
    /// Actual prepared window text words: includes a synthetic terminal word,
    /// excludes the schema's enum prefix. It is not whole-document word count.
    window_words: Range,
    /// Actual padded encoder sequence dimension, including schema/prefix and
    /// special tokens. Never use document words or configured capacity here.
    padded_sequence_tokens: Range,

    pub fn valid(self: LengthContract) bool {
        inline for (@typeInfo(LengthContract).@"struct".fields) |field| {
            if (!@field(self, field.name).valid()) return false;
        }
        return true;
    }

    pub fn contains(self: LengthContract, observed: LengthContract) bool {
        inline for (@typeInfo(LengthContract).@"struct".fields) |field| {
            if (!@field(self, field.name).contains(@field(observed, field.name))) return false;
        }
        return true;
    }
};

/// A bounded-length rejection: identity, backend, and every required feature
/// already matched at least one production row (see start()/Candidates), but
/// the observed geometry falls outside every remaining candidate row's
/// reviewed range. Distinct from error.UnsupportedGlinerBoundaryRuntime,
/// which covers everything else this policy can refuse -- an unreviewed
/// identity, backend, feature, or a malformed table -- so a caller (and the
/// HTTP error mapping) can tell a "this document is too big/too batched for
/// what was reviewed" rejection apart from "this request shape was never
/// reviewed at all" and report the exceeded dimension by name. One error per
/// LengthContract field, in the same declaration order.
pub const LengthLimitError = error{
    GlinerBoundaryRequestItemsLimitExceeded,
    GlinerBoundaryDocumentBytesLimitExceeded,
    GlinerBoundaryDocumentWordsLimitExceeded,
    GlinerBoundaryWindowCountLimitExceeded,
    GlinerBoundaryWindowWordsLimitExceeded,
    GlinerBoundaryPaddedSequenceLimitExceeded,
};

/// Every error narrow()/require() can return: the generic closed-policy
/// refusal plus the named bounded-length rejections above.
pub const QualificationError = error{UnsupportedGlinerBoundaryRuntime} || LengthLimitError;

fn fieldLimitError(comptime field_name: []const u8) LengthLimitError {
    if (comptime std.mem.eql(u8, field_name, "request_items")) return error.GlinerBoundaryRequestItemsLimitExceeded;
    if (comptime std.mem.eql(u8, field_name, "document_bytes")) return error.GlinerBoundaryDocumentBytesLimitExceeded;
    if (comptime std.mem.eql(u8, field_name, "document_words")) return error.GlinerBoundaryDocumentWordsLimitExceeded;
    if (comptime std.mem.eql(u8, field_name, "window_count")) return error.GlinerBoundaryWindowCountLimitExceeded;
    if (comptime std.mem.eql(u8, field_name, "window_words")) return error.GlinerBoundaryWindowWordsLimitExceeded;
    if (comptime std.mem.eql(u8, field_name, "padded_sequence_tokens")) return error.GlinerBoundaryPaddedSequenceLimitExceeded;
    @compileError("gliner_boundary_qualification: unmapped LengthContract field " ++ field_name);
}

/// `previous` is a nonempty bitmask of rows that matched identity, backend,
/// and every required feature; `remaining` (all of `previous`'s rows failed
/// to contain `observed`) is why this is being called. Blame the first
/// LengthContract field, in declaration order, for which NONE of the
/// `previous` rows' range contains `observed`'s value -- the dimension that
/// actually closed off every candidate. If every field is individually
/// satisfiable by some previous row but no single row satisfies all of them
/// together, that is a cross-row combination rather than one measured bound;
/// blame the first field as a conservative default (still a genuine
/// bounded-length rejection, not an unreviewed request shape).
fn blamedLengthDimension(entries: []const Entry, previous: u64, observed: LengthContract) LengthLimitError {
    inline for (@typeInfo(LengthContract).@"struct".fields) |field| {
        var covered = false;
        for (entries, 0..) |entry, index| {
            const bit = @as(u64, 1) << @intCast(index);
            if (previous & bit != 0 and @field(entry.lengths, field.name).contains(@field(observed, field.name))) covered = true;
        }
        if (!covered) return fieldLimitError(field.name);
    }
    return fieldLimitError(@typeInfo(LengthContract).@"struct".fields[0].name);
}

/// Data only. Identity includes all five file sizes and digests, the variant,
/// and precision. No wildcard identity, implicit backend, or inherited limits.
pub const Entry = struct {
    identity: bundle.Identity,
    backend: Backend,
    features: Features,
    lengths: LengthContract,
};

// fastino/gliner2.5-base-v1, HuggingFace revision
// 72ac19b486cd4557424c8d61114e7530c243e9b0. Reviewed 2026-09-17 for the
// native and Metal backends at fp32 (the published safetensors precision;
// no GGUF conversion is qualified). Evidence:
//
//  - Identity: digests match scripts/gliner25/oracle_manifest.json's "base"
//    entry byte-for-byte and were reproduced independently with
//    `shasum -a 256` against the pulled artifact
//    (~/.antfly/inference/models/fastino/gliner2.5-base-v1).
//  - Correctness: native and Metal full-pipeline parity against the pinned
//    Python/Fastino reference for all ten canonical task fixtures
//    (testdata/gliner25/pipeline_cases_base.json) -- entities, relations,
//    entity attributes, classification, natural/latent/anchorless records,
//    legacy structures, enum fields, constrained classification, and
//    JointIE -- via `zig build inference-test -Doptimize=ReleaseFast --
//    --test-filter "gliner boundary"` with ANTFLY_GLINER25_BASE_MODEL_DIR
//    set to the pulled artifact: "gliner boundary pipeline Python parity
//    pinned base checkpoint all inference tasks" and "gliner boundary
//    device Metal pinned base full inference pipeline parity".
//  - Geometry: measured directly against the pinned tokenizer over the same
//    ten fixtures plus the shortest and longest reviewed requests, in
//    ../../extractors/gliner_boundary_qualification.zig ("gliner boundary
//    qualification measures pinned base checkpoint production geometry").
//    The bounds below are the exact observed range; widening them requires
//    new measurement, not extrapolation.
//  - Throughput: BENCHMARK.md's 2026-09-09 recorded CPU run for this exact
//    checkpoint (native/Python latency ratios 0.58-0.80 across the same ten
//    tasks; mixed-task median 36.174 ms native versus 45.159 ms Python).
//
// See zig/pkg/inference/models/gliner2/GLINER25.md for the full record,
// evidence pointers, and how to re-qualify a wider or different artifact.
const fastino_gliner25_base_v1 = bundle.Identity{
    .backbone = .base,
    .precision = .fp32,
    .weight = .{ .size_bytes = 774366564, .sha256 = "7274094de2e0c2a37a386f55fc4e23061a954da5bd7a335e7dfe56f2743c277a".* },
    .sidecars = .{
        .{ .size_bytes = 3150, .sha256 = "0eb92d00584d613aab32b2178f84a85176b62c87ae3689ce9084e83f6eba64d1".* },
        .{ .size_bytes = 857, .sha256 = "d36a845b9f25dcaf1ec45a1c4bdf65ea4ac20596537e14530ec9f660a63aeca4".* },
        .{ .size_bytes = 8341713, .sha256 = "cbc8ae6037812709c9c26f2a160f8dc48b0440bcb79c8141804259ae2d6adac3".* },
        .{ .size_bytes = 645, .sha256 = "0bf3ea0873234bd9bfdd3853c440395009ac6365a925b91654daed5396d655e1".* },
    },
};

// Every feature actually exercised, end to end, against the real weights by
// the parity tests above, plus the request-shaping options (confidence,
// spans, single window, default splitter/overlap/offset unit) needed to
// serve a plain extraction request. Long documents, non-default decoders,
// regex validators, typed relation endpoints, and the other Feature enum
// members are deliberately excluded: this artifact has not been measured
// against them yet.
const fastino_gliner25_base_v1_features = Features.initMany(&.{
    .entities,                 .entity_attributes,         .schema_descriptions,
    .classification_single,    .classification_structured, .classification_constraints,
    .legacy_structures,        .records_natural,           .records_latent,
    .records_anchorless,       .field_choices,             .field_rules,
    .record_occurrence_policy, .relations,                 .joint_ie,
    .decoder_auto,             .word_whitespace,           .overlap_flat,
    .offset_utf8,              .offset_codepoints,         .single_window,
    .confidence,               .spans,
});

// Exact min/max observed by the geometry-measuring test cited above: the
// ten canonical fixtures, the shortest and longest reviewed short requests,
// and examples/dogfood's real production schema (11 entities, 6 relations)
// against both its own short repro text and a realistic 107-word/610-byte
// corpus paragraph (zig/ENRICHMENTS.md). The wider schema alone roughly
// doubles padded_sequence_tokens versus the earlier 3-entity/2-relation
// rows at the same document length (56 -> 118), which is why this bound
// widened well past the document-length increase alone. A document needing
// more than this measured single-window range -- most of examples/dogfood's
// longer design-doc sections -- still requires long-document windowing,
// which remains unqualified (.long_document is not in the feature set
// above) and correctly fails closed with UnsupportedGlinerBoundaryRuntime.
//
// Lower bounds re-measured 2026-09-19 (same geometry test, two added cases):
// examples/dogfood's smallest real section, zig/SCHEMA.md's "Related Docs"
// list as docsaf emits it ("TODO.mdSERVERLESS.md": 20 bytes, 5 splitter
// words, 6 window words, 110 padded tokens with the dogfood schema), and a
// one-character document ("a": 1 byte, 1 word, 2 window words, 103 padded
// tokens). Both run end to end on native and Metal through the
// corpus-minimum provider tests in server/gliner_boundary_service_test.zig.
// Before this measurement the 26-byte floor (the shortest canonical fixture)
// made the SCHEMA.md section the one document a full in-process dogfood
// ingest rejected, and that rejection is terminal for the whole drain.
const fastino_gliner25_base_v1_lengths = LengthContract{
    .request_items = .{ .min = 1, .max = 1 },
    .document_bytes = .{ .min = 1, .max = 610 },
    .document_words = .{ .min = 1, .max = 112 },
    .window_count = .{ .min = 1, .max = 1 },
    .window_words = .{ .min = 2, .max = 112 },
    .padded_sequence_tokens = .{ .min = 14, .max = 218 },
};

// Long-document windowed execution, reviewed 2026-09-18. Evidence:
//
//  - Design: gliner_boundary_long_executor.zig's executor tokenizes and
//    admits each window serially (model tensors -- encoder/head activations
//    -- are freed after every window; only bounded scalar evidence survives
//    into the next), so a window's memory profile is bounded independent of
//    document length; cumulative admission
//    (Limits.max_total_encoded_tokens/max_total_attention_work) bounds total
//    work across the whole document. Entities are deduplicated across
//    overlapping windows by gliner_boundary_long_document.zig's
//    mergeMentions; relations are resolved only when both endpoints fall in
//    one window and then merged/deduplicated document-wide by
//    gliner_boundary_long_relations.zig's merge -- exactly the design this
//    row qualifies, not a superset of it.
//  - Correctness/shape: "gliner boundary long executor HTTP canonical
//    schema_version 2 shape for a real multi-window document with relations
//    native/Metal" and "gliner boundary long executor provider extractDirect
//    canonical schema_version 2 relations shape for a windowed request
//    native/Metal" (server/gliner_boundary_service_test.zig) exercise this
//    exact merge path end to end -- one against a real 37KB multi-window
//    design-doc section (zig/VOPR.md's "Completion-Claim Audit"), one
//    deterministically against the known repro relation -- through both the
//    HTTP handler and the in-process provider entry, and assert the
//    response matches zig/EXTRACT.md's canonical envelope (entities with
//    text/label/start/end/score; relations with type,
//    source.entity_index/target.entity_index, score) with no head/tail
//    fields, on both backends.
//  - Geometry: measured directly against the pinned tokenizer/planner (no
//    model weights) over the real examples/dogfood production schema (11
//    entities, 6 relations) against the same short fixtures as the
//    single-window row above, three real repository sections spanning the
//    corpus-wide range this file's long-document section documents (95th
//    percentile and max section size across zig/*.md and work-log/**/*.md):
//    zig/pkg/antfly/src/storage/lsm/LSM.md's "Read And Scan Work" (6.8KB),
//    zig/VOPR.md's "Completion-Claim Audit" (37KB), and zig/PDF.md's "Review
//    findings and required fixes" (99KB) -- plus two synthetic documents
//    built by concatenating real corpus sections (zig/PDF.md's two largest
//    real sections, per a docsaf-accurate Go-side sweep of the whole ingest
//    corpus) up to 130KB and 182KB/28275 words, well past every individual
//    real section docsaf currently produces, to qualify past the corpus's
//    single-section maximum with real margin -- each swept at
//    window_words=1024 (the wire's default; see extraction_v2.zig's
//    LongDocument doc comment and GLINER25.md's throughput section for why)
//    AND window_words=4096 (the widest a request may still explicitly opt
//    into), in ../../extractors/gliner_boundary_qualification.zig ("gliner
//    boundary qualification measures pinned base checkpoint long-document
//    production geometry"). The bounds below are the union of both sweeps'
//    exact observed ranges (window_count differs sharply by window size --
//    up to 29 windows at 1024 words each versus up to 8 at 4096 -- everything
//    else the two sweeps measured overlaps). This also qualifies the
//    request_items dimension up to the single reviewed value of 1: a request
//    batching more than one document into one call (examples/dogfood never
//    does this, but the inference server's generic "extract" task capability
//    advertisement did, until this same change fixed
//    resolvedExecutorBatchImplementation in server/server.zig to stop
//    advertising native batching for GLiNER boundary extraction) still
//    correctly fails closed, now with the named
//    error.GlinerBoundaryRequestItemsLimitExceeded instead of the generic
//    error.UnsupportedGlinerBoundaryRuntime -- see GLINER25.md's long-document
//    section for the production incident this traces to.
//  - Throughput: see GLINER25.md's long-document section for per-window and
//    per-section throughput on Metal, before and after switching the
//    default window size and adding grouped-window batching.
//
// This is a SEPARATE row from fastino_gliner25_base_v1's single-window row
// above, not a widening of it: the two rows require disjoint features
// (.single_window vs .long_document) and this row's feature set covers
// exactly examples/dogfood's real schema shape (entities + relations only;
// no entity attributes, classification, records, or JointIE have been
// measured through this merge path), so a request for any of those task
// types together with long-document windowing still correctly fails closed.
const fastino_gliner25_base_v1_long_document_features = Features.initMany(&.{
    .entities,    .relations,    .word_whitespace, .overlap_flat,
    .offset_utf8, .decoder_auto, .long_document,   .record_identity_occurrence,
    .confidence,  .spans,
});

// Union of the exact min/max observed at window_words=1024 and
// window_words=4096 across every case in the long-document geometry test
// cited above (the two short single-window-shaped fixtures, still requested
// with long_document.mode=window since examples/dogfood requests it
// unconditionally -- see index_config.go's knowledgeGraphIndexJSON -- plus
// the three real multi-window sections and the two synthetic 130KB/182KB
// documents). A document needing more than this measured range --
// window_count > 29, or bytes/words/tokens above the printed maxima -- has
// not been measured and correctly fails closed with the named
// error.GlinerBoundaryWindowCountLimitExceeded (etc.), not the generic
// error.UnsupportedGlinerBoundaryRuntime.
//
// Lower bounds re-measured 2026-09-19 with the same two tiny documents as
// the single-window row above (SCHEMA.md "Related Docs": 20 bytes / 5 words
// / 6 window words / 110 padded tokens; one character: 1 / 1 / 2 / 103),
// still requested with long_document.mode=window as examples/dogfood does,
// at both window sizes. Verified end to end on native and Metal by the
// corpus-minimum provider tests in server/gliner_boundary_service_test.zig.
const fastino_gliner25_base_v1_long_document_lengths = LengthContract{
    .request_items = .{ .min = 1, .max = 1 },
    .document_bytes = .{ .min = 1, .max = 182000 },
    .document_words = .{ .min = 1, .max = 28275 },
    .window_count = .{ .min = 1, .max = 29 },
    .window_words = .{ .min = 2, .max = 4096 },
    .padded_sequence_tokens = .{ .min = 103, .max = 5708 },
};

// fastino/gliner2.5-base-v1, same HuggingFace revision, converted via
// `antfly-inference-gliner25-convert --precision fp16_encoder` (encoder
// matrices narrowed to F16; every bias, normalization, relative-position
// table, and the whole extraction head stay FP32 -- see
// gliner_boundary_artifact.zig's `role()`). Reviewed 2026-09-19 for the
// native and Metal backends, single-window only. Evidence:
//
//  - Identity: `antfly-inference-gliner25-convert` run twice into independent
//    output directories produced byte-identical `model.gguf` files
//    (independently reproduced with `shasum -a 256`, not just the tool's own
//    receipt); the four sidecars are copied verbatim from the pinned fp32
//    checkpoint and carry the identical digests as `fastino_gliner25_base_v1`
//    above (`gliner_boundary_bundle.validate` enforces this on every load).
//    The bundle's own receipt independently records its exact fp32
//    `model.safetensors` source digest.
//  - Correctness: `pipelines/gliner_boundary_pipeline.zig`'s "... converted
//    fp16 encoder base checkpoint all inference tasks native/metal" open the
//    converted bundle through the SAME `session_factory.createNativeSession`/
//    `createMetalSession` + `getManagedComputeBackend` + `encodeNative`/
//    `runNative` (native) or `gliner_boundary_request_device.run` (Metal)
//    path production requests use (not a raw-safetensors harness, and not
//    `gliner25-bundle-check`'s informal diagnostic), and assert all ten
//    canonical fixtures with the same `expectSample` comparator the fp32
//    pinned tests use.
//  - Reviewed tolerance decision: an independent `gliner25-bundle-check`
//    sweep of every one of the 62 comparable confidence values across all
//    ten canonical fixtures, both backends (not just the values `expectSample`
//    happens to assert), found 61/62 within the fp32 rows' 5e-4 bound; the
//    62nd (`entity_attributes`, idx 1) is 5.909e-4 (Metal) / 5.911e-4
//    (native) -- deterministically the SAME fixture, value, and magnitude to
//    six decimal places on both backends despite independent native/Metal
//    matmul kernels, which is the signature of fp16 weight-rounding noise
//    propagated through the encoder, not a backend-specific compute defect
//    (confirmed by code audit: both backends' F16 matmul upcasts each weight
//    element to f32 before multiply-accumulate --
//    zig/lib/linalg/src/mod.zig's `sgemmTransBF16Weights*` and
//    `metal_kernels.m`'s `termite_apply_linear_f16_multi_row_reduce` --  and
//    every boundary activation/attention/softmax/layernorm op runs through a
//    strictly-FP32 pipeline on both backends regardless of weight precision).
//    Next-largest deltas are 3.1e-4, 2.3e-4, 2.2e-4, comfortably inside
//    tolerance; mean 5.7e-5, median 2.2e-5 across all 62 values. Every one of
//    the 62 values' associated decision -- entity/relation set, label, and
//    span -- is byte-identical to the fp32 reference on every fixture, both
//    backends: only the confidence float itself ever differs. This is the
//    reviewed basis for `pipelines/gliner_boundary_pipeline.zig`'s
//    `fp16_encoder_confidence_tolerance = 7.5e-4` (measured max plus
//    headroom), which is the qualified bound for this row's evidence, not a
//    tolerance bump made for its own sake.
//  - Geometry: re-running `../../extractors/gliner_boundary_qualification.zig`'s
//    geometry-measuring test with `ANTFLY_GLINER25_BASE_MODEL_DIR` pointed at
//    the converted fp16 bundle directory instead of the pinned fp32 one
//    reproduces `fastino_gliner25_base_v1_lengths` byte-for-byte (the test
//    reads only the tokenizer and JSON config, both byte-identical to the
//    fp32 pins), so no new geometry measurement was needed for this row.
//
// See zig/pkg/inference/models/gliner2/GLINER25.md's fp16-encoder
// qualification sections for the full record.
const fastino_gliner25_base_v1_fp16_encoder = bundle.Identity{
    .backbone = .base,
    .precision = .fp16_encoder,
    .weight = .{ .size_bytes = 407861568, .sha256 = "1dce97cb1727e3b4e4c8242e88b46ad5f8f31801c2c9d24919393a8816a92d11".* },
    .sidecars = .{
        .{ .size_bytes = 3150, .sha256 = "0eb92d00584d613aab32b2178f84a85176b62c87ae3689ce9084e83f6eba64d1".* },
        .{ .size_bytes = 857, .sha256 = "d36a845b9f25dcaf1ec45a1c4bdf65ea4ac20596537e14530ec9f660a63aeca4".* },
        .{ .size_bytes = 8341713, .sha256 = "cbc8ae6037812709c9c26f2a160f8dc48b0440bcb79c8141804259ae2d6adac3".* },
        .{ .size_bytes = 645, .sha256 = "0bf3ea0873234bd9bfdd3853c440395009ac6365a925b91654daed5396d655e1".* },
    },
};

// Long-document windowed execution for fastino_gliner25_base_v1_fp16_encoder,
// reviewed 2026-09-19. examples/dogfood requests windowing unconditionally
// (index_config.go's knowledgeGraphIndexJSON), and essentially every real
// section its corpus produces needs it (section 3/9's LengthContract),
// so a single-window-only fp16 row cannot serve real dogfood traffic.
// Evidence, following exactly the pattern the fp32 long-document row above
// used:
//
//  - Geometry: re-running the long-document geometry-measuring test in
//    ../../extractors/gliner_boundary_qualification.zig with
//    ANTFLY_GLINER25_BASE_MODEL_DIR pointed at the converted fp16 bundle
//    directory reproduces fastino_gliner25_base_v1_long_document_lengths
//    byte-for-byte at both the 1024- and 4096-word sweeps (the test reads
//    only the tokenizer/JSON config and runs the planner, never model
//    weights, so this is a valid substitution). The row below reuses that
//    fp32 LengthContract and feature set unchanged.
//  - Correctness/shape: the same long-executor canonical-shape tests the
//    fp32 row's evidence used, parametrized over ANTFLY_GLINER25_BASE_FP16_MODEL_DIR
//    instead of ANTFLY_GLINER25_BASE_MODEL_DIR (server/gliner_boundary_service_test.zig's
//    resolveModelDirectoryFromEnv) rather than copy-pasted: the 37KB VOPR.md
//    "Completion-Claim Audit" multi-window document through both the HTTP
//    handler and the in-process provider entry (native and Metal), the
//    ~99KB PDF.md corpus-maximum section through both entries (native, the
//    same backend the fp32 row's corpus-maximum evidence used), and the
//    corpus-minimum documents (20-byte SCHEMA.md section, 1-byte document)
//    through the provider entry on both backends -- all pass with the
//    canonical schema_version 2 envelope, zero head/tail keys, and (for the
//    multi-window/corpus-maximum documents) window_count confirming the
//    merge path actually ran.
//  - fp32-vs-fp16 parity through the long executor (not required of the
//    fp32 row itself, but specific evidence for qualifying a second
//    precision against it): "gliner boundary long executor fp32 vs fp16
//    encoder parity on real long documents" in
//    server/gliner_boundary_service_test.zig runs the SAME VOPR.md,
//    PDF.md-max, and corpus-minimum documents through both bundles on both
//    backends and matches every entity/relation decision (label, text,
//    span) between precisions order-independently by identity (a near-tied
//    pair can legitimately trade places in the final array without being a
//    decision difference), then requires every matched confidence delta to
//    fall within the reviewed fp16_encoder_long_document_confidence_tolerance
//    = 2.5e-3 (pipelines/gliner_boundary_pipeline.zig) -- deliberately wider
//    than the single-window fp16_encoder_confidence_tolerance, because the
//    long executor's cross-window duplicate-mention tie-break can surface a
//    genuinely larger, but still bounded and deterministic, disagreement
//    between two different windows' independent estimates for the same
//    span. See that constant's doc comment and GLINER25.md's fp16-encoder
//    long-document qualification section for the measured delta
//    distribution and root cause.
//
// See zig/pkg/inference/models/gliner2/GLINER25.md's fp16-encoder
// qualification sections for the full record.

const production_entries: []const Entry = &.{
    .{ .identity = fastino_gliner25_base_v1, .backend = .native, .features = fastino_gliner25_base_v1_features, .lengths = fastino_gliner25_base_v1_lengths },
    .{ .identity = fastino_gliner25_base_v1, .backend = .metal, .features = fastino_gliner25_base_v1_features, .lengths = fastino_gliner25_base_v1_lengths },
    .{ .identity = fastino_gliner25_base_v1, .backend = .native, .features = fastino_gliner25_base_v1_long_document_features, .lengths = fastino_gliner25_base_v1_long_document_lengths },
    .{ .identity = fastino_gliner25_base_v1, .backend = .metal, .features = fastino_gliner25_base_v1_long_document_features, .lengths = fastino_gliner25_base_v1_long_document_lengths },
    .{ .identity = fastino_gliner25_base_v1_fp16_encoder, .backend = .native, .features = fastino_gliner25_base_v1_features, .lengths = fastino_gliner25_base_v1_lengths },
    .{ .identity = fastino_gliner25_base_v1_fp16_encoder, .backend = .metal, .features = fastino_gliner25_base_v1_features, .lengths = fastino_gliner25_base_v1_lengths },
    .{ .identity = fastino_gliner25_base_v1_fp16_encoder, .backend = .native, .features = fastino_gliner25_base_v1_long_document_features, .lengths = fastino_gliner25_base_v1_long_document_lengths },
    .{ .identity = fastino_gliner25_base_v1_fp16_encoder, .backend = .metal, .features = fastino_gliner25_base_v1_long_document_features, .lengths = fastino_gliner25_base_v1_long_document_lengths },
};

comptime {
    // Each entry's five digests are hex-validated one character at a time
    // (validDigest); the default 1000-branch comptime quota covers roughly
    // three entries' worth of that work at 64 hex characters each. Eight
    // reviewed rows (fp32 and fp16_encoder, each single-window +
    // long-document, each native + Metal) need more room than the default.
    @setEvalBranchQuota(1 << 14);
    if (!validEntries(production_entries)) @compileError("invalid GLiNER2.5 runtime qualification policy");
}

/// Coarse closed gate only. True would still require an exact live-session
/// lookup; it must never authorize a listing that has no consumed identity.
pub fn hasPublishedProfiles() bool {
    return production_entries.len != 0;
}

/// True only for the exact reviewed weight/sidecar/backbone/precision
/// identity of a production row, on ANY reviewed backend. This grants no
/// backend, feature, or geometry permission by itself; it exists only so
/// pull-time manifest synthesis can decide whether to advertise a task for
/// THIS specific downloaded artifact rather than the whole architecture
/// family. The live session still requires an exact backend, feature, and
/// prepared-geometry match through require()/Gate at request time.
pub fn hasQualifiedIdentity(consumed: bundle.Identity) bool {
    if (!validEntries(production_entries)) return false;
    for (production_entries) |entry| {
        if (equalIdentity(entry.identity, consumed)) return true;
    }
    return false;
}

/// Coarser still: true if some production row shares this backbone and
/// precision, on any backend, WITHOUT checking the weight or sidecar
/// digests at all. This exists only so a manifest-level "is it worth
/// attempting to load this specific artifact" decision can fail fast on an
/// obviously unreviewed backbone/precision (e.g. small or multi while only
/// base is reviewed) without reading the weight file. It grants no
/// identity, backend, feature, or geometry permission whatsoever;
/// hasQualifiedIdentity (pull-time) and require() (every request) are the
/// only checks that ever authorize anything.
pub fn hasQualifiedBackbonePrecision(backbone: model.Backbone, precision: artifact.Precision) bool {
    if (!validEntries(production_entries)) return false;
    for (production_entries) |entry| {
        if (entry.identity.backbone == backbone and entry.identity.precision == precision) return true;
    }
    return false;
}

/// A bounded selection of private production rows. Treat its representation as
/// opaque; callers obtain it only from start(). No row data or operation for
/// widening a selection is exposed. Reuse the SAME value for the whole request.
pub const Candidates = enum(u64) {
    rejected = 0,
    _,

    /// Intersect with one actual document/window observation. A failed check
    /// consumes all candidates, so retrying with shorter geometry cannot revive
    /// a rejected request. This mutates no global state and allocates nothing.
    pub fn narrow(self: *Candidates, observed: LengthContract) QualificationError!void {
        return narrowEntries(production_entries, self, observed);
    }
};

/// Begin using the union of features from EVERY item in the public request.
/// This is feature preflight, not an execution permit. Narrow with all actual
/// prepared geometries before learned work; retain this same candidate set
/// across items and the long-document tokenizer/planning preflight.
pub fn start(consumed: bundle.Identity, backend: Backend, required: Features) error{UnsupportedGlinerBoundaryRuntime}!Candidates {
    return startEntries(production_entries, consumed, backend, required);
}

/// Post-load early rejection before tokenization. Success alone is NOT an
/// execution permit: require() must also check actual prepared geometry before
/// learned work. Advertisement requires a verified identity and actual backend.
pub fn supportsFeatures(consumed: bundle.Identity, backend: Backend, required: Features) error{UnsupportedGlinerBoundaryRuntime}!void {
    _ = try start(consumed, backend, required);
}

/// The same row must cover exact identity, backend, the full feature union, and
/// all supplied length ranges. For long documents, use actual tokenizer and
/// planner observations, and enforce before any window executes. For a batch,
/// accumulate observed ranges rather than joining independently matching rows.
pub fn require(consumed: bundle.Identity, backend: Backend, required: Features, observed: LengthContract) QualificationError!void {
    var candidates = try start(consumed, backend, required);
    try candidates.narrow(observed);
}

fn equalDigest(expected: bundle.Digest, actual: bundle.Digest) bool {
    return expected.size_bytes == actual.size_bytes and std.mem.eql(u8, &expected.sha256, &actual.sha256);
}

fn equalIdentity(expected: bundle.Identity, actual: bundle.Identity) bool {
    if (expected.backbone != actual.backbone or expected.precision != actual.precision or !equalDigest(expected.weight, actual.weight)) return false;
    for (expected.sidecars, actual.sidecars) |left, right| if (!equalDigest(left, right)) return false;
    return true;
}

fn containsFeatures(supported: Features, required: Features) bool {
    inline for (@typeInfo(Feature).@"enum".fields) |field| {
        const feature: Feature = @enumFromInt(field.value);
        if (required.contains(feature) and !supported.contains(feature)) return false;
    }
    return true;
}

fn validDigest(digest: bundle.Digest) bool {
    if (digest.size_bytes == 0) return false;
    for (digest.sha256) |char| if (!((char >= '0' and char <= '9') or (char >= 'a' and char <= 'f'))) return false;
    return true;
}

fn validEntries(entries: []const Entry) bool {
    if (entries.len > max_entries) return false;
    for (entries) |entry| {
        if (!entry.lengths.valid() or !validDigest(entry.identity.weight)) return false;
        for (entry.identity.sidecars) |digest| if (!validDigest(digest)) return false;
    }
    return true;
}

fn startEntries(entries: []const Entry, consumed: bundle.Identity, backend: Backend, required: Features) error{UnsupportedGlinerBoundaryRuntime}!Candidates {
    if (!validEntries(entries)) return error.UnsupportedGlinerBoundaryRuntime;
    var remaining: u64 = 0;
    for (entries, 0..) |entry, index| {
        if (!equalIdentity(entry.identity, consumed) or entry.backend != backend or !containsFeatures(entry.features, required)) continue;
        remaining |= @as(u64, 1) << @intCast(index);
    }
    if (remaining == 0) return error.UnsupportedGlinerBoundaryRuntime;
    return @enumFromInt(remaining);
}

fn narrowEntries(entries: []const Entry, candidates: *Candidates, observed: LengthContract) QualificationError!void {
    const previous = @intFromEnum(candidates.*);
    candidates.* = .rejected;
    if (!validEntries(entries) or !observed.valid()) return error.UnsupportedGlinerBoundaryRuntime;
    const valid_mask = if (entries.len == max_entries) std.math.maxInt(u64) else (@as(u64, 1) << @intCast(entries.len)) - 1;
    if (previous & ~valid_mask != 0) return error.UnsupportedGlinerBoundaryRuntime;
    // Nothing matched identity/backend/features to begin with: that is an
    // unreviewed request shape, not a bounded-length rejection.
    if (previous == 0) return error.UnsupportedGlinerBoundaryRuntime;
    var remaining: u64 = 0;
    for (entries, 0..) |entry, index| {
        const bit = @as(u64, 1) << @intCast(index);
        if (previous & bit != 0 and entry.lengths.contains(observed)) remaining |= bit;
    }
    candidates.* = @enumFromInt(remaining);
    if (remaining == 0) return blamedLengthDimension(entries, previous, observed);
}

/// Only this module's tests can supply synthetic rows. Runtime callers always
/// use the immutable production table; no allocation or external state occurs.
fn matchEntries(entries: []const Entry, consumed: bundle.Identity, backend: Backend, required: Features, observed: ?LengthContract) QualificationError!void {
    var candidates = try startEntries(entries, consumed, backend, required);
    if (observed) |lengths| try narrowEntries(entries, &candidates, lengths);
}

fn testIdentity() bundle.Identity {
    return .{
        .backbone = .small,
        .precision = .fp32,
        .weight = bundle.Digest.of("synthetic tensor bytes"),
        .sidecars = .{
            bundle.Digest.of("synthetic config"),
            bundle.Digest.of("synthetic encoder config"),
            bundle.Digest.of("synthetic tokenizer"),
            bundle.Digest.of("synthetic tokenizer config"),
        },
    };
}

fn testLengths() LengthContract {
    return .{
        .request_items = .{ .min = 1, .max = 4 },
        .document_bytes = .{ .min = 0, .max = 4096 },
        .document_words = .{ .min = 0, .max = 256 },
        .window_count = .{ .min = 1, .max = 8 },
        .window_words = .{ .min = 1, .max = 128 },
        .padded_sequence_tokens = .{ .min = 16, .max = 512 },
    };
}

fn testEntry() Entry {
    return .{ .identity = testIdentity(), .backend = .native, .features = Features.initOne(.entities), .lengths = testLengths() };
}

test "boundary qualification production table denies every unreviewed variant precision and backend" {
    // The family runtime is reviewed and published (see the reviewed
    // fastino_gliner25_base_v1 row above), but this synthetic identity uses
    // hashes that never collide with a real reviewed digest, on purpose: it
    // stands in for every other checkpoint, fine-tune, or bit-flipped
    // artifact that has NOT been reviewed, across every backbone, precision,
    // and backend this policy knows about.
    try std.testing.expect(@import("gliner_boundary.zig").runtime_available);
    try std.testing.expect(hasPublishedProfiles());
    var identity = testIdentity();
    const features = Features.initFull();
    inline for (@typeInfo(@TypeOf(identity.backbone)).@"enum".fields) |variant| {
        identity.backbone = @enumFromInt(variant.value);
        inline for (@typeInfo(@TypeOf(identity.precision)).@"enum".fields) |precision| {
            identity.precision = @enumFromInt(precision.value);
            inline for (@typeInfo(Backend).@"enum".fields) |backend| {
                try std.testing.expect(!hasQualifiedIdentity(identity));
                try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, supportsFeatures(identity, @enumFromInt(backend.value), features));
                try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, require(identity, @enumFromInt(backend.value), features, testLengths()));
            }
        }
    }
}

test "boundary qualification serves the reviewed fastino gliner2.5 base checkpoint and still denies any mismatch" {
    const identity = fastino_gliner25_base_v1;
    try std.testing.expect(hasQualifiedIdentity(identity));
    try require(identity, .native, fastino_gliner25_base_v1_features, fastino_gliner25_base_v1_lengths);
    try require(identity, .metal, fastino_gliner25_base_v1_features, fastino_gliner25_base_v1_lengths);

    // A single flipped weight byte, a different backbone, a different
    // precision, an unreviewed backend, an unreviewed feature, or
    // out-of-range geometry must each still be denied. The row grants
    // exactly what was measured and nothing wider.
    var mismatched = identity;
    mismatched.weight.sha256[0] = if (mismatched.weight.sha256[0] == '7') '8' else '7';
    try std.testing.expect(!hasQualifiedIdentity(mismatched));
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, require(mismatched, .native, fastino_gliner25_base_v1_features, fastino_gliner25_base_v1_lengths));

    mismatched = identity;
    mismatched.sidecars[0].sha256[0] = if (mismatched.sidecars[0].sha256[0] == '0') '1' else '0';
    try std.testing.expect(!hasQualifiedIdentity(mismatched));

    mismatched = identity;
    mismatched.backbone = .small;
    try std.testing.expect(!hasQualifiedIdentity(mismatched));
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, require(mismatched, .native, fastino_gliner25_base_v1_features, fastino_gliner25_base_v1_lengths));

    mismatched = identity;
    mismatched.precision = .q8_0;
    try std.testing.expect(!hasQualifiedIdentity(mismatched));

    // Requiring only .long_document is satisfied by the long-document row
    // alone (its feature set is a superset), so this reaches geometry: the
    // single-window row's narrower padded_sequence_tokens minimum (14) falls
    // under the long-document row's reviewed minimum (103) -- a genuine
    // bounded-length rejection, named accordingly, not a feature mismatch.
    try std.testing.expectError(error.GlinerBoundaryPaddedSequenceLimitExceeded, require(identity, .native, Features.initOne(.long_document), fastino_gliner25_base_v1_lengths));

    // Out-of-range geometry, unlike every identity/backend mismatch above, is
    // a bounded-length rejection: identity, backend, and features all matched
    // a reviewed row, so it must name the exceeded dimension rather than the
    // generic error.
    var observed = fastino_gliner25_base_v1_lengths;
    observed.document_bytes = Range.exact(fastino_gliner25_base_v1_lengths.document_bytes.max + 1);
    try std.testing.expectError(error.GlinerBoundaryDocumentBytesLimitExceeded, require(identity, .native, fastino_gliner25_base_v1_features, observed));
    observed = fastino_gliner25_base_v1_lengths;
    observed.padded_sequence_tokens = Range.exact(fastino_gliner25_base_v1_lengths.padded_sequence_tokens.max + 1);
    try std.testing.expectError(error.GlinerBoundaryPaddedSequenceLimitExceeded, require(identity, .metal, fastino_gliner25_base_v1_features, observed));

    // The same distinction holds for the long-document row: a batch of more
    // than one item is a bounded-length (request_items) rejection now, not
    // the generic "unreviewed request shape" error -- this is the exact
    // production incident (native batching advertised for GLiNER extraction
    // groups multiple documents into one call; see server/server.zig's
    // resolvedExecutorBatchImplementation and GLINER25.md's long-document
    // section) that motivated naming this dimension.
    observed = fastino_gliner25_base_v1_long_document_lengths;
    observed.request_items = Range.exact(2);
    try std.testing.expectError(error.GlinerBoundaryRequestItemsLimitExceeded, require(identity, .native, fastino_gliner25_base_v1_long_document_features, observed));
}

test "boundary qualification matches all five consumed file sizes and hashes" {
    const row = testEntry();
    try matchEntries(&.{row}, row.identity, row.backend, row.features, row.lengths);
    for (0..5) |file| {
        var actual = row.identity;
        const digest = if (file == 0) &actual.weight else &actual.sidecars[file - 1];
        digest.size_bytes += 1;
        // Identity.fingerprint currently omits sizes; the policy must not.
        try std.testing.expectEqual(row.identity.fingerprint(), actual.fingerprint());
        try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, actual, row.backend, row.features, row.lengths));
        digest.size_bytes -= 1;
        digest.sha256[0] = if (digest.sha256[0] == 'a') 'b' else 'a';
        try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, actual, row.backend, row.features, row.lengths));
    }
    var swapped = row.identity;
    std.mem.swap(bundle.Digest, &swapped.sidecars[0], &swapped.sidecars[1]);
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, swapped, row.backend, row.features, row.lengths));
}

test "boundary qualification does not inherit variant precision or backend support" {
    const row = testEntry();
    var actual = row.identity;
    actual.backbone = .base;
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, actual, row.backend, row.features, row.lengths));
    actual = row.identity;
    actual.precision = .q8_0;
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, actual, row.backend, row.features, row.lengths));
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, row.identity, .metal, row.features, row.lengths));
}

test "boundary qualification requires one row for a mixed feature contract" {
    const entity = testEntry();
    var classification = entity;
    classification.features = Features.initOne(.classification_multi);
    var mixed = entity;
    mixed.features.insert(.classification_multi);
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{ entity, classification }, entity.identity, .native, mixed.features, entity.lengths));
    try matchEntries(&.{mixed}, entity.identity, .native, mixed.features, entity.lengths);
    try matchEntries(&.{mixed}, entity.identity, .native, entity.features, entity.lengths);
    inline for (@typeInfo(Feature).@"enum".fields) |field| {
        const feature: Feature = @enumFromInt(field.value);
        if (feature != .entities) {
            var required = entity.features;
            required.insert(feature);
            try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{entity}, entity.identity, .native, required, entity.lengths));
        }
    }
}

test "boundary qualification keeps source and native JointIE contracts in one row" {
    var source = testEntry();
    source.features = Features.initOne(.joint_ie);
    source.features.insert(.joint_fastino_v1);
    var native = source;
    native.features.remove(.joint_fastino_v1);
    native.features.insert(.decoder_auto);
    try matchEntries(&.{source}, source.identity, source.backend, source.features, source.lengths);
    try matchEntries(&.{native}, native.identity, native.backend, native.features, native.lengths);
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{source}, source.identity, source.backend, native.features, source.lengths));
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{native}, native.identity, native.backend, source.features, native.lengths));
    var mixed = source;
    mixed.features.insert(.decoder_auto);
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{ source, native }, mixed.identity, mixed.backend, mixed.features, mixed.lengths));
    try matchEntries(&.{mixed}, mixed.identity, mixed.backend, mixed.features, mixed.lengths);
}

test "boundary qualification checks every inclusive geometry endpoint and unit" {
    const row = testEntry();
    inline for (@typeInfo(LengthContract).@"struct".fields) |field| {
        var observed = row.lengths;
        const range = @field(row.lengths, field.name);
        @field(observed, field.name) = Range.exact(range.min);
        try matchEntries(&.{row}, row.identity, .native, row.features, observed);
        @field(observed, field.name) = Range.exact(range.max);
        try matchEntries(&.{row}, row.identity, .native, row.features, observed);
        // A single field outside its row's range, with the rest of the
        // contract otherwise valid, is a bounded-length rejection: it must
        // name exactly this field, not the generic error.
        @field(observed, field.name) = Range.exact(range.max + 1);
        try std.testing.expectError(fieldLimitError(field.name), matchEntries(&.{row}, row.identity, .native, row.features, observed));
        if (range.min != 0) {
            @field(observed, field.name) = Range.exact(range.min - 1);
            try std.testing.expectError(fieldLimitError(field.name), matchEntries(&.{row}, row.identity, .native, row.features, observed));
        }
        // An internally-invalid range (min > max) is a malformed observation,
        // not a bounded-length rejection: it fails before any row is checked.
        @field(observed, field.name) = .{ .min = 2, .max = 1 };
        try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{row}, row.identity, .native, row.features, observed));
    }
    // Few words do not excuse an encoded sequence made large by schema tokens.
    var schema_heavy = row.lengths;
    schema_heavy.document_words = Range.exact(1);
    schema_heavy.padded_sequence_tokens = Range.exact(513);
    try std.testing.expectError(error.GlinerBoundaryPaddedSequenceLimitExceeded, matchEntries(&.{row}, row.identity, .native, row.features, schema_heavy));
}

test "boundary qualification does not merge geometry or feature grants across rows" {
    var short = testEntry();
    var long = short;
    short.lengths.padded_sequence_tokens = .{ .min = 16, .max = 255 };
    long.lengths.padded_sequence_tokens = .{ .min = 256, .max = 512 };
    try std.testing.expectError(error.GlinerBoundaryPaddedSequenceLimitExceeded, matchEntries(&.{ short, long }, short.identity, .native, short.features, testLengths()));
    // Nor may a feature match from one row borrow another row's larger lengths.
    long.features.insert(.joint_ie);
    var observed = short.lengths;
    try std.testing.expectError(error.GlinerBoundaryPaddedSequenceLimitExceeded, matchEntries(&.{ short, long }, short.identity, .native, long.features, observed));
    observed.padded_sequence_tokens = Range.exact(512);
    try matchEntries(&.{ short, long }, short.identity, .native, long.features, observed);
}

test "boundary qualification intersects every window and cannot revive a failed request" {
    var short = testEntry();
    var long = short;
    short.lengths.padded_sequence_tokens = .{ .min = 16, .max = 255 };
    long.lengths.padded_sequence_tokens = .{ .min = 256, .max = 512 };
    const rows = [_]Entry{ short, long };
    var candidates = try startEntries(&rows, short.identity, .native, short.features);
    var observed = short.lengths;
    observed.padded_sequence_tokens = Range.exact(128);
    try narrowEntries(&rows, &candidates, observed);
    observed.padded_sequence_tokens = Range.exact(384);
    try std.testing.expectError(error.GlinerBoundaryPaddedSequenceLimitExceeded, narrowEntries(&rows, &candidates, observed));
    observed.padded_sequence_tokens = Range.exact(128);
    // Candidates were already fully consumed by the failed narrow above: this
    // retry has no previously-matched row left to blame a dimension against,
    // so it is the generic error, not a revived bounded-length one.
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, narrowEntries(&rows, &candidates, observed));
    // A real covering row survives both observations in either order.
    const covering = testEntry();
    const covered_rows = [_]Entry{ short, long, covering };
    candidates = try startEntries(&covered_rows, short.identity, .native, short.features);
    try narrowEntries(&covered_rows, &candidates, observed);
    observed.padded_sequence_tokens = Range.exact(384);
    try narrowEntries(&covered_rows, &candidates, observed);
    // Total request count cannot be replaced by a smaller current batch size,
    // and the rejection names request_items -- the exact production incident
    // this dimension exists to diagnose.
    observed.request_items = Range.exact(5);
    try std.testing.expectError(error.GlinerBoundaryRequestItemsLimitExceeded, narrowEntries(&covered_rows, &candidates, observed));
}

test "boundary qualification feature preflight never grants unchecked geometry" {
    const row = testEntry();
    try matchEntries(&.{row}, row.identity, .native, row.features, null);
    var observed = row.lengths;
    observed.window_count = Range.exact(9);
    try std.testing.expectError(error.GlinerBoundaryWindowCountLimitExceeded, matchEntries(&.{row}, row.identity, .native, row.features, observed));
    // supportsFeatures/start() always check the real production table, never
    // the test-local `row`, so this is an unreviewed identity, not a
    // bounded-length rejection.
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, supportsFeatures(row.identity, .native, row.features));
}

test "boundary qualification rejects malformed or oversized tables before matching" {
    const row = testEntry();
    var malformed = row;
    malformed.identity.sidecars[3].sha256[63] = 'G';
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{ row, malformed }, row.identity, .native, row.features, row.lengths));
    malformed = row;
    malformed.identity.weight.size_bytes = 0;
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{malformed}, row.identity, .native, row.features, row.lengths));
    malformed = row;
    malformed.lengths.document_bytes = .{ .min = 3, .max = 2 };
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&.{malformed}, row.identity, .native, row.features, row.lengths));
    const rows: [max_entries + 1]Entry = @splat(row);
    try matchEntries(rows[0..max_entries], row.identity, .native, row.features, row.lengths);
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, matchEntries(&rows, row.identity, .native, row.features, row.lengths));
    var invalid_selection: Candidates = @enumFromInt(@as(u64, 1) << 63);
    try std.testing.expectError(error.UnsupportedGlinerBoundaryRuntime, narrowEntries(&.{row}, &invalid_selection, row.lengths));
    // Even test-supplied synthetic selections cannot access a production row:
    // the public narrow() always checks against the real production table, so
    // this bit index instead lands on the real fastino single-window row,
    // whose tight request_items=[1,1] rejects testLengths()'s {1,4} -- a
    // bounded-length rejection against the real row, still not a route to
    // pretending the synthetic `row` was ever reviewed.
    var synthetic = try startEntries(&.{row}, row.identity, .native, row.features);
    try std.testing.expectError(error.GlinerBoundaryRequestItemsLimitExceeded, synthetic.narrow(row.lengths));
}

test "boundary qualification range matching has no saturating or overflow acceptance" {
    var row = testEntry();
    row.lengths.document_bytes = .{ .min = 0, .max = std.math.maxInt(u64) };
    var observed = row.lengths;
    observed.document_bytes = Range.exact(std.math.maxInt(u64));
    try matchEntries(&.{row}, row.identity, .native, row.features, observed);
    row.lengths.document_bytes.max -= 1;
    try std.testing.expectError(error.GlinerBoundaryDocumentBytesLimitExceeded, matchEntries(&.{row}, row.identity, .native, row.features, observed));
}
