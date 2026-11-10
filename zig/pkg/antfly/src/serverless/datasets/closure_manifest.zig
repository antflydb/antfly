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

//! A closed partition has one ordered export frontier. This record lives in
//! the independent conditional catalog, never in the source owner's directory.
const std = @import("std");
const local = @import("antfly_local_sources");
const t = local.retained_dataset_types;
const series = local.storage_retained_series;

pub const Cut = struct {
    closure: t.Id,
    sequence: u64,
    complete_from_ns: i128,
    admitted_ns: i128,
};

pub fn segmentId(closure: t.Id, span: t.KeySpan) t.Id {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly/retained-closure-page/v1");
    hash.update(&closure);
    hash.update(&span.start);
    hash.update(&span.end);
    return hash.finalResult();
}

pub const Manifest = struct {
    format: u8 = 1,
    dataset: t.Id,
    generation: u64,
    definition: t.Id,
    source: t.SourcePosition,
    cut: Cut,
    first: t.RowKey,
    next: t.RowKey,
    /// Persist the exclusive expired bucket boundary independently of the
    /// content receipt so clock rollback cannot make expired history visible.
    expired_before_ns: ?i128 = null,
    pages: u64 = 0,
    last: ?t.Id = null,
    chain: t.Id,
    /// Once sealed, `next` starts a complete empty tail. No sentinel file
    /// with an unbounded expiry is needed, even for a completely empty owner.
    sealed: bool = false,

    pub fn init(spec: series.Spec, cut: Cut) !Manifest {
        try spec.validate();
        if (t.zero(cut.closure) or try spec.bucket(cut.complete_from_ns) != cut.complete_from_ns) return error.InvalidDatasetCoverage;
        const first_ns = if (try spec.expired(cut.complete_from_ns, cut.admitted_ns))
            try spec.bucket(std.math.sub(i128, cut.admitted_ns, spec.retention_ns) catch return error.RetainedSeriesTimeOverflow)
        else
            cut.complete_from_ns;
        const first = t.rowKey(first_ns, @splat(0));
        var seed = std.crypto.hash.sha2.Sha256.init(.{});
        seed.update("antfly/retained-closure-manifest/v1");
        seed.update(&cut.closure);
        seed.update(&first);
        return .{ .dataset = spec.id, .generation = spec.generation, .definition = spec.definition, .source = .{ .partition = spec.source_partition, .incarnation = spec.source_incarnation, .sequence = cut.sequence }, .cut = cut, .first = first, .next = first, .chain = seed.finalResult() };
    }

    pub fn requireScope(self: Manifest, initial: Manifest) !void {
        if (self.format != 1 or !std.meta.eql(self.cut, initial.cut) or !std.meta.eql(self.source, initial.source) or
            self.generation != initial.generation or !std.mem.eql(u8, &self.dataset, &initial.dataset) or
            !std.mem.eql(u8, &self.definition, &initial.definition) or !std.mem.eql(u8, &self.first, &initial.first) or
            std.mem.order(u8, &self.next, &self.first) == .lt or (self.pages == 0) != (self.last == null) or
            (self.pages == 0 and (!std.mem.eql(u8, &self.first, &self.next) or !std.mem.eql(u8, &self.chain, &initial.chain))) or
            (self.sealed and !std.mem.allEqual(u8, self.next[32..], '0'))) return error.InvalidDatasetCoverage;
        if (self.pages > 0) try (t.KeySpan{ .start = self.first, .end = self.next }).validate();
        if (self.expired_before_ns) |floor| {
            if (!self.sealed or self.pages == 0 or floor <= try t.rowKeyTimestamp(self.first) or
                floor > try t.rowKeyTimestamp(self.next)) return error.InvalidDatasetCoverage;
        }
    }

    /// The catalog caller must check the actual immutable object and exact
    /// catalog segment at the same conditional root before advancing.
    pub fn append(self: Manifest, value: t.Segment) !Manifest {
        const span = try value.span();
        if (self.sealed or !value.finalized or value.retiring or value.access != .queryable or
            !std.meta.eql(self.source, value.source) or value.definition_generation != self.generation or
            !std.mem.eql(u8, &self.dataset, &value.dataset) or !std.mem.eql(u8, &self.next, &span.start) or
            !std.mem.eql(u8, &value.id, &segmentId(self.cut.closure, span))) return error.InvalidDatasetCoverage;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(&self.chain);
        hash.update(&value.id);
        hash.update(&value.checksum);
        var result = self;
        result.next = span.end;
        result.last = value.id;
        result.pages = std.math.add(u64, self.pages, 1) catch return error.DatasetPageBudgetExceeded;
        result.chain = hash.finalResult();
        return result;
    }

    /// Only the owner scan can certify that no rows follow this key. A gap or
    /// a page ending partway through a bucket is not a terminal scan result.
    pub fn seal(self: Manifest, empty_tail: t.RowKey) !Manifest {
        if (!std.mem.eql(u8, &self.next, &empty_tail) or !std.mem.allEqual(u8, empty_tail[32..], '0')) return error.InvalidDatasetCoverage;
        var result = self;
        result.sealed = true;
        return result;
    }

    pub fn receipt(self: Manifest) !t.Id {
        if (!self.sealed) return error.DatasetDependencyPending;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly/retained-closed-partition/v1");
        hash.update(&self.cut.closure);
        hash.update(&self.chain);
        hash.update(&self.next);
        return hash.finalResult();
    }
};
