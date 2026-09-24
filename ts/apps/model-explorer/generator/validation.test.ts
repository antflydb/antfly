import assert from "node:assert/strict";
import test from "node:test";
import { ModelSpec, SourceLink } from "../lib/schema/index.ts";
import { extractOpKinds, parseSchedules } from "./extract.ts";
import { validateFrame } from "./frames.ts";
import { verifySourcePath } from "./lib.ts";
import { buildMergeContext, validateSpec } from "./merge.ts";

const context = () => buildMergeContext([{ name: "linear" }], [], [], []);
function model() {
  return ModelSpec.parse({
    schemaVersion: 1,
    id: "gliner2",
    family: "test",
    displayName: "Test",
    tagline: "Fixture",
    stages: [{ id: "encoder", title: "Encoder", kind: "encoder" }],
    graphs: {
      decode: {
        nodes: [
          { id: "a", stageId: "encoder", opKind: "linear" },
          { id: "b", stageId: "encoder", opKind: "linear" },
        ],
        edges: [{ id: "a-b", from: "a", to: "b" }],
      },
    },
    sankey: {
      nodes: [
        { id: "a", label: "A" },
        { id: "b", label: "B" },
      ],
      links: [{ source: "a", target: "b", value: 1 }],
    },
  });
}

test("valid model graphs pass; duplicate nodes and dangling edges fail", () => {
  validateSpec(model(), context(), "fixture");
  const duplicate = model();
  duplicate.graphs.decode.nodes[1].id = "a";
  assert.throws(() => validateSpec(duplicate, context(), "fixture"), /duplicate ID/);
  const dangling = model();
  dangling.graphs.decode.edges[0].to = "missing";
  assert.throws(() => validateSpec(dangling, context(), "fixture"), /edge endpoint missing/);
});

test("graphs reject stale op, kernel, route and flag references", () => {
  for (const field of ["kernels", "kernelRouteIds", "envFlagNames"] as const) {
    const spec = model();
    spec.graphs.decode.nodes[0][field] = ["missing"];
    assert.throws(() => validateSpec(spec, context(), "fixture"), /unknown|not found/);
  }
  const spec = model();
  spec.graphs.decode.nodes[0].opKind = "missing";
  assert.throws(() => validateSpec(spec, context(), "fixture"), /unknown opKind/);
});

test("Sankey rejects missing endpoints and cycles before rendering", () => {
  const missing = model();
  assert.ok(missing.sankey);
  missing.sankey.links[0].target = "missing";
  assert.throws(() => validateSpec(missing, context(), "fixture"), /edge endpoint missing/);
  const cycle = model();
  assert.ok(cycle.sankey);
  cycle.sankey.links.push({ source: "b", target: "a", value: 1 });
  assert.throws(() => validateSpec(cycle, context(), "fixture"), /circular link/);
});

test("frame validation rejects stale kernels and invalid barrier positions", () => {
  const base = {
    schemaVersion: 1,
    id: "test",
    modelId: "gemma4-e4b",
    title: "Test",
    encoderScopes: [{ id: "a", ops: [{ label: "norm", family: "norm_rope" }] }],
  };
  assert.equal(validateFrame(base, []).mode, "planned");
  assert.throws(
    () =>
      validateFrame(
        {
          ...base,
          encoderScopes: [{ id: "a", ops: [{ label: "bad", family: "other", kernel: "missing" }] }],
        },
        []
      ),
    /unknown kernel/
  );
  assert.throws(
    () => validateFrame({ ...base, barriers: [{ afterScope: "missing", hazard: "raw" }] }, []),
    /unknown barrier scope/
  );
  assert.throws(
    () =>
      validateFrame(
        { ...base, barriers: [{ afterScope: "a", afterOpIndex: 1, hazard: "raw" }] },
        []
      ),
    /outside scope/
  );
  assert.throws(() => validateFrame({ ...base, mode: "captured" }, []), /machine and source/);
});

test("source links reject traversal, line pins and anchors", () => {
  for (const path of ["../private", "/etc/passwd", "zig/../private", "zig\\private"]) {
    assert.equal(SourceLink.safeParse({ path }).success, false);
  }
  // The explorer links by path only; stale line/anchor metadata must not
  // creep back into curated data where it would silently rot.
  assert.equal(SourceLink.safeParse({ path: "zig/file.zig", line: 10 }).success, false);
  assert.equal(SourceLink.safeParse({ path: "zig/file.zig", anchor: "fn main(" }).success, false);
  assert.equal(SourceLink.safeParse({ path: "zig/lib/ml/src/graph/node.zig" }).success, true);
  assert.throws(() => verifySourcePath({ path: "zig/lib/ml/src/graph/missing.zig" }), /not found/);
  assert.doesNotThrow(() => verifySourcePath({ path: "zig/lib/ml/src/graph/node.zig" }));
});

test("extracted op kinds link to the enum declaration file", () => {
  const ops = extractOpKinds();
  assert.ok(ops.length > 50);
  for (const op of ops) {
    assert.equal(op.source.path, "zig/lib/ml/src/graph/node.zig");
    assert.doesNotThrow(() => verifySourcePath(op.source));
  }
});

function table(row: string) {
  return `pub const metal_production_schedules = [_]MetalRouteSchedule{\n${row}\n};`;
}
const row =
  ".{ .format = .q4_0, .row_bucket = .rows_2_8, .epilogue = .none, .schedule = .{ .threads_per_threadgroup = 32, .skip_rescale = false, .new_field = 7 } }";
test("schedule extraction tolerates whitespace and preserves additional fields and false", () => {
  const routes = parseSchedules(table(row.replaceAll(", ", ",\n  ")));
  assert.equal(routes.length, 1);
  assert.equal(routes[0].schedule.skipRescale, false);
  assert.deepEqual(routes[0].schedule.extra, { new_field: 7 });
});
test("schedule extraction fails instead of silently dropping unsupported rows", () => {
  assert.throws(
    () => parseSchedules(table(`${row},\n.{ .format = .q6_k, .unexpected = true }`)),
    /incomplete/
  );
  assert.throws(
    () => parseSchedules(table(row.replace("32", "config.threads"))),
    /unsupported schedule/
  );
  assert.throws(() => parseSchedules(table(`${row},\n${row}`)), /duplicate ID/);
});
