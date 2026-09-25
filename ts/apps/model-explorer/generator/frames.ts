import { FrameScenario, type KernelInventoryEntry } from "../lib/schema/index.ts";
import { verifySourcePath } from "./lib.ts";
import { requireUnique } from "./merge.ts";

/** Validate representative frames as eagerly as the model graphs. */
export function validateFrame(raw: unknown, kernels: KernelInventoryEntry[]): FrameScenario {
  const frame = FrameScenario.parse(raw);
  const kernelNames = new Set(kernels.map((kernel) => kernel.name));
  requireUnique(
    frame.encoderScopes.map((scope) => scope.id),
    `${frame.id}/scopes`
  );
  const scopes = new Map(frame.encoderScopes.map((scope) => [scope.id, scope]));
  for (const scope of frame.encoderScopes) {
    for (const op of scope.ops) {
      if (op.kernel && !kernelNames.has(op.kernel)) {
        throw new Error(`${frame.id}/${scope.id}: unknown kernel "${op.kernel}"`);
      }
    }
  }
  for (const barrier of frame.barriers) {
    const scope = scopes.get(barrier.afterScope);
    if (!scope) throw new Error(`${frame.id}: unknown barrier scope "${barrier.afterScope}"`);
    if (barrier.afterOpIndex !== undefined && barrier.afterOpIndex >= scope.ops.length) {
      throw new Error(`${frame.id}/${scope.id}: barrier op index outside scope`);
    }
  }
  if (frame.source) verifySourcePath(frame.source);
  return frame;
}
