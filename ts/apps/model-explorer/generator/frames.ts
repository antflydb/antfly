import { FrameScenario, type KernelInventoryEntry } from "../lib/schema/index.ts";
import { verifySourceLink } from "./lib.ts";
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
  if (frame.source) {
    const result = verifySourceLink(frame.source);
    if (result?.healed) {
      if (frame.source.line !== undefined) {
        console.warn(
          `  warn: healed anchor for frame ${frame.id}: ${frame.source.path}:${frame.source.line} -> :${result.line}`
        );
      }
      if (frame.source.endLine !== undefined && frame.source.line !== undefined)
        frame.source.endLine += result.line - frame.source.line;
      frame.source.line = result.line;
    }
  }
  return frame;
}
