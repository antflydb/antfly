import { z } from "zod";

/**
 * A pointer into the Antfly repo. `path` is repo-root-relative
 * (e.g. "zig/pkg/inference/src/graph/quant_kernel_compiler.zig").
 * `anchor` is an expected substring at `line`, used by the generator for
 * drift detection + self-healing line numbers.
 */
export const SourceLink = z
  .object({
    path: z
      .string()
      .min(1)
      .refine(
        (path) =>
          !path.startsWith("/") &&
          !path.includes("\\") &&
          !path.split("/").some((part) => part === ".." || part === "." || part === ""),
        "expected a repo-relative path"
      ),
    line: z.number().int().positive().optional(),
    endLine: z.number().int().positive().optional(),
    anchor: z.string().min(1).optional(),
    symbol: z.string().optional(),
  })
  .refine(
    (link) => link.endLine === undefined || (link.line !== undefined && link.endLine >= link.line),
    "endLine requires line and must not precede it"
  );
export type SourceLink = z.infer<typeof SourceLink>;
