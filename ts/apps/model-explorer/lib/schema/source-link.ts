import { z } from "zod";

/**
 * A pointer into the Antfly repo. `path` is repo-root-relative
 * (e.g. "zig/pkg/inference/src/graph/quant_kernel_compiler.zig"). Links open
 * the file on the default branch; they carry no line numbers or anchors, so
 * routine edits to the referenced source never invalidate the snapshot.
 */
export const SourceLink = z.strictObject({
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
});
export type SourceLink = z.infer<typeof SourceLink>;
