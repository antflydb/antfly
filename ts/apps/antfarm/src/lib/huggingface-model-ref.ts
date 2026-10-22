import { baseModelName } from "@antfly/sdk";

/** Resolve Antfly's owner/repo[:format[:variant]] model reference to a Hugging Face repo ID. */
export function huggingFaceRepoId(model: string): string | null {
  if (!model.includes("/") || model.startsWith("/") || model.includes("://")) return null;
  try {
    return baseModelName(model);
  } catch {
    return null;
  }
}
