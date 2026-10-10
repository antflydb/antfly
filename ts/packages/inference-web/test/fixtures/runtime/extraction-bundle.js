import { RUNTIME_COMPATIBILITY } from "../../../dist/runtime-compatibility.js";
export const INFERENCE_RUNTIME_ID = RUNTIME_COMPATIBILITY.runtimeId;
export async function inspectBundle(files, precision) {
  await globalThis.inferenceFixture.inspect?.(files);
  return { architecture: "span", precision: precision ?? "fp32", bytes: 16 };
}
