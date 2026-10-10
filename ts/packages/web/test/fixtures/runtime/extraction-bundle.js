import { RUNTIME_COMPATIBILITY } from "../../../dist/runtime-compatibility.js";
export const INFERENCE_RUNTIME_ID = RUNTIME_COMPATIBILITY.runtimeId;
export async function inspectBundle(files, precision, signal) {
  await globalThis.inferenceFixture.inspect?.(files, precision, signal);
  return {
    family: "gliner2",
    runtimeArchitecture: "span",
    architecture: "span",
    precision: precision ?? "fp32",
    bytes: 16,
  };
}
