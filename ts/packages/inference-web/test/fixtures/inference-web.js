import { RUNTIME_COMPATIBILITY } from "../../dist/runtime-compatibility.js";
export const INFERENCE_RUNTIME_ID = RUNTIME_COMPATIBILITY.runtimeId;
export class InferenceWeb {
  constructor() {
    globalThis.inferenceFixture.runtimes.push(this);
  }
  async init(...args) {
    await globalThis.inferenceFixture.initRuntime?.(...args);
  }
  async loadExtractionBundle(files, precision, progress) {
    const fixture = globalThis.inferenceFixture;
    fixture.loads.push(files);
    await fixture.load?.(files, precision, progress);
    return { architecture: "span", precision: "fp32", bytes: 16 };
  }
  async runExtraction(request, validate) {
    const value =
      (await globalThis.inferenceFixture.run?.(request, validate)) ??
      (validate ? { valid: true, encoded_tokens: 4 } : { schema_version: 1, entities: [] });
    return { value, elapsedMs: 1, wasmBytes: 1024 };
  }
  destroy(error) {
    this.destroyed = (this.destroyed ?? 0) + 1;
    globalThis.inferenceFixture.destroyRuntime?.(error);
  }
}
