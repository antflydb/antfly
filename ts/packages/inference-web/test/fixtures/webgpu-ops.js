import { RUNTIME_COMPATIBILITY } from "../../dist/runtime-compatibility.js";
export const INFERENCE_RUNTIME_ID = RUNTIME_COMPATIBILITY.runtimeId;
export class WebGPUOps {
  constructor() {
    globalThis.inferenceFixture.gpus.push(this);
  }
  async init() {
    return globalThis.inferenceFixture.initGpu(this);
  }
  destroy() {
    this.destroyed = (this.destroyed ?? 0) + 1;
    this.device?.destroy();
  }
}
