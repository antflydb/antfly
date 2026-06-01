import { AntflyClient } from "./client.js";
import { InferenceClient } from "./inference-client.js";
import type { AntflyConfig } from "./types.js";

export interface SDKConfig extends AntflyConfig {
  inferenceBaseUrl?: string;
}

export class Client {
  private readonly antflyClient: AntflyClient;
  private readonly inferenceClient: InferenceClient;

  constructor(config: SDKConfig) {
    this.antflyClient = new AntflyClient(config);
    this.inferenceClient = new InferenceClient({
      baseUrl: config.inferenceBaseUrl ?? config.baseUrl,
      headers: config.headers,
    });
  }

  Antfly(): AntflyClient {
    return this.antflyClient;
  }

  Inference(): InferenceClient {
    return this.inferenceClient;
  }

  get antfly(): AntflyClient {
    return this.antflyClient;
  }

  get inference(): InferenceClient {
    return this.inferenceClient;
  }
}
