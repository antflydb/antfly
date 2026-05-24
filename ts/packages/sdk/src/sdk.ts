import { AntflyClient } from "./client.js";
import { TermiteClient } from "./termite-client.js";
import type { AntflyConfig } from "./types.js";

export interface SDKConfig extends AntflyConfig {
  termiteBaseUrl?: string;
}

export class Client {
  private readonly antflyClient: AntflyClient;
  private readonly termiteClient: TermiteClient;

  constructor(config: SDKConfig) {
    this.antflyClient = new AntflyClient(config);
    this.termiteClient = new TermiteClient({
      baseUrl: config.termiteBaseUrl ?? config.baseUrl,
      headers: config.headers,
    });
  }

  Antfly(): AntflyClient {
    return this.antflyClient;
  }

  Termite(): TermiteClient {
    return this.termiteClient;
  }

  get antfly(): AntflyClient {
    return this.antflyClient;
  }

  get termite(): TermiteClient {
    return this.termiteClient;
  }
}
