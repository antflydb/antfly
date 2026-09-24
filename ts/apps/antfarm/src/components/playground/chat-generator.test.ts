import type { GeneratorConfig } from "@antfly/sdk";
import { describe, expect, it } from "vitest";
import { getChatRequestGenerator } from "./chat-generator";

describe("chat request generator", () => {
  it("preserves configured OpenRouter credentials, endpoint, and generation settings", () => {
    const configured: GeneratorConfig = {
      provider: "openrouter",
      model: "openai/gpt-4.1",
      url: "https://gateway.example/v1",
      // biome-ignore lint/suspicious/noTemplateCurlyInString: Server-side secret reference.
      api_key: "${secret:team.openrouter}",
      temperature: 0.4,
      max_tokens: 256,
    };
    expect(getChatRequestGenerator(configured)).toEqual(configured);
  });

  it("defaults only missing OpenRouter settings without changing the saved preference", () => {
    const configured: GeneratorConfig = {
      provider: "openrouter",
      model: "openai/gpt-4.1",
      // biome-ignore lint/suspicious/noTemplateCurlyInString: Server-side secret reference.
      api_key: "${secret:team.openrouter}",
    };
    expect(getChatRequestGenerator(configured)).toEqual({
      ...configured,
      url: "https://openrouter.ai/api/v1",
    });
    expect(configured).not.toHaveProperty("url");
    expect(
      getChatRequestGenerator({
        provider: "openrouter",
        model: "openai/gpt-4.1",
        url: "https://gateway.example/v1",
      })
    ).toEqual({
      provider: "openrouter",
      model: "openai/gpt-4.1",
      url: "https://gateway.example/v1",
      // biome-ignore lint/suspicious/noTemplateCurlyInString: Server-side secret reference.
      api_key: "${secret:openrouter.api_key}",
    });
  });

  it("preserves other providers and an unset generator", () => {
    const configured: GeneratorConfig = { provider: "openai", model: "gpt-4.1" };
    expect(getChatRequestGenerator(configured)).toBe(configured);
    expect(getChatRequestGenerator(null)).toBeNull();
  });
});
