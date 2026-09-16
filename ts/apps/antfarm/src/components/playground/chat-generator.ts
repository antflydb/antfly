import type { GeneratorConfig } from "@antfly/sdk";

export function getChatRequestGenerator(generator: GeneratorConfig | null): GeneratorConfig | null {
  if (generator?.provider !== "openrouter") return generator;
  return {
    ...generator,
    url: generator.url ?? "https://openrouter.ai/api/v1",
    // biome-ignore lint/suspicious/noTemplateCurlyInString: Server-side secret reference.
    api_key: generator.api_key ?? "${secret:openrouter.api_key}",
  };
}
