import type { ChatToolsConfig, ExaSearchConfig, WebSearchProviderConfig } from "../src/index.js";

const exa = {
  provider: "exa",
  num_results: 3,
  search_type: "auto",
  include_domains: ["antfly.io"],
  start_published_date: "2026-01-01T00:00:00Z",
} satisfies ExaSearchConfig;

const provider: WebSearchProviderConfig = exa;
export const webTools = {
  enabled_tools: ["web_search"],
  web_search_config: provider,
} satisfies ChatToolsConfig;
