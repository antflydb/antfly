import type { AntflyType } from "@antfly/sdk";
import { AntflyClient } from "@antfly/sdk";

const baseUrl = "/api/v1";

/**
 * @deprecated Use the `useApi()` hook from `@/hooks/use-api-config` instead
 * for components that need dynamic API URL configuration.
 *
 * This static client is kept for backward compatibility but won't respect
 * runtime API URL changes made through the Settings dialog.
 *
 * Example migration:
 * ```tsx
 * // Old:
 * import { api } from "../api";
 * const result = await api.getTables();
 *
 * // New:
 * import { useApi } from "@/hooks/use-api-config";
 * function MyComponent() {
 *   const api = useApi();
 *   const result = await api.getTables();
 * }
 * ```
 */
export const api = new AntflyClient({
  baseUrl,
});

// --- Type Definitions based on PLAN.md ---

export type { AntflyType };

export interface JSONSchemaProperty {
  type: "string" | "number" | "boolean" | "object" | "array" | "integer";
  description?: string;
  "x-antfly-index"?: boolean;
  "x-antfly-types"?: AntflyType[];
  // Support for nested objects and arrays
  properties?: { [key: string]: JSONSchemaProperty };
  items?: JSONSchemaProperty;
  [key: string]: unknown;
}

export interface JSONSchema {
  type: "object";
  properties: {
    [key: string]: JSONSchemaProperty;
  };
  required?: string[];
  "x-antfly-include-in-all"?: string[];
  [key: string]: unknown;
}

export interface DocumentSchema {
  key?: string;
  schema: JSONSchema;
}

export interface TableSchema {
  version?: number;
  enforce_types?: boolean;
  default_type?: string;
  document_schemas?: {
    [schemaName: string]: DocumentSchema;
  };
}

export interface ChunkerConfig {
  provider: "termite" | "mock";
  strategy: "hugot" | "fixed";
  api_url?: string;
  target_tokens?: number;
  overlap_tokens?: number;
  separator?: string;
  max_chunks?: number;
  threshold?: number; // For hugot strategy
}
