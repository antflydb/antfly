import type { AntflyType } from "@antfly/sdk";

export interface FieldDetectionInfo {
  frequency: number;
  sampleCount: number;
  exampleValue: unknown;
}

export interface DetectedField {
  name: string;
  inferredType: "string" | "number" | "boolean" | "object" | "array";
  exampleValue: unknown;
  frequency: number;
  sampleCount: number;
  suggestedAntflyTypes: AntflyType[];
}

export const ANTFLY_TYPES: { value: AntflyType; label: string }[] = [
  { value: "text", label: "Text" },
  { value: "keyword", label: "Keyword" },
  { value: "html", label: "HTML" },
  { value: "numeric", label: "Numeric" },
  { value: "boolean", label: "Boolean" },
  { value: "datetime", label: "Datetime" },
  { value: "geopoint", label: "Geo Point" },
  { value: "geoshape", label: "Geo Shape" },
  { value: "embedding", label: "Embedding" },
  { value: "link", label: "Link" },
  { value: "blob", label: "Blob" },
  { value: "search_as_you_type", label: "Search as You Type" },
];

export const ANTFLY_TYPE_VALUES: AntflyType[] = ANTFLY_TYPES.map((t) => t.value);

export const RESERVED_FIELD_NAMES = ["_type", "_id", "_embeddings", "_summaries"];

export function inferJSONType(
  value: unknown
): "string" | "number" | "boolean" | "object" | "array" {
  if (value === null || value === undefined) return "string";
  if (Array.isArray(value)) return "array";
  if (typeof value === "object") return "object";
  if (typeof value === "number") return "number";
  if (typeof value === "boolean") return "boolean";
  return "string";
}

export function getDefaultAntflyType(jsonType: string, value: unknown): AntflyType[] {
  if (jsonType === "number") return ["numeric"];
  if (jsonType === "boolean") return [];
  if (jsonType === "array") return ["keyword"];
  if (jsonType === "object") return [];

  if (typeof value === "string") {
    if (/^\d{4}-\d{2}-\d{2}(T|\s)?\d{2}:\d{2}/.test(value)) {
      return ["datetime"];
    }
    if (/<[a-z][\s\S]*>/i.test(value)) {
      return ["html"];
    }
  }

  return ["text"];
}

export function truncateValue(value: unknown, maxLen = 80): string {
  if (value === null) return "null";
  if (value === undefined) return "undefined";

  let str: string;
  if (typeof value === "object") {
    str = JSON.stringify(value);
  } else {
    str = String(value);
  }

  if (str.length > maxLen) {
    return `${str.substring(0, maxLen)}...`;
  }
  return str;
}
