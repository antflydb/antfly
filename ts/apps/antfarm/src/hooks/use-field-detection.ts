import type { QueryHit } from "@antfly/sdk";
import { useCallback, useMemo, useState } from "react";
import {
  type DetectedField,
  getDefaultAntflyType,
  inferJSONType,
  RESERVED_FIELD_NAMES,
} from "@/components/schema-builder/schema-utils";
import { useApi } from "@/hooks/use-api-config";

export interface DetectionGroup {
  typeName: string;
  fields: DetectedField[];
  docCount: number;
}

interface UseFieldDetectionResult {
  detect: () => Promise<DetectedField[]>;
  isDetecting: boolean;
  detectionError: string | null;
  detectedFields: DetectedField[];
  detectionGroups: DetectionGroup[];
  sampleCount: number;
}

export function useFieldDetection(tableName?: string): UseFieldDetectionResult {
  const client = useApi();
  const [isDetecting, setIsDetecting] = useState(false);
  const [detectionError, setDetectionError] = useState<string | null>(null);
  const [detectedFields, setDetectedFields] = useState<DetectedField[]>([]);
  const [detectionGroups, setDetectionGroups] = useState<DetectionGroup[]>([]);
  const [sampleCount, setSampleCount] = useState(0);

  const reservedNames = useMemo(() => new Set(RESERVED_FIELD_NAMES), []);

  const detect = useCallback(async (): Promise<DetectedField[]> => {
    if (!tableName) return [];

    setIsDetecting(true);
    setDetectionError(null);
    setDetectedFields([]);
    setDetectionGroups([]);

    try {
      const response = await client.tables.query(tableName, {
        full_text_search: { match_all: {} },
        limit: 50,
      });

      const hits = response?.responses?.[0]?.hits?.hits;
      if (!hits || hits.length === 0) {
        setDetectionError("No documents found in table");
        return [];
      }

      const results = hits as QueryHit[];
      const count = results.length;
      setSampleCount(count);

      // Group hits by _type
      const typeGroups = new Map<string, QueryHit[]>();
      for (const hit of results) {
        const typeName = (hit._source?._type as string) || "default";
        const group = typeGroups.get(typeName) || [];
        group.push(hit);
        typeGroups.set(typeName, group);
      }

      // Build per-type field detection
      const groups: DetectionGroup[] = [];
      const allFieldMap = new Map<
        string,
        {
          name: string;
          exampleValue: unknown;
          seenCount: number;
          inferredType: "string" | "number" | "boolean" | "object" | "array";
        }
      >();

      for (const [typeName, typeHits] of typeGroups) {
        const typeFieldMap = new Map<
          string,
          {
            name: string;
            exampleValue: unknown;
            seenCount: number;
            inferredType: "string" | "number" | "boolean" | "object" | "array";
          }
        >();

        for (const hit of typeHits) {
          const source = hit._source;
          if (!source) continue;

          for (const [key, value] of Object.entries(source)) {
            if (key.startsWith("_") || reservedNames.has(key)) continue;

            // Per-type tracking
            const existing = typeFieldMap.get(key);
            if (existing) {
              existing.seenCount++;
              if (existing.exampleValue === null && value !== null) {
                existing.exampleValue = value;
              }
            } else {
              typeFieldMap.set(key, {
                name: key,
                exampleValue: value,
                seenCount: 1,
                inferredType: inferJSONType(value),
              });
            }

            // Global tracking
            const globalExisting = allFieldMap.get(key);
            if (globalExisting) {
              globalExisting.seenCount++;
              if (globalExisting.exampleValue === null && value !== null) {
                globalExisting.exampleValue = value;
              }
            } else {
              allFieldMap.set(key, {
                name: key,
                exampleValue: value,
                seenCount: 1,
                inferredType: inferJSONType(value),
              });
            }
          }
        }

        const typeFields = Array.from(typeFieldMap.values())
          .sort((a, b) => b.seenCount - a.seenCount)
          .map(
            (f): DetectedField => ({
              name: f.name,
              inferredType: f.inferredType,
              exampleValue: f.exampleValue,
              frequency: f.seenCount / typeHits.length,
              sampleCount: typeHits.length,
              suggestedAntflyTypes: getDefaultAntflyType(f.inferredType, f.exampleValue),
            })
          );

        groups.push({ typeName, fields: typeFields, docCount: typeHits.length });
      }

      // Flat list (all types combined) for backward compatibility
      const sorted = Array.from(allFieldMap.values())
        .sort((a, b) => b.seenCount - a.seenCount)
        .map(
          (f): DetectedField => ({
            name: f.name,
            inferredType: f.inferredType,
            exampleValue: f.exampleValue,
            frequency: f.seenCount / count,
            sampleCount: count,
            suggestedAntflyTypes: getDefaultAntflyType(f.inferredType, f.exampleValue),
          })
        );

      setDetectedFields(sorted);
      setDetectionGroups(groups);
      return sorted;
    } catch (err) {
      setDetectionError(err instanceof Error ? err.message : "Failed to detect fields");
      return [];
    } finally {
      setIsDetecting(false);
    }
  }, [tableName, client, reservedNames]);

  return { detect, isDetecting, detectionError, detectedFields, detectionGroups, sampleCount };
}
