import {
  Badge,
  Button,
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@antfly/design-system";
import type { QueryHit } from "@antfly/sdk";
import { ChevronDown, ChevronRight } from "lucide-react";
import type React from "react";
import { useMemo } from "react";
import FieldValueDisplay from "./FieldValueDisplay";

type UnknownRecord = Record<string, unknown>;

const PREVIEW_TEXT_FIELDS = [
  "text",
  "content",
  "chunk_text",
  "body",
  "description",
  "title",
  "name",
];
const SOURCE_LABEL_FIELDS = ["filename", "source_path", "url", "path", "title", "name", "id"];

function asRecord(value: unknown): UnknownRecord | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as UnknownRecord)
    : null;
}

function firstStringField(record: unknown, fields: string[]): string | null {
  const object = asRecord(record);
  if (!object) return null;
  for (const field of fields) {
    const value = object[field];
    if (typeof value === "string" && value.trim()) return value;
  }
  return null;
}

function nestedRecord(record: unknown, path: string[]): UnknownRecord | null {
  let current: unknown = record;
  for (const segment of path) {
    const object = asRecord(current);
    if (!object) return null;
    current = object[segment];
  }
  return asRecord(current);
}

function firstHierarchyChunk(hierarchy: unknown): UnknownRecord | null {
  const object = asRecord(hierarchy);
  const chunks = object?.chunks;
  return Array.isArray(chunks) ? asRecord(chunks[0]) : null;
}

function hierarchyPreviewText(hit: QueryHit): string | null {
  const hierarchy = asRecord(hit.hierarchy);
  if (!hierarchy) return null;
  const chunk = firstHierarchyChunk(hierarchy);
  return (
    firstStringField(hit._source, PREVIEW_TEXT_FIELDS) ||
    firstStringField(hierarchy.artifact, PREVIEW_TEXT_FIELDS) ||
    firstStringField(chunk, PREVIEW_TEXT_FIELDS) ||
    firstStringField(nestedRecord(chunk, ["_source"]), PREVIEW_TEXT_FIELDS) ||
    firstStringField(nestedRecord(chunk, ["document"]), PREVIEW_TEXT_FIELDS) ||
    firstStringField(
      nestedRecord(hierarchy, ["ancestors", "unit", "document"]),
      PREVIEW_TEXT_FIELDS
    )
  );
}

function hierarchySourceLabel(hit: QueryHit): string | null {
  const hierarchy = asRecord(hit.hierarchy);
  if (!hierarchy) return null;
  const sourceDocument = nestedRecord(hierarchy, ["ancestors", "source", "document"]);
  const source = nestedRecord(hierarchy, ["ancestors", "source"]);
  return (
    firstStringField(sourceDocument, SOURCE_LABEL_FIELDS) ||
    firstStringField(source, SOURCE_LABEL_FIELDS) ||
    firstStringField(hit._source, SOURCE_LABEL_FIELDS) ||
    (typeof hierarchy.parent_doc_key === "string" ? hierarchy.parent_doc_key : null)
  );
}

function truncatePreview(value: string, maxLength = 140): string {
  return value.length > maxLength ? `${value.substring(0, maxLength)}...` : value;
}

function looksLikeLowQualityExtractedText(value: string): boolean {
  const compact = value.replace(/\s+/g, "");
  if (compact.length < 40) return false;
  const alphaNumeric = (compact.match(/[\p{L}\p{N}]/gu) || []).length;
  const controlsOrReplacement = (compact.match(/[\u0000-\u001f\ufffd]/gu) || []).length;
  const symbolHeavy = alphaNumeric / compact.length < 0.35;
  return controlsOrReplacement > 0 || symbolHeavy;
}

interface QueryResultItemProps {
  hit: QueryHit;
  index: number;
  isExpanded: boolean;
  onToggle: () => void;
  visibleFields?: Set<string>;
  previewFields?: string[];
}

const QueryResultItem: React.FC<QueryResultItemProps> = ({
  hit,
  index,
  isExpanded,
  onToggle,
  visibleFields,
  previewFields = ["title", "name", "description", "text", "content"],
}) => {
  const { _id, _source, _score } = hit;

  // Calculate score display
  const scoreDisplay = useMemo(() => {
    if (typeof _score !== "number") return null;

    const percentage = Math.min(100, Math.max(0, _score * 100));

    return {
      percentage,
      value: _score.toFixed(4),
    };
  }, [_score]);

  // Get preview field values
  const previewData = useMemo(() => {
    if (!_source) return [];

    const fields = previewFields
      .filter((field) => _source[field] !== undefined && _source[field] !== null)
      .slice(0, 3);

    return fields.map((field) => ({
      name: field,
      value: _source[field],
    }));
  }, [_source, previewFields]);

  // Get all fields for expanded view
  const allFields = useMemo(() => {
    if (!_source) return [];

    return Object.entries(_source)
      .filter(([key]) => !visibleFields || visibleFields.has(key))
      .sort(([a], [b]) => {
        // Sort special fields first
        const aSpecial = ["_id", "title", "name", "description"].includes(a);
        const bSpecial = ["_id", "title", "name", "description"].includes(b);
        if (aSpecial && !bSpecial) return -1;
        if (!aSpecial && bSpecial) return 1;
        return a.localeCompare(b);
      });
  }, [_source, visibleFields]);

  // Get preview text
  const previewText = useMemo(() => {
    const hierarchyText = hierarchyPreviewText(hit);
    if (hierarchyText) return truncatePreview(hierarchyText);

    if (previewData.length === 0) return "No preview available";

    const firstField = previewData[0];
    const value = firstField.value;

    if (typeof value === "string") {
      return truncatePreview(value, 100);
    }

    if (Array.isArray(value)) {
      return `[${value.length} items]`;
    }

    if (typeof value === "object" && value !== null) {
      return `{${Object.keys(value).length} fields}`;
    }

    return String(value);
  }, [hit, previewData]);

  const sourceLabel = useMemo(() => hierarchySourceLabel(hit), [hit]);
  const hasLowQualityExtractedText = useMemo(
    () => looksLikeLowQualityExtractedText(previewText),
    [previewText]
  );

  return (
    <Collapsible
      open={isExpanded}
      onOpenChange={onToggle}
      className="border rounded-none bg-card hover:bg-accent/50 transition-colors"
    >
      <CollapsibleTrigger asChild>
        <Button variant="ghost" className="w-full h-auto p-4 hover:bg-transparent justify-start">
          <div className="flex items-start gap-3 w-full text-left">
            {/* Expand/Collapse Icon */}
            <div className="shrink-0 mt-0.5">
              {isExpanded ? (
                <ChevronDown className="h-4 w-4 text-muted-foreground" />
              ) : (
                <ChevronRight className="h-4 w-4 text-muted-foreground" />
              )}
            </div>

            {/* Content */}
            <div className="flex-1 min-w-0 space-y-1">
              {/* Header: ID and Score */}
              <div className="flex items-center gap-2 flex-wrap">
                <code className="text-sm font-mono bg-muted px-2 py-0.5 rounded-none">
                  {_id || `Result #${index + 1}`}
                </code>

                {scoreDisplay && (
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Badge className="gap-1 cursor-help">
                        <span className="text-muted-foreground">Score:</span>
                        {scoreDisplay.value}
                      </Badge>
                    </TooltipTrigger>
                    <TooltipContent className="max-w-xs">
                      <p>
                        Search ranking score. For vector search: L2 squared distance (lower = more
                        similar). For hybrid search: RRF fusion score.
                      </p>
                    </TooltipContent>
                  </Tooltip>
                )}
              </div>

              {/* Preview Text */}
              {sourceLabel && (
                <p className="text-xs text-muted-foreground truncate">Source: {sourceLabel}</p>
              )}
              {hasLowQualityExtractedText && (
                <Badge variant="default" className="w-fit text-[11px]">
                  Low-quality extracted text
                </Badge>
              )}
              {!isExpanded && (
                <p className="text-sm text-muted-foreground line-clamp-2">{previewText}</p>
              )}
            </div>
          </div>
        </Button>
      </CollapsibleTrigger>

      <CollapsibleContent className="px-4 pb-4">
        <div className="pl-7 space-y-3 mt-2">
          {/* Score Details */}
          {scoreDisplay && (
            <div className="flex items-center gap-2">
              <Tooltip>
                <TooltipTrigger asChild>
                  <span className="text-xs font-medium text-muted-foreground min-w-24 cursor-help underline decoration-dotted">
                    Score:
                  </span>
                </TooltipTrigger>
                <TooltipContent className="max-w-xs">
                  <p>
                    For vector search: L2 squared distance (lower = more similar). For hybrid
                    search: RRF fusion score.
                  </p>
                </TooltipContent>
              </Tooltip>
              <span className="text-sm font-mono">{scoreDisplay.value}</span>
            </div>
          )}

          {/* Divider */}
          <div className="border-t" />

          {/* All Fields */}
          <div className="space-y-3">
            {allFields.length === 0 ? (
              <p className="text-sm text-muted-foreground italic">No fields to display</p>
            ) : (
              allFields.map(([fieldName, fieldValue]) => (
                <div key={fieldName} className="grid grid-cols-12 gap-3 items-start">
                  <div className="col-span-12 sm:col-span-3">
                    <span className="text-xs font-medium text-muted-foreground break-words">
                      {fieldName}
                    </span>
                  </div>
                  <div className="col-span-12 sm:col-span-9">
                    <FieldValueDisplay value={fieldValue} fieldName={fieldName} />
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
};

export default QueryResultItem;
