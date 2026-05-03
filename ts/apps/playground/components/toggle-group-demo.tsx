"use client";

import { ToggleGroup, ToggleGroupItem } from "@antfly/design-system";
import * as React from "react";

/**
 * ToggleGroup — three variants:
 *   default  — segmented bar, connected items, shared rounded ends
 *   outline  — same shape, outline bg + shadow
 *   pill     — detached wrapping pills; state-colored bg/border; floats freely
 *
 * Pill is the right choice for filter-chip clusters: file-type selectors,
 * date-preset chooser, dynamic facet tags. Default/outline fit segmented
 * controls: view switchers, theme toggles, small option rows.
 */

const FILE_TYPES = ["text", "code", "pdf", "document", "image", "audio", "video"];
const DATE_PRESETS = [
  { value: "any", label: "Any time" },
  { value: "today", label: "Today" },
  { value: "7d", label: "7 days" },
  { value: "30d", label: "30 days" },
  { value: "90d", label: "90 days" },
  { value: "1y", label: "1 year" },
  { value: "custom", label: "Custom" },
];

export function ToggleGroupDemo() {
  const [types, setTypes] = React.useState<string[]>(["code", "pdf"]);
  const [preset, setPreset] = React.useState("7d");
  const [align, setAlign] = React.useState<string>("left");

  return (
    <div className="space-y-6">
      {/* Pill — multi-select */}
      <div className="space-y-2">
        <div className="mono-label">pill · multi-select</div>
        <ToggleGroup
          type="multiple"
          variant="pill"
          size="sm"
          value={types}
          onValueChange={setTypes}
        >
          {FILE_TYPES.map((ft) => (
            <ToggleGroupItem key={ft} value={ft}>
              {ft}
            </ToggleGroupItem>
          ))}
        </ToggleGroup>
      </div>

      {/* Pill — single-select */}
      <div className="space-y-2">
        <div className="mono-label">pill · single-select</div>
        <ToggleGroup
          type="single"
          variant="pill"
          size="sm"
          value={preset}
          onValueChange={(v) => v && setPreset(v)}
        >
          {DATE_PRESETS.map((dp) => (
            <ToggleGroupItem key={dp.value} value={dp.value}>
              {dp.label}
            </ToggleGroupItem>
          ))}
        </ToggleGroup>
      </div>

      {/* Default — segmented */}
      <div className="space-y-2">
        <div className="mono-label">default · segmented</div>
        <ToggleGroup type="single" value={align} onValueChange={(v) => v && setAlign(v)} size="sm">
          <ToggleGroupItem value="left">Left</ToggleGroupItem>
          <ToggleGroupItem value="center">Center</ToggleGroupItem>
          <ToggleGroupItem value="right">Right</ToggleGroupItem>
        </ToggleGroup>
      </div>

      {/* Outline — segmented with border */}
      <div className="space-y-2">
        <div className="mono-label">outline · segmented</div>
        <ToggleGroup
          type="single"
          variant="outline"
          value={align}
          onValueChange={(v) => v && setAlign(v)}
          size="sm"
        >
          <ToggleGroupItem value="left">Left</ToggleGroupItem>
          <ToggleGroupItem value="center">Center</ToggleGroupItem>
          <ToggleGroupItem value="right">Right</ToggleGroupItem>
        </ToggleGroup>
      </div>
    </div>
  );
}
