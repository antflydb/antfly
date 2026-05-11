"use client";

import {
  MultiSelect,
  MultiSelectContent,
  MultiSelectItem,
  MultiSelectTrigger,
} from "@antfly/design-system";
import * as React from "react";

const frameworks = [
  { value: "react", label: "React" },
  { value: "vue", label: "Vue" },
  { value: "svelte", label: "Svelte" },
  { value: "angular", label: "Angular" },
  { value: "solid", label: "SolidJS" },
  { value: "qwik", label: "Qwik" },
];

export function MultiSelectDemo() {
  const [selected, setSelected] = React.useState<string[]>(["react", "svelte"]);

  return (
    <div className="w-80 space-y-4">
      <MultiSelect value={selected} onValueChange={setSelected}>
        <MultiSelectTrigger placeholder="Pick frameworks…" />
        <MultiSelectContent searchPlaceholder="Search frameworks…" emptyMessage="No framework found.">
          {frameworks.map((fw) => (
            <MultiSelectItem key={fw.value} value={fw.value}>
              {fw.label}
            </MultiSelectItem>
          ))}
        </MultiSelectContent>
      </MultiSelect>
      <p className="text-sm text-muted-foreground">
        Selected: {selected.length === 0 ? "none" : selected.join(", ")}
      </p>
    </div>
  );
}
