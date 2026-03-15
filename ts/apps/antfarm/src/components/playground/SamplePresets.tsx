import { FileText } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

export interface SamplePreset {
  name: string;
  description: string;
  onLoad: () => void;
}

interface SamplePresetsProps {
  presets: SamplePreset[];
}

export function SamplePresets({ presets }: SamplePresetsProps) {
  if (presets.length === 0) return null;

  // If only one preset, render a simple button
  if (presets.length === 1) {
    return (
      <Button variant="outline" onClick={presets[0].onLoad}>
        <FileText className="h-4 w-4 mr-2" />
        Load Sample
      </Button>
    );
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline">
          <FileText className="h-4 w-4 mr-2" />
          Load Sample
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-72">
        {presets.map((preset) => (
          <DropdownMenuItem key={preset.name} onClick={preset.onLoad}>
            <div>
              <div className="font-medium">{preset.name}</div>
              <div className="text-xs text-muted-foreground">{preset.description}</div>
            </div>
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
