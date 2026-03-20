import { Sparkles } from "lucide-react";
import {
  formatGeneratorSummary,
  GENERATOR_DEFAULT_CONFIG,
  GeneratorSelector,
} from "@/components/playground/GeneratorSelector";
import { Button } from "@/components/ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { useGeneratorPreference } from "@/hooks/use-generator-preference";

export function DashboardGeneratorControl() {
  const { dashboardGenerator, setDashboardGenerator } = useGeneratorPreference();

  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button variant="outline" size="sm" className="max-w-[260px] gap-2">
          <Sparkles className="size-4 shrink-0" />
          <span className="hidden md:inline">Generator</span>
          <span className="hidden lg:inline truncate text-muted-foreground">
            {formatGeneratorSummary(dashboardGenerator)}
          </span>
        </Button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-[420px] space-y-3">
        <div className="space-y-1">
          <h4 className="text-sm font-medium">Dashboard Generator</h4>
          <p className="text-xs text-muted-foreground">
            Playgrounds and query builders inherit this unless they set a local override.
          </p>
        </div>
        <GeneratorSelector
          value={dashboardGenerator}
          onChange={setDashboardGenerator}
          defaultLabel="Server default"
          defaultDescription="Leave unset to defer to the backend default generator."
          customLabel="Dashboard default"
          defaultConfig={GENERATOR_DEFAULT_CONFIG}
        />
      </PopoverContent>
    </Popover>
  );
}
