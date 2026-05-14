import { Button } from "@antfly/design-system";
import { MoonIcon, SunIcon } from "@radix-ui/react-icons";
import { Monitor, Rows3, Rows4, Search } from "lucide-react";
import type * as React from "react";
import { useCommandPalette } from "@/components/command-palette-provider";
import { DashboardGeneratorControl } from "@/components/playground/DashboardGeneratorControl";
import { SettingsDialog } from "@/components/SettingsDialog";
import { useDensity } from "@/hooks/use-density";
import { useTheme } from "@/hooks/use-theme";
import { cn } from "@/lib/utils";

interface WorkspaceHeaderProps extends React.HTMLAttributes<HTMLDivElement> {
  title?: string;
}

export function WorkspaceHeader({ title, className, ...props }: WorkspaceHeaderProps) {
  const { toggle: toggleCommandPalette } = useCommandPalette();
  const { density, toggleDensity } = useDensity();
  const { theme, setTheme } = useTheme();

  const toggleTheme = () => {
    const next = theme === "system" ? "light" : theme === "light" ? "dark" : "system";
    setTheme(next);
  };

  return (
    <header
      className={cn(
        "af-workspace-header sticky top-0 z-10 flex shrink-0 h-14 items-center gap-4 border-b border-border bg-background px-4",
        className
      )}
      {...props}
    >
      {title && <h1 className="text-lg font-semibold">{title}</h1>}

      <div className="ml-auto flex items-center gap-2">
        {/* Command Palette Trigger */}
        <Button variant="outline" size="sm" onClick={toggleCommandPalette} className="gap-2">
          <Search className="size-4" />
          <span className="hidden md:inline">⌘K</span>
        </Button>

        <DashboardGeneratorControl />

        {/* Settings */}
        <SettingsDialog />

        {/* Dark Mode Toggle */}
        <Button
          variant="ghost"
          size="icon"
          onClick={toggleTheme}
          title={
            theme === "system"
              ? "Switch to Light Mode"
              : theme === "light"
                ? "Switch to Dark Mode"
                : "Switch to System Theme"
          }
        >
          {theme === "system" ? (
            <SunIcon className="size-4" />
          ) : theme === "light" ? (
            <MoonIcon className="size-4" />
          ) : (
            <Monitor className="size-4" />
          )}
        </Button>

        {/* Density Toggle */}
        <Button
          variant="ghost"
          size="icon"
          onClick={toggleDensity}
          title={density === "compact" ? "Switch to Comfortable" : "Switch to Compact"}
        >
          {density === "compact" ? <Rows3 className="size-4" /> : <Rows4 className="size-4" />}
        </Button>
      </div>
    </header>
  );
}
