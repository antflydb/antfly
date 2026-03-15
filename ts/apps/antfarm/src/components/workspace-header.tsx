import { MoonIcon, SunIcon } from "@radix-ui/react-icons";
import { Maximize2, Minimize2, Monitor, Search } from "lucide-react";
import type * as React from "react";
import { useCommandPalette } from "@/components/command-palette-provider";
import { useContentWidth } from "@/components/content-width-provider";
import { SettingsDialog } from "@/components/SettingsDialog";
import { Button } from "@/components/ui/button";
import { useTheme } from "@/hooks/use-theme";
import { cn } from "@/lib/utils";

interface WorkspaceHeaderProps extends React.HTMLAttributes<HTMLDivElement> {
  title?: string;
}

export function WorkspaceHeader({ title, className, ...props }: WorkspaceHeaderProps) {
  const { toggle: toggleCommandPalette } = useCommandPalette();
  const { contentWidth, toggleContentWidth } = useContentWidth();
  const { theme, setTheme } = useTheme();

  const toggleTheme = () => {
    const next = theme === "system" ? "light" : theme === "light" ? "dark" : "system";
    setTheme(next);
  };

  return (
    <header
      className={cn("sticky top-0 z-10 flex h-14 items-center gap-4 bg-background px-4", className)}
      {...props}
    >
      {title && <h1 className="text-lg font-semibold">{title}</h1>}

      <div className="ml-auto flex items-center gap-2">
        {/* Command Palette Trigger */}
        <Button variant="outline" size="sm" onClick={toggleCommandPalette} className="gap-2">
          <Search className="size-4" />
          <span className="hidden md:inline">⌘K</span>
        </Button>

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

        {/* Content Width Toggle */}
        <Button
          variant="ghost"
          size="icon"
          onClick={toggleContentWidth}
          title={contentWidth === "restricted" ? "Expand Content Width" : "Restrict Content Width"}
        >
          {contentWidth === "restricted" ? (
            <Maximize2 className="size-4" />
          ) : (
            <Minimize2 className="size-4" />
          )}
        </Button>
      </div>
    </header>
  );
}
