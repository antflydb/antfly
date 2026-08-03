"use client";

import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from "@antfly/design-system";
import {
  ArrowUpDown,
  Bot,
  ClipboardCheck,
  Database,
  FileInput,
  FileStack,
  HelpCircle,
  KeyRound,
  Library,
  Loader2,
  Mic,
  Moon,
  Network,
  Plug,
  Plus,
  Repeat2,
  RotateCw,
  ScanLine,
  Scissors,
  Search,
  Shield,
  Sun,
  Table,
  Tag,
  Upload,
  Users,
  Waypoints,
} from "lucide-react";
import * as React from "react";
import { useNavigate } from "react-router-dom";
import { isProductEnabled, type ProductId } from "@/config/products";
import { type CommandAction, type CommandDefinition, commandDefinitions } from "@/data/commands";
import { useApiConfig } from "@/hooks/use-api-config";
import { useAuth } from "@/hooks/use-auth";
import { useTable } from "@/hooks/use-table";
import { useTheme } from "@/hooks/use-theme";
import { type SemanticResult, semanticSearch } from "@/lib/semantic-search";
import { isExternalAuthMode } from "@/runtime-config";

// Map icon names to components
const iconMap: Record<string, React.ComponentType<{ className?: string }>> = {
  Table,
  Plus,
  Library,
  Users,
  FileInput,
  FileStack,
  Shield,
  KeyRound,
  Scissors,
  Tag,
  HelpCircle,
  Network,
  Plug,
  ClipboardCheck,
  Database,
  Bot,
  Search,
  Upload,
  Waypoints,
  ArrowUpDown,
  Repeat2,
  RotateCw,
  ScanLine,
  Mic,
  Moon,
  Sun,
};

interface CommandPaletteContextType {
  isOpen: boolean;
  setIsOpen: (open: boolean) => void;
  toggle: () => void;
}

const CommandPaletteContext = React.createContext<CommandPaletteContextType | undefined>(undefined);

interface PaletteCommand {
  id: string;
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  href?: string;
  action?: CommandAction;
  product?: ProductId;
  adminOnly?: boolean;
}

function isSupportedAction(action?: string) {
  return !action || action === "toggle-theme";
}

function toPaletteCommand(command: CommandDefinition): PaletteCommand {
  return {
    id: command.id,
    icon: iconMap[command.icon] ?? HelpCircle,
    label: command.label,
    href: command.href,
    action: command.action,
    product: command.product,
    adminOnly: command.adminOnly,
  };
}

export function CommandPaletteProvider({ children }: { children: React.ReactNode }) {
  const [isOpen, setIsOpen] = React.useState(false);
  const [mounted, setMounted] = React.useState(false);
  const [searchValue, setSearchValue] = React.useState("");
  const [semanticResults, setSemanticResults] = React.useState<SemanticResult[]>([]);
  const [isSearching, setIsSearching] = React.useState(false);
  const navigate = useNavigate();

  const { hasPermission } = useAuth();
  const { selectedTable, graphIndexes, isLoadingIndexes } = useTable();
  const { theme, setTheme } = useTheme();
  const { inferenceUrl } = useApiConfig();
  const showLocalAdminRoutes = !isExternalAuthMode();
  const showAdmin = showLocalAdminRoutes && hasPermission("*", "*", "admin");

  const isCommandAvailable = React.useCallback(
    (item: { href?: string; action?: string; product?: ProductId; adminOnly?: boolean }) => {
      if (!isSupportedAction(item.action)) {
        return false;
      }
      if (item.product && !isProductEnabled(item.product)) {
        return false;
      }
      if (item.adminOnly && !showAdmin) {
        return false;
      }
      if (item.href === "/retrieval/graph" && !isLoadingIndexes && graphIndexes.length === 0) {
        return false;
      }
      return true;
    },
    [graphIndexes.length, isLoadingIndexes, showAdmin]
  );

  React.useEffect(() => {
    setMounted(true);
  }, []);

  const toggle = React.useCallback(() => {
    setIsOpen((prev) => !prev);
  }, []);

  // Global keyboard shortcut for command palette (⌘K)
  React.useEffect(() => {
    const down = (e: KeyboardEvent) => {
      if (e.key === "k" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        toggle();
      }
    };

    document.addEventListener("keydown", down);
    return () => document.removeEventListener("keydown", down);
  }, [toggle]);

  const navigationCommands = React.useMemo(() => {
    const commands = commandDefinitions
      .filter((command) => command.group === "navigation")
      .map(toPaletteCommand);
    return commands.filter(isCommandAvailable);
  }, [isCommandAvailable]);

  const toolCommands = React.useMemo(() => {
    const commands = commandDefinitions
      .filter((command) => command.group === "tools")
      .map(toPaletteCommand);
    return commands.filter(isCommandAvailable);
  }, [isCommandAvailable]);

  const quickActionCommands = React.useMemo(() => {
    const commands = commandDefinitions
      .filter((command) => command.group === "quickActions")
      .map(toPaletteCommand);
    return commands.filter(isCommandAvailable);
  }, [isCommandAvailable]);

  const contextualCommands = React.useMemo<PaletteCommand[]>(() => {
    if (!selectedTable) return [];
    const tablePath = `/tables/${encodeURIComponent(selectedTable)}`;
    return [
      {
        id: "context-create-index",
        icon: Plus,
        label: `Create index for ${selectedTable}`,
        href: `${tablePath}?section=indexes`,
        product: "antfly",
      },
      {
        id: "context-upload-data",
        icon: Upload,
        label: `Upload data to ${selectedTable}`,
        href: `${tablePath}?section=bulk`,
        product: "antfly",
      },
      {
        id: "context-search-table",
        icon: Search,
        label: `Search ${selectedTable}`,
        href: `${tablePath}?section=semantic`,
        product: "antfly",
      },
      {
        id: "context-view-retrieval-trace",
        icon: ClipboardCheck,
        label: `View retrieval trace for ${selectedTable}`,
        href: `${tablePath}?section=semantic`,
        product: "antfly",
      },
    ];
  }, [selectedTable]);

  // All command items for string matching check
  const allItems = React.useMemo(
    () => [
      ...navigationCommands.map((c) => c.label),
      ...toolCommands.map((c) => c.label),
      ...contextualCommands.map((c) => c.label),
      ...quickActionCommands.map((c) => c.label),
    ],
    [navigationCommands, toolCommands, contextualCommands, quickActionCommands]
  );

  // Check if cmdk's string filter would find any matches
  const hasStringMatches = React.useMemo(() => {
    if (!searchValue) return true;
    const query = searchValue.toLowerCase();
    return allItems.some((label) => label.toLowerCase().includes(query));
  }, [searchValue, allItems]);

  // Debounced semantic search when no string matches
  React.useEffect(() => {
    if (hasStringMatches || searchValue.length < 2) {
      setSemanticResults([]);
      return;
    }

    setIsSearching(true);
    const timer = setTimeout(async () => {
      try {
        const results = await semanticSearch(searchValue, inferenceUrl("embed"));
        const filteredResults = results.filter((result) => isCommandAvailable(result.item));
        setSemanticResults(filteredResults);
      } catch (e) {
        console.error("Semantic search failed:", e);
        setSemanticResults([]);
      }
      setIsSearching(false);
    }, 300);

    return () => clearTimeout(timer);
  }, [searchValue, hasStringMatches, inferenceUrl, isCommandAvailable]);

  // Reset search state when dialog closes
  React.useEffect(() => {
    if (!isOpen) {
      setSearchValue("");
      setSemanticResults([]);
      setIsSearching(false);
    }
  }, [isOpen]);

  const handleSelect = React.useCallback(
    (href?: string, action?: string) => {
      setIsOpen(false);

      if (action === "toggle-theme") {
        setTheme(theme === "system" ? "light" : theme === "light" ? "dark" : "system");
      } else if (href) {
        navigate(href);
      }
    },
    [navigate, theme, setTheme]
  );

  return (
    <CommandPaletteContext.Provider
      value={{
        isOpen,
        setIsOpen,
        toggle,
      }}
    >
      {children}

      <CommandDialog open={isOpen} onOpenChange={setIsOpen}>
        <CommandInput
          placeholder="Type a command or search..."
          value={searchValue}
          onValueChange={setSearchValue}
        />
        <CommandList>
          <CommandEmpty>
            {isSearching ? (
              <div className="flex items-center justify-center gap-2 py-6">
                <Loader2 className="h-4 w-4 animate-spin" />
                <span>Searching...</span>
              </div>
            ) : (
              "No results found."
            )}
          </CommandEmpty>

          {/* Semantic Search Results - shown when no string matches */}
          {!hasStringMatches && semanticResults.length > 0 && (
            <CommandGroup heading="Closest Matches">
              {semanticResults.map((result) => {
                const Icon = iconMap[result.item.icon] || HelpCircle;
                return (
                  <CommandItem
                    key={result.item.id}
                    value={`${searchValue} ${result.item.label}`}
                    onSelect={() => handleSelect(result.item.href, result.item.action)}
                  >
                    <Icon className="h-4 w-4" />
                    <span>{result.item.label}</span>
                  </CommandItem>
                );
              })}
            </CommandGroup>
          )}

          {/* Quick Actions */}
          <CommandGroup heading="Quick Actions">
            {quickActionCommands.map((command) => {
              const Icon = command.icon;
              let DynamicIcon = Icon;
              let DynamicLabel = command.label;

              // Update icon and label based on current state (only when mounted to avoid hydration issues)
              if (mounted) {
                if (command.action === "toggle-theme") {
                  DynamicIcon = theme === "dark" ? Sun : Moon;
                  DynamicLabel = theme === "dark" ? "Switch to Light Mode" : "Switch to Dark Mode";
                }
              }

              return (
                <CommandItem
                  key={command.id}
                  onSelect={() => handleSelect(undefined, command.action)}
                  className="flex items-center gap-2 cursor-pointer"
                >
                  <DynamicIcon className="h-4 w-4" />
                  <span>{DynamicLabel}</span>
                </CommandItem>
              );
            })}
          </CommandGroup>

          <CommandSeparator />

          {/* Navigation */}
          <CommandGroup heading="Navigation">
            {navigationCommands.map((command) => (
              <CommandItem
                key={command.id}
                onSelect={() => handleSelect(command.href)}
                className="flex items-center gap-2 cursor-pointer"
              >
                <command.icon className="h-4 w-4" />
                <span>{command.label}</span>
              </CommandItem>
            ))}
          </CommandGroup>

          <CommandSeparator />

          {contextualCommands.length > 0 && (
            <>
              <CommandGroup heading="Current Table">
                {contextualCommands.map((command) => (
                  <CommandItem
                    key={command.id}
                    onSelect={() => handleSelect(command.href)}
                    className="flex items-center gap-2 cursor-pointer"
                  >
                    <command.icon className="h-4 w-4" />
                    <span>{command.label}</span>
                  </CommandItem>
                ))}
              </CommandGroup>

              <CommandSeparator />
            </>
          )}

          {/* Tools */}
          <CommandGroup heading="Tools">
            {toolCommands.map((command) => (
              <CommandItem
                key={command.id}
                onSelect={() => handleSelect(command.href)}
                className="flex items-center gap-2 cursor-pointer"
              >
                <command.icon className="h-4 w-4" />
                <span>{command.label}</span>
              </CommandItem>
            ))}
          </CommandGroup>
        </CommandList>
      </CommandDialog>
    </CommandPaletteContext.Provider>
  );
}

// eslint-disable-next-line react-refresh/only-export-components
export function useCommandPalette() {
  const context = React.useContext(CommandPaletteContext);
  if (context === undefined) {
    throw new Error("useCommandPalette must be used within a CommandPaletteProvider");
  }
  return context;
}
