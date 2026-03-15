"use client";

import { TermiteClient } from "@antfly/termite-sdk";
import {
  ArrowUpDown,
  ClipboardCheck,
  HelpCircle,
  Library,
  Loader2,
  Maximize2,
  MessageSquare,
  Minimize2,
  Moon,
  Network,
  Plus,
  Repeat2,
  Scissors,
  Sun,
  Table,
  Tag,
  Users,
} from "lucide-react";
import * as React from "react";
import { useNavigate } from "react-router-dom";
import { useContentWidth } from "@/components/content-width-provider";
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from "@/components/ui/command";
import { useApiConfig } from "@/hooks/use-api-config";
import { useTheme } from "@/hooks/use-theme";
import { type SemanticResult, semanticSearch } from "@/lib/semantic-search";

// Map icon names to components
const iconMap: Record<string, React.ComponentType<{ className?: string }>> = {
  Table,
  Plus,
  Library,
  Users,
  Scissors,
  Tag,
  HelpCircle,
  Network,
  ClipboardCheck,
  MessageSquare,
  Moon,
  Sun,
  Maximize2,
  Minimize2,
};

interface CommandPaletteContextType {
  isOpen: boolean;
  setIsOpen: (open: boolean) => void;
  toggle: () => void;
}

const CommandPaletteContext = React.createContext<CommandPaletteContextType | undefined>(undefined);

export function CommandPaletteProvider({ children }: { children: React.ReactNode }) {
  const [isOpen, setIsOpen] = React.useState(false);
  const [mounted, setMounted] = React.useState(false);
  const [searchValue, setSearchValue] = React.useState("");
  const [semanticResults, setSemanticResults] = React.useState<SemanticResult[]>([]);
  const [isSearching, setIsSearching] = React.useState(false);
  const navigate = useNavigate();

  const { theme, setTheme } = useTheme();
  const { contentWidth, toggleContentWidth } = useContentWidth();
  const { termiteApiUrl } = useApiConfig();

  // Create TermiteClient for semantic search
  const termiteClient = React.useMemo(
    () => new TermiteClient({ baseUrl: termiteApiUrl }),
    [termiteApiUrl]
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

  const navigationCommands = [
    { icon: Table, label: "Tables", href: "/" },
    { icon: Plus, label: "Create Table", href: "/create" },
    { icon: Library, label: "Models", href: "/models" },
    { icon: Users, label: "Users", href: "/users" },
  ];

  const playgroundCommands = [
    { icon: Scissors, label: "Chunking Playground", href: "/playground/chunking" },
    { icon: Tag, label: "Recognize Playground", href: "/playground/recognize" },
    { icon: Repeat2, label: "Rewriting Playground", href: "/playground/rewrite" },
    { icon: ArrowUpDown, label: "Reranking Playground", href: "/playground/rerank" },
    { icon: Network, label: "Knowledge Graph", href: "/playground/kg" },
    { icon: ClipboardCheck, label: "Evals", href: "/playground/evals" },
  ];

  const quickActionCommands = [
    { icon: Moon, label: "Toggle Theme", action: "toggle-theme" },
    { icon: Maximize2, label: "Toggle Content Width", action: "toggle-width" },
  ];

  // All command items for string matching check
  const allItems = React.useMemo(
    () => [
      ...navigationCommands.map((c) => c.label),
      ...playgroundCommands.map((c) => c.label),
      ...quickActionCommands.map((c) => c.label),
    ],
    [navigationCommands.map, playgroundCommands.map, quickActionCommands.map]
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
        const results = await semanticSearch(searchValue, termiteClient);
        setSemanticResults(results);
      } catch (e) {
        console.error("Semantic search failed:", e);
        setSemanticResults([]);
      }
      setIsSearching(false);
    }, 300);

    return () => clearTimeout(timer);
  }, [searchValue, hasStringMatches, termiteClient]);

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
      } else if (action === "toggle-width") {
        toggleContentWidth();
      } else if (href) {
        navigate(href);
      }
    },
    [navigate, theme, setTheme, toggleContentWidth]
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
                } else if (command.action === "toggle-width") {
                  DynamicIcon = contentWidth === "restricted" ? Maximize2 : Minimize2;
                  DynamicLabel =
                    contentWidth === "restricted"
                      ? "Expand Content Width"
                      : "Restrict Content Width";
                }
              }

              return (
                <CommandItem
                  key={command.action}
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
                key={command.href}
                onSelect={() => handleSelect(command.href)}
                className="flex items-center gap-2 cursor-pointer"
              >
                <command.icon className="h-4 w-4" />
                <span>{command.label}</span>
              </CommandItem>
            ))}
          </CommandGroup>

          <CommandSeparator />

          {/* Playgrounds */}
          <CommandGroup heading="Playgrounds">
            {playgroundCommands.map((command) => (
              <CommandItem
                key={command.href}
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
