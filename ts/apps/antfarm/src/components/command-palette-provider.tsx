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
import { InferenceClient } from "@antfly/sdk";
import {
  ArrowUpDown,
  Bot,
  ClipboardCheck,
  FileInput,
  HelpCircle,
  KeyRound,
  Library,
  Loader2,
  Mic,
  Moon,
  Network,
  Plus,
  Repeat2,
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
import { useAuth } from "@/hooks/use-auth";
import { useApiConfig } from "@/hooks/use-api-config";
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
  Shield,
  KeyRound,
  Scissors,
  Tag,
  HelpCircle,
  Network,
  ClipboardCheck,
  Bot,
  Search,
  Upload,
  Waypoints,
  ArrowUpDown,
  Repeat2,
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
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  href?: string;
  action?: string;
  product?: ProductId;
  adminOnly?: boolean;
}

function productForHref(href?: string): ProductId | undefined {
  if (!href) return undefined;
  if (href.startsWith("/inference")) return "inference";
  if (
    href === "/" ||
    href.startsWith("/create") ||
    href.startsWith("/tables") ||
    href.startsWith("/retrieval") ||
    href.startsWith("/ingest") ||
    href.startsWith("/data/playground") ||
    href === "/cluster" ||
    href === "/users" ||
    href === "/secrets"
  ) {
    return "antfly";
  }
  return undefined;
}

function isAdminHref(href?: string) {
  return href === "/users" || href === "/secrets";
}

export function CommandPaletteProvider({ children }: { children: React.ReactNode }) {
  const [isOpen, setIsOpen] = React.useState(false);
  const [mounted, setMounted] = React.useState(false);
  const [searchValue, setSearchValue] = React.useState("");
  const [semanticResults, setSemanticResults] = React.useState<SemanticResult[]>([]);
  const [isSearching, setIsSearching] = React.useState(false);
  const navigate = useNavigate();

  const { hasPermission } = useAuth();
  const { theme, setTheme } = useTheme();
  const { inferenceApiUrl } = useApiConfig();
  const showLocalAdminRoutes = !isExternalAuthMode();
  const showAdmin = showLocalAdminRoutes && hasPermission("*", "*", "admin");

  // Create InferenceClient for semantic search
  const inferenceClient = React.useMemo(
    () => new InferenceClient({ baseUrl: inferenceApiUrl }),
    [inferenceApiUrl]
  );

  const isCommandAvailable = React.useCallback(
    (item: { href?: string; product?: ProductId; adminOnly?: boolean }) => {
      const product = item.product ?? productForHref(item.href);
      if (product && !isProductEnabled(product)) {
        return false;
      }
      if ((item.adminOnly || isAdminHref(item.href)) && !showAdmin) {
        return false;
      }
      return true;
    },
    [showAdmin]
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
    const commands: PaletteCommand[] = [
      { icon: Table, label: "Tables", href: "/", product: "antfly" },
      { icon: Plus, label: "Create Table", href: "/create", product: "antfly" },
      { icon: Search, label: "Search", href: "/retrieval/search", product: "antfly" },
      { icon: Bot, label: "Ask Questions", href: "/retrieval/ask", product: "antfly" },
      {
        icon: ClipboardCheck,
        label: "Evaluate Retrieval",
        href: "/retrieval/evaluate",
        product: "antfly",
      },
      { icon: Upload, label: "Upload Data", href: "/ingest/upload", product: "antfly" },
      { icon: Library, label: "Models", href: "/inference/models", product: "inference" },
      { icon: Network, label: "Cluster", href: "/cluster", product: "antfly" },
      { icon: Users, label: "Users", href: "/users", product: "antfly", adminOnly: true },
      { icon: KeyRound, label: "Secrets", href: "/secrets", product: "antfly", adminOnly: true },
    ];
    return commands.filter(isCommandAvailable);
  }, [isCommandAvailable]);

  const toolCommands = React.useMemo(() => {
    const commands: PaletteCommand[] = [
      { icon: Network, label: "Graph Retrieval", href: "/retrieval/graph", product: "antfly" },
      {
        icon: Scissors,
        label: "Chunk Text",
        href: "/inference/playground/chunk",
        product: "inference",
      },
      {
        icon: Tag,
        label: "Extract Structured Data",
        href: "/inference/playground/extract",
        product: "inference",
      },
      {
        icon: Repeat2,
        label: "Rewrite Text",
        href: "/inference/playground/rewrite",
        product: "inference",
      },
      {
        icon: ArrowUpDown,
        label: "Rerank Text",
        href: "/inference/playground/rerank",
        product: "inference",
      },
      {
        icon: Waypoints,
        label: "Embed Text",
        href: "/inference/playground/embed",
        product: "inference",
      },
      {
        icon: ScanLine,
        label: "Read Image",
        href: "/inference/playground/read",
        product: "inference",
      },
      {
        icon: Mic,
        label: "Transcribe Audio",
        href: "/inference/playground/transcribe",
        product: "inference",
      },
      { icon: FileInput, label: "Manual Entry", href: "/ingest/manual", product: "antfly" },
    ];
    return commands.filter(isCommandAvailable);
  }, [isCommandAvailable]);

  const quickActionCommands = React.useMemo(
    () => [{ icon: Moon, label: "Toggle Theme", action: "toggle-theme" }],
    []
  );

  // All command items for string matching check
  const allItems = React.useMemo(
    () => [
      ...navigationCommands.map((c) => c.label),
      ...toolCommands.map((c) => c.label),
      ...quickActionCommands.map((c) => c.label),
    ],
    [navigationCommands, toolCommands, quickActionCommands]
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
        const results = await semanticSearch(searchValue, inferenceClient);
        const filteredResults = results.filter((result) => isCommandAvailable(result.item));
        setSemanticResults(filteredResults);
      } catch (e) {
        console.error("Semantic search failed:", e);
        setSemanticResults([]);
      }
      setIsSearching(false);
    }, 300);

    return () => clearTimeout(timer);
  }, [searchValue, hasStringMatches, inferenceClient, isCommandAvailable]);

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

          {/* Tools */}
          <CommandGroup heading="Tools">
            {toolCommands.map((command) => (
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
