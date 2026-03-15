import {
  ArrowUpDown,
  Check,
  ChevronsUpDown,
  ClipboardCheck,
  Database,
  FileInput,
  FileText,
  KeyRound,
  Library,
  MessageCircle,
  MessageSquare,
  Mic,
  Network,
  PanelLeft,
  PanelLeftOpen,
  Repeat2,
  ScanLine,
  Scissors,
  Search,
  Shield,
  Table as TableIcon,
  Tag,
  Upload,
  Waypoints,
  Wrench,
  X,
} from "lucide-react";
import * as React from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { ProductSwitcher } from "@/components/product-switcher";
import { SidebarUser } from "@/components/sidebar-user";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from "@/components/ui/sidebar";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import type { ProductId } from "@/config/products";
import { useAuth } from "@/hooks/use-auth";
import { useTable } from "@/hooks/use-table";
import { cn } from "@/lib/utils";

interface AppSidebarProps extends React.ComponentProps<typeof Sidebar> {
  currentSection?: string;
  onSectionChange?: (section: string) => void;
  currentProduct: ProductId;
  onProductChange: (product: ProductId) => void;
}

export function AppSidebar({
  currentSection,
  onSectionChange,
  currentProduct,
  onProductChange,
  ...props
}: AppSidebarProps) {
  const location = useLocation();
  const navigate = useNavigate();
  const { state: sidebarState, toggleSidebar, isMobile } = useSidebar();
  const { hasPermission } = useAuth();
  const { tables, selectedTable, setSelectedTable } = useTable();

  const [comboboxOpen, setComboboxOpen] = React.useState(false);
  const [tooltipOpen, setTooltipOpen] = React.useState(false);
  const [isMounted, setIsMounted] = React.useState(false);

  React.useEffect(() => {
    setIsMounted(true);
  }, []);

  const handleExpandClick = () => {
    toggleSidebar();
  };

  const handleSectionClick = (section: string) => {
    if (!selectedTable) return;

    // Section-based items: navigate to table page if needed, then set section
    if (!location.pathname.startsWith("/tables/")) {
      navigate(`/tables/${selectedTable}`);
    }
    if (onSectionChange) {
      onSectionChange(section);
    }
  };

  const isOnTablePage = location.pathname.startsWith("/tables/");

  return (
    <Sidebar collapsible="icon" {...props}>
      {/* Hover overlay expand button for collapsed state */}
      {sidebarState === "collapsed" && !isMobile && (
        <div className="absolute top-4 left-2 z-40 opacity-0 hover:opacity-100 transition-opacity duration-200 pointer-events-none">
          <TooltipProvider delayDuration={0}>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  size="icon"
                  className="size-8 bg-sidebar hover:bg-sidebar-accent text-sidebar-foreground hover:text-sidebar-accent-foreground pointer-events-auto cursor-e-resize transition-colors border-0"
                  onClick={handleExpandClick}
                >
                  <PanelLeftOpen className="size-4" />
                  <span className="sr-only">Open sidebar</span>
                </Button>
              </TooltipTrigger>
              <TooltipContent side="right" align="center">
                Open sidebar
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
      )}
      <SidebarHeader className="border-r border-b-0 group-data-[collapsible=icon]:border-r-0 gap-0 pb-0">
        <SidebarMenu>
          <SidebarMenuItem>
            <div className="flex items-center justify-between gap-2">
              <ProductSwitcher
                currentProduct={currentProduct}
                onProductChange={onProductChange}
                collapsed={sidebarState === "collapsed"}
              />
              {isMounted && (isMobile || sidebarState !== "collapsed") && (
                <TooltipProvider delayDuration={0}>
                  <Tooltip open={tooltipOpen} onOpenChange={setTooltipOpen}>
                    <TooltipTrigger asChild>
                      <Button
                        variant="ghost"
                        size="icon"
                        className={cn(
                          "size-7 shrink-0",
                          isMobile ? "cursor-pointer" : "cursor-w-resize"
                        )}
                        onClick={toggleSidebar}
                        onMouseEnter={() => setTooltipOpen(true)}
                        onMouseLeave={() => setTooltipOpen(false)}
                      >
                        {isMobile ? <X className="size-4" /> : <PanelLeft className="size-4" />}
                        <span className="sr-only">
                          {isMobile ? "Close sidebar" : "Collapse sidebar"}
                        </span>
                      </Button>
                    </TooltipTrigger>
                    <TooltipContent side="right" align="center">
                      {isMobile ? "Close sidebar" : "Collapse sidebar"}
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              )}
            </div>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>

      <SidebarContent className="border-r border-t-0 group-data-[collapsible=icon]:border-r-0">
        {/* Antfly Management */}
        {currentProduct === "antfly" && (
          <SidebarGroup>
            <SidebarGroupLabel>Management</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={location.pathname === "/" || location.pathname === "/create"}
                    tooltip="Tables"
                  >
                    <a
                      href="/"
                      onClick={(e) => {
                        e.preventDefault();
                        navigate("/");
                      }}
                    >
                      <TableIcon className="size-4" />
                      <span>Tables</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>

                {/* Users Link - only show if user has admin permission */}
                {hasPermission("*", "*", "admin") && (
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      asChild
                      isActive={location.pathname === "/users"}
                      tooltip="User Management"
                    >
                      <a
                        href="/users"
                        onClick={(e) => {
                          e.preventDefault();
                          navigate("/users");
                        }}
                      >
                        <Shield className="size-4" />
                        <span>Users</span>
                      </a>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                )}

                {/* Secrets Link - only show if user has admin permission */}
                {hasPermission("*", "*", "admin") && (
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      asChild
                      isActive={location.pathname === "/secrets"}
                      tooltip="Secret Management"
                    >
                      <a
                        href="/secrets"
                        onClick={(e) => {
                          e.preventDefault();
                          navigate("/secrets");
                        }}
                      >
                        <KeyRound className="size-4" />
                        <span>Secrets</span>
                      </a>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                )}

                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={location.pathname === "/cluster"}
                    tooltip="Cluster Overview"
                  >
                    <a
                      href="/cluster"
                      onClick={(e) => {
                        e.preventDefault();
                        navigate("/cluster");
                      }}
                    >
                      <Network className="size-4" />
                      <span>Cluster</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        )}

        {/* Antfly Table-Scoped Items - always visible */}
        {currentProduct === "antfly" && (
          <SidebarGroup>
            <SidebarGroupLabel>Table</SidebarGroupLabel>
            <SidebarGroupContent>
              {/* Table Selector */}
              <div className="px-2 pb-2 group-data-[collapsible=icon]:hidden">
                <Popover open={comboboxOpen} onOpenChange={setComboboxOpen}>
                  <PopoverTrigger asChild>
                    <Button
                      variant="outline"
                      role="combobox"
                      aria-expanded={comboboxOpen}
                      className="w-full justify-between"
                      disabled={tables.length === 0}
                    >
                      <span className="truncate">{selectedTable || "Select a table..."}</span>
                      <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                    </Button>
                  </PopoverTrigger>
                  <PopoverContent className="w-[--radix-popover-trigger-width] p-0">
                    <Command>
                      <CommandInput placeholder="Search tables..." />
                      <CommandList>
                        <CommandEmpty>No table found.</CommandEmpty>
                        <CommandGroup>
                          {tables.map((table) => (
                            <CommandItem
                              key={table.name}
                              value={table.name}
                              onSelect={(currentValue) => {
                                setSelectedTable(currentValue);
                                setComboboxOpen(false);
                              }}
                            >
                              <Check
                                className={cn(
                                  "mr-2 h-4 w-4",
                                  selectedTable === table.name ? "opacity-100" : "opacity-0"
                                )}
                              />
                              {table.name}
                            </CommandItem>
                          ))}
                        </CommandGroup>
                      </CommandList>
                    </Command>
                  </PopoverContent>
                </Popover>
              </div>
              <SidebarMenu>
                {/* Configure subgroup */}
                <div className="px-2 py-1.5 text-xs font-medium text-sidebar-foreground/50 group-data-[collapsible=icon]:hidden">
                  Configure
                </div>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    isActive={isOnTablePage && currentSection === "schema"}
                    tooltip="Schema"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                    onClick={() => handleSectionClick("schema")}
                  >
                    <FileText className="size-4" />
                    <span>Schema</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    isActive={isOnTablePage && currentSection === "indexes"}
                    tooltip="Indexes"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                    onClick={() => handleSectionClick("indexes")}
                  >
                    <Database className="size-4" />
                    <span>Indexes</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>

                {/* Ingest subgroup */}
                <div className="px-2 py-1.5 text-xs font-medium text-sidebar-foreground/50 group-data-[collapsible=icon]:hidden">
                  Ingest
                </div>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    isActive={isOnTablePage && currentSection === "bulk"}
                    tooltip="Upload"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                    onClick={() => handleSectionClick("bulk")}
                  >
                    <Upload className="size-4" />
                    <span>Upload</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    isActive={isOnTablePage && currentSection === "document-builder"}
                    tooltip="Document Builder"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                    onClick={() => handleSectionClick("document-builder")}
                  >
                    <FileInput className="size-4" />
                    <span>Document Builder</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>

                {/* Explore subgroup */}
                <div className="px-2 py-1.5 text-xs font-medium text-sidebar-foreground/50 group-data-[collapsible=icon]:hidden">
                  Explore
                </div>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    isActive={isOnTablePage && currentSection === "semantic"}
                    tooltip="Search"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                    onClick={() => handleSectionClick("semantic")}
                  >
                    <Search className="size-4" />
                    <span>Search</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    isActive={isOnTablePage && currentSection === "faceted"}
                    tooltip="Component Builder"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                    onClick={() => handleSectionClick("faceted")}
                  >
                    <FileText className="size-4" />
                    <span>Component Builder</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={location.pathname === "/playground/rag"}
                    tooltip="RAG"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                  >
                    <a
                      href="/playground/rag"
                      onClick={(e) => {
                        e.preventDefault();
                        navigate(
                          selectedTable
                            ? `/playground/rag?table=${encodeURIComponent(selectedTable)}`
                            : "/playground/rag"
                        );
                      }}
                    >
                      <MessageSquare className="size-4" />
                      <span>RAG</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={location.pathname === "/playground/chat"}
                    tooltip="Chat"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                  >
                    <a
                      href="/playground/chat"
                      onClick={(e) => {
                        e.preventDefault();
                        navigate(
                          selectedTable
                            ? `/playground/chat?table=${encodeURIComponent(selectedTable)}`
                            : "/playground/chat"
                        );
                      }}
                    >
                      <MessageCircle className="size-4" />
                      <span>Chat</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={location.pathname === "/playground/evals"}
                    tooltip="Evals"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                  >
                    <a
                      href="/playground/evals"
                      onClick={(e) => {
                        e.preventDefault();
                        navigate(
                          selectedTable
                            ? `/playground/evals?table=${encodeURIComponent(selectedTable)}`
                            : "/playground/evals"
                        );
                      }}
                    >
                      <ClipboardCheck className="size-4" />
                      <span>Evals</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={location.pathname === "/playground/embedding"}
                    tooltip="Embedding"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                  >
                    <a
                      href="/playground/embedding"
                      onClick={(e) => {
                        e.preventDefault();
                        navigate(
                          selectedTable
                            ? `/playground/embedding?table=${encodeURIComponent(selectedTable)}`
                            : "/playground/embedding"
                        );
                      }}
                    >
                      <Waypoints className="size-4" />
                      <span>Embedding</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={location.pathname === "/playground/reranking"}
                    tooltip="Reranking"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                  >
                    <a
                      href="/playground/reranking"
                      onClick={(e) => {
                        e.preventDefault();
                        navigate(
                          selectedTable
                            ? `/playground/reranking?table=${encodeURIComponent(selectedTable)}`
                            : "/playground/reranking"
                        );
                      }}
                    >
                      <ArrowUpDown className="size-4" />
                      <span>Reranking</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={location.pathname === "/playground/chunking"}
                    tooltip="Chunking"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                  >
                    <a
                      href="/playground/chunking"
                      onClick={(e) => {
                        e.preventDefault();
                        navigate(
                          selectedTable
                            ? `/playground/chunking?table=${encodeURIComponent(selectedTable)}`
                            : "/playground/chunking"
                        );
                      }}
                    >
                      <Scissors className="size-4" />
                      <span>Chunking</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        )}

        {/* Termite Tools Section */}
        {currentProduct === "termite" && (
          <SidebarGroup>
            <SidebarGroupLabel>Tools</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                {/* Models Directory */}
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={location.pathname === "/models"}
                    tooltip="Model Directory"
                  >
                    <a
                      href="/models"
                      onClick={(e) => {
                        e.preventDefault();
                        navigate("/models");
                      }}
                    >
                      <Library className="size-4" />
                      <span>Models</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>

                <Collapsible defaultOpen>
                  <SidebarMenuItem>
                    <CollapsibleTrigger asChild>
                      <SidebarMenuButton
                        isActive={
                          location.pathname === "/playground/chunk" ||
                          location.pathname === "/playground/recognize" ||
                          location.pathname === "/playground/rewrite" ||
                          location.pathname === "/playground/rerank" ||
                          location.pathname === "/playground/kg" ||
                          location.pathname === "/playground/embed" ||
                          location.pathname === "/playground/read" ||
                          location.pathname === "/playground/transcribe"
                        }
                        tooltip="Playgrounds"
                      >
                        <Wrench className="size-4" />
                        <span>Playgrounds</span>
                      </SidebarMenuButton>
                    </CollapsibleTrigger>
                    <CollapsibleContent>
                      <SidebarMenu className="ml-4 border-l pl-2">
                        <SidebarMenuItem>
                          <SidebarMenuButton
                            asChild
                            isActive={location.pathname === "/playground/chunk"}
                          >
                            <a
                              href="/playground/chunk"
                              onClick={(e) => {
                                e.preventDefault();
                                navigate("/playground/chunk");
                              }}
                            >
                              <Scissors className="size-4" />
                              <span>Chunking</span>
                            </a>
                          </SidebarMenuButton>
                        </SidebarMenuItem>
                        <SidebarMenuItem>
                          <SidebarMenuButton
                            asChild
                            isActive={location.pathname === "/playground/recognize"}
                          >
                            <a
                              href="/playground/recognize"
                              onClick={(e) => {
                                e.preventDefault();
                                navigate("/playground/recognize");
                              }}
                            >
                              <Tag className="size-4" />
                              <span>Recognize</span>
                            </a>
                          </SidebarMenuButton>
                        </SidebarMenuItem>
                        <SidebarMenuItem>
                          <SidebarMenuButton
                            asChild
                            isActive={location.pathname === "/playground/rewrite"}
                          >
                            <a
                              href="/playground/rewrite"
                              onClick={(e) => {
                                e.preventDefault();
                                navigate("/playground/rewrite");
                              }}
                            >
                              <Repeat2 className="size-4" />
                              <span>Rewriting</span>
                            </a>
                          </SidebarMenuButton>
                        </SidebarMenuItem>
                        <SidebarMenuItem>
                          <SidebarMenuButton
                            asChild
                            isActive={location.pathname === "/playground/rerank"}
                          >
                            <a
                              href="/playground/rerank"
                              onClick={(e) => {
                                e.preventDefault();
                                navigate("/playground/rerank");
                              }}
                            >
                              <ArrowUpDown className="size-4" />
                              <span>Reranking</span>
                            </a>
                          </SidebarMenuButton>
                        </SidebarMenuItem>
                        <SidebarMenuItem>
                          <SidebarMenuButton
                            asChild
                            isActive={location.pathname === "/playground/kg"}
                          >
                            <a
                              href="/playground/kg"
                              onClick={(e) => {
                                e.preventDefault();
                                navigate("/playground/kg");
                              }}
                            >
                              <Network className="size-4" />
                              <span>Knowledge Graph</span>
                            </a>
                          </SidebarMenuButton>
                        </SidebarMenuItem>
                        <SidebarMenuItem>
                          <SidebarMenuButton
                            asChild
                            isActive={location.pathname === "/playground/embed"}
                          >
                            <a
                              href="/playground/embed"
                              onClick={(e) => {
                                e.preventDefault();
                                navigate("/playground/embed");
                              }}
                            >
                              <Waypoints className="size-4" />
                              <span>Embedding</span>
                            </a>
                          </SidebarMenuButton>
                        </SidebarMenuItem>
                        <SidebarMenuItem>
                          <SidebarMenuButton
                            asChild
                            isActive={location.pathname === "/playground/read"}
                          >
                            <a
                              href="/playground/read"
                              onClick={(e) => {
                                e.preventDefault();
                                navigate("/playground/read");
                              }}
                            >
                              <ScanLine className="size-4" />
                              <span>Reader</span>
                            </a>
                          </SidebarMenuButton>
                        </SidebarMenuItem>
                        <SidebarMenuItem>
                          <SidebarMenuButton
                            asChild
                            isActive={location.pathname === "/playground/transcribe"}
                          >
                            <a
                              href="/playground/transcribe"
                              onClick={(e) => {
                                e.preventDefault();
                                navigate("/playground/transcribe");
                              }}
                            >
                              <Mic className="size-4" />
                              <span>Transcribe</span>
                            </a>
                          </SidebarMenuButton>
                        </SidebarMenuItem>
                      </SidebarMenu>
                    </CollapsibleContent>
                  </SidebarMenuItem>
                </Collapsible>
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        )}
      </SidebarContent>

      <SidebarFooter className="border-r group-data-[collapsible=icon]:border-r-0">
        <SidebarUser />
      </SidebarFooter>
    </Sidebar>
  );
}
