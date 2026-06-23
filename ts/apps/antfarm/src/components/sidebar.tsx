import {
  Anty,
  Button,
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
  Switcher,
  SwitcherContent,
  SwitcherFooter,
  SwitcherItem,
  SwitcherTrigger,
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
  useSidebar,
} from "@antfly/design-system";
import {
  ArrowUpDown,
  Bot,
  ClipboardCheck,
  Database,
  FileInput,
  FileStack,
  KeyRound,
  Library,
  Mic,
  Network,
  PanelLeft,
  Plug,
  Plus,
  Repeat2,
  RotateCw,
  ScanLine,
  Scissors,
  Search,
  Shield,
  Table as TableIcon,
  Tag,
  Upload,
  Waypoints,
  X,
} from "lucide-react";
import * as React from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { SidebarUser } from "@/components/sidebar-user";
import { isProductEnabled } from "@/config/products";
import { useAuth } from "@/hooks/use-auth";
import { useTable } from "@/hooks/use-table";
import { cn } from "@/lib/utils";
import { isExternalAuthMode } from "@/runtime-config";

interface AppSidebarProps extends React.ComponentProps<typeof Sidebar> {
  currentSection?: string;
  onSectionChange?: (section: string) => void;
}

function tableRoute(table: string, section: string) {
  return `/tables/${encodeURIComponent(table)}?section=${encodeURIComponent(section)}`;
}

export function AppSidebar({ currentSection, onSectionChange, ...props }: AppSidebarProps) {
  const location = useLocation();
  const navigate = useNavigate();
  const { state: sidebarState, toggleSidebar, isMobile } = useSidebar();
  const { hasPermission } = useAuth();
  const { tables, selectedTable, setSelectedTable, graphIndexes, isLoadingIndexes } = useTable();
  const showLocalAdminRoutes = !isExternalAuthMode();
  const showAdmin = showLocalAdminRoutes && hasPermission("*", "*", "admin");

  const [comboboxOpen, setComboboxOpen] = React.useState(false);
  const [tooltipOpen, setTooltipOpen] = React.useState(false);
  const [isMounted, setIsMounted] = React.useState(false);

  React.useEffect(() => {
    setIsMounted(true);
  }, []);

  const navigateTableSection = (section: string) => {
    if (!selectedTable) return;
    onSectionChange?.(section);
    navigate(tableRoute(selectedTable, section));
  };

  const navigateSelectedTableRoute = (route: string) => {
    const suffix = selectedTable ? `?table=${encodeURIComponent(selectedTable)}` : "";
    navigate(`${route}${suffix}`);
  };

  const isOnTablePage = location.pathname.startsWith("/tables/");
  const isTableSection = (section: string) => isOnTablePage && currentSection === section;
  const isPath = (path: string) => location.pathname === path;

  return (
    <Sidebar collapsible="icon" {...props}>
      <SidebarHeader className="border-r border-b-0 group-data-[collapsible=icon]:border-r-0 gap-0 pb-0">
        <SidebarMenu>
          <SidebarMenuItem>
            <div className="flex items-center justify-between gap-2">
              {sidebarState === "collapsed" && !isMobile ? (
                <TooltipProvider delayDuration={0}>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <SidebarMenuButton
                        size="lg"
                        className="cursor-e-resize"
                        onClick={toggleSidebar}
                      >
                        <div
                          className="flex items-center justify-center"
                          style={{ minWidth: 32, height: 32 }}
                        >
                          <Anty
                            size={32}
                            eyeStyle="original"
                            float={false}
                            showShadow={false}
                            showGlow
                            style={{ height: 32 }}
                          />
                        </div>
                      </SidebarMenuButton>
                    </TooltipTrigger>
                    <TooltipContent side="right" align="center">
                      Open sidebar
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              ) : (
                <SidebarMenuButton size="lg" className="cursor-default">
                  <div className="flex h-8 min-w-8 items-center justify-center">
                    <Anty
                      size={24}
                      eyeStyle="original"
                      float={false}
                      showShadow={false}
                      showGlow
                      style={{ height: 24 }}
                    />
                  </div>
                  <div className="flex min-w-0 flex-1 items-center text-left text-sm leading-tight">
                    <span className="truncate font-semibold">Antfarm</span>
                  </div>
                </SidebarMenuButton>
              )}
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
        {isProductEnabled("antfly") && (
          <>
            <SidebarGroup>
              <SidebarGroupLabel className="mono-label">Data</SidebarGroupLabel>
              <SidebarGroupContent>
                <SidebarMenu>
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      asChild
                      isActive={isPath("/") || isPath("/create") || isTableSection("overview")}
                      tooltip="Tables"
                    >
                      <a
                        href="/"
                        onClick={(event) => {
                          event.preventDefault();
                          onSectionChange?.("overview");
                          navigate("/");
                        }}
                      >
                        <TableIcon className="size-4" />
                        <span>Tables</span>
                      </a>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                  <SidebarMenuItem>
                    <SidebarMenuButton asChild isActive={isPath("/sql")} tooltip="SQL">
                      <a
                        href="/sql"
                        onClick={(event) => {
                          event.preventDefault();
                          navigateSelectedTableRoute("/sql");
                        }}
                      >
                        <Database className="size-4" />
                        <span>SQL</span>
                      </a>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                </SidebarMenu>

                <div className="px-2 pt-2 pb-1 group-data-[collapsible=icon]:hidden">
                  <Switcher open={comboboxOpen} onOpenChange={setComboboxOpen}>
                    <SwitcherTrigger disabled={tables.length === 0} placeholder="Select a table...">
                      {selectedTable}
                    </SwitcherTrigger>
                    <SwitcherContent
                      searchPlaceholder="Search tables..."
                      emptyMessage="No table found."
                      heading="Tables"
                      footer={
                        <SwitcherFooter onClick={() => navigate("/create")}>
                          <Plus className="size-4" />
                          <span>Create table</span>
                        </SwitcherFooter>
                      }
                    >
                      {tables.map((table) => (
                        <SwitcherItem
                          key={table.name}
                          value={table.name}
                          selected={selectedTable === table.name}
                          onSelect={(value) => {
                            setSelectedTable(value);
                            onSectionChange?.("overview");
                            navigate(tableRoute(value, "overview"));
                          }}
                        >
                          {table.name}
                        </SwitcherItem>
                      ))}
                    </SwitcherContent>
                  </Switcher>
                </div>
              </SidebarGroupContent>
            </SidebarGroup>

            <SidebarGroup>
              <SidebarGroupLabel className="mono-label">Retrieval</SidebarGroupLabel>
              <SidebarGroupContent>
                <SidebarMenu>
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      isActive={isTableSection("semantic")}
                      tooltip="Search"
                      disabled={!selectedTable}
                      className="disabled:opacity-50"
                      onClick={() => navigateTableSection("semantic")}
                    >
                      <Search className="size-4" />
                      <span>Search</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      isActive={isPath("/data/playground/chat")}
                      tooltip="Ask"
                      disabled={!selectedTable}
                      className="disabled:opacity-50"
                      onClick={() => navigateSelectedTableRoute("/data/playground/chat")}
                    >
                      <Bot className="size-4" />
                      <span>Ask</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      isActive={isPath("/data/playground/evals")}
                      tooltip="Evaluate"
                      disabled={!selectedTable}
                      className="disabled:opacity-50"
                      onClick={() => navigateSelectedTableRoute("/data/playground/evals")}
                    >
                      <ClipboardCheck className="size-4" />
                      <span>Evaluate</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      isActive={isTableSection("graph")}
                      tooltip={graphIndexes.length > 0 ? "Graph" : "Create a graph index first"}
                      disabled={!selectedTable || (!isLoadingIndexes && graphIndexes.length === 0)}
                      className="disabled:opacity-50"
                      onClick={() => navigateTableSection("graph")}
                    >
                      <Network className="size-4" />
                      <span>Graph</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                </SidebarMenu>
              </SidebarGroupContent>
            </SidebarGroup>

            <SidebarGroup>
              <SidebarGroupLabel className="mono-label">Ingest</SidebarGroupLabel>
              <SidebarGroupContent>
                <SidebarMenu>
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      isActive={isTableSection("bulk")}
                      tooltip="Upload"
                      disabled={!selectedTable}
                      className="disabled:opacity-50"
                      onClick={() => navigateTableSection("bulk")}
                    >
                      <Upload className="size-4" />
                      <span>Upload</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      isActive={isTableSection("document-builder")}
                      tooltip="Manual Entry"
                      disabled={!selectedTable}
                      className="disabled:opacity-50"
                      onClick={() => navigateTableSection("document-builder")}
                    >
                      <FileInput className="size-4" />
                      <span>Manual Entry</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      isActive={isTableSection("artifacts")}
                      tooltip="Artifacts"
                      disabled={!selectedTable}
                      className="disabled:opacity-50"
                      onClick={() => navigateTableSection("artifacts")}
                    >
                      <FileStack className="size-4" />
                      <span>Artifacts</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      isActive={isTableSection("reprocess")}
                      tooltip="Reprocess"
                      disabled={!selectedTable}
                      className="disabled:opacity-50"
                      onClick={() => navigateTableSection("reprocess")}
                    >
                      <RotateCw className="size-4" />
                      <span>Reprocess</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                </SidebarMenu>
              </SidebarGroupContent>
            </SidebarGroup>
          </>
        )}

        {isProductEnabled("inference") && (
          <SidebarGroup>
            <SidebarGroupLabel className="mono-label">Models</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={isPath("/inference/models")}
                    tooltip="Runtime"
                  >
                    <a
                      href="/inference/models"
                      onClick={(event) => {
                        event.preventDefault();
                        navigate("/inference/models");
                      }}
                    >
                      <Library className="size-4" />
                      <span>Runtime</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>

                <div className="px-2 py-1.5 mono-label text-sidebar-foreground/50 group-data-[collapsible=icon]:hidden">
                  Tools
                </div>

                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={isPath("/inference/playground/chunk")}
                    tooltip="Chunk Text"
                  >
                    <a
                      href="/inference/playground/chunk"
                      onClick={(event) => {
                        event.preventDefault();
                        navigate("/inference/playground/chunk");
                      }}
                    >
                      <Scissors className="size-4" />
                      <span>Chunk Text</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={isPath("/inference/playground/extract")}
                    tooltip="Extract"
                  >
                    <a
                      href="/inference/playground/extract"
                      onClick={(event) => {
                        event.preventDefault();
                        navigate("/inference/playground/extract");
                      }}
                    >
                      <Tag className="size-4" />
                      <span>Extract</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={isPath("/inference/playground/rewrite")}
                    tooltip="Rewrite"
                  >
                    <a
                      href="/inference/playground/rewrite"
                      onClick={(event) => {
                        event.preventDefault();
                        navigate("/inference/playground/rewrite");
                      }}
                    >
                      <Repeat2 className="size-4" />
                      <span>Rewrite</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={isPath("/inference/playground/rerank")}
                    tooltip="Rerank"
                  >
                    <a
                      href="/inference/playground/rerank"
                      onClick={(event) => {
                        event.preventDefault();
                        navigate("/inference/playground/rerank");
                      }}
                    >
                      <ArrowUpDown className="size-4" />
                      <span>Rerank</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={isPath("/inference/playground/kg")}
                    tooltip="Knowledge Graph"
                  >
                    <a
                      href="/inference/playground/kg"
                      onClick={(event) => {
                        event.preventDefault();
                        navigate("/inference/playground/kg");
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
                    isActive={isPath("/inference/playground/embed")}
                    tooltip="Embed"
                  >
                    <a
                      href="/inference/playground/embed"
                      onClick={(event) => {
                        event.preventDefault();
                        navigate("/inference/playground/embed");
                      }}
                    >
                      <Waypoints className="size-4" />
                      <span>Embed</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={isPath("/inference/playground/read")}
                    tooltip="Read Image"
                  >
                    <a
                      href="/inference/playground/read"
                      onClick={(event) => {
                        event.preventDefault();
                        navigate("/inference/playground/read");
                      }}
                    >
                      <ScanLine className="size-4" />
                      <span>Read Image</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={isPath("/inference/playground/transcribe")}
                    tooltip="Transcribe"
                  >
                    <a
                      href="/inference/playground/transcribe"
                      onClick={(event) => {
                        event.preventDefault();
                        navigate("/inference/playground/transcribe");
                      }}
                    >
                      <Mic className="size-4" />
                      <span>Transcribe</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        )}

        {isProductEnabled("antfly") && (
          <SidebarGroup>
            <SidebarGroupLabel className="mono-label">Operations</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                <SidebarMenuItem>
                  <SidebarMenuButton asChild isActive={isPath("/cluster")} tooltip="Cluster">
                    <a
                      href="/cluster"
                      onClick={(event) => {
                        event.preventDefault();
                        navigate("/cluster");
                      }}
                    >
                      <Network className="size-4" />
                      <span>Cluster</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>

                {showAdmin && (
                  <>
                    <SidebarMenuItem>
                      <SidebarMenuButton asChild isActive={isPath("/users")} tooltip="Users">
                        <a
                          href="/users"
                          onClick={(event) => {
                            event.preventDefault();
                            navigate("/users");
                          }}
                        >
                          <Shield className="size-4" />
                          <span>Users</span>
                        </a>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                    <SidebarMenuItem>
                      <SidebarMenuButton asChild isActive={isPath("/secrets")} tooltip="Secrets">
                        <a
                          href="/secrets"
                          onClick={(event) => {
                            event.preventDefault();
                            navigate("/secrets");
                          }}
                        >
                          <KeyRound className="size-4" />
                          <span>Secrets</span>
                        </a>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  </>
                )}

                <SidebarMenuItem>
                  <SidebarMenuButton
                    asChild
                    isActive={location.pathname === "/connections"}
                    tooltip="Connected Providers & Stores"
                  >
                    <a
                      href="/connections"
                      onClick={(e) => {
                        e.preventDefault();
                        navigate("/connections");
                      }}
                    >
                      <Plug className="size-4" />
                      <span>Connections</span>
                    </a>
                  </SidebarMenuButton>
                </SidebarMenuItem>
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
