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
  Plus,
  Repeat2,
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
import { ProductSwitcher } from "@/components/product-switcher";
import { SidebarUser } from "@/components/sidebar-user";
import type { ProductId } from "@/config/products";
import { useAuth } from "@/hooks/use-auth";
import { useTable } from "@/hooks/use-table";
import { cn } from "@/lib/utils";
import { isExternalAuthMode } from "@/runtime-config";

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
  const showLocalAdminRoutes = !isExternalAuthMode();

  const [comboboxOpen, setComboboxOpen] = React.useState(false);
  const [tooltipOpen, setTooltipOpen] = React.useState(false);
  const [isMounted, setIsMounted] = React.useState(false);

  React.useEffect(() => {
    setIsMounted(true);
  }, []);

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
                <ProductSwitcher
                  currentProduct={currentProduct}
                  onProductChange={onProductChange}
                />
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
        {/* Antfly Management */}
        {currentProduct === "antfly" && (
          <SidebarGroup>
            <SidebarGroupLabel className="mono-label">Management</SidebarGroupLabel>
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
                {showLocalAdminRoutes && hasPermission("*", "*", "admin") && (
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
                {showLocalAdminRoutes && hasPermission("*", "*", "admin") && (
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
            <SidebarGroupLabel className="mono-label">Table</SidebarGroupLabel>
            <SidebarGroupContent>
              {/* Table Selector */}
              <div className="px-2 pb-2 group-data-[collapsible=icon]:hidden">
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
                        onSelect={(value) => setSelectedTable(value)}
                      >
                        {table.name}
                      </SwitcherItem>
                    ))}
                  </SwitcherContent>
                </Switcher>
              </div>
              <SidebarMenu>
                {/* Configure subgroup */}
                <div className="px-2 py-1.5 mono-label text-sidebar-foreground/50 group-data-[collapsible=icon]:hidden">
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
                <div className="px-2 py-1.5 mono-label text-sidebar-foreground/50 group-data-[collapsible=icon]:hidden">
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
                <div className="px-2 py-1.5 mono-label text-sidebar-foreground/50 group-data-[collapsible=icon]:hidden">
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
                    isActive={location.pathname === "/data/playground/rag"}
                    tooltip="RAG"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                  >
                    <a
                      href="/data/playground/rag"
                      onClick={(e) => {
                        e.preventDefault();
                        navigate(
                          selectedTable
                            ? `/data/playground/rag?table=${encodeURIComponent(selectedTable)}`
                            : "/data/playground/rag"
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
                    isActive={location.pathname === "/data/playground/chat"}
                    tooltip="Chat"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                  >
                    <a
                      href="/data/playground/chat"
                      onClick={(e) => {
                        e.preventDefault();
                        navigate(
                          selectedTable
                            ? `/data/playground/chat?table=${encodeURIComponent(selectedTable)}`
                            : "/data/playground/chat"
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
                    isActive={location.pathname === "/data/playground/evals"}
                    tooltip="Evals"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                  >
                    <a
                      href="/data/playground/evals"
                      onClick={(e) => {
                        e.preventDefault();
                        navigate(
                          selectedTable
                            ? `/data/playground/evals?table=${encodeURIComponent(selectedTable)}`
                            : "/data/playground/evals"
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
                    isActive={location.pathname === "/data/playground/embed"}
                    tooltip="Embedding"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                  >
                    <a
                      href="/data/playground/embed"
                      onClick={(e) => {
                        e.preventDefault();
                        navigate(
                          selectedTable
                            ? `/data/playground/embed?table=${encodeURIComponent(selectedTable)}`
                            : "/data/playground/embed"
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
                    isActive={location.pathname === "/data/playground/rerank"}
                    tooltip="Reranking"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                  >
                    <a
                      href="/data/playground/rerank"
                      onClick={(e) => {
                        e.preventDefault();
                        navigate(
                          selectedTable
                            ? `/data/playground/rerank?table=${encodeURIComponent(selectedTable)}`
                            : "/data/playground/rerank"
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
                    isActive={location.pathname === "/data/playground/chunk"}
                    tooltip="Chunking"
                    disabled={!selectedTable}
                    className="disabled:opacity-50"
                  >
                    <a
                      href="/data/playground/chunk"
                      onClick={(e) => {
                        e.preventDefault();
                        navigate(
                          selectedTable
                            ? `/data/playground/chunk?table=${encodeURIComponent(selectedTable)}`
                            : "/data/playground/chunk"
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

        {/* Inference Management */}
        {currentProduct === "inference" && (
          <>
            <SidebarGroup>
              <SidebarGroupLabel className="mono-label">Management</SidebarGroupLabel>
              <SidebarGroupContent>
                <SidebarMenu>
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      asChild
                      isActive={location.pathname === "/inference/models"}
                      tooltip="Models & Runtime"
                    >
                      <a
                        href="/inference/models"
                        onClick={(e) => {
                          e.preventDefault();
                          navigate("/inference/models");
                        }}
                      >
                        <Library className="size-4" />
                        <span>Models & Runtime</span>
                      </a>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                </SidebarMenu>
              </SidebarGroupContent>
            </SidebarGroup>

            <SidebarGroup>
              <SidebarGroupLabel className="mono-label">Playgrounds</SidebarGroupLabel>
              <SidebarGroupContent>
                <SidebarMenu>
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      asChild
                      isActive={location.pathname === "/inference/playground/chunk"}
                      tooltip="Chunking"
                    >
                      <a
                        href="/inference/playground/chunk"
                        onClick={(e) => {
                          e.preventDefault();
                          navigate("/inference/playground/chunk");
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
                      isActive={location.pathname === "/inference/playground/extract"}
                      tooltip="Extraction"
                    >
                      <a
                        href="/inference/playground/extract"
                        onClick={(e) => {
                          e.preventDefault();
                          navigate("/inference/playground/extract");
                        }}
                      >
                        <Tag className="size-4" />
                        <span>Extraction</span>
                      </a>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                  <SidebarMenuItem>
                    <SidebarMenuButton
                      asChild
                      isActive={location.pathname === "/inference/playground/rewrite"}
                      tooltip="Rewriting"
                    >
                      <a
                        href="/inference/playground/rewrite"
                        onClick={(e) => {
                          e.preventDefault();
                          navigate("/inference/playground/rewrite");
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
                      isActive={location.pathname === "/inference/playground/rerank"}
                      tooltip="Reranking"
                    >
                      <a
                        href="/inference/playground/rerank"
                        onClick={(e) => {
                          e.preventDefault();
                          navigate("/inference/playground/rerank");
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
                      isActive={location.pathname === "/inference/playground/kg"}
                      tooltip="Knowledge Graph"
                    >
                      <a
                        href="/inference/playground/kg"
                        onClick={(e) => {
                          e.preventDefault();
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
                      isActive={location.pathname === "/inference/playground/embed"}
                      tooltip="Embedding"
                    >
                      <a
                        href="/inference/playground/embed"
                        onClick={(e) => {
                          e.preventDefault();
                          navigate("/inference/playground/embed");
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
                      isActive={location.pathname === "/inference/playground/read"}
                      tooltip="Reader"
                    >
                      <a
                        href="/inference/playground/read"
                        onClick={(e) => {
                          e.preventDefault();
                          navigate("/inference/playground/read");
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
                      isActive={location.pathname === "/inference/playground/transcribe"}
                      tooltip="Transcribe"
                    >
                      <a
                        href="/inference/playground/transcribe"
                        onClick={(e) => {
                          e.preventDefault();
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
          </>
        )}
      </SidebarContent>

      <SidebarFooter className="border-r group-data-[collapsible=icon]:border-r-0">
        <SidebarUser />
      </SidebarFooter>
    </Sidebar>
  );
}
