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
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
  useSidebar,
} from "@antfly/design-system";
import {
  Database,
  FileText,
  FlaskConical,
  Home,
  KeyRound,
  Library,
  Network,
  PanelLeft,
  Plus,
  Shield,
  Table as TableIcon,
  Upload,
  X,
} from "lucide-react";
import * as React from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { SidebarUser } from "@/components/sidebar-user";
import { useAuth } from "@/hooks/use-auth";
import { useTable } from "@/hooks/use-table";
import { cn } from "@/lib/utils";
import { isExternalAuthMode } from "@/runtime-config";

interface AppSidebarProps extends React.ComponentProps<typeof Sidebar> {
  currentSection?: string;
  onSectionChange?: (section: string) => void;
}

function NavLink({
  href,
  label,
  icon: Icon,
  active,
  disabled,
  tooltip,
}: {
  href: string;
  label: string;
  icon: React.ComponentType<{ className?: string }>;
  active?: boolean;
  disabled?: boolean;
  tooltip?: string;
}) {
  const navigate = useNavigate();
  return (
    <SidebarMenuItem>
      <SidebarMenuButton
        asChild={!disabled}
        isActive={active}
        tooltip={tooltip ?? label}
        disabled={disabled}
        className="disabled:opacity-50"
      >
        {disabled ? (
          <>
            <Icon className="size-4" />
            <span>{label}</span>
          </>
        ) : (
          <a
            href={href}
            onClick={(event) => {
              event.preventDefault();
              navigate(href);
            }}
          >
            <Icon className="size-4" />
            <span>{label}</span>
          </a>
        )}
      </SidebarMenuButton>
    </SidebarMenuItem>
  );
}

export function AppSidebar({ currentSection, onSectionChange, ...props }: AppSidebarProps) {
  const location = useLocation();
  const navigate = useNavigate();
  const { state: sidebarState, toggleSidebar, isMobile } = useSidebar();
  const { hasPermission } = useAuth();
  const { tables, selectedTable, setSelectedTable } = useTable();
  const showLocalAdminRoutes = !isExternalAuthMode();

  const [tooltipOpen, setTooltipOpen] = React.useState(false);
  const [isMounted, setIsMounted] = React.useState(false);

  React.useEffect(() => {
    setIsMounted(true);
  }, []);

  const isOnTablePage = location.pathname.startsWith("/tables/");

  const handleSectionClick = (section: string) => {
    if (!selectedTable) return;
    if (!isOnTablePage) {
      navigate(`/tables/${selectedTable}`);
    }
    onSectionChange?.(section);
  };

  const selectedTableExists = tables.some((table) => table.name === selectedTable);
  const tableScopedDisabled = !selectedTable || !selectedTableExists;

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
                <div className="flex min-h-12 items-center gap-3 px-2">
                  <Anty
                    size={32}
                    eyeStyle="original"
                    float={false}
                    showShadow={false}
                    showGlow
                    style={{ height: 32 }}
                  />
                  <div className="min-w-0 group-data-[collapsible=icon]:hidden">
                    <div className="truncate font-aeonik font-semibold">Antfly</div>
                    <div className="truncate text-muted-foreground text-xs">
                      Instance Management
                    </div>
                  </div>
                </div>
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
        <SidebarGroup>
          <SidebarGroupLabel className="mono-label">Home</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              <NavLink href="/" label="Home" icon={Home} active={location.pathname === "/"} />
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarGroup>
          <SidebarGroupLabel className="mono-label">Data</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              <NavLink
                href="/tables"
                label="Tables"
                icon={TableIcon}
                active={location.pathname === "/tables" || location.pathname === "/create"}
              />
              <SidebarMenuItem>
                <div className="px-2 pb-2 group-data-[collapsible=icon]:hidden">
                  <select
                    className="h-9 w-full rounded-md border bg-background px-2 text-sm disabled:opacity-50"
                    value={selectedTableExists ? selectedTable : ""}
                    disabled={tables.length === 0}
                    onChange={(event) => {
                      const tableName = event.target.value;
                      if (!tableName) return;
                      setSelectedTable(tableName);
                      navigate(`/tables/${tableName}`);
                    }}
                  >
                    <option value="">Select a table...</option>
                    {tables.map((table) => (
                      <option key={table.name} value={table.name}>
                        {table.name}
                      </option>
                    ))}
                  </select>
                </div>
              </SidebarMenuItem>
              <SidebarMenuItem>
                <SidebarMenuButton
                  isActive={isOnTablePage && currentSection === "schema"}
                  tooltip="Schema"
                  disabled={tableScopedDisabled}
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
                  disabled={tableScopedDisabled}
                  className="disabled:opacity-50"
                  onClick={() => handleSectionClick("indexes")}
                >
                  <Database className="size-4" />
                  <span>Indexes</span>
                </SidebarMenuButton>
              </SidebarMenuItem>
              <SidebarMenuItem>
                <SidebarMenuButton
                  isActive={isOnTablePage && currentSection === "bulk"}
                  tooltip="Upload"
                  disabled={tableScopedDisabled}
                  className="disabled:opacity-50"
                  onClick={() => handleSectionClick("bulk")}
                >
                  <Upload className="size-4" />
                  <span>Upload</span>
                </SidebarMenuButton>
              </SidebarMenuItem>
              <SidebarMenuItem>
                <SidebarMenuButton
                  isActive={isOnTablePage && currentSection === "graph"}
                  tooltip="Graph Explorer"
                  disabled={tableScopedDisabled}
                  className="disabled:opacity-50"
                  onClick={() => handleSectionClick("graph")}
                >
                  <Network className="size-4" />
                  <span>Graph Explorer</span>
                </SidebarMenuButton>
              </SidebarMenuItem>
              <NavLink href="/create" label="Create Table" icon={Plus} active={false} />
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarGroup>
          <SidebarGroupLabel className="mono-label">Lab</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              <NavLink
                href={selectedTable ? `/lab?table=${encodeURIComponent(selectedTable)}` : "/lab"}
                label="Lab"
                icon={FlaskConical}
                active={location.pathname.startsWith("/lab")}
              />
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarGroup>
          <SidebarGroupLabel className="mono-label">Inference</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              <NavLink
                href="/inference/models"
                label="Models"
                icon={Library}
                active={location.pathname === "/inference/models"}
              />
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        <SidebarGroup>
          <SidebarGroupLabel className="mono-label">Observe</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              <NavLink
                href="/observe"
                label="Cluster"
                icon={Network}
                active={location.pathname === "/observe"}
              />
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        {showLocalAdminRoutes && hasPermission("*", "*", "admin") && (
          <SidebarGroup>
            <SidebarGroupLabel className="mono-label">Settings</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                <NavLink
                  href="/users"
                  label="Users"
                  icon={Shield}
                  active={location.pathname === "/users"}
                />
                <NavLink
                  href="/secrets"
                  label="Secrets"
                  icon={KeyRound}
                  active={location.pathname === "/secrets"}
                />
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
