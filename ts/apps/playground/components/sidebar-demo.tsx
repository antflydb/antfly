"use client";

import {
  Avatar,
  AvatarFallback,
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
  Lockup,
  Logo,
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuBadge,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarMenuSub,
  SidebarMenuSubButton,
  SidebarMenuSubItem,
  SidebarProvider,
  Wordmark,
} from "@antfly/design-system";
import {
  Activity,
  ChevronRight,
  Database,
  FileText,
  KeyRound,
  LayoutDashboard,
  LogOut,
  Search,
  Settings,
  Webhook,
} from "lucide-react";

/**
 * Bounded-frame sidebar demo. We use `collapsible="none"` so the sidebar
 * renders in normal flex flow — the `offcanvas` / `icon` modes use
 * `position: fixed` + `h-svh` against the viewport, which is correct for a
 * real app layout but escapes a bounded gallery frame.
 *
 * For the full interactive behavior (Cmd/Ctrl-B toggle, icon-only state,
 * mobile drawer) the component must be mounted at the page level.
 */
export function SidebarDemo() {
  return (
    <div className="flex h-[560px] w-full overflow-hidden rounded-lg border border-border">
      <SidebarProvider defaultOpen className="contents">
        <Sidebar collapsible="none" className="border-r border-border">
          <SidebarHeader>
            <Lockup className="px-2 py-1">
              <Logo src="/af-logo.svg" srcDark="/af-logo-dark.svg" alt="Antfarm" size="sm" />
              <Wordmark className="text-base">Antfarm</Wordmark>
            </Lockup>
          </SidebarHeader>

          <SidebarContent>
            <SidebarGroup>
              <SidebarGroupLabel className="font-mono uppercase tracking-wider">
                Overview
              </SidebarGroupLabel>
              <SidebarMenu>
                <SidebarMenuItem>
                  <SidebarMenuButton isActive>
                    <LayoutDashboard />
                    <span>Dashboard</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton>
                    <Search />
                    <span>Queries</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton>
                    <Activity />
                    <span>Activity</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              </SidebarMenu>
            </SidebarGroup>

            <SidebarGroup>
              <SidebarGroupLabel className="font-mono uppercase tracking-wider">
                Data
              </SidebarGroupLabel>
              <SidebarMenu>
                <SidebarMenuItem>
                  <SidebarMenuButton>
                    <Database />
                    <span>Indexes</span>
                  </SidebarMenuButton>
                  <SidebarMenuBadge>12</SidebarMenuBadge>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton>
                    <FileText />
                    <span>Documents</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              </SidebarMenu>
            </SidebarGroup>

            <SidebarGroup>
              <SidebarGroupLabel className="font-mono uppercase tracking-wider">
                API
              </SidebarGroupLabel>
              <SidebarMenu>
                <SidebarMenuItem>
                  <SidebarMenuButton>
                    <Webhook />
                    <span>Endpoints</span>
                    <ChevronRight className="ml-auto size-4 opacity-60" />
                  </SidebarMenuButton>
                  <SidebarMenuSub>
                    <SidebarMenuSubItem>
                      <SidebarMenuSubButton>Search</SidebarMenuSubButton>
                    </SidebarMenuSubItem>
                    <SidebarMenuSubItem>
                      <SidebarMenuSubButton>Ingest</SidebarMenuSubButton>
                    </SidebarMenuSubItem>
                  </SidebarMenuSub>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton>
                    <KeyRound />
                    <span>Keys</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              </SidebarMenu>
            </SidebarGroup>
          </SidebarContent>

          <SidebarFooter>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <SidebarMenuButton size="lg" className="data-[state=open]:bg-sidebar-accent">
                  <Avatar className="size-7">
                    <AvatarFallback className="text-xs">DL</AvatarFallback>
                  </Avatar>
                  <div className="flex flex-col items-start text-left leading-tight">
                    <span className="text-sm font-medium">Drew Lanenga</span>
                    <span className="text-xs text-muted-foreground">drew@antfly.io</span>
                  </div>
                </SidebarMenuButton>
              </DropdownMenuTrigger>
              <DropdownMenuContent side="top" align="start" className="w-56">
                <DropdownMenuLabel>Account</DropdownMenuLabel>
                <DropdownMenuSeparator />
                <DropdownMenuItem>
                  <Settings className="size-4" /> Settings
                </DropdownMenuItem>
                <DropdownMenuItem>
                  <LogOut className="size-4" /> Sign out
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </SidebarFooter>
        </Sidebar>

        <main className="flex-1 overflow-auto bg-background p-6">
          <p className="font-display text-xl font-bold">Main content lives here.</p>
          <p className="mt-2 max-w-prose text-sm text-muted-foreground">
            This demo uses <code className="font-mono text-xs">collapsible="none"</code> to stay
            inside the gallery frame. In a real app, pair{" "}
            <code className="font-mono text-xs">SidebarProvider</code> +{" "}
            <code className="font-mono text-xs">Sidebar</code> +{" "}
            <code className="font-mono text-xs">SidebarInset</code> at the page root with{" "}
            <code className="font-mono text-xs">collapsible="icon"</code> or{" "}
            <code className="font-mono text-xs">collapsible="offcanvas"</code> to get icon-only
            collapse, mobile drawer, and the{" "}
            <kbd className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs">Cmd/Ctrl</kbd>+
            <kbd className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs">B</kbd> toggle.
          </p>
        </main>
      </SidebarProvider>
    </div>
  );
}
