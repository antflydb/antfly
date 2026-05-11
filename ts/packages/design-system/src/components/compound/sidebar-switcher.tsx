"use client";

import type * as React from "react";
import { ChevronsUpDown } from "lucide-react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { SidebarMenuButton } from "@/components/compound/sidebar";
import { cn } from "@/lib/utils";

function SidebarSwitcher({ children }: { children: React.ReactNode }) {
  return <DropdownMenu>{children}</DropdownMenu>;
}

function SidebarSwitcherTrigger({
  icon,
  label,
  className,
}: {
  icon?: React.ReactNode;
  label: string;
  className?: string;
}) {
  return (
    <DropdownMenuTrigger asChild>
      <SidebarMenuButton
        size="lg"
        data-slot="sidebar-switcher-trigger"
        className={cn(
          "data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground",
          className,
        )}
      >
        {icon && (
          <div className="flex items-center justify-center min-w-8 h-8">
            {icon}
          </div>
        )}
        <div className="flex flex-1 items-center text-left text-sm leading-tight">
          <span className="truncate font-semibold">{label}</span>
        </div>
        <ChevronsUpDown className="ml-auto size-4 opacity-50" />
      </SidebarMenuButton>
    </DropdownMenuTrigger>
  );
}

function SidebarSwitcherContent({
  label,
  className,
  children,
}: {
  label?: string;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <DropdownMenuContent
      data-slot="sidebar-switcher-content"
      className={cn(
        "w-[--radix-dropdown-menu-trigger-width] min-w-56 rounded-lg",
        className,
      )}
      align="start"
      side="bottom"
      sideOffset={4}
    >
      {label && (
        <DropdownMenuLabel className="mono-label px-2 py-1.5">
          {label}
        </DropdownMenuLabel>
      )}
      {children}
    </DropdownMenuContent>
  );
}

function SidebarSwitcherItem({
  name,
  description,
  selected = false,
  onSelect,
  className,
}: {
  name: string;
  description?: string;
  selected?: boolean;
  onSelect?: () => void;
  className?: string;
}) {
  return (
    <DropdownMenuItem
      data-slot="sidebar-switcher-item"
      onClick={onSelect}
      className={cn("gap-2 p-2", selected && "bg-accent", className)}
    >
      <div className="flex flex-col">
        <span className="font-medium">{name}</span>
        {description && (
          <span className="text-xs text-muted-foreground">{description}</span>
        )}
      </div>
    </DropdownMenuItem>
  );
}

export {
  SidebarSwitcher,
  SidebarSwitcherContent,
  SidebarSwitcherItem,
  SidebarSwitcherTrigger,
};
