"use client";

import {
  Button,
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@antfly/design-system";
import { Book, ChevronDown, Layers, Plus, ShoppingBag } from "lucide-react";
import * as React from "react";

/**
 * Switcher — header entity-switcher pattern.
 *
 * Recipe:
 *   outline sm Button (name + ChevronDown)
 *   → DropdownMenu w/ align="start"
 *     → DropdownMenuLabel styled with .mono-label
 *     → items; current marked with a left-side primary-colored dot
 *     → DropdownMenuSeparator
 *     → "Create X" action with <Plus />
 *
 * Two flavors below: plain (org-style) and rich (project-style with
 * per-item type icon).
 */

type Org = { id: string; name: string };

const orgs: Org[] = [
  { id: "acme", name: "Acme Inc." },
  { id: "antfly", name: "Antfly" },
  { id: "seafront", name: "Seafront Labs" },
];

type ProjectType = "shopify" | "docs" | "generic";
type Project = { id: string; name: string; type: ProjectType };

const projectIcons: Record<ProjectType, React.ElementType> = {
  shopify: ShoppingBag,
  docs: Book,
  generic: Layers,
};

const projects: Project[] = [
  { id: "p-1", name: "Storefront", type: "shopify" },
  { id: "p-2", name: "Developer docs", type: "docs" },
  { id: "p-3", name: "Marketing site", type: "generic" },
];

/** Left-side active indicator: small purple dot for current, otherwise transparent. */
function CurrentDot({ active }: { active: boolean }) {
  return (
    <span
      aria-hidden
      className={`size-1.5 shrink-0 rounded-full ${active ? "bg-primary" : "bg-transparent"}`}
    />
  );
}

export function SwitcherDemo() {
  const [orgId, setOrgId] = React.useState("antfly");
  const [projectId, setProjectId] = React.useState("p-2");

  const currentOrg = orgs.find((o) => o.id === orgId);
  const currentProject = projects.find((p) => p.id === projectId);
  const CurrentProjectIcon = currentProject ? projectIcons[currentProject.type] : null;

  return (
    <div className="flex flex-wrap items-center gap-3 rounded-lg border border-border bg-background/60 p-4">
      {/* Plain switcher — org-style */}
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" size="sm" className="gap-2">
            {currentOrg?.name ?? "Select organization"}
            <ChevronDown className="size-3.5 opacity-50" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start" className="min-w-60">
          <DropdownMenuLabel className="mono-label px-2 py-1.5">Organizations</DropdownMenuLabel>
          {orgs.map((org) => (
            <DropdownMenuItem
              key={org.id}
              onClick={() => setOrgId(org.id)}
              className="flex items-center gap-2.5 p-2"
            >
              <CurrentDot active={org.id === orgId} />
              <span className="flex-1 truncate">{org.name}</span>
            </DropdownMenuItem>
          ))}
          <DropdownMenuSeparator />
          <DropdownMenuItem className="flex items-center gap-2 p-2 text-muted-foreground">
            <Plus className="size-4" />
            <span>Create organization</span>
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>

      {/* Rich switcher — project-style with per-item type icon */}
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="outline" size="sm" className="gap-2">
            {CurrentProjectIcon && (
              <CurrentProjectIcon className="size-3.5 shrink-0 text-muted-foreground" />
            )}
            {currentProject?.name ?? "Select a project"}
            <ChevronDown className="size-3.5 opacity-50" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start" className="min-w-60">
          <DropdownMenuLabel className="mono-label px-2 py-1.5">Projects</DropdownMenuLabel>
          {projects.map((project) => {
            const Icon = projectIcons[project.type];
            return (
              <DropdownMenuItem
                key={project.id}
                onClick={() => setProjectId(project.id)}
                className="flex items-center gap-2.5 p-2"
              >
                <CurrentDot active={project.id === projectId} />
                <Icon className="size-4 shrink-0 text-muted-foreground" />
                <span className="flex-1 truncate">{project.name}</span>
              </DropdownMenuItem>
            );
          })}
          <DropdownMenuSeparator />
          <DropdownMenuItem className="flex items-center gap-2 p-2 text-muted-foreground">
            <Plus className="size-4" />
            <span>Create project</span>
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
}
