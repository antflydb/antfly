"use client";

import {
  SidebarProvider,
  SidebarSwitcher,
  SidebarSwitcherContent,
  SidebarSwitcherItem,
  SidebarSwitcherTrigger,
  Switcher,
  SwitcherContent,
  SwitcherFooter,
  SwitcherItem,
  SwitcherTrigger,
} from "@antfly/design-system";
import { Book, Database, Layers, Plus, ShoppingBag } from "lucide-react";
import * as React from "react";

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
  { id: "p-4", name: "Help center", type: "docs" },
  { id: "p-5", name: "Product catalog", type: "shopify" },
];

const products = [
  { id: "antfarm", name: "Antfarm", description: "Database management dashboard" },
  { id: "searchaf", name: "SearchAF", description: "Managed search & answer engines" },
  { id: "inference", name: "Antfly Inference", description: "Local ML inference runtime" },
];

export function SwitcherDemo() {
  const [projectId, setProjectId] = React.useState("p-2");
  const [selectedProduct, setSelectedProduct] = React.useState("antfarm");

  const currentProject = projects.find((p) => p.id === projectId);
  const CurrentProjectIcon = currentProject ? projectIcons[currentProject.type] : null;
  const currentProduct = products.find((p) => p.id === selectedProduct);

  return (
    <div className="flex flex-wrap items-start gap-8">
      {/* Switcher — searchable popover with icons, footer */}
      <div className="space-y-2">
        <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
          Switcher (searchable, icons, footer)
        </p>
        <div className="w-60">
          <Switcher>
            <SwitcherTrigger placeholder="Select a project…">
              {CurrentProjectIcon && (
                <CurrentProjectIcon className="size-3.5 shrink-0 text-muted-foreground" />
              )}
              {currentProject?.name}
            </SwitcherTrigger>
            <SwitcherContent
              searchPlaceholder="Search projects…"
              emptyMessage="No project found."
              heading="Projects"
              footer={
                <SwitcherFooter>
                  <Plus className="size-4" />
                  <span>Create project</span>
                </SwitcherFooter>
              }
            >
              {projects.map((project) => {
                const Icon = projectIcons[project.type];
                return (
                  <SwitcherItem
                    key={project.id}
                    value={project.id}
                    selected={projectId === project.id}
                    icon={<Icon />}
                    onSelect={setProjectId}
                  >
                    {project.name}
                  </SwitcherItem>
                );
              })}
            </SwitcherContent>
          </Switcher>
        </div>
      </div>

      {/* SidebarSwitcher — dropdown menu for sidebar header */}
      <div className="space-y-2">
        <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
          SidebarSwitcher (dropdown)
        </p>
        <SidebarProvider defaultOpen className="w-64 rounded-lg border border-sidebar-border bg-sidebar p-2 [&>*]:!min-h-0">
          <SidebarSwitcher>
            <SidebarSwitcherTrigger
              icon={<Database className="size-4" />}
              label={currentProduct?.name ?? "Select product"}
            />
            <SidebarSwitcherContent label="Products">
              {products.map((product) => (
                <SidebarSwitcherItem
                  key={product.id}
                  name={product.name}
                  description={product.description}
                  selected={selectedProduct === product.id}
                  onSelect={() => setSelectedProduct(product.id)}
                />
              ))}
            </SidebarSwitcherContent>
          </SidebarSwitcher>
        </SidebarProvider>
      </div>
    </div>
  );
}
