import {
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  DashboardPage,
  DashboardPageDescription,
  DashboardPageHeader,
  DashboardPageTitle,
  GraphPaperBg,
} from "@antfly/design-system";
import { ArrowRight, Bot, Boxes, GitBranch, Search, Upload, Waypoints } from "lucide-react";
import type React from "react";
import { useNavigate } from "react-router-dom";

const labs = [
  {
    title: "Search Lab",
    description: "Compare keyword, vector, and reranked search on a table.",
    href: "/lab/search",
    icon: Search,
  },
  {
    title: "Ingest Lab",
    description: "Preview parsing, chunking, extraction, and transcription before upload.",
    href: "/lab/ingest",
    icon: Upload,
  },
  {
    title: "Model Lab",
    description: "Test generators, embedders, and rerankers with small inputs.",
    href: "/lab/model",
    icon: Waypoints,
  },
  {
    title: "Agent Lab",
    description: "Use Anty for setup plans, RAG answers, chat, and eval runs.",
    href: "/lab/agent",
    icon: Bot,
  },
  {
    title: "Graph Lab",
    description: "Extract entities and relations, then promote graph index setup.",
    href: "/lab/graph",
    icon: GitBranch,
  },
];

export default function LabPage() {
  const navigate = useNavigate();

  return (
    <DashboardPage>
      <div className="relative isolate">
        <GraphPaperBg className="absolute inset-0 -z-10 rounded-none" />
        <DashboardPageHeader>
          <div>
            <DashboardPageTitle className="font-aeonik">Lab</DashboardPageTitle>
            <DashboardPageDescription>
              Focused experiments that can graduate into real instance configuration.
            </DashboardPageDescription>
          </div>
        </DashboardPageHeader>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {labs.map((lab) => {
          const Icon = lab.icon;
          return (
            <Card key={lab.href}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-base">
                  <Icon className="size-4" />
                  {lab.title}
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <p className="text-muted-foreground text-sm">{lab.description}</p>
                <Button variant="outline" onClick={() => navigate(lab.href)}>
                  Open lab
                  <ArrowRight className="ml-2 size-4" />
                </Button>
              </CardContent>
            </Card>
          );
        })}
      </div>

      <Card>
        <CardContent className="flex items-center gap-3 py-4 text-muted-foreground text-sm">
          <Boxes className="size-4" />
          Every lab follows Input, Run, Inspect, Promote.
        </CardContent>
      </Card>
    </DashboardPage>
  );
}

export function LabSection({
  title,
  description,
  promote,
  children,
}: {
  title: string;
  description: string;
  promote: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <DashboardPage>
      <div className="relative isolate">
        <GraphPaperBg className="absolute inset-0 -z-10 rounded-none" />
        <DashboardPageHeader>
          <div>
            <DashboardPageTitle className="font-aeonik">{title}</DashboardPageTitle>
            <DashboardPageDescription>{description}</DashboardPageDescription>
          </div>
        </DashboardPageHeader>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Promote</CardTitle>
        </CardHeader>
        <CardContent>{promote}</CardContent>
      </Card>

      <div className="space-y-6">{children}</div>
    </DashboardPage>
  );
}
