import { GitGraph, MessageSquare, Text } from "lucide-react";
import type React from "react";
import { type ReactNode, useCallback, useMemo, useState } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { PipelineDetailPanel } from "./PipelineDetailPanel";
import { PipelineGraph } from "./PipelineGraph";
import type { PipelineState } from "./pipeline-types";

interface PipelineTraceProps {
  pipeline: PipelineState;
  onFollowupClick?: (question: string) => void;
  formatAnswer?: (text: string) => React.ReactNode;
  responseContent?: ReactNode;
}

export const PipelineTrace: React.FC<PipelineTraceProps> = ({
  pipeline,
  onFollowupClick,
  formatAnswer,
  responseContent,
}) => {
  const [selectedStepId, setSelectedStepId] = useState<string | null>(null);

  const handleSelectStep = useCallback((stepId: string) => {
    setSelectedStepId((prev) => (prev === stepId ? null : stepId));
  }, []);

  const selectedStep = useMemo(
    () => pipeline.steps.find((s) => s.id === selectedStepId) ?? null,
    [pipeline.steps, selectedStepId]
  );

  const pipelineIsIdle = pipeline.overallStatus === "idle" || pipeline.steps.length === 0;

  const statusIndicator = (
    <div className="flex items-center gap-2">
      {pipeline.overallStatus === "running" && (
        <span className="flex items-center gap-1.5 text-xs text-blue-500">
          <span className="w-1.5 h-1.5 rounded-full bg-blue-500 animate-pulse" />
          Running
        </span>
      )}
      {pipeline.overallStatus === "complete" && (
        <span className="text-xs text-green-500">Complete</span>
      )}
      {pipeline.overallStatus === "error" && <span className="text-xs text-red-500">Error</span>}
    </div>
  );

  return (
    <Tabs defaultValue="response" className="gap-3">
      <div className="flex items-center justify-between">
        <TabsList>
          <TabsTrigger value="response">
            <Text className="h-3.5 w-3.5" />
            Response
          </TabsTrigger>
          <TabsTrigger value="pipeline">
            <GitGraph className="h-3.5 w-3.5" />
            Pipeline
          </TabsTrigger>
        </TabsList>
        {statusIndicator}
      </div>

      <TabsContent value="pipeline" forceMount className="data-[state=inactive]:hidden">
        {pipelineIsIdle ? (
          <div className="h-64 flex items-center justify-center text-muted-foreground">
            <div className="text-center">
              <MessageSquare className="h-12 w-12 mx-auto mb-3 opacity-20" />
              <p>Run a query to see the pipeline trace</p>
            </div>
          </div>
        ) : (
          <div className="space-y-2">
            <PipelineGraph
              steps={pipeline.steps}
              selectedStepId={selectedStepId}
              onSelectStep={handleSelectStep}
            />
            <PipelineDetailPanel
              step={selectedStep}
              open={selectedStepId !== null}
              onFollowupClick={onFollowupClick}
              formatAnswer={formatAnswer}
            />
          </div>
        )}
      </TabsContent>

      <TabsContent value="response" forceMount className="data-[state=inactive]:hidden">
        {responseContent}
      </TabsContent>
    </Tabs>
  );
};
