import type { ClassificationTransformationResult, QueryHit } from "@antfly/sdk";

export type PipelineStepStatus = "pending" | "running" | "complete" | "error" | "skipped";
export type PipelineStepId = "classification" | "search" | "generation" | "confidence" | "followup";

export interface PipelineStepState {
  id: PipelineStepId;
  label: string;
  status: PipelineStepStatus;
  startTime?: number;
  endTime?: number;
  data?: unknown;
}

export interface PipelineState {
  steps: PipelineStepState[];
  overallStatus: "idle" | "running" | "complete" | "error";
}

// Step-specific data types
export interface ClassificationStepData {
  classification: ClassificationTransformationResult;
}

export interface SearchStepData {
  hits: QueryHit[];
  filterApplied?: string;
  queryExecuted?: string;
}

export interface GenerationStepData {
  answer: string;
  provider?: string;
  model?: string;
}

export interface ConfidenceStepData {
  generation: number;
  context: number;
}

export interface FollowupStepData {
  questions: string[];
}

// Reducer actions
export type PipelineAction =
  | { type: "RESET" }
  | { type: "START"; enabledSteps: PipelineStepId[] }
  | { type: "STEP_START"; stepId: PipelineStepId }
  | { type: "STEP_COMPLETE"; stepId: PipelineStepId; data?: unknown }
  | { type: "STEP_ERROR"; stepId: PipelineStepId; error?: string }
  | { type: "STEP_UPDATE"; stepId: PipelineStepId; data: unknown }
  | { type: "COMPLETE" }
  | { type: "ERROR"; error?: string };

export const STEP_DEFINITIONS: { id: PipelineStepId; label: string }[] = [
  { id: "classification", label: "Classification" },
  { id: "search", label: "Search" },
  { id: "generation", label: "Generation" },
  { id: "confidence", label: "Confidence" },
  { id: "followup", label: "Follow-up Questions" },
];

export const initialPipelineState: PipelineState = {
  steps: [],
  overallStatus: "idle",
};

export function pipelineReducer(state: PipelineState, action: PipelineAction): PipelineState {
  switch (action.type) {
    case "RESET":
      return initialPipelineState;

    case "START": {
      const steps = STEP_DEFINITIONS.filter((def) => action.enabledSteps.includes(def.id)).map(
        (def) => ({
          id: def.id,
          label: def.label,
          status: "pending" as PipelineStepStatus,
        })
      );
      return { steps, overallStatus: "running" };
    }

    case "STEP_START":
      return {
        ...state,
        steps: state.steps.map((s) =>
          s.id === action.stepId ? { ...s, status: "running", startTime: Date.now() } : s
        ),
      };

    case "STEP_COMPLETE":
      return {
        ...state,
        steps: state.steps.map((s) =>
          s.id === action.stepId
            ? { ...s, status: "complete", endTime: Date.now(), data: action.data ?? s.data }
            : s
        ),
      };

    case "STEP_ERROR":
      return {
        ...state,
        steps: state.steps.map((s) =>
          s.id === action.stepId
            ? { ...s, status: "error", endTime: Date.now(), data: action.error }
            : s
        ),
      };

    case "STEP_UPDATE":
      return {
        ...state,
        steps: state.steps.map((s) => (s.id === action.stepId ? { ...s, data: action.data } : s)),
      };

    case "COMPLETE":
      return { ...state, overallStatus: "complete" };

    case "ERROR":
      return { ...state, overallStatus: "error" };

    default:
      return state;
  }
}
