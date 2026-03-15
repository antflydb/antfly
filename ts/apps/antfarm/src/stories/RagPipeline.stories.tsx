import type { Meta, StoryObj } from "@storybook/react-vite";
import { fn } from "storybook/test";
import { PipelineStep } from "../components/rag/PipelineStep";
import type {
  ClassificationStepData,
  ConfidenceStepData,
  FollowupStepData,
  GenerationStepData,
  PipelineStepState,
  SearchStepData,
} from "../components/rag/pipeline-types";

// ─── PipelineStep stories (legacy card view) ───

const stepMeta: Meta<typeof PipelineStep> = {
  title: "RAG Pipeline/PipelineStep",
  component: PipelineStep,
  parameters: { layout: "centered" },
  tags: ["autodocs"],
  decorators: [
    (Story) => (
      <div className="w-[600px]">
        <Story />
      </div>
    ),
  ],
  args: {
    isLast: true,
    onFollowupClick: fn(),
  },
};

export default stepMeta;
type StepStory = StoryObj<typeof stepMeta>;

const now = Date.now();

export const ClassificationComplete: StepStory = {
  args: {
    step: {
      id: "classification",
      label: "Classification",
      status: "complete",
      startTime: now - 120,
      endTime: now,
      data: {
        classification: {
          route_type: "question",
          strategy: "hyde",
          semantic_mode: "rewrite",
          improved_query: "best restaurants in San Francisco with outdoor seating",
          semantic_query: "best restaurants in San Francisco with outdoor seating",
          confidence: 0.95,
          reasoning:
            "Query contains a location constraint and qualitative preference, suggesting hybrid retrieval with semantic understanding.",
        },
      } satisfies ClassificationStepData,
    } satisfies PipelineStepState,
  },
};

export const SearchComplete: StepStory = {
  args: {
    step: {
      id: "search",
      label: "Search",
      status: "complete",
      startTime: now - 85,
      endTime: now,
      data: {
        hits: [
          {
            _id: "doc-1",
            _score: 0.952,
            _source: {
              title: "Top SF Restaurants 2025",
              body: "A curated list of the best restaurants in San Francisco featuring outdoor dining...",
            },
          },
          {
            _id: "doc-2",
            _score: 0.891,
            _source: {
              title: "Outdoor Dining Guide",
              body: "San Francisco's best patios, rooftops, and garden seating options...",
            },
          },
          {
            _id: "doc-3",
            _score: 0.847,
            _source: {
              title: "Restaurant Reviews: Bay Area",
              body: "In-depth reviews of popular Bay Area restaurants with ambiance ratings...",
            },
          },
        ],
        filterApplied: 'city = "San Francisco"',
        queryExecuted: "best restaurants outdoor seating",
      } satisfies SearchStepData,
    } satisfies PipelineStepState,
  },
};

export const GenerationComplete: StepStory = {
  args: {
    step: {
      id: "generation",
      label: "Generation",
      status: "complete",
      startTime: now - 1200,
      endTime: now,
      data: {
        answer:
          "Based on the retrieved documents, here are the top restaurants in San Francisco with outdoor seating:\n\n1. **Nopa** — Known for its organic, wood-fired cuisine and spacious heated patio on Divisadero.\n2. **Foreign Cinema** — A Mission District staple with a stunning courtyard where films are projected nightly.\n3. **Waterbar** — Waterfront dining on the Embarcadero with panoramic Bay Bridge views.",
        provider: "openai",
        model: "gpt-4o",
      } satisfies GenerationStepData,
    } satisfies PipelineStepState,
  },
};

export const GenerationStreaming: StepStory = {
  args: {
    step: {
      id: "generation",
      label: "Generation",
      status: "running",
      startTime: now - 600,
      data: {
        answer: "Based on the retrieved documents, here are the top restaurants",
        provider: "openai",
        model: "gpt-4o",
      } satisfies GenerationStepData,
    } satisfies PipelineStepState,
  },
};

export const ConfidenceHigh: StepStory = {
  args: {
    step: {
      id: "confidence",
      label: "Confidence",
      status: "complete",
      startTime: now - 45,
      endTime: now,
      data: { generation: 0.92, context: 0.88 } satisfies ConfidenceStepData,
    } satisfies PipelineStepState,
  },
};

export const ConfidenceLow: StepStory = {
  args: {
    step: {
      id: "confidence",
      label: "Confidence",
      status: "complete",
      startTime: now - 45,
      endTime: now,
      data: { generation: 0.31, context: 0.25 } satisfies ConfidenceStepData,
    } satisfies PipelineStepState,
  },
};

export const FollowupQuestions: StepStory = {
  args: {
    step: {
      id: "followup",
      label: "Follow-up Questions",
      status: "complete",
      startTime: now - 30,
      endTime: now,
      data: {
        questions: [
          "Which of these restaurants is most affordable?",
          "Are any of these restaurants dog-friendly?",
          "What are the reservation wait times like?",
        ],
      } satisfies FollowupStepData,
    } satisfies PipelineStepState,
  },
};

export const StepPending: StepStory = {
  args: {
    step: {
      id: "generation",
      label: "Generation",
      status: "pending",
    } satisfies PipelineStepState,
  },
};

export const StepError: StepStory = {
  args: {
    step: {
      id: "search",
      label: "Search",
      status: "error",
      startTime: now - 200,
      endTime: now,
      data: "Connection timeout: Antfly instance unreachable after 5000ms",
    } satisfies PipelineStepState,
  },
};
