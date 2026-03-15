import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, fn, userEvent, within } from "storybook/test";
import { PipelineTrace } from "../components/rag/PipelineTrace";
import type {
  ClassificationStepData,
  ConfidenceStepData,
  FollowupStepData,
  GenerationStepData,
  PipelineState,
  SearchStepData,
} from "../components/rag/pipeline-types";

const meta: Meta<typeof PipelineTrace> = {
  title: "RAG Pipeline/PipelineTrace",
  component: PipelineTrace,
  parameters: { layout: "padded" },
  tags: ["autodocs"],
  decorators: [
    (Story) => (
      <div className="w-[650px] mx-auto">
        <Story />
      </div>
    ),
  ],
  args: {
    onFollowupClick: fn(),
  },
};

export default meta;
type Story = StoryObj<typeof meta>;

const now = Date.now();

export const Idle: Story = {
  args: {
    pipeline: {
      steps: [],
      overallStatus: "idle",
    },
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);
    await expect(canvas.getByText("Run a query to see the pipeline trace")).toBeInTheDocument();
    // No node buttons should exist
    const buttons = canvas.queryAllByRole("button");
    await expect(buttons).toHaveLength(0);
  },
};

export const FullRunComplete: Story = {
  args: {
    pipeline: {
      overallStatus: "complete",
      steps: [
        {
          id: "classification",
          label: "Classification",
          status: "complete",
          startTime: now - 3000,
          endTime: now - 2880,
          data: {
            classification: {
              route_type: "question",
              strategy: "hyde",
              semantic_mode: "rewrite",
              improved_query: "best restaurants in San Francisco with outdoor seating",
              semantic_query: "best restaurants in San Francisco with outdoor seating",
              confidence: 0.95,
              reasoning:
                "Query contains a location constraint and qualitative preference — hybrid retrieval will combine keyword filtering with semantic ranking.",
            },
          } satisfies ClassificationStepData,
        },
        {
          id: "search",
          label: "Search",
          status: "complete",
          startTime: now - 2880,
          endTime: now - 2795,
          data: {
            hits: [
              {
                _id: "doc-1",
                _score: 0.952,
                _source: {
                  title: "Top SF Restaurants 2025",
                  body: "A curated list of the best restaurants in San Francisco...",
                },
              },
              {
                _id: "doc-2",
                _score: 0.891,
                _source: {
                  title: "Outdoor Dining Guide",
                  body: "San Francisco's best patios, rooftops, and gardens...",
                },
              },
              {
                _id: "doc-3",
                _score: 0.847,
                _source: {
                  title: "Restaurant Reviews: Bay Area",
                  body: "In-depth reviews of popular Bay Area restaurants...",
                },
              },
            ],
            filterApplied: 'city = "San Francisco"',
          } satisfies SearchStepData,
        },
        {
          id: "generation",
          label: "Generation",
          status: "complete",
          startTime: now - 2795,
          endTime: now - 1500,
          data: {
            answer:
              "Based on the retrieved documents, here are the top restaurants in San Francisco with outdoor seating:\n\n1. **Nopa** — Known for its organic, wood-fired cuisine and spacious heated patio.\n2. **Foreign Cinema** — A Mission District staple with a stunning courtyard.\n3. **Waterbar** — Waterfront dining with panoramic Bay Bridge views.",
            provider: "openai",
            model: "gpt-4o",
          } satisfies GenerationStepData,
        },
        {
          id: "confidence",
          label: "Confidence",
          status: "complete",
          startTime: now - 1500,
          endTime: now - 1455,
          data: { generation: 0.92, context: 0.88 } satisfies ConfidenceStepData,
        },
        {
          id: "followup",
          label: "Follow-up Questions",
          status: "complete",
          startTime: now - 1455,
          endTime: now - 1425,
          data: {
            questions: [
              "Which of these restaurants is most affordable?",
              "Are any of these restaurants dog-friendly?",
              "What are the reservation wait times like?",
            ],
          } satisfies FollowupStepData,
        },
      ],
    } satisfies PipelineState,
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);

    // All 5 nodes should render
    const buttons = canvas.getAllByRole("button");
    await expect(buttons).toHaveLength(5);

    // Click Search node — detail panel should open with search content
    await userEvent.click(canvas.getByRole("button", { name: /Search/i }));
    await expect(canvas.getByRole("heading", { name: "Search" })).toBeInTheDocument();
    await expect(canvas.getByText("3 documents retrieved")).toBeInTheDocument();

    // Click Search again — detail panel should close
    await userEvent.click(canvas.getByRole("button", { name: /Search/i }));
    await expect(canvas.queryByRole("heading", { name: "Search" })).not.toBeInTheDocument();

    // Click Classification — detail panel shows classification content
    await userEvent.click(canvas.getByRole("button", { name: /Classification/i }));
    await expect(canvas.getByRole("heading", { name: "Classification" })).toBeInTheDocument();
    await expect(canvas.getByText("Strategy:")).toBeInTheDocument();

    // Switch to Confidence — detail panel swaps content
    await userEvent.click(canvas.getByRole("button", { name: /Confidence/i }));
    await expect(canvas.getByRole("heading", { name: "Confidence" })).toBeInTheDocument();
    await expect(canvas.getByText("Generation Confidence")).toBeInTheDocument();
    await expect(canvas.getByText("92%")).toBeInTheDocument();
  },
};

export const MidRunStreaming: Story = {
  args: {
    pipeline: {
      overallStatus: "running",
      steps: [
        {
          id: "classification",
          label: "Classification",
          status: "complete",
          startTime: now - 1200,
          endTime: now - 1080,
          data: {
            classification: {
              route_type: "question",
              strategy: "simple",
              semantic_mode: "rewrite",
              improved_query: "how does photosynthesis work",
              semantic_query: "how does photosynthesis work",
              confidence: 0.98,
              reasoning: "Factual question — pure semantic retrieval is optimal.",
            },
          } satisfies ClassificationStepData,
        },
        {
          id: "search",
          label: "Search",
          status: "complete",
          startTime: now - 1080,
          endTime: now - 995,
          data: {
            hits: [
              {
                _id: "bio-101",
                _score: 0.97,
                _source: {
                  title: "Photosynthesis Overview",
                  body: "Photosynthesis is the process by which green plants convert sunlight into chemical energy...",
                },
              },
              {
                _id: "bio-204",
                _score: 0.91,
                _source: {
                  title: "Light Reactions and Calvin Cycle",
                  body: "The light-dependent reactions occur in the thylakoid membranes...",
                },
              },
            ],
          } satisfies SearchStepData,
        },
        {
          id: "generation",
          label: "Generation",
          status: "running",
          startTime: now - 995,
          data: {
            answer:
              "Photosynthesis is the process by which plants, algae, and some bacteria convert light energy into chemical energy stored in glucose. It occurs in two main stages:",
            provider: "openai",
            model: "gpt-4o",
          } satisfies GenerationStepData,
        },
        {
          id: "confidence",
          label: "Confidence",
          status: "pending",
        },
        {
          id: "followup",
          label: "Follow-up Questions",
          status: "pending",
        },
      ],
    } satisfies PipelineState,
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);

    // Running status indicator should show
    await expect(canvas.getByText("Running")).toBeInTheDocument();

    // Pending nodes should be disabled
    const confidenceNode = canvas.getByRole("button", { name: /Confidence/i });
    await expect(confidenceNode).toBeDisabled();
    const followupNode = canvas.getByRole("button", { name: /Follow-up/i });
    await expect(followupNode).toBeDisabled();

    // Running node (Generation) should be enabled and clickable
    const genNode = canvas.getByRole("button", { name: /Generation/i });
    await expect(genNode).toBeEnabled();
    await userEvent.click(genNode);
    await expect(canvas.getByRole("heading", { name: "Generation" })).toBeInTheDocument();
  },
};

export const ErrorDuringSearch: Story = {
  args: {
    pipeline: {
      overallStatus: "error",
      steps: [
        {
          id: "classification",
          label: "Classification",
          status: "complete",
          startTime: now - 500,
          endTime: now - 380,
          data: {
            classification: {
              route_type: "search",
              strategy: "simple",
              semantic_mode: "rewrite",
              improved_query: "simple keyword lookup query",
              semantic_query: "simple keyword lookup query",
              confidence: 0.85,
              reasoning: "Simple keyword lookup.",
            },
          } satisfies ClassificationStepData,
        },
        {
          id: "search",
          label: "Search",
          status: "error",
          startTime: now - 380,
          endTime: now - 200,
          data: "Connection timeout: Antfly instance unreachable after 5000ms",
        },
        {
          id: "generation",
          label: "Generation",
          status: "pending",
        },
      ],
    } satisfies PipelineState,
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);

    // Error status indicator should show
    await expect(canvas.getByText("Error")).toBeInTheDocument();

    // Click the error Search node — should show error message in detail panel
    const searchNode = canvas.getByRole("button", { name: /Search/i });
    await expect(searchNode).toBeEnabled();
    await userEvent.click(searchNode);
    await expect(canvas.getByRole("heading", { name: "Search" })).toBeInTheDocument();
    await expect(
      canvas.getByText("Connection timeout: Antfly instance unreachable after 5000ms")
    ).toBeInTheDocument();
  },
};

export const LowConfidence: Story = {
  args: {
    pipeline: {
      overallStatus: "complete",
      steps: [
        {
          id: "classification",
          label: "Classification",
          status: "complete",
          startTime: now - 2000,
          endTime: now - 1900,
          data: {
            classification: {
              route_type: "question",
              strategy: "step_back",
              semantic_mode: "hypothetical",
              improved_query: "quantum gravity unified theory latest results",
              semantic_query: "quantum gravity unified theory latest results",
              confidence: 0.62,
              reasoning:
                "Highly specialized query — semantic retrieval may find tangentially related content.",
            },
          } satisfies ClassificationStepData,
        },
        {
          id: "search",
          label: "Search",
          status: "complete",
          startTime: now - 1900,
          endTime: now - 1820,
          data: {
            hits: [
              {
                _id: "phys-42",
                _score: 0.61,
                _source: {
                  title: "Introduction to Quantum Mechanics",
                  body: "A broad overview of quantum mechanical principles...",
                },
              },
            ],
          } satisfies SearchStepData,
        },
        {
          id: "generation",
          label: "Generation",
          status: "complete",
          startTime: now - 1820,
          endTime: now - 800,
          data: {
            answer:
              "I found some related content about quantum mechanics, but the available documents don't specifically cover recent quantum gravity or unified field theory results.",
            provider: "anthropic",
            model: "claude-sonnet-4-20250514",
          } satisfies GenerationStepData,
        },
        {
          id: "confidence",
          label: "Confidence",
          status: "complete",
          startTime: now - 800,
          endTime: now - 770,
          data: { generation: 0.31, context: 0.22 } satisfies ConfidenceStepData,
        },
      ],
    } satisfies PipelineState,
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);

    // 4 nodes should render (no followup step)
    const buttons = canvas.getAllByRole("button");
    await expect(buttons).toHaveLength(4);

    // Click Confidence — should show low confidence values
    await userEvent.click(canvas.getByRole("button", { name: /Confidence/i }));
    await expect(canvas.getByRole("heading", { name: "Confidence" })).toBeInTheDocument();
    await expect(canvas.getByText("31%")).toBeInTheDocument();
    await expect(canvas.getByText("22%")).toBeInTheDocument();
  },
};
