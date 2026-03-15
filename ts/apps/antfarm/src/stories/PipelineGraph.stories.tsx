import type { Meta, StoryObj } from "@storybook/react-vite";
import { useState } from "react";
import { expect, fn, userEvent, within } from "storybook/test";
import { PipelineGraph } from "../components/rag/PipelineGraph";
import type { PipelineStepState } from "../components/rag/pipeline-types";

const meta: Meta<typeof PipelineGraph> = {
  title: "RAG Pipeline/PipelineGraph",
  component: PipelineGraph,
  parameters: { layout: "padded" },
  tags: ["autodocs"],
  decorators: [
    (Story) => (
      <div className="w-[650px] mx-auto p-8">
        <Story />
      </div>
    ),
  ],
  args: {
    onSelectStep: fn(),
  },
};

export default meta;
type Story = StoryObj<typeof meta>;

const now = Date.now();

const allCompleteSteps: PipelineStepState[] = [
  {
    id: "classification",
    label: "Classification",
    status: "complete",
    startTime: now - 3000,
    endTime: now - 2880,
  },
  {
    id: "search",
    label: "Search",
    status: "complete",
    startTime: now - 2880,
    endTime: now - 2795,
  },
  {
    id: "generation",
    label: "Generation",
    status: "complete",
    startTime: now - 2795,
    endTime: now - 1500,
  },
  {
    id: "confidence",
    label: "Confidence",
    status: "complete",
    startTime: now - 1500,
    endTime: now - 1455,
  },
  {
    id: "followup",
    label: "Follow-up",
    status: "complete",
    startTime: now - 1455,
    endTime: now - 1425,
  },
];

const midRunSteps: PipelineStepState[] = [
  {
    id: "classification",
    label: "Classification",
    status: "complete",
    startTime: now - 1200,
    endTime: now - 1080,
  },
  { id: "search", label: "Search", status: "complete", startTime: now - 1080, endTime: now - 995 },
  { id: "generation", label: "Generation", status: "running", startTime: now - 995 },
  { id: "confidence", label: "Confidence", status: "pending" },
  { id: "followup", label: "Follow-up", status: "pending" },
];

const errorSteps: PipelineStepState[] = [
  {
    id: "classification",
    label: "Classification",
    status: "complete",
    startTime: now - 500,
    endTime: now - 380,
  },
  { id: "search", label: "Search", status: "error", startTime: now - 380, endTime: now - 200 },
  { id: "generation", label: "Generation", status: "pending" },
];

export const FiveNodesComplete: Story = {
  args: {
    steps: allCompleteSteps,
    selectedStepId: null,
  },
  play: async ({ canvasElement, args }) => {
    const canvas = within(canvasElement);
    const buttons = canvas.getAllByRole("button");
    await expect(buttons).toHaveLength(5);

    // Click the Search node
    await userEvent.click(canvas.getByRole("button", { name: /Search/i }));
    await expect(args.onSelectStep).toHaveBeenCalledWith("search");
  },
};

export const FiveNodesMidRun: Story = {
  args: {
    steps: midRunSteps,
    selectedStepId: null,
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);
    const buttons = canvas.getAllByRole("button");
    await expect(buttons).toHaveLength(5);

    // Pending nodes should be disabled
    const confidenceNode = canvas.getByRole("button", { name: /Confidence/i });
    await expect(confidenceNode).toBeDisabled();
    const followupNode = canvas.getByRole("button", { name: /Follow-up/i });
    await expect(followupNode).toBeDisabled();

    // Complete and running nodes should be enabled
    const classNode = canvas.getByRole("button", { name: /Classification/i });
    await expect(classNode).toBeEnabled();
    const genNode = canvas.getByRole("button", { name: /Generation/i });
    await expect(genNode).toBeEnabled();
  },
};

export const ThreeNodesWithError: Story = {
  args: {
    steps: errorSteps,
    selectedStepId: null,
  },
  play: async ({ canvasElement, args }) => {
    const canvas = within(canvasElement);
    const buttons = canvas.getAllByRole("button");
    await expect(buttons).toHaveLength(3);

    // Pending Generation node should be disabled
    const genNode = canvas.getByRole("button", { name: /Generation/i });
    await expect(genNode).toBeDisabled();

    // Error node should be enabled and clickable
    const searchNode = canvas.getByRole("button", { name: /Search/i });
    await expect(searchNode).toBeEnabled();
    await userEvent.click(searchNode);
    await expect(args.onSelectStep).toHaveBeenCalledWith("search");
  },
};

export const ThreeNodesComplete: Story = {
  args: {
    steps: allCompleteSteps.slice(0, 3),
    selectedStepId: null,
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);
    const buttons = canvas.getAllByRole("button");
    await expect(buttons).toHaveLength(3);

    // All three should be enabled (all complete)
    for (const button of buttons) {
      await expect(button).toBeEnabled();
    }
  },
};

export const WithSelection: Story = {
  render: () => {
    const [selected, setSelected] = useState<string | null>("search");
    return (
      <PipelineGraph
        steps={allCompleteSteps}
        selectedStepId={selected}
        onSelectStep={(id) => setSelected((prev) => (prev === id ? null : id))}
      />
    );
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);

    // Search should start selected (has ring class)
    const searchNode = canvas.getByRole("button", { name: /Search/i });
    await expect(searchNode.className).toContain("ring-2");

    // Click Search again to deselect
    await userEvent.click(searchNode);
    await expect(searchNode.className).not.toContain("ring-2");

    // Click Classification to select it
    const classNode = canvas.getByRole("button", { name: /Classification/i });
    await userEvent.click(classNode);
    await expect(classNode.className).toContain("ring-2");
  },
};
