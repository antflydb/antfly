import type { Meta, StoryObj } from "@storybook/react-vite";
import { QueryDiffView } from "../components/QueryDiffView";

const meta: Meta<typeof QueryDiffView> = {
  title: "RAG Pipeline/QueryDiffView",
  component: QueryDiffView,
  parameters: { layout: "padded" },
  tags: ["autodocs"],
  decorators: [
    (Story) => (
      <div className="w-[750px] mx-auto">
        <Story />
      </div>
    ),
  ],
};

export default meta;
type Story = StoryObj<typeof meta>;

export const AddedSemanticSearch: Story = {
  args: {
    currentQuery: {
      full_text_search: {
        match_all: {},
      },
      order_by: { _score: false },
    },
    proposedQuery: {
      full_text_search: {
        match: { body: "best restaurants SF" },
      },
      semantic_search: {
        query: "best restaurants in San Francisco with outdoor seating",
        top_k: 10,
      },
      order_by: { _score: true },
    },
  },
};

export const AddedFacetsAndFilters: Story = {
  args: {
    currentQuery: {
      full_text_search: {
        match: { title: "machine learning" },
      },
    },
    proposedQuery: {
      full_text_search: {
        match: { title: "machine learning" },
      },
      filter: {
        range: { published_date: { gte: "2024-01-01" } },
      },
      facets: {
        topics: { field: "topic", size: 10 },
        authors: { field: "author", size: 5 },
      },
    },
  },
};

export const MinimalChange: Story = {
  args: {
    currentQuery: {
      full_text_search: {
        match: { body: "kubernetes deployment" },
      },
      size: 10,
    },
    proposedQuery: {
      full_text_search: {
        match: { body: "kubernetes deployment strategies" },
      },
      size: 20,
    },
  },
};
