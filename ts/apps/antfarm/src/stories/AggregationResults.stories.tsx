import type { Meta, StoryObj } from "@storybook/react-vite";
import AggregationResults from "../components/AggregationResults";

const meta: Meta<typeof AggregationResults> = {
  title: "RAG Pipeline/AggregationResults",
  component: AggregationResults,
  parameters: { layout: "padded" },
  tags: ["autodocs"],
  decorators: [
    (Story) => (
      <div className="w-[700px] mx-auto">
        <Story />
      </div>
    ),
  ],
};

export default meta;
type Story = StoryObj<typeof meta>;

export const TermsBuckets: Story = {
  args: {
    aggregations: {
      top_categories: {
        buckets: [
          { key: "electronics", doc_count: 1234 },
          { key: "clothing", doc_count: 891 },
          { key: "books", doc_count: 567 },
          { key: "home", doc_count: 432 },
          { key: "sports", doc_count: 198 },
        ],
      },
    },
  },
};

export const MetricResults: Story = {
  args: {
    aggregations: {
      avg_price: { avg: 42.5 },
      total_revenue: { sum: 128750.0 },
      min_rating: { min: 1.2 },
      max_rating: { max: 5.0 },
    },
  },
};

export const MixedAggregations: Story = {
  args: {
    aggregations: {
      status_breakdown: {
        buckets: [
          { key: "published", doc_count: 3420 },
          { key: "draft", doc_count: 891 },
          { key: "archived", doc_count: 234 },
        ],
      },
      avg_word_count: { avg: 1250.8 },
      price_ranges: {
        buckets: [
          { key: "0-25", doc_count: 450, from: 0, to: 25 },
          { key: "25-75", doc_count: 890, from: 25, to: 75 },
          { key: "75+", doc_count: 210, from: 75 },
        ],
      },
    },
  },
};

export const Empty: Story = {
  args: {
    aggregations: {},
  },
};
