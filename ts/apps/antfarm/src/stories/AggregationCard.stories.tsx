import type { Meta, StoryObj } from "@storybook/react-vite";
import { fn } from "storybook/test";
import AggregationCard from "../components/AggregationCard";

const meta: Meta<typeof AggregationCard> = {
  title: "RAG Pipeline/AggregationCard",
  component: AggregationCard,
  parameters: { layout: "centered" },
  tags: ["autodocs"],
  decorators: [
    (Story) => (
      <div className="w-[400px]">
        <Story />
      </div>
    ),
  ],
  args: {
    onDelete: fn(),
    onEdit: fn(),
  },
};

export default meta;
type Story = StoryObj<typeof meta>;

export const TermsAggregation: Story = {
  args: {
    name: "top_categories",
    aggregation: { type: "terms", field: "category", size: 10 },
  },
};

export const RangeAggregation: Story = {
  args: {
    name: "price_ranges",
    aggregation: {
      type: "range",
      field: "price",
      ranges: [
        { name: "cheap", from: 0, to: 25 },
        { name: "mid", from: 25, to: 75 },
        { name: "expensive", from: 75, to: undefined },
      ],
    },
  },
};

export const HistogramAggregation: Story = {
  args: {
    name: "rating_distribution",
    aggregation: { type: "histogram", field: "rating", interval: 0.5 },
  },
};

export const DateHistogramAggregation: Story = {
  args: {
    name: "posts_over_time",
    aggregation: { type: "date_histogram", field: "created_at", calendar_interval: "month" },
  },
};

export const ReadOnly: Story = {
  args: {
    name: "status_counts",
    aggregation: { type: "terms", field: "status", size: 5 },
    onEdit: undefined,
  },
};
