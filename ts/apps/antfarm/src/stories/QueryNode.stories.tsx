import type { Meta, StoryObj } from "@storybook/react-vite";
import { fn } from "storybook/test";
import QueryNode from "../components/querybuilder/QueryNode";

const meta: Meta<typeof QueryNode> = {
  title: "QueryBuilder/QueryNode",
  component: QueryNode,
  parameters: {
    layout: "centered",
  },
  tags: ["autodocs"],
  decorators: [
    (Story) => (
      <div className="w-[800px]">
        <Story />
      </div>
    ),
  ],
  args: {
    onChange: fn(),
    onDelete: fn(),
  },
};

export default meta;
type Story = StoryObj<typeof meta>;

export const MatchAll: Story = {
  args: {
    query: { match_all: {} },
  },
};

export const Term: Story = {
  args: {
    query: { term: "antfly", field: "name" },
  },
};

export const Match: Story = {
  args: {
    query: { match: "database", field: "description" },
  },
};

export const MatchPhrase: Story = {
  args: {
    query: { match_phrase: "fast and easy", field: "tagline" },
  },
};

export const NumericRange: Story = {
  args: {
    query: { min: 10, max: 100, field: "size" },
  },
};

export const BooleanQuery: Story = {
  args: {
    query: {
      must: {
        conjuncts: [{ term: "database" }],
      },
      should: {
        disjuncts: [{ match: "fast" }],
      },
      must_not: {
        disjuncts: [{ term: "slow" }],
      },
    },
  },
};

export const Conjunction: Story = {
  args: {
    query: {
      conjuncts: [{ term: "database" }, { match: "fast" }],
    },
  },
};

export const Disjunction: Story = {
  args: {
    query: {
      disjuncts: [{ term: "database" }, { match: "fast" }],
    },
  },
};
