import type { Meta, StoryObj } from "@storybook/react-vite";
import { fn } from "storybook/test";
import QueryBuilder from "../components/querybuilder/QueryBuilder";

const meta: Meta<typeof QueryBuilder> = {
  title: "QueryBuilder/QueryBuilder",
  component: QueryBuilder,
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
  },
};

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    value: JSON.stringify(
      {
        full_text_search: {
          match_all: {},
        },
        order_by: {
          _score: false,
        },
        facets: {
          tags: {
            field: "tags",
            size: 10,
          },
        },
      },
      null,
      2
    ),
  },
};

export const WithoutQueryNode: Story = {
  args: {
    value: JSON.stringify(
      {
        full_text_search: {
          match_all: {},
        },
        order_by: {
          _score: false,
        },
        facets: {
          tags: {
            field: "tags",
            size: 10,
          },
        },
      },
      null,
      2
    ),
    showQueryNode: false,
  },
};

export const WithoutOrderByAndFacets: Story = {
  args: {
    value: JSON.stringify(
      {
        full_text_search: {
          match_all: {},
        },
        order_by: {
          _score: false,
        },
        facets: {
          tags: {
            field: "tags",
            size: 10,
          },
        },
      },
      null,
      2
    ),
    showOrderByAndFacets: false,
  },
};

export const EmptyQuery: Story = {
  args: {
    value: JSON.stringify({}),
  },
};
