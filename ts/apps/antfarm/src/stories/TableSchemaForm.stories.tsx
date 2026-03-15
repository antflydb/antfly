import type { Meta, StoryObj } from "@storybook/react-vite";
import { fn } from "storybook/test";
import TableSchemaForm from "../components/schema-builder/TableSchemaForm";

const meta: Meta<typeof TableSchemaForm> = {
  title: "SchemaBuilder/TableSchemaForm",
  component: TableSchemaForm,
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
    onSubmit: fn(),
  },
};

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    theme: "light",
  },
};
