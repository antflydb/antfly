import type { Meta, StoryObj } from "@storybook/react-vite";
import { useState } from "react";
import { fn } from "storybook/test";
import { Button } from "@/components/ui/button";
import { ImportJsonDialog } from "../components/schema-builder/ImportJsonDialog";

const meta = {
  title: "SchemaBuilder/ImportJsonDialog",
  component: ImportJsonDialog,
  parameters: {
    layout: "centered",
  },
  tags: ["autodocs"],
  decorators: [(Story) => <Story />],
  args: {
    onClose: fn(),
    onImport: fn(),
  },
} satisfies Meta<typeof ImportJsonDialog>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    open: false,
  },
  render: function Render(args) {
    const [open, setOpen] = useState(false);
    return (
      <>
        <Button onClick={() => setOpen(true)}>Open Dialog</Button>
        <ImportJsonDialog {...args} open={open} onClose={() => setOpen(false)} />
      </>
    );
  },
};
