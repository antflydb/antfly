import type { Meta, StoryObj } from "@storybook/react-vite";
import { FormProvider, useForm } from "react-hook-form";
import { fn } from "storybook/test";
import IndexField from "../components/schema-builder/IndexField";

const meta: Meta<typeof IndexField> = {
  title: "SchemaBuilder/IndexField",
  component: IndexField,
  parameters: {
    layout: "centered",
  },
  tags: ["autodocs"],
  decorators: [
    (Story) => {
      const methods = useForm({
        defaultValues: {
          indexes: [
            {
              name: "my_index",
              sourceType: "field",
              field: "content",
              embedder: {
                provider: "ollama",
                model: "all-minilm",
              },
            },
          ],
        },
      });
      return (
        <FormProvider {...methods}>
          <div className="w-[600px]">
            <Story />
          </div>
        </FormProvider>
      );
    },
  ],
  args: {
    onRemove: fn(),
  },
};

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    index: 0,
    schemaFields: ["title", "content"],
  },
};

export const WithTemplate: Story = {
  decorators: [
    (Story) => {
      const methods = useForm({
        defaultValues: {
          indexes: [
            {
              name: "my_template_index",
              sourceType: "template",
              template: "Title: {{.title}} Content: {{.content}}",
              field: "",
              embedder: {
                provider: "openai",
                model: "text-embedding-3-small",
              },
            },
          ],
        },
      });
      return (
        <FormProvider {...methods}>
          <div className="w-[600px]">
            <Story />
          </div>
        </FormProvider>
      );
    },
  ],
  args: {
    index: 0,
    schemaFields: ["title", "content"],
  },
};
