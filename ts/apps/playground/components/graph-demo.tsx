"use client";

import { ForceGraph, type GraphData } from "@antfly/graph";

const data: GraphData = {
  nodes: [
    // Concepts
    { id: "nn", label: "Neural Networks", type: "concept", metric: 4200 },
    { id: "dl", label: "Deep Learning", type: "concept", metric: 3800 },
    { id: "nlp", label: "NLP", type: "concept", metric: 3000 },
    { id: "cv", label: "Computer Vision", type: "concept", metric: 2800 },
    { id: "rl", label: "Reinforcement Learning", type: "concept", metric: 1800 },
    { id: "gan", label: "GANs", type: "concept", metric: 1500 },
    { id: "backprop", label: "Backpropagation", type: "concept", metric: 2200 },

    // People
    { id: "hinton", label: "Geoffrey Hinton", type: "person", metric: 3500 },
    { id: "lecun", label: "Yann LeCun", type: "person", metric: 3000 },
    { id: "bengio", label: "Yoshua Bengio", type: "person", metric: 2800 },
    { id: "goodfellow", label: "Ian Goodfellow", type: "person", metric: 1600 },
    { id: "vaswani", label: "Ashish Vaswani", type: "person", metric: 2000 },

    // Technology
    { id: "transformer", label: "Transformer", type: "technology", metric: 4000 },
    { id: "cnn", label: "CNN", type: "technology", metric: 3200 },
    { id: "rnn", label: "RNN", type: "technology", metric: 2000 },
    { id: "lstm", label: "LSTM", type: "technology", metric: 1800 },
    { id: "attention", label: "Attention Mechanism", type: "technology", metric: 3400 },
    { id: "diffusion", label: "Diffusion Models", type: "technology", metric: 2200 },

    // Fields
    { id: "ml", label: "Machine Learning", type: "field", metric: 4500 },
    { id: "ai", label: "Artificial Intelligence", type: "field", metric: 5000 },
    { id: "stats", label: "Statistics", type: "field", metric: 1200 },

    // Methods
    { id: "sgd", label: "SGD", type: "method", metric: 1400 },
    { id: "dropout", label: "Dropout", type: "method", metric: 1200 },
    { id: "batchnorm", label: "Batch Normalization", type: "method", metric: 1000 },
    { id: "transfer", label: "Transfer Learning", type: "method", metric: 1600 },
    { id: "selfsup", label: "Self-Supervised Learning", type: "method", metric: 1800 },
  ],
  edges: [
    // Field relationships
    { source: "ai", target: "ml", weight: 5 },
    { source: "ml", target: "dl", weight: 4 },
    { source: "ml", target: "stats", weight: 3 },
    { source: "dl", target: "nn", weight: 5 },
    { source: "dl", target: "nlp", weight: 3 },
    { source: "dl", target: "cv", weight: 3 },
    { source: "dl", target: "rl", weight: 2 },

    // People → contributions
    { source: "hinton", target: "nn", weight: 5 },
    { source: "hinton", target: "backprop", weight: 4 },
    { source: "hinton", target: "dropout", weight: 3 },
    { source: "lecun", target: "cnn", weight: 5 },
    { source: "lecun", target: "cv", weight: 3 },
    { source: "bengio", target: "nlp", weight: 4 },
    { source: "bengio", target: "rnn", weight: 3 },
    { source: "bengio", target: "attention", weight: 3 },
    { source: "goodfellow", target: "gan", weight: 5 },
    { source: "vaswani", target: "transformer", weight: 5 },
    { source: "vaswani", target: "attention", weight: 4 },

    // Technology connections
    { source: "transformer", target: "attention", weight: 5 },
    { source: "transformer", target: "nlp", weight: 4 },
    { source: "cnn", target: "cv", weight: 4 },
    { source: "rnn", target: "nlp", weight: 3 },
    { source: "rnn", target: "lstm", weight: 4 },
    { source: "lstm", target: "nlp", weight: 2 },
    { source: "gan", target: "diffusion", weight: 2 },
    { source: "nn", target: "backprop", weight: 4 },
    { source: "nn", target: "sgd", weight: 3 },

    // Methods
    { source: "sgd", target: "backprop", weight: 3 },
    { source: "dropout", target: "nn", weight: 2 },
    { source: "batchnorm", target: "nn", weight: 2 },
    { source: "transfer", target: "dl", weight: 3 },
    { source: "transfer", target: "cv", weight: 2 },
    { source: "selfsup", target: "transformer", weight: 3 },
    { source: "selfsup", target: "dl", weight: 2 },

    // Cross-field
    { source: "diffusion", target: "cv", weight: 3 },
    { source: "transformer", target: "cv", weight: 2 },
  ],
};

export function GraphDemo() {
  return (
    <div className="h-[520px] min-w-0 w-full basis-full">
      <ForceGraph
        data={data}
        colorConfig={{
          concept: { label: "Concept" },
          person: { label: "Person" },
          technology: { label: "Technology" },
          field: { label: "Field" },
          method: { label: "Method" },
        }}
        layoutOptions={{ chargeStrength: -250, linkDistance: 90 }}
      />
    </div>
  );
}
