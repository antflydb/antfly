# Image Search with CLIP

Search your image collection using text descriptions or find visually similar images using CLIP embeddings.

## Prerequisites

- Antfly running with Termite and ONNX Runtime (CLIP requires ONNX)
- CLIP model: `antfly termite pull openai/clip-vit-base-patch32`

## Step 1: Create the Table

Create a table with a CLIP embeddings index. The template combines the image URL and caption for multimodal embedding:

<!-- include: main.go#create_table -->

> **Note:** ONNX Runtime is experimental. If you encounter issues like "model not found" errors, empty results, or embeddings not being computed, try restarting Antfly. Check `antfly.log` for errors if problems persist.

## Step 2: Add a Sample Image

Let's add the famous [Utah teapot](https://en.wikipedia.org/wiki/Utah_teapot):

<!-- include: main.go#add_image -->

Antfly fetches and embeds the image automatically when using a URL.

## Step 3: Search with Text

<!-- include: main.go#search -->

The Utah teapot should appear as the top result:

```
Score: 0.0164, ID: utah_teapot
Score: 0.0161, ID: mmir_3bc4b3613ed9
Score: 0.0159, ID: mmir_83ca037bd2ad
...
```

## Batch Import with Timing

For larger datasets, here's how to import images in bulk. This example uses the [MMIR dataset](https://github.com/google-research/mmir) from Google Research:

<!-- include: main.go#batch_import -->

Example output:
```
Imported: 100 / 100
Imported 100 images in 45.2s (2.2 images/sec)
```

## Running the Example

```bash
# From the repository root
go run ./examples/image-search

# Or build and run
go build -o examples/image-search/image-search ./examples/image-search
./examples/image-search/image-search
```

To run the batch import, first download the MMIR dataset:
```bash
curl -o mmir_dataset.tsv.gz "https://storage.googleapis.com/gresearch/wit-retrieval/mmir_dataset_train-00000-of-00005.tsv.gz"
```

## Tips

**Use visual descriptions**: CLIP responds better to concrete visual concepts ("red sports car", "snowy mountain") than brand names or abstract terms.

**Captions affect results**: The template combines image + caption. For pure visual search, use `"template": "{{media url=image_url}}"` instead.

## Related

- [Multimodal Guide](/docs/guides/multimodal) - PDFs, audio, and remote content
- [Termite Models](/termite) - Available CLIP variants
