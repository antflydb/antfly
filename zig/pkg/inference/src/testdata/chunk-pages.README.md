# Chunk page fixture

`chunk-pages.pdf` is an original Apache-2.0 synthetic PDF with two 64×64-point
pages. Page 1 draws “Hello chunk world” in Helvetica at 8 points; page 2 is blank.
It exercises text extraction and page-image fallback within the same document.
All seven PDF objects, stream lengths, byte offsets and the xref table are plain
text in the file. It contains no external content, embedded fonts or annotations.
The all-media chunk parity runner uses the same fixture as the native tests.
