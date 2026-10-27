The `native-*.pptx` fixtures are small, deterministic OOXML archives created
specifically for the native Zig renderer tests. They contain no office macros,
external relationships, embedded applications or font dependencies.

- `native-pictures.pptx`: a 200-by-100-point slide with a red rectangle in the
  left half and a two-by-two RGBA picture in the right half. The picture's top
  quadrants are blue and green; its bottom quadrants are transparent and yellow.
  These colors verify image row direction and preservation of alpha.
- `native-layers.pptx`: the red rectangle belongs to the master. Its blue
  background is overridden by the layout's green background. A master
  placeholder containing a template prompt must be excluded. The master's red
  decoration must remain above the layout background, and the picture's
  transparent quadrant must reveal green.
- `native-groups.pptx`: the picture/rectangle pair is inside two nested groups.
  The inner group scales it by one half and translates it to (50, 25) points.
  An ordinary zero-sized root group transform remains the identity. Native
  pixel tests verify placement without changing public slide EMU coordinates.

ZIP entry timestamps are fixed to 2026-01-01. All visual content uses explicit
RGB colors and integer EMU extents; the embedded PNGs use ordinary RGB or RGBA samples.

`native-themed-text.pptx` adds a master-owned theme, a tinted accent fill and a
bold text box. Tests check font outlines, wrapped glyph metrics and embedded
CID-to-Unicode text recovery. `native-chart.pptx`, `native-line-chart.pptx` and
`native-pie-chart.pptx` use cached series (10, 20) and labels (Jan, Feb); pixel
tests distinguish bar heights, a stroked line and colored pie sectors. Formula
strings remain opaque and no workbook is fetched.

`native-many-pictures.pptx` extends the picture slide with 20 distinct solid RGB
PNGs, each 1100 by 1000 pixels. Decoding each distinct part creates its own
durable pixel grant. The fixture exercises bounded receipt storage beyond the
old raster JSON header limit and measured-cost replay after ownership handoff.
