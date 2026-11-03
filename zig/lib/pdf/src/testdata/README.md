# PDF decoder test data

`treasury-page25-symbols.b64` contains the first four embedded JBIG2 segments
from page 25 of the February 1939 *Treasury Bulletin*, published by the United
States Department of the Treasury and hosted by the Federal Reserve Bank of
St. Louis. The reduced stream includes page information, two arithmetic symbol
dictionaries (including refinement aggregation), and the immediate text
region. The trailing generic region is omitted because a compact synthetic
vector covers that path separately.

Source: https://fraser.stlouisfed.org/title/treasury-bulletin-407/february-1939-6505?page=25

`treasury-1985-page2-empty-refinement-dictionary.hex` contains the minimal
symbol-dictionary payload that previously caused native rendering to reject
page 2 of the March 1985 *Treasury Bulletin*. The dictionary enables refinement
aggregation but has zero imported, new, and exported symbols, followed by the
original arithmetic terminator bytes.

Source: https://fraser.stlouisfed.org/files/docs/publications/tbulletin/1985_03_treasurybulletin.pdf
SHA-256: `6fcfabf9f21f9e8bc0565eed1f9430ecc18c543ee540e066ed24329e69e04b70`

## NASA Hubble Servicing Mission 3A OCR text

`nasa-hubble-sm3a.pdf` is the unmodified public NASA NTRS report 20000025316,
"Hubble Space Telescope Servicing Mission 3A" (FS-1999-10-033-GSFC).
Source: https://ntrs.nasa.gov/api/citations/20000025316/downloads/20000025316.pdf
SHA-256: `d12bf7d43556c870cc7a724e1ae072eba4da4f8387bfb4390b49d68d95561eb3`.

Its scanned pages have invisible (`3 Tr`) positioned OCR words. Excluding those
runs from extraction produced one concatenated string per page. Regression tests
check both plain and analyzed extraction, searchable phrase spacing, span offsets,
and the analysis run limit. Invisible text remains excluded from paint runs.
