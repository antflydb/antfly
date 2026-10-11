# VP9 source ownership

See [the maintained port contract](../PORTING.md). The initial fixtures qualify
profiles 0–3 at 8/10/12-bit across 4:2:0, 4:2:2 and 4:4:4, odd dimensions,
interframes and superframes. These fixtures establish reconstruction parity for
those streams, not exhaustive profile conformance. Public calls configure one
worker and do not expose the reference implementation's variadic control API.
