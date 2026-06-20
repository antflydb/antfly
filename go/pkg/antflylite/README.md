# Antfly Lite Go Binding

`go/pkg/antflylite` is the first language binding above the stable Zig and C
Antfly Lite APIs. It wraps the C ABI, so applications embed a live `.aflite`
database directly instead of talking to the network SDK.

Build the C library before running cgo-backed tests from the source tree:

```sh
cd zig
zig build lite-capi
cd ../go/pkg/antflylite
go test -tags antflylite_capi ./...
```

Normal `go test ./...` does not run the C ABI smoke test. The
`antflylite_capi` tag is intentional so package consumers do not need a freshly
built `libantflylite` unless they are testing the local binding against the
source-tree C library.
