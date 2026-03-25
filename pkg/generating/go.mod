module github.com/antflydb/antfly/pkg/generating

go 1.26.0

// Pin deps compatible with oapi-codegen v2.5.1.
// kin-openapi v0.134.0 breaks oapi-codegen (MappingRef type change) and
// oasdiff/yaml v0.0.1 breaks kin-openapi v0.133.0 (OriginOpt API change).
replace (
	github.com/antflydb/antfly/pkg/genkit/openrouter => ../genkit/openrouter
	github.com/antflydb/antfly/pkg/libaf => ../libaf
	github.com/antflydb/termite/pkg/client => ../../termite/pkg/client
	github.com/dprotaso/go-yit v0.0.0-20250909171706-0a81c39169bc => github.com/dprotaso/go-yit v0.0.0-20250513224043-18a80f8f6df4
	github.com/getkin/kin-openapi v0.134.0 => github.com/getkin/kin-openapi v0.133.0
	github.com/oasdiff/yaml v0.0.1 => github.com/oasdiff/yaml v0.0.0-20250309154309-f31be36b4037
	github.com/oasdiff/yaml3 v0.0.1 => github.com/oasdiff/yaml3 v0.0.0-20250309153720-d2182401db90
)

require (
	github.com/ajroetker/pdf/render v0.0.1-antfly003
	github.com/antflydb/antfly/pkg/genkit/openrouter v0.0.0
	github.com/antflydb/antfly/pkg/libaf v0.0.1
	github.com/antflydb/termite/pkg/client v0.0.0
	github.com/firebase/genkit/go v1.5.0
	github.com/getkin/kin-openapi v0.134.0
	github.com/oapi-codegen/runtime v1.3.0
	github.com/openai/openai-go v1.12.0
	github.com/sethvargo/go-retry v0.3.0
)

require (
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/auth v0.18.2 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	github.com/ajroetker/go-highway v0.0.12 // indirect
	github.com/ajroetker/go-jpeg2000 v0.0.2 // indirect
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/buger/jsonparser v1.1.2 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dprotaso/go-yit v0.0.0-20250909171706-0a81c39169bc // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/jsonpointer v0.22.5 // indirect
	github.com/go-openapi/swag/jsonname v0.25.5 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/google/dotprompt/go v0.0.0-20260227225921-0911cf9ecf0e // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.14 // indirect
	github.com/googleapis/gax-go/v2 v2.19.0 // indirect
	github.com/gorilla/websocket v1.5.4-0.20250319132907-e064f32e3674 // indirect
	github.com/hhrutter/lzw v1.0.0 // indirect
	github.com/hhrutter/pkcs7 v0.2.0 // indirect
	github.com/hhrutter/tiff v1.0.2 // indirect
	github.com/invopop/jsonschema v0.13.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/crc32 v1.3.0 // indirect
	github.com/mailru/easyjson v0.9.2 // indirect
	github.com/mbleigh/raymond v0.0.0-20250414171441-6b3a58ab9e0a // indirect
	github.com/minio/crc64nvme v1.1.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.0.99 // indirect
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/oapi-codegen/oapi-codegen/v2 v2.5.1 // indirect
	github.com/oasdiff/yaml v0.0.1 // indirect
	github.com/oasdiff/yaml3 v0.0.1 // indirect
	github.com/pdfcpu/pdfcpu v0.11.1 // indirect
	github.com/perimeterx/marshmallow v1.1.5 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/revrost/go-openrouter v1.1.7 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/speakeasy-api/jsonpath v0.6.2 // indirect
	github.com/speakeasy-api/openapi-overlay v0.10.3 // indirect
	github.com/tidwall/gjson v1.18.0 // indirect
	github.com/tidwall/match v1.2.0 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
	github.com/tinylib/msgp v1.6.3 // indirect
	github.com/vmware-labs/yaml-jsonpath v0.3.2 // indirect
	github.com/wk8/go-ordered-map/v2 v2.1.8 // indirect
	github.com/woodsbury/decimal128 v1.4.0 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.67.0 // indirect
	go.opentelemetry.io/otel v1.42.0 // indirect
	go.opentelemetry.io/otel/metric v1.42.0 // indirect
	go.opentelemetry.io/otel/sdk v1.42.0 // indirect
	go.opentelemetry.io/otel/trace v1.42.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.49.0 // indirect
	golang.org/x/image v0.37.0 // indirect
	golang.org/x/mod v0.34.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	golang.org/x/tools v0.43.0 // indirect
	gonum.org/v1/gonum v0.17.0 // indirect
	google.golang.org/genai v1.51.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260319201613-d00831a3d3e7 // indirect
	google.golang.org/grpc v1.79.3 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

tool github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen
