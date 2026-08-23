module github.com/antflydb/antfly/go/pkg/libaf

go 1.26.0

// go-yit 20250909 pulls in go.yaml.in/yaml/v4, which breaks yaml-jsonpath.
replace (
	github.com/antflydb/antfly/go/pkg/generating => ../generating
	github.com/dprotaso/go-yit v0.0.0-20250909171706-0a81c39169bc => github.com/dprotaso/go-yit v0.0.0-20250513224043-18a80f8f6df4
)

require (
	github.com/antflydb/antfly/go/pkg/generating v0.0.0
	github.com/minio/minio-go/v7 v7.0.99
	github.com/oapi-codegen/runtime v1.6.0
	github.com/prometheus/client_golang v1.23.2
	github.com/stretchr/testify v1.11.1
	go.uber.org/zap v1.27.1
	go.yaml.in/yaml/v3 v3.0.4
)

require (
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/go-openapi/swag/jsonname v0.26.0 // indirect
	github.com/klauspost/crc32 v1.3.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.67.5 // indirect
	github.com/prometheus/procfs v0.20.1 // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2 // indirect
	github.com/sergi/go-diff v1.4.0 // indirect
	github.com/speakeasy-api/openapi v1.24.0 // indirect
	github.com/tinylib/msgp v1.6.3 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

require (
	github.com/dprotaso/go-yit v0.0.0-20250909171706-0a81c39169bc // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/getkin/kin-openapi v0.142.0
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/go-openapi/jsonpointer v0.23.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/minio/crc64nvme v1.1.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/oapi-codegen/oapi-codegen/v2 v2.8.0 // indirect
	github.com/oasdiff/yaml v0.1.1 // indirect
	github.com/oasdiff/yaml3 v0.0.14 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/speakeasy-api/jsonpath v0.6.3 // indirect
	github.com/vmware-labs/yaml-jsonpath v0.3.2 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	golang.org/x/tools v0.48.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

tool github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen
