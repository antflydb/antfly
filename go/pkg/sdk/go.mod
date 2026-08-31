module github.com/antflydb/antfly/go/pkg/sdk

go 1.26.0

// go-yit 20250909 pulls in go.yaml.in/yaml/v4 which breaks yaml-jsonpath.
replace github.com/dprotaso/go-yit v0.0.0-20250909171706-0a81c39169bc => github.com/dprotaso/go-yit v0.0.0-20250513224043-18a80f8f6df4

require (
	github.com/getkin/kin-openapi v0.142.0
	github.com/go-json-experiment/json v0.0.0-20260214004413-d219187c3433
	github.com/kaptinlin/jsonschema v0.7.6
	github.com/oapi-codegen/runtime v1.6.0
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dprotaso/go-yit v0.0.0-20250909171706-0a81c39169bc // indirect
	github.com/go-openapi/jsonpointer v0.23.1 // indirect
	github.com/go-openapi/swag/jsonname v0.26.0 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/kaptinlin/go-i18n v0.2.12 // indirect
	github.com/kaptinlin/jsonpointer v0.4.17 // indirect
	github.com/kaptinlin/messageformat-go v0.4.18 // indirect
	github.com/oapi-codegen/oapi-codegen/v2 v2.8.0 // indirect
	github.com/oasdiff/yaml v0.1.1 // indirect
	github.com/oasdiff/yaml3 v0.0.14 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2 // indirect
	github.com/speakeasy-api/jsonpath v0.6.3 // indirect
	github.com/speakeasy-api/openapi v1.24.0 // indirect
	github.com/vmware-labs/yaml-jsonpath v0.3.2 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	golang.org/x/tools v0.48.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

tool github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen
