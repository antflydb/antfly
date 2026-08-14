module github.com/antflydb/antfly/go/pkg/memoryaf

go 1.26.0

replace github.com/antflydb/antfly/go/pkg/libaf => ../libaf

replace github.com/antflydb/antfly/go/pkg/antfly => ../antfly

replace github.com/antflydb/antfly/go/pkg/generating => ../generating

replace github.com/antflydb/antfly/go/pkg/sdk => ../sdk

require (
	github.com/antflydb/antfly/go/pkg/antfly v0.0.0
	github.com/antflydb/antfly/go/pkg/sdk v0.0.1
	github.com/google/uuid v1.6.0
	github.com/modelcontextprotocol/go-sdk v1.4.1
	go.uber.org/zap v1.27.1
)

require (
	github.com/antflydb/antfly/go/pkg/generating v0.0.0 // indirect
	github.com/antflydb/antfly/go/pkg/libaf v0.0.1 // indirect
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/getkin/kin-openapi v0.142.0 // indirect
	github.com/go-json-experiment/json v0.0.0-20260214004413-d219187c3433 // indirect
	github.com/go-openapi/jsonpointer v0.23.1 // indirect
	github.com/go-openapi/swag/jsonname v0.26.0 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/google/jsonschema-go v0.4.2 // indirect
	github.com/kaptinlin/go-i18n v0.2.12 // indirect
	github.com/kaptinlin/jsonpointer v0.4.17 // indirect
	github.com/kaptinlin/jsonschema v0.7.6 // indirect
	github.com/kaptinlin/messageformat-go v0.4.18 // indirect
	github.com/oapi-codegen/runtime v1.6.0 // indirect
	github.com/oasdiff/yaml v0.1.1 // indirect
	github.com/oasdiff/yaml3 v0.0.14 // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/segmentio/encoding v0.5.4 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
)
