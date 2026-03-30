package proxy

type BackendKind string

const (
	BackendStateful   BackendKind = "stateful"
	BackendServerless BackendKind = "serverless"
)

type RequestPolicy struct {
	View            string
	MaxLagRecords   uint64
	RequiredVersion *uint64
}

type NamespaceRoute struct {
	Tenant             string
	Table              string
	Namespace          string
	PreferredBackend   BackendKind
	AllowStateful      bool
	AllowServerless    bool
	StatefulURL        string
	ServerlessQueryURL string
	ServerlessAPIURL   string
}

type GatewayConfig struct {
	PublicAddr string
}
