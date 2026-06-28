package wsaggregator

import "go.viam.com/rdk/resource"

var (
	NamespaceFamily = resource.ModelNamespace("viam").WithFamily("world-state-aggregator")
	DefaultModel    = NamespaceFamily.WithModel("default")
)
