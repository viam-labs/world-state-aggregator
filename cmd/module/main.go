package main

import (
	"go.viam.com/rdk/module"
	"go.viam.com/rdk/resource"
	"go.viam.com/rdk/services/worldstatestore"

	wsaggregator "github.com/viam-labs/world-state-aggregator"
)

func main() {
	module.ModularMain(
		resource.APIModel{API: worldstatestore.API, Model: wsaggregator.DefaultModel},
	)
}
