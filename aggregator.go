package wsaggregator

import (
	"context"

	commonpb "go.viam.com/api/common/v1"

	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	"go.viam.com/rdk/services/worldstatestore"
)

func init() {
	resource.RegisterService(
		worldstatestore.API,
		DefaultModel,
		resource.Registration[worldstatestore.Service, *Config]{
			Constructor: newAggregator,
		},
	)
}

func newAggregator(
	ctx context.Context,
	deps resource.Dependencies,
	conf resource.Config,
	logger logging.Logger,
) (worldstatestore.Service, error) {
	return &aggregator{
		Named:  conf.ResourceName().AsNamed(),
		logger: logger,
	}, nil
}

type aggregator struct {
	resource.Named
	resource.TriviallyReconfigurable
	resource.TriviallyCloseable

	logger logging.Logger
}

func (a *aggregator) ListUUIDs(ctx context.Context, extra map[string]any) ([][]byte, error) {
	return nil, nil
}

func (a *aggregator) GetTransform(ctx context.Context, uuid []byte, extra map[string]any) (*commonpb.Transform, error) {
	return nil, worldstatestore.ErrNilResponse
}

func (a *aggregator) StreamTransformChanges(ctx context.Context, extra map[string]any) (*worldstatestore.TransformChangeStream, error) {
	ch := make(chan worldstatestore.TransformChange)
	close(ch)
	return worldstatestore.NewTransformChangeStreamFromChannel(ctx, ch), nil
}
