package wsaggregator

// Config is the configuration for the world-state-aggregator service.
type Config struct {
	// SubscriberBufferSize bounds each subscriber's channel. Default 64 when zero
	// or unset. When a subscriber's buffer fills, changes are dropped and logged
	// for that subscriber only; producers are never blocked.
	SubscriberBufferSize int `json:"subscriber_buffer_size,omitempty"`
}

// Validate ensures the config is well-formed. The aggregator has no upstream
// dependencies, so this never returns required/optional dep lists.
func (c *Config) Validate(path string) ([]string, []string, error) {
	return nil, nil, nil
}
