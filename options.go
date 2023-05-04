package mqdedup

func WithRetentionSeconds[T any](seconds int) Option[T] {
	return func(c *Config[T]) {
		c.retentionSeconds = seconds
	}
}

func WithWorkerSeconds[T any](seconds int) Option[T] {
	return func(c *Config[T]) {
		c.workerSeconds = seconds
	}
}

func WithWorker[T any](worker WorkerFunc[T]) Option[T] {
	return func(c *Config[T]) {
		c.workerFunc = worker
	}
}

func WithIdFunc[T any](idFunc IdFunc[T]) Option[T] {
	return func(c *Config[T]) {
		c.idFunc = idFunc
	}
}

func WithPersist[T any](persist Persist) Option[T] {
	return func(c *Config[T]) {
		c.persist = persist
	}
}

func applyOptions[T any](c *Config[T], opts ...Option[T]) {
	for _, opt := range opts {
		opt(c)
	}
}
