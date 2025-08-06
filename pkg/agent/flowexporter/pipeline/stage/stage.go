package stage

import (
	"context"

	"antrea.io/antrea/pkg/agent/flowexporter/connection"
)

type Stage interface {
	Run(ctx context.Context, input <-chan *connection.Connection) <-chan *connection.Connection
}
