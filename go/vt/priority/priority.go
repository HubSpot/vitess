package priority

import (
	"context"

	"vitess.io/vitess/go/vt/log"
)

type Priority int

type key int

var priorityKey key = 1

const (
	Penalized Priority = iota
	Low
	Medium
	High
	User
	Critical

	SupportedPriorities int = int(Critical) + 1
)

func NewPriority(priority string) Priority {
	switch priority {
	case "PENALIZED":
		return Penalized
	case "LOW":
		return Low
	case "UNKNOWN", "MEDIUM":
		return Medium
	case "HIGH":
		return High
	case "USER":
		return User
	case "CRITICAL":
		return Critical

	default:
		log.Errorf("Invalid priority: %v", priority)
		return Low
	}
}

// NewContext adds the provided Priority to the context
func NewContext(ctx context.Context, p Priority) context.Context {
	return context.WithValue(ctx, priorityKey, p)
}

// FromContext returns the Priority value stored in ctx, if any.
func FromContext(ctx context.Context) (Priority, bool) {
	ci, ok := ctx.Value(priorityKey).(Priority)
	return ci, ok
}
