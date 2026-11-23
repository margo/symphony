package margo

import (
	"errors"
	"hash/fnv"
	"sync"

	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
)

// NBI and SBI managers have separate database wrappers. Share bounded locks
// across those wrappers, not just one manager instance. These locks coordinate
// one WFM process; they are not a distributed lock for multiple WFM replicas.
var bundleLocks [64]struct {
	generation sync.Mutex
	write      sync.Mutex
}

func bundleLockIndex(device string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(device))
	return h.Sum64() % uint64(len(bundleLocks))
}

func bundleNotFound(err error) bool {
	var stateError v1alpha2.COAError
	return errors.As(err, &stateError) && stateError.State == v1alpha2.NotFound
}
