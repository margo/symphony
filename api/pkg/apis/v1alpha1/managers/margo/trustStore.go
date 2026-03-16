package margo

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
)

var trustStoreLogger = logger.NewLogger("coa.truststore")

type DeviceTrustStore struct {
	sourceDir   string
	TrustedKeys map[string]bool
	mu          sync.RWMutex
}

func NewDeviceTrustStore(sourceDir string) *DeviceTrustStore {
	if sourceDir == "" {
		return nil
	}
	store := &DeviceTrustStore{
		sourceDir: sourceDir,
	}
	// Load initial keys
	store.refresh(context.Background())
	return store
}

func (s *DeviceTrustStore) refresh(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear existing keys
	s.TrustedKeys = make(map[string]bool)

	// TODO: add directory, and file permission checks

	// NOTE: we read trusted device keys from directory, and this is one way of implementation
	// a different implementation might handle all of this via APIs, or something else
	entries, err := os.ReadDir(s.sourceDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			trustStoreLogger.InfofCtx(ctx, "DeviceTrustStore: sourceDir does not exist (%s), clearing trusted keys", s.sourceDir)
			return
		}
		trustStoreLogger.ErrorfCtx(ctx, "DeviceTrustStore: unable to read the source dir content %s: %w", s.sourceDir, err)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		filePath := filepath.Join(s.sourceDir, filename)
		data, err := os.ReadFile(filePath)
		if err != nil {
			if os.IsPermission(err) {
				trustStoreLogger.ErrorfCtx(ctx, "DeviceTrustStore: permission denied reading file: %s", filePath)
			} else {
				trustStoreLogger.InfofCtx(ctx, "DeviceTrustStore: failed to read file: %s", filePath)
			}
			continue
		}

		// Validate key format: ensure non-empty
		if len(data) == 0 {
			trustStoreLogger.WarnfCtx(ctx, "DeviceTrustStore: skipping empty key file: %s", filePath)
			continue
		}

		// Store the key
		s.TrustedKeys[string(data)] = true
	}

	trustStoreLogger.InfofCtx(ctx, "DeviceTrustStore: loaded %d trusted keys from %s", len(s.TrustedKeys), s.sourceDir)
}

func (s *DeviceTrustStore) IsDeviceInTrustStore(ctx context.Context, deviceKeyToSearch []byte) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.TrustedKeys[string(deviceKeyToSearch)]
	return ok, nil
}
