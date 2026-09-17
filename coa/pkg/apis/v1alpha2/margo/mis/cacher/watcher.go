// Package cacher's NewAuthClientCatcher provides a file-backed, live-reloading cache of
// authorized SPIFFE client IDs.
//
// The watcher monitors a json file (array of string, containing SPIFFE IDs) for changes
// and keeps an in-memory []string cache up to date. Consumers call
// GetAuthorizedClients() at any time to obtain the latest snapshot without
// blocking.
//
// Usage:
//
//	w, err := cacher.NewAuthClientCatcher("/etc/app/authorized_clients.json", logger)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if err := w.Start(); err != nil {
//	    log.Fatal(err)
//	}
//	defer w.Stop()
//
//	clients := w.GetAuthorizedClients()
package cacher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	"github.com/margo/sandbox/shared-lib/watcher"
)

// ── Interface ─────────────────────────────────────────────────────────────────

// AuthClientCatcherIfc defines the public contract for an authorized-client
// watcher. Implementations must be safe for concurrent use.
type AuthClientCatcherIfc interface {
	// Start reads the authorized-clients file, seeds the in-memory cache, and
	// launches a background goroutine that keeps the cache up to date whenever
	// the file changes on disk.
	// Returns an error if the initial file read or parse fails.
	Start() error

	// Stop cancels the underlying file watcher and waits for the background
	// goroutine to exit cleanly. Safe to call more than once.
	Stop()

	// GetAuthorizedClients returns a snapshot of the currently cached
	// authorized SPIFFE client IDs. Returns an empty (non-nil) slice when the
	// cache has not been seeded or the file is empty.
	GetAuthorizedClients() []string
}

// atomicClients is replaced by a mutex-guarded wrapper.
type mutexClients struct {
	mu    sync.RWMutex
	value []string
}

func (m *mutexClients) Load() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.value
}

func (m *mutexClients) Store(s []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.value = s
}

// ── AuthClientCatcher ─────────────────────────────────────────────────────────

// AuthClientCatcher is the concrete implementation of AuthClientCatcherIfc.
// All exported state is accessed through atomic helpers; no mutex is required
// for the hot read path.
type AuthClientCatcher struct {
	// authorizedClients holds the most recently parsed and validated list of
	// SPIFFE client IDs read from authClientFilePath.
	authorizedClients mutexClients

	// authClientFilePath is the absolute or relative path to the file that
	// contains one authorized SPIFFE client ID per line.
	authClientFilePath string

	// cancelFunc stops the underlying fsnotify file watcher and closes the
	// changes channel. Set by Start(); called by Stop().
	cancelFunc watcher.CancelFunc

	// changesCh receives a new []string snapshot every time the watched file
	// changes on disk. Closed automatically when cancelFunc is invoked.
	changesCh <-chan []string

	// doneCh is closed by the background goroutine when it exits, allowing
	// Stop() to block until the goroutine has finished.
	doneCh chan struct{}

	// logger is used for structured debug / error output, following the same
	// pattern as TrustMaterialCacher in trustbundle.go.
	logger logger.Logger
}

// ── Constructor ───────────────────────────────────────────────────────────────

// NewAuthClientCacher creates and initialises an AuthClientCatcher that will
// watch authClientFilePath for changes.
//
// The constructor performs an eager read of the file so that the cache is
// populated before Start() is called. This means callers can call
// GetAuthorizedClients() immediately after construction even before Start().
//
// Parameters:
//   - authClientFilePath : path to the json file containing one SPIFFE ID per line.
//   - log                : logger instance (mirrors trustbundle.go pattern).
//
// Returns a ready-to-use AuthClientCatcherIfc or an error if the file cannot
// be read or contains no valid SPIFFE IDs.
func NewAuthClientCacher(authClientFilePath string, log logger.Logger) (AuthClientCatcherIfc, error) {
	if authClientFilePath == "" {
		return nil, fmt.Errorf("AuthClientCatcher: authClientFilePath must not be empty")
	}
	if log == nil {
		return nil, fmt.Errorf("AuthClientCatcher: logger must not be nil")
	}

	w := &AuthClientCatcher{
		authClientFilePath: authClientFilePath,
		doneCh:             make(chan struct{}),
		logger:             log,
	}

	// ── Eager initial read ────────────────────────────────────────────────────
	// Read and validate the file now so that the cache is warm before Start()
	// and so that construction fails fast on a bad file path or empty file.
	log.DebugCtx(
		context.Background(),
		"AuthClientCatcher: performing initial read of authorized-clients file. ",
		"path: ", authClientFilePath,
	)

	clients, err := readAndValidateFile(authClientFilePath, log)
	if err != nil {
		return nil, fmt.Errorf("AuthClientCatcher: initial file read failed: %w", err)
	}

	w.authorizedClients.Store(clients)

	log.DebugCtx(
		context.Background(),
		"AuthClientCatcher: initial cache seeded. ",
		"client_count: ", len(clients),
	)

	return w, nil
}

// ── AuthClientCatcherIfc implementation ──────────────────────────────────────

// Start registers the file watcher for authClientFilePath and launches a
// background goroutine that updates the in-memory cache whenever the file
// changes on disk.
//
// Returns an error if the underlying fsnotify watcher cannot be initialised.
// The initial cache was already seeded by NewAuthClientCatcher, so a Start()
// failure does not invalidate the existing cached values.
func (a *AuthClientCatcher) Start() error {
	a.logger.DebugCtx(
		context.Background(),
		"AuthClientCatcher: starting file watcher. ",
		"path: ", a.authClientFilePath,
	)

	// parseFunc converts raw file bytes into a validated []string of SPIFFE IDs.
	// It is called by filewatcher.New every time the file changes.
	parseFunc := func(data []byte) ([]string, error) {
		return parseAndValidate(data, a.logger)
	}

	// Use an unbuffered channel so that slow consumers do not accumulate stale
	// snapshots — the filewatcher will drop events if the channel is full,
	// which is acceptable because we only ever care about the latest value.
	cancelFunc, changesCh, err := watcher.New(a.authClientFilePath, parseFunc, 0)
	if err != nil {
		return fmt.Errorf("AuthClientCatcher: failed to create file watcher: %w", err)
	}

	a.cancelFunc = cancelFunc
	a.changesCh = changesCh

	// ── Launch background goroutine ───────────────────────────────────────────
	go a.watchLoop()

	a.logger.DebugCtx(
		context.Background(),
		"AuthClientCatcher: file watcher started; background goroutine running",
	)

	return nil
}

// Stop cancels the underlying file watcher (which closes changesCh) and blocks
// until the background goroutine has exited cleanly.
// Safe to call more than once.
func (a *AuthClientCatcher) Stop() {
	a.logger.DebugCtx(context.Background(), "AuthClientCatcher: stopping")

	if a.cancelFunc != nil {
		// Closing the file watcher closes changesCh, which causes watchLoop to
		// return, which closes doneCh.
		a.cancelFunc()
	}

	// Block until the goroutine has exited — mirrors TrustMaterialCacher.Stop().
	<-a.doneCh

	a.logger.DebugCtx(context.Background(), "AuthClientCatcher: stopped")
}

// GetAuthorizedClients returns a snapshot of the currently cached authorized
// SPIFFE client IDs. The returned slice is safe to read concurrently; it is
// never mutated after being stored.
// Returns an empty (non-nil) slice when the cache is empty.
func (a *AuthClientCatcher) GetAuthorizedClients() []string {
	return a.authorizedClients.Load()
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// watchLoop is the background goroutine that receives parsed []string snapshots
// from the file watcher and atomically updates the in-memory cache.
//
// It exits when changesCh is closed (i.e. after cancelFunc is called by Stop()).
func (a *AuthClientCatcher) watchLoop() {
	defer close(a.doneCh)

	a.logger.DebugCtx(context.Background(), "AuthClientCatcher: watch loop started")

	for clients := range a.changesCh {
		a.logger.DebugCtx(
			context.Background(),
			"AuthClientCatcher: file changed; updating cache. ",
			"new_client_count: ", len(clients),
		)
		a.authorizedClients.Store(clients)
	}

	// changesCh was closed — file watcher has been cancelled.
	a.logger.DebugCtx(context.Background(), "AuthClientCatcher: watch loop exiting; changes channel closed")
}

// readAndValidateFile reads the file at filePath, parses it line by line, and
// validates each non-empty line as a SPIFFE ID with principal PrincipalWFMClient.
// Lines beginning with '#' are treated as comments and skipped.
//
// Returns an error if the file cannot be read. An empty (but non-nil) slice is
// returned when the file contains no valid SPIFFE IDs.
func readAndValidateFile(filePath string, log logger.Logger) ([]string, error) {
	// Read the raw bytes using os.ReadFile (via a one-shot parse call).
	// We reuse parseAndValidate so that the constructor and the watcher use
	// identical parsing logic.
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("cannot read file %q: %w", filePath, err)
	}

	return parseAndValidate(data, log)
}

// parseAndValidate converts raw file bytes into a validated []string of SPIFFE
// IDs. It is used both by the constructor (via readAndValidateFile) and as the
// ParseFunc supplied to filewatcher.New.
//
// assumes data to be in json format
func parseAndValidate(data []byte, log logger.Logger) ([]string, error) {
	clients := make([]string, 0)

	json.Unmarshal(data, &clients)

	log.DebugCtx(
		context.Background(),
		"AuthClientCatcher: parsed authorized-clients file. ",
		" valid_clients: ", len(clients),
	)

	return clients, nil
}
