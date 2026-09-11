package trustbundle

// Package trustbundle provides utilities including trust material caching
// for SPIFFE/MARGO identity and authorization framework (MIAF).

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	"github.com/margo/sandbox/shared-lib/mis/trustbundle"
)

// ── Configuration ─────────────────────────────────────────────────────────────

// TrustMaterialCacherConfig holds all configuration required to initialise a
// TrustMaterialCacher.
type TrustMaterialCacherConfig struct {
	// MISEndpoint is the base URL of the Margo Infrastructure Service,
	// e.g. "https://mis.margo.org:9443".
	MISEndpoint string

	// MISCAPem is the PEM-encoded CA certificate used for TLS verification
	// when connecting to the MIS endpoint.
	MISCAPem []byte

	// TrustBundleURI is an optional well-known URI path
	// (e.g. "/.well-known/spiffe/bundle.json"). May be empty.
	TrustBundleURI string

	// TrustBundleJSON is an optional operator-supplied fallback SPIFFE bundle
	// in JWKS JSON format. May be nil.
	TrustBundleJSON []byte

	// TrustDomain is the SPIFFE trust domain (e.g. "margo.org").
	// May be empty if not yet known; the MIS discovery response will populate it.
	TrustDomain string

	// Interval is the polling cadence in seconds at which the trust bundle and
	// trust domain are refreshed. If zero or negative, defaults to 60 seconds.
	// This value is overridden by the spiffe_refresh_hint field in the fetched
	// bundle when that field is present and positive.
	Interval int

	// for logging
	logger logger.Logger
}

// ── Interface ─────────────────────────────────────────────────────────────────

// TrustMaterialCacherIfc defines the public contract for a trust-material cache.
type TrustMaterialCacherIfc interface {
	// Start performs the initial trust-bundle fetch, seeds the cache, and
	// launches the background refresh goroutine. Returns an error if the
	// initial fetch fails.
	Start() error

	// Stop signals the background refresh goroutine to exit and blocks until
	// it has done so.
	Stop()

	// GetTrustBundle returns the most recently cached raw SPIFFE bundle bytes
	// (JWKS JSON format). Returns nil if the cache has not been seeded yet.
	GetTrustBundle() []byte

	// GetTrustDomain returns the most recently cached SPIFFE trust domain
	// string. Returns "" if the cache has not been seeded yet.
	GetTrustDomain() string
}

// ── Atomic string helper ──────────────────────────────────────────────────────

// atomicString provides lock-free load/store semantics for a string value by
// storing a pointer to the underlying string header.
type atomicString struct {
	p unsafe.Pointer // *string
}

func (a *atomicString) Load() string {
	p := atomic.LoadPointer(&a.p)
	if p == nil {
		return ""
	}
	return *(*string)(p)
}

func (a *atomicString) Store(s string) {
	atomic.StorePointer(&a.p, unsafe.Pointer(&s))
}

// ── Atomic bytes helper ───────────────────────────────────────────────────────

// atomicBytes provides lock-free load/store semantics for a []byte value.
type atomicBytes struct {
	p unsafe.Pointer // *[]byte
}

func (a *atomicBytes) Load() []byte {
	p := atomic.LoadPointer(&a.p)
	if p == nil {
		return nil
	}
	return *(*[]byte)(p)
}

func (a *atomicBytes) Store(b []byte) {
	atomic.StorePointer(&a.p, unsafe.Pointer(&b))
}

// ── TrustMaterialCacher ───────────────────────────────────────────────────────

// trustBundle groups the cached SPIFFE bundle value and its associated ETag so
// that conditional HTTP requests (If-None-Match) can be made on refresh.
type cachedTrustBundle struct {
	etag  atomicString // ETag returned by the MIS server for the bundle
	value atomicBytes  // raw SPIFFE bundle bytes (JWKS JSON)
}

// cachedTrustDomain groups the cached trust-domain value and its ETag.
type cachedTrustDomain struct {
	etag  atomicString // ETag associated with the discovery document
	value atomicString // SPIFFE trust domain string, e.g. "margo.org"
}

// TrustMaterialCacher is the concrete implementation of TrustMaterialCacherIfc.
// All exported state is accessed through atomic helpers; no mutex is required
// for the hot read path.
type TrustMaterialCacher struct {
	// ── cached material ──────────────────────────────────────────────────────
	trustBundle cachedTrustBundle
	trustDomain cachedTrustDomain

	// ── configuration ────────────────────────────────────────────────────────
	cfg TrustMaterialCacherConfig

	// ── runtime helpers ──────────────────────────────────────────────────────
	getter   trustbundle.Getter // MIS trust-bundle retrieval client
	interval time.Duration      // current refresh interval (may be updated by spiffe_refresh_hint)
	stopCh   chan struct{}      // closed by Stop() to signal the refresh goroutine
	doneCh   chan struct{}      // closed by the refresh goroutine when it exits
}

// New validates cfg and returns an initialised TrustMaterialCacherIfc.
// The returned cacher is ready to use; call Start() to begin background refresh.
func New(cfg TrustMaterialCacherConfig) TrustMaterialCacherIfc {
	const defaultInterval = 60 * time.Second

	interval := time.Duration(cfg.Interval) * time.Second
	if interval <= 0 {
		interval = defaultInterval
	}

	return &TrustMaterialCacher{
		cfg:      cfg,
		interval: interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// ── TrustMaterialCacherIfc implementation ─────────────────────────────────────

// Start performs the initial trust-bundle fetch, seeds the in-memory cache, and
// launches the background refresh goroutine.
//
// Errors during the initial fetch are returned immediately; the background
// goroutine is NOT started in that case.
func (c *TrustMaterialCacher) Start() error {
	c.cfg.logger.DebugCtx(
		context.Background(),
		"TrustMaterialCacher: starting; endpoint=%s interval=%s",
		c.cfg.MISEndpoint, c.interval,
	)

	// ── Build the trustbundle.Getter ─────────────────────────────────────────
	getter, err := trustbundle.New(
		c.cfg.MISEndpoint,
		c.cfg.MISCAPem,
		c.cfg.TrustBundleURI,
		c.cfg.TrustBundleJSON,
		c.cfg.TrustDomain,
	)
	if err != nil {
		return fmt.Errorf("TrustMaterialCacher: failed to create trust-bundle getter: %w", err)
	}
	c.getter = getter

	// ── Initial fetch ────────────────────────────────────────────────────────
	trustDomain, bundleBytes, etag, err := c.getter.GetTrustBundle(context.Background(), "")
	if err != nil {
		return fmt.Errorf("TrustMaterialCacher: initial trust-bundle fetch failed: %w", err)
	}

	// Seed the cache.
	c.trustBundle.value.Store(bundleBytes)
	c.trustBundle.etag.Store(etag)
	c.trustDomain.value.Store(trustDomain)

	c.cfg.logger.DebugCtx(
		context.Background(),
		"TrustMaterialCacher: initial fetch succeeded; trustDomain=%s etag=%q bundleLen=%d",
		trustDomain, etag, len(bundleBytes),
	)

	// ── Derive refresh interval from spiffe_refresh_hint ─────────────────────
	if hint, ok := extractRefreshHint(bundleBytes, c.cfg.logger); ok {
		hintDuration := time.Duration(hint) * time.Second
		c.cfg.logger.DebugCtx(
			context.Background(),
			"TrustMaterialCacher: using spiffe_refresh_hint=%d s from bundle (was %s)",
			hint, c.interval,
		)
		c.interval = hintDuration
	} else {
		c.cfg.logger.DebugCtx(
			context.Background(),
			"TrustMaterialCacher: no valid spiffe_refresh_hint in bundle; using configured interval=%s",
			c.interval,
		)
	}

	// ── Launch background refresh goroutine ───────────────────────────────────
	go c.refreshLoop()

	c.cfg.logger.DebugCtx(
		context.Background(),
		"TrustMaterialCacher: background refresh goroutine started",
	)
	return nil
}

// Stop signals the background refresh goroutine to exit and blocks until it
// has done so.
func (c *TrustMaterialCacher) Stop() {
	c.cfg.logger.DebugCtx(context.Background(), "TrustMaterialCacher: stopping")
	close(c.stopCh)
	<-c.doneCh
	c.cfg.logger.DebugCtx(context.Background(), "TrustMaterialCacher: stopped")
}

// GetTrustBundle returns the most recently cached raw SPIFFE bundle bytes.
// Returns nil if the cache has not been seeded yet.
func (c *TrustMaterialCacher) GetTrustBundle() []byte {
	return c.trustBundle.value.Load()
}

// GetTrustDomain returns the most recently cached SPIFFE trust domain string.
// Returns "" if the cache has not been seeded yet.
func (c *TrustMaterialCacher) GetTrustDomain() string {
	return c.trustDomain.value.Load()
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// refreshLoop is the background goroutine that periodically refreshes the
// cached trust material. It runs until Stop() closes stopCh.
//
// Errors during a refresh cycle are logged but do NOT terminate the loop; the
// stale cached values remain in place until a successful refresh.
func (c *TrustMaterialCacher) refreshLoop() {
	defer close(c.doneCh)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			c.cfg.logger.DebugCtx(
				context.Background(),
				"TrustMaterialCacher: refresh loop received stop signal; exiting",
			)
			return

		case <-ticker.C:
			c.cfg.logger.DebugCtx(
				context.Background(),
				"TrustMaterialCacher: refresh tick; fetching trust bundle",
			)
			c.refresh(ticker)
		}
	}
}

// refresh performs a single trust-bundle refresh cycle.
// On success it updates the cache and, if spiffe_refresh_hint has changed,
// resets the ticker to the new interval.
// On error it logs the failure and leaves the existing cached values intact.
func (c *TrustMaterialCacher) refresh(ticker *time.Ticker) {
	currentEtag := c.trustBundle.etag.Load()

	trustDomain, bundleBytes, newEtag, err := c.getter.GetTrustBundle(
		context.Background(),
		currentEtag,
	)
	if err != nil {
		// ErrNotModified is not a real error: the cached bundle is still valid.
		if err == trustbundle.ErrNotModified {
			c.cfg.logger.DebugCtx(
				context.Background(),
				"TrustMaterialCacher: trust bundle not modified (304); retaining cached values",
			)
			return
		}

		// Any other error: log and retain stale cache.
		c.cfg.logger.ErrorCtx(
			context.Background(),
			"TrustMaterialCacher: refresh failed; retaining stale cache: %s", err.Error(),
		)
		return
	}

	// Update the cache atomically.
	c.trustBundle.value.Store(bundleBytes)
	c.trustBundle.etag.Store(newEtag)
	c.trustDomain.value.Store(trustDomain)

	c.cfg.logger.DebugCtx(
		context.Background(),
		"TrustMaterialCacher: cache updated; trustDomain=%s etag=%q bundleLen=%d",
		trustDomain, newEtag, len(bundleBytes),
	)

	// Re-evaluate spiffe_refresh_hint and reset the ticker if it has changed.
	if hint, ok := extractRefreshHint(bundleBytes, c.cfg.logger); ok {
		newInterval := time.Duration(hint) * time.Second
		if newInterval != c.interval {
			c.cfg.logger.DebugCtx(
				context.Background(),
				"TrustMaterialCacher: spiffe_refresh_hint changed; resetting ticker from %s to %s",
				c.interval, newInterval,
			)
			c.interval = newInterval
			ticker.Reset(newInterval)
		}
	}
}

// spiffeBundle is a minimal representation of a SPIFFE JWKS bundle used solely
// to extract the spiffe_refresh_hint field.
type spiffeBundle struct {
	RefreshHint *int64 `json:"spiffe_refresh_hint"`
}

// extractRefreshHint parses bundleBytes as a SPIFFE JWKS bundle and returns the
// value of spiffe_refresh_hint if it is present and a positive integer.
// Returns (0, false) when the field is absent, zero, or negative.
func extractRefreshHint(bundleBytes []byte, logger logger.Logger) (int64, bool) {
	if len(bundleBytes) == 0 {
		return 0, false
	}

	var doc spiffeBundle
	if err := json.Unmarshal(bundleBytes, &doc); err != nil {
		logger.DebugCtx(
			context.Background(),
			"TrustMaterialCacher: failed to parse bundle for spiffe_refresh_hint: %s", err.Error(),
		)
		return 0, false
	}

	if doc.RefreshHint == nil || *doc.RefreshHint <= 0 {
		return 0, false
	}

	return *doc.RefreshHint, true
}
