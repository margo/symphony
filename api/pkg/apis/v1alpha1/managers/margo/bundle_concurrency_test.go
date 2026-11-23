package margo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/contexts"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/pubsub"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/states"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/states/memorystate"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	sbi "github.com/margo/sandbox/standard/generatedCode/wfm/sbi"
)

type bundleTestPublisher struct {
	pubsub.IPubSubProvider
	callback func() error
}

func (p *bundleTestPublisher) Publish(_ string, _ v1alpha2.Event) error { return p.callback() }

func TestBundlePublicationDoesNotHoldStorageLock(t *testing.T) {
	db, store := newBundleTestDB(t)
	seedBundle(t, db, store, "event-device", 5)
	row := DeploymentBundleRow{DeviceClientId: "event-device", Manifest: sbi.UnsignedAppStateManifest{ManifestVersion: 6, Deployments: []sbi.DeploymentManifestRef{}}}
	other := NewMargoDatabase(&contexts.ManagerContext{Logger: logger.NewLogger("coa.runtime")}, deviceManagerPublisherGroup, store)
	called := false
	db.MgrContext.PubsubProvider = &bundleTestPublisher{callback: func() error { called = true; return other.UpsertDeploymentBundle(context.Background(), row, false) }}
	done := make(chan error, 1)
	go func() { done <- db.UpsertDeploymentBundle(context.Background(), row, true) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("synchronous cache publication deadlocked")
	}
	if !called {
		t.Fatal("bundle event not published")
	}
}

type bundleTestStore struct {
	states.IStateProvider
	beforeWrite func(DeploymentBundleRow)
	getError    error
	writeError  error
	writes      atomic.Int32
	deletes     atomic.Int32
}

func (s *bundleTestStore) Get(ctx context.Context, req states.GetRequest) (states.StateEntry, error) {
	if req.Metadata["resource"] == "deploymentBundle" && s.getError != nil {
		return states.StateEntry{}, s.getError
	}
	return s.IStateProvider.Get(ctx, req)
}

func (s *bundleTestStore) List(ctx context.Context, req states.ListRequest) ([]states.StateEntry, string, error) {
	// This fixture seeds bundle rows only. MemoryStateProvider otherwise lists
	// every kind in a namespace, unlike the Redis provider's resource partition.
	if req.Metadata["resource"] == "deployments" {
		return nil, "", nil
	}
	return s.IStateProvider.List(ctx, req)
}

func (s *bundleTestStore) Upsert(ctx context.Context, req states.UpsertRequest) (string, error) {
	if row, ok := req.Value.Body.(DeploymentBundleRow); ok {
		s.writes.Add(1)
		if s.beforeWrite != nil {
			s.beforeWrite(row)
		}
		if s.writeError != nil {
			return "", s.writeError
		}
	}
	return s.IStateProvider.Upsert(ctx, req)
}

func TestBundleWriteFailurePreservesPreviousRevision(t *testing.T) {
	db, store := newBundleTestDB(t)
	seedBundle(t, db, store, "device", 5)
	store.writeError = errors.New("write unavailable")
	err := db.UpsertDeploymentBundle(context.Background(), DeploymentBundleRow{DeviceClientId: "device", Manifest: sbi.UnsignedAppStateManifest{ManifestVersion: 6}}, false)
	if err == nil {
		t.Fatal("expected failed replacement")
	}
	row, err := db.GetDeploymentBundle(context.Background(), "device")
	if err != nil || row.Manifest.ManifestVersion != 5 {
		t.Fatalf("previous revision lost: row=%+v err=%v", row, err)
	}
}

func TestBundleRejectsLateOrConflictingWrites(t *testing.T) {
	db, store := newBundleTestDB(t)
	seedBundle(t, db, store, "device", 5)
	for _, version := range []int{4, 5} {
		err := db.UpsertDeploymentBundle(context.Background(), DeploymentBundleRow{DeviceClientId: "device", Manifest: sbi.UnsignedAppStateManifest{ManifestVersion: sbi.ManifestVersion(version)}, ArchivePath: "different"}, false)
		if err == nil {
			t.Errorf("accepted late/conflicting version %d", version)
		}
	}
	row, err := db.GetDeploymentBundle(context.Background(), "device")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertDeploymentBundle(context.Background(), *row, false); err != nil {
		t.Fatal("idempotent event failed:", err)
	}
	if store.writes.Load() != 0 || store.deletes.Load() != 0 {
		t.Fatal("late/conflicting/idempotent event mutated storage")
	}
}

func TestRebuildReadFailureDoesNotResetVersion(t *testing.T) {
	db, store := newBundleTestDB(t)
	seedBundle(t, db, store, "device", 5)
	store.getError = errors.New("backend unavailable")
	err := (&DeploymentBundleManager{Database: db}).rebuildTheBundleForDevice(context.Background(), "device")
	if err == nil || store.writes.Load() != 0 || store.deletes.Load() != 0 {
		t.Fatalf("rebuild mutated state on read failure: %v", err)
	}
}

func (s *bundleTestStore) Delete(ctx context.Context, req states.DeleteRequest) error {
	if req.Metadata["resource"] == "deploymentBundle" {
		s.deletes.Add(1)
	}
	return s.IStateProvider.Delete(ctx, req)
}

func newBundleTestDB(t *testing.T) (*MargoDatabase, *bundleTestStore) {
	t.Helper()
	memory := &memorystate.MemoryStateProvider{}
	if err := memory.Init(memorystate.MemoryStateProviderConfig{}); err != nil {
		t.Fatal(err)
	}
	store := &bundleTestStore{IStateProvider: memory}
	return NewMargoDatabase(&contexts.ManagerContext{Logger: logger.NewLogger("coa.runtime")}, deploymentManagerPublisherGroup, store), store
}

func seedBundle(t *testing.T, db *MargoDatabase, s *bundleTestStore, device string, version int) {
	t.Helper()
	_, err := s.IStateProvider.Upsert(context.Background(), states.UpsertRequest{Metadata: db.bundleMetadata, Value: states.StateEntry{ID: device, Body: DeploymentBundleRow{DeviceClientId: device, Manifest: sbi.UnsignedAppStateManifest{ManifestVersion: sbi.ManifestVersion(version), Deployments: []sbi.DeploymentManifestRef{}}}}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBundleReplacementNeverAppearsMissingToPoller(t *testing.T) {
	db, store := newBundleTestDB(t)
	seedBundle(t, db, store, "device", 5)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	store.beforeWrite = func(row DeploymentBundleRow) {
		if row.Manifest.ManifestVersion == 6 {
			close(entered)
			<-release
		}
	}
	done := make(chan error, 1)
	go func() {
		done <- db.UpsertDeploymentBundle(context.Background(), DeploymentBundleRow{DeviceClientId: "device", Manifest: sbi.UnsignedAppStateManifest{ManifestVersion: 6, Deployments: []sbi.DeploymentManifestRef{}}}, false)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("replacement did not reach write")
	}
	manager := &DeviceManager{Database: db}
	_, _, manifest, _, err := manager.ShouldReplaceBundle(context.Background(), "device", nil)
	unblock()
	if err != nil {
		t.Fatal(err)
	}
	if manifest.ManifestVersion != 5 {
		t.Errorf("poller observed version %v during replacement, want 5", manifest.ManifestVersion)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if store.deletes.Load() != 0 {
		t.Errorf("replacement deleted the live bundle %d times", store.deletes.Load())
	}
	if store.writes.Load() != 1 {
		t.Errorf("poller initialized a replacement: writes=%d", store.writes.Load())
	}
}

func TestBundleReadFailureDoesNotInitializeVersionOne(t *testing.T) {
	for _, kind := range []string{"backend-error", "malformed-state"} {
		t.Run(kind, func(t *testing.T) {
			db, store := newBundleTestDB(t)
			seedBundle(t, db, store, "device", 5)
			if kind == "backend-error" {
				store.getError = errors.New("backend unavailable")
			} else {
				_, err := store.IStateProvider.Upsert(context.Background(), states.UpsertRequest{Metadata: db.bundleMetadata, Value: states.StateEntry{ID: "device", Body: "malformed bundle"}})
				if err != nil {
					t.Fatal(err)
				}
			}
			_, _, _, _, err := (&DeviceManager{Database: db}).ShouldReplaceBundle(context.Background(), "device", nil)
			if err == nil {
				t.Fatal("expected read failure")
			}
			if store.writes.Load() != 0 || store.deletes.Load() != 0 {
				t.Fatalf("read failure mutated bundle: writes=%d deletes=%d", store.writes.Load(), store.deletes.Load())
			}
		})
	}
}

func TestConcurrentBundleInitializationAndRebuildsKeepVersionsMonotonic(t *testing.T) {
	db, store := newBundleTestDB(t)
	const workers = 8
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Use separate database/manager instances as the NBI and SBI vendors do.
			other := NewMargoDatabase(db.MgrContext, deviceManagerPublisherGroup, store)
			_, _, _, _, err := (&DeviceManager{Database: other}).ShouldReplaceBundle(context.Background(), "device", nil)
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if store.writes.Load() != 1 {
		t.Fatalf("initialized %d times, want once", store.writes.Load())
	}
	start = make(chan struct{})
	errs = make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			other := NewMargoDatabase(db.MgrContext, deploymentManagerPublisherGroup, store)
			errs <- (&DeploymentBundleManager{Database: other}).rebuildTheBundleForDevice(context.Background(), "device")
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	row, err := db.GetDeploymentBundle(context.Background(), "device")
	if err != nil {
		t.Fatal(err)
	}
	if got := int(row.Manifest.ManifestVersion); got != 1+workers {
		t.Fatal(fmt.Sprintf("version %d, want %d distinct revisions", got, 1+workers))
	}
}
