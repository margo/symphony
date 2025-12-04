package margo

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/validation"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/contexts"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/states"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	"github.com/margo/sandbox/shared-lib/archive"
	"github.com/margo/sandbox/shared-lib/crypto"
	"github.com/margo/sandbox/shared-lib/pointers"
	margoStdAPI "github.com/margo/sandbox/standard/generatedCode/wfm/sbi"
	"gopkg.in/yaml.v2"
)

var (
	deploymentBundleLogger = logger.NewLogger("coa.runtime")
)

type DeploymentBundleManager struct {
	managers.Manager
	Database       *MargoDatabase
	StateProvider  states.IStateProvider
	MargoValidator validation.MargoValidator
	needValidate   bool
}

func (s *DeploymentBundleManager) Init(pCtx *contexts.VendorContext, config managers.ManagerConfig, providers map[string]providers.IProvider) error {
	err := s.Manager.Init(pCtx, config, providers)
	if err != nil {
		return err
	}

	stateprovider, err := managers.GetPersistentStateProvider(config, providers)
	if err != nil {
		return err
	}
	s.Database = NewMargoDatabase(s.Context, deploymentBundleManagerPublisherGroup, stateprovider)

	s.needValidate = managers.NeedObjectValidate(config, providers)
	if s.needValidate {
		// Turn off validation of differnt types: https://github.com/eclipse-symphony/symphony/issues/445
		s.MargoValidator = validation.NewMargoValidator()
	}

	// subscribe to events
	pCtx.Subscribe(string(upsertDeploymentFeed), v1alpha2.EventHandler{
		Handler: func(topic string, event v1alpha2.Event) error {
			deploymentBundleLogger.InfofCtx(context.Background(), "RECEIVED EVENT: topic=%s, producer=%s", topic, event.Metadata["producerName"])

			producerName, exists := event.Metadata["producerName"]
			if !exists {
				deploymentBundleLogger.WarnfCtx(context.Background(), "Event missing producerName metadata")
				return nil
			}
			if producerName != string(deploymentManagerPublisherGroup) {
				deploymentBundleLogger.InfofCtx(context.Background(), "Ignoring event from producer: %s (expected: %s)", producerName, string(deploymentManagerPublisherGroup))
				return nil
			}
			return s.upsertObjectInCache(topic, event)
		},
		Group: "events-from-deployment-manager",
	})

	pCtx.Subscribe(string(deleteDeploymentFeed), v1alpha2.EventHandler{
		Handler: func(topic string, event v1alpha2.Event) error {
			producerName, exists := event.Metadata["producerName"]
			if !exists {
				// not of our concern
				return nil
			}
			if producerName != string(deploymentManagerPublisherGroup) {
				// we want updates from this producer only
				return nil
			}
			return s.deleteObjectFromCache(topic, event)
		},
		Group: "events-from-deployment-manager",
	})

	return nil
}

// upsertObjectInCache handles the "newDeployment" event.
func (s *DeploymentBundleManager) upsertObjectInCache(topic string, event v1alpha2.Event) error {
	deploymentBundleLogger.InfofCtx(context.Background(), "upsertObjectInCache: Received event on topic '%s'", topic)

	var err error
	switch event.Body.(type) {
	case DeploymentDatabaseRow:
		deployment := event.Body.(DeploymentDatabaseRow)
		err = s.Database.UpsertDeployment(context.Background(), deployment, false)
		if err != nil {
			deploymentBundleLogger.ErrorfCtx(context.Background(), "upsertObjectInCache: Failed to cache object %v", err)
			return fmt.Errorf("failed to cache object %w", err)
		}

		//  Add nil checks before accessing DeviceRef
		if deployment.DeploymentRequest.Spec.DeviceRef != nil && deployment.DeploymentRequest.Spec.DeviceRef.Id != nil {
			deviceId := *deployment.DeploymentRequest.Spec.DeviceRef.Id
			deploymentBundleLogger.InfofCtx(context.Background(), "upsertObjectInCache: Triggering bundle rebuild for device %s", deviceId)
			err = s.rebuildTheBundleForDevice(context.Background(), deviceId)
			if err != nil {
				deploymentBundleLogger.ErrorfCtx(context.Background(), "upsertObjectInCache: Failed to rebuild bundle for device %s: %v", deviceId, err)
			} else {
				deploymentBundleLogger.InfofCtx(context.Background(), "upsertObjectInCache: Successfully rebuilt bundle for device %s", deviceId)
			}
		} else {
			deploymentBundleLogger.WarnfCtx(context.Background(), "upsertObjectInCache: Deployment %s has no device reference",
				*deployment.DeploymentRequest.Metadata.Id)
		}
	default:
		deploymentBundleLogger.ErrorfCtx(context.Background(), "upsertObjectInCache: Invalid event body: known object is missing or not of the correct type")
		return fmt.Errorf("invalid event body: deployment is missing or not of the correct type")
	}

	if err != nil {
		deploymentBundleLogger.ErrorfCtx(context.Background(), "upsertObjectInCache: Failed to process deployment %v", err)
		return fmt.Errorf("failed to process deployment %w", err)
	}

	deploymentBundleLogger.InfofCtx(context.Background(), "upsertObjectInCache: Successfully processed deployment")
	return nil
}

// deleteObjectFromCache handles the "newDeployment" event.
func (s *DeploymentBundleManager) deleteObjectFromCache(topic string, event v1alpha2.Event) error {
	deploymentBundleLogger.InfofCtx(context.Background(), "deleteObjectFromCache: Received event on topic '%s'", topic)

	var err error
	switch event.Body.(type) {
	case DeploymentDatabaseRow:
		err = s.Database.DeleteDeployment(context.Background(), *event.Body.(DeploymentDatabaseRow).DeploymentRequest.Metadata.Id, false)
	default:
		deploymentBundleLogger.ErrorfCtx(context.Background(), "deleteObjectFromCache: Invalid event body: known object is missing or not of the correct type")
		return fmt.Errorf("invalid event body: deployment is missing or not of the correct type")
	}

	if err != nil {
		deploymentBundleLogger.ErrorfCtx(context.Background(), "deleteObjectFromCache: Failed to remove cached object %v", err)
		return fmt.Errorf("failed to cache object %w", err)
	}

	deploymentBundleLogger.InfofCtx(context.Background(), "deleteObjectFromCache: Successfully removed object from cache")
	return nil
}

func (s *DeploymentBundleManager) rebuildTheBundleForDevice(ctx context.Context, deviceClientId string) error {
	deploymentBundleLogger.InfofCtx(ctx, "rebuildTheBundleForDevice: Starting bundle rebuild for device %s", deviceClientId)

	// Use uint64 for all version calculations
	manifestVersionInt := uint64(1)
	var oldArchivePath string

	// Check for existing bundle to increment version
	existingBundle, err := s.Database.GetDeploymentBundle(ctx, deviceClientId)
	if err == nil && existingBundle != nil {
		// CAST: float32 to uint64 for increment operation
		manifestVersionInt = uint64(existingBundle.Manifest.ManifestVersion) + 1
		oldArchivePath = existingBundle.ArchivePath // ✅ Save for cleanup later
		deploymentBundleLogger.InfofCtx(ctx, "rebuildTheBundleForDevice: Found existing bundle version %d, incrementing to %d",
			uint64(existingBundle.Manifest.ManifestVersion), manifestVersionInt)
	}

	// Get all deployments for this device
	dbRows, err := s.Database.GetDeploymentsByDevice(ctx, deviceClientId)
	if err != nil {
		deploymentBundleLogger.ErrorfCtx(ctx, "rebuildTheBundleForDevice: Failed to get deployments for device %s: %v", deviceClientId, err)
		return fmt.Errorf("failed to get deployments for device client: %s, err: %w", deviceClientId, err)
	}

	// Filter out deployments in REMOVING or REMOVED states
	activeDeployments := []DeploymentDatabaseRow{}
	for _, row := range dbRows {
		state := row.DesiredState.Status.Status.State

		// Only include deployments that are NOT being removed
		if state != margoStdAPI.DeploymentStatusManifestStatusStateRemoving &&
			state != margoStdAPI.DeploymentStatusManifestStatusStateRemoved {
			activeDeployments = append(activeDeployments, row)
		} else {
			deploymentBundleLogger.InfofCtx(ctx, "rebuildTheBundleForDevice: Excluding deployment %s in state '%s' from bundle",
				*row.DesiredState.Metadata.Id, state)
		}
	}

	deploymentBundleLogger.InfofCtx(ctx, "rebuildTheBundleForDevice: Found %d total deployments, %d active (excluded %d removing/removed) for device %s",
		len(dbRows), len(activeDeployments), len(dbRows)-len(activeDeployments), deviceClientId)

	// Create manifest with CAST: uint64 to float32
	newBundleManifest := margoStdAPI.UnsignedAppStateManifest{
		ManifestVersion: margoStdAPI.ManifestVersion(manifestVersionInt), // CAST: uint64 to float32
		Deployments:     []margoStdAPI.DeploymentManifestRef{},
		Bundle:          nil,
	}

	var archivePath string

	// Use activeDeployments instead of dbRows
	if len(activeDeployments) > 0 {
		// Sort deployments by ID for deterministic ordering
		sort.Slice(activeDeployments, func(i, j int) bool {
			return *activeDeployments[i].DesiredState.Metadata.Id < *activeDeployments[j].DesiredState.Metadata.Id
		})

		deploymentBundleLogger.InfofCtx(ctx, "rebuildTheBundleForDevice: Sorted %d active deployments for deterministic bundle creation", len(activeDeployments))

		// Create archive for deployments
		archiver := archive.NewArchiver(archive.ArchiveFormatTarGZ)

		for _, row := range activeDeployments {
			deploymentBundleLogger.InfofCtx(ctx, "rebuildTheBundleForDevice: Adding deployment %s (state: %s) to bundle",
				*row.DesiredState.Metadata.Id, row.DesiredState.Status.Status.State)

			// Use JSON-to-YAML conversion to preserve component data
			// First marshal to JSON (which handles union types correctly via MarshalJSON())
			jsonData, err := json.Marshal(row.DesiredState.AppDeploymentManifest)
			if err != nil {
				deploymentBundleLogger.ErrorfCtx(ctx, "rebuildTheBundleForDevice: Failed to marshal deployment to JSON %s: %v",
					*row.DesiredState.Metadata.Id, err)
				return fmt.Errorf("failed to marshal deployment to JSON: %w", err)
			}

			// Convert JSON to YAML (preserves all data including components)
			var yamlInterface interface{}
			if err := json.Unmarshal(jsonData, &yamlInterface); err != nil {
				deploymentBundleLogger.ErrorfCtx(ctx, "rebuildTheBundleForDevice: Failed to unmarshal JSON %s: %v",
					*row.DesiredState.Metadata.Id, err)
				return fmt.Errorf("failed to unmarshal JSON: %w", err)
			}

			data, err := yaml.Marshal(yamlInterface)
			if err != nil {
				deploymentBundleLogger.ErrorfCtx(ctx, "rebuildTheBundleForDevice: Failed to marshal to YAML %s: %v",
					*row.DesiredState.Metadata.Id, err)
				return fmt.Errorf("failed to marshal to YAML: %w", err)
			}

			filename := fmt.Sprintf("%s.yaml", *row.DesiredState.Metadata.Id)
			_, _, err = archiver.AppendContent(data, filename)
			if err != nil {
				deploymentBundleLogger.ErrorfCtx(ctx, "rebuildTheBundleForDevice: Failed to add deployment content: %v", err)
				return fmt.Errorf("failed to append content to bundle: %w", err)
			}

			// Calculate digest of the YAML content (not the archive)
			yamlDigest, err := crypto.GetDigestOfContent(data)
			if err != nil {
				deploymentBundleLogger.ErrorfCtx(ctx, "rebuildTheBundleForDevice: Failed to calculate digest: %v", err)
				return fmt.Errorf("failed to calculate digest of deployment content: %w", err)
			}

			newBundleManifest.Deployments = append(newBundleManifest.Deployments, margoStdAPI.DeploymentManifestRef{
				DeploymentId: *row.DesiredState.Metadata.Id,
				Digest:       yamlDigest,
				SizeBytes:    pointers.Ptr(float32(len(data))),
				Url:          fmt.Sprintf("/api/v1/clients/%s/deployments/%s/%s", deviceClientId, *row.DesiredState.Metadata.Id, yamlDigest),
			})
		}

		// Declare variables before assignment
		var archiveDigest string
		var archiveSize uint64

		// Create the archive (archiver will sort entries internally for determinism)
		_, archiveDigest, archiveSize, archivePath, err = archiver.CreateArchive()
		if err != nil {
			return fmt.Errorf("failed to create archive: %w", err)
		}

		// Set the Bundle metadata
		bundleMediaType := "application/vnd.margo.bundle.v1+tar+gzip"
		bundleDownloadUrl := fmt.Sprintf("/api/v1/clients/%s/bundles/%s", deviceClientId, archiveDigest)

		newBundleManifest.Bundle = &margoStdAPI.DeploymentBundleRef{
			Digest:    &archiveDigest,
			MediaType: &bundleMediaType,
			SizeBytes: pointers.Ptr(float32(archiveSize)),
			Url:       &bundleDownloadUrl,
		}

		deploymentBundleLogger.InfofCtx(ctx, "rebuildTheBundleForDevice: Created archive with digest: %s, path: %s, size: %d bytes",
			archiveDigest, archivePath, archiveSize)
	} else {
		deploymentBundleLogger.InfofCtx(ctx, "rebuildTheBundleForDevice: No active deployments for device %s, creating empty bundle", deviceClientId)
	}

	// ✅ Store new bundle FIRST (atomic replacement)
	if err := s.Database.UpsertDeploymentBundle(ctx, DeploymentBundleRow{
		DeviceClientId: deviceClientId,
		Manifest:       newBundleManifest,
		ArchivePath:    archivePath,
	}, true); err != nil {
		return fmt.Errorf("failed to upsert deployment bundle: %w", err)
	}

	// ✅ NOW cleanup old archive file (after new bundle is safely stored)
	if oldArchivePath != "" && oldArchivePath != archivePath {
		deploymentBundleLogger.InfofCtx(ctx, "rebuildTheBundleForDevice: Cleaning up old archive file: %s", oldArchivePath)
		if err := os.Remove(oldArchivePath); err != nil {
			// Log warning but don't fail - cleanup is best-effort
			deploymentBundleLogger.WarnfCtx(ctx, "rebuildTheBundleForDevice: Failed to remove old archive file %s: %v (continuing anyway)", oldArchivePath, err)
		}
	}

	// DEBUG: List all bundles after storage
	s.Database.DebugListAllBundles(ctx)

	// CAST: Use manifestVersionInt (uint64) for proper integer logging
	deploymentBundleLogger.InfofCtx(ctx, "rebuildTheBundleForDevice: Successfully created bundle for device %s with %d active deployments (version %d)",
		deviceClientId, len(activeDeployments), manifestVersionInt)
	return nil
}
