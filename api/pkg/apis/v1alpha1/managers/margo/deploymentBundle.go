package margo

import (
	"context"
	"fmt"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/validation"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/contexts"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/states"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	"github.com/margo/dev-repo/shared-lib/archive"
	"github.com/margo/dev-repo/shared-lib/crypto"
	"github.com/margo/dev-repo/shared-lib/pointers"
	margoStdAPI "github.com/margo/dev-repo/standard/generatedCode/wfm/sbi"
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
			producerName, exists := event.Metadata["producerName"]
			if !exists {
				// not of our concern
				return nil
			}
			if producerName != string(deploymentManagerPublisherGroup) {
				// we want updates from this producer only
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
		err = s.Database.UpsertDeployment(context.Background(), event.Body.(DeploymentDatabaseRow), false)
		s.rebuildTheBundleForDevice(*event.Body.(DeploymentDatabaseRow).DeploymentRequest.Spec.DeviceRef.Id)
	default:
		deploymentBundleLogger.ErrorfCtx(context.Background(), "upsertObjectInCache: Invalid event body: known object is missing or not of the correct type")
		return fmt.Errorf("invalid event body: deployment is missing or not of the correct type")
	}

	if err != nil {
		deploymentBundleLogger.ErrorfCtx(context.Background(), "upsertObjectInCache: Failed to cache object %v", err)
		return fmt.Errorf("failed to cache object %w", err)
	}

	deploymentBundleLogger.InfofCtx(context.Background(), "upsertObjectInCache: Successfully upsert object in cache")
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
	// TODO: add a caching logic here, later
	// s.Database.IsBundleOutdated(bundleDigestOnDevice)
	// if not outdated, return that there is no change

	// else we'll prepare the bundle now
	// 1. Get all deployments for this device
	dbRows, err := s.Database.GetDeploymentsByDevice(ctx, deviceClientId)
	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "PollDeviceBundle: Failed to get deployments for device client: %s, err: %v", deviceClientId, err)
		return fmt.Errorf("failed to get deployments for device client: %s, err: %w", deviceClientId, err)
	}

	if len(dbRows) == 0 {
		if err := s.Database.DeleteDeploymentBundle(ctx, deviceClientId, true); err != nil {
			return fmt.Errorf("failed to cache deployment bundle for device %s: %w", deviceClientId, err)
		}
		return nil
	}

	// 2. prepare manifest object for response
	archiver := archive.NewArchiver(archive.ArchiveFormatTarGZ)
	newBundleManifest := margoStdAPI.UnsignedStateManifest{}
	for _, row := range dbRows {
		data, _ := yaml.Marshal(row)
		_, _, err = archiver.AppendContent(data, ".")
		if err != nil {
			deviceLogger.ErrorfCtx(ctx, "PollDeviceBundle: Failed to add deployment content: %v", err)
			return fmt.Errorf("failed to append content to bundle: %w", err)
		}

		digest, err := crypto.GetDigestOfContent(data)
		if err != nil {
			deviceLogger.ErrorfCtx(ctx, "PollDeviceBundle: Failed to calculate digest of deployment content: %v", err)
			return fmt.Errorf("failed to calculate digest of deployment content: %w", err)
		}
		newBundleManifest.Deployments = append(newBundleManifest.Deployments, margoStdAPI.Deployment{
			DeploymentId: *row.DesiredDeployment.Metadata.Id,
			Digest:       digest,
			SizeBytes:    pointers.Ptr(float32(len(data))),
			Url:          "",
		})
	}

	_, archiveDigest, archiveSize, archivePath, err := archiver.CreateArchive()
	bundleMediaType := "tar"
	// url is supposed to be of the format: ("/api/v1/devices/{deviceId}/bundles/{digest}"
	// REVIEW: should url be set by someone else in the codebase?
	bundleDownloadUrl := fmt.Sprintf("/api/v1/devices/%s/bundles/%s", deviceClientId, archiveDigest)
	newBundleManifest.Bundle = &margoStdAPI.ApplicationDeploymentBundle{
		Digest:    &archiveDigest,
		MediaType: &bundleMediaType,
		SizeBytes: pointers.Ptr(float32(archiveSize)),
		Url:       &bundleDownloadUrl,
	}

	if err := s.Database.UpsertDeploymentBundle(ctx, DeploymentBundleRow{
		DeviceClientId: deviceClientId,
		Manifest:       newBundleManifest,
		ArchivePath:    archivePath,
	}, true); err != nil {
		return fmt.Errorf("failed to upsert deployment bundle for device %s: %w", deviceClientId, err)
	}

	return nil
}
