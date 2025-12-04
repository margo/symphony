package margo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/validation"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/contexts"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	margoNonStdAPI "github.com/margo/sandbox/non-standard/generatedCode/wfm/nbi"
	margoUtils "github.com/margo/sandbox/non-standard/pkg/utils"
	"github.com/margo/sandbox/standard/generatedCode/wfm/sbi"
)

var (
	deploymentLogger = logger.NewLogger("coa.runtime")
)

type DeploymentManager struct {
	managers.Manager
	needValidate   bool
	margoValidator validation.MargoValidator
	database       *MargoDatabase
	stateMachine   *DeploymentStateMachine
	tranformer     *MargoTransformer
	bundleManager  *DeploymentBundleManager
}

func (s *DeploymentManager) Init(context *contexts.VendorContext, config managers.ManagerConfig, providers map[string]providers.IProvider) error {
	err := s.Manager.Init(context, config, providers)
	if err != nil {
		return err
	}

	stateprovider, err := managers.GetPersistentStateProvider(config, providers)
	if err != nil {
		return err
	}

	s.database = NewMargoDatabase(s.Context, deploymentManagerPublisherGroup, stateprovider)
	s.stateMachine = NewDeploymentStateMachine(s.database, deploymentLogger)
	s.tranformer = NewMargoTransformer()

	// ADDED: Initialize bundle manager properly
	s.bundleManager = &DeploymentBundleManager{
		Manager:       s.Manager,
		Database:      s.database,
		StateProvider: stateprovider,
		needValidate:  managers.NeedObjectValidate(config, providers),
	}
	if s.bundleManager.needValidate {
		s.bundleManager.MargoValidator = validation.NewMargoValidator()
	}

	s.needValidate = managers.NeedObjectValidate(config, providers)
	if s.needValidate {
		s.margoValidator = validation.NewMargoValidator()
	}

	// subscribe to events
	context.Subscribe(string(upsertDeploymentFeed), v1alpha2.EventHandler{
		Handler: func(topic string, event v1alpha2.Event) error {
			producerName, exists := event.Metadata["producerName"]
			if !exists {
				// not of our concern
				return nil
			}
			if producerName != string(deviceManagerPublisherGroup) {
				// we want updates from this producer only
				return nil
			}
			return s.upsertObjectInCache(topic, event)
		},
		Group: "events-from-device-manager",
	})

	context.Subscribe(string(deleteDeploymentFeed), v1alpha2.EventHandler{
		Handler: func(topic string, event v1alpha2.Event) error {
			producerName, exists := event.Metadata["producerName"]
			if !exists {
				// not of our concern
				return nil
			}
			if producerName != string(deviceManagerPublisherGroup) {
				// we want updates from this producer only
				return nil
			}
			return s.deleteObjectFromCache(topic, event)
		},
		Group: "events-from-device-manager",
	})

	return nil
}

func (s *DeploymentManager) Shutdown(ctx context.Context) error {
	return nil
}

// upsertObjectInCache handles the "newDeployment" event.
func (s *DeploymentManager) upsertObjectInCache(topic string, event v1alpha2.Event) error {
	deploymentLogger.InfofCtx(context.Background(), "upsertObjectInCache: Received event on topic '%s'", topic)

	var err error
	switch event.Body.(type) {
	case DeploymentDatabaseRow:
		err = s.database.UpsertDeployment(context.Background(), event.Body.(DeploymentDatabaseRow), false)
	default:
		deploymentLogger.ErrorfCtx(context.Background(), "upsertObjectInCache: Invalid event body: known object is missing or not of the correct type")
		return fmt.Errorf("invalid event body: deployment is missing or not of the correct type")
	}

	if err != nil {
		deploymentLogger.ErrorfCtx(context.Background(), "upsertObjectInCache: Failed to cache object %v", err)
		return fmt.Errorf("failed to cache object %w", err)
	}

	deploymentLogger.InfofCtx(context.Background(), "upsertObjectInCache: Successfully upsert object in cache")
	return nil
}

// deleteObjectFromCache handles the "newDeployment" event.
func (s *DeploymentManager) deleteObjectFromCache(topic string, event v1alpha2.Event) error {
	deploymentLogger.InfofCtx(context.Background(), "deleteObjectFromCache: Received event on topic '%s'", topic)

	var err error
	switch event.Body.(type) {
	case AppPackageDatabaseRow:
		err = s.database.DeleteAppPackage(context.Background(), *event.Body.(AppPackageDatabaseRow).PackageRequest.Metadata.Id)
	case DeploymentDatabaseRow:
		err = s.database.DeleteDeployment(context.Background(), *event.Body.(DeploymentDatabaseRow).DeploymentRequest.Metadata.Id, false)
	case DeviceDatabaseRow:
		err = s.database.DeleteDevice(context.Background(), event.Body.(DeviceDatabaseRow).Capabilities.Properties.Id)
	default:
		deploymentLogger.ErrorfCtx(context.Background(), "deleteObjectFromCache: Invalid event body: known object is missing or not of the correct type")
		return fmt.Errorf("invalid event body: deployment is missing or not of the correct type")
	}

	if err != nil {
		deploymentLogger.ErrorfCtx(context.Background(), "deleteObjectFromCache: Failed to remove cached object %v", err)
		return fmt.Errorf("failed to cache object %w", err)
	}

	deploymentLogger.InfofCtx(context.Background(), "deleteObjectFromCache: Successfully removed object from cache")
	return nil
}

// CreateDeployment handles the deployment of an application deployment.
func (s *DeploymentManager) CreateDeployment(ctx context.Context, req margoNonStdAPI.ApplicationDeploymentManifestRequest, appPkg ApplicationPackage) (*margoNonStdAPI.ApplicationDeploymentManifestResp, error) {
	deploymentLogger.InfofCtx(ctx, "CreateDeployment: Starting deployment process for '%s'", req.Metadata.Name)

	if req.Metadata.Name == "" {
		return nil, fmt.Errorf("deployment name is required")
	}

	// Create deployment response with clean builder
	deployment := s.buildInitialDeployment(req)

	// Process deployment profiles
	if err := s.tranformer.MergeWithAppPackage(deployment, appPkg); err != nil {
		return nil, fmt.Errorf("failed to process deployment profiles: %w", err)
	}

	// Store in database (single call)
	if err := s.storeDeployment(ctx, *deployment, *deployment.Metadata.Id, appPkg.Description.Metadata.Id, appPkg.Description.Metadata.Version); err != nil {
		return nil, fmt.Errorf("failed to store deployment: %w", err)
	}

	// Trigger bundle generation when deployment assigned to device
	if req.Spec.DeviceRef != nil && req.Spec.DeviceRef.Id != nil {
		deviceId := *req.Spec.DeviceRef.Id
		deploymentLogger.InfofCtx(ctx, "CreateDeployment: Triggering bundle generation for device %s", deviceId)

		go func() {
			// Add delay to ensure database consistency
			time.Sleep(500 * time.Millisecond)

			if s.bundleManager != nil {
				if err := s.bundleManager.rebuildTheBundleForDevice(context.Background(), deviceId); err != nil {
					deploymentLogger.ErrorfCtx(context.Background(), "Failed to rebuild bundle for device %s: %v", deviceId, err)
				} else {
					deploymentLogger.InfofCtx(context.Background(), "Successfully rebuilt bundle for device %s", deviceId)
				}
			} else {
				deploymentLogger.ErrorfCtx(context.Background(), "Bundle manager is nil for device %s", deviceId)
			}
		}()
	} else {
		deploymentLogger.WarnfCtx(ctx, "CreateDeployment: No device reference found in deployment request")
	}

	deploymentLogger.InfofCtx(ctx, "CreateDeployment: Successfully created deployment '%s'", *deployment.Metadata.Id)
	return deployment, nil
}

// buildInitialDeployment creates a clean initial deployment structure
func (s *DeploymentManager) buildInitialDeployment(req margoNonStdAPI.ApplicationDeploymentManifestRequest) *margoNonStdAPI.ApplicationDeploymentManifestResp {
	now := time.Now().UTC()
	deploymentId := margoUtils.GenerateAppDeploymentId()
	state := margoNonStdAPI.ApplicationDeploymentStatusState(DeploymentStatePending)
	contextMsg := "stored on wfm, yet to be synced with the device"

	return &margoNonStdAPI.ApplicationDeploymentManifestResp{
		ApiVersion: req.ApiVersion,
		Kind:       "ApplicationDeploymentManifest",
		Metadata: margoNonStdAPI.Metadata{
			Id:                &deploymentId,
			Name:              req.Metadata.Name,
			Namespace:         req.Metadata.Namespace,
			CreationTimestamp: &now,
		},
		Spec: req.Spec,
		Status: &margoNonStdAPI.ApplicationDeploymentStatus{
			State:          &state,
			LastUpdateTime: &now,
			ContextualInfo: &margoNonStdAPI.ContextualInfo{
				Message: &contextMsg,
			},
		},
		RecentOperation: &margoNonStdAPI.ApplicationDeploymentRecentOperation{
			Op:     margoNonStdAPI.DEPLOY,
			Status: margoNonStdAPI.ApplicationDeploymentOperationStatusPENDING,
		},
	}
}

// storeDeployment handles all database storage operations
func (s *DeploymentManager) storeDeployment(ctx context.Context, deployment margoNonStdAPI.ApplicationDeploymentManifestResp, deploymentId, appId, appVersion string) error {
	now := time.Now().UTC()

	// Store deployment record
	dbRow := DeploymentDatabaseRow{
		DeploymentRequest: deployment,
		LastStatusUpdate:  now,
	}

	if err := s.database.UpsertDeployment(ctx, dbRow, true); err != nil {
		return fmt.Errorf("failed to store deployment: %w", err)
	}

	deploymentLogger.InfofCtx(ctx, "STORED DEPLOYMENT: deploymentId=%s", deploymentId)

	// Store desired state
	return s.storeDesiredState(ctx, deployment, deploymentId, appId, appVersion)
}

// Simplified CRUD operations
func (s *DeploymentManager) ListDeployments(ctx context.Context) (margoNonStdAPI.ApplicationDeploymentListResp, error) {
	dbRows, err := s.database.ListDeployments(ctx)
	if err != nil {
		return margoNonStdAPI.ApplicationDeploymentListResp{}, fmt.Errorf("failed to list deployments: %w", err)
	}
	return s.tranformer.DbRowToDeploymentList(dbRows)
}

func (s *DeploymentManager) GetDeployments(ctx context.Context, deploymentId string) (*margoNonStdAPI.ApplicationDeploymentManifestResp, error) {
	dbRow, err := s.database.GetDeployment(ctx, deploymentId)
	if err != nil {
		return nil, fmt.Errorf("failed to get deployment '%s': %w", deploymentId, err)
	}
	return &dbRow.DeploymentRequest, nil
}

func (s *DeploymentManager) DeleteDeployment(ctx context.Context, deploymentId string) (*margoNonStdAPI.ApplicationDeploymentManifestResp, error) {
	if deploymentId == "" {
		return nil, fmt.Errorf("deployment ID is required")
	}

	deploymentLogger.InfofCtx(ctx, "DeleteDeployment: Starting deletion for deployment '%s'", deploymentId)

	// Get deployment to extract device ID before deletion
	deployment, err := s.database.GetDeployment(ctx, deploymentId)
	if err != nil {
		return nil, fmt.Errorf("failed to get deployment '%s': %w", deploymentId, err)
	}

	// Extract device ID for bundle regeneration
	var deviceId string
	if deployment.DeploymentRequest.Spec.DeviceRef != nil && deployment.DeploymentRequest.Spec.DeviceRef.Id != nil {
		deviceId = *deployment.DeploymentRequest.Spec.DeviceRef.Id
	}

	// Use state machine for clean state transition to "Removing"
	if err := s.stateMachine.ProcessEvent(ctx, deploymentId, EventStartRemoval, "deletion requested by user", nil); err != nil {
		return nil, fmt.Errorf("failed to initiate deployment deletion: %w", err)
	}

	deploymentLogger.InfofCtx(ctx, "DeleteDeployment: Transitioned deployment '%s' to REMOVING state", deploymentId)

	// Trigger bundle regeneration if deployment was assigned to a device
	if deviceId != "" {
		deploymentLogger.InfofCtx(ctx, "DeleteDeployment: Triggering bundle regeneration for device %s after deletion", deviceId)

		go func() {
			// Add delay to ensure state machine update is persisted
			time.Sleep(500 * time.Millisecond)

			if s.bundleManager != nil {
				if err := s.bundleManager.rebuildTheBundleForDevice(context.Background(), deviceId); err != nil {
					deploymentLogger.ErrorfCtx(context.Background(), "Failed to rebuild bundle for device %s after deletion: %v", deviceId, err)
				} else {
					deploymentLogger.InfofCtx(context.Background(), "Successfully rebuilt bundle for device %s after deletion", deviceId)
				}
			}
		}()
	}

	// Return updated deployment (now in REMOVING state)
	return s.GetDeployments(ctx, deploymentId)
}

func (s *DeploymentManager) ProcessDeploymentEvent(ctx context.Context, deploymentId string, event DeploymentEvent, contextInfo string, err error) error {
	return s.stateMachine.ProcessEvent(ctx, deploymentId, event, contextInfo, err)
}

func (s *DeploymentManager) onAppPkgUpdate(topic string, event v1alpha2.Event) error {
	deploymentLogger.InfofCtx(context.Background(), "onAppPkgUpdate: Received event on topic '%s'", topic)
	// TODO: implement app package update handling
	return nil
}

// storeDesiredState handles the desired state storage with clean conversion
func (s *DeploymentManager) storeDesiredState(ctx context.Context, deployment margoNonStdAPI.ApplicationDeploymentManifestResp, deploymentId, appId, appVersion string) error {
	// Convert to desired state directly without JSON marshaling
	desiredState, err := s.buildDesiredState(deployment, appId, appVersion)
	if err != nil {
		return fmt.Errorf("failed to build desired state: %w", err)
	}

	if err := s.database.UpsertDeploymentDesiredState(ctx, deploymentId, desiredState, true); err != nil {
		return fmt.Errorf("failed to store desired state: %w", err)
	}

	return nil
}

// buildDesiredState creates the desired state structure cleanly
func (s *DeploymentManager) buildDesiredState(deployment margoNonStdAPI.ApplicationDeploymentManifestResp, appId, appVersion string) (AppDeploymentState, error) {
	desiredState := AppDeploymentState{
		AppDeploymentManifest: sbi.AppDeploymentManifest{
			ApiVersion: deployment.ApiVersion,
			Kind:       deployment.Kind,
			Metadata: sbi.AppDeploymentMetadata{
				Name:        deployment.Metadata.Name,
				Namespace:   deployment.Metadata.Namespace,
				Id:          deployment.Metadata.Id,
				Annotations: deployment.Metadata.Annotations,
				Labels:      deployment.Metadata.Labels,
			},
			Spec: sbi.AppDeploymentSpec{
				DeploymentProfile: s.tranformer.ConvertDeploymentProfile(deployment.Spec.DeploymentProfile),
				Parameters:        &sbi.AppDeploymentParams{},
			},
		},
		Status: sbi.DeploymentStatusManifest{
			ApiVersion:   "margo.org",
			Kind:         "DeploymentStatus",
			DeploymentId: *deployment.Metadata.Id,
			Status: struct {
				Error *struct {
					Code    *string "json:\"code,omitempty\""
					Message *string "json:\"message,omitempty\""
				} "json:\"error,omitempty\""
				State sbi.DeploymentStatusManifestStatusState "json:\"state\""
			}{
				Error: nil,
				State: sbi.DeploymentStatusManifestStatusStateInstalled,
			},
		},
	}

	// Convert parameters properly
	if deployment.Spec.Parameters != nil {
		data, err := json.Marshal(deployment.Spec.Parameters)
		if err != nil {
			return AppDeploymentState{}, fmt.Errorf("failed to marshal deployment parameters: %w", err)
		}

		if err := json.Unmarshal(data, desiredState.Spec.Parameters); err != nil {
			return AppDeploymentState{}, fmt.Errorf("failed to unmarshal deployment parameters: %w", err)
		}
	}

	return desiredState, nil
}
