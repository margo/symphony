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
	margoNonStdAPI "github.com/margo/dev-repo/non-standard/generatedCode/wfm/nbi"
	margoUtils "github.com/margo/dev-repo/non-standard/pkg/utils"
	"github.com/margo/dev-repo/standard/generatedCode/wfm/sbi"
	"github.com/margo/dev-repo/standard/pkg"
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

	// Store in database
	if err := s.storeDeployment(ctx, *deployment, *deployment.Metadata.Id, appPkg.Description.Metadata.Id, appPkg.Description.Metadata.Version); err != nil {
		return nil, fmt.Errorf("failed to store deployment: %w", err)
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

	// Use state machine for clean state transition
	if err := s.stateMachine.ProcessEvent(ctx, deploymentId, EventStartDeletion, "deletion requested by user", nil); err != nil {
		return nil, fmt.Errorf("failed to initiate deployment deletion: %w", err)
	}

	// Return updated deployment
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
func (s *DeploymentManager) buildDesiredState(deployment margoNonStdAPI.ApplicationDeploymentManifestResp, appId, appVersion string) (sbi.AppState, error) {
	// Create app deployment structure directly
	// Merge app package configuration with deployment parameters
	// mergedConfig, err := s.tranformer.MergeConfigurationWithAppPackage(req.Spec.Parameters, appPkg.Description.Configuration, appPkg.Description.Parameters)
	// if err != nil {
	// 	return sbi.AppState{}, fmt.Errorf("failed to merge configuration: %w", err)
	// }
	appDeployment := sbi.AppDeployment{
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
	}

	{
		data, _ := json.Marshal(deployment.Spec.Parameters)
		json.Unmarshal(data, appDeployment.Spec.Parameters)
	}

	// Convert to desired state using the package converter
	desiredState, err := pkg.ConvertAppDeploymentToAppState(
		&appDeployment,
		appId,
		appVersion,
		string(margoNonStdAPI.ApplicationDeploymentStatusStateRUNNING),
	)
	if err != nil {
		return desiredState, fmt.Errorf("failed to convert to app state: %w", err)
	}

	return desiredState, nil
}
