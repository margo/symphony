package margo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/validation"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/contexts"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/states"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	margoNonStdAPI "github.com/margo/dev-repo/non-standard/generatedCode/wfm/nbi"
	margoUtils "github.com/margo/dev-repo/non-standard/pkg/utils"
)

var (
	deploymentLogger    = logger.NewLogger("coa.runtime")
	deploymentNamespace = "margo"
	deploymentResource  = "app-deployments"
	deploymentKind      = "ApplicationDeployment"
	deploymentMetadata  = map[string]interface{}{
		"version":   "v1",
		"group":     model.MargoGroup,
		"resource":  deploymentResource,
		"namespace": deploymentNamespace,
		"kind":      deploymentKind,
	}

	pkgCatalogNamespace = "margo-catalog"
	pkgCatalogResource  = "margo-catalog"
	pkgCatalogKind      = "MargoCatalog"
	pkgCatalogMetadata  = map[string]interface{}{
		"version":   "v1",
		"group":     model.MargoGroup,
		"resource":  pkgCatalogResource,
		"namespace": pkgCatalogNamespace,
		"kind":      pkgCatalogKind,
	}
)

type DeploymentManager struct {
	managers.Manager
	StateProvider  states.IStateProvider
	needValidate   bool
	MargoValidator validation.MargoValidator
}

func (s *DeploymentManager) Init(context *contexts.VendorContext, config managers.ManagerConfig, providers map[string]providers.IProvider) error {
	err := s.Manager.Init(context, config, providers)
	if err != nil {
		return err
	}
	stateprovider, err := managers.GetPersistentStateProvider(config, providers)
	if err == nil {
		s.StateProvider = stateprovider
	} else {
		return err
	}
	s.needValidate = managers.NeedObjectValidate(config, providers)
	if s.needValidate {
		// Turn off validation of differnt types: https://github.com/eclipse-symphony/symphony/issues/445
		s.MargoValidator = validation.NewMargoValidator()
	}

	context.Subscribe("deploymentStatusUpdates", v1alpha2.EventHandler{
		Handler: s.onDeploymentStatusUpdate,
		Group:   "events-from-device",
	})

	context.Subscribe("appPkgAddition", v1alpha2.EventHandler{
		Handler: s.onAppPkgAddition,
		Group:   "events-from-app-catalog",
	})

	context.Subscribe("appPkgDeletion", v1alpha2.EventHandler{
		Handler: s.onAppPkgDeletion,
		Group:   "events-from-app-catalog",
	})

	context.Subscribe("appPkgUpdate", v1alpha2.EventHandler{
		Handler: s.onAppPkgUpdate,
		Group:   "events-from-app-catalog",
	})
	return nil
}

// Shutdown is required by the symphony's manager plugin interface
func (s *DeploymentManager) Shutdown(ctx context.Context) error {
	return nil
}

func (s *DeploymentManager) storeDeploymentInDB(ctx context.Context, id string, deployment margoNonStdAPI.ApplicationDeploymentManifestResp) error {
	_, err := s.StateProvider.Upsert(ctx, states.UpsertRequest{
		Options:  states.UpsertOption{},
		Metadata: deploymentMetadata,
		Value: states.StateEntry{
			ID:   id,
			Body: deployment,
		},
	})
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "storeDeploymentInDB: Failed to upsert deployment '%s': %v", id, err)
		return fmt.Errorf("failed to upsert deployment '%s': %w", id, err)
	}
	deploymentLogger.InfofCtx(ctx, "storeDeploymentInDB: deployment '%s' stored successfully", id)
	return nil
}

func (s *DeploymentManager) updateDeploymentInDB(ctx context.Context, id string, deployment margoNonStdAPI.ApplicationDeploymentManifestResp) error {
	_, err := s.StateProvider.Upsert(ctx, states.UpsertRequest{
		Options:  states.UpsertOption{},
		Metadata: deploymentMetadata,
		Value: states.StateEntry{
			ID:   id,
			Body: deployment,
		},
	})
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "updateDeploymentInDB: Failed to update deployment '%s': %v", id, err)
		return fmt.Errorf("failed to update deployment '%s': %w", id, err)
	}
	deploymentLogger.InfofCtx(ctx, "updateDeploymentInDB: deployment '%s' updated successfully", id)
	return nil
}

func (s *DeploymentManager) deleteDeploymentFromDB(ctx context.Context, deploymentId string) error {
	err := s.StateProvider.Delete(ctx, states.DeleteRequest{
		Metadata: deploymentMetadata,
		ID:       deploymentId,
	})

	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "deleteDeploymentFromDB: Failed to delete deployment '%s': %v", deploymentId, err)
		return fmt.Errorf("failed to delete deployment '%s': %w", deploymentId, err)
	}

	deploymentLogger.InfofCtx(ctx, "deleteDeploymentFromDB: deployment '%s' deleted successfully", deploymentId)
	return nil
}

func (s *DeploymentManager) getDeploymentFromDB(ctx context.Context, deploymentId string) (*margoNonStdAPI.ApplicationDeploymentManifestResp, error) {
	entry, err := s.StateProvider.Get(ctx, states.GetRequest{
		Metadata: deploymentMetadata,
		ID:       deploymentId,
	})
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "getDeploymentFromDB: Failed to get deployment '%s': %v", deploymentId, err)
		return nil, fmt.Errorf("failed to get deployment '%s': %w", deploymentId, err)
	}

	var deployment margoNonStdAPI.ApplicationDeploymentManifestResp
	jData, _ := json.Marshal(entry.Body)
	err = json.Unmarshal(jData, &deployment)
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "getDeploymentFromDB: Failed to unmarshal deployment '%s': %v", deploymentId, err)
		return nil, fmt.Errorf("failed to unmarshal deployment '%s': %w", deploymentId, err)
	}

	deploymentLogger.InfofCtx(ctx, "getDeploymentFromDB: deployment '%s' retrieved successfully", deploymentId)
	return &deployment, nil
}

// CreateDeployment handles the deployment of an application deployment.
func (s *DeploymentManager) CreateDeployment(ctx context.Context, deploymentReq margoNonStdAPI.ApplicationDeploymentManifestRequest, existingAppPkg ApplicationPackage) (*margoNonStdAPI.ApplicationDeploymentManifestResp, error) {
	deploymentLogger.InfofCtx(ctx, "CreateDeployment: Starting deployment process for deployment '%s'", deploymentReq.Metadata.Name)

	// Validate input parameters
	if deploymentReq.Metadata.Name == "" {
		deploymentLogger.ErrorfCtx(ctx, "CreateDeployment: deployment name is required but was empty")
		return nil, fmt.Errorf("deployment name is required")
	}

	// Generate unique identifier for the deployment
	now := time.Now().UTC()
	deploymentId := margoUtils.GenerateAppDeploymentId()
	operation := margoNonStdAPI.DEPLOY
	operationState := margoNonStdAPI.ApplicationDeploymentOperationStatusPENDING

	deploymentLogger.InfofCtx(ctx, "CreateDeployment: Generated deployment ID '%s' for deployment '%s'", deploymentId, deploymentReq.Metadata.Name)
	deploymentLogger.DebugfCtx(ctx, "CreateDeployment: Initial operation state set to '%s'", operationState)

	// Convert ApplicationDeploymentManifestRequest to ApplicationDeploymentResp
	var deploymentResp margoNonStdAPI.ApplicationDeploymentManifestResp
	{
		by, _ := json.Marshal(&deploymentReq)
		json.Unmarshal(by, &deploymentResp)
	}

	deploymentResp.Metadata.Id = &deploymentId
	deploymentResp.RecentOperation = &margoNonStdAPI.ApplicationDeploymentRecentOperation{}
	deploymentResp.RecentOperation.Op = operation
	deploymentResp.RecentOperation.Status = operationState
	deploymentResp.Metadata.CreationTimestamp = &now
	deploymentState := margoNonStdAPI.ApplicationDeploymentStatusStatePENDING
	contextualInfo := "stored on wfm, yet to be synced with the device"
	deploymentResp.Status = &margoNonStdAPI.ApplicationDeploymentStatus{
		ContextualInfo: &margoNonStdAPI.ContextualInfo{
			Code:    nil,
			Message: &contextualInfo,
		},
		LastUpdateTime: &now,
		State:          &deploymentState,
	}

	for _, profile := range existingAppPkg.Description.DeploymentProfiles {
		if profile.Type == margoNonStdAPI.AppDeploymentProfileType(deploymentReq.Spec.DeploymentProfile.Type) &&
			margoNonStdAPI.DeploymentExecutionProfileType(profile.Type) == margoNonStdAPI.DeploymentExecutionProfileTypeHelmV3 {

			// Create a map of app description components for efficient lookup
			appDescComponents := make(map[string]margoNonStdAPI.HelmApplicationDeploymentProfileComponent)
			for _, componentInAppDescription := range profile.Components {
				componentAsHelm, err := componentInAppDescription.AsHelmApplicationDeploymentProfileComponent()
				if err != nil {
					deploymentLogger.Warn("Failed to parse helm component from app description, skipping",
						"error", err)
					continue
				}
				appDescComponents[componentAsHelm.Name] = componentAsHelm
			}

			// Process each component in deployment request
			for i, component := range deploymentReq.Spec.DeploymentProfile.Components {
				helmComponent, err := component.AsHelmDeploymentProfileComponent()
				if err != nil {
					deploymentLogger.Warn("Failed to parse helm component from deployment request, skipping",
						"componentIndex", i,
						"error", err)
					continue
				}

				// Find matching component in app description by name
				if appDescComponent, exists := appDescComponents[helmComponent.Name]; exists {
					deploymentLogger.Debug("Merging component properties",
						"componentName", helmComponent.Name)

					// Merge properties: start with app description, override with deployment request
					mergedProperties := s.mergeHelmComponentProperties(appDescComponent, helmComponent)
					helmComponent.Properties = mergedProperties.Properties

					// Update the component back in the deployment request
					if err := deploymentReq.Spec.DeploymentProfile.Components[i].FromHelmDeploymentProfileComponent(helmComponent); err != nil {
						deploymentLogger.Error("Failed to update merged component in deployment request",
							"componentName", helmComponent.Name,
							"error", err)
						return nil, fmt.Errorf("failed to update component %s: %w", helmComponent.Name, err)
					}

					deploymentLogger.Debug("Successfully merged and updated component",
						"componentName", helmComponent.Name)
				} else {
					deploymentLogger.Warn("Component in deployment request not found in app description",
						"componentName", helmComponent.Name,
						"availableComponents", s.getAppDescComponentNames(appDescComponents))
				}
			}

			break // Found matching profile, exit loop
		}
	}

	deploymentLogger.DebugfCtx(ctx, "CreateDeployment: deployment object prepared with ID '%s', Name '%s', Operation '%s', State '%s'",
		*deploymentResp.Metadata.Id, deploymentResp.Metadata.Name, deploymentResp.RecentOperation.Op, deploymentResp.RecentOperation.Status)

	// Store initial deployment record in database
	deploymentLogger.InfofCtx(ctx, "CreateDeployment: Storing initial deployment record in database")
	if err := s.storeDeploymentInDB(ctx, *deploymentResp.Metadata.Id, deploymentResp); err != nil {
		deploymentLogger.ErrorfCtx(ctx, "CreateDeployment: Failed to store deployment in database: %v", err)
		return nil, fmt.Errorf("failed to store app deployment in database: %w", err)
	}
	deploymentLogger.InfofCtx(ctx, "CreateDeployment: Successfully stored initial deployment record with ID '%s'", *deploymentResp.Metadata.Id)

	// Publish event after successful creation
	s.Manager.Context.Publish("newDeployment", v1alpha2.Event{
		Body: deploymentResp,
	})
	deploymentLogger.InfofCtx(ctx, "CreateDeployment: Published 'newDeployment' event for deployment '%s'", *deploymentResp.Metadata.Id)

	// Create and return response
	deploymentLogger.InfofCtx(ctx, "CreateDeployment: Successfully initiated deployment process for deployment '%s' with ID '%s'",
		deploymentResp.Metadata.Name, *deploymentResp.Metadata.Id)
	return &deploymentResp, nil
}

// mergeHelmComponentProperties merges properties with deployment request taking precedence
func (s *DeploymentManager) mergeHelmComponentProperties(
	appDescProps margoNonStdAPI.HelmApplicationDeploymentProfileComponent,
	deploymentProps margoNonStdAPI.HelmDeploymentProfileComponent) margoNonStdAPI.HelmDeploymentProfileComponent {

	merged := margoNonStdAPI.HelmDeploymentProfileComponent{}

	// Start with app description properties as defaults
	merged.Properties.Repository = appDescProps.Properties.Repository
	if appDescProps.Properties.Revision != nil {
		revision := *appDescProps.Properties.Revision
		merged.Properties.Revision = &revision
	}
	if appDescProps.Properties.Timeout != nil {
		timeout := *appDescProps.Properties.Timeout
		merged.Properties.Timeout = &timeout
	}
	if appDescProps.Properties.Wait != nil {
		wait := *appDescProps.Properties.Wait
		merged.Properties.Wait = &wait
	}

	// Override with deployment request properties (these have precedence)
	if deploymentProps.Properties.Repository != "" {
		merged.Properties.Repository = deploymentProps.Properties.Repository
	}
	if deploymentProps.Properties.Revision != nil {
		merged.Properties.Revision = deploymentProps.Properties.Revision
	}
	if deploymentProps.Properties.Timeout != nil {
		merged.Properties.Timeout = deploymentProps.Properties.Timeout
	}
	if deploymentProps.Properties.Wait != nil {
		merged.Properties.Wait = deploymentProps.Properties.Wait
	}

	return merged
}

// getAppDescComponentNames extracts component names for logging
func (s *DeploymentManager) getAppDescComponentNames(components map[string]margoNonStdAPI.HelmApplicationDeploymentProfileComponent) []string {
	names := make([]string, 0, len(components))
	for name := range components {
		names = append(names, name)
	}
	return names
}

func (s *DeploymentManager) ListDeployments(ctx context.Context) (*margoNonStdAPI.ApplicationDeploymentListResp, error) {
	var deployments []margoNonStdAPI.ApplicationDeploymentManifestResp
	entries, _, err := s.StateProvider.List(ctx, states.ListRequest{
		Metadata: deploymentMetadata,
	})
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "ListDeployments: Failed to list deployments: %v", err)
		return nil, fmt.Errorf("failed to list deployments: %w", err)
	}

	for _, entry := range entries {
		var deployment margoNonStdAPI.ApplicationDeploymentManifestResp
		jData, _ := json.Marshal(entry.Body)
		err = json.Unmarshal(jData, &deployment)
		if err == nil {
			deployments = append(deployments, deployment)
		} else {
			deploymentLogger.WarnfCtx(ctx, "ListDeployments: Failed to unmarshal entry: %v", err)
		}
	}

	toContinue := false
	resp := margoNonStdAPI.ApplicationDeploymentListResp{
		ApiVersion: "margo.org", // Update with your API version
		Kind:       "ApplicationDeploymentList",
		Items:      deployments,
		Metadata: margoNonStdAPI.PaginationMetadata{
			Continue:           &toContinue,
			RemainingItemCount: nil,
		},
	}

	deploymentLogger.InfofCtx(ctx, "ListDeployments: Listed %d deployments successfully", len(deployments))
	return &resp, nil
}

func (s *DeploymentManager) GetDeployments(ctx context.Context, deploymentId string) (*margoNonStdAPI.ApplicationDeploymentManifestResp, error) {
	deployment, err := s.getDeploymentFromDB(ctx, deploymentId)
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "GetDeployments: Failed to get deployment '%s': %v", deploymentId, err)
		return nil, fmt.Errorf("failed to get deployment '%s': %w", deploymentId, err)
	}

	deploymentLogger.InfofCtx(ctx, "GetDeployments: deployment '%s' retrieved successfully", deploymentId)
	return deployment, nil
}

// DeleteDeployment initiates the deletion process for an application deployment.
func (s *DeploymentManager) DeleteDeployment(ctx context.Context, deploymentId string) (*margoNonStdAPI.ApplicationDeploymentManifestResp, error) {
	deploymentLogger.InfofCtx(ctx, "DeleteDeployment: Starting deletion process for deployment ID '%s'", deploymentId)

	// Validate input parameter
	if deploymentId == "" {
		deploymentLogger.ErrorfCtx(ctx, "DeleteDeployment: deployment ID is required but was empty")
		return nil, fmt.Errorf("deployment ID is required")
	}

	// Retrieve deployment from database to verify existence and get current state
	deploymentLogger.DebugfCtx(ctx, "DeleteDeployment: Retrieving deployment from database")
	deployment, err := s.getDeploymentFromDB(ctx, deploymentId)
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "DeleteDeployment: Failed to retrieve deployment from database: %v", err)
		return nil, fmt.Errorf("failed to check the latest state of the app deployment: %w", err)
	}

	if deployment == nil {
		deploymentLogger.WarnfCtx(ctx, "DeleteDeployment: deployment with ID '%s' does not exist", deploymentId)
		return nil, fmt.Errorf("deployment with id %s does not exist", deploymentId)
	}

	deploymentLogger.InfofCtx(ctx, "DeleteDeployment: Found deployment '%s' with current operation '%s' and state '%s'",
		deployment.Metadata.Name, deployment.RecentOperation.Op, deployment.RecentOperation.Status)

	// Update deployment state to indicate deletion is starting
	deploymentLogger.InfofCtx(ctx, "DeleteDeployment: Initiating deletion operation for deployment '%s'", deployment.Metadata.Name)
	now := time.Now().UTC()
	deploymentLogger.DebugfCtx(ctx, "Setting operation to DELETE and status to PENDING")
	deployment.RecentOperation.Op = margoNonStdAPI.DELETE                                                                                               // Changed from DEBOARD to DELETE
	deployment.RecentOperation.Status = margoNonStdAPI.ApplicationDeploymentOperationStatus(margoNonStdAPI.ApplicationDeploymentOperationStatusPENDING) // Changed from ApplicationPackageOperationStatusPENDING to ApplicationDeploymentOperationStatusPENDING
	deployment.Status.LastUpdateTime = &now

	deploymentLogger.DebugfCtx(ctx, "DeleteDeployment: Updating deployment state to Operation='%s', State='%s'",
		deployment.RecentOperation.Op, deployment.RecentOperation.Status)

	if err := s.updateDeploymentInDB(ctx, deploymentId, *deployment); err != nil {
		deploymentLogger.ErrorfCtx(ctx, "DeleteDeployment: Failed to update deployment state in database: %v", err)
		return nil, fmt.Errorf("failed to change the app deployment operation state before triggering deletion: %w", err)
	}

	deploymentLogger.InfofCtx(ctx, "DeleteDeployment: Successfully updated deployment state to pending deletion")

	// Start asynchronous deletion process
	deploymentLogger.InfofCtx(ctx, "DeleteDeployment: Starting background deletion process for deployment '%s'", deploymentId)

	// Publish event after successful deletion
	s.Manager.Context.Publish("deleteDeployment", v1alpha2.Event{
		Body: *deployment,
	})
	deploymentLogger.InfofCtx(ctx, "DeleteDeployment: Published 'deleteDeployment' event for deployment '%s'", deploymentId)

	return deployment, nil
}

func (s *DeploymentManager) onDeploymentStatusUpdate(topic string, event v1alpha2.Event) error {
	// update the status of the deployment in database
	deviceLogger.InfofCtx(context.Background(), "onDeploymentStatusUpdate: Received event on topic '%s'", topic)

	deploymentResp, ok := event.Body.(margoNonStdAPI.ApplicationDeploymentManifestResp)
	if !ok {
		deviceLogger.ErrorfCtx(context.Background(), "onDeploymentStatusUpdate: Invalid event body: deployment is missing or not of the correct type")
		return fmt.Errorf("invalid event body: deployment is missing or not of the correct type")
	}

	deploymentLogger.InfofCtx(context.Background(), "onDeploymentStatusUpdate: Handling new deployment event for deployment '%s'", *deploymentResp.Metadata.Id)
	deviceId := *deploymentResp.Spec.DeviceRef.Id

	// Save app state to device's local database
	err := s.updateDeploymentInDB(context.Background(), deviceId, deploymentResp)
	if err != nil {
		deploymentLogger.ErrorfCtx(context.Background(), "onDeploymentStatusUpdate: Failed to save app state for deployment '%s': %v", *deploymentResp.Metadata.Id, err)
		return fmt.Errorf("failed to save app state for deployment '%s': %w", *deploymentResp.Metadata.Id, err)
	}

	deploymentLogger.InfofCtx(context.Background(), "onDeploymentStatusUpdate: Successfully handled new deployment event for deployment '%s'", *deploymentResp.Metadata.Id)
	return nil
}

func (s *DeploymentManager) onAppPkgAddition(topic string, event v1alpha2.Event) error {
	// update the status of the deployment in database
	deviceLogger.InfofCtx(context.Background(), "onAppPkgAddition: Received event on topic '%s'", topic)

	deploymentResp, ok := event.Body.(margoNonStdAPI.ApplicationDeploymentManifestResp)
	if !ok {
		deviceLogger.ErrorfCtx(context.Background(), "onAppPkgAddition: Invalid event body: deployment is missing or not of the correct type")
		return fmt.Errorf("invalid event body: deployment is missing or not of the correct type")
	}

	deploymentLogger.InfofCtx(context.Background(), "onAppPkgAddition: Handling new deployment event for deployment '%s'", *deploymentResp.Metadata.Id)
	deviceId := *deploymentResp.Spec.DeviceRef.Id

	// Save app state to device's local database
	err := s.updateDeploymentInDB(context.Background(), deviceId, deploymentResp)
	if err != nil {
		deploymentLogger.ErrorfCtx(context.Background(), "onAppPkgAddition: Failed to save app state for deployment '%s': %v", *deploymentResp.Metadata.Id, err)
		return fmt.Errorf("failed to save app state for deployment '%s': %w", *deploymentResp.Metadata.Id, err)
	}

	deploymentLogger.InfofCtx(context.Background(), "onAppPkgAddition: Successfully handled new deployment event for deployment '%s'", *deploymentResp.Metadata.Id)
	return nil
}

func (s *DeploymentManager) onAppPkgDeletion(topic string, event v1alpha2.Event) error {
	// update the status of the deployment in database
	deviceLogger.InfofCtx(context.Background(), "onAppPkgDeletion: Received event on topic '%s'", topic)

	deploymentResp, ok := event.Body.(margoNonStdAPI.ApplicationDeploymentManifestResp)
	if !ok {
		deviceLogger.ErrorfCtx(context.Background(), "onAppPkgDeletion: Invalid event body: deployment is missing or not of the correct type")
		return fmt.Errorf("invalid event body: deployment is missing or not of the correct type")
	}

	deploymentLogger.InfofCtx(context.Background(), "onAppPkgDeletion: Handling new deployment event for deployment '%s'", *deploymentResp.Metadata.Id)
	deviceId := *deploymentResp.Spec.DeviceRef.Id

	// Save app state to device's local database
	err := s.updateDeploymentInDB(context.Background(), deviceId, deploymentResp)
	if err != nil {
		deploymentLogger.ErrorfCtx(context.Background(), "onAppPkgDeletion: Failed to save app state for deployment '%s': %v", *deploymentResp.Metadata.Id, err)
		return fmt.Errorf("failed to save app state for deployment '%s': %w", *deploymentResp.Metadata.Id, err)
	}

	deploymentLogger.InfofCtx(context.Background(), "onAppPkgDeletion: Successfully handled new deployment event for deployment '%s'", *deploymentResp.Metadata.Id)
	return nil
}

func (s *DeploymentManager) onAppPkgUpdate(topic string, event v1alpha2.Event) error {
	// update the status of the deployment in database
	deviceLogger.InfofCtx(context.Background(), "onAppPkgUpdate: Received event on topic '%s'", topic)

	deploymentResp, ok := event.Body.(margoNonStdAPI.ApplicationDeploymentManifestResp)
	if !ok {
		deviceLogger.ErrorfCtx(context.Background(), "onAppPkgUpdate: Invalid event body: deployment is missing or not of the correct type")
		return fmt.Errorf("invalid event body: deployment is missing or not of the correct type")
	}

	deploymentLogger.InfofCtx(context.Background(), "onAppPkgUpdate: Handling new deployment event for deployment '%s'", *deploymentResp.Metadata.Id)
	deviceId := *deploymentResp.Spec.DeviceRef.Id

	// Save app state to device's local database
	err := s.updateDeploymentInDB(context.Background(), deviceId, deploymentResp)
	if err != nil {
		deploymentLogger.ErrorfCtx(context.Background(), "onAppPkgUpdate: Failed to save app state for deployment '%s': %v", *deploymentResp.Metadata.Id, err)
		return fmt.Errorf("failed to save app state for deployment '%s': %w", *deploymentResp.Metadata.Id, err)
	}

	deploymentLogger.InfofCtx(context.Background(), "onAppPkgUpdate: Successfully handled new deployment event for deployment '%s'", *deploymentResp.Metadata.Id)
	return nil
}
