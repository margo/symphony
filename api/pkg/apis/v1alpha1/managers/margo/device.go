package margo

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/validation"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/contexts"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/states"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	margoNonStdAPI "github.com/margo/dev-repo/non-standard/generatedCode/wfm/nbi"
	"github.com/margo/dev-repo/standard/generatedCode/wfm/sbi"
	margoStdAPI "github.com/margo/dev-repo/standard/generatedCode/wfm/sbi"
	"github.com/margo/dev-repo/standard/pkg"
)

var (
	deviceLogger    = logger.NewLogger("coa.runtime")
	deviceNamespace = "margo"
	deviceResource  = "device"
	deviceKind      = "Device"
	deviceMetadata  = map[string]interface{}{
		"version":   "v1",
		"group":     model.MargoGroup,
		"resource":  deviceResource,
		"namespace": deviceNamespace,
		"kind":      deviceKind,
	}
)

type DeviceManager struct {
	managers.Manager
	StateProvider  states.IStateProvider
	needValidate   bool
	MargoValidator validation.MargoValidator
}

func (s *DeviceManager) Init(context *contexts.VendorContext, config managers.ManagerConfig, providers map[string]providers.IProvider) error {
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

	// subscribe to events
	context.Subscribe("newDeployment", v1alpha2.EventHandler{
		Handler: s.onNewDeploymentEvent,
		Group:   "events-from-deployment-manager",
	})

	context.Subscribe("deleteDeployment", v1alpha2.EventHandler{
		Handler: s.onUpdateDeploymentEvent,
		Group:   "events-from-deployment-manager",
	})

	return nil
}

// onNewDeploymentEvent handles the "newDeployment" event.
func (s *DeviceManager) onNewDeploymentEvent(topic string, event v1alpha2.Event) error {
	deviceLogger.InfofCtx(context.Background(), "onNewDeploymentEvent: Received event on topic '%s'", topic)

	deploymentResp, ok := event.Body.(margoNonStdAPI.ApplicationDeploymentResp)
	if !ok {
		deviceLogger.ErrorfCtx(context.Background(), "onNewDeploymentEvent: Invalid event body: deployment is missing or not of the correct type")
		return fmt.Errorf("invalid event body: deployment is missing or not of the correct type")
	}

	deploymentLogger.InfofCtx(context.Background(), "onNewDeploymentEvent: Handling new deployment event for deployment '%s'", *deploymentResp.Metadata.Id)
	deviceId := *deploymentResp.Spec.DeviceRef.Id

	// Save app state to device's local database
	err := s.saveAppState(context.Background(), deviceId, deploymentResp)
	if err != nil {
		deploymentLogger.ErrorfCtx(context.Background(), "onNewDeploymentEvent: Failed to save app state for deployment '%s': %v", *deploymentResp.Metadata.Id, err)
		return fmt.Errorf("failed to save app state for deployment '%s': %w", *deploymentResp.Metadata.Id, err)
	}

	deploymentLogger.InfofCtx(context.Background(), "onNewDeploymentEvent: Successfully handled new deployment event for deployment '%s'", *deploymentResp.Metadata.Id)
	return nil
}

// onUpdateDeploymentEvent handles the "deleteDeployment" event.
func (s *DeviceManager) onUpdateDeploymentEvent(topic string, event v1alpha2.Event) error {
	deploymentLogger.InfofCtx(context.Background(), "onUpdateDeploymentEvent: Received event on topic '%s'", topic)

	deploymentResp, ok := event.Body.(margoNonStdAPI.ApplicationDeploymentResp)
	if !ok {
		deviceLogger.ErrorfCtx(context.Background(), "onUpdateDeploymentEvent: Invalid event body: deployment is missing or not of the correct type")
		return fmt.Errorf("invalid event body: deployment is missing or not of the correct type")
	}

	// Remove app state from device's local database
	err := s.updateAppState(context.Background(), *deploymentResp.Spec.DeviceRef.Id, deploymentResp)
	if err != nil {
		deploymentLogger.ErrorfCtx(context.Background(), "onUpdateDeploymentEvent: Failed to remove app state for deployment '%s': %v", deploymentResp.Metadata.Id, err)
		return fmt.Errorf("failed to remove app state for deployment '%s': %w", deploymentResp.Metadata.Id, err)
	}

	deploymentLogger.InfofCtx(context.Background(), "onUpdateDeploymentEvent: Successfully handled deletion of deployment '%s'", deploymentResp.Metadata.Id)
	return nil
}

// Called when device reports status update for the deployments
func (s *DeviceManager) OnDeploymentStatus(ctx context.Context, deviceId, deploymentId string, status string) error {
	// Update device DB with new status
	// ...
	// Notify deployment manager
	s.Manager.Context.Publish("deploymentStatusUpdates", v1alpha2.Event{
		Body: map[string]interface{}{
			"deploymentId": deploymentId,
			"status":       status,
		},
	})
	return nil
}

// saveAppState saves the application deployment state to the state provider.
func (s *DeviceManager) saveAppState(context context.Context, deviceId string, deployment margoNonStdAPI.ApplicationDeploymentResp) error {
	compositeKey := s.getCompositeKey(deviceId, *deployment.Metadata.Id)
	_, err := s.StateProvider.Upsert(context, states.UpsertRequest{
		Options:  states.UpsertOption{},
		Metadata: deviceMetadata,
		Value: states.StateEntry{
			ID:   compositeKey,
			Body: deployment,
		},
	})
	if err != nil {
		deviceLogger.ErrorfCtx(context, "saveAppState: Failed to upsert app state for deployment '%s' on device '%s': %v", *deployment.Metadata.Id, deviceId, err)
		return fmt.Errorf("failed to upsert app state for deployment '%s' on device '%s': %w", *deployment.Metadata.Id, deviceId, err)
	}
	deviceLogger.InfofCtx(context, "saveAppState: App state for deployment '%s' on device '%s' saved successfully", *deployment.Metadata.Id, deviceId)
	return nil
}

// updateAppState updates the application deployment state in the state provider.
func (s *DeviceManager) updateAppState(context context.Context, deviceId string, deployment margoNonStdAPI.ApplicationDeploymentResp) error {
	compositeKey := s.getCompositeKey(deviceId, *deployment.Metadata.Id)
	_, err := s.StateProvider.Upsert(context, states.UpsertRequest{
		Options:  states.UpsertOption{},
		Metadata: deviceMetadata,
		Value: states.StateEntry{
			ID:   compositeKey,
			Body: deployment,
		},
	})
	if err != nil {
		deviceLogger.ErrorfCtx(context, "updateAppState: Failed to update app state for deployment '%s' on device '%s': %v", *deployment.Metadata.Id, deviceId, err)
		return fmt.Errorf("failed to update app state for deployment '%s' on device '%s': %w", *deployment.Metadata.Id, deviceId, err)
	}
	deviceLogger.InfofCtx(context, "updateAppState: App state for deployment '%s' on device '%s' updated successfully", *deployment.Metadata.Id, deviceId)
	return nil
}

// removeAppState removes the application deployment state from the state provider.
func (s *DeviceManager) removeAppState(context context.Context, deviceId string, deploymentId string) error {
	compositeKey := s.getCompositeKey(deviceId, deploymentId)
	err := s.StateProvider.Delete(context, states.DeleteRequest{
		Metadata: deviceMetadata,
		ID:       compositeKey,
	})
	if err != nil {
		deviceLogger.ErrorfCtx(context, "removeAppState: Failed to delete app state for deployment '%s' on device '%s': %v", deploymentId, deviceId, err)
		return fmt.Errorf("failed to delete app state for deployment '%s' on device '%s': %w", deploymentId, deviceId, err)
	}
	deviceLogger.InfofCtx(context, "removeAppState: App state for deployment '%s' on device '%s' removed successfully", deploymentId, deviceId)
	return nil
}

// listAppStates is not implemented.
func (s *DeviceManager) listAppStates(context context.Context, deviceId string) ([]margoNonStdAPI.ApplicationDeploymentResp, error) {
	var deployments []margoNonStdAPI.ApplicationDeploymentResp
	entries, _, err := s.StateProvider.List(context, states.ListRequest{
		Metadata: deviceMetadata,
	})
	if err != nil {
		deviceLogger.ErrorfCtx(context, "GetDeploymentsByDevice: Failed to list deployments for device '%s': %v", deviceId, err)
		return nil, fmt.Errorf("failed to list deployments for device '%s': %w", deviceId, err)
	}

	for _, entry := range entries {
		var appState margoNonStdAPI.ApplicationDeploymentResp
		jData, _ := json.Marshal(entry.Body)
		err = json.Unmarshal(jData, &appState)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal app state %w", err)
		}

		deployments = append(deployments, appState)
	}

	return deployments, nil
}

// getAppState retrieves the application deployment state from the state provider.
func (s *DeviceManager) getAppState(context context.Context, deviceId string, deploymentId string) (*margoNonStdAPI.ApplicationDeploymentResp, error) {
	compositeKey := s.getCompositeKey(deviceId, deploymentId)
	entry, err := s.StateProvider.Get(context, states.GetRequest{
		Metadata: deviceMetadata,
		ID:       compositeKey,
	})
	if err != nil {
		deviceLogger.ErrorfCtx(context, "getAppState: Failed to get app state for deployment '%s' on device '%s': %v", deploymentId, deviceId, err)
		return nil, fmt.Errorf("failed to get app state for deployment '%s' on device '%s': %w", deploymentId, deviceId, err)
	}

	var appState margoNonStdAPI.ApplicationDeploymentResp
	jData, _ := json.Marshal(entry.Body)
	err = json.Unmarshal(jData, &appState)
	if err != nil {
		deviceLogger.ErrorfCtx(context, "getAppState: Failed to unmarshal app state for deployment '%s' on device '%s': %v", deploymentId, deviceId, err)
		return nil, fmt.Errorf("failed to unmarshal app state for deployment '%s' on device '%s': %w", deploymentId, deviceId, err)
	}

	deviceLogger.InfofCtx(context, "getAppState: App state for deployment '%s' on device '%s' retrieved successfully", deploymentId, deviceId)
	return &appState, nil
}

// compareAppState is not implemented.
func (s *DeviceManager) compareAppState(context context.Context, pkgId string) (*margoStdAPI.AppState, error) {
	return nil, nil
}

// PollDesiredState is not implemented.
func (s *DeviceManager) PollDesiredState(ctx context.Context, deviceId string, currentStates margoStdAPI.CurrentAppStates) (margoStdAPI.DesiredAppStates, error) {
	desiredStates := margoStdAPI.CurrentAppStates{}
	if len(currentStates) == 0 {
		allDeployments, err := s.listAppStates(ctx, deviceId)
		if err != nil {
			return desiredStates, err
		}

		for _, deploymentInDB := range allDeployments {
			dep, err := ConvertNBIAppDeploymentToSBIAppDeployment(&deploymentInDB)
			if err != nil {
				return nil, err
			}
			appState, err := pkg.ConvertAppDeploymentToAppState(dep, "", "")
			if err != nil {
				return desiredStates, err
			}

			desiredStates = append(desiredStates, appState)
		}
		return desiredStates, nil
	}

	// for _, currentState := range currentStates {
	// currentState.AppDeploymentYAML
	// appDeploymentHashOnDevice := currentState.AppDeploymentYAMLHash
	// appDeploymentOnDevice, err := pkg.ConvertAppStateToAppDeployment(currentState)
	// if err != nil {
	// 	return desiredStates, err
	// }

	// deploymentInDB, err := s.getAppState(ctx, deviceId, *appDeploymentOnDevice.Metadata.Id)
	// if err != nil {
	// 	return desiredStates, err
	// }
	// // deploymentHashInDB := deploymentInDB.

	// deploymentDefinition, err := ConvertNBIAppDeploymentToSBIAppDeployment(deploymentInDB)
	// if err != nil {
	// 	return nil, err
	// }

	// if apphashInDB != appDeploymentHashOnDevice {
	// 	// state has changed, we need to send the new app deployment yaml with hash
	// 	newAppState, err := pkg.ConvertAppDeploymentToAppState(deploymentDefinition, "", "")
	// 	if err != nil {
	// 		return desiredStates, err
	// 	}

	// 	desiredStates = append(desiredStates, newAppState)
	// }

	// if currentState.AppState == margoStdAPI.RUNNING && deploymentInDB.
	// }
	return desiredStates, nil
}

// Shutdown is required by the symphony's manager plugin interface
func (s *DeviceManager) Shutdown(ctx context.Context) error {
	return nil
}

// getCompositeKey generates a composite key from device ID and deployment ID.
func (s *DeviceManager) getCompositeKey(deviceId string, deploymentId string) string {
	return fmt.Sprintf("%s-%s", deviceId, deploymentId)
}

// GetDeploymentsByDevice retrieves all deployments for a given device ID.
func (s *DeviceManager) GetDeploymentsByDevice(ctx context.Context, deviceId string) ([]margoNonStdAPI.ApplicationDeploymentResp, error) {
	var deployments []margoNonStdAPI.ApplicationDeploymentResp
	entries, _, err := s.StateProvider.List(ctx, states.ListRequest{
		Metadata: deviceMetadata,
	})
	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "GetDeploymentsByDevice: Failed to list deployments for device '%s': %v", deviceId, err)
		return nil, fmt.Errorf("failed to list deployments for device '%s': %w", deviceId, err)
	}

	for _, entry := range entries {
		if strings.HasPrefix(entry.ID, deviceId+"-") {
			var deployment margoNonStdAPI.ApplicationDeploymentResp
			jData, _ := json.Marshal(entry.Body)
			err = json.Unmarshal(jData, &deployment)
			if err == nil {
				deployments = append(deployments, deployment)
			} else {
				deviceLogger.WarnfCtx(ctx, "GetDeploymentsByDevice: Failed to unmarshal entry: %v", err)
			}
		}
	}

	deviceLogger.InfofCtx(ctx, "GetDeploymentsByDevice: Retrieved %d deployments for device '%s' successfully", len(deployments), deviceId)
	return deployments, nil
}

// ConvertNBIAppDeploymentToSBIAppDeployment converts AppDeployment to AppState.
func ConvertNBIAppDeploymentToSBIAppDeployment(appDeployment *margoNonStdAPI.ApplicationDeploymentResp) (*sbi.AppDeployment, error) {
	var appDeploymentOnSBI sbi.AppDeployment
	{
		by, err := json.Marshal(appDeployment)
		if err != nil {
			return nil, err
		}
		json.Unmarshal(by, &appDeploymentOnSBI)
	}

	return &appDeploymentOnSBI, nil
}
