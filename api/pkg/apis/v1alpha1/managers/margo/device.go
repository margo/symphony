package margo

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/Nerzal/gocloak/v13"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/validation"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/contexts"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/states"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	"github.com/kr/pretty"
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

	deviceAppNamespace = "margoApp"
	deviceAppResource  = "deviceApp"
	deviceAppKind      = "DeviceApp"
	deviceAppMetadata  = map[string]interface{}{
		"version":   "v1",
		"group":     model.MargoGroup,
		"resource":  deviceAppResource,
		"namespace": deviceAppNamespace,
		"kind":      deviceAppKind,
	}
)

type TokenData struct {
	AccessToken  string
	RefreshToken string
	ExpiresIn    int
	TokenType    string
}

type DeviceManager struct {
	managers.Manager
	StateProvider  states.IStateProvider
	needValidate   bool
	MargoValidator validation.MargoValidator
	// keycloak related
	keycloakURL   string
	adminUsername string
	adminPassword string
	realm         string
}

func (s *DeviceManager) Init(context *contexts.VendorContext, config managers.ManagerConfig, providers map[string]providers.IProvider) error {
	err := s.Manager.Init(context, config, providers)
	if err != nil {
		return err
	}
	// Initialize Keycloak configuration
	s.keycloakURL = config.Properties["keycloakURL"]
	s.adminUsername = config.Properties["adminUsername"]
	s.adminPassword = config.Properties["adminPassword"]
	s.realm = config.Properties["realm"]
	// Validate required Keycloak configuration
	if s.keycloakURL == "" {
		return fmt.Errorf("keycloak.url is required for device onboarding")
	}
	if s.adminUsername == "" {
		return fmt.Errorf("keycloak.admin.username is required for device onboarding")
	}
	if s.adminPassword == "" {
		return fmt.Errorf("keycloak.admin.password is required for device onboarding")
	}
	if s.realm == "" {
		s.realm = "master" // default realm
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

	// // subscribe to events
	// context.Subscribe("newAppPackage", v1alpha2.EventHandler{
	// 	Handler: s.onNewAppPackage,
	// 	Group:   "events-from-deployment-manager",
	// })

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
	if deviceId == "" || deploymentId == "" || status == "" {
		return fmt.Errorf("deviceId, deploymentId, and status are required")
	}

	// Update deployment status in database
	allDeployments, _ := s.GetDeploymentsByDevice(ctx, deviceId)
	var deployment *margoNonStdAPI.ApplicationDeploymentResp
	for _, deploymentInDB := range allDeployments {
		if deploymentInDB.Metadata.Id == &deploymentId {
			deployment = &deploymentInDB
			break
		}
	}
	if deployment == nil {
		return fmt.Errorf("deployment: %s doesnot seem to be under device: %s", deploymentId, deviceId)
	}
	// Missing: Validate status values
	deployment.Status.State = (*margoNonStdAPI.ApplicationDeploymentStatusState)(&status)

	if err := s.saveAppState(ctx, deviceId, *deployment); err != nil {
		return fmt.Errorf("failed to store update the app status in database, %s", err.Error())
	}

	appState, _ := s.getAppState(ctx, deviceId, deploymentId)

	s.Manager.Context.Publish("deploymentStatusUpdates", v1alpha2.Event{
		Body: appState,
	})
	return nil
}

// saveAppState saves the application deployment state to the state provider.
func (s *DeviceManager) saveAppState(context context.Context, deviceId string, deployment margoNonStdAPI.ApplicationDeploymentResp) error {
	compositeKey := s.getCompositeKey(deviceId, *deployment.Metadata.Id)
	_, err := s.StateProvider.Upsert(context, states.UpsertRequest{
		Options:  states.UpsertOption{},
		Metadata: deviceAppMetadata,
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
		Metadata: deviceAppMetadata,
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
		Metadata: deviceAppMetadata,
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
		Metadata: deviceAppMetadata,
	})
	if err != nil {
		deviceLogger.ErrorfCtx(context, "GetDeploymentsByDevice: Failed to list deployments for device '%s': %v", deviceId, err)
		return nil, fmt.Errorf("failed to list deployments for device '%s': %w", deviceId, err)
	}
	fmt.Println("Entries found: ", pretty.Sprint(entries))
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
		Metadata: deviceAppMetadata,
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

// reportDeviceCapabilities handles the POST /device/{deviceId}/capabilities endpoint
// This creates new device capabilities entry
func (s *DeviceManager) ReportDeviceCapabilities(ctx context.Context, deviceId string, capabilities margoStdAPI.DeviceCapabilities) error {
	deviceLogger.InfofCtx(ctx, "ReportDeviceCapabilities: Reporting capabilities for device '%s'", deviceId)

	// Validate input
	if deviceId == "" {
		deviceLogger.ErrorfCtx(ctx, "ReportDeviceCapabilities: Device ID is required")
		return fmt.Errorf("device ID is required")
	}

	// Create composite key for capabilities
	capabilitiesKey := s.getCapabilitiesKey(deviceId)

	// Check if capabilities already exist (for POST, we expect them not to exist)
	_, err := s.StateProvider.Get(ctx, states.GetRequest{
		Metadata: deviceMetadata,
		ID:       capabilitiesKey,
	})
	if err == nil {
		deviceLogger.ErrorfCtx(ctx, "ReportDeviceCapabilities: Capabilities already exist for device '%s'", deviceId)
		return fmt.Errorf("capabilities already exist for device '%s', use PUT to update", deviceId)
	}

	// Save capabilities to state provider
	_, err = s.StateProvider.Upsert(ctx, states.UpsertRequest{
		Options:  states.UpsertOption{},
		Metadata: deviceMetadata,
		Value: states.StateEntry{
			ID:   capabilitiesKey,
			Body: capabilities,
		},
	})
	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "ReportDeviceCapabilities: Failed to save capabilities for device '%s': %v", deviceId, err)
		return fmt.Errorf("failed to save capabilities for device '%s': %w", deviceId, err)
	}

	deviceLogger.InfofCtx(ctx, "ReportDeviceCapabilities: Successfully reported capabilities for device '%s'", deviceId)
	return nil
}

// updateDeviceCapabilities handles the PUT /device/{deviceId}/capabilities endpoint
// This updates existing device capabilities entry
func (s *DeviceManager) UpdateDeviceCapabilities(ctx context.Context, deviceId string, capabilities margoStdAPI.DeviceCapabilities) error {
	deviceLogger.InfofCtx(ctx, "UpdateDeviceCapabilities: Updating capabilities for device '%s'", deviceId)

	// Validate input
	if deviceId == "" {
		deviceLogger.ErrorfCtx(ctx, "UpdateDeviceCapabilities: Device ID is required")
		return fmt.Errorf("device ID is required")
	}

	// Create composite key for capabilities
	capabilitiesKey := s.getCapabilitiesKey(deviceId)

	// Check if capabilities exist (for PUT, we expect them to exist)
	_, err := s.StateProvider.Get(ctx, states.GetRequest{
		Metadata: deviceMetadata,
		ID:       capabilitiesKey,
	})
	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "UpdateDeviceCapabilities: Capabilities not found for device '%s': %v", deviceId, err)
		return fmt.Errorf("capabilities not found for device '%s': %w", deviceId, err)
	}

	// Update capabilities in state provider
	_, err = s.StateProvider.Upsert(ctx, states.UpsertRequest{
		Options:  states.UpsertOption{},
		Metadata: deviceMetadata,
		Value: states.StateEntry{
			ID:   capabilitiesKey,
			Body: capabilities,
		},
	})
	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "UpdateDeviceCapabilities: Failed to update capabilities for device '%s': %v", deviceId, err)
		return fmt.Errorf("failed to update capabilities for device '%s': %w", deviceId, err)
	}

	deviceLogger.InfofCtx(ctx, "UpdateDeviceCapabilities: Successfully updated capabilities for device '%s'", deviceId)
	return nil
}

// getDeviceCapabilities retrieves device capabilities from state provider
func (s *DeviceManager) GetDeviceCapabilities(ctx context.Context, deviceId string) (*margoStdAPI.DeviceCapabilities, error) {
	deviceLogger.InfofCtx(ctx, "GetDeviceCapabilities: Retrieving capabilities for device '%s'", deviceId)

	capabilitiesKey := s.getCapabilitiesKey(deviceId)
	entry, err := s.StateProvider.Get(ctx, states.GetRequest{
		Metadata: deviceMetadata,
		ID:       capabilitiesKey,
	})
	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "GetDeviceCapabilities: Failed to get capabilities for device '%s': %v", deviceId, err)
		return nil, fmt.Errorf("failed to get capabilities for device '%s': %w", deviceId, err)
	}

	var capabilities margoStdAPI.DeviceCapabilities
	jData, _ := json.Marshal(entry.Body)
	err = json.Unmarshal(jData, &capabilities)
	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "GetDeviceCapabilities: Failed to unmarshal capabilities for device '%s': %v", deviceId, err)
		return nil, fmt.Errorf("failed to unmarshal capabilities for device '%s': %w", deviceId, err)
	}

	deviceLogger.InfofCtx(ctx, "GetDeviceCapabilities: Successfully retrieved capabilities for device '%s'", deviceId)
	return &capabilities, nil
}

// Helper function to generate capabilities key
func (s *DeviceManager) getCapabilitiesKey(deviceId string) string {
	return fmt.Sprintf("%s-capabilities", deviceId)
}

// compareAppState is not implemented.
func (s *DeviceManager) compareAppState(context context.Context, pkgId string) (*margoStdAPI.AppState, error) {
	return nil, nil
}

func (dm *DeviceManager) GetToken(ctx context.Context, clientId, clientSecret, tokenEndpointUrl string) (*TokenData, error) {
	client := gocloak.NewClient(dm.keycloakURL)

	token, err := client.LoginClient(ctx, clientId, clientSecret, dm.realm)
	if err != nil {
		return nil, fmt.Errorf("failed to authenticate with Keycloak: %w", err)
	}

	return &TokenData{
		AccessToken:  token.AccessToken,
		RefreshToken: token.RefreshToken,
		ExpiresIn:    token.ExpiresIn,
		TokenType:    token.TokenType,
	}, nil
}

func (dm *DeviceManager) OnboardDevice(ctx context.Context) (*DeviceOnboardingData, error) {
	// Generate unique client ID for the device
	clientID := fmt.Sprintf("device-%s-%d", generateDeviceID(), time.Now().Unix())

	// Create Keycloak admin client
	client := gocloak.NewClient(dm.keycloakURL)

	// Get admin token
	token, err := client.LoginAdmin(ctx, dm.adminUsername, dm.adminPassword, dm.realm)
	if err != nil {
		return nil, fmt.Errorf("failed to authenticate with Keycloak: %w", err)
	}

	// Generate client secret
	clientSecret := generateClientSecret()

	// Define client configuration
	keycloakClient := gocloak.Client{
		ClientID:                  &clientID,
		Secret:                    &clientSecret,
		Enabled:                   gocloak.BoolP(true),
		PublicClient:              gocloak.BoolP(false),
		ServiceAccountsEnabled:    gocloak.BoolP(true),
		StandardFlowEnabled:       gocloak.BoolP(false),
		DirectAccessGrantsEnabled: gocloak.BoolP(true),
		Protocol:                  gocloak.StringP("openid-connect"),
		Attributes: &map[string]string{
			"device.onboarded": "true",
			"created.by":       "device-manager",
		},
	}

	// Create client in Keycloak
	createdClientID, err := client.CreateClient(ctx, token.AccessToken, dm.realm, keycloakClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create Keycloak client: %w", err)
	}

	// Verify client was created successfully
	if createdClientID == "" {
		return nil, fmt.Errorf("client creation returned empty ID")
	}

	// Log successful onboarding
	log.Printf("Successfully onboarded device with client ID: %s", clientID)

	return &DeviceOnboardingData{
		ClientId:         clientID,
		ClientSecret:     clientSecret,
		TokenEndpointUrl: dm.keycloakURL,
	}, nil
}

// Helper function to generate unique device ID
func generateDeviceID() string {
	return fmt.Sprintf("%x", rand.Uint64())
}

// Helper function to generate secure client secret
func generateClientSecret() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const secretLength = 32

	b := make([]byte, secretLength)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

type DeviceOnboardingData struct {
	ClientId         string
	ClientSecret     string
	TokenEndpointUrl string
}

// PollDesiredState is not implemented.
func (s *DeviceManager) PollDesiredState(ctx context.Context, deviceId string, currentStates margoStdAPI.CurrentAppStates) (margoStdAPI.DesiredAppStates, error) {
	potentialCandidates := margoStdAPI.CurrentAppStates{}
	if len(currentStates) == 0 {
		allDeployments, err := s.listAppStates(ctx, deviceId)
		if err != nil {
			return potentialCandidates, err
		}

		for _, deploymentInDB := range allDeployments {
			dep, err := ConvertNBIAppDeploymentToSBIAppDeployment(&deploymentInDB)
			if err != nil {
				return nil, err
			}
			var desiredState margoStdAPI.AppStateAppState
			if deploymentInDB.RecentOperation.Op == margoNonStdAPI.DELETE {
				desiredState = margoStdAPI.REMOVING
			}
			if deploymentInDB.RecentOperation.Op == margoNonStdAPI.DEPLOY {
				desiredState = margoStdAPI.RUNNING
			}
			if deploymentInDB.RecentOperation.Op == margoNonStdAPI.UPDATE {
				desiredState = margoStdAPI.UPDATING
			}

			appState, err := pkg.ConvertAppDeploymentToAppState(dep, "app-123", "v1", string(desiredState))
			if err != nil {
				return potentialCandidates, err
			}

			potentialCandidates = append(potentialCandidates, appState)
		}
		return potentialCandidates, nil
	}

	finalCandidates := margoStdAPI.CurrentAppStates{}
	for _, currentState := range currentStates {
		for index, potentialCandiate := range potentialCandidates {
			if potentialCandiate.AppId == currentState.AppId {

				// this is a scenario where the hash of the application has changed hence the device should use this new version
				if potentialCandiate.AppDeploymentYAMLHash != currentState.AppDeploymentYAMLHash {
					finalCandidates = append(finalCandidates, potentialCandidates[index])
					continue
				}

				// this is a scenario where the state of the application is supposed to be changed, but the device has some different state
				if string(potentialCandiate.AppState) != string(currentState.AppState) {
					finalCandidates = append(finalCandidates, potentialCandidates[index])
				}

			}
		}
	}
	return finalCandidates, nil
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
