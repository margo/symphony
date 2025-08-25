package margo

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/Nerzal/gocloak/v13"
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
	deviceLogger = logger.NewLogger("coa.runtime")
)

type PackageData struct {
	CurrentState margoNonStdAPI.ApplicationPackageListResp
}

type DeploymentData struct {
	DeviceId                string
	DeploymentRequest       margoNonStdAPI.ApplicationDeploymentManifestResp
	CurrentState            *margoStdAPI.AppState
	DesiredState            *margoStdAPI.AppState
	CorrespondingAppPackage margoNonStdAPI.ApplicationPackageListResp
}

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
	Database       *MargoDatabase
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
	if err != nil {
		return err
	}
	s.Database = NewMargoDatabase(s.Context, publishGroupNameDeviceManager, stateprovider)

	s.needValidate = managers.NeedObjectValidate(config, providers)
	if s.needValidate {
		// Turn off validation of differnt types: https://github.com/eclipse-symphony/symphony/issues/445
		s.MargoValidator = validation.NewMargoValidator()
	}

	// subscribe to events
	context.Subscribe(string(upsertDeploymentFeed), v1alpha2.EventHandler{
		Handler: func(topic string, event v1alpha2.Event) error {
			producerName, exists := event.Metadata["producerName"]
			if !exists {
				// not of our concern
				return nil
			}
			if producerName != string(publishGroupNameDeploymentManager) {
				// we want updates from this producer only
				return nil
			}
			return s.upsertObjectInCache(topic, event)
		},
		Group: "events-from-deployment-manager",
	})

	context.Subscribe(string(deleteDeploymentFeed), v1alpha2.EventHandler{
		Handler: func(topic string, event v1alpha2.Event) error {
			producerName, exists := event.Metadata["producerName"]
			if !exists {
				// not of our concern
				return nil
			}
			if producerName != string(publishGroupNameDeploymentManager) {
				// we want updates from this producer only
				return nil
			}
			return s.deleteObjectFromCache(topic, event)
		},
		Group: "events-from-deployment-manager",
	})

	context.Subscribe(string(upsertPackageFeed), v1alpha2.EventHandler{
		Handler: func(topic string, event v1alpha2.Event) error {
			producerName, exists := event.Metadata["producerName"]
			if !exists {
				// not of our concern
				return nil
			}
			if producerName != string(publishGroupNamePackageManager) {
				// we want updates from this producer only
				return nil
			}
			return s.upsertObjectInCache(topic, event)
		},
		Group: "events-from-package-manager",
	})

	context.Subscribe(string(deletePackageFeed), v1alpha2.EventHandler{
		Handler: func(topic string, event v1alpha2.Event) error {
			producerName, exists := event.Metadata["producerName"]
			if !exists {
				// not of our concern
				return nil
			}
			if producerName != string(publishGroupNameDeploymentManager) &&
				producerName != string(publishGroupNamePackageManager) {
				// we want updates from this producer only
				return nil
			}
			return s.deleteObjectFromCache(topic, event)
		},
		Group: "events-from-package-manager",
	})

	return nil
}

// upsertObjectInCache handles the "newDeployment" event.
func (s *DeviceManager) upsertObjectInCache(topic string, event v1alpha2.Event) error {
	deviceLogger.InfofCtx(context.Background(), "upsertObjectInCache: Received event on topic '%s'", topic)

	var err error
	switch event.Body.(type) {
	case AppPackageDatabaseRow:
		err = s.Database.UpsertAppPackage(context.Background(), event.Body.(AppPackageDatabaseRow))
	case DeploymentDatabaseRow:
		err = s.Database.UpsertDeployment(context.Background(), event.Body.(DeploymentDatabaseRow), false)
	case DeviceDatabaseRow:
		err = s.Database.UpsertDevice(context.Background(), event.Body.(DeviceDatabaseRow))
	default:
		deviceLogger.ErrorfCtx(context.Background(), "upsertObjectInCache: Invalid event body: known object is missing or not of the correct type")
		return fmt.Errorf("invalid event body: deployment is missing or not of the correct type")
	}

	if err != nil {
		deviceLogger.ErrorfCtx(context.Background(), "upsertObjectInCache: Failed to cache object %v", err)
		return fmt.Errorf("failed to cache object %w", err)
	}

	deviceLogger.InfofCtx(context.Background(), "upsertObjectInCache: Successfully upsert object in cache")
	return nil
}

// deleteObjectFromCache handles the "newDeployment" event.
func (s *DeviceManager) deleteObjectFromCache(topic string, event v1alpha2.Event) error {
	deviceLogger.InfofCtx(context.Background(), "deleteObjectFromCache: Received event on topic '%s'", topic)

	var err error
	switch event.Body.(type) {
	case AppPackageDatabaseRow:
		err = s.Database.DeleteAppPackage(context.Background(), *event.Body.(AppPackageDatabaseRow).PackageRequest.Metadata.Id)
	case DeploymentDatabaseRow:
		err = s.Database.DeleteDeployment(context.Background(), *event.Body.(DeploymentDatabaseRow).DeploymentRequest.Metadata.Id, false)
	case DeviceDatabaseRow:
		err = s.Database.DeleteDevice(context.Background(), event.Body.(DeviceDatabaseRow).Capabilities.Properties.Id)
	default:
		deviceLogger.ErrorfCtx(context.Background(), "deleteObjectFromCache: Invalid event body: known object is missing or not of the correct type")
		return fmt.Errorf("invalid event body: deployment is missing or not of the correct type")
	}

	if err != nil {
		deviceLogger.ErrorfCtx(context.Background(), "deleteObjectFromCache: Failed to remove cached object %v", err)
		return fmt.Errorf("failed to cache object %w", err)
	}

	deviceLogger.InfofCtx(context.Background(), "deleteObjectFromCache: Successfully removed object from cache")
	return nil
}

// Called when device reports status update for the deployments
func (s *DeviceManager) OnDeploymentStatus(ctx context.Context, deviceId, deploymentId string, status string) error {
	if deviceId == "" || deploymentId == "" || status == "" {
		return fmt.Errorf("deviceId, deploymentId, and status are required")
	}
	deviceLogger.DebugCtx(ctx,
		"msg", "deployment status update request received",
		"deviceId", deviceId,
		"deploymentId", deploymentId,
		"status", status)

	// Update deployment status in database
	// Get deployment
	dbRow, err := s.Database.GetDeployment(ctx, deploymentId)
	if err != nil {
		return fmt.Errorf("deployment not found: %w", err)
	}

	// Update the current state (what device reports)
	currentState := dbRow.CurrentDeployment
	currentState.AppState = sbi.AppStateAppState(status)

	switch currentState.AppState {
	case "REMOVED":
		// Update database
		if err := s.Database.DeleteDeployment(ctx, deploymentId, true); err != nil {
			return fmt.Errorf("failed to delete current state: %w", err)
		}
	default:
		// Update database
		if err := s.Database.UpsertDeploymentCurrentState(ctx, deploymentId, currentState, true); err != nil {
			return fmt.Errorf("failed to update current state: %w", err)
		}
		// Also update the deployment request status for backward compatibility
		dbRow.DeploymentRequest.Status.State = (*margoNonStdAPI.ApplicationDeploymentStatusState)(&status)
		if err := s.Database.UpsertDeployment(ctx, *dbRow, true); err != nil {
			return fmt.Errorf("failed to update deployment: %w", err)
		}
	}
	return nil
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

func (s *DeviceManager) PollDesiredState(ctx context.Context, deviceId string, currentStates margoStdAPI.CurrentAppStates) (margoStdAPI.DesiredAppStates, error) {
	deviceLogger.InfofCtx(ctx, "PollDesiredState: Polling desired state for device '%s'", deviceId)

	// Get all deployments for this device
	dbRows, err := s.Database.GetDeploymentsByDevice(ctx, deviceId)
	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "PollDesiredState: Failed to get deployments: %v", err)
		return nil, fmt.Errorf("failed to get deployments for device %s: %w", deviceId, err)
	}

	desiredStates := make(margoStdAPI.DesiredAppStates, 0, len(dbRows))

	for _, row := range dbRows {
		// Use the actual desired state from database, not the operation
		if row.DesiredDeployment.AppId == "" {
			// If no desired state set, derive from deployment request
			desiredState := s.deriveDesiredStateFromDeployment(row.DeploymentRequest)
			row.DesiredDeployment = desiredState

			// Update database with derived state
			s.Database.UpsertDeploymentDesiredState(ctx, *row.DeploymentRequest.Metadata.Id, desiredState, true)
		}

		desiredStates = append(desiredStates, row.DesiredDeployment)
	}

	// If no current states provided, return all desired states
	if len(currentStates) == 0 {
		deviceLogger.InfofCtx(ctx, "PollDesiredState: Returning all %d desired states", len(desiredStates))
		return desiredStates, nil
	}

	// Filter based on differences
	filteredStates := make(margoStdAPI.DesiredAppStates, 0)
	for _, desired := range desiredStates {
		needsUpdate := true

		for _, current := range currentStates {
			if desired.AppId == current.AppId {
				if s.statesAreEqual(desired, current) {
					needsUpdate = false
					break
				}
			}
		}

		if needsUpdate {
			filteredStates = append(filteredStates, desired)
		}
	}

	deviceLogger.InfofCtx(ctx, "PollDesiredState: Returning %d filtered states", len(filteredStates))
	return filteredStates, nil
}

// Helper to derive desired state from deployment request
func (s *DeviceManager) deriveDesiredStateFromDeployment(deployment margoNonStdAPI.ApplicationDeploymentManifestResp) sbi.AppState {
	var appState sbi.AppStateAppState

	switch deployment.RecentOperation.Op {
	case margoNonStdAPI.DEPLOY:
		appState = sbi.RUNNING
	case margoNonStdAPI.UPDATE:
		appState = sbi.UPDATING
	case margoNonStdAPI.DELETE:
		appState = sbi.REMOVING
	default:
		appState = sbi.RUNNING // Default
	}

	// Convert deployment to SBI format
	sbiDeployment, _ := ConvertNBIAppDeploymentToSBIAppDeployment(&deployment)

	// Create proper AppState
	desiredState, _ := pkg.ConvertAppDeploymentToAppState(
		sbiDeployment,
		deployment.Spec.AppPackageRef.Id,
		"v1",
		string(appState))

	return desiredState
}

// Helper method to compare states
func (s *DeviceManager) statesAreEqual(desired, current margoStdAPI.AppState) bool {
	// Compare app state
	if desired.AppState != current.AppState {
		return false
	}

	// Compare deployment hash (configuration changes)
	if desired.AppDeploymentYAMLHash != current.AppDeploymentYAMLHash {
		return false
	}

	return true
}

// Shutdown is required by the symphony's manager plugin interface
func (s *DeviceManager) Shutdown(ctx context.Context) error {
	return nil
}

// getCompositeKey generates a composite key from device ID and deployment ID.
func (s *DeviceManager) getCompositeKey(deviceId string, deploymentId string) string {
	return fmt.Sprintf("%s-%s", deviceId, deploymentId)
}

// ConvertNBIAppDeploymentToSBIAppDeployment converts AppDeployment to AppState.
func ConvertNBIAppDeploymentToSBIAppDeployment(appDeployment *margoNonStdAPI.ApplicationDeploymentManifestResp) (*sbi.AppDeployment, error) {
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

func (s *DeviceManager) ReportDeviceCapabilities(ctx context.Context, deviceId string, capabilities margoStdAPI.DeviceCapabilities) error {
	deviceLogger.InfofCtx(ctx, "ReportDeviceCapabilities: Reporting capabilities for device '%s'", deviceId)

	if deviceId == "" {
		return fmt.Errorf("device ID is required")
	}

	// Check if device already exists
	exists, err := s.Database.DeviceExists(ctx, deviceId)
	if err != nil {
		return fmt.Errorf("failed to check device existence: %w", err)
	}
	if exists {
		return fmt.Errorf("capabilities already exist for device '%s', use PUT to update", deviceId)
	}

	// Create new device record
	deviceRow := DeviceDatabaseRow{
		DeviceId:      deviceId,
		Capabilities:  &capabilities,
		LastStateSync: time.Now().UTC(),
	}

	err = s.Database.UpsertDevice(ctx, deviceRow)
	if err != nil {
		return fmt.Errorf("failed to save capabilities for device '%s': %w", deviceId, err)
	}

	deviceLogger.InfofCtx(ctx, "ReportDeviceCapabilities: Successfully reported capabilities for device '%s'", deviceId)
	return nil
}

func (s *DeviceManager) UpdateDeviceCapabilities(ctx context.Context, deviceId string, capabilities margoStdAPI.DeviceCapabilities) error {
	return s.Database.UpdateDeviceCapabilities(ctx, deviceId, &capabilities)
}

func (s *DeviceManager) GetDeviceCapabilities(ctx context.Context, deviceId string) (*margoStdAPI.DeviceCapabilities, error) {
	device, err := s.Database.GetDevice(ctx, deviceId)
	if err != nil {
		return nil, fmt.Errorf("failed to get device capabilities: %w", err)
	}
	return device.Capabilities, nil
}
