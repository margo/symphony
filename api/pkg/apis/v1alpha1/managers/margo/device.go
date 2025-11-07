package margo

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/providers/keycloak"
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
)

var (
	deviceLogger = logger.NewLogger("coa.runtime")
)

type PackageData struct {
	CurrentState margoNonStdAPI.ApplicationPackageListResp
}

type DeploymentData struct {
	deviceClientId          string
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
	Database         *MargoDatabase
	StateProvider    states.IStateProvider
	KeycloakProvider *keycloak.KeycloakProvider
	MargoValidator   validation.MargoValidator
	needValidate     bool
}

func (s *DeviceManager) Init(pCtx *contexts.VendorContext, config managers.ManagerConfig, providers map[string]providers.IProvider) error {
	err := s.Manager.Init(pCtx, config, providers)
	if err != nil {
		return err
	}

	stateprovider, err := managers.GetPersistentStateProvider(config, providers)
	if err != nil {
		return err
	}
	s.Database = NewMargoDatabase(s.Context, deviceManagerPublisherGroup, stateprovider)

	for _, provider := range providers {
		switch p := provider.(type) {
		case *keycloak.KeycloakProvider:
			s.KeycloakProvider = p
		}
	}

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

	pCtx.Subscribe(string(upsertPackageFeed), v1alpha2.EventHandler{
		Handler: func(topic string, event v1alpha2.Event) error {
			producerName, exists := event.Metadata["producerName"]
			if !exists {
				// not of our concern
				return nil
			}
			if producerName != string(packageManagerPublisherGroup) {
				// we want updates from this producer only
				return nil
			}
			return s.upsertObjectInCache(topic, event)
		},
		Group: "events-from-package-manager",
	})

	pCtx.Subscribe(string(deletePackageFeed), v1alpha2.EventHandler{
		Handler: func(topic string, event v1alpha2.Event) error {
			producerName, exists := event.Metadata["producerName"]
			if !exists {
				// not of our concern
				return nil
			}
			if producerName != string(deploymentManagerPublisherGroup) &&
				producerName != string(packageManagerPublisherGroup) {
				// we want updates from this producer only
				return nil
			}
			return s.deleteObjectFromCache(topic, event)
		},
		Group: "events-from-package-manager",
	})

	pCtx.Subscribe(string(upsertDeploymentBundleFeed), v1alpha2.EventHandler{
		Handler: func(topic string, event v1alpha2.Event) error {
			producerName, exists := event.Metadata["producerName"]
			if !exists {
				// not of our concern
				return nil
			}
			if producerName != string(deploymentBundleManagerPublisherGroup) {
				// we want updates from this producer only
				return nil
			}
			return s.upsertObjectInCache(topic, event)
		},
		Group: "events-from-deployment-bundle-manager",
	})

	pCtx.Subscribe(string(deleteDeploymentBundleFeed), v1alpha2.EventHandler{
		Handler: func(topic string, event v1alpha2.Event) error {
			producerName, exists := event.Metadata["producerName"]
			if !exists {
				// not of our concern
				return nil
			}
			if producerName != string(deploymentBundleManagerPublisherGroup) {
				// we want updates from this producer only
				return nil
			}
			return s.deleteObjectFromCache(topic, event)
		},
		Group: "events-from-deployment-bundle-manager",
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
	case DeploymentBundleRow:
		err = s.Database.UpsertDeploymentBundle(context.Background(), event.Body.(DeploymentBundleRow), false)
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
	case DeploymentBundleRow:
		err = s.Database.DeleteDeploymentBundle(context.Background(), event.Body.(DeploymentBundleRow).DeviceClientId, false)
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
func (s *DeviceManager) OnDeploymentStatus(ctx context.Context, deviceClientId, deploymentId string, status string) error {
	if deviceClientId == "" || deploymentId == "" || status == "" {
		return fmt.Errorf("deviceClientId, deploymentId, and status are required")
	}
	deviceLogger.DebugCtx(ctx,
		"msg", "deployment status update request received",
		"deviceClientId", deviceClientId,
		"deploymentId", deploymentId,
		"status", status)

	// Update deployment status in database
	// Get deployment
	dbRow, err := s.Database.GetDeployment(ctx, deploymentId)
	if err != nil {
		return fmt.Errorf("deployment not found: %w", err)
	}

	// Update the current state (what device reports)
	currentState := dbRow.DeploymentOnDevice
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

func (dm *DeviceManager) GetToken(ctx context.Context, clientId, clientSecret string, userClaims map[string]interface{}) (*TokenData, error) {
	// dm.AuthProvider.ValidateToken(ctx, )
	result, err := dm.KeycloakProvider.GetTokenWithClaims(ctx,
		clientId,
		clientSecret,
		userClaims,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to authenticate with auth provider: %w", err)
	}

	return &TokenData{
		AccessToken:  result.AccessToken,
		RefreshToken: result.RefreshToken,
		ExpiresIn:    result.ExpiresIn,
		TokenType:    result.TokenType,
	}, nil
}

func (dm *DeviceManager) OnboardDevice(ctx context.Context, devicePubCert string) (*DeviceOnboardingData, error) {
	var success bool
	// Generate unique client ID for the device
	clientID := generateDeviceClientID()
	authClientSecret := ""
	authTokenUrl := ""

	onboardStatus := margoNonStdAPI.INPROGRESS
	if err := dm.Database.UpsertDevice(ctx, DeviceDatabaseRow{
		DeviceClientId:    clientID,
		OAuthClientSecret: authClientSecret,
		OAuthClientId:     clientID,
		OAuthTokenURL:     authTokenUrl,
		DevicePubCert:     devicePubCert,
		OnboardingStatus:  onboardStatus,
		Capabilities:      nil,
		LastStateSync:     time.Now().UTC(),
		CreatedAt:         time.Now().UTC(),
		UpdatedAt:         time.Now().UTC(),
	}); err != nil {
		return nil, fmt.Errorf("failed to save device details: %w", err)
	}

	defer func() {
		onboardStatus = margoNonStdAPI.ONBOARDED
		if !success {
			onboardStatus = margoNonStdAPI.FAILED
		}
		_ = dm.Database.UpsertDevice(ctx, DeviceDatabaseRow{
			DeviceClientId:    clientID,
			OAuthClientSecret: authClientSecret,
			OAuthClientId:     clientID,
			OAuthTokenURL:     authTokenUrl,
			DevicePubCert:     devicePubCert,
			OnboardingStatus:  onboardStatus,
			Capabilities:      nil,
			LastStateSync:     time.Now().UTC(),
			CreatedAt:         time.Now().UTC(),
			UpdatedAt:         time.Now().UTC(),
		})
	}()

	// review: devise a cleaner way for this
	if dm.KeycloakProvider != nil {
		// Define client configuration
		config := keycloak.ClientConfig{
			ClientID:                  clientID,
			Enabled:                   true,
			ServiceAccountsEnabled:    true,
			StandardFlowEnabled:       false,
			DirectAccessGrantsEnabled: true,
			// Name: ,
			Attributes: &map[string]string{
				"device.onboarded": "true",
				"created.by":       "device-manager",
			},
		}

		// Get admin token
		clientResult, err := dm.KeycloakProvider.CreateClientWithClaims(ctx, config, map[string]interface{}{
			"deviceClientId": clientID,
		})
		if err != nil {
			success = false
			return nil, fmt.Errorf("failed to authenticate with Keycloak: %s", err.Error())
		}

		// Verify client was created successfully
		if clientResult.ClientID == "" {
			success = false
			return nil, fmt.Errorf("client creation returned empty ID")
		}
		if clientResult.ClientSecret == "" {
			success = false
			return nil, fmt.Errorf("client creation returned empty ID")
		}
		if clientResult.ClientUUID == "" {
			success = false
			return nil, fmt.Errorf("client creation returned empty ID")
		}
		if clientResult.TokenUrl == "" {
			success = false
			return nil, fmt.Errorf("client creation returned empty token url")
		}

		clientID = clientResult.ClientID
		authClientSecret = clientResult.ClientSecret
		authTokenUrl = clientResult.TokenUrl
	}

	success = true

	// Log successful onboarding
	deviceLogger.InfofCtx(context.Background(), "Successfully onboarded device", "clientId", clientID)

	return &DeviceOnboardingData{
		ClientId:         clientID,
		ClientSecret:     authClientSecret,
		TokenEndpointUrl: authTokenUrl,
	}, nil
}

func (dm *DeviceManager) ListDevices(ctx context.Context) (margoNonStdAPI.DeviceListResp, error) {
	devices := margoNonStdAPI.DeviceListResp{
		ApiVersion: "non.margo.org",
		Kind:       "DeviceList",
		Items:      []margoNonStdAPI.DeviceManifestResp{},
		Metadata:   &margoNonStdAPI.PaginationMetadata{},
	}

	rows, err := dm.Database.ListDevices(ctx)
	if err != nil {
		return devices, err
	}

	for _, row := range rows {
		devices.Items = append(devices.Items, margoNonStdAPI.DeviceManifestResp{
			ApiVersion: "non.margo.org",
			Kind:       "Device",
			Metadata: margoNonStdAPI.Metadata{
				Id:                &row.DeviceClientId,
				CreationTimestamp: &row.CreatedAt,
			},
			Spec: margoNonStdAPI.DeviceSpec{
				Capabilities: row.Capabilities,
				Signature:    row.DevicePubCert,
			},
			State: margoNonStdAPI.DeviceState{
				Onboard: margoNonStdAPI.ONBOARDED,
			},
		})
	}
	deviceLogger.DebugfCtx(ctx, "Devices: ", len(devices.Items))
	return devices, nil
}

// Helper function to generate unique device ID
func generateDeviceClientID() string {
	return fmt.Sprintf("client-%s-%d", fmt.Sprintf("%x", rand.Uint64()), time.Now().Unix())
	// return )
}

type Deployment struct {
	State        sbi.DeploymentStatusStatusState
	ETag         string
	DeploymentId string
}

type DeviceOnboardingData struct {
	ClientId         string
	ClientSecret     string
	TokenEndpointUrl string
}

func (s *DeviceManager) ShouldReplaceBundle(ctx context.Context, deviceClientId string, digest *string) (shouldReplace bool, bundleArchivePath string, bundleManifest *margoStdAPI.UnsignedStateManifest, err error) {
	bundle, err := s.Database.GetDeploymentBundle(ctx, deviceClientId)
	if err != nil {
		return false, "", nil, err
	}

	if digest != nil && *bundleManifest.Bundle.Digest == *digest {
		// the bundle has not changed hence no need to send any changes
		return false, "", nil, nil
	}

	deviceLogger.InfofCtx(ctx, "ShouldReplaceBundle: Found bundle with a different digest %s, sizeBytes: %d", bundle.Manifest.Bundle.Digest, bundle.Manifest.Bundle.SizeBytes)
	return true, bundle.ArchivePath, &bundle.Manifest, nil
}

func (s *DeviceManager) GetBundle(ctx context.Context, deviceClientId string, digest *string) (bundleArchivePath string, bundleManifest *margoStdAPI.UnsignedStateManifest, err error) {
	bundle, err := s.Database.GetDeploymentBundle(ctx, deviceClientId)
	if err != nil {
		return "", nil, err
	}

	if digest != nil && *bundleManifest.Bundle.Digest != *digest {
		// no bundle found with the mentioned digest
		return "", nil, nil
	}

	deviceLogger.InfofCtx(ctx, "GetBundle: Found bundle with digest %s, sizeBytes: %d", bundle.Manifest.Bundle.Digest, bundle.Manifest.Bundle.SizeBytes)
	return bundle.ArchivePath, &bundle.Manifest, nil
}

// Shutdown is required by the symphony's manager plugin interface
func (s *DeviceManager) Shutdown(ctx context.Context) error {
	return nil
}

// getCompositeKey generates a composite key from device ID and deployment ID.
func (s *DeviceManager) getCompositeKey(deviceClientId string, deploymentId string) string {
	return fmt.Sprintf("%s-%s", deviceClientId, deploymentId)
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

func (s *DeviceManager) SaveDeviceCapabilities(ctx context.Context, deviceClientId string, capabilities margoStdAPI.DeviceCapabilities) error {
	deviceLogger.InfofCtx(ctx, "SaveDeviceCapabilities: Saving capabilities for device '%s'", deviceClientId)

	if deviceClientId == "" {
		return fmt.Errorf("device ID is required")
	}

	// Check if device already exists
	err := s.Database.UpdateDeviceCapabilities(ctx, deviceClientId, &capabilities)
	if err != nil {
		return fmt.Errorf("failed to save device capabilities: %w", err)
	}

	deviceLogger.InfofCtx(ctx, "SaveDeviceCapabilities: Successfully saved capabilities for device '%s'", deviceClientId)
	return nil
}

func (s *DeviceManager) UpdateDeviceCapabilities(ctx context.Context, deviceClientId string, capabilities margoStdAPI.DeviceCapabilities) error {
	return s.Database.UpdateDeviceCapabilities(ctx, deviceClientId, &capabilities)
}

func (s *DeviceManager) GetDeviceCapabilities(ctx context.Context, deviceClientId string) (*margoStdAPI.DeviceCapabilities, error) {
	device, err := s.Database.GetDevice(ctx, deviceClientId)
	if err != nil {
		return nil, fmt.Errorf("failed to get device capabilities: %w", err)
	}
	return device.Capabilities, nil
}

func (s *DeviceManager) GetDeviceFromSignature(ctx context.Context, sign string) (*DeviceDatabaseRow, error) {
	return s.Database.GetDeviceUsingPubCert(ctx, sign)
}

func (s *DeviceManager) GetDeviceClientUsingId(ctx context.Context, id string) (*DeviceDatabaseRow, error) {
	return s.Database.GetDevice(ctx, id)
}
