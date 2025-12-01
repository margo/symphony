package margo

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"
	"crypto/sha256"
	"strings"
	
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
	CurrentState            AppDeploymentState
	DesiredState            AppDeploymentState
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
		//err = s.Database.DeleteDeploymentBundle(context.Background(), event.Body.(DeploymentBundleRow).DeviceClientId, false)
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
func (s *DeviceManager) OnDeploymentStatus(ctx context.Context, deviceClientId, deploymentId string, state string) error {
    if deviceClientId == "" || deploymentId == "" || state == "" {
        return fmt.Errorf("deviceClientId, deploymentId, and status are required")
    }
    
    deviceLogger.InfofCtx(ctx, "OnDeploymentStatus: Received status update - device: %s, deployment: %s, state: %s", 
        deviceClientId, deploymentId, state)

    // Get deployment
    dbRow, err := s.Database.GetDeployment(ctx, deploymentId)
    if err != nil {
        return fmt.Errorf("deployment not found: %w", err)
    }

    // Special handling for REMOVED state
    if state == string(sbi.DeploymentStatusManifestStatusStateRemoved) {
        deviceLogger.InfofCtx(ctx, "OnDeploymentStatus: Device confirmed removal of deployment %s", deploymentId)
        
        // Delete the deployment from database
        if err := s.Database.DeleteDeployment(ctx, deploymentId, true); err != nil {
            return fmt.Errorf("failed to delete deployment from database: %w", err)
        }
        
        deviceLogger.InfofCtx(ctx, "OnDeploymentStatus: Successfully deleted deployment %s after device confirmation", deploymentId)
        
        // Note: Bundle regeneration is triggered by DeleteDeployment event subscription
        return nil
    }

    // Normal state updates (not REMOVED)
    existingState := dbRow.CurrentState
    existingState.Status.Status.State = margoStdAPI.DeploymentStatusManifestStatusState(state)
    
    if err := s.Database.UpsertDeploymentCurrentState(ctx, deploymentId, existingState, true); err != nil {
        return fmt.Errorf("failed to update current state: %w", err)
    }

    // Update the deployment request status for CLI display
    var nbiState margoNonStdAPI.ApplicationDeploymentStatusState
    switch margoStdAPI.DeploymentStatusManifestStatusState(state) {
    case margoStdAPI.DeploymentStatusManifestStatusStateInstalled:
        nbiState = margoNonStdAPI.ApplicationDeploymentStatusStateINSTALLED
    case margoStdAPI.DeploymentStatusManifestStatusStateInstalling:
        nbiState = margoNonStdAPI.ApplicationDeploymentStatusStateINSTALLING
    case margoStdAPI.DeploymentStatusManifestStatusStateFailed:
        nbiState = margoNonStdAPI.ApplicationDeploymentStatusStateFAILED
    case margoStdAPI.DeploymentStatusManifestStatusStateRemoving:
        nbiState = margoNonStdAPI.ApplicationDeploymentStatusStateREMOVING
    case margoStdAPI.DeploymentStatusManifestStatusStatePending:
        nbiState = margoNonStdAPI.ApplicationDeploymentStatusStatePENDING
    default:
        nbiState = margoNonStdAPI.ApplicationDeploymentStatusStatePENDING
    }
    
    dbRow.DeploymentRequest.Status.State = &nbiState
    now := time.Now().UTC()
    dbRow.DeploymentRequest.Status.LastUpdateTime = &now
    
    if err := s.Database.UpsertDeployment(ctx, *dbRow, true); err != nil {
        return fmt.Errorf("failed to update deployment: %w", err)
    }
    
    deviceLogger.InfofCtx(ctx, "OnDeploymentStatus: Successfully updated deployment %s to state %s", deploymentId, nbiState)
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
			"deviceId": clientID,
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

type DeviceOnboardingData struct {
	ClientId         string
	ClientSecret     string
	TokenEndpointUrl string
}

func (s *DeviceManager) ShouldReplaceBundle(ctx context.Context, deviceClientId string, clientETag *string) (bool, string, *margoStdAPI.UnsignedAppStateManifest, error) {
    deviceLogger.InfofCtx(ctx, "ShouldReplaceBundle: Processing request for device %s", deviceClientId)
    
    // Log client's ETag if provided
    if clientETag != nil && *clientETag != "" {
        deviceLogger.InfofCtx(ctx, "ShouldReplaceBundle: Client provided ETag: %s", *clientETag)
    } else {
        deviceLogger.InfofCtx(ctx, "ShouldReplaceBundle: No client ETag provided (first sync)")
    }

    // Call GetBundle to retrieve bundle information
    bundleArchivePath, bundleManifest, err := s.GetBundle(ctx, deviceClientId, nil)
    
    // Log the return values from GetBundle
    if err != nil {
        deviceLogger.InfofCtx(ctx, "ShouldReplaceBundle: GetBundle returned error: %v", err)
        deviceLogger.InfofCtx(ctx, "ShouldReplaceBundle: No bundle found for device %s, creating initial empty bundle", deviceClientId)
        
        // Create initial empty bundle (version 1) per WFM-SBI.yaml spec
        initialBundle := DeploymentBundleRow{
            DeviceClientId: deviceClientId,
            Manifest: margoStdAPI.UnsignedAppStateManifest{
                ManifestVersion: margoStdAPI.ManifestVersion(1.0),
                Bundle:          nil,
                Deployments:     []margoStdAPI.DeploymentManifestRef{},
            },
            ArchivePath: "",
        }

        // Store the initial bundle
        if createErr := s.Database.UpsertDeploymentBundle(ctx, initialBundle, false); createErr != nil {
            deviceLogger.ErrorfCtx(ctx, "ShouldReplaceBundle: Failed to create initial bundle for device %s: %v", deviceClientId, createErr)
            return false, "", nil, fmt.Errorf("failed to create initial bundle: %w", createErr)
        }

        deviceLogger.InfofCtx(ctx, "ShouldReplaceBundle: Created initial empty bundle (version 1) for device %s", deviceClientId)
        
        // Return the initial bundle (client needs to fetch it)
        return true, "", &initialBundle.Manifest, nil
																											  
    }
    
    // Validate manifest
    if bundleManifest == nil {
        deviceLogger.ErrorfCtx(ctx, "ShouldReplaceBundle: GetBundle returned nil manifest for device %s", deviceClientId)
        return false, "", nil, fmt.Errorf("bundle manifest is nil for device %s", deviceClientId)
																									
    }

    bundleVersionInt := uint64(bundleManifest.ManifestVersion)
																		   
    deviceLogger.InfofCtx(ctx, "ShouldReplaceBundle: Retrieved bundle - Version: %d, Deployments: %d, Bundle nil: %t", 
        bundleVersionInt, len(bundleManifest.Deployments), bundleManifest.Bundle == nil)

    // SPEC-COMPLIANT: Compute server-side ETag
    var serverETag string

    if bundleManifest.Bundle == nil {
        // Empty bundle: Compute SHA-256 digest of the manifest JSON (per spec)
        manifestJSON, err := json.Marshal(bundleManifest)
        if err != nil {
            deviceLogger.ErrorfCtx(ctx, "ShouldReplaceBundle: Failed to marshal manifest for digest: %v", err)
            return false, "", nil, fmt.Errorf("failed to compute manifest digest: %w", err)
        }
        
        hash := sha256.Sum256(manifestJSON)
        serverETag = fmt.Sprintf("\"sha256:%x\"", hash)

        deviceLogger.InfofCtx(ctx, "ShouldReplaceBundle: Computed ETag for empty bundle: %s", serverETag)
    } else {
        // Bundle with deployments: Use bundle digest as ETag
        if bundleManifest.Bundle.Digest == nil {
            deviceLogger.ErrorfCtx(ctx, "ShouldReplaceBundle: Bundle digest is nil for device %s", deviceClientId)
            return false, "", nil, fmt.Errorf("bundle digest is nil for device %s", deviceClientId)
        }
        
        serverETag = fmt.Sprintf("\"%s\"", *bundleManifest.Bundle.Digest)
												
        deviceLogger.InfofCtx(ctx, "ShouldReplaceBundle: Using bundle digest as ETag: %s", serverETag)
 
    }

    // Compare client ETag with server ETag
    if clientETag != nil && *clientETag != "" {
        // Normalize ETags for comparison (remove quotes)
        clientETagClean := strings.Trim(*clientETag, "\"")
        serverETagClean := strings.Trim(serverETag, "\"")
        
        if clientETagClean == serverETagClean {
            deviceLogger.InfofCtx(ctx, "ShouldReplaceBundle: ✅ ETags match - Client: %s, Server: %s", *clientETag, serverETag)
										 
            deviceLogger.InfofCtx(ctx, "ShouldReplaceBundle: RETURN - shouldReplace=false (304 Not Modified), version=%d, deployments=%d", 
                bundleVersionInt, len(bundleManifest.Deployments))
            return false, bundleArchivePath, bundleManifest, nil
        }
        
										   
																					  
										   
        deviceLogger.InfofCtx(ctx, "ShouldReplaceBundle: ❌ ETags differ - Client: %s, Server: %s", *clientETag, serverETag)
																																							   
													 
			 
    }

    // ETags don't match or client didn't send one - return new manifest
    deviceLogger.InfofCtx(ctx, "ShouldReplaceBundle: Returning new/updated manifest - ETag: %s, Version: %d, Deployments: %d", 
        serverETag, bundleVersionInt, len(bundleManifest.Deployments))
    deviceLogger.InfofCtx(ctx, "ShouldReplaceBundle: RETURN - shouldReplace=true, archivePath='%s', version=%d, deployments=%d", 
        bundleArchivePath, bundleVersionInt, len(bundleManifest.Deployments))
									 
    return true, bundleArchivePath, bundleManifest, nil
}


func (s *DeviceManager) GetBundle(ctx context.Context, deviceClientId string, digest *string) (bundleArchivePath string, bundleManifest *margoStdAPI.UnsignedAppStateManifest, err error) {
    bundle, err := s.Database.GetDeploymentBundle(ctx, deviceClientId)
    if err != nil {
        return "", nil, err
    }

    // Check if bundle is nil
    if bundle == nil {
        return "", nil, fmt.Errorf("bundle is nil for device %s", deviceClientId)
    }
    
    // SPEC: Empty bundles (no deployments) have Bundle = nil
    // This is VALID and expected
    if len(bundle.Manifest.Deployments) == 0 {
        if bundle.Manifest.Bundle != nil {
            deviceLogger.WarnfCtx(ctx, "GetBundle: SPEC VIOLATION - Empty deployments but Bundle is not null for device %s", deviceClientId)
        }
        // Return empty bundle (Bundle field is nil, which is correct)
        deviceLogger.InfofCtx(ctx, "GetBundle: Returning empty bundle for device %s, version: %d", 
            deviceClientId, uint64(bundle.Manifest.ManifestVersion))
        return bundle.ArchivePath, &bundle.Manifest, nil
    }
    
    // SPEC: Bundles with deployments MUST have Bundle field populated
    if bundle.Manifest.Bundle == nil {
        deviceLogger.ErrorfCtx(ctx, "GetBundle: SPEC VIOLATION - Has deployments but Bundle is nil for device %s", deviceClientId)
        return "", nil, fmt.Errorf("invalid bundle structure: has deployments but Bundle field is nil for device %s", deviceClientId)
    }
    
    if bundle.Manifest.Bundle.Digest == nil {
        deviceLogger.ErrorfCtx(ctx, "GetBundle: Bundle digest is nil for device %s", deviceClientId)
        return "", nil, fmt.Errorf("bundle digest is nil for device %s", deviceClientId)
    }

    // Check digest match if provided
    if digest != nil && *bundle.Manifest.Bundle.Digest != *digest {
        deviceLogger.InfofCtx(ctx, "GetBundle: Digest mismatch for device %s - requested: %s, found: %s", 
            deviceClientId, *digest, *bundle.Manifest.Bundle.Digest)
        return "", nil, nil
    }

    bundleVersionInt := uint64(bundle.Manifest.ManifestVersion)
    deviceLogger.InfofCtx(ctx, "GetBundle: Found bundle with digest %s, version: %d, deployments: %d, sizeBytes: %d", 
        *bundle.Manifest.Bundle.Digest, bundleVersionInt, len(bundle.Manifest.Deployments), *bundle.Manifest.Bundle.SizeBytes)
    return bundle.ArchivePath, &bundle.Manifest, nil
}



// Shutdown is required by the symphony's manager plugin interface
func (s *DeviceManager) Shutdown(ctx context.Context) error {
	return nil
}


// ConvertNBIAppDeploymentToSBIAppDeployment converts AppDeployment to AppState.
func ConvertNBIAppDeploymentToSBIAppDeployment(appDeployment *margoNonStdAPI.ApplicationDeploymentManifestResp) (*AppDeploymentState, error) {
	var appDeploymentOnSBI AppDeploymentState
	{
		by, err := json.Marshal(appDeployment)
		if err != nil {
			return nil, err
		}
		json.Unmarshal(by, &appDeploymentOnSBI)
	}

	return &appDeploymentOnSBI, nil
}

func (s *DeviceManager) SaveDeviceCapabilities(ctx context.Context, deviceClientId string, capabilities margoStdAPI.DeviceCapabilitiesManifest) error {
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

func (s *DeviceManager) UpdateDeviceCapabilities(ctx context.Context, deviceClientId string, capabilities margoStdAPI.DeviceCapabilitiesManifest) error {
	return s.Database.UpdateDeviceCapabilities(ctx, deviceClientId, &capabilities)
}

func (s *DeviceManager) GetDeviceCapabilities(ctx context.Context, deviceClientId string) (*margoStdAPI.DeviceCapabilitiesManifest, error) {
	device, err := s.Database.GetDevice(ctx, deviceClientId)
	if err != nil {
		return nil, fmt.Errorf("failed to get device capabilities: %w", err)
	}
	return device.Capabilities, nil
}

func (s *DeviceManager) GetDeviceFromSignature(ctx context.Context, sign string) (*DeviceDatabaseRow, error) {
	return s.Database.GetDeviceUsingPubCert(ctx, sign)
}

func (s *DeviceManager) GetDeviceClientUsingId(ctx context.Context, clientId string) (*DeviceDatabaseRow, error) {
    device, err := s.Database.GetDevice(ctx, clientId)
    if err != nil {
        deviceLogger.ErrorfCtx(ctx, "GetDeviceClientUsingId: Failed to get device %s: %v", clientId, err)
        return nil, err
    }
    
    if device == nil {
        deviceLogger.ErrorfCtx(ctx, "GetDeviceClientUsingId: Device %s not found", clientId)
        return nil, fmt.Errorf("device %s not found", clientId)
    }
    
    return device, nil
}

