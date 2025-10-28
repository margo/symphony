package margo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/contexts"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/states"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	margoNonStdAPI "github.com/margo/dev-repo/non-standard/generatedCode/wfm/nbi"
	"github.com/margo/dev-repo/standard/generatedCode/wfm/sbi"
	margoStdAPI "github.com/margo/dev-repo/standard/generatedCode/wfm/sbi"
)

type PublishGroupName string
type PublishFeed string

var (
	margoDbLogger                                    = logger.NewLogger("coa.runtime")
	packageManagerPublisherGroup    PublishGroupName = "package-manager"
	deploymentManagerPublisherGroup PublishGroupName = "deployment-manager"
	deviceManagerPublisherGroup     PublishGroupName = "device-manager"
	upsertPackageFeed               PublishFeed      = "upsertPackage"
	upsertDeploymentFeed            PublishFeed      = "upsertDeployment"
	upsertDeviceFeed                PublishFeed      = "upsertDevice"
	deletePackageFeed               PublishFeed      = "deletePackage"
	deleteDeploymentFeed            PublishFeed      = "deleteDeployment"
	deleteDeviceFeed                PublishFeed      = "deleteDevice"
	changeDeploymentCurrentState    PublishFeed      = "changeDeploymentCurrentState"
	changeDeploymentDesiredState    PublishFeed      = "changeDeploymentDesiredState"
)

// AppPackageDatabaseRow represents a complete application package record in the database.
// It contains the package manifest, application description, and associated resources.
type AppPackageDatabaseRow struct {
	// PackageRequest contains the complete package manifest response including metadata and status
	PackageRequest margoNonStdAPI.ApplicationPackageManifestResp

	// AppDescription contains the parsed application description from the package source
	AppDescription *margoNonStdAPI.AppDescription

	// AppResources contains all resource files extracted from the package source
	AppResources map[string][]byte
}

// DeploymentDatabaseRow represents a complete deployment record in the database.
// It tracks both the deployment request and the current/desired states for device synchronization.
type DeploymentDatabaseRow struct {
	// DeploymentRequest contains the complete deployment manifest including metadata and specifications
	// Note: device id is embedded inside the current and desired deployment objects
	DeploymentRequest margoNonStdAPI.ApplicationDeploymentManifestResp

	// CurrentDeployment represents the actual state of the deployment as reported by the device
	CurrentDeployment sbi.AppState

	// DesiredDeployment represents the target state that the deployment should achieve
	DesiredDeployment sbi.AppState

	// LastStatusUpdate tracks when this deployment record was last modified
	LastStatusUpdate time.Time
}

// DeviceDatabaseRow represents a device record in the database.
// It contains device identification, capabilities, and synchronization information.
type DeviceDatabaseRow struct {
	// DeviceClientId is the unique identifier for the device
	DeviceClientId string

	// OAuthClientId is the unique identifier for the device auth
	OAuthClientId string

	// Client secret is the information that helps the device to generate/ask for an oauth token
	OAuthClientSecret string

	// OAuth token url
	OAuthTokenURL string

	// unique signature that is bind to this device, eg TPM, certificate etc...
	DevicePubCert string

	// status of the onboarding
	OnboardingStatus margoNonStdAPI.DeviceOnboardStatus

	// Capabilities contains the device's reported capabilities and properties
	Capabilities *margoStdAPI.DeviceCapabilities

	// LastStateSync tracks when the device last synchronized its state with the orchestrator
	LastStateSync time.Time

	// entry time
	CreatedAt time.Time

	// entry time
	UpdatedAt time.Time
}

// MargoDatabase provides a centralized database interface for all Margo entities.
// It abstracts the underlying state provider and provides type-safe operations
// for application packages, deployments, and devices.
type MargoDatabase struct {
	// StateProvider is the underlying storage
	StateProvider states.IStateProvider
	// --- this actually breaks the single responsibility, but we embedded it to do pub-sub easily
	// todo the pub-sub about the database operations, we thought this would be easiest place to embed it into
	MgrContext      *contexts.ManagerContext
	PubSubGroupName string

	// Metadata definitions for different entity types used by the state provider
	// think of them as the name of the database tables
	appPkgMetadata     map[string]interface{}
	deploymentMetadata map[string]interface{}
	deviceMetadata     map[string]interface{}
}

func NewMargoDatabase(mgrCtx *contexts.ManagerContext, pubsubGroupName PublishGroupName, stateProvider states.IStateProvider) *MargoDatabase {
	return &MargoDatabase{
		StateProvider:   stateProvider,
		MgrContext:      mgrCtx,
		PubSubGroupName: string(pubsubGroupName),
		appPkgMetadata: map[string]interface{}{
			"version":      "v1",
			"producerName": model.MargoGroup,
			"resource":     "packages",
			"namespace":    "margo",
			"kind":         "ApplicationPackage",
		},
		deploymentMetadata: map[string]interface{}{
			"version":      "v1",
			"producerName": model.MargoGroup,
			"resource":     "deployments",
			"namespace":    "margo",
			"kind":         "ApplicationDeployment",
		},
		deviceMetadata: map[string]interface{}{
			"version":      "v1",
			"producerName": model.MargoGroup,
			"resource":     "devices",
			"namespace":    "margo",
			"kind":         "Device",
		},
	}
}

func (db *MargoDatabase) UpsertAppPackage(ctx context.Context, pkg AppPackageDatabaseRow) error {
	packageId := *pkg.PackageRequest.Metadata.Id
	_, err := db.StateProvider.Upsert(ctx, states.UpsertRequest{
		Options:  states.UpsertOption{},
		Metadata: db.appPkgMetadata,
		Value: states.StateEntry{
			ID:   packageId,
			Body: pkg,
		},
	})
	if err != nil {
		db.MgrContext.Logger.ErrorfCtx(ctx, "UpsertAppPackage: Failed to upsert app package '%s': %v", packageId, err)
		return fmt.Errorf("failed to upsert app package '%s': %w", packageId, err)
	}
	db.MgrContext.Logger.InfofCtx(ctx, "UpsertAppPackage: app package '%s' stored successfully", packageId)

	db.MgrContext.Publish(string(upsertPackageFeed), v1alpha2.Event{
		Metadata: map[string]string{
			"producerName": db.PubSubGroupName,
		},
		Body: pkg,
	})
	return nil
}

func (db *MargoDatabase) GetAppPackage(ctx context.Context, packageId string) (*AppPackageDatabaseRow, error) {
	entry, err := db.StateProvider.Get(ctx, states.GetRequest{
		Metadata: db.appPkgMetadata,
		ID:       packageId,
	})
	if err != nil {
		db.MgrContext.Logger.ErrorfCtx(ctx, "GetAppPackage: Failed to get app package '%s': %v", packageId, err)
		return nil, fmt.Errorf("failed to get app package '%s': %w", packageId, err)
	}

	var appPkg AppPackageDatabaseRow
	jData, _ := json.Marshal(entry.Body)
	err = json.Unmarshal(jData, &appPkg)
	if err != nil {
		db.MgrContext.Logger.ErrorfCtx(ctx, "GetAppPackage: Failed to unmarshal app package '%s': %v", packageId, err)
		return nil, fmt.Errorf("failed to unmarshal app package '%s': %w", packageId, err)
	}

	db.MgrContext.Logger.InfofCtx(ctx, "GetAppPackage: app package '%s' retrieved successfully", packageId)
	return &appPkg, nil
}

func (db *MargoDatabase) DeleteAppPackage(ctx context.Context, packageId string) error {
	existingPkg, err := db.GetAppPackage(ctx, packageId)
	if err != nil {
		return fmt.Errorf("app package doesn't exist, hence can't perform deletion")
	}

	err = db.StateProvider.Delete(ctx, states.DeleteRequest{
		Metadata: db.appPkgMetadata,
		ID:       packageId,
	})

	if err != nil {
		db.MgrContext.Logger.ErrorfCtx(ctx, "DeleteAppPackage: Failed to delete app package '%s': %v", packageId, err)
		return fmt.Errorf("failed to delete app package '%s': %w", packageId, err)
	}

	db.MgrContext.Logger.InfofCtx(ctx, "DeleteAppPackage: app package '%s' deleted successfully", packageId)

	db.MgrContext.Publish(string(deletePackageFeed), v1alpha2.Event{
		Metadata: map[string]string{
			"producerName": db.PubSubGroupName,
		},
		Body: existingPkg,
	})
	return nil
}

func (db *MargoDatabase) ListAppPackages(ctx context.Context) ([]AppPackageDatabaseRow, error) {
	var packages []AppPackageDatabaseRow
	entries, _, err := db.StateProvider.List(ctx, states.ListRequest{
		Metadata: db.appPkgMetadata,
	})
	if err != nil {
		db.MgrContext.Logger.ErrorfCtx(ctx, "ListAppPackages: Failed to list app packages: %v", err)
		return nil, fmt.Errorf("failed to list app packages: %w", err)
	}

	for _, entry := range entries {
		var appPkg AppPackageDatabaseRow
		jData, _ := json.Marshal(entry.Body)
		err = json.Unmarshal(jData, &appPkg)
		if err == nil {
			packages = append(packages, appPkg)
		} else {
			db.MgrContext.Logger.WarnfCtx(ctx, "ListAppPackages: Failed to unmarshal entry: %v", err)
		}
	}

	db.MgrContext.Logger.InfofCtx(ctx, "ListAppPackages: Listed %d app packages successfully", len(packages))
	return packages, nil
}

func (db *MargoDatabase) AppPackageExists(ctx context.Context, packageId string) (bool, error) {
	_, err := db.StateProvider.Get(ctx, states.GetRequest{
		Metadata: db.appPkgMetadata,
		ID:       packageId,
	})
	if err != nil {
		// If error is "not found", return false, nil
		// Otherwise return false with the error
		db.MgrContext.Logger.DebugfCtx(ctx, "AppPackageExists: App package '%s' check resulted in: %v", packageId, err)
		return false, nil // Assuming not found errors are expected
	}

	db.MgrContext.Logger.DebugfCtx(ctx, "AppPackageExists: App package '%s' exists", packageId)
	return true, nil
}

func (db *MargoDatabase) UpsertDeployment(ctx context.Context, deployment DeploymentDatabaseRow, publishEvent bool) error {
	deploymentId := *deployment.DeploymentRequest.Metadata.Id
	_, err := db.StateProvider.Upsert(ctx, states.UpsertRequest{
		Options:  states.UpsertOption{},
		Metadata: db.deploymentMetadata,
		Value: states.StateEntry{
			ID:   deploymentId,
			Body: deployment,
		},
	})
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "UpsertDeployment: Failed to upsert deployment '%s': %v", deploymentId, err)
		return fmt.Errorf("failed to upsert deployment '%s': %w", deploymentId, err)
	}
	deploymentLogger.InfofCtx(ctx, "UpsertDeployment: deployment '%s' stored successfully", deploymentId)

	if publishEvent {
		db.MgrContext.Publish(string(upsertDeploymentFeed), v1alpha2.Event{
			Metadata: map[string]string{
				"producerName": db.PubSubGroupName,
			},
			Body: deployment,
		})
	}
	return nil
}

func (db *MargoDatabase) GetDeployment(ctx context.Context, deploymentId string) (*DeploymentDatabaseRow, error) {
	entry, err := db.StateProvider.Get(ctx, states.GetRequest{
		Metadata: db.deploymentMetadata,
		ID:       deploymentId,
	})
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "GetDeployment: Failed to get deployment '%s': %v", deploymentId, err)
		return nil, fmt.Errorf("failed to get deployment '%s': %w", deploymentId, err)
	}

	var deployment DeploymentDatabaseRow
	jData, _ := json.Marshal(entry.Body)
	err = json.Unmarshal(jData, &deployment)
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "GetDeployment: Failed to unmarshal deployment '%s': %v", deploymentId, err)
		return nil, fmt.Errorf("failed to unmarshal deployment '%s': %w", deploymentId, err)
	}

	deploymentLogger.InfofCtx(ctx, "GetDeployment: deployment '%s' retrieved successfully", deploymentId)
	return &deployment, nil
}

func (db *MargoDatabase) DeleteDeployment(ctx context.Context, deploymentId string, publishEvent bool) error {
	existingDeployment, err := db.GetDeployment(ctx, deploymentId)
	if err != nil {
		return fmt.Errorf("deployment doesn't exist, hence can't perform deletion")
	}
	deploymentCopy := *existingDeployment

	err = db.StateProvider.Delete(ctx, states.DeleteRequest{
		Metadata: db.deploymentMetadata,
		ID:       deploymentId,
	})

	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "DeleteDeployment: Failed to delete deployment '%s': %v", deploymentId, err)
		return fmt.Errorf("failed to delete deployment '%s': %w", deploymentId, err)
	}

	deploymentLogger.InfofCtx(ctx, "DeleteDeployment: deployment '%s' deleted successfully", deploymentId)

	if publishEvent {
		db.MgrContext.Publish(string(deleteDeploymentFeed), v1alpha2.Event{
			Metadata: map[string]string{
				"producerName": db.PubSubGroupName,
			},
			Body: deploymentCopy,
		})
	}

	return nil
}

func (db *MargoDatabase) ListDeployments(ctx context.Context) ([]DeploymentDatabaseRow, error) {
	var deployments []DeploymentDatabaseRow
	entries, _, err := db.StateProvider.List(ctx, states.ListRequest{
		Metadata: db.deploymentMetadata,
	})
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "ListDeployments: Failed to list deployments: %v", err)
		return nil, fmt.Errorf("failed to list deployments: %w", err)
	}

	for _, entry := range entries {
		var deployment DeploymentDatabaseRow
		jData, _ := json.Marshal(entry.Body)
		err = json.Unmarshal(jData, &deployment)
		if err == nil {
			deployments = append(deployments, deployment)
		} else {
			deploymentLogger.WarnfCtx(ctx, "ListDeployments: Failed to unmarshal entry: %v", err)
		}
	}

	deploymentLogger.InfofCtx(ctx, "ListDeployments: Listed %d deployments successfully", len(deployments))
	return deployments, nil
}

func (db *MargoDatabase) DeploymentExists(ctx context.Context, deploymentId string) (bool, error) {
	_, err := db.StateProvider.Get(ctx, states.GetRequest{
		Metadata: db.deploymentMetadata,
		ID:       deploymentId,
	})
	if err != nil {
		// If error is "not found", return false, nil
		// Otherwise return false with the error
		deploymentLogger.DebugfCtx(ctx, "DeploymentExists: Deployment '%s' check resulted in: %v", deploymentId, err)
		return false, nil // Assuming not found errors are expected
	}

	deploymentLogger.DebugfCtx(ctx, "DeploymentExists: Deployment '%s' exists", deploymentId)
	return true, nil
}

func (db *MargoDatabase) UpdateDeploymentStatus(ctx context.Context, deploymentId string, status margoNonStdAPI.ApplicationDeploymentOperationStatus, publishEvent bool) error {
	// Get existing deployment
	deployment, err := db.GetDeployment(ctx, deploymentId)
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "UpdateDeploymentStatus: Failed to get deployment '%s': %v", deploymentId, err)
		return fmt.Errorf("failed to get deployment '%s' for status update: %w", deploymentId, err)
	}

	// Update the status
	deployment.DeploymentRequest.RecentOperation.Status = status
	now := time.Now().UTC()
	deployment.DeploymentRequest.Status.LastUpdateTime = &now
	deployment.LastStatusUpdate = now

	// Save updated deployment
	err = db.UpsertDeployment(ctx, *deployment, publishEvent)
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "UpdateDeploymentStatus: Failed to update deployment status for '%s': %v", deploymentId, err)
		return fmt.Errorf("failed to update deployment status for '%s': %w", deploymentId, err)
	}

	deploymentLogger.InfofCtx(ctx, "UpdateDeploymentStatus: deployment '%s' status updated to '%s' successfully", deploymentId, status)
	return nil
}

func (db *MargoDatabase) UpdateDeploymentOperation(ctx context.Context, deploymentId string, operation margoNonStdAPI.ApplicationDeploymentOperation, publishEvent bool) error {
	// Get existing deployment
	deployment, err := db.GetDeployment(ctx, deploymentId)
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "UpdateDeploymentOperation: Failed to get deployment '%s': %v", deploymentId, err)
		return fmt.Errorf("failed to get deployment '%s' for operation update: %w", deploymentId, err)
	}

	// Update the operation
	deployment.DeploymentRequest.RecentOperation.Op = operation
	now := time.Now().UTC()
	deployment.DeploymentRequest.Status.LastUpdateTime = &now
	deployment.LastStatusUpdate = now

	// Save updated deployment
	err = db.UpsertDeployment(ctx, *deployment, publishEvent)
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "UpdateDeploymentOperation: Failed to update deployment operation for '%s': %v", deploymentId, err)
		return fmt.Errorf("failed to update deployment operation for '%s': %w", deploymentId, err)
	}

	deploymentLogger.InfofCtx(ctx, "UpdateDeploymentOperation: deployment '%s' operation updated to '%s' successfully", deploymentId, operation)
	return nil
}

func (db *MargoDatabase) UpsertDeploymentDesiredState(ctx context.Context, deploymentId string, desired sbi.AppState, publishEvent bool) error {
	// Get existing deployment or create new one if it doesn't exist
	deployment, err := db.GetDeployment(ctx, deploymentId)
	if err != nil {
		// If deployment doesn't exist, create a new DeploymentDatabaseRow
		deploymentLogger.InfofCtx(ctx, "UpsertDeploymentDesiredState: Deployment '%s' not found, creating new entry", deploymentId)
		deployment = &DeploymentDatabaseRow{
			LastStatusUpdate: time.Now().UTC(),
		}
	}

	// Update the desired state
	deployment.DesiredDeployment = desired
	deployment.LastStatusUpdate = time.Now().UTC()

	// Save updated deployment
	err = db.UpsertDeployment(ctx, *deployment, publishEvent)
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "UpsertDeploymentDesiredState: Failed to upsert deployment desired state for '%s': %v", deploymentId, err)
		return fmt.Errorf("failed to upsert deployment desired state for '%s': %w", deploymentId, err)
	}

	deploymentLogger.InfofCtx(ctx, "UpsertDeploymentDesiredState: deployment '%s' desired state updated successfully", deploymentId)
	return nil
}

func (db *MargoDatabase) UpsertDeploymentCurrentState(ctx context.Context, deploymentId string, current sbi.AppState, publishEvent bool) error {
	// Get existing deployment or create new one if it doesn't exist
	deployment, err := db.GetDeployment(ctx, deploymentId)
	if err != nil {
		// If deployment doesn't exist, create a new DeploymentDatabaseRow
		deploymentLogger.InfofCtx(ctx, "UpsertDeploymentCurrentState: Deployment '%s' not found, creating new entry", deploymentId)
		deployment = &DeploymentDatabaseRow{
			LastStatusUpdate: time.Now().UTC(),
		}
	}

	// Update the current state
	deployment.CurrentDeployment = current
	deployment.LastStatusUpdate = time.Now().UTC()

	// Save updated deployment
	err = db.UpsertDeployment(ctx, *deployment, publishEvent)
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "UpsertDeploymentCurrentState: Failed to upsert deployment current state for '%s': %v", deploymentId, err)
		return fmt.Errorf("failed to upsert deployment current state for '%s': %w", deploymentId, err)
	}

	deploymentLogger.InfofCtx(ctx, "UpsertDeploymentCurrentState: deployment '%s' current state updated successfully", deploymentId)
	return nil
}

func (db *MargoDatabase) GetDeploymentsByDevice(ctx context.Context, deviceId string) ([]DeploymentDatabaseRow, error) {
	// Get all deployments first
	allDeployments, err := db.ListDeployments(ctx)
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "GetDeploymentsByDevice: Failed to list deployments for device filtering: %v", err)
		return nil, fmt.Errorf("failed to list deployments for device '%s': %w", deviceId, err)
	}

	var deviceDeployments []DeploymentDatabaseRow
	for _, deployment := range allDeployments {
		// Check if deployment has device reference and matches the deviceId
		if deployment.DeploymentRequest.Spec.DeviceRef != nil &&
			deployment.DeploymentRequest.Spec.DeviceRef.Id != nil &&
			*deployment.DeploymentRequest.Spec.DeviceRef.Id == deviceId {
			deviceDeployments = append(deviceDeployments, deployment)
		}
	}

	deploymentLogger.InfofCtx(ctx, "GetDeploymentsByDevice: Found %d deployments for device '%s'", len(deviceDeployments), deviceId)
	return deviceDeployments, nil
}

func (db *MargoDatabase) GetDeploymentsByPackage(ctx context.Context, packageId string) ([]DeploymentDatabaseRow, error) {
	// Get all deployments first
	allDeployments, err := db.ListDeployments(ctx)
	if err != nil {
		deploymentLogger.ErrorfCtx(ctx, "GetDeploymentsByPackage: Failed to list deployments for package filtering: %v", err)
		return nil, fmt.Errorf("failed to list deployments for package '%s': %w", packageId, err)
	}

	var packageDeployments []DeploymentDatabaseRow
	for _, deployment := range allDeployments {
		// Check if deployment references the specified package
		if deployment.DeploymentRequest.Spec.AppPackageRef.Id == packageId {
			packageDeployments = append(packageDeployments, deployment)
		}
	}

	deploymentLogger.InfofCtx(ctx, "GetDeploymentsByPackage: Found %d deployments for package '%s'", len(packageDeployments), packageId)
	return packageDeployments, nil
}

func (db *MargoDatabase) UpsertDevice(ctx context.Context, device DeviceDatabaseRow) error {
	device.UpdatedAt = time.Now().UTC()

	deviceId := device.DeviceClientId
	_, err := db.StateProvider.Upsert(ctx, states.UpsertRequest{
		Options:  states.UpsertOption{},
		Metadata: db.deviceMetadata,
		Value: states.StateEntry{
			ID:   deviceId,
			Body: device,
		},
	})
	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "UpsertDevice: Failed to upsert device '%s': %v", deviceId, err)
		return fmt.Errorf("failed to upsert device '%s': %w", deviceId, err)
	}
	deviceLogger.InfofCtx(ctx, "UpsertDevice: device '%s' stored successfully", deviceId)
	return nil
}

func (db *MargoDatabase) GetDevice(ctx context.Context, deviceId string) (*DeviceDatabaseRow, error) {
	entry, err := db.StateProvider.Get(ctx, states.GetRequest{
		Metadata: db.deviceMetadata,
		ID:       deviceId,
	})
	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "GetDevice: Failed to get device '%s': %v", deviceId, err)
		return nil, fmt.Errorf("failed to get device '%s': %w", deviceId, err)
	}

	var device DeviceDatabaseRow
	jData, _ := json.Marshal(entry.Body)
	err = json.Unmarshal(jData, &device)
	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "GetDevice: Failed to unmarshal device '%s': %v", deviceId, err)
		return nil, fmt.Errorf("failed to unmarshal device '%s': %w", deviceId, err)
	}

	deviceLogger.InfofCtx(ctx, "GetDevice: device '%s' retrieved successfully", deviceId)
	return &device, nil
}

func (db *MargoDatabase) GetDeviceUsingPubCert(ctx context.Context, cert string) (*DeviceDatabaseRow, error) {
	devices, err := db.ListDevices(ctx)
	if err != nil {
		return nil, err
	}

	for _, device := range devices {
		if device.DevicePubCert == cert {
			return &device, nil
		}
	}
	return nil, fmt.Errorf("no device found with sign: %s", cert)
}

func (db *MargoDatabase) DeleteDevice(ctx context.Context, deviceId string) error {
	err := db.StateProvider.Delete(ctx, states.DeleteRequest{
		Metadata: db.deviceMetadata,
		ID:       deviceId,
	})

	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "DeleteDevice: Failed to delete device '%s': %v", deviceId, err)
		return fmt.Errorf("failed to delete device '%s': %w", deviceId, err)
	}

	deviceLogger.InfofCtx(ctx, "DeleteDevice: device '%s' deleted successfully", deviceId)
	return nil
}

func (db *MargoDatabase) ListDevices(ctx context.Context) ([]DeviceDatabaseRow, error) {
	var devices []DeviceDatabaseRow
	entries, _, err := db.StateProvider.List(ctx, states.ListRequest{
		Metadata: db.deviceMetadata,
	})
	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "ListDevices: Failed to list devices: %v", err)
		return nil, fmt.Errorf("failed to list devices: %w", err)
	}

	for _, entry := range entries {
		var device DeviceDatabaseRow
		jData, _ := json.Marshal(entry.Body)
		err = json.Unmarshal(jData, &device)
		if err == nil {
			devices = append(devices, device)
		} else {
			deviceLogger.WarnfCtx(ctx, "ListDevices: Failed to unmarshal entry: %v", err)
		}
	}

	deviceLogger.InfofCtx(ctx, "ListDevices: Listed %d devices successfully", len(devices))
	return devices, nil
}

func (db *MargoDatabase) DeviceExists(ctx context.Context, deviceId string) (bool, error) {
	_, err := db.StateProvider.Get(ctx, states.GetRequest{
		Metadata: db.deviceMetadata,
		ID:       deviceId,
	})
	if err != nil {
		// If error is "not found", return false, nil
		// Otherwise return false with the error
		deviceLogger.DebugfCtx(ctx, "DeviceExists: Device '%s' check resulted in: %v", deviceId, err)
		return false, nil // Assuming not found errors are expected
	}

	deviceLogger.DebugfCtx(ctx, "DeviceExists: Device '%s' exists", deviceId)
	return true, nil
}

func (db *MargoDatabase) DevicePubCertExists(ctx context.Context, deviceCert string) (DeviceDatabaseRow, bool, error) {
	devices, err := db.ListDevices(ctx)
	if err != nil {
		return DeviceDatabaseRow{}, false, err
	}

	for _, device := range devices {
		if device.DevicePubCert == deviceCert {
			return device, true, nil
		}
	}
	return DeviceDatabaseRow{}, false, nil
}

func (db *MargoDatabase) UpdateDeviceCapabilities(ctx context.Context, deviceId string, capabilities *margoStdAPI.DeviceCapabilities) error {
	// Get existing device
	device, err := db.GetDevice(ctx, deviceId)
	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "UpdateDeviceCapabilities: Failed to get device '%s': %v", deviceId, err)
		return fmt.Errorf("failed to get device '%s' for capabilities update: %w", deviceId, err)
	}

	// Update the capabilities
	device.Capabilities = capabilities
	device.UpdatedAt = time.Now().UTC()

	// Save updated device
	err = db.UpsertDevice(ctx, *device)
	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "UpdateDeviceCapabilities: Failed to update device capabilities for '%s': %v", deviceId, err)
		return fmt.Errorf("failed to update device capabilities for '%s': %w", deviceId, err)
	}

	deviceLogger.InfofCtx(ctx, "UpdateDeviceCapabilities: device '%s' capabilities updated successfully", deviceId)
	return nil
}

func (db *MargoDatabase) UpdateDeviceLastSync(ctx context.Context, deviceId string, syncTime time.Time) error {
	// Get existing device
	device, err := db.GetDevice(ctx, deviceId)
	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "UpdateDeviceLastSync: Failed to get device '%s': %v", deviceId, err)
		return fmt.Errorf("failed to get device '%s' for sync time update: %w", deviceId, err)
	}

	// Update the last sync time
	device.LastStateSync = syncTime
	device.UpdatedAt = time.Now().UTC()

	// Save updated device
	err = db.UpsertDevice(ctx, *device)
	if err != nil {
		deviceLogger.ErrorfCtx(ctx, "UpdateDeviceLastSync: Failed to update device last sync time for '%s': %v", deviceId, err)
		return fmt.Errorf("failed to update device last sync time for '%s': %w", deviceId, err)
	}

	deviceLogger.InfofCtx(ctx, "UpdateDeviceLastSync: device '%s' last sync time updated to '%s' successfully", deviceId, syncTime.Format(time.RFC3339))
	return nil
}
