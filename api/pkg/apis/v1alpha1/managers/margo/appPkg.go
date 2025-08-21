package margo

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/managers/catalogs"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/managers/solutioncontainers"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/managers/solutions"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/validation"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/contexts"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	powerfulYaml "github.com/ghodss/yaml"
	"github.com/kr/pretty"
	margoNonStdAPI "github.com/margo/dev-repo/non-standard/generatedCode/wfm/nbi"
	"github.com/margo/dev-repo/non-standard/pkg/packageManager"
	margoUtils "github.com/margo/dev-repo/non-standard/pkg/utils"
	margoGitHelper "github.com/margo/dev-repo/shared-lib/git"
)

var appPkgLogger = logger.NewLogger("coa.runtime")

type ApplicationPackage struct {
	Package     margoNonStdAPI.ApplicationPackageManifestResp
	Description *margoNonStdAPI.AppDescription
	Resources   map[string][]byte
}

type AppPkgManager struct {
	managers.Manager
	Database       *MargoDatabase
	Transformer    *MargoTransformer
	needValidate   bool
	MargoValidator validation.MargoValidator
}

func (s *AppPkgManager) Init(context *contexts.VendorContext, config managers.ManagerConfig, providers map[string]providers.IProvider) error {
	appPkgLogger.Debug("Initializing AppPkgManager")

	err := s.Manager.Init(context, config, providers)
	if err != nil {
		appPkgLogger.Error("Failed to initialize base manager", "error", err)
		return err
	}

	stateprovider, err := managers.GetPersistentStateProvider(config, providers)
	if err != nil {
		appPkgLogger.Error("Failed to get persistent state provider", "error", err)
		return err
	}

	s.Database = NewMargoDatabase(s.Context, "package-manager", stateprovider)

	appPkgLogger.Debug("MargoDatabase initialized successfully")

	s.Transformer = NewMargoTransformer()
	appPkgLogger.Debug("MargoTransformer initialized successfully")

	s.needValidate = managers.NeedObjectValidate(config, providers)
	if s.needValidate {
		s.MargoValidator = validation.NewMargoValidator()
		appPkgLogger.Debug("Object validation enabled")
	} else {
		appPkgLogger.Debug("Object validation disabled")
	}

	appPkgLogger.Info("AppPkgManager initialized successfully",
		"validationEnabled", s.needValidate)
	return nil
}

// OnboardAppPkg handles the complete application package onboarding process.
func (s *AppPkgManager) OnboardAppPkg(
	ctx context.Context,
	req margoNonStdAPI.ApplicationPackageManifestRequest,
	solutionsManager *solutions.SolutionsManager,
	solutionContainerManager *solutioncontainers.SolutionContainersManager,
	catalogsManager *catalogs.CatalogsManager) (*ApplicationPackage, error) {
	startTime := time.Now()
	appPkgLogger.Info("Starting package onboarding process",
		"packageName", req.Metadata.Name,
		"sourceType", req.Spec.SourceType,
		"startTime", startTime)

	// Validate input parameters
	if req.Metadata.Name == "" {
		appPkgLogger.Error("Package onboarding validation failed",
			"error", "package name is required")
		return nil, fmt.Errorf("package name is required")
	}
	if req.Spec.SourceType == "" {
		appPkgLogger.Error("Package onboarding validation failed",
			"packageName", req.Metadata.Name,
			"error", "source type is required")
		return nil, fmt.Errorf("source type is required")
	}

	appPkgLogger.Debug("Package onboarding validation passed",
		"packageName", req.Metadata.Name,
		"sourceType", req.Spec.SourceType)

	var appPkg ApplicationPackage
	{
		by, _ := json.Marshal(&req)
		json.Unmarshal(by, &appPkg.Package)
	}

	// Generate unique identifier and set initial state
	now := time.Now().UTC()
	appPkgId := margoUtils.GenerateAppPkgId()
	operation := margoNonStdAPI.ONBOARD
	operationState := margoNonStdAPI.ApplicationPackageOperationStatusPENDING
	appPkgStatus := margoNonStdAPI.ApplicationPackageOperationStatusPENDING

	appPkgLogger.Info("Generated package metadata",
		"packageId", appPkgId,
		"packageName", appPkg.Package.Metadata.Name,
		"operation", operation,
		"initialStatus", operationState)

	appPkg.Package.Metadata.Id = &appPkgId
	appPkg.Package.RecentOperation = &margoNonStdAPI.ApplicationPackageRecentOperation{}
	appPkg.Package.RecentOperation.Op = operation
	appPkg.Package.RecentOperation.Status = operationState
	appPkg.Package.Metadata.CreationTimestamp = &now
	appPkg.Package.Status = &margoNonStdAPI.ApplicationPackageStatus{
		State:          (*margoNonStdAPI.ApplicationPackageStatusState)(&appPkgStatus),
		LastUpdateTime: &now,
	}

	appPkgLogger.Debug("Package object prepared with metadata",
		"packageId", *appPkg.Package.Metadata.Id,
		"packageName", appPkg.Package.Metadata.Name,
		"operation", appPkg.Package.RecentOperation.Op,
		"status", appPkg.Package.RecentOperation.Status)

	// Store initial package record in database
	appPkgLogger.Debug("Storing initial package record in database")
	dbRow := AppPackageDatabaseRow{
		PackageRequest: appPkg.Package,
		AppDescription: appPkg.Description,
		AppResources:   appPkg.Resources,
	}
	if err := s.Database.UpsertAppPackage(ctx, dbRow); err != nil {
		appPkgLogger.Error("Failed to store package in database",
			"packageId", *appPkg.Package.Metadata.Id,
			"error", err)
		return nil, fmt.Errorf("failed to store app pkg in database: %w", err)
	}

	// Start async processing
	appPkgLogger.Info("Starting async processing for package",
		"packageId", *appPkg.Package.Metadata.Id,
		"packageName", appPkg.Package.Metadata.Name)
	go func() {
		time.Sleep(time.Second * 8)
		s.processPackageAsync(ctx, appPkg, solutionsManager, solutionContainerManager, catalogsManager)
	}()

	onboardingDuration := time.Since(startTime)
	appPkgLogger.Info("Package onboarding initiated successfully",
		"packageId", *appPkg.Package.Metadata.Id,
		"packageName", appPkg.Package.Metadata.Name,
		"onboardingDuration", onboardingDuration)

	return &appPkg, nil
}

// processPackageAsync handles the asynchronous package processing workflow.
func (s *AppPkgManager) processPackageAsync(
	ctx context.Context,
	appPkg ApplicationPackage,
	solutionsManager *solutions.SolutionsManager,
	solutionContainerManager *solutioncontainers.SolutionContainersManager,
	catalogsManager *catalogs.CatalogsManager) {
	processStart := time.Now()
	appPkgLogger.Info("Starting async package processing",
		"packageId", *appPkg.Package.Metadata.Id,
		"packageName", appPkg.Package.Metadata.Name,
		"processStart", processStart)
	var err error
	operationContextualInfo := ""
	// Ensure final state update regardless of success or failure
	defer func(appPkg *ApplicationPackage) {
		processDuration := time.Since(processStart)
		now := time.Now().UTC()

		appPkgLogger.Debug("Finalizing package processing state",
			"packageId", *appPkg.Package.Metadata.Id,
			"processDuration", processDuration,
			"hasError", err != nil)
		if err != nil {
			appPkg.Package.RecentOperation.Status = margoNonStdAPI.ApplicationPackageOperationStatusFAILED
			status := margoNonStdAPI.ApplicationPackageStatusStateFAILED
			appPkg.Package.Status.State = &status
			appPkgLogger.Error("Package processing failed",
				"packageId", *appPkg.Package.Metadata.Id,
				"error", err,
				"processDuration", processDuration)
			operationContextualInfo = fmt.Sprintf("Processing failed: %s", err.Error())
		} else {
			appPkg.Package.RecentOperation.Status = margoNonStdAPI.ApplicationPackageOperationStatusCOMPLETED
			status := margoNonStdAPI.ApplicationPackageStatusStateONBOARDED
			appPkg.Package.Status.State = &status
			appPkgLogger.Info("Package processing completed successfully",
				"packageId", *appPkg.Package.Metadata.Id,
				"processDuration", processDuration)
			operationContextualInfo = "Package onboarded successfully"
		}
		appPkg.Package.Status.ContextualInfo = &margoNonStdAPI.ContextualInfo{
			Message: &operationContextualInfo,
		}
		appPkg.Package.Status.LastUpdateTime = &now
		dbRow := AppPackageDatabaseRow{
			PackageRequest: appPkg.Package,
			AppDescription: appPkg.Description,
			AppResources:   appPkg.Resources,
		}
		if updateErr := s.Database.UpsertAppPackage(ctx, dbRow); updateErr != nil {
			appPkgLogger.Error("Failed to update final package state",
				"packageId", *appPkg.Package.Metadata.Id,
				"error", updateErr)
		} else {
			appPkgLogger.Debug("Successfully updated final package state",
				"packageId", *appPkg.Package.Metadata.Id,
				"finalStatus", appPkg.Package.RecentOperation.Status)
		}
	}(&appPkg)

	// Initialize package manager
	appPkgLogger.Debug("Initializing package manager for processing")
	pkgMgr := packageManager.NewPackageManager()
	// Process based on source type
	appPkgLogger.Info("Processing package source",
		"packageId", *appPkg.Package.Metadata.Id,
		"sourceType", appPkg.Package.Spec.SourceType)
	switch appPkg.Package.Spec.SourceType {
	case margoNonStdAPI.GITREPO:
		tempPkg, err := s.processGitRepository(ctx, pkgMgr, appPkg, &operationContextualInfo, solutionsManager, solutionContainerManager, catalogsManager)
		if err != nil {
			return
		}
		appPkg.Description = tempPkg.Description
		appPkg.Resources = tempPkg.Resources
	default:
		err = fmt.Errorf("unsupported source type: %s", appPkg.Package.Spec.SourceType)
		operationContextualInfo = fmt.Sprintf("Unsupported source type: %s", appPkg.Package.Spec.SourceType)
		appPkgLogger.Error("Unsupported source type",
			"packageId", *appPkg.Package.Metadata.Id,
			"sourceType", appPkg.Package.Spec.SourceType)
	}
}

// processGitRepository handles Git repository source processing.
func (s *AppPkgManager) processGitRepository(
	ctx context.Context,
	pkgMgr *packageManager.PackageManager,
	pkg ApplicationPackage,
	operationContextualInfo *string,
	solutionsManager *solutions.SolutionsManager,
	solutionContainerManager *solutioncontainers.SolutionContainersManager,
	catalogsManager *catalogs.CatalogsManager) (*ApplicationPackage, error) {
	gitProcessStart := time.Now()
	appPkgLogger.Info("Starting Git repository processing", "packageId", *pkg.Package.Metadata.Id, "gitProcessStart", gitProcessStart)

	// Parse Git repository configuration
	gitRepo, err := pkg.Package.Spec.Source.AsGitRepo()
	if err != nil {
		*operationContextualInfo = fmt.Sprintf("Failed to parse Git repository spec: %s", err.Error())
		appPkgLogger.Error("Failed to parse Git repository configuration",
			"packageId", *pkg.Package.Metadata.Id,
			"error", err)
		return nil, err
	}

	appPkgLogger.Info("Git repository configuration parsed",
		"packageId", *pkg.Package.Metadata.Id,
		"gitUrl", gitRepo.Url,
		"hasAuth", gitRepo.AccessToken != nil && gitRepo.Username != nil)

	// Set up authentication
	var gitAuth *margoGitHelper.Auth
	if gitRepo.AccessToken != nil && gitRepo.Username != nil {
		gitAuth = &margoGitHelper.Auth{
			Username: *gitRepo.Username,
			Token:    *gitRepo.AccessToken,
		}
		appPkgLogger.Debug("Git authentication configured", "packageId", *pkg.Package.Metadata.Id, "username", *gitRepo.Username)
	} else {
		appPkgLogger.Debug("No Git authentication provided, using anonymous access", "packageId", *pkg.Package.Metadata.Id)
	}

	branch := "main"
	if gitRepo.Branch != nil {
		branch = *gitRepo.Branch
	}
	subPath := ""
	if gitRepo.SubPath != nil {
		subPath = *gitRepo.SubPath
	}
	// Download package from Git repository
	appPkgLogger.Info("Downloading package from Git repository",
		"packageId", *pkg.Package.Metadata.Id,
		"gitUrl", gitRepo.Url,
		"branch", branch,
		"subPath", subPath)

	pkgPath, downloadedAppPkg, err := pkgMgr.LoadPackageFromGit(
		gitRepo.Url,
		branch,
		subPath,
		gitAuth,
	)
	if err != nil {
		*operationContextualInfo = fmt.Sprintf("Failed to download package from Git: %s", err.Error())
		appPkgLogger.Error("Failed to download package from Git repository",
			"packageId", *pkg.Package.Metadata.Id,
			"gitUrl", gitRepo.Url,
			"error", err)
		return nil, err
	}

	// Ensure cleanup of downloaded package
	defer func() {
		if cleanupErr := os.RemoveAll(pkgPath); cleanupErr != nil {
			appPkgLogger.Warn("Failed to cleanup downloaded package",
				"packageId", *pkg.Package.Metadata.Id,
				"packagePath", pkgPath,
				"error", cleanupErr)
		} else {
			appPkgLogger.Debug("Successfully cleaned up downloaded package",
				"packageId", *pkg.Package.Metadata.Id,
				"packagePath", pkgPath)
		}
	}()

	downloadDuration := time.Since(gitProcessStart)
	appPkgLogger.Info("Package downloaded successfully from Git",
		"packageId", *pkg.Package.Metadata.Id,
		"packagePath", pkgPath,
		"resourceCount", len(downloadedAppPkg.Resources),
		"downloadDuration", downloadDuration)

	// Parse application description
	appPkgLogger.Info("Parsing application description from downloaded package",
		"packageId", *pkg.Package.Metadata.Id,
		"packagePath", pkgPath)

	appDesc, packageResources, err := s.parseApplicationDescription(pkgPath)
	if err != nil {
		*operationContextualInfo = fmt.Sprintf("Failed to parse application description: %s", err.Error())
		appPkgLogger.Error("Failed to parse application description",
			"packageId", *pkg.Package.Metadata.Id,
			"packagePath", pkgPath,
			"error", err)
		return nil, err
	}

	// Validate application description
	if err := s.validateApplicationDescription(appDesc); err != nil {
		*operationContextualInfo = fmt.Sprintf("Application description validation failed: %s", err.Error())
		appPkgLogger.Error("Application description validation failed",
			"packageId", *pkg.Package.Metadata.Id,
			"appId", appDesc.Metadata.Id,
			"error", err)
		return nil, err
	}

	// Merge resources from Git download and package parsing
	allResources := make(map[string][]byte)
	for k, v := range downloadedAppPkg.Resources {
		allResources[k] = v
	}
	for k, v := range packageResources {
		allResources[k] = v
	}

	appPkgLogger.Info("Application description parsed and validated successfully",
		"packageId", *pkg.Package.Metadata.Id,
		"appId", appDesc.Metadata.Id,
		"appName", appDesc.Metadata.Name,
		"appVersion", appDesc.Metadata.Version,
		"totalResourceCount", len(allResources))

	// Convert to Symphony objects
	appPkgLogger.Info("Converting application to Symphony objects",
		"packageId", *pkg.Package.Metadata.Id,
		"appId", appDesc.Metadata.Id)

	dbRow := AppPackageDatabaseRow{
		PackageRequest: pkg.Package,
		AppDescription: appDesc,
		AppResources:   allResources,
	}
	catalog, solution, solutionContainer, err := s.Transformer.AppPackageToSymphonyObjects(ctx, dbRow, allResources)
	if err != nil {
		*operationContextualInfo = fmt.Sprintf("Failed to convert to Symphony objects: %s", err.Error())
		appPkgLogger.Error("Failed to convert to Symphony objects",
			"packageId", *pkg.Package.Metadata.Id,
			"appId", appDesc.Metadata.Id,
			"error", err)
		return nil, err
	}

	appPkgLogger.Info("Successfully converted to Symphony objects",
		"packageId", *pkg.Package.Metadata.Id,
		"catalogId", catalog.ObjectMeta.Name,
		"solutionId", solution.ObjectMeta.Name,
		"containerId", solutionContainer.ObjectMeta.Name)

	// Store Symphony objects
	appPkgLogger.Info("Storing Symphony objects in state provider",
		"packageId", *pkg.Package.Metadata.Id)

	if err := s.storeSymphonyObjects(ctx, catalog, solution, solutionContainer, solutionsManager, solutionContainerManager, catalogsManager); err != nil {
		*operationContextualInfo = fmt.Sprintf("Failed to store Symphony objects: %s", err.Error())
		appPkgLogger.Error("Failed to store Symphony objects",
			"packageId", *pkg.Package.Metadata.Id,
			"error", err)
		return nil, err
	}

	totalProcessDuration := time.Since(gitProcessStart)
	appPkgLogger.Info("Git repository processing completed successfully",
		"packageId", *pkg.Package.Metadata.Id,
		"totalProcessDuration", totalProcessDuration,
		"downloadDuration", downloadDuration)

	*operationContextualInfo = "Package processed and Symphony objects created successfully"

	pkg.Description = appDesc
	pkg.Resources = allResources

	tempPkg := pkg
	appPkgLogger.Debug("Successfully prepared temporary package object for return",
		"package", pretty.Sprint(pkg))

	return &tempPkg, nil
}

// parseApplicationDescription parses the YAML application description and extracts resources
func (s *AppPkgManager) parseApplicationDescription(pkgPath string) (*margoNonStdAPI.AppDescription, map[string][]byte, error) {
	appPkgLogger.Debug("Parsing application description from package",
		"packagePath", pkgPath)

	// Look for application description YAML file
	descriptionFile := filepath.Join(pkgPath, "margo.yaml")
	if _, err := os.Stat(descriptionFile); os.IsNotExist(err) {
		// Try alternative names
		found := false
		if !found {
			appPkgLogger.Error("Application description file not found",
				"packagePath", pkgPath,
				"searchedFiles", "margo.yaml")
			return nil, nil, fmt.Errorf("application description file not found in package")
		}
	}

	appPkgLogger.Debug("Found application description file",
		"descriptionFile", descriptionFile)

	// Read and parse YAML file
	yamlData, err := os.ReadFile(descriptionFile)
	if err != nil {
		appPkgLogger.Error("Failed to read application description file",
			"descriptionFile", descriptionFile,
			"error", err)
		return nil, nil, fmt.Errorf("failed to read application description: %w", err)
	}

	jsonData, err := powerfulYaml.YAMLToJSON(yamlData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert application description yaml to json, %w", err)
	}

	var appDesc margoNonStdAPI.AppDescription
	if err := json.Unmarshal(jsonData, &appDesc); err != nil {
		appPkgLogger.Error("Failed to parse application description YAML",
			"descriptionFile", descriptionFile,
			"error", err)
		return nil, nil, fmt.Errorf("failed to parse application description YAML: %w", err)
	}

	appPkgLogger.Info("Successfully parsed application description",
		"appId", appDesc.Metadata.Id,
		"appName", appDesc.Metadata.Name,
		"appVersion", appDesc.Metadata.Version,
		"deploymentProfilesCount", len(appDesc.DeploymentProfiles))

	// Extract resource files
	resources, err := s.extractResourceFiles(pkgPath)
	if err != nil {
		appPkgLogger.Warn("Failed to extract resource files, continuing without resources",
			"packagePath", pkgPath,
			"error", err)
		resources = make(map[string][]byte)
	}

	appPkgLogger.Debug("Resource extraction completed",
		"resourceCount", len(resources),
		"resourceFiles", s.getResourceFileNames(resources))

	return &appDesc, resources, nil
}

// extractResourceFiles extracts all resource files from the package directory
func (s *AppPkgManager) extractResourceFiles(pkgPath string) (map[string][]byte, error) {
	resources := make(map[string][]byte)

	appPkgLogger.Debug("Extracting resource files from package", "packagePath", pkgPath)

	err := filepath.Walk(pkgPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and hidden files
		if info.IsDir() || strings.HasPrefix(info.Name(), ".") {
			return nil
		}

		// Get relative path from package root
		relPath, err := filepath.Rel(pkgPath, path)
		if err != nil {
			return err
		}

		// Read file content
		content, err := os.ReadFile(path)
		if err != nil {
			appPkgLogger.Warn("Failed to read resource file, skipping",
				"filePath", path,
				"error", err)
			return nil // Continue processing other files
		}

		resources[relPath] = content
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk package directory: %w", err)
	}

	return resources, nil
}

// getResourceFileNames returns a slice of resource file names for logging
func (s *AppPkgManager) getResourceFileNames(resources map[string][]byte) []string {
	names := make([]string, 0, len(resources))
	for name := range resources {
		names = append(names, name)
	}
	return names
}

// validateApplicationDescription validates the parsed application description
func (s *AppPkgManager) validateApplicationDescription(appDesc *margoNonStdAPI.AppDescription) error {
	appPkgLogger.Debug("Validating application description",
		"appId", appDesc.Metadata.Id,
		"appName", appDesc.Metadata.Name)

	// Validate required fields
	if appDesc.Metadata.Id == "" {
		return fmt.Errorf("application ID is required")
	}
	if appDesc.Metadata.Name == "" {
		return fmt.Errorf("application name is required")
	}
	if appDesc.Metadata.Version == "" {
		return fmt.Errorf("application version is required")
	}
	if len(appDesc.DeploymentProfiles) == 0 {
		return fmt.Errorf("at least one deployment profile is required")
	}

	// Validate deployment profiles
	for i, profile := range appDesc.DeploymentProfiles {
		if profile.Type == "" {
			return fmt.Errorf("deployment profile %d: type is required", i)
		}
		if len(profile.Components) == 0 {
			return fmt.Errorf("deployment profile %d: at least one component is required", i)
		}

		if profile.Type == margoNonStdAPI.AppDeploymentProfileTypeHelmV3 {
			for j, component := range profile.Components {
				helmComp, _ := component.AsHelmApplicationDeploymentProfileComponent()
				if helmComp.Name == "" {
					return fmt.Errorf("deployment profile %d, component %d: name is required", i, j)
				}
			}
		}
		if profile.Type == margoNonStdAPI.AppDeploymentProfileTypeCompose {
			for j, component := range profile.Components {
				composeComp, _ := component.AsComposeApplicationDeploymentProfileComponent()
				if composeComp.Name == "" {
					return fmt.Errorf("deployment profile %d, component %d: name is required", i, j)
				}
			}
		}
	}

	appPkgLogger.Debug("Application description validation passed",
		"appId", appDesc.Metadata.Id,
		"deploymentProfilesCount", len(appDesc.DeploymentProfiles))

	return nil
}

// storeSymphonyObjects stores the converted Symphony objects in the appropriate systems
func (s *AppPkgManager) storeSymphonyObjects(
	ctx context.Context,
	catalog *model.CatalogState,
	solution *model.SolutionState,
	container *model.SolutionContainerState,
	solutionsManager *solutions.SolutionsManager,
	solutionContainerManager *solutioncontainers.SolutionContainersManager,
	catalogsManager *catalogs.CatalogsManager) error {
	appPkgLogger.Info("Storing Symphony objects",
		"catalogId", catalog.ObjectMeta.Name,
		"solutionId", solution.ObjectMeta.Name,
		"containerId", container.ObjectMeta.Name)

	// Store Catalog
	appPkgLogger.Debug("Storing Catalog object", "catalogId", catalog.ObjectMeta.Name)
	if err := catalogsManager.UpsertState(ctx, catalog.ObjectMeta.Name, *catalog); err != nil {
		appPkgLogger.Error("Failed to store Catalog object",
			"catalogId", catalog.ObjectMeta.Name,
			"error", err)
		return fmt.Errorf("failed to store catalog: %w", err)
	}

	// Store Solution
	appPkgLogger.Debug("Storing Solution object", "solutionId", solution.ObjectMeta.Name)
	if err := solutionsManager.UpsertState(ctx, solution.ObjectMeta.Name, *solution); err != nil {
		appPkgLogger.Error("Failed to store Solution object",
			"solutionId", solution.ObjectMeta.Name,
			"error", err)
		return fmt.Errorf("failed to store solution: %w", err)
	}

	// Store SolutionContainer
	appPkgLogger.Debug("Storing SolutionContainer object", "containerId", container.ObjectMeta.Name)
	if err := solutionContainerManager.UpsertState(ctx, container.ObjectMeta.Name, *container); err != nil {
		appPkgLogger.Error("Failed to store SolutionContainer object",
			"containerId", container.ObjectMeta.Name,
			"error", err)
		return fmt.Errorf("failed to store solution container: %w", err)
	}

	appPkgLogger.Info("Successfully stored all Symphony objects",
		"catalogId", catalog.ObjectMeta.Name,
		"solutionId", solution.ObjectMeta.Name,
		"containerId", container.ObjectMeta.Name)

	return nil
}

// DeleteAppPkg handles application package deletion
func (s *AppPkgManager) DeleteAppPkg(ctx context.Context, pkgId string) error {
	deleteStart := time.Now()
	appPkgLogger.Info("Starting package deletion process",
		"packageId", pkgId,
		"deleteStart", deleteStart)

	// Validate package ID
	if pkgId == "" {
		appPkgLogger.Error("Package deletion validation failed",
			"error", "package ID is required")
		return fmt.Errorf("package ID is required")
	}

	// Check if package exists
	existingPkg, err := s.Database.GetAppPackage(ctx, pkgId)
	if err != nil {
		appPkgLogger.Error("Failed to retrieve package for deletion",
			"packageId", pkgId,
			"error", err)
		return fmt.Errorf("failed to retrieve package: %w", err)
	}

	appPkgLogger.Info("Package found for deletion",
		"packageId", pkgId,
		"packageName", existingPkg.PackageRequest.Metadata.Name,
		"currentStatus", existingPkg.PackageRequest.Status.State)

	// Delete associated Symphony objects if they exist
	if err := s.deleteSymphonyObjects(ctx, pkgId); err != nil {
		appPkgLogger.Warn("Failed to delete Symphony objects, continuing with package deletion",
			"packageId", pkgId,
			"error", err)
	}

	// Delete package from database
	if err := s.Database.DeleteAppPackage(ctx, pkgId); err != nil {
		appPkgLogger.Error("Failed to delete package from database",
			"packageId", pkgId,
			"error", err)
		return fmt.Errorf("failed to delete package from database: %w", err)
	}

	deleteDuration := time.Since(deleteStart)
	appPkgLogger.Info("Package deletion completed successfully",
		"packageId", pkgId,
		"deleteDuration", deleteDuration)

	return nil
}

// deleteSymphonyObjects deletes associated Symphony objects
func (s *AppPkgManager) deleteSymphonyObjects(ctx context.Context, pkgId string) error {
	appPkgLogger.Debug("Deleting associated Symphony objects", "packageId", pkgId)
	// TODO: pending

	appPkgLogger.Debug("Symphony objects deletion completed", "packageId", pkgId)
	return nil
}

// GetAppPkg retrieves an application package by ID
func (s *AppPkgManager) GetAppPkg(ctx context.Context, pkgId string) (*ApplicationPackage, error) {
	appPkgLogger.Debug("Retrieving application package", "packageId", pkgId)

	if pkgId == "" {
		appPkgLogger.Error("Package retrieval validation failed",
			"error", "package ID is required")
		return nil, fmt.Errorf("package ID is required")
	}

	dbRow, err := s.Database.GetAppPackage(ctx, pkgId)
	if err != nil {
		appPkgLogger.Error("Failed to retrieve package",
			"packageId", pkgId,
			"error", err)
		return nil, fmt.Errorf("failed to retrieve package: %w", err)
	}
	pkg := &ApplicationPackage{
		Package:     dbRow.PackageRequest,
		Description: dbRow.AppDescription,
		Resources:   dbRow.AppResources,
	}

	appPkgLogger.Debug("Package retrieved successfully",
		"packageId", pkgId,
		"packageName", pkg.Package.Metadata.Name,
		"status", pkg.Package.Status.State)

	return pkg, nil
}

// ListAppPkgs lists all application packages
func (s *AppPkgManager) ListAppPkgs(ctx context.Context) (*margoNonStdAPI.ApplicationPackageListResp, error) {
	appPkgLogger.Debug("Listing all application packages")

	// Get all packages from database
	dbRows, err := s.Database.ListAppPackages(ctx)
	if err != nil {
		appPkgLogger.Error("Failed to list packages from database", "error", err)
		return nil, fmt.Errorf("failed to list packages: %w", err)
	}

	var packages []margoNonStdAPI.ApplicationPackageManifestResp
	for _, dbRow := range dbRows {
		packages = append(packages, dbRow.PackageRequest)
	}

	result := &margoNonStdAPI.ApplicationPackageListResp{
		Items: packages,
	}

	appPkgLogger.Info("Package listing completed",
		"totalPackages", len(packages))

	return result, nil
}
