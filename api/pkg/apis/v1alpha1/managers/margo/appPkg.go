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
	"github.com/margo/dev-repo/non-standard/pkg/packageManager"
	margoUtils "github.com/margo/dev-repo/non-standard/pkg/utils"
	margoGitHelper "github.com/margo/dev-repo/shared-lib/git"
)

var (
	appPkgLogger    = logger.NewLogger("coa.runtime")
	appPkgNamespace = "margo"
	appPkgResource  = "app-pkg"
	appPkgKind      = "ApplicationPackage"
	appPkgMetadata  = map[string]interface{}{
		"version":   "v1",
		"group":     model.MargoGroup,
		"resource":  appPkgResource,
		"namespace": appPkgNamespace,
		"kind":      appPkgKind,
	}
)

type AppPkgManager struct {
	managers.Manager
	StateProvider  states.IStateProvider
	needValidate   bool
	MargoValidator validation.MargoValidator
}

func (s *AppPkgManager) Init(context *contexts.VendorContext, config managers.ManagerConfig, providers map[string]providers.IProvider) error {
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
		Handler: func(topic string, event v1alpha2.Event) error {
			switch topic {
			case "NewDeployment":
				// event.Body
			case "UpdateDeployment":
			case "DeleteDeployment":
			}
			return nil
		},
		Group: "job",
	})

	context.Subscribe("onNewDeployment", v1alpha2.EventHandler{
		Handler: func(topic string, event v1alpha2.Event) error {
			switch topic {
			case "NewDeployment":
				// event.Body
			case "UpdateDeployment":
			case "DeleteDeployment":
			}
			return nil
		},
		Group: "job",
	})

	return nil
}

func (s *AppPkgManager) storePkgInDB(context context.Context, id string, pkg margoNonStdAPI.ApplicationPackageResp) error {
	_, err := s.StateProvider.Upsert(context, states.UpsertRequest{
		Options:  states.UpsertOption{},
		Metadata: appPkgMetadata,
		Value: states.StateEntry{
			ID:   id,
			Body: pkg,
		},
	})
	return err
}

func (s *AppPkgManager) updatePkgInDB(context context.Context, id string, pkg margoNonStdAPI.ApplicationPackageResp) error {
	_, err := s.StateProvider.Upsert(context, states.UpsertRequest{
		Options:  states.UpsertOption{},
		Metadata: appPkgMetadata,
		Value: states.StateEntry{
			ID:   id,
			Body: pkg,
		},
	})
	return err
}

func (s *AppPkgManager) deletePkgFromDB(context context.Context, pkgId string) error {
	return s.StateProvider.Delete(context, states.DeleteRequest{
		Metadata: appPkgMetadata,
		ID:       pkgId,
	})
}

func (s *AppPkgManager) listPkgFromDB(context context.Context, pkg margoNonStdAPI.ApplicationPackageResp) error {
	return nil
}

func (s *AppPkgManager) getPkgFromDB(context context.Context, pkgId string) (*margoNonStdAPI.ApplicationPackageResp, error) {
	entry, err := s.StateProvider.Get(context, states.GetRequest{
		Metadata: appPkgMetadata,
		ID:       pkgId,
	})
	if err != nil {
		return nil, err
	}

	var appPkg margoNonStdAPI.ApplicationPackageResp
	jData, _ := json.Marshal(entry.Body)
	_ = json.Unmarshal(jData, &appPkg)

	return &appPkg, nil
}

// OnboardAppPkg handles the complete application package onboarding process.
func (s *AppPkgManager) OnboardAppPkg(ctx context.Context, req margoNonStdAPI.ApplicationPackageRequest) (*margoNonStdAPI.ApplicationPackageResp, error) {
	appPkgLogger.InfofCtx(ctx, "OnboardAppPkg: Starting onboarding process for package '%s' with source type '%s'", req.Metadata.Name, req.Spec.SourceType)

	// Validate input parameters
	if req.Metadata.Name == "" {
		appPkgLogger.ErrorfCtx(ctx, "OnboardAppPkg: Package name is required but was empty")
		return nil, fmt.Errorf("package name is required")
	}
	if req.Spec.SourceType == "" {
		appPkgLogger.ErrorfCtx(ctx, "OnboardAppPkg: Source type is required but was empty")
		return nil, fmt.Errorf("source type is required")
	}

	var appPkg margoNonStdAPI.ApplicationPackageResp
	{
		by, _ := json.Marshal(&req)
		json.Unmarshal(by, &appPkg)
	}

	// Generate unique identifier for the onboard AppPkg to be onboarded
	now := time.Now().UTC()
	appPkgId := margoUtils.GenerateAppPkgId()
	operation := margoNonStdAPI.ONBOARD
	operationState := margoNonStdAPI.ApplicationPackageOperationStatusPENDING
	appPkgStatus := margoNonStdAPI.ApplicationPackageOperationStatusPENDING

	appPkgLogger.InfofCtx(ctx, "OnboardAppPkg: Generated package ID '%s' for package '%s'", appPkgId, appPkg.Metadata.Name)
	appPkgLogger.DebugfCtx(ctx, "OnboardAppPkg: Initial operation state set to '%s'", operationState)

	appPkg.Metadata.Id = &appPkgId
	appPkg.RecentOperation.Op = operation
	appPkg.RecentOperation.Status = operationState
	appPkg.Metadata.CreationTimestamp = &now
	appPkg.Status = &margoNonStdAPI.ApplicationPackageStatus{
		State: (*margoNonStdAPI.ApplicationPackageStatusState)(&appPkgStatus),
		// ContextualInfo: "onboarding the app",
		LastUpdateTime: &now,
	}

	appPkgLogger.DebugfCtx(ctx, "OnboardAppPkg: Package object prepared with ID '%s', Name '%s', Operation '%s', State '%s'",
		appPkg.Metadata.Id, appPkg.Metadata.Name, appPkg.RecentOperation.Op, appPkg.RecentOperation.Status)

	// Store initial package record in database
	appPkgLogger.InfofCtx(ctx, "OnboardAppPkg: Storing initial package record in database")
	if err := s.storePkgInDB(ctx, *appPkg.Metadata.Id, appPkg); err != nil {
		appPkgLogger.ErrorfCtx(ctx, "OnboardAppPkg: Failed to store package in database: %v", err)
		return nil, fmt.Errorf("failed to store app pkg in database: %w", err)
	}
	appPkgLogger.InfofCtx(ctx, "OnboardAppPkg: Successfully stored initial package record with ID '%s'", appPkg.Metadata.Id)

	// Start async processing
	appPkgLogger.InfofCtx(ctx, "OnboardAppPkg: Starting async processing for package '%s'", appPkg.Metadata.Id)
	go func() {
		time.Sleep(time.Second * 8)
		s.processPackageAsync(ctx, appPkg)
	}()

	// Create and return response
	appPkgLogger.InfofCtx(ctx, "OnboardAppPkg: Successfully initiated onboarding process for package '%s' with ID '%s'",
		appPkg.Metadata.Name, appPkg.Metadata.Id)

	return &appPkg, nil
}

// processPackageAsync handles the asynchronous package processing workflow.
//
// This function runs in a separate goroutine and handles the actual package download,
// processing, and final state updates. It ensures proper error handling and state
// tracking throughout the async workflow.
//
// Parameters:
//   - ctx: The context from the original request (for logging and tracing)
//   - appPkg: The initial package record to be processed
//   - spec: The original onboarding specification
func (s *AppPkgManager) processPackageAsync(ctx context.Context, appPkg margoNonStdAPI.ApplicationPackageResp) {
	appPkgLogger.InfofCtx(ctx, "processPackageAsync: Starting async processing for package '%s'", appPkg.Metadata.Id)

	var err error
	operationContextualInfo := ""

	// Ensure final state update regardless of success or failure
	defer func() {
		appPkgLogger.DebugfCtx(ctx, "processPackageAsync: Finalizing package state for ID '%s'", appPkg.Metadata.Id)

		appPkg.Status.ContextualInfo = &margoNonStdAPI.ContextualInfo{
			Message: &operationContextualInfo,
			Code:    nil,
		}

		if err != nil {
			appPkg.RecentOperation.Status = margoNonStdAPI.ApplicationPackageOperationStatusFAILED
			appPkgLogger.WarnfCtx(ctx, "processPackageAsync: Setting package state to Failed due to error: %v", err)
		} else {
			appPkg.RecentOperation.Status = margoNonStdAPI.ApplicationPackageOperationStatusCOMPLETED
			appPkgLogger.InfofCtx(ctx, "processPackageAsync: Setting package state to Completed")
		}

		now := time.Now().UTC()
		appPkg.Status.LastUpdateTime = &now
		appPkgLogger.DebugfCtx(ctx, "processPackageAsync: Updating package final state to '%s' in database", appPkg.RecentOperation.Status)

		if updateErr := s.updatePkgInDB(ctx, *appPkg.Metadata.Id, appPkg); updateErr != nil {
			appPkgLogger.ErrorfCtx(ctx, "processPackageAsync: Error occurred while updating package state in database: %v", updateErr)
		} else {
			appPkgLogger.InfofCtx(ctx, "processPackageAsync: Successfully updated final package state to '%s'", appPkg.RecentOperation.Status)
		}
	}()

	// Initialize package manager
	appPkgLogger.InfofCtx(ctx, "processPackageAsync: Initializing package manager")
	pkgMgr := packageManager.NewPackageManager()

	// Process based on source type
	appPkgLogger.InfofCtx(ctx, "processPackageAsync: Processing source type '%s'", appPkg.Spec.SourceType)
	switch appPkg.Spec.SourceType {
	case margoNonStdAPI.GITREPO:
		err = s.processGitRepository(ctx, pkgMgr, appPkg, &operationContextualInfo)
	default:
		err = fmt.Errorf("unsupported source type: %s", appPkg.Spec.SourceType)
		operationContextualInfo = err.Error()
		appPkgLogger.ErrorfCtx(ctx, "processPackageAsync: %s", err.Error())
	}

	if err == nil {
		appPkgLogger.InfofCtx(ctx, "processPackageAsync: Successfully completed async processing for package '%s'", appPkg.Metadata.Id)
	} else {
		appPkgLogger.ErrorfCtx(ctx, "processPackageAsync: Failed async processing for package '%s': %v", appPkg.Metadata.Id, err)
	}
}

// processGitRepository handles Git repository source processing.
//
// This function manages the Git repository download process including authentication,
// reference resolution, and package extraction.
//
// Parameters:
//   - ctx: The request context for logging
//   - pkgMgr: The package manager instance
//   - spec: The onboarding specification
//   - operationContextualInfo: Pointer to store contextual error information
//
// Returns:
//   - error: An error if the Git processing fails
func (s *AppPkgManager) processGitRepository(ctx context.Context, pkgMgr *packageManager.PackageManager, spec margoNonStdAPI.ApplicationPackageResp, operationContextualInfo *string) error {
	appPkgLogger.InfofCtx(ctx, "processGitRepository: Processing Git repository source")

	// Parse Git repository configuration
	gitRepo, err := spec.Spec.Source.AsGitRepo()
	if err != nil {
		*operationContextualInfo = fmt.Sprintf("Failed to parse the git repo spec: %s", err.Error())
		appPkgLogger.ErrorfCtx(ctx, "processGitRepository: Failed to parse Git repository spec: %v", err)
		return err
	}

	appPkgLogger.InfofCtx(ctx, "processGitRepository: Git repository URL: %s", gitRepo.Url)
	{
		jsonBytes, _ := gitRepo.MarshalJSON()
		appPkgLogger.DebugfCtx(ctx, "processGitRepository: Git repository configuration: %s", string(jsonBytes))
	}

	// Set up authentication
	var gitAuth *margoGitHelper.Auth
	if gitRepo.AccessToken != nil && gitRepo.Username != nil {
		gitAuth = &margoGitHelper.Auth{
			Username: *gitRepo.Username,
			Token:    *gitRepo.AccessToken,
		}
		appPkgLogger.InfofCtx(ctx, "processGitRepository: Using Git authentication for user '%s'", *gitRepo.Username)
	} else {
		appPkgLogger.InfofCtx(ctx, "processGitRepository: Using anonymous Git access (no credentials provided)")
	}

	// Determine Git reference
	gitRefName := ""
	if gitRepo.Branch != nil {
		gitRefName = *gitRepo.Branch
		appPkgLogger.InfofCtx(ctx, "processGitRepository: Using Git branch '%s'", gitRefName)
	} else if gitRepo.Tag != nil {
		gitRefName = *gitRepo.Tag
		appPkgLogger.InfofCtx(ctx, "processGitRepository: Using Git tag '%s'", gitRefName)
	} else {
		appPkgLogger.WarnfCtx(ctx, "processGitRepository: No branch or tag specified, using empty reference")
	}

	// Determine subpath
	subPath := ""
	if gitRepo.SubPath != nil {
		subPath = *gitRepo.SubPath
		appPkgLogger.InfofCtx(ctx, "processGitRepository: Using Git subpath '%s'", subPath)
	} else {
		appPkgLogger.DebugfCtx(ctx, "processGitRepository: No subpath specified, using repository root")
	}

	// Download package from Git repository
	appPkgLogger.InfofCtx(ctx, "processGitRepository: Starting package download from Git repository")
	appPkgLogger.DebugfCtx(ctx, "processGitRepository: Download parameters - URL: %s, Ref: %s, SubPath: %s",
		gitRepo.Url, gitRefName, subPath)

	pkgPath, _, err := pkgMgr.LoadPackageFromGit(
		gitRepo.Url,
		gitRefName,
		subPath,
		gitAuth,
	)
	if err != nil {
		*operationContextualInfo = fmt.Sprintf("Failed to download the package from git repo: %s", err.Error())
		appPkgLogger.ErrorfCtx(ctx, "processGitRepository: Failed to load package from Git repository: %v", err)
		return err
	}

	appPkgLogger.InfofCtx(ctx, "processGitRepository: Successfully downloaded package to directory: %s", pkgPath)

	// TODO: Add cleanup for pkgPath when processing is complete
	// TODO: Add package validation and processing logic here

	return nil
}

func (s *AppPkgManager) ListAppPkgs(context context.Context) (*margoNonStdAPI.ApplicationPackageListResp, error) {
	var appPkgs []margoNonStdAPI.ApplicationPackageResp
	entries, _, err := s.StateProvider.List(context, states.ListRequest{
		Metadata: appPkgMetadata,
	})
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		var appPkg margoNonStdAPI.ApplicationPackageResp
		jData, _ := json.Marshal(entry.Body)
		err = json.Unmarshal(jData, &appPkg)
		if err == nil {
			appPkgs = append(appPkgs, appPkg)
		}
	}

	toContinue := false
	resp := margoNonStdAPI.ApplicationPackageListResp{
		ApiVersion: "margo.org",
		Kind:       "ApplicationPackageList",
		Items:      make([]margoNonStdAPI.ApplicationPackageResp, len(appPkgs)),
		Metadata: &margoNonStdAPI.PaginationMetadata{
			Continue:           &toContinue,
			RemainingItemCount: nil,
		},
	}
	for _, pkg := range appPkgs {
		var summary margoNonStdAPI.ApplicationPackageResp
		{
			bytes, _ := json.Marshal(pkg)
			_ = json.Unmarshal(bytes, &summary)
		}
		resp.Items = append(resp.Items, summary)
	}

	return &resp, nil
}

func (s *AppPkgManager) GetAppPkg(context context.Context, pkgId string) (*margoNonStdAPI.ApplicationPackageResp, error) {
	entry, err := s.StateProvider.Get(context, states.GetRequest{
		Metadata: appPkgMetadata,
		ID:       pkgId,
	})
	if err != nil {
		return nil, err
	}

	var appPkg margoNonStdAPI.ApplicationPackageResp
	jData, _ := json.Marshal(entry.Body)
	_ = json.Unmarshal(jData, &appPkg)

	return &appPkg, nil
}

// DeleteAppPkg initiates the deletion process for an application package.
//
// This method starts an asynchronous deletion process that includes deboarding the package
// and removing it from the system. The operation is performed in the background to simulate
// real-world scenarios where package deletion can take time.
//
// Parameters:
//   - ctx: The request context for database operations and logging
//   - pkgId: The unique identifier of the package to delete
//
// Returns:
//   - *margoNonStdAPI.DeleteAppPkgResp: The deletion response with operation status
//   - error: An error if the deletion cannot be initiated
func (s *AppPkgManager) DeleteAppPkg(ctx context.Context, pkgId string) (*margoNonStdAPI.ApplicationPackageResp, error) {
	appPkgLogger.InfofCtx(ctx, "DeleteAppPkg: Starting deletion process for package ID '%s'", pkgId)

	// Validate input parameter
	if pkgId == "" {
		appPkgLogger.ErrorfCtx(ctx, "DeleteAppPkg: Package ID is required but was empty")
		return nil, fmt.Errorf("package ID is required")
	}

	// Retrieve package from database to verify existence and get current state
	appPkgLogger.DebugfCtx(ctx, "DeleteAppPkg: Retrieving package from database")
	appPkg, err := s.getPkgFromDB(ctx, pkgId)
	if err != nil {
		appPkgLogger.ErrorfCtx(ctx, "DeleteAppPkg: Failed to retrieve package from database: %v", err)
		return nil, fmt.Errorf("failed to check the latest state of the app pkg: %w", err)
	}

	if appPkg == nil {
		appPkgLogger.WarnfCtx(ctx, "DeleteAppPkg: Package with ID '%s' does not exist", pkgId)
		return nil, fmt.Errorf("package with id %s does not exist", pkgId)
	}

	appPkgLogger.InfofCtx(ctx, "DeleteAppPkg: Found package '%s' with current operation '%s' and state '%s'",
		appPkg.Metadata.Name, appPkg.RecentOperation.Op, appPkg.RecentOperation.Status)

	// Update package state to indicate deletion is starting
	appPkgLogger.InfofCtx(ctx, "DeleteAppPkg: Initiating deboarding operation for package '%s'", appPkg.Metadata.Name)
	now := time.Now().UTC()
	appPkg.RecentOperation.Op = margoNonStdAPI.DEBOARD
	appPkg.RecentOperation.Status = margoNonStdAPI.ApplicationPackageOperationStatusPENDING
	appPkg.Status.LastUpdateTime = &now

	appPkgLogger.DebugfCtx(ctx, "DeleteAppPkg: Updating package state to Operation='%s', State='%s'",
		appPkg.RecentOperation.Op, appPkg.RecentOperation.Status)

	if err := s.updatePkgInDB(ctx, pkgId, *appPkg); err != nil {
		appPkgLogger.ErrorfCtx(ctx, "DeleteAppPkg: Failed to update package state in database: %v", err)
		return nil, fmt.Errorf("failed to change the app pkg operation state before triggering deletion: %w", err)
	}

	appPkgLogger.InfofCtx(ctx, "DeleteAppPkg: Successfully updated package state to pending deletion")

	// Start asynchronous deletion process
	appPkgLogger.InfofCtx(ctx, "DeleteAppPkg: Starting background deletion process for package '%s'", pkgId)
	go s.performAsyncDeletion(ctx, pkgId, appPkg)

	// Create and return response
	appPkgLogger.InfofCtx(ctx, "DeleteAppPkg: Successfully initiated deletion process for package '%s'", pkgId)
	return appPkg, nil
}

// performAsyncDeletion handles the asynchronous deletion process in the background.
func (s *AppPkgManager) performAsyncDeletion(pCtx context.Context, pkgId string, appPkg *margoNonStdAPI.ApplicationPackageResp) {
	appPkgLogger.InfofCtx(pCtx, "performAsyncDeletion: Starting background deletion for package '%s'", pkgId)

	// Phase 1: Simulate deboarding process
	appPkgLogger.InfofCtx(pCtx, "performAsyncDeletion: Phase 1 - Deboarding package '%s' (simulated delay)", appPkg.Metadata.Name)
	// TODO: this is done deliberately to simulate a real-life scenario where deboarding a package can take some time
	// this has no practical use case but done for PoC demo, unless a concrete implementation is completed here
	time.Sleep(time.Second * 8)

	// Update package state to completed deboarding
	appPkgLogger.InfofCtx(pCtx, "performAsyncDeletion: Phase 1 completed - Updating package state to completed")
	now := time.Now().UTC()
	appPkg.RecentOperation.Status = margoNonStdAPI.ApplicationPackageOperationStatusCOMPLETED
	appPkg.Status.LastUpdateTime = &now

	if err := s.updatePkgInDB(pCtx, pkgId, *appPkg); err != nil {
		appPkgLogger.ErrorfCtx(pCtx, "performAsyncDeletion: Failed to update package state to completed: %v", err)
		// Continue with deletion even if state update fails
	} else {
		appPkgLogger.InfofCtx(pCtx, "performAsyncDeletion: Successfully updated package state to completed")
	}

	// Phase 2: Final cleanup and removal from state provider
	appPkgLogger.InfofCtx(pCtx, "performAsyncDeletion: Phase 2 - Final cleanup for package '%s' (simulated delay)", pkgId)
	// Additional delay to simulate cleanup operations
	time.Sleep(time.Second * 5)

	// Force delete from state provider
	appPkgLogger.InfofCtx(pCtx, "performAsyncDeletion: Removing package '%s' from state provider", pkgId)
	err := s.deletePkgFromDB(pCtx, pkgId)

	if err != nil {
		appPkgLogger.ErrorfCtx(pCtx, "performAsyncDeletion: Failed to delete package '%s' from state provider: %v", pkgId, err)
		// TODO: Consider implementing retry logic or dead letter queue for failed deletions
	} else {
		appPkgLogger.InfofCtx(pCtx, "performAsyncDeletion: Successfully removed package '%s' from state provider", pkgId)
	}

	appPkgLogger.InfofCtx(pCtx, "performAsyncDeletion: Completed background deletion process for package '%s'", pkgId)
}

// TODO: move this to some suitable package
func (s *AppPkgManager) ConvertToSolution(context context.Context, appId string, spec margoNonStdAPI.ApplicationPackageResp) (model.SolutionState, error) {
	return model.SolutionState{}, nil
}

func (s *AppPkgManager) WatchAppChanges(context context.Context) error {
	return nil
}

// Shutdown is required by the symphony's manager plugin interface
func (s *AppPkgManager) Shutdown(ctx context.Context) error {
	return nil
}
