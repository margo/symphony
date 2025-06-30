package margo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/validation"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/contexts"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/states"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	margoAPIModels "github.com/margo/dev-repo/sdk/api/wfm/northbound/models"
	"github.com/margo/dev-repo/sdk/pkg/packageManager"
	margoUtils "github.com/margo/dev-repo/sdk/utils"
)

var margoLog = logger.NewLogger("coa.runtime")
var namespace = "margo"
var appPkgResource = "app-pkg"
var appPkgKind = "ApplicationPackage"

var appPkgMetadata = map[string]interface{}{
	"version":   "v1",
	"group":     model.MargoGroup,
	"resource":  appPkgResource,
	"namespace": namespace,
	"kind":      appPkgKind,
}

type MargoManager struct {
	managers.Manager
	StateProvider  states.IStateProvider
	needValidate   bool
	MargoValidator validation.MargoValidator
}

func (s *MargoManager) Init(context *contexts.VendorContext, config managers.ManagerConfig, providers map[string]providers.IProvider) error {
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
	return nil
}

func (s *MargoManager) storePkgInDB(context context.Context, id string, pkg margoAPIModels.AppPkg) error {
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

func (s *MargoManager) updatePkgInDB(context context.Context, id string, pkg margoAPIModels.AppPkg) error {
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

func (s *MargoManager) deletePkgInDB(context context.Context, pkg margoAPIModels.AppPkg) error {
	return nil
}

func (s *MargoManager) listPkgFromDB(context context.Context, pkg margoAPIModels.AppPkg) error {
	return nil
}

func (s *MargoManager) getPkgFromDB(context context.Context, pkgId string) (*margoAPIModels.AppPkg, error) {
	entry, err := s.StateProvider.Get(context, states.GetRequest{
		Metadata: appPkgMetadata,
		ID:       pkgId,
	})
	if err != nil {
		return nil, err
	}

	var appPkg margoAPIModels.AppPkg
	jData, _ := json.Marshal(entry.Body)
	_ = json.Unmarshal(jData, &appPkg)

	return &appPkg, nil
}

// OnboardAppPkg handles the complete application package onboarding process.
func (s *MargoManager) OnboardAppPkg(ctx context.Context, spec margoAPIModels.AppPkgOnboardingReq) (*margoAPIModels.AppPkgOnboardingResp, error) {
	margoLog.InfofCtx(ctx, "OnboardAppPkg: Starting onboarding process for package '%s' with source type '%s'", spec.Name, spec.SourceType)

	// Validate input parameters
	if spec.Name == "" {
		margoLog.ErrorfCtx(ctx, "OnboardAppPkg: Package name is required but was empty")
		return nil, fmt.Errorf("package name is required")
	}
	if spec.SourceType == "" {
		margoLog.ErrorfCtx(ctx, "OnboardAppPkg: Source type is required but was empty")
		return nil, fmt.Errorf("source type is required")
	}

	// Generate unique identifier for the onboard AppPkg to be onboarded
	now := time.Now().UTC()
	appPkgId := margoUtils.GenerateAppPkgId()
	operation := margoAPIModels.Onboard
	operationState := margoAPIModels.Pending

	margoLog.InfofCtx(ctx, "OnboardAppPkg: Generated package ID '%s' for package '%s'", appPkgId, spec.Name)
	margoLog.DebugfCtx(ctx, "OnboardAppPkg: Initial operation state set to '%s'", operationState)

	// Prepare app pkg object
	var appPkg margoAPIModels.AppPkg
	{
		margoLog.DebugfCtx(ctx, "OnboardAppPkg: Creating package object from spec")
		bytes, _ := json.Marshal(&spec)
		_ = json.Unmarshal(bytes, &appPkg)
	}
	appPkg.Id = appPkgId
	appPkg.Name = spec.Name
	appPkg.Operation = operation
	appPkg.OperationState = operationState
	appPkg.CreatedAt = now
	appPkg.UpdatedAt = now

	margoLog.DebugfCtx(ctx, "OnboardAppPkg: Package object prepared with ID '%s', Name '%s', Operation '%s', State '%s'",
		appPkg.Id, appPkg.Name, appPkg.Operation, appPkg.OperationState)

	// Store initial package record in database
	margoLog.InfofCtx(ctx, "OnboardAppPkg: Storing initial package record in database")
	if err := s.storePkgInDB(ctx, appPkg.Id, appPkg); err != nil {
		margoLog.ErrorfCtx(ctx, "OnboardAppPkg: Failed to store package in database: %v", err)
		return nil, fmt.Errorf("failed to store app pkg in database: %w", err)
	}
	margoLog.InfofCtx(ctx, "OnboardAppPkg: Successfully stored initial package record with ID '%s'", appPkg.Id)

	// Start async processing
	margoLog.InfofCtx(ctx, "OnboardAppPkg: Starting async processing for package '%s'", appPkg.Id)
	go func() {
		time.Sleep(time.Second * 8)
		s.processPackageAsync(ctx, appPkg, spec)
	}()

	// Create and return response
	margoLog.DebugfCtx(ctx, "OnboardAppPkg: Creating response object for package '%s'", appPkg.Name)
	var resp margoAPIModels.AppPkgOnboardingResp
	{
		bytes, _ := json.Marshal(&appPkg)
		_ = json.Unmarshal(bytes, &resp)
	}

	margoLog.InfofCtx(ctx, "OnboardAppPkg: Successfully initiated onboarding process for package '%s' with ID '%s'",
		spec.Name, appPkg.Id)

	return &resp, nil
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
func (s *MargoManager) processPackageAsync(ctx context.Context, appPkg margoAPIModels.AppPkg, spec margoAPIModels.AppPkgOnboardingReq) {
	margoLog.InfofCtx(ctx, "processPackageAsync: Starting async processing for package '%s'", appPkg.Id)

	var err error
	operationContextualInfo := ""

	// Ensure final state update regardless of success or failure
	defer func() {
		margoLog.DebugfCtx(ctx, "processPackageAsync: Finalizing package state for ID '%s'", appPkg.Id)

		appPkg.OperationContextualInfo = &operationContextualInfo
		if err != nil {
			appPkg.OperationState = margoAPIModels.Failed
			margoLog.WarnfCtx(ctx, "processPackageAsync: Setting package state to Failed due to error: %v", err)
		} else {
			appPkg.OperationState = margoAPIModels.Completed
			margoLog.InfofCtx(ctx, "processPackageAsync: Setting package state to Completed")
		}

		appPkg.UpdatedAt = time.Now().UTC()
		margoLog.DebugfCtx(ctx, "processPackageAsync: Updating package final state to '%s' in database", appPkg.OperationState)

		if updateErr := s.updatePkgInDB(ctx, appPkg.Id, appPkg); updateErr != nil {
			margoLog.ErrorfCtx(ctx, "processPackageAsync: Error occurred while updating package state in database: %v", updateErr)
		} else {
			margoLog.InfofCtx(ctx, "processPackageAsync: Successfully updated final package state to '%s'", appPkg.OperationState)
		}
	}()

	// Initialize package manager
	margoLog.InfofCtx(ctx, "processPackageAsync: Initializing package manager")
	pkgMgr := packageManager.NewPackageManager()

	// Process based on source type
	margoLog.InfofCtx(ctx, "processPackageAsync: Processing source type '%s'", spec.SourceType)
	switch spec.SourceType {
	case margoAPIModels.AppPkgOnboardingReqSourceTypeGITREPO:
		err = s.processGitRepository(ctx, pkgMgr, spec, &operationContextualInfo)
	default:
		err = fmt.Errorf("unsupported source type: %s", spec.SourceType)
		operationContextualInfo = err.Error()
		margoLog.ErrorfCtx(ctx, "processPackageAsync: %s", err.Error())
	}

	if err == nil {
		margoLog.InfofCtx(ctx, "processPackageAsync: Successfully completed async processing for package '%s'", appPkg.Id)
	} else {
		margoLog.ErrorfCtx(ctx, "processPackageAsync: Failed async processing for package '%s': %v", appPkg.Id, err)
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
func (s *MargoManager) processGitRepository(ctx context.Context, pkgMgr *packageManager.PackageManager, spec margoAPIModels.AppPkgOnboardingReq, operationContextualInfo *string) error {
	margoLog.InfofCtx(ctx, "processGitRepository: Processing Git repository source")

	// Parse Git repository configuration
	gitRepo, err := spec.Source.AsGitRepo()
	if err != nil {
		*operationContextualInfo = fmt.Sprintf("Failed to parse the git repo spec: %s", err.Error())
		margoLog.ErrorfCtx(ctx, "processGitRepository: Failed to parse Git repository spec: %v", err)
		return err
	}

	margoLog.InfofCtx(ctx, "processGitRepository: Git repository URL: %s", gitRepo.Url)
	{
		jsonBytes, _ := gitRepo.MarshalJSON()
		margoLog.DebugfCtx(ctx, "processGitRepository: Git repository configuration: %s", string(jsonBytes))
	}

	// Set up authentication
	var gitAuth *margoUtils.GitAuth
	if gitRepo.AccessToken != nil && gitRepo.Username != nil {
		gitAuth = &margoUtils.GitAuth{
			Username: *gitRepo.Username,
			Token:    *gitRepo.AccessToken,
		}
		margoLog.InfofCtx(ctx, "processGitRepository: Using Git authentication for user '%s'", *gitRepo.Username)
	} else {
		margoLog.InfofCtx(ctx, "processGitRepository: Using anonymous Git access (no credentials provided)")
	}

	// Determine Git reference
	gitRefName := ""
	if gitRepo.Branch != nil {
		gitRefName = *gitRepo.Branch
		margoLog.InfofCtx(ctx, "processGitRepository: Using Git branch '%s'", gitRefName)
	} else if gitRepo.Tag != nil {
		gitRefName = *gitRepo.Tag
		margoLog.InfofCtx(ctx, "processGitRepository: Using Git tag '%s'", gitRefName)
	} else {
		margoLog.WarnfCtx(ctx, "processGitRepository: No branch or tag specified, using empty reference")
	}

	// Determine subpath
	subPath := ""
	if gitRepo.SubPath != nil {
		subPath = *gitRepo.SubPath
		margoLog.InfofCtx(ctx, "processGitRepository: Using Git subpath '%s'", subPath)
	} else {
		margoLog.DebugfCtx(ctx, "processGitRepository: No subpath specified, using repository root")
	}

	// Download package from Git repository
	margoLog.InfofCtx(ctx, "processGitRepository: Starting package download from Git repository")
	margoLog.DebugfCtx(ctx, "processGitRepository: Download parameters - URL: %s, Ref: %s, SubPath: %s",
		gitRepo.Url, gitRefName, subPath)

	pkgPath, _, err := pkgMgr.LoadPackageFromGit(
		gitRepo.Url,
		gitRefName,
		subPath,
		gitAuth,
	)
	if err != nil {
		*operationContextualInfo = fmt.Sprintf("Failed to download the package from git repo: %s", err.Error())
		margoLog.ErrorfCtx(ctx, "processGitRepository: Failed to load package from Git repository: %v", err)
		return err
	}

	margoLog.InfofCtx(ctx, "processGitRepository: Successfully downloaded package to directory: %s", pkgPath)

	// TODO: Add cleanup for pkgPath when processing is complete
	// TODO: Add package validation and processing logic here

	return nil
}

func (s *MargoManager) ListAppPkgs(context context.Context) (*margoAPIModels.ListAppPkgsResp, error) {
	var appPkgs []margoAPIModels.AppPkg
	entries, _, err := s.StateProvider.List(context, states.ListRequest{
		Metadata: appPkgMetadata,
	})
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		var appPkg margoAPIModels.AppPkg
		jData, _ := json.Marshal(entry.Body)
		err = json.Unmarshal(jData, &appPkg)
		if err == nil {
			appPkgs = append(appPkgs, appPkg)
		}
	}

	resp := margoAPIModels.ListAppPkgsResp{
		AppPkgs: make([]margoAPIModels.AppPkgSummary, len(appPkgs)),
	}
	for _, pkg := range appPkgs {
		var summary margoAPIModels.AppPkgSummary
		{
			bytes, _ := json.Marshal(pkg)
			_ = json.Unmarshal(bytes, &summary)
		}
		resp.AppPkgs = append(resp.AppPkgs, summary)
	}

	return &resp, nil
}

func (s *MargoManager) GetAppPkg(context context.Context, pkgId string) (*margoAPIModels.AppPkgSummary, error) {
	entry, err := s.StateProvider.Get(context, states.GetRequest{
		Metadata: appPkgMetadata,
		ID:       pkgId,
	})
	if err != nil {
		return nil, err
	}

	var appPkg margoAPIModels.AppPkg
	jData, _ := json.Marshal(entry.Body)
	_ = json.Unmarshal(jData, &appPkg)

	var resp margoAPIModels.AppPkgSummary
	{
		bytes, _ := json.Marshal(appPkg)
		_ = json.Unmarshal(bytes, &resp)
	}

	return &resp, nil
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
//   - *margoAPIModels.DeleteAppPkgResp: The deletion response with operation status
//   - error: An error if the deletion cannot be initiated
func (s *MargoManager) DeleteAppPkg(ctx context.Context, pkgId string) (*margoAPIModels.DeleteAppPkgResp, error) {
	margoLog.InfofCtx(ctx, "DeleteAppPkg: Starting deletion process for package ID '%s'", pkgId)

	// Validate input parameter
	if pkgId == "" {
		margoLog.ErrorfCtx(ctx, "DeleteAppPkg: Package ID is required but was empty")
		return nil, fmt.Errorf("package ID is required")
	}

	// Retrieve package from database to verify existence and get current state
	margoLog.DebugfCtx(ctx, "DeleteAppPkg: Retrieving package from database")
	appPkg, err := s.getPkgFromDB(ctx, pkgId)
	if err != nil {
		margoLog.ErrorfCtx(ctx, "DeleteAppPkg: Failed to retrieve package from database: %v", err)
		return nil, fmt.Errorf("failed to check the latest state of the app pkg: %w", err)
	}

	if appPkg == nil {
		margoLog.WarnfCtx(ctx, "DeleteAppPkg: Package with ID '%s' does not exist", pkgId)
		return nil, fmt.Errorf("package with id %s does not exist", pkgId)
	}

	margoLog.InfofCtx(ctx, "DeleteAppPkg: Found package '%s' with current operation '%s' and state '%s'",
		appPkg.Name, appPkg.Operation, appPkg.OperationState)

	// Update package state to indicate deletion is starting
	margoLog.InfofCtx(ctx, "DeleteAppPkg: Initiating deboarding operation for package '%s'", appPkg.Name)
	appPkg.Operation = margoAPIModels.Deboard
	appPkg.OperationState = margoAPIModels.Pending
	appPkg.UpdatedAt = time.Now().UTC()

	margoLog.DebugfCtx(ctx, "DeleteAppPkg: Updating package state to Operation='%s', State='%s'",
		appPkg.Operation, appPkg.OperationState)

	if err := s.updatePkgInDB(ctx, pkgId, *appPkg); err != nil {
		margoLog.ErrorfCtx(ctx, "DeleteAppPkg: Failed to update package state in database: %v", err)
		return nil, fmt.Errorf("failed to change the app pkg operation state before triggering deletion: %w", err)
	}

	margoLog.InfofCtx(ctx, "DeleteAppPkg: Successfully updated package state to pending deletion")

	// Start asynchronous deletion process
	margoLog.InfofCtx(ctx, "DeleteAppPkg: Starting background deletion process for package '%s'", pkgId)
	go s.performAsyncDeletion(ctx, pkgId, appPkg)

	// Create and return response
	msg := "App package deletion has been triggered."
	resp := &margoAPIModels.DeleteAppPkgResp{
		Id:             appPkg.Id,
		Message:        &msg,
		Operation:      appPkg.Operation,
		OperationState: appPkg.OperationState,
	}

	margoLog.InfofCtx(ctx, "DeleteAppPkg: Successfully initiated deletion process for package '%s'", pkgId)
	return resp, nil
}

// performAsyncDeletion handles the asynchronous deletion process in the background.
func (s *MargoManager) performAsyncDeletion(pCtx context.Context, pkgId string, appPkg *margoAPIModels.AppPkg) {
	margoLog.InfofCtx(pCtx, "performAsyncDeletion: Starting background deletion for package '%s'", pkgId)

	// Phase 1: Simulate deboarding process
	margoLog.InfofCtx(pCtx, "performAsyncDeletion: Phase 1 - Deboarding package '%s' (simulated delay)", appPkg.Name)
	// TODO: this is done deliberately to simulate a real-life scenario where deboarding a package can take some time
	// this has no practical use case but done for PoC demo, unless a concrete implementation is completed here
	time.Sleep(time.Second * 8)

	// Update package state to completed deboarding
	margoLog.InfofCtx(pCtx, "performAsyncDeletion: Phase 1 completed - Updating package state to completed")
	appPkg.OperationState = margoAPIModels.Completed
	appPkg.UpdatedAt = time.Now().UTC()

	if err := s.updatePkgInDB(pCtx, pkgId, *appPkg); err != nil {
		margoLog.ErrorfCtx(pCtx, "performAsyncDeletion: Failed to update package state to completed: %v", err)
		// Continue with deletion even if state update fails
	} else {
		margoLog.InfofCtx(pCtx, "performAsyncDeletion: Successfully updated package state to completed")
	}

	// Phase 2: Final cleanup and removal from state provider
	margoLog.InfofCtx(pCtx, "performAsyncDeletion: Phase 2 - Final cleanup for package '%s' (simulated delay)", pkgId)
	// Additional delay to simulate cleanup operations
	time.Sleep(time.Second * 5)

	// Force delete from state provider
	margoLog.InfofCtx(pCtx, "performAsyncDeletion: Removing package '%s' from state provider", pkgId)
	err := s.StateProvider.Delete(pCtx, states.DeleteRequest{
		Metadata: appPkgMetadata,
		ID:       pkgId,
	})

	if err != nil {
		margoLog.ErrorfCtx(pCtx, "performAsyncDeletion: Failed to delete package '%s' from state provider: %v", pkgId, err)
		// TODO: Consider implementing retry logic or dead letter queue for failed deletions
	} else {
		margoLog.InfofCtx(pCtx, "performAsyncDeletion: Successfully removed package '%s' from state provider", pkgId)
	}

	margoLog.InfofCtx(pCtx, "performAsyncDeletion: Completed background deletion process for package '%s'", pkgId)
}

func (s *MargoManager) UpdateAppPkgState(context context.Context, appId string, op margoAPIModels.AppPkgOperation, opState margoAPIModels.AppPkgOperationState) error {
	return nil
}

// TODO: move this to some suitable package
func (s *MargoManager) ConvertToSolution(context context.Context, appId string, spec margoAPIModels.AppPkgOnboardingReq) (model.SolutionState, error) {
	return model.SolutionState{}, nil
}

func (s *MargoManager) WatchAppChanges(context context.Context) error {
	return nil
}

// GetCampaign retrieves a CampaignSpec object by name
func (s *MargoManager) Shutdown(ctx context.Context) error {
	return nil
}
