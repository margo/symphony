package margo

import (
	"context"
	"fmt"
	"time"

	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	margoNonStdAPI "github.com/margo/sandbox/non-standard/generatedCode/wfm/nbi"
)

// Simplified AppPkgState based on actual usage
type AppPkgState string

const (
	// Core states that actually exist in the code
	StatePending    AppPkgState = "PENDING"    // Initial state when package is received
	StateProcessing AppPkgState = "PROCESSING" // Being processed (downloading, parsing, converting)
	StateOnboarded  AppPkgState = "ONBOARDED"  // Successfully completed
	StateFailed     AppPkgState = "FAILED"     // Failed with error
	StateDeleting   AppPkgState = "DELETING"   // Being deleted
)

// Simplified events based on actual code flow
type AppPkgEvent string

const (
	EventStartProcessing    AppPkgEvent = "START_PROCESSING"
	EventProcessingComplete AppPkgEvent = "PROCESSING_COMPLETE"
	EventProcessingFailed   AppPkgEvent = "PROCESSING_FAILED"
	EventDeleteRequested    AppPkgEvent = "DELETE_REQUESTED"
	EventDeleteComplete     AppPkgEvent = "DELETE_COMPLETE"
)

// Simplified state machine
type AppPkgStateMachine struct {
	database *MargoDatabase
	log      logger.Logger
}

func NewAppPkgStateMachine(database *MargoDatabase, log logger.Logger) *AppPkgStateMachine {
	return &AppPkgStateMachine{
		database: database,
		log:      log,
	}
}

// ProcessEvent handles state transitions
func (sm *AppPkgStateMachine) ProcessEvent(ctx context.Context, packageId string, event AppPkgEvent, contextInfo string, err error) error {
	sm.log.InfofCtx(ctx, "AppPkgStateMachine: Processing event '%s' for package '%s'", event, packageId)

	// Get current package
	pkg, dbErr := sm.database.GetAppPackage(ctx, packageId)
	if dbErr != nil {
		return fmt.Errorf("failed to get package: %w", dbErr)
	}

	currentState := AppPkgState(*pkg.PackageRequest.Status.State)
	var newState AppPkgState
	var operationStatus margoNonStdAPI.ApplicationPackageOperationStatus

	// Simple state transitions based on events
	switch event {
	case EventStartProcessing:
		if currentState == StatePending {
			newState = StateProcessing
			operationStatus = margoNonStdAPI.ApplicationPackageOperationStatusPROCESSING
		} else {
			return fmt.Errorf("cannot start processing from state %s", currentState)
		}

	case EventProcessingComplete:
		if currentState == StateProcessing || currentState == StatePending {
			newState = StateOnboarded
			operationStatus = margoNonStdAPI.ApplicationPackageOperationStatusCOMPLETED
		} else {
			return fmt.Errorf("cannot complete processing from state %s", currentState)
		}

	case EventProcessingFailed:
		if currentState == StateProcessing || currentState == StatePending {
			newState = StateFailed
			operationStatus = margoNonStdAPI.ApplicationPackageOperationStatusFAILED
		} else {
			return fmt.Errorf("cannot fail processing from state %s", currentState)
		}

	case EventDeleteRequested:
		if currentState == StateOnboarded || currentState == StateFailed {
			newState = StateDeleting
			operationStatus = margoNonStdAPI.ApplicationPackageOperationStatusPROCESSING
		} else {
			return fmt.Errorf("cannot delete from state %s", currentState)
		}

	case EventDeleteComplete:
		// Package will be removed from database, no state update needed
		return sm.database.DeleteAppPackage(ctx, packageId)

	default:
		return fmt.Errorf("unknown event: %s", event)
	}

	// Update package state
	return sm.updatePackageState(ctx, pkg, newState, operationStatus, contextInfo, err)
}

// updatePackageState updates the package state in database
func (sm *AppPkgStateMachine) updatePackageState(
	ctx context.Context,
	pkg *AppPackageDatabaseRow,
	newState AppPkgState,
	operationStatus margoNonStdAPI.ApplicationPackageOperationStatus,
	contextInfo string,
	err error) error {

	now := time.Now().UTC()
	state := margoNonStdAPI.ApplicationPackageStatusState(newState)

	// Update package fields
	pkg.PackageRequest.Status.State = &state
	pkg.PackageRequest.Status.LastUpdateTime = &now
	pkg.PackageRequest.RecentOperation.Status = operationStatus

	// Update contextual info
	if contextInfo != "" || err != nil {
		message := contextInfo
		if err != nil {
			if message == "" {
				message = err.Error()
			} else {
				message = fmt.Sprintf("%s: %s", message, err.Error())
			}
		}

		pkg.PackageRequest.Status.ContextualInfo = &margoNonStdAPI.ContextualInfo{
			Message: &message,
		}
	}

	// Save to database
	if updateErr := sm.database.UpsertAppPackage(ctx, *pkg); updateErr != nil {
		return fmt.Errorf("failed to update package state: %w", updateErr)
	}

	sm.log.InfofCtx(ctx, "AppPkgStateMachine: Updated package '%s' to state '%s'",
		*pkg.PackageRequest.Metadata.Id, newState)

	return nil
}

// IsTerminalState checks if state is terminal
func (sm *AppPkgStateMachine) IsTerminalState(state AppPkgState) bool {
	return state == StateOnboarded || state == StateFailed
}
