package margo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	margoNonStdAPI "github.com/margo/sandbox/non-standard/generatedCode/wfm/nbi"
	"github.com/margo/sandbox/standard/generatedCode/wfm/sbi"
)

// Simplified DeploymentState based on actual usage
type DeploymentState string

const (
	// Core states that actually exist in the code
	DeploymentStatePending    DeploymentState = "PENDING"    // Initial state when deployment is created
	DeploymentStateInstalling DeploymentState = "INSTALLING" // Being deployed to device
	DeploymentStateInstalled  DeploymentState = "INSTALLED"  // Successfully deployed and running
	DeploymentStateRunning    DeploymentState = "RUNNING"    // Being deployed to device
	DeploymentStateUpdating   DeploymentState = "UPDATING"   // Being updated on device
	DeploymentStateRemoving   DeploymentState = "REMOVING"   // Marked for deletion, waiting for device confirmation
	DeploymentStateRemoved    DeploymentState = "REMOVED"    // Confirmed deleted by device (terminal state)
	DeploymentStateFailed     DeploymentState = "FAILED"     // Deployment failed
)

// Simplified events based on actual code flow
type DeploymentEvent string

const (
	EventStartInstallation   DeploymentEvent = "START_INSTALLATION"
	EventInstallationSuccess DeploymentEvent = "INSTALLATION_SUCCESSFUL"
	EventInstallationFailed  DeploymentEvent = "INSTALLATION_FAILED"
	EventStartUpdate         DeploymentEvent = "START_UPDATE"
	EventUpdateSuccess       DeploymentEvent = "UPDATE_SUCCESSFUL"
	EventUpdateFailed        DeploymentEvent = "UPDATE_FAILED"
	EventStartRemoval        DeploymentEvent = "START_REMOVAL"
	EventRemovalSuccess      DeploymentEvent = "REMOVAL_SUCCESSFUL"
	EventRemovalFailed       DeploymentEvent = "REMOVAL_FAILED"
)

// Simplified state machine
type DeploymentStateMachine struct {
	database *MargoDatabase
	log      logger.Logger
}

func NewDeploymentStateMachine(database *MargoDatabase, log logger.Logger) *DeploymentStateMachine {
	return &DeploymentStateMachine{
		database: database,
		log:      log,
	}
}

// ProcessEvent handles state transitions
func (sm *DeploymentStateMachine) ProcessEvent(ctx context.Context, deploymentId string, event DeploymentEvent, contextInfo string, err error) error {
	sm.log.InfofCtx(ctx, "DeploymentStateMachine: Processing event '%s' for deployment '%s'", event, deploymentId)

	// Get current deployment
	deployment, dbErr := sm.database.GetDeployment(ctx, deploymentId)
	if dbErr != nil {
		return fmt.Errorf("failed to get deployment: %w", dbErr)
	}

	// Normalize state to uppercase for consistent comparison
	currentStateRaw := string(*deployment.DeploymentRequest.Status.State)
	currentState := DeploymentState(strings.ToUpper(currentStateRaw))

	sm.log.InfofCtx(ctx, "DeploymentStateMachine: Current state '%s' (normalized: '%s')", currentStateRaw, currentState)

	var newState DeploymentState
	var operationStatus margoNonStdAPI.ApplicationDeploymentOperationStatus

	// Simple state transitions based on events
	switch event {
	case EventStartInstallation:
		if currentState == DeploymentStatePending {
			newState = DeploymentStateInstalling
			operationStatus = margoNonStdAPI.ApplicationDeploymentOperationStatusPROCESSING
		} else {
			return fmt.Errorf("cannot start deployment from state %s", currentState)
		}

	case EventInstallationSuccess:
		if currentState == DeploymentStateInstalling {
			newState = DeploymentStateInstalled
			operationStatus = margoNonStdAPI.ApplicationDeploymentOperationStatusCOMPLETED
		} else {
			return fmt.Errorf("cannot complete deployment from state %s", currentState)
		}

	case EventInstallationFailed:
		if currentState == DeploymentStateInstalling {
			newState = DeploymentStateFailed
			operationStatus = margoNonStdAPI.ApplicationDeploymentOperationStatusFAILED
		} else {
			return fmt.Errorf("cannot fail deployment from state %s", currentState)
		}

	case EventStartUpdate:
		if currentState == DeploymentStateInstalled {
			newState = DeploymentStateUpdating
			operationStatus = margoNonStdAPI.ApplicationDeploymentOperationStatusPROCESSING
		} else {
			return fmt.Errorf("cannot start update from state %s", currentState)
		}

	case EventUpdateSuccess:
		if currentState == DeploymentStateUpdating {
			newState = DeploymentStateInstalled
			operationStatus = margoNonStdAPI.ApplicationDeploymentOperationStatusCOMPLETED
		} else {
			return fmt.Errorf("cannot complete update from state %s", currentState)
		}

	case EventUpdateFailed:
		if currentState == DeploymentStateUpdating {
			newState = DeploymentStateFailed
			operationStatus = margoNonStdAPI.ApplicationDeploymentOperationStatusFAILED
		} else {
			return fmt.Errorf("cannot fail update from state %s", currentState)
		}

	case EventStartRemoval:
		// Allow deletion from Pending, Installed, or Failed states
		if currentState == DeploymentStatePending ||
			currentState == DeploymentStateInstalled ||
			currentState == DeploymentStateFailed {
			newState = DeploymentStateRemoving
			operationStatus = margoNonStdAPI.ApplicationDeploymentOperationStatusPROCESSING
			sm.log.InfofCtx(ctx, "DeploymentStateMachine: Transitioning from '%s' to REMOVING", currentState)
		} else {
			return fmt.Errorf("cannot start deletion from state %s (must be Pending, Installed, or Failed)", currentState)
		}

	case EventRemovalSuccess:
		// Added missing EventRemovalSuccess case
		if currentState == DeploymentStateRemoving {
			newState = DeploymentStateRemoved
			operationStatus = margoNonStdAPI.ApplicationDeploymentOperationStatusCOMPLETED
		} else {
			return fmt.Errorf("cannot confirm deletion from state %s", currentState)
		}

	case EventRemovalFailed:
		if currentState == DeploymentStateRemoving {
			newState = DeploymentStateFailed
			operationStatus = margoNonStdAPI.ApplicationDeploymentOperationStatusFAILED
		} else {
			return fmt.Errorf("cannot fail deletion from state %s", currentState)
		}

	default:
		return fmt.Errorf("unknown event: %s", event)
	}

	// Update deployment state
	return sm.updateDeploymentState(ctx, deployment, newState, operationStatus, contextInfo, err)
}

// updateDeploymentState updates the deployment state in database
func (sm *DeploymentStateMachine) updateDeploymentState(
	ctx context.Context,
	deployment *DeploymentDatabaseRow,
	newState DeploymentState,
	operationStatus margoNonStdAPI.ApplicationDeploymentOperationStatus,
	contextInfo string,
	err error) error {

	var desiredState sbi.DeploymentStatusManifestStatusState
	state := margoNonStdAPI.ApplicationDeploymentStatusState("")
	now := time.Now().UTC()
	switch newState {
	case DeploymentStatePending, DeploymentStateInstalling:
		state = margoNonStdAPI.ApplicationDeploymentStatusStatePENDING
		desiredState = sbi.DeploymentStatusManifestStatusStatePending
	case DeploymentStateInstalled:
		state = margoNonStdAPI.ApplicationDeploymentStatusStateINSTALLED
		desiredState = sbi.DeploymentStatusManifestStatusStateInstalled
	case DeploymentStateUpdating:
		state = margoNonStdAPI.ApplicationDeploymentStatusStateUPDATING
		desiredState = sbi.DeploymentStatusManifestStatusStateUpdating
	case DeploymentStateRemoving:
		state = margoNonStdAPI.ApplicationDeploymentStatusStateREMOVING
		desiredState = sbi.DeploymentStatusManifestStatusStateRemoving
	case DeploymentStateFailed:
		state = margoNonStdAPI.ApplicationDeploymentStatusStateFAILED
		desiredState = sbi.DeploymentStatusManifestStatusStateFailed
	case DeploymentStateRemoved:
		state = margoNonStdAPI.ApplicationDeploymentStatusStateREMOVED
		desiredState = sbi.DeploymentStatusManifestStatusStateRemoved
	default:
		state = margoNonStdAPI.ApplicationDeploymentStatusState(newState)
	}

	// Update deployment fields
	deployment.DeploymentRequest.Status.State = &state
	deployment.DeploymentRequest.Status.LastUpdateTime = &now
	deployment.DeploymentRequest.RecentOperation.Status = operationStatus
	deployment.LastStatusUpdate = now
	deployment.DesiredState.Status.Status.State = desiredState

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

		deployment.DeploymentRequest.Status.ContextualInfo = &margoNonStdAPI.ContextualInfo{
			Message: &message,
		}
	}

	// Save to database
	if updateErr := sm.database.UpsertDeployment(ctx, *deployment, true); updateErr != nil {
		return fmt.Errorf("failed to update deployment state: %w", updateErr)
	}

	sm.log.InfofCtx(ctx, "DeploymentStateMachine: Updated deployment '%s' to state '%s'",
		*deployment.DeploymentRequest.Metadata.Id, newState)

	return nil
}

// IsTerminalState checks if state is terminal
func (sm *DeploymentStateMachine) IsTerminalState(state DeploymentState) bool {
	return state == DeploymentStateRemoved
}
