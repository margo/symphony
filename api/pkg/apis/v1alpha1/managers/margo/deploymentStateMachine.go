package margo

import (
	"context"
	"fmt"
	"time"

	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	margoNonStdAPI "github.com/margo/dev-repo/non-standard/generatedCode/wfm/nbi"
	"github.com/margo/dev-repo/standard/generatedCode/wfm/sbi"
)

// Simplified DeploymentState based on actual usage
type DeploymentState string

const (
	// Core states that actually exist in the code
	DeploymentStatePending   DeploymentState = "PENDING"   // Initial state when deployment is created
	DeploymentStateDeploying DeploymentState = "DEPLOYING" // Being deployed to device
	DeploymentStateRunning   DeploymentState = "RUNNING"   // Successfully deployed and running
	DeploymentStateUpdating  DeploymentState = "UPDATING"  // Being updated on device
	DeploymentStateDeleting  DeploymentState = "DELETING"  // Marked for deletion, waiting for device confirmation
	DeploymentStateFailed    DeploymentState = "FAILED"    // Deployment failed
	DeploymentStateRemoved   DeploymentState = "REMOVED"   // Confirmed deleted by device (terminal state)
)

// Simplified events based on actual code flow
type DeploymentEvent string

const (
	EventStartDeployment   DeploymentEvent = "START_DEPLOYMENT"
	EventDeploymentSuccess DeploymentEvent = "DEPLOYMENT_SUCCESS"
	EventDeploymentFailed  DeploymentEvent = "DEPLOYMENT_FAILED"
	EventStartUpdate       DeploymentEvent = "START_UPDATE"
	EventUpdateSuccess     DeploymentEvent = "UPDATE_SUCCESS"
	EventUpdateFailed      DeploymentEvent = "UPDATE_FAILED"
	EventStartDeletion     DeploymentEvent = "START_DELETION"
	EventDeletionConfirmed DeploymentEvent = "DELETION_CONFIRMED"
	EventDeletionFailed    DeploymentEvent = "DELETION_FAILED"
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

	currentState := DeploymentState(*deployment.DeploymentRequest.Status.State)
	var newState DeploymentState
	var operationStatus margoNonStdAPI.ApplicationDeploymentOperationStatus

	// Simple state transitions based on events
	switch event {
	case EventStartDeployment:
		if currentState == DeploymentStatePending {
			newState = DeploymentStateDeploying
			operationStatus = margoNonStdAPI.ApplicationDeploymentOperationStatusPROCESSING
		} else {
			return fmt.Errorf("cannot start deployment from state %s", currentState)
		}

	case EventDeploymentSuccess:
		if currentState == DeploymentStateDeploying {
			newState = DeploymentStateRunning
			operationStatus = margoNonStdAPI.ApplicationDeploymentOperationStatusCOMPLETED
		} else {
			return fmt.Errorf("cannot complete deployment from state %s", currentState)
		}

	case EventDeploymentFailed:
		if currentState == DeploymentStateDeploying {
			newState = DeploymentStateFailed
			operationStatus = margoNonStdAPI.ApplicationDeploymentOperationStatusFAILED
		} else {
			return fmt.Errorf("cannot fail deployment from state %s", currentState)
		}

	case EventStartUpdate:
		if currentState == DeploymentStateRunning {
			newState = DeploymentStateUpdating
			operationStatus = margoNonStdAPI.ApplicationDeploymentOperationStatusPROCESSING
		} else {
			return fmt.Errorf("cannot start update from state %s", currentState)
		}

	case EventUpdateSuccess:
		if currentState == DeploymentStateUpdating {
			newState = DeploymentStateRunning
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

	case EventStartDeletion:
		if currentState == DeploymentStateRunning || currentState == DeploymentStateFailed {
			newState = DeploymentStateDeleting
			operationStatus = margoNonStdAPI.ApplicationDeploymentOperationStatusPROCESSING
		} else {
			return fmt.Errorf("cannot start deletion from state %s", currentState)
		}

	case EventDeletionConfirmed:
		if currentState == DeploymentStateDeleting {
			newState = DeploymentStateRemoved
			operationStatus = margoNonStdAPI.ApplicationDeploymentOperationStatusCOMPLETED
		} else {
			return fmt.Errorf("cannot confirm deletion from state %s", currentState)
		}

	case EventDeletionFailed:
		if currentState == DeploymentStateDeleting {
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

	var desiredState sbi.AppStateAppState
	now := time.Now().UTC()
	state := margoNonStdAPI.ApplicationDeploymentStatusState(newState)
	switch newState {
	case DeploymentStatePending, DeploymentStateDeploying:
		state = margoNonStdAPI.ApplicationDeploymentStatusStatePENDING
		desiredState = sbi.PENDING
	case DeploymentStateRunning:
		state = margoNonStdAPI.ApplicationDeploymentStatusStateRUNNING
		desiredState = sbi.RUNNING
	case DeploymentStateUpdating:
		state = margoNonStdAPI.ApplicationDeploymentStatusStateUPDATING
		desiredState = sbi.UPDATING
	case DeploymentStateDeleting:
		state = margoNonStdAPI.ApplicationDeploymentStatusStateREMOVING
		desiredState = sbi.REMOVING
	case DeploymentStateFailed:
		state = margoNonStdAPI.ApplicationDeploymentStatusStateUNKNOWN
		desiredState = sbi.STOPPED
	case DeploymentStateRemoved:
		state = margoNonStdAPI.ApplicationDeploymentStatusStateUNKNOWN
		desiredState = sbi.REMOVING
	}

	// Update deployment fields
	deployment.DeploymentRequest.Status.State = &state
	deployment.DeploymentRequest.Status.LastUpdateTime = &now
	deployment.DeploymentRequest.RecentOperation.Status = operationStatus
	deployment.LastStatusUpdate = now
	deployment.DesiredDeployment.AppState = desiredState

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
