package margo

import (
	"context"
	"encoding/json"
	"testing"

	manager "github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/managers/margo"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/contexts"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/states/memorystate"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	nbi "github.com/margo/sandbox/non-standard/generatedCode/wfm/nbi"
	"github.com/stretchr/testify/require"
)

func TestCreateDeploymentReturnsConflictForUnavailablePackage(t *testing.T) {
	for _, state := range []nbi.ApplicationPackageStatusState{nbi.ApplicationPackageStatusStatePENDING, nbi.ApplicationPackageStatusStateFAILED, nbi.ApplicationPackageStatusStateONBOARDED} {
		t.Run(string(state), func(t *testing.T) {
			memory := &memorystate.MemoryStateProvider{}
			require.NoError(t, memory.Init(memorystate.MemoryStateProviderConfig{}))
			db := manager.NewMargoDatabase(&contexts.ManagerContext{Logger: logger.NewLogger("coa.runtime")}, "package-manager", memory)
			id := "package-id"
			require.NoError(t, db.UpsertAppPackage(context.Background(), manager.AppPackageDatabaseRow{PackageRequest: nbi.ApplicationPackageManifestResp{Id: &id, Status: &nbi.ApplicationPackageStatus{State: &state}}}))
			vendor := &WorkloadMgmtVendor{AppPkgManager: &manager.AppPkgManager{Database: db}, DeploymentManager: &manager.DeploymentManager{}}
			req := nbi.ApplicationDeploymentManifestRequest{Metadata: nbi.Metadata{Name: "trial"}}
			req.Spec.AppPackageRef.Id = id
			body, err := json.Marshal(req)
			require.NoError(t, err)
			response := vendor.createDeployment(v1alpha2.COARequest{Context: context.Background(), Body: body, Method: "POST"})
			require.Equal(t, v1alpha2.Conflict, response.State)
			var failure nbi.ErrorResponse
			require.NoError(t, json.Unmarshal(response.Body, &failure))
			require.NotEmpty(t, failure.ErrorCode)
		})
	}
}
