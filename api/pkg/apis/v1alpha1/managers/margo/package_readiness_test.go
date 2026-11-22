package margo

import (
	"context"
	"testing"

	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/contexts"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/states/memorystate"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	nbi "github.com/margo/sandbox/non-standard/generatedCode/wfm/nbi"
	"github.com/stretchr/testify/require"
)

func readyPackage() ApplicationPackage {
	id, description := "io.example.app", "Example application"
	state := nbi.ApplicationPackageStatusStateONBOARDED
	return ApplicationPackage{
		Package:     nbi.ApplicationPackageManifestResp{Status: &nbi.ApplicationPackageStatus{State: &state}},
		Description: &nbi.AppDescription{Id: &id, Metadata: nbi.AppDescriptionMetadata{Name: "Example", Version: "1.0.0", Description: &description}, DeploymentProfiles: []nbi.AppDeploymentProfile{{Type: nbi.AppDeploymentProfileTypeCompose}}},
	}
}

func TestCreateDeploymentRejectsUnavailablePackageWithoutWrites(t *testing.T) {
	cases := []struct {
		name   string
		change func(*ApplicationPackage)
	}{
		{"absent status", func(p *ApplicationPackage) { p.Package.Status = nil }},
		{"absent state", func(p *ApplicationPackage) { p.Package.Status.State = nil }},
		{"pending", func(p *ApplicationPackage) { *p.Package.Status.State = nbi.ApplicationPackageStatusStatePENDING }},
		{"processing", func(p *ApplicationPackage) { *p.Package.Status.State = nbi.ApplicationPackageStatusState("PROCESSING") }},
		{"failed", func(p *ApplicationPackage) { *p.Package.Status.State = nbi.ApplicationPackageStatusStateFAILED }},
		{"deleting", func(p *ApplicationPackage) { *p.Package.Status.State = nbi.ApplicationPackageStatusState("DELETING") }},
		{"absent description", func(p *ApplicationPackage) { p.Description = nil }},
		{"absent application ID", func(p *ApplicationPackage) { p.Description.Id = nil }},
		{"empty application ID", func(p *ApplicationPackage) { *p.Description.Id = "" }},
		{"empty application version", func(p *ApplicationPackage) { p.Description.Metadata.Version = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := readyPackage()
			tc.change(&p)
			// A nil database and transformer make any access before validation fail.
			manager := &DeploymentManager{}
			req := nbi.ApplicationDeploymentManifestRequest{Metadata: nbi.Metadata{Name: "trial"}}
			var err error
			require.NotPanics(t, func() {
				var result *nbi.ApplicationDeploymentManifestResp
				result, err = manager.CreateDeployment(context.Background(), req, p)
				require.Nil(t, result)
			})
			require.Error(t, err)
			require.Equal(t, v1alpha2.Conflict, v1alpha2.GetErrorState(err))
		})
	}
}

func TestCreateDeploymentAcceptsOnboardedPackage(t *testing.T) {
	store := &memorystate.MemoryStateProvider{}
	require.NoError(t, store.Init(memorystate.MemoryStateProviderConfig{}))
	manager := &DeploymentManager{database: NewMargoDatabase(&contexts.ManagerContext{Logger: logger.NewLogger("coa.runtime")}, deploymentManagerPublisherGroup, store), tranformer: NewMargoTransformer()}
	req := nbi.ApplicationDeploymentManifestRequest{Metadata: nbi.Metadata{Name: "trial"}}
	deviceID := "test-device"
	req.Spec.DeviceRef = &nbi.ApplicationDeploymentSpec_DeviceRef{Id: &deviceID}
	req.Spec.DeploymentProfile.Type = nbi.DeploymentExecutionProfileTypeCompose
	response, err := manager.CreateDeployment(context.Background(), req, readyPackage())
	require.NoError(t, err)
	row, err := manager.database.GetDeployment(context.Background(), *response.Id)
	require.NoError(t, err)
	require.Equal(t, "trial", row.DeploymentRequest.Metadata.Name)
}

func TestMergeRejectsMissingDescriptionAndUnsupportedProfile(t *testing.T) {
	transformer := NewMargoTransformer()
	require.Error(t, transformer.MergeWithAppPackage(nil, readyPackage()))
	req := &nbi.ApplicationDeploymentManifestResp{}
	require.Error(t, transformer.MergeWithAppPackage(req, ApplicationPackage{}))
	req.Spec.DeploymentProfile.Type = nbi.DeploymentExecutionProfileTypeHelm
	require.Error(t, transformer.MergeWithAppPackage(req, readyPackage()))
}
