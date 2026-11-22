package margo

import (
	"context"
	"testing"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/managers/solutions"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/contexts"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/states/memorystate"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestSameNamedMargoPackagesCoexistWithSolutionValidation(t *testing.T) {
	store := &memorystate.MemoryStateProvider{}
	require.NoError(t, store.Init(memorystate.MemoryStateProviderConfig{}))
	manager := &solutions.SolutionsManager{}
	require.NoError(t, manager.Init(&contexts.VendorContext{Logger: logger.NewLogger("coa.runtime")}, managers.ManagerConfig{Properties: map[string]string{v1alpha2.ProvidersPersistentState: "memory"}}, map[string]providers.IProvider{"memory": store}))
	transformer := NewMargoTransformer()
	legacy, err := transformer.convertToSolution(*readyPackage().Description, "legacy-catalog")
	require.NoError(t, err)
	legacy.Spec.DisplayName = "Example"
	require.NoError(t, manager.UpsertState(context.Background(), legacy.ObjectMeta.Name, *legacy))
	for _, identity := range []struct{ id, version string }{{"io.example.app", "1.0.0"}, {"io.example.app", "2.0.0"}, {"io.other.app", "2.0.0"}} {
		p := readyPackage()
		p.Description.Id = &identity.id
		p.Description.Metadata.Version = identity.version
		solution, err := transformer.convertToSolution(*p.Description, identity.id+"-v-"+identity.version)
		require.NoError(t, err)
		require.NoError(t, manager.UpsertState(context.Background(), solution.ObjectMeta.Name, *solution))
		// Reimporting the same identity is an update, not another display-name conflict.
		require.NoError(t, manager.UpsertState(context.Background(), solution.ObjectMeta.Name, *solution))
		catalog, err := transformer.convertToCatalog(*p.Description, ConversionContext{}, nil)
		require.NoError(t, err)
		spec := catalog.Spec.Properties["spec"].(map[string]interface{})
		require.Equal(t, "Example", spec["displayName"])
	}
	all, err := manager.ListState(context.Background(), "default")
	require.NoError(t, err)
	require.Len(t, all, 3)
}

func TestTransformationRejectsMissingDataBeforeLogging(t *testing.T) {
	transformer := NewMargoTransformer()
	for _, row := range []AppPackageDatabaseRow{{}, {AppDescription: readyPackage().Description}} {
		require.NotPanics(t, func() {
			_, _, _, err := transformer.AppPackageToSymphonyObjects(context.Background(), row, nil)
			require.Error(t, err)
		})
	}
}
