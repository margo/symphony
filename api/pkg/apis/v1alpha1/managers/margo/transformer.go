package margo

import (
	"context"
	"fmt"
	"strings"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	margoNonStdAPI "github.com/margo/dev-repo/non-standard/generatedCode/wfm/nbi"
)

var (
	transformerLogger = logger.NewLogger("coa.runtime")
)

// ConversionContext holds minimal data needed from appPkg for conversion
type ConversionContext struct {
	SourcePackageName string
	SourceType        string
	SourceInfo        interface{}
}

type MargoTransformer struct {
}

func NewMargoTransformer() *MargoTransformer {
	return &MargoTransformer{}
}

// Transform database objects to Symphony objects
func (t *MargoTransformer) AppPackageToSymphonyObjects(
	ctx context.Context,
	dbRow AppPackageDatabaseRow,
	resources map[string][]byte) (*model.CatalogState, *model.SolutionState, *model.SolutionContainerState, error) {

	transformerLogger.Info("Starting Symphony transformation",
		"appId", dbRow.AppDescription.Metadata.Id,
		"appName", dbRow.AppDescription.Metadata.Name,
		"packageId", *dbRow.PackageRequest.Metadata.Id)

	// Validate required data
	if dbRow.AppDescription == nil {
		return nil, nil, nil, fmt.Errorf("app description is required for transformation")
	}
	if dbRow.PackageRequest.Metadata.Id == nil {
		return nil, nil, nil, fmt.Errorf("package ID is required for transformation")
	}

	appDesc := *dbRow.AppDescription

	// Extract conversion context from database row
	convCtx := ConversionContext{
		SourcePackageName: dbRow.PackageRequest.Metadata.Name,
		SourceType:        string(dbRow.PackageRequest.Spec.SourceType),
		SourceInfo:        dbRow.PackageRequest.Spec.Source,
	}

	// Convert to Catalog
	catalog, err := t.convertToCatalog(ctx, appDesc, convCtx, resources)
	if err != nil {
		transformerLogger.Error("Failed to convert to catalog",
			"appId", appDesc.Metadata.Id,
			"error", err)
		return nil, nil, nil, fmt.Errorf("failed to convert to catalog: %w", err)
	}

	// Convert to Solution
	solution, err := t.convertToSolution(ctx, appDesc, catalog.ObjectMeta.Name)
	if err != nil {
		transformerLogger.Error("Failed to convert to solution",
			"appId", appDesc.Metadata.Id,
			"catalogId", catalog.ObjectMeta.Name,
			"error", err)
		return nil, nil, nil, fmt.Errorf("failed to convert to solution: %w", err)
	}

	// Convert to SolutionContainer
	solutionContainer, err := t.convertToSolutionContainer(ctx, appDesc, solution.ObjectMeta.Name)
	if err != nil {
		transformerLogger.Error("Failed to convert to solution container",
			"appId", appDesc.Metadata.Id,
			"solutionId", solution.ObjectMeta.Name,
			"error", err)
		return nil, nil, nil, fmt.Errorf("failed to convert to solution container: %w", err)
	}

	transformerLogger.Info("Symphony transformation completed successfully",
		"appId", appDesc.Metadata.Id,
		"catalogId", catalog.ObjectMeta.Name,
		"solutionId", solution.ObjectMeta.Name,
		"containerId", solutionContainer.ObjectMeta.Name)

	return catalog, solution, solutionContainer, nil
}

// convertToCatalog converts application description to Catalog object
func (t *MargoTransformer) convertToCatalog(
	ctx context.Context,
	appDesc margoNonStdAPI.AppDescription,
	convCtx ConversionContext,
	resources map[string][]byte) (*model.CatalogState, error) {

	transformerLogger.Debug("Converting to Catalog object",
		"appId", appDesc.Metadata.Id,
		"appName", appDesc.Metadata.Name)

	// catalog name should be <rootresource>-v-<version> as per symphony convention
	catalogId := appDesc.Metadata.Id + "-v-" + appDesc.Metadata.Version

	catalog := &model.CatalogState{
		ObjectMeta: model.ObjectMeta{
			Name:      catalogId,
			Namespace: "default",
		},
		Spec: &model.CatalogSpec{
			CatalogType:  "solution",
			RootResource: appDesc.Metadata.Id,
			Version:      appDesc.Metadata.Version,
			Properties: map[string]interface{}{
				"spec": map[string]interface{}{
					"displayName": appDesc.Metadata.Name,
					"description": appDesc.Metadata.Description,
					"version":     appDesc.Metadata.Version,
					"sourcePackage": map[string]interface{}{
						"sourceType":  convCtx.SourceType,
						"sourceInfo":  convCtx.SourceInfo,
						"packageName": convCtx.SourcePackageName,
					},
					"metadata":      appDesc.Metadata,
					"resourceCount": len(resources),
				},
			},
		},
	}

	transformerLogger.Debug("Catalog object created successfully",
		"catalogId", catalogId,
		"appId", appDesc.Metadata.Id)

	return catalog, nil
}

// convertToSolution converts application description to Solution object
func (t *MargoTransformer) convertToSolution(
	ctx context.Context,
	appDesc margoNonStdAPI.AppDescription,
	catalogId string) (*model.SolutionState, error) {

	transformerLogger.Debug("Converting to Solution object",
		"appId", appDesc.Metadata.Id,
		"catalogId", catalogId)

	solutionId := appDesc.Metadata.Id + "-v-" + appDesc.Metadata.Version

	// Convert deployment profiles to components
	components, err := t.convertDeploymentProfilesToComponents(ctx, appDesc.DeploymentProfiles, appDesc.Configuration, appDesc.Parameters)
	if err != nil {
		return nil, fmt.Errorf("failed to convert deployment profiles: %w", err)
	}

	solution := &model.SolutionState{
		ObjectMeta: model.ObjectMeta{
			Name:      solutionId,
			Namespace: "default",
		},
		Spec: &model.SolutionSpec{
			Version:      appDesc.Metadata.Version,
			RootResource: appDesc.Metadata.Id,
			DisplayName:  appDesc.Metadata.Name,
			Components:   components,
			Metadata: map[string]string{
				"description":   *appDesc.Metadata.Description,
				"applicationId": appDesc.Metadata.Id,
				"catalogRef":    catalogId,
				"profileCount":  fmt.Sprintf("%d", len(appDesc.DeploymentProfiles)),
			},
		},
	}

	transformerLogger.Debug("Solution object created successfully",
		"solutionId", solutionId,
		"appId", appDesc.Metadata.Id,
		"componentCount", len(components))

	return solution, nil
}

// convertToSolutionContainer converts application description to SolutionContainer object
func (t *MargoTransformer) convertToSolutionContainer(
	ctx context.Context,
	appDesc margoNonStdAPI.AppDescription,
	solutionId string) (*model.SolutionContainerState, error) {

	transformerLogger.Debug("Converting to SolutionContainer object",
		"appId", appDesc.Metadata.Id,
		"solutionId", solutionId)

	containerId := appDesc.Metadata.Id + "-v-" + appDesc.Metadata.Version

	solutionContainer := &model.SolutionContainerState{
		ObjectMeta: model.ObjectMeta{
			Name:      containerId,
			Namespace: "default",
		},
		Spec: &model.SolutionContainerSpec{
			// Add solution container specific properties here
		},
	}

	transformerLogger.Debug("SolutionContainer object created successfully",
		"containerId", containerId,
		"appId", appDesc.Metadata.Id,
		"solutionId", solutionId)

	return solutionContainer, nil
}

// convertDeploymentProfilesToComponents converts deployment profiles to Symphony components
func (t *MargoTransformer) convertDeploymentProfilesToComponents(
	ctx context.Context,
	deploymentProfiles []margoNonStdAPI.AppDeploymentProfile,
	config *margoNonStdAPI.AppConfigurationSchema,
	parameters *margoNonStdAPI.AppDescriptionParametersMap) ([]model.ComponentSpec, error) {

	var components []model.ComponentSpec

	for profileIdx, profile := range deploymentProfiles {
		for compIdx, component := range profile.Components {
			var symphonyComponent model.ComponentSpec
			var err error

			switch profile.Type {
			case margoNonStdAPI.AppDeploymentProfileTypeHelmV3:
				symphonyComponent, err = t.convertHelmComponent(component, profile, parameters)
			case margoNonStdAPI.AppDeploymentProfileTypeCompose:
				symphonyComponent, err = t.convertComposeComponent(component, profile, parameters)
			default:
				return nil, fmt.Errorf("unsupported profile type: %s", profile.Type)
			}

			if err != nil {
				return nil, fmt.Errorf("failed to convert component %d in profile %d: %w", compIdx, profileIdx, err)
			}

			components = append(components, symphonyComponent)
		}
	}

	transformerLogger.Debug("Deployment profiles conversion completed",
		"totalComponents", len(components))

	return components, nil
}

// convertHelmComponent converts a Helm component to Symphony ComponentSpec
func (t *MargoTransformer) convertHelmComponent(
	component margoNonStdAPI.AppDeploymentProfile_Components_Item,
	profile margoNonStdAPI.AppDeploymentProfile,
	parameters *margoNonStdAPI.AppDescriptionParametersMap) (model.ComponentSpec, error) {

	helmComp, err := component.AsHelmApplicationDeploymentProfileComponent()
	if err != nil {
		return model.ComponentSpec{}, fmt.Errorf("failed to parse helm component: %w", err)
	}

	// Build component properties
	properties := map[string]interface{}{
		"chart": map[string]interface{}{
			"repository": helmComp.Properties.Repository,
			"name":       helmComp.Name,
		},
	}

	// Add optional properties
	if helmComp.Properties.Revision != nil {
		properties["chart"].(map[string]interface{})["version"] = *helmComp.Properties.Revision
	}
	if helmComp.Properties.Timeout != nil {
		properties["timeout"] = *helmComp.Properties.Timeout
	}
	if helmComp.Properties.Wait != nil {
		properties["wait"] = *helmComp.Properties.Wait
	}

	// Apply parameter overrides
	if parameters != nil {
		values, err := t.resolveComponentParameters(helmComp.Name, *parameters)
		if err != nil {
			return model.ComponentSpec{}, fmt.Errorf("failed to resolve parameters: %w", err)
		}
		if len(values) > 0 {
			properties["values"] = values
		}
	}

	return model.ComponentSpec{
		Name:       helmComp.Name,
		Type:       "helm.v3",
		Properties: properties,
	}, nil
}

// convertComposeComponent converts a Compose component to Symphony ComponentSpec
func (t *MargoTransformer) convertComposeComponent(
	component margoNonStdAPI.AppDeploymentProfile_Components_Item,
	profile margoNonStdAPI.AppDeploymentProfile,
	parameters *margoNonStdAPI.AppDescriptionParametersMap) (model.ComponentSpec, error) {

	composeComp, err := component.AsComposeApplicationDeploymentProfileComponent()
	if err != nil {
		return model.ComponentSpec{}, fmt.Errorf("failed to parse compose component: %w", err)
	}

	// Build component properties for Compose
	properties := map[string]interface{}{
		"compose": map[string]interface{}{
			"packageLocation": composeComp.Properties.PackageLocation,
			"name":            composeComp.Name,
		},
	}

	// Add optional properties
	if composeComp.Properties.KeyLocation != nil {
		properties["compose"].(map[string]interface{})["keyLocation"] = *composeComp.Properties.KeyLocation
	}
	if composeComp.Properties.Timeout != nil {
		properties["timeout"] = *composeComp.Properties.Timeout
	}
	if composeComp.Properties.Wait != nil {
		properties["wait"] = *composeComp.Properties.Wait
	}

	// Apply parameter overrides
	if parameters != nil {
		values, err := t.resolveComponentParameters(composeComp.Name, *parameters)
		if err != nil {
			return model.ComponentSpec{}, fmt.Errorf("failed to resolve parameters: %w", err)
		}
		if len(values) > 0 {
			properties["values"] = values
		}
	}

	return model.ComponentSpec{
		Name:       composeComp.Name,
		Type:       "compose",
		Properties: properties,
	}, nil
}

// resolveComponentParameters resolves parameters for a specific component
func (t *MargoTransformer) resolveComponentParameters(
	componentName string,
	parameters margoNonStdAPI.AppDescriptionParametersMap) (map[string]interface{}, error) {

	values := make(map[string]interface{})

	for paramName, paramValue := range parameters {
		if paramValue.Targets == nil {
			continue
		}

		for _, target := range paramValue.Targets {
			// Check if this parameter targets our component
			for _, targetComponent := range target.Components {
				if targetComponent == componentName {
					// Apply parameter using JSONPath pointer
					if paramValue.Value != nil {
						if err := t.applyParameterValue(values, target.Pointer, paramValue.Value); err != nil {
							return nil, fmt.Errorf("failed to apply parameter %s: %w", paramName, err)
						}
					}
				}
			}
		}
	}

	return values, nil
}

// applyParameterValue applies a parameter value to a nested map using JSONPath-like pointer
func (s *MargoTransformer) applyParameterValue(values map[string]interface{}, pointer string, value interface{}) error {
	transformerLogger.Debug("Applying parameter value",
		"pointer", pointer,
		"value", value)

	// Remove leading slash if present (JSONPath format)
	pointer = strings.TrimPrefix(pointer, "/")

	if pointer == "" {
		return fmt.Errorf("empty parameter pointer")
	}

	// Split the pointer into path segments
	segments := strings.Split(pointer, "/")

	// Navigate to the parent of the target field
	current := values
	for i, segment := range segments[:len(segments)-1] {
		// Handle array indices (e.g., "items[0]" or "items.0")
		if strings.Contains(segment, "[") && strings.Contains(segment, "]") {
			return fmt.Errorf("array indexing not yet supported in parameter pointer: %s", pointer)
		}

		// Create nested map if it doesn't exist
		if _, exists := current[segment]; !exists {
			current[segment] = make(map[string]interface{})
		}

		// Ensure the current segment is a map
		nextMap, ok := current[segment].(map[string]interface{})
		if !ok {
			return fmt.Errorf("cannot navigate through non-map value at segment %d (%s) in pointer %s", i, segment, pointer)
		}

		current = nextMap
	}

	// Set the final value
	finalKey := segments[len(segments)-1]

	// Try to parse value as different types
	parsedValue := s.parseParameterValue(value)
	current[finalKey] = parsedValue

	transformerLogger.Debug("Successfully applied parameter value",
		"pointer", pointer,
		"finalKey", finalKey,
		"parsedValue", parsedValue)

	return nil
}

// parseParameterValue attempts to parse a string value into appropriate Go types
func (t *MargoTransformer) parseParameterValue(value interface{}) interface{} {
	return value
}

func (s *MargoTransformer) convertConfigurationSchema(
	config *margoNonStdAPI.AppConfigurationSchema) (map[string]interface{}, error) {

	if config == nil {
		return nil, nil
	}

	configMap := make(map[string]interface{})

	// Convert schema definitions
	if config.Schema != nil {
		schemas := make(map[string]interface{})
		for _, schema := range *config.Schema {
			schemas[schema.Name] = map[string]interface{}{
				"dataType":   schema.DataType,
				"allowEmpty": schema.AllowEmpty,
				"minLength":  schema.MinLength,
				"maxLength":  schema.MaxLength,
				"minValue":   schema.MinValue,
				"maxValue":   schema.MaxValue,
				"regexMatch": schema.RegexMatch,
			}
		}
		configMap["schemas"] = schemas
	}

	// Convert sections for UI organization
	if config.Sections != nil {
		sections := make([]interface{}, 0, len(*config.Sections))
		for _, section := range *config.Sections {
			sectionMap := map[string]interface{}{
				"name":     section.Name,
				"settings": make([]interface{}, 0, len(section.Settings)),
			}

			for _, setting := range section.Settings {
				settingMap := map[string]interface{}{
					"name":        setting.Name,
					"parameter":   setting.Parameter,
					"schema":      setting.Schema,
					"description": setting.Description,
					"immutable":   setting.Immutable,
				}
				sectionMap["settings"] = append(sectionMap["settings"].([]interface{}), settingMap)
			}

			sections = append(sections, sectionMap)
		}
		configMap["sections"] = sections
	}

	return configMap, nil
}
