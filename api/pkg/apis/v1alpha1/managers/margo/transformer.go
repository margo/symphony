package margo

import (
	"context"
	"fmt"
	"strings"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/model"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	margoNonStdAPI "github.com/margo/dev-repo/non-standard/generatedCode/wfm/nbi"
	"github.com/margo/dev-repo/standard/generatedCode/wfm/sbi"
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
	catalog, err := t.convertToCatalog(appDesc, convCtx, resources)
	if err != nil {
		transformerLogger.Error("Failed to convert to catalog",
			"appId", appDesc.Metadata.Id,
			"error", err)
		return nil, nil, nil, fmt.Errorf("failed to convert to catalog: %w", err)
	}

	// Convert to Solution
	solution, err := t.convertToSolution(appDesc, catalog.ObjectMeta.Name)
	if err != nil {
		transformerLogger.Error("Failed to convert to solution",
			"appId", appDesc.Metadata.Id,
			"catalogId", catalog.ObjectMeta.Name,
			"error", err)
		return nil, nil, nil, fmt.Errorf("failed to convert to solution: %w", err)
	}

	// Convert to SolutionContainer
	solutionContainer, err := t.convertToSolutionContainer(appDesc, solution.ObjectMeta.Name)
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
	appDesc margoNonStdAPI.AppDescription,
	catalogId string) (*model.SolutionState, error) {

	transformerLogger.Debug("Converting to Solution object",
		"appId", appDesc.Metadata.Id,
		"catalogId", catalogId)

	solutionId := appDesc.Metadata.Id + "-v-" + appDesc.Metadata.Version

	// Convert deployment profiles to components
	components, err := t.convertDeploymentProfilesToComponents(appDesc.DeploymentProfiles, appDesc.Configuration, appDesc.Parameters)
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
func (t *MargoTransformer) applyParameterValue(values map[string]interface{}, pointer string, value interface{}) error {
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
	parsedValue := t.parseParameterValue(value)
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

func (t *MargoTransformer) convertConfigurationSchema(
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

// ConvertDeploymentProfile converts the deployment profile cleanly
func (t *MargoTransformer) ConvertDeploymentProfile(profile margoNonStdAPI.DeploymentExecutionProfile) sbi.AppDeploymentProfile {
	return sbi.AppDeploymentProfile{
		Type:       sbi.AppDeploymentProfileType(profile.Type),
		Components: t.convertComponents(profile.Components),
	}
}

// convertComponents converts deployment profile components
func (t *MargoTransformer) convertComponents(components []margoNonStdAPI.DeploymentExecutionProfile_Components_Item) []sbi.AppDeploymentProfile_Components_Item {
	result := make([]sbi.AppDeploymentProfile_Components_Item, len(components))
	for i, comp := range components {
		result[i] = t.convertComponent(comp)
	}
	return result
}

// convertComponent converts a single component
func (t *MargoTransformer) convertComponent(comp margoNonStdAPI.DeploymentExecutionProfile_Components_Item) sbi.AppDeploymentProfile_Components_Item {
	// Convert based on component type - this replaces the JSON marshaling approach
	sbiComp := sbi.AppDeploymentProfile_Components_Item{}
	data, _ := comp.MarshalJSON()
	sbiComp.UnmarshalJSON(data)
	return sbiComp
}

// Helper functions for clean property selection
func (t *MargoTransformer) selectString(preferred, fallback string) string {
	if preferred != "" {
		return preferred
	}
	return fallback
}

func (t *MargoTransformer) selectStringPtr(preferred, fallback *string) *string {
	if preferred != nil {
		return preferred
	}
	return fallback
}

func (t *MargoTransformer) selectIntPtr(preferred, fallback *int) *int {
	if preferred != nil {
		return preferred
	}
	return fallback
}

func (t *MargoTransformer) selectBoolPtr(preferred, fallback *bool) *bool {
	if preferred != nil {
		return preferred
	}
	return fallback
}

// MergeWithAppPackage handles profile merging with cleaner logic
func (t *MargoTransformer) MergeWithAppPackage(req *margoNonStdAPI.ApplicationDeploymentManifestResp, appPkg ApplicationPackage) error {
	targetProfile := t.findMatchingProfileInApp(req.Spec.DeploymentProfile.Type, appPkg.Description.DeploymentProfiles)
	if targetProfile == nil {
		return nil // No matching profile found, continue without merging
	}

	appComponents := t.buildComponentMap(*targetProfile)

	for i := range req.Spec.DeploymentProfile.Components {
		if err := t.mergeComponent(&req.Spec.DeploymentProfile.Components[i], appComponents, targetProfile.Type); err != nil {
			deploymentLogger.Warn("Failed to merge component", "index", i, "error", err)
			return err
		}
	}

	if err := t.mergeParameters(req.Spec.Parameters, *appPkg.Description.Parameters); err != nil {
		deploymentLogger.Warn("Failed to merge parameters", "error", err)
		return err
	}

	return nil
}

func (t *MargoTransformer) mergeParameters(overrides *margoNonStdAPI.DeploymentParameters, defaultParams margoNonStdAPI.AppDescriptionParametersMap) error {
	transformerLogger.Debug("Merging deployment parameter overrides with app description parameters",
		"hasOverrides", overrides != nil,
		"hasCompleteParams", len(defaultParams) > 0)

	// If no overrides provided, nothing to merge
	if overrides == nil {
		transformerLogger.Debug("No deployment parameter overrides to merge")
		return nil
	}

	// Iterate through deployment parameter overrides
	for paramName, overrideValue := range *overrides {
		existingParam, exists := (defaultParams)[paramName]
		if !exists {
			transformerLogger.Error("parameter doesn't exist in app description, can't proceed with overrides", "paramName", paramName)
			return fmt.Errorf("parameter: %s doesn't exist in the application description, can't proceed with overrides", paramName)
		}

		// Parameter exists in app description - override the value while preserving other properties
		existingParam.Value = overrideValue
		(defaultParams)[paramName] = existingParam

		transformerLogger.Debug("Overrode existing parameter value",
			"paramName", paramName,
			"newValue", overrideValue,
			"hasTargets", len(existingParam.Targets) > 0)
	}

	transformerLogger.Debug("Parameter merging completed successfully",
		"totalParams", len(defaultParams),
		"overrideCount", len(*overrides))

	return nil
}

// findMatchingProfileInApp finds the deployment profile that matches the request
func (t *MargoTransformer) findMatchingProfileInApp(reqType margoNonStdAPI.DeploymentExecutionProfileType, profiles []margoNonStdAPI.AppDeploymentProfile) *margoNonStdAPI.AppDeploymentProfile {
	for _, profile := range profiles {
		if profile.Type == margoNonStdAPI.AppDeploymentProfileType(reqType) {
			return &profile
		}
	}
	return nil
}

// buildComponentMap creates a lookup map for app description components
func (t *MargoTransformer) buildComponentMap(profile margoNonStdAPI.AppDeploymentProfile) map[string]interface{} {
	components := make(map[string]interface{})

	for _, component := range profile.Components {
		switch profile.Type {
		case margoNonStdAPI.AppDeploymentProfileTypeHelmV3:
			if helmComp, err := component.AsHelmApplicationDeploymentProfileComponent(); err == nil {
				components[helmComp.Name] = helmComp
			}
		case margoNonStdAPI.AppDeploymentProfileTypeCompose:
			if composeComp, err := component.AsComposeApplicationDeploymentProfileComponent(); err == nil {
				components[composeComp.Name] = composeComp
			}
		}
	}
	return components
}

func (t *MargoTransformer) mergeComponent(reqComponent *margoNonStdAPI.DeploymentExecutionProfile_Components_Item, appComponents map[string]interface{}, profileType margoNonStdAPI.AppDeploymentProfileType) error {
	switch profileType {
	case margoNonStdAPI.AppDeploymentProfileTypeHelmV3:
		return t.mergeHelmComponent(reqComponent, appComponents)
	case margoNonStdAPI.AppDeploymentProfileTypeCompose:
		return t.mergeComposeComponent(reqComponent, appComponents)
	default:
		return fmt.Errorf("unsupported profile type: %s", profileType)
	}
}

func (t *MargoTransformer) mergeHelmComponent(reqComponent *margoNonStdAPI.DeploymentExecutionProfile_Components_Item, appComponents map[string]interface{}) error {
	helmReqComp, err := reqComponent.AsHelmDeploymentProfileComponent()
	if err != nil {
		return err
	}

	if appCompInterface, exists := appComponents[helmReqComp.Name]; exists {
		if appComp, ok := appCompInterface.(margoNonStdAPI.HelmApplicationDeploymentProfileComponent); ok {
			merged := t.mergeHelmProperties(appComp, helmReqComp)
			return reqComponent.FromHelmDeploymentProfileComponent(merged)
		}
	}
	return nil
}

func (t *MargoTransformer) mergeComposeComponent(reqComponent *margoNonStdAPI.DeploymentExecutionProfile_Components_Item, appComponents map[string]interface{}) error {
	composeReqComp, err := reqComponent.AsComposeDeploymentProfileComponent()
	if err != nil {
		return err
	}

	if appCompInterface, exists := appComponents[composeReqComp.Name]; exists {
		if appComp, ok := appCompInterface.(margoNonStdAPI.ComposeApplicationDeploymentProfileComponent); ok {
			merged := t.mergeComposeProperties(appComp, composeReqComp)
			return reqComponent.FromComposeDeploymentProfileComponent(merged)
		}
	}
	return nil
}

// mergeHelmProperties merges helm component properties cleanly
func (t *MargoTransformer) mergeHelmProperties(appComp margoNonStdAPI.HelmApplicationDeploymentProfileComponent, reqComp margoNonStdAPI.HelmDeploymentProfileComponent) margoNonStdAPI.HelmDeploymentProfileComponent {
	merged := margoNonStdAPI.HelmDeploymentProfileComponent{
		Name: reqComp.Name,
		Properties: struct {
			Repository string  "json:\"repository\""
			Revision   *string "json:\"revision,omitempty\""
			Timeout    *string "json:\"timeout,omitempty\""
			Wait       *bool   "json:\"wait,omitempty\""
		}{
			Repository: t.selectString(reqComp.Properties.Repository, appComp.Properties.Repository),
			Revision:   t.selectStringPtr(reqComp.Properties.Revision, appComp.Properties.Revision),
			Timeout:    t.selectStringPtr(reqComp.Properties.Timeout, appComp.Properties.Timeout),
			Wait:       t.selectBoolPtr(reqComp.Properties.Wait, appComp.Properties.Wait),
		},
	}
	return merged
}

func (t *MargoTransformer) DbRowToDeploymentList(data []DeploymentDatabaseRow) (margoNonStdAPI.ApplicationDeploymentListResp, error) {
	deployments := make([]margoNonStdAPI.ApplicationDeploymentManifestResp, len(data))
	for i, row := range data {
		deployments[i] = row.DeploymentRequest
	}

	return margoNonStdAPI.ApplicationDeploymentListResp{
		ApiVersion: "margo.org",
		Kind:       "ApplicationDeploymentList",
		Items:      deployments,
		Metadata: margoNonStdAPI.PaginationMetadata{
			Continue: &[]bool{false}[0],
		},
	}, nil
}

// ADD THIS METHOD to MargoTransformer:
func (t *MargoTransformer) MergeConfigurationWithAppPackage(
	deploymentParams *margoNonStdAPI.AppConfigurationSchema,
	appConfig *margoNonStdAPI.AppConfigurationSchema,
	appParams *margoNonStdAPI.AppDescriptionParametersMap) (map[string]interface{}, error) {

	// Start with app package configuration as base
	baseConfig := appConfig
	if baseConfig == nil {
		baseConfig = &margoNonStdAPI.AppConfigurationSchema{}
	}

	// Convert app package configuration to map
	appConfigMap, err := t.convertConfigurationSchema(baseConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to convert app configuration: %w", err)
	}

	// Convert deployment parameters to map
	var deploymentConfigMap map[string]interface{}
	if deploymentParams != nil {
		deploymentConfigMap, err = t.convertConfigurationSchema(deploymentParams)
		if err != nil {
			return nil, fmt.Errorf("failed to convert deployment parameters: %w", err)
		}
	}

	// Merge: deployment parameters override app package configuration
	merged := make(map[string]interface{})

	// Add app package config first
	if appConfigMap != nil {
		for k, v := range appConfigMap {
			merged[k] = v
		}
	}

	// Override with deployment config
	if deploymentConfigMap != nil {
		for k, v := range deploymentConfigMap {
			merged[k] = v
		}
	}

	// Apply app package parameter values
	if appParams != nil {
		paramValues := t.extractParameterValues(*appParams)
		if len(paramValues) > 0 {
			merged["parameterValues"] = paramValues
		}
	}

	return merged, nil
}

func (t *MargoTransformer) extractParameterValues(params margoNonStdAPI.AppDescriptionParametersMap) map[string]interface{} {
	values := make(map[string]interface{})
	for paramName, paramValue := range params {
		if paramValue.Value != nil {
			values[paramName] = paramValue.Value
		}
	}
	return values
}

// ADD THIS METHOD for Compose merging:
func (t *MargoTransformer) mergeComposeProperties(appComp margoNonStdAPI.ComposeApplicationDeploymentProfileComponent, reqComp margoNonStdAPI.ComposeDeploymentProfileComponent) margoNonStdAPI.ComposeDeploymentProfileComponent {
	return margoNonStdAPI.ComposeDeploymentProfileComponent{
		Name: reqComp.Name,
		Properties: struct {
			KeyLocation     *string "json:\"keyLocation,omitempty\""
			PackageLocation string  "json:\"packageLocation\""
			Timeout         *string "json:\"timeout,omitempty\""
			Wait            *bool   "json:\"wait,omitempty\""
		}{
			PackageLocation: t.selectString(reqComp.Properties.PackageLocation, appComp.Properties.PackageLocation),
			KeyLocation:     t.selectStringPtr(reqComp.Properties.KeyLocation, appComp.Properties.KeyLocation),
			Timeout:         t.selectStringPtr(reqComp.Properties.Timeout, appComp.Properties.Timeout),
			Wait:            t.selectBoolPtr(reqComp.Properties.Wait, appComp.Properties.Wait),
		},
	}
}
