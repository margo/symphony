package margo

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	margoNonStdAPI "github.com/margo/sandbox/non-standard/generatedCode/wfm/nbi"
	"github.com/margo/sandbox/standard/generatedCode/wfm/sbi"
)

var transformerLogger = logger.NewLogger("coa.runtime")

type MargoTransformer struct{}

func NewMargoTransformer() *MargoTransformer {
	return &MargoTransformer{}
}

func (t *MargoTransformer) convertConfigurationSchema(
	config *margoNonStdAPI.AppConfigurationSchema,
) (map[string]interface{}, error) {
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
		Type:              sbi.AppDeploymentProfileType(profile.Type),
		Components:        t.convertComponents(profile.Components),
		DeviceConstraints: t.convertDeviceConstraints(profile.DeviceConstraints),
	}
}

// convertComponents converts deployment profile components
func (t *MargoTransformer) convertComponents(components []margoNonStdAPI.DeploymentExecutionProfile_Components_Item) []sbi.ApplicationDeploymentProfileComponent {
	result := make([]sbi.ApplicationDeploymentProfileComponent, len(components))
	for i, comp := range components {
		result[i] = t.convertComponent(comp)
	}
	return result
}

func (t *MargoTransformer) convertDeviceConstraints(dc *margoNonStdAPI.DeviceConstraints) *sbi.DeviceConstraints {
	// device constraints are not a required parameter, if not present, omit it.
	if dc == nil {
		return nil
	}

	result := sbi.DeviceConstraints{}
	// TODO: do not supress errors
	rawDc, _ := json.Marshal(*dc)
	json.Unmarshal(rawDc, &result)
	return &result
}

// convertComponent converts a single component
func (t *MargoTransformer) convertComponent(comp margoNonStdAPI.DeploymentExecutionProfile_Components_Item) sbi.ApplicationDeploymentProfileComponent {
	sbiComp := sbi.ApplicationDeploymentProfileComponent{}
	data, _ := comp.MarshalJSON()
	_ = json.Unmarshal(data, &sbiComp)
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

	// as device constraints are not a required field
	if targetProfile.DeviceConstraints != nil {
		req.Spec.DeploymentProfile.DeviceConstraints = targetProfile.DeviceConstraints
	}

	appComponents := t.buildComponentMap(*targetProfile)

	for i := range req.Spec.DeploymentProfile.Components {
		if err := t.mergeComponent(&req.Spec.DeploymentProfile.Components[i], appComponents, targetProfile.Type); err != nil {
			deploymentLogger.Warn("Failed to merge component", "index", i, "error", err)
			return err
		}
	}

	appPkgParams := margoNonStdAPI.AppDescriptionParametersMap{}
	if appPkg.Description.Parameters != nil {
		appPkgParams = *appPkg.Description.Parameters
	}
	if err := t.mergeParameters(req.Spec.Parameters, appPkgParams); err != nil {
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
		existingParam, exists := defaultParams[paramName]
		if !exists {
			transformerLogger.Error("parameter doesn't exist in app description, can't proceed with overrides", "paramName", paramName)
			return fmt.Errorf("parameter: %s doesn't exist in the application description, can't proceed with overrides", paramName)
		}

		// Parameter exists in app description - override the value while preserving other properties
		existingParam.Value = overrideValue
		defaultParams[paramName] = existingParam

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
		case margoNonStdAPI.AppDeploymentProfileTypeHelm:
			if helmComp, err := component.AsApplicationDeploymentProfileComponent(); err == nil {
				components[helmComp.Name] = helmComp
			}
		case margoNonStdAPI.AppDeploymentProfileTypeCompose:
			if composeComp, err := component.AsApplicationDeploymentProfileComponent(); err == nil {
				components[composeComp.Name] = composeComp
			}
		}
	}
	return components
}

func (t *MargoTransformer) mergeComponent(reqComponent *margoNonStdAPI.DeploymentExecutionProfile_Components_Item, appComponents map[string]interface{}, profileType margoNonStdAPI.AppDeploymentProfileType) error {
	switch profileType {
	case margoNonStdAPI.AppDeploymentProfileTypeHelm:
		return t.mergeHelmComponent(reqComponent, appComponents)
	case margoNonStdAPI.AppDeploymentProfileTypeCompose:
		return t.mergeComposeComponent(reqComponent, appComponents)
	default:
		return fmt.Errorf("unsupported profile type: %s", profileType)
	}
}

func (t *MargoTransformer) mergeHelmComponent(reqComponent *margoNonStdAPI.DeploymentExecutionProfile_Components_Item, appComponents map[string]interface{}) error {
	reqComp, err := reqComponent.AsDeploymentProfileComponent()
	if err != nil {
		return err
	}
	if appCompInterface, exists := appComponents[reqComp.Name]; exists {
		if appComp, ok := appCompInterface.(margoNonStdAPI.ApplicationDeploymentProfileComponent); ok {
			merged := t.mergeHelmProperties(appComp, reqComp)
			return reqComponent.FromDeploymentProfileComponent(merged)
		}
	}
	return nil
}

func (t *MargoTransformer) mergeComposeComponent(reqComponent *margoNonStdAPI.DeploymentExecutionProfile_Components_Item, appComponents map[string]interface{}) error {
	reqComp, err := reqComponent.AsDeploymentProfileComponent()
	if err != nil {
		return err
	}
	if appCompInterface, exists := appComponents[reqComp.Name]; exists {
		if appComp, ok := appCompInterface.(margoNonStdAPI.ApplicationDeploymentProfileComponent); ok {
			merged := t.mergeComposeProperties(appComp, reqComp)
			return reqComponent.FromDeploymentProfileComponent(merged)
		}
	}
	return nil
}

// mergeHelmProperties merges helm component properties cleanly
func (t *MargoTransformer) mergeHelmProperties(
	appComp margoNonStdAPI.ApplicationDeploymentProfileComponent,
	reqComp margoNonStdAPI.DeploymentProfileComponent,
) margoNonStdAPI.DeploymentProfileComponent {
	return margoNonStdAPI.DeploymentProfileComponent{
		Name: reqComp.Name,
		Properties: struct {
			Repository string  `json:"repository"`
			Revision   string  `json:"revision"`
			Timeout    *string `json:"timeout,omitempty"`
			Wait       *bool   `json:"wait,omitempty"`
		}{
			Repository: t.selectString(reqComp.Properties.Repository, appComp.Properties.Repository),
			Revision:   t.selectString(reqComp.Properties.Revision, appComp.Properties.Revision),
			Timeout:    t.selectStringPtr(reqComp.Properties.Timeout, appComp.Properties.Timeout),
			Wait:       t.selectBoolPtr(reqComp.Properties.Wait, appComp.Properties.Wait),
		},
	}
}

func (t *MargoTransformer) DbRowToDeploymentList(data []DeploymentDatabaseRow) (margoNonStdAPI.ApplicationDeploymentListResp, error) {
	deployments := make([]margoNonStdAPI.ApplicationDeploymentManifestResp, len(data))
	for i, row := range data {
		deployments[i] = row.DeploymentRequest
	}

	return margoNonStdAPI.ApplicationDeploymentListResp{
		Items: deployments,
		Metadata: margoNonStdAPI.PaginationMetadata{
			Continue: &[]bool{false}[0],
		},
	}, nil
}

// ADD THIS METHOD to MargoTransformer:
func (t *MargoTransformer) MergeConfigurationWithAppPackage(
	deploymentParams *margoNonStdAPI.AppConfigurationSchema,
	appConfig *margoNonStdAPI.AppConfigurationSchema,
	appParams *margoNonStdAPI.AppDescriptionParametersMap,
) (map[string]interface{}, error) {
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
func (t *MargoTransformer) mergeComposeProperties(
	appComp margoNonStdAPI.ApplicationDeploymentProfileComponent,
	reqComp margoNonStdAPI.DeploymentProfileComponent,
) margoNonStdAPI.DeploymentProfileComponent {
	return margoNonStdAPI.DeploymentProfileComponent{
		Name: reqComp.Name,
		Properties: struct {
			Repository string  `json:"repository"`
			Revision   string  `json:"revision"`
			Timeout    *string `json:"timeout,omitempty"`
			Wait       *bool   `json:"wait,omitempty"`
		}{
			Repository: t.selectString(reqComp.Properties.Repository, appComp.Properties.Repository),
			Revision:   t.selectString(reqComp.Properties.Revision, appComp.Properties.Revision),
			Timeout:    t.selectStringPtr(reqComp.Properties.Timeout, appComp.Properties.Timeout),
			Wait:       t.selectBoolPtr(reqComp.Properties.Wait, appComp.Properties.Wait),
		},
	}
}
