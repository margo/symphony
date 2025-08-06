package margo

import (
	"encoding/json"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/managers/margo"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/observability"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/pubsub"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/vendors"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	margoStdSbiAPI "github.com/margo/dev-repo/standard/generatedCode/wfm/sbi"
	"github.com/valyala/fasthttp"
)

var deviceVendorLogger = logger.NewLogger("coa.runtime")

type DeviceVendor struct {
	vendors.Vendor
	DeviceManager *margo.DeviceManager
}

// struct for the onboarding response
type DeviceOnboardingResponse struct {
	ClientId         string `json:"clientId"`
	ClientSecret     string `json:"clientSecret"`
	TokenEndpointUrl string `json:"tokenEndpointUrl"`
}

// struct for the token request
type TokenRequest struct {
	ClientId         string `json:"clientId"`
	ClientSecret     string `json:"clientSecret"`
	TokenEndpointUrl string `json:"tokenEndpointUrl"`
}

// struct for the token response
type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

func (o *DeviceVendor) GetInfo() vendors.VendorInfo {
	return vendors.VendorInfo{
		Version:  o.Vendor.Version,
		Name:     "MargoDeviceVendor",
		Producer: "Margo",
	}
}

func (e *DeviceVendor) Init(config vendors.VendorConfig, factories []managers.IManagerFactroy, providers map[string]map[string]providers.IProvider, pubsubProvider pubsub.IPubSubProvider) error {
	err := e.Vendor.Init(config, factories, providers, pubsubProvider)
	if err != nil {
		return err
	}
	for _, m := range e.Managers {
		switch c := m.(type) {
		case *margo.DeviceManager:
			e.DeviceManager = c
		}
	}
	if e.DeviceManager == nil {
		return v1alpha2.NewCOAError(nil, "margo manager is not supplied", v1alpha2.MissingConfig)
	}
	return nil
}

func (o *DeviceVendor) GetEndpoints() []v1alpha2.Endpoint {
	route := DeviceInterfaceDefaultBaseURL
	if o.Route != "" {
		route = o.Route
	}
	return []v1alpha2.Endpoint{
		{
			Methods: []string{fasthttp.MethodPost},
			Route:   route + "/wfm/state",
			Version: o.Version,
			Handler: o.pollDesiredState,
		},

		{
			Methods: []string{fasthttp.MethodPost},
			Route:   route + "/onboarding/device",
			Version: o.Version,
			Handler: o.onboardDevice,
		},
		{
			Methods: []string{fasthttp.MethodPost},
			Route:   route + "/auth/token",
			Version: o.Version,
			Handler: o.getToken,
		},
	}
}

// Handler func for getToken
func (c *DeviceVendor) getToken(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Device Vendor",
		request.Context,
		&map[string]string{
			"method": "getToken",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	deviceVendorLogger.InfofCtx(pCtx, "V (MargoDeviceVendor): getToken, method: %s", request.Method)

	// Parse request
	var tokenReq TokenRequest
	if err := json.Unmarshal(request.Body, &tokenReq); err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to parse the token request", v1alpha2.BadRequest)
	}

	// Validate required fields
	if tokenReq.ClientId == "" || tokenReq.ClientSecret == "" || tokenReq.TokenEndpointUrl == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "ClientId, ClientSecret, and TokenEndpointUrl are required", v1alpha2.BadRequest),
			"Missing required fields", v1alpha2.BadRequest)
	}

	// Call DeviceManager to get token from Keycloak
	tokenData, err := c.DeviceManager.GetToken(pCtx, tokenReq.ClientId, tokenReq.ClientSecret, tokenReq.TokenEndpointUrl)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to get token from Keycloak", v1alpha2.InternalError)
	}

	// Create response
	response := TokenResponse{
		AccessToken:  tokenData.AccessToken,
		TokenType:    tokenData.TokenType,
		ExpiresIn:    tokenData.ExpiresIn,
		RefreshToken: tokenData.RefreshToken,
	}

	return createSuccessResponse(span, v1alpha2.OK, &response)
}

// Handler func for onboardDevice
func (c *DeviceVendor) onboardDevice(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Device Vendor",
		request.Context,
		&map[string]string{
			"method": "onboardDevice",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	deviceVendorLogger.InfofCtx(pCtx, "V (MargoDeviceVendor): onboardDevice, method: %s", request.Method)

	// Call DeviceManager to handle Keycloak onboarding
	onboardingData, err := c.DeviceManager.OnboardDevice(pCtx)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to onboard device", v1alpha2.InternalError)
	}

	// Create response
	response := DeviceOnboardingResponse{
		ClientId:         onboardingData.ClientId,
		ClientSecret:     onboardingData.ClientSecret,
		TokenEndpointUrl: onboardingData.TokenEndpointUrl,
	}

	return createSuccessResponse(span, v1alpha2.OK, &response)
}

func (c *DeviceVendor) pollDesiredState(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Device Vendor",
		request.Context,
		&map[string]string{
			"method": "pollDesiredState",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	deviceVendorLogger.InfofCtx(pCtx, "V (MargoDeviceVendor): pollDesiredState, method: %s, %s", request.Method, string(request.Body))

	deviceId := "device-101" // TODO: extract this from the jwt token

	// Parse request
	var syncReq margoStdSbiAPI.StateJSONRequestBody
	if err := json.Unmarshal(request.Body, &syncReq); err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to parse the request", v1alpha2.BadRequest)
	}

	// Call MargoManager to sync state
	desiredStates, err := c.DeviceManager.PollDesiredState(pCtx, deviceId, syncReq)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to sync state", v1alpha2.InternalError)
	}

	// Create success response
	return createSuccessResponse(span, v1alpha2.OK, &desiredStates)
}
