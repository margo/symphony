package margo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/managers/margo"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/observability"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/pubsub"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/vendors"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	"github.com/margo/dev-repo/non-standard/generatedCode/wfm/nbi"
	"github.com/margo/dev-repo/shared-lib/crypto"
	margoStdSbiAPI "github.com/margo/dev-repo/standard/generatedCode/wfm/sbi"
	"github.com/valyala/fasthttp"
)

var deviceVendorLogger = logger.NewLogger("coa.runtime")

type DeviceAgentVendor struct {
	vendors.Vendor
	DeviceManager *margo.DeviceManager
}

// struct for the onboarding response
type DeviceOnboardingResponse struct {
	// ClientId The uuid assigned to the device client.
	ClientId string `json:"clientId"`

	// EndpointList The endpoints
	EndpointList *[]string `json:"endpointList,omitempty"`

	// ClientId         string `json:"clientId"`
	// ClientSecret     string `json:"clientSecret"`
	// TokenEndpointUrl string `json:"tokenEndpointUrl"`
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

func (self *DeviceAgentVendor) GetInfo() vendors.VendorInfo {
	return vendors.VendorInfo{
		Version:  self.Vendor.Version,
		Name:     "MargoDeviceVendor",
		Producer: "Margo",
	}
}

func (self *DeviceAgentVendor) Init(config vendors.VendorConfig, factories []managers.IManagerFactroy, providers map[string]map[string]providers.IProvider, pubsubProvider pubsub.IPubSubProvider) error {
	err := self.Vendor.Init(config, factories, providers, pubsubProvider)
	if err != nil {
		return err
	}
	for _, m := range self.Managers {
		switch c := m.(type) {
		case *margo.DeviceManager:
			self.DeviceManager = c
		}
	}
	if self.DeviceManager == nil {
		return v1alpha2.NewCOAError(nil, "margo manager is not supplied", v1alpha2.MissingConfig)
	}
	return nil
}

func (self *DeviceAgentVendor) GetEndpoints() []v1alpha2.Endpoint {
	route := DeviceAgentInterfaceDefaultBaseURL
	if self.Route != "" {
		route = self.Route
	}
	return []v1alpha2.Endpoint{
		{
			Methods:    []string{fasthttp.MethodGet},
			Route:      route + "/clients/{clientId}/deployments",
			Version:    self.Version,
			Handler:    self.getAllDesiredStates,
			Parameters: []string{"clientId?"},
		},
		{
			Methods:    []string{fasthttp.MethodPost},
			Route:      route + "/client/{clientId}/wfm/state",
			Version:    self.Version,
			Handler:    self.pollDesiredState,
			Parameters: []string{"clientId?"},
		},
		{
			Methods: []string{fasthttp.MethodPost},
			Route:   route + "/onboarding",
			Version: self.Version,
			Handler: self.onboardDevice,
		},
		{
			Methods: []string{fasthttp.MethodPost},
			Route:   route + "/auth/token",
			Version: self.Version,
			Handler: self.getToken,
		},
		// Endpoints for device capabilities
		{
			Methods:    []string{fasthttp.MethodPost},
			Route:      route + "/client/{clientId}/capabilities",
			Version:    self.Version,
			Handler:    self.saveDeviceCapabilities,
			Parameters: []string{"clientId?"},
		},
		{
			Methods:    []string{fasthttp.MethodPut},
			Route:      route + "/client/{clientId}/capabilities",
			Version:    self.Version,
			Handler:    self.updateDeviceCapabilities,
			Parameters: []string{"clientId?"},
		},
		{
			Methods:    []string{fasthttp.MethodPost},
			Route:      route + "/client/{clientId}/deployment/{deploymentId}/status",
			Version:    self.Version,
			Handler:    self.onDeploymentStatusUpdate,
			Parameters: []string{"clientId?", "deploymentId?"},
		},
	}
}

// Handler for POST /device/{deviceId}/capabilities
func (self *DeviceAgentVendor) saveDeviceCapabilities(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Device Vendor",
		request.Context,
		&map[string]string{
			"method": "saveDeviceCapabilities",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	deviceVendorLogger.InfofCtx(pCtx, "V (MargoDeviceVendor): saveDeviceCapabilities, method: %s", request.Method)

	// Extract deviceId from URL parameters
	fmt.Println("<---------------Request Prameteres----------------->", request.Parameters)
	deviceClientId := request.Parameters["__clientId"]
	if deviceClientId == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "clientId is required", v1alpha2.BadRequest),
			"Missing deviceId parameter", v1alpha2.BadRequest)
	}

	validReq, err := self.verifyRequestSignature(pCtx, deviceClientId, request)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to verify the request signature", v1alpha2.BadRequest)
	}
	if !validReq {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "request signaure is invalid", v1alpha2.BadRequest),
			"Invalid Request Signature", v1alpha2.BadRequest)
	}

	// Parse request body using the correct DeviceCapabilities type
	var capabilities margoStdSbiAPI.DeviceCapabilities
	if err := json.Unmarshal(request.Body, &capabilities); err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to parse device capabilities", v1alpha2.BadRequest)
	}

	// Validate required fields
	if capabilities.Properties.Id == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "device ID in properties is required", v1alpha2.BadRequest),
			"Missing device ID in capabilities", v1alpha2.BadRequest)
	}

	// Validate deviceId matches the one in properties
	if capabilities.Properties.Id != deviceClientId {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "device ID mismatch", v1alpha2.BadRequest),
			"Device ID in URL does not match device ID in capabilities", v1alpha2.BadRequest)
	}

	// Call DeviceManager to report capabilities
	err = self.DeviceManager.SaveDeviceCapabilities(pCtx, deviceClientId, capabilities)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to report device capabilities", v1alpha2.InternalError)
	}

	// Need to Return 201 Created - currently used the 200 status code
	return v1alpha2.COAResponse{
		State:       v1alpha2.OK,
		Body:        []byte(`{"message": "Device capabilities reported successfully"}`),
		ContentType: "application/json",
	}
}

// Handler for PUT /device/{deviceId}/capabilities
func (self *DeviceAgentVendor) updateDeviceCapabilities(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Device Vendor",
		request.Context,
		&map[string]string{
			"method": "updateDeviceCapabilities",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	deviceVendorLogger.InfofCtx(pCtx, "V (MargoDeviceVendor): updateDeviceCapabilities, method: %s", request.Method)

	// Extract deviceClientId from URL parameters
	deviceClientId := request.Parameters["__clientId"]
	if deviceClientId == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "clientId is required", v1alpha2.BadRequest),
			"Missing deviceId parameter", v1alpha2.BadRequest)
	}

	validReq, err := self.verifyRequestSignature(pCtx, deviceClientId, request)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to verify the request signature", v1alpha2.BadRequest)
	}
	if !validReq {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "request signaure is invalid", v1alpha2.BadRequest),
			"Invalid Request Signature", v1alpha2.BadRequest)
	}

	// Parse request body using the correct DeviceCapabilities type
	var capabilities margoStdSbiAPI.DeviceCapabilities
	if err := json.Unmarshal(request.Body, &capabilities); err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to parse device capabilities", v1alpha2.BadRequest)
	}

	// Validate required fields
	if capabilities.Properties.Id == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "device ID in properties is required", v1alpha2.BadRequest),
			"Missing device ID in capabilities", v1alpha2.BadRequest)
	}

	// Validate deviceId matches the one in properties
	if capabilities.Properties.Id != deviceClientId {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "device ID mismatch", v1alpha2.BadRequest),
			"Device ID in URL does not match device ID in capabilities", v1alpha2.BadRequest)
	}

	// Call DeviceManager to update capabilities
	err = self.DeviceManager.UpdateDeviceCapabilities(pCtx, deviceClientId, capabilities)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to update device capabilities", v1alpha2.InternalError)
	}

	// Need to Return 201 Created - currently used the 200 status code

	return v1alpha2.COAResponse{
		State:       v1alpha2.OK,
		Body:        []byte(`{"message": "Device capabilities updated successfully"}`),
		ContentType: "application/json",
	}
}

// Handler func for getToken
func (self *DeviceAgentVendor) getToken(request v1alpha2.COARequest) v1alpha2.COAResponse {
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
	tokenData, err := self.DeviceManager.GetToken(pCtx, tokenReq.ClientId, tokenReq.ClientSecret, nil)
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
func (self *DeviceAgentVendor) onboardDevice(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Device Vendor",
		request.Context,
		&map[string]string{
			"method": "onboardDevice",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	deviceVendorLogger.InfofCtx(pCtx, "V (MargoDeviceVendor): onboardDevice, method: %s", request.Method)

	// Parse request body using the correct DeviceCapabilities type
	onboardingRequest := map[string]any{}
	if err := json.Unmarshal(request.Body, &onboardingRequest); err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to parse device onboarding request", v1alpha2.BadRequest)
	}

	devicePubCert, exists := onboardingRequest["publicCertificate"]
	if !exists {
		err := fmt.Errorf("device pub cert must be passed in the request and should be non-empty value")
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to onboard device", v1alpha2.BadRequest)
	}
	if len(devicePubCert.(string)) == 0 {
		err := fmt.Errorf("device pub cert must be non-empty value")
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to onboard device", v1alpha2.BadRequest)
	}

	device, deviceSignatureExists, err := self.DeviceManager.Database.DevicePubCertExists(pCtx, devicePubCert.(string))
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to check if device pub cert already onboarded", v1alpha2.InternalError)
	}

	if deviceSignatureExists && (device.OnboardingStatus == nbi.ONBOARDED || device.OnboardingStatus == nbi.INPROGRESS) {
		return createErrorResponse2(deviceVendorLogger, span, fmt.Errorf("Device signature already exists"), "Device onboarding denied", v1alpha2.Conflict)
	}

	onboardingResult, err := self.DeviceManager.OnboardDevice(pCtx, devicePubCert.(string))
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to onboard device", v1alpha2.InternalError)
	}

	// Create response
	response := DeviceOnboardingResponse{
		ClientId:     onboardingResult.ClientId,
		EndpointList: &[]string{},
		// ClientSecret:     onboardingResult.ClientSecret,
		// TokenEndpointUrl: onboardingResult.TokenEndpointUrl,
	}
	return createSuccessResponse(span, v1alpha2.OK, &response)
}

func (self *DeviceAgentVendor) pollDesiredState(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Device Vendor",
		request.Context,
		&map[string]string{
			"method": "pollDesiredState",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	// Extract the fasthttp request from the context
	headers, err := ParseRequestHeaders(request.Context)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(err, "Failed to extract fasthttp request from context", v1alpha2.InternalError),
			"Internal server error", v1alpha2.InternalError)
	}

	deviceVendorLogger.InfofCtx(pCtx, "V (MargoDeviceVendor): pollDesiredState, parsedHeaders, method: sign(%v)", headers)

	// Access a specific header
	deviceClientId := request.Parameters["__clientId"]
	if deviceClientId == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "clientId is required", v1alpha2.BadRequest),
			"Missing deviceId parameter", v1alpha2.BadRequest)
	}

	validReq, err := self.verifyRequestSignature(pCtx, deviceClientId, request)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to verify the request signature", v1alpha2.BadRequest)
	}
	if !validReq {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "request signaure is invalid", v1alpha2.BadRequest),
			"Invalid Request Signature", v1alpha2.BadRequest)
	}

	// Parse request
	deviceVendorLogger.InfofCtx(pCtx, "V (MargoDeviceVendor): pollDesiredState, method: %s, %s, %s, %s", request.Method, string(request.Body), request.Metadata, request.Context.Value("deviceId"), request.Context.Value("X-DEVICE-SIGNATURE"))
	var syncReq margoStdSbiAPI.StateJSONRequestBody
	if err := json.Unmarshal(request.Body, &syncReq); err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to parse the request", v1alpha2.BadRequest)
	}

	// device, err := self.DeviceManager.GetDeviceClientUsingId(pCtx, deviceClientId)
	// if err != nil {
	// 	return createErrorResponse2(deviceVendorLogger, span, err, "No device found with the given signature", v1alpha2.InternalError)
	// }

	// Call MargoManager to sync state
	desiredStates, err := self.DeviceManager.PollDesiredState(pCtx, deviceClientId, syncReq)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to sync state", v1alpha2.InternalError)
	}

	// Create success response
	return createSuccessResponse(span, v1alpha2.OK, &desiredStates)
}

func (self *DeviceAgentVendor) onDeploymentStatusUpdate(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Device Vendor",
		request.Context,
		&map[string]string{
			"method": "onDeploymentStatusUpdate",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	deviceClientId := request.Parameters["__clientId"]
	if deviceClientId == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "clientId is required", v1alpha2.BadRequest),
			"Missing deviceId parameter", v1alpha2.BadRequest)
	}
	deploymentId := request.Parameters["__deploymentId"]
	if deploymentId == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "deploymentId is required", v1alpha2.BadRequest),
			"Missing deploymentId parameter", v1alpha2.BadRequest)
	}

	validReq, err := self.verifyRequestSignature(pCtx, deviceClientId, request)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to verify the request signature", v1alpha2.BadRequest)
	}
	if !validReq {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "request signaure is invalid", v1alpha2.BadRequest),
			"Invalid Request Signature", v1alpha2.BadRequest)
	}

	deviceVendorLogger.InfofCtx(pCtx, "V (MargoDeviceVendor): onDeploymentStatusUpdate, method: %s, %s", request.Method, string(request.Body))
	// Parse request
	var statusReq margoStdSbiAPI.DeploymentStatus
	if err := json.Unmarshal(request.Body, &statusReq); err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to parse the request", v1alpha2.BadRequest)
	}

	if err := self.DeviceManager.OnDeploymentStatus(pCtx, deviceClientId, deploymentId, string(statusReq.Status.State)); err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to update the status", v1alpha2.BadRequest)
	}

	return createSuccessResponse(span, v1alpha2.Created, (*int)(nil))
}

func (self *DeviceAgentVendor) verifyRequestSignature(ctx context.Context, clientId string, request v1alpha2.COARequest) (valid bool, err error) {
	deviceClient, err := self.DeviceManager.GetDeviceClientUsingId(ctx, clientId)
	if len(deviceClient.DevicePubCert) == 0 {
		return false, fmt.Errorf("device public certificate is not yet available with the wfm")
	}

	verifier, err := crypto.NewVerifier(deviceClient.DevicePubCert, true)
	if err != nil {
		// TODO: add proper logs over here
		return false, fmt.Errorf("failed to verify the request using the device certificate, %s", err.Error())
	}

	httpReq, err := COARequestToHTTPRequest(request)
	if err != nil {
		return false, fmt.Errorf("failed to parse the request, %s", err.Error())
	}

	if err := verifier.VerifyRequest(ctx, httpReq); err != nil {
		return false, err
	}
	return true, nil
}

// Create a utility function for consistent header parsing
func ParseRequestHeaders(ctx context.Context) (map[string]string, error) {
	headers := make(map[string]string)
	if httpReq, ok := ctx.Value((v1alpha2.COAFastHTTPContextKey)).(*fasthttp.RequestCtx); ok {
		for _, key := range httpReq.Request.Header.PeekKeys() {
			value := httpReq.Request.Header.Peek(string(key))
			headers[strings.ToLower(string(key))] = string(value)
		}
		return headers, nil
	}
	return nil, nil
}

// COARequestToHTTPRequest converts a COARequest to *http.Request (best-effort).
// If the fasthttp.RequestCtx is present in the COARequest.Context (v1alpha2.COAFastHTTPContextKey)
// this preserves headers and the exact request URI. Otherwise builds a request using Route,
// Parameters and Body. Some fields (RemoteAddr, TLS info, RequestURI internals) cannot be reconstructed.
func COARequestToHTTPRequest(cr v1alpha2.COARequest) (*http.Request, error) {
	// prefer fasthttp.RequestCtx when available
	if fhCtx, ok := cr.Context.Value(v1alpha2.COAFastHTTPContextKey).(*fasthttp.RequestCtx); ok {
		scheme := "http"
		if fhCtx.IsTLS() {
			scheme = "https"
		}
		host := string(fhCtx.Request.Host())
		uri := string(fhCtx.RequestURI())
		full := scheme + "://" + host + uri

		body := io.NopCloser(bytes.NewReader(cr.Body))
		r, err := http.NewRequest(cr.Method, full, body)
		if err != nil {
			return nil, err
		}

		// copy headers
		fhCtx.Request.Header.VisitAll(func(k, v []byte) {
			r.Header.Add(string(k), string(v))
		})

		// best-effort: fill remote addr
		if addr := fhCtx.RemoteAddr(); addr != nil {
			r.RemoteAddr = addr.String()
		}
		return r, nil
	}

	// fallback: build from Route + Parameters + headers via ParseRequestHeaders
	u := &url.URL{Path: cr.Route}
	q := u.Query()
	for k, v := range cr.Parameters {
		if v != "" {
			q.Set(k, v)
		}
	}
	u.RawQuery = q.Encode()

	body := io.NopCloser(bytes.NewReader(cr.Body))
	r, err := http.NewRequest(cr.Method, u.String(), body)
	if err != nil {
		return nil, err
	}

	if headers, _ := ParseRequestHeaders(cr.Context); headers != nil {
		for k, v := range headers {
			r.Header.Set(k, v)
		}
	}

	return r, nil
}

func (self *DeviceAgentVendor) getAllDesiredStates(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Device Vendor",
		request.Context,
		&map[string]string{
			"method": "getAllDesiredStates",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	// Extract the fasthttp request from the context
	headers, err := ParseRequestHeaders(request.Context)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(err, "Failed to extract fasthttp request from context", v1alpha2.InternalError),
			"Internal server error", v1alpha2.InternalError)
	}

	deviceVendorLogger.InfofCtx(pCtx, "V (MargoDeviceVendor): getAllDesiredStates, parsedHeaders, method: sign(%v)", headers)

	if accept := headers["Accept"]; accept != "application/vnd.margo.manifest.v1+json" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(err, "The accept encoding should be application/vnd.margo.manifest.v1+json", v1alpha2.BadRequest),
			"Bad Request", v1alpha2.BadRequest)
	}

	// Access a specific header
	deviceClientId := request.Parameters["__clientId"]
	if deviceClientId == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "clientId is required", v1alpha2.BadRequest),
			"Missing deviceId parameter", v1alpha2.BadRequest)
	}

	validReq, err := self.verifyRequestSignature(pCtx, deviceClientId, request)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to verify the request signature", v1alpha2.BadRequest)
	}
	if !validReq {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "request signaure is invalid", v1alpha2.BadRequest),
			"Invalid Request Signature", v1alpha2.BadRequest)
	}

	// device, err := self.DeviceManager.GetDeviceClientUsingId(pCtx, deviceClientId)
	// if err != nil {
	// 	return createErrorResponse2(deviceVendorLogger, span, err, "No device found with the given signature", v1alpha2.InternalError)
	// }

	digest := headers["If-None-Match"]
	hasDigestChanged, path, err := self.DeviceManager.FetchBundle(pCtx, deviceClientId, &digest)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to sync state", v1alpha2.InternalError)
	}

	// set headers
	// Content-Type:
	// ETag:

	if !hasDigestChanged {
		// return that nothing has changed
		return createSuccessResponse(span, v1alpha2.NotModified, (*int)(nil))
	}

	// Create success response
	return createSuccessResponse(span, v1alpha2.OK, &desiredStates)
}
