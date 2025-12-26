package margo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/managers/margo"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/observability"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/pubsub"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/vendors"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	"github.com/margo/sandbox/non-standard/generatedCode/wfm/nbi"
	"github.com/margo/sandbox/shared-lib/crypto"
	margoStdSbiAPI "github.com/margo/sandbox/standard/generatedCode/wfm/sbi"
	"github.com/valyala/fasthttp"
	"gopkg.in/yaml.v2"
)

var deviceVendorLogger = logger.NewLogger("coa.runtime")

type DeviceAgentVendor struct {
	vendors.Vendor
	DeviceManager *margo.DeviceManager
}

// struct for the onboarding response
type DeviceOnboardingResponse struct {
	// ClientId The uuid assigned to the device client.
	ClientId string `json:"client_id"`

	// EndpointList The endpoints
	EndpointList *[]string `json:"endpoint_list,omitempty"`

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
	// if self.Route != "" {
	// 	route = self.Route
	// }
	return []v1alpha2.Endpoint{
		{
			Methods:    []string{fasthttp.MethodGet},
			Route:      route + "/clients/{clientId}/deployments",
			Version:    self.Version,
			Handler:    self.getDesiredManifest,
			Parameters: []string{"clientId?"},
		},
		{
			Methods:    []string{fasthttp.MethodGet},
			Route:      route + "/clients/{clientId}/bundles/{digest}",
			Version:    self.Version,
			Handler:    self.downloadBundle,
			Parameters: []string{"clientId?", "digest?"},
		},
		{
			Methods:    []string{fasthttp.MethodGet},
			Route:      route + "/clients/{clientId}/deployments/{deploymentId}/{digest}",
			Version:    self.Version,
			Handler:    self.downloadDeployment,
			Parameters: []string{"clientId?", "deploymentId?", "digest?"},
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
			Route:      route + "/clients/{clientId}/capabilities",
			Version:    self.Version,
			Handler:    self.saveDeviceCapabilities,
			Parameters: []string{"clientId?"},
		},
		{
			Methods:    []string{fasthttp.MethodPut},
			Route:      route + "/clients/{clientId}/capabilities",
			Version:    self.Version,
			Handler:    self.updateDeviceCapabilities,
			Parameters: []string{"clientId?"},
		},
		{
			Methods:    []string{fasthttp.MethodPost},
			Route:      route + "/clients/{clientId}/deployment/{deploymentId}/status",
			Version:    self.Version,
			Handler:    self.onDeploymentStatusUpdate,
			Parameters: []string{"clientId?", "deploymentId?"},
		},
		{
			Methods:    []string{fasthttp.MethodGet},
			Route:      route + "/onboarding/certificate",
			Version:    self.Version,
			Handler:    self.downloadServerCA,
			Parameters: []string{},
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
	var capabilities margoStdSbiAPI.DeviceCapabilitiesManifest
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

	return v1alpha2.COAResponse{
		State:       v1alpha2.Created,
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
	var capabilities margoStdSbiAPI.DeviceCapabilitiesManifest
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

	return v1alpha2.COAResponse{
		State:       v1alpha2.Created,
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

	devicePubCert, exists := onboardingRequest["public_certificate"]
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

	// Debug: List all devices to see what's in the database
	deviceVendorLogger.InfofCtx(pCtx, "DEBUG: Calling DebugListAllDevices after onboarding")
	self.DeviceManager.Database.DebugListAllDevices(pCtx)

	// Verify the specific device exists
	if device, err := self.DeviceManager.Database.GetDevice(pCtx, onboardingResult.ClientId); err != nil {
		deviceVendorLogger.ErrorfCtx(pCtx, "CRITICAL: Device %s not found immediately after onboarding: %v", onboardingResult.ClientId, err)
	} else {
		deviceVendorLogger.InfofCtx(pCtx, "VERIFIED: Device %s exists with status %s", onboardingResult.ClientId, device.OnboardingStatus)
	}

	// Create response
	response := DeviceOnboardingResponse{
		ClientId:     onboardingResult.ClientId,
		EndpointList: &[]string{},
		// ClientSecret:     onboardingResult.ClientSecret,
		// TokenEndpointUrl: onboardingResult.TokenEndpointUrl,
	}
	return createSuccessResponse(span, v1alpha2.Created, &response)
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
	var statusReq margoStdSbiAPI.DeploymentStatusManifest
	if err := json.Unmarshal(request.Body, &statusReq); err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to parse the request", v1alpha2.BadRequest)
	}

	if err := self.DeviceManager.OnDeploymentStatus(pCtx, deviceClientId, deploymentId, string(statusReq.Status.State)); err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to update the status", v1alpha2.BadRequest)
	}

	return createSuccessResponse(span, v1alpha2.Created, (*int)(nil))
}

func (self *DeviceAgentVendor) getDesiredManifest(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Device Vendor",
		request.Context,
		&map[string]string{
			"method": "getDesiredManifest",
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

	deviceVendorLogger.InfofCtx(pCtx, "V (MargoDeviceVendor): getDesiredManifest, parsedHeaders, method: sign(%v)", headers)

	if accept := headers["accept"]; accept != "application/vnd.margo.manifest.v1+json" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "The accept header should be application/vnd.margo.manifest.v1+json", v1alpha2.NotAcceptable),
			"Not Acceptable", v1alpha2.NotAcceptable)
	}

	// Access a specific header
	deviceClientId := request.Parameters["__clientId"]
	if deviceClientId == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "clientId is required", v1alpha2.BadRequest),
			"Missing deviceId parameter", v1alpha2.BadRequest)
	}

	deviceVendorLogger.InfofCtx(pCtx, "Processing request for deviceClientId: %s", deviceClientId)

	validReq, err := self.verifyRequestSignature(pCtx, deviceClientId, request)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to verify the request signature", v1alpha2.BadRequest)
	}
	if !validReq {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "request signature is invalid", v1alpha2.BadRequest),
			"Invalid Request Signature", v1alpha2.BadRequest)
	}

	// Fix: Use lowercase header key
	digest := headers["if-none-match"]
	deviceVendorLogger.DebugfCtx(pCtx, "If-None-Match digest: %s", digest)

	shouldReplaceBundle, _, manifest, err := self.DeviceManager.ShouldReplaceBundle(pCtx, deviceClientId, &digest)
	if err != nil {
		deviceVendorLogger.ErrorfCtx(pCtx, "ShouldReplaceBundle failed for device %s: %v", deviceClientId, err)
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to get the desired states", v1alpha2.InternalError)
	}

	if manifest == nil {
		deviceVendorLogger.ErrorfCtx(pCtx, "Manifest is nil for device %s", deviceClientId)
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "manifest is nil", v1alpha2.InternalError),
			"Internal server error", v1alpha2.InternalError)
	}

	// SPEC-COMPLIANT: Compute ETag as digest of the manifest JSON
	var etag string
	manifestVersionInt := uint64(manifest.ManifestVersion)

	if manifest.Bundle == nil {
		// Empty bundle: Compute digest of the manifest JSON (per spec)
		manifestJSON, err := json.Marshal(manifest)
		if err != nil {
			deviceVendorLogger.ErrorfCtx(pCtx, "Failed to marshal manifest for digest: %v", err)
			return createErrorResponse2(deviceVendorLogger, span, err, "Failed to compute manifest digest", v1alpha2.InternalError)
		}

		// Compute SHA-256 digest of the manifest JSON
		hash := sha256.Sum256(manifestJSON)
		etag = fmt.Sprintf("\"sha256:%x\"", hash)

		deviceVendorLogger.InfofCtx(pCtx, "Returning empty manifest for device %s - Version: %d, ETag: %s",
			deviceClientId, manifestVersionInt, etag)
	} else {
		if manifest.Bundle.Digest == nil {
			deviceVendorLogger.ErrorfCtx(pCtx, "Manifest bundle digest is nil for device %s", deviceClientId)
			return createErrorResponse2(deviceVendorLogger, span,
				v1alpha2.NewCOAError(nil, "manifest bundle digest is nil", v1alpha2.InternalError),
				"Internal server error", v1alpha2.InternalError)
		}

		// Bundle with deployments: Use bundle digest as ETag
		etag = fmt.Sprintf("\"%s\"", *manifest.Bundle.Digest)

		deviceVendorLogger.InfofCtx(pCtx, "Returning bundle manifest for device %s - Version: %d, Digest: %s, Deployments: %d",
			deviceClientId, manifestVersionInt, *manifest.Bundle.Digest, len(manifest.Deployments))
	}

	// Set headers directly in fasthttp context
	if fhCtx, ok := request.Context.Value(v1alpha2.COAFastHTTPContextKey).(*fasthttp.RequestCtx); ok {

		fhCtx.Response.Header.Set("ETag", etag)
		fhCtx.Response.Header.Set("Cache-Control", "public, max-age=31536000, immutable")
		fhCtx.Response.Header.Set("Content-Type", "application/vnd.margo.manifest.v1+json")

		deviceVendorLogger.InfofCtx(pCtx, "Set response headers directly - ETag: %s", etag)
	} else {
		deviceVendorLogger.WarnfCtx(pCtx, "Could not access fasthttp context to set headers")
	}

	// Check if client already has this manifest (digest matches)
	if !shouldReplaceBundle {
		deviceVendorLogger.InfofCtx(pCtx, "Bundle not modified for device %s, returning 304 - ETag: %s", deviceClientId, etag)

		// Return NotModified state - COA framework will convert to HTTP 304
		response := v1alpha2.COAResponse{
			State:       v1alpha2.NotModified,
			Body:        []byte{},
			ContentType: "application/vnd.margo.manifest.v1+json",
		}

		deviceVendorLogger.InfofCtx(pCtx, "Created 304 response - State: %v, BodyLen: %d",
			response.State, len(response.Body))

		return response
	}

	deviceVendorLogger.InfofCtx(pCtx, "Returning new manifest for device %s - ETag: %s", deviceClientId, etag)

	// Serialize manifest
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		deviceVendorLogger.ErrorfCtx(pCtx, "Failed to marshal manifest: %v", err)
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to marshal manifest", v1alpha2.InternalError)
	}

	return v1alpha2.COAResponse{
		State:       v1alpha2.OK,
		Body:        manifestJSON,
		ContentType: "application/vnd.margo.manifest.v1+json",
	}
}

func (self *DeviceAgentVendor) downloadBundle(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Device Vendor",
		request.Context,
		&map[string]string{
			"method": "downloadBundle",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	// Extract headers
	headers, err := ParseRequestHeaders(request.Context)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(err, "Failed to extract headers", v1alpha2.InternalError),
			"Internal server error", v1alpha2.InternalError)
	}

	// Validate Accept header (406 Not Acceptable)
	acceptedTypes := []string{
		"application/vnd.margo.bundle.v1+tar+gzip",
		"application/octet-stream",
		"*/*",
	}
	accept := headers["accept"]
	if accept != "" {
		validAccept := false
		for _, validType := range acceptedTypes {
			if accept == validType {
				validAccept = true
				break
			}
		}
		if !validAccept {
			return createErrorResponse2(deviceVendorLogger, span,
				v1alpha2.NewCOAError(nil,
					"Accept header must be application/vnd.margo.bundle.v1+tar+gzip",
					v1alpha2.NotAcceptable),
				"Not Acceptable", v1alpha2.NotAcceptable)
		}
	}

	// Extract and validate parameters
	deviceClientId := request.Parameters["__clientId"]
	if deviceClientId == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "clientId is required", v1alpha2.BadRequest),
			"Missing clientId parameter", v1alpha2.BadRequest)
	}

	requestedDigest := request.Parameters["__digest"]
	if requestedDigest == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "digest is required", v1alpha2.BadRequest),
			"Missing digest parameter", v1alpha2.BadRequest)
	}

	// Verify request signature
	validReq, err := self.verifyRequestSignature(pCtx, deviceClientId, request)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err,
			"Signature verification failed", v1alpha2.Unauthorized)
	}
	if !validReq {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "Invalid signature", v1alpha2.Unauthorized),
			"Signature verification failed", v1alpha2.Unauthorized)
	}

	//Extract If-None-Match header from client
	clientETag := headers["if-none-match"]

	// Get bundle from database
	path, manifest, err := self.DeviceManager.GetBundle(pCtx, deviceClientId, &requestedDigest)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err,
			"Bundle not found", v1alpha2.NotFound)
	}
	if path == "" || manifest == nil {
		return createSuccessResponseWithHeaders(span,
			"application/vnd.margo.bundle.v1+tar+gzip",
			nil,
			v1alpha2.NotFound,
			(*int)(nil),
		)
	}

	//  Check If-None-Match before reading file
	if manifest.Bundle != nil && manifest.Bundle.Digest != nil {
		serverETag := fmt.Sprintf("\"%s\"", *manifest.Bundle.Digest)

		// Normalize ETags for comparison (remove quotes)
		clientETagClean := strings.Trim(clientETag, "\"")
		serverETagClean := strings.Trim(serverETag, "\"")

		if clientETag != "" && clientETagClean == serverETagClean {
			deviceVendorLogger.InfofCtx(pCtx,
				"Bundle not modified for device %s (304) - ETag: %s",
				deviceClientId, serverETag)

			// Return 304 Not Modified
			return v1alpha2.COAResponse{
				State:       v1alpha2.NotModified,
				Body:        []byte{},
				ContentType: "application/vnd.margo.bundle.v1+tar+gzip",
			}
		}
	}

	// Read bundle archive (this is the "exact bytes" that will be sent)
	bundleData, err := os.ReadFile(path)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err,
			"Failed to read bundle", v1alpha2.InternalError)
	}

	// Verify digest of the bundle archive (Exact Bytes Rule)
	hash := sha256.Sum256(bundleData)
	actualDigest := fmt.Sprintf("sha256:%x", hash)

	if actualDigest != requestedDigest {
		deviceVendorLogger.ErrorfCtx(pCtx,
			"Bundle digest mismatch for device %s: requested=%s, actual=%s",
			deviceClientId, requestedDigest, actualDigest)

		// Per spec: "If the server cannot produce content whose digest matches this value
		// it MUST return 404 Not Found"
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil,
				fmt.Sprintf("Digest mismatch: requested %s, actual %s",
					requestedDigest, actualDigest),
				v1alpha2.NotFound),
			"Bundle not found for the given digest", v1alpha2.NotFound)
	}

	deviceVendorLogger.InfofCtx(pCtx,
		"Serving bundle for device %s with verified digest %s (%d bytes)",
		deviceClientId, actualDigest, len(bundleData))

	// Return with proper headers
	return createSuccessResponseWithHeaders(span,
		"application/vnd.margo.bundle.v1+tar+gzip",
		map[string]string{
			"Cache-Control": "public, max-age=31536000, immutable",
			"ETag":          fmt.Sprintf("\"%s\"", actualDigest), // Quoted ETag
		},
		v1alpha2.OK,
		&bundleData,
	)
}

func (self *DeviceAgentVendor) downloadDeployment(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Device Vendor",
		request.Context,
		&map[string]string{
			"method": "downloadDeployment",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	// Extract headers
	headers, err := ParseRequestHeaders(request.Context)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(err, "Failed to extract headers", v1alpha2.InternalError),
			"Internal server error", v1alpha2.InternalError)
	}

	// Validate Accept header (406 Not Acceptable)
	if accept := headers["accept"]; accept != "" && accept != "application/yaml" && accept != "*/*" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "Accept header must be application/yaml", v1alpha2.NotAcceptable),
			"Not Acceptable", v1alpha2.NotAcceptable)
	}

	// Extract and validate parameters
	deviceClientId := request.Parameters["__clientId"]
	if deviceClientId == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "clientId is required", v1alpha2.BadRequest),
			"Missing clientId parameter", v1alpha2.BadRequest)
	}

	deploymentId := request.Parameters["__deploymentId"]
	if deploymentId == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "deploymentId is required", v1alpha2.BadRequest),
			"Missing deploymentId parameter", v1alpha2.BadRequest)
	}

	requestedDigest := request.Parameters["__digest"]
	if requestedDigest == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "digest is required", v1alpha2.BadRequest),
			"Missing digest parameter", v1alpha2.BadRequest)
	}

	// Verify request signature
	validReq, err := self.verifyRequestSignature(pCtx, deviceClientId, request)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err,
			"Signature verification failed", v1alpha2.Unauthorized)
	}
	if !validReq {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "Invalid signature", v1alpha2.Unauthorized),
			"Signature verification failed", v1alpha2.Unauthorized)
	}

	// Extract If-None-Match header from client
	clientETag := headers["if-none-match"]

	// Get deployment from database
	deployment, err := self.DeviceManager.Database.GetDeployment(pCtx, deploymentId)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err,
			"Deployment not found", v1alpha2.NotFound)
	}
	if deployment == nil {
		return createSuccessResponseWithHeaders(span,
			"application/yaml",
			nil,
			v1alpha2.NotFound,
			(*int)(nil),
		)
	}

	// Marshal to YAML (this is the "exact bytes" that will be sent)
	yamlContent, err := yaml.Marshal(deployment.DesiredState.AppDeploymentManifest)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err,
			"Failed to marshal deployment", v1alpha2.InternalError)
	}

	// Compute digest of the YAML content (Exact Bytes Rule)
	hash := sha256.Sum256(yamlContent)
	actualDigest := fmt.Sprintf("sha256:%x", hash)

	// Check If-None-Match before verifying digest match
	serverETag := fmt.Sprintf("\"%s\"", actualDigest)
	clientETagClean := strings.Trim(clientETag, "\"")
	serverETagClean := strings.Trim(serverETag, "\"")

	if clientETag != "" && clientETagClean == serverETagClean {
		deviceVendorLogger.InfofCtx(pCtx,
			"Deployment not modified (304) - deploymentId: %s, ETag: %s",
			deploymentId, serverETag)

		// Return 304 Not Modified
		return v1alpha2.COAResponse{
			State:       v1alpha2.NotModified,
			Body:        []byte{},
			ContentType: "application/yaml",
		}
	}

	// Verify digest matches the requested digest
	if actualDigest != requestedDigest {
		deviceVendorLogger.ErrorfCtx(pCtx,
			"Digest mismatch for deployment %s: requested=%s, actual=%s",
			deploymentId, requestedDigest, actualDigest)

		// Per spec: "If the server cannot produce content whose digest matches this value
		// it MUST return 404 Not Found"
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil,
				fmt.Sprintf("Digest mismatch: requested %s, actual %s",
					requestedDigest, actualDigest),
				v1alpha2.NotFound),
			"Deployment not found for the given digest", v1alpha2.NotFound)
	}

	deviceVendorLogger.InfofCtx(pCtx,
		"Serving deployment %s with verified digest %s (%d bytes)",
		deploymentId, actualDigest, len(yamlContent))

	// Return with proper headers
	return createSuccessResponseWithHeaders(span,
		"application/yaml",
		map[string]string{
			"Cache-Control": "public, max-age=31536000, immutable",
			"ETag":          fmt.Sprintf("\"%s\"", actualDigest), // Quoted ETag
			"Vary":          "Accept-Encoding",
		},
		v1alpha2.OK,
		&yamlContent,
	)
}

func (self *DeviceAgentVendor) verifyRequestSignature(ctx context.Context, clientId string, request v1alpha2.COARequest) (valid bool, err error) {
	deviceClient, err := self.DeviceManager.GetDeviceClientUsingId(ctx, clientId)
	if err != nil {
		deviceVendorLogger.ErrorfCtx(ctx, "verifyRequestSignature: Failed to get device %s: %v", clientId, err)
		return false, fmt.Errorf("device %s not found: %w", clientId, err)
	}

	// Add nil check for device
	if deviceClient == nil {
		deviceVendorLogger.ErrorfCtx(ctx, "verifyRequestSignature: Device %s is nil", clientId)
		return false, fmt.Errorf("device %s not found", clientId)
	}

	// Check if device certificate exists
	if len(deviceClient.DevicePubCert) == 0 {
		deviceVendorLogger.ErrorfCtx(ctx, "verifyRequestSignature: Device %s has no public certificate", clientId)
		return false, fmt.Errorf("device public certificate is not yet available with the wfm")
	}

	verifier, err := crypto.NewVerifier(deviceClient.DevicePubCert, true)
	if err != nil {
		deviceVendorLogger.ErrorfCtx(ctx, "verifyRequestSignature: Failed to create verifier for device %s: %v", clientId, err)
		return false, fmt.Errorf("failed to verify the request using the device certificate, %s", err.Error())
	}
	httpReq, err := COARequestToHTTPRequest(request)
	if err != nil {
		deviceVendorLogger.ErrorfCtx(ctx, "verifyRequestSignature: Failed to convert request for device %s: %v", clientId, err)
		return false, fmt.Errorf("failed to parse the request, %s", err.Error())
	}

	if err := verifier.VerifyRequest(ctx, httpReq); err != nil {
		deviceVendorLogger.ErrorfCtx(ctx, "verifyRequestSignature: Signature verification failed for device %s: %v", clientId, err)
		return false, err
	}

	deviceVendorLogger.DebugfCtx(ctx, "verifyRequestSignature: Successfully verified signature for device %s", clientId)
	return true, nil
}

func (self *DeviceAgentVendor) downloadServerCA(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Device Vendor",
		request.Context,
		&map[string]string{
			"method": "downloadServerCA",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()
	deviceVendorLogger.InfofCtx(pCtx, "V (MargoDeviceVendor): downloadServerCA, method: %s", request.Method)

	ca, err := self.DeviceManager.GetServerCA(pCtx)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, fmt.Errorf("Unable to find Server CA"), "Server CA download failed", v1alpha2.InternalError)
	}
	return v1alpha2.COAResponse{
		State:       v1alpha2.OK,
		Body:        []byte(`{"certificate": "` + base64.RawURLEncoding.EncodeToString(ca) + `"}`),
		ContentType: "application/json",
	}
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
