package margo

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/managers/margo"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/observability"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/pubsub"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/vendors"
	"github.com/margo/sandbox/shared-lib/mis/parser"
	margoStdSbiAPI "github.com/margo/sandbox/standard/generatedCode/wfm/sbi"
	"github.com/valyala/fasthttp"
	"gopkg.in/yaml.v2"
)

var deviceVendorLogger = logger.NewLogger("coa.runtime")

type DeviceAgentVendor struct {
	vendors.Vendor
	DeviceManager *margo.DeviceManager
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
		// GET /api/v1/deployments
		{
			Methods: []string{fasthttp.MethodGet},
			Route:   route + "/deployments",
			Version: self.Version,
			Handler: self.getDesiredManifest,
		},
		// GET /api/v1/bundles/{digest}
		{
			Methods:    []string{fasthttp.MethodGet},
			Route:      route + "/bundles/{digest}",
			Version:    self.Version,
			Handler:    self.downloadBundle,
			Parameters: []string{"digest?"},
		},
		// GET /api/v1/deployments/{deploymentId}/{digest}
		{
			Methods:    []string{fasthttp.MethodGet},
			Route:      route + "/deployments/{deploymentId}/{digest}",
			Version:    self.Version,
			Handler:    self.downloadDeployment,
			Parameters: []string{"deploymentId?", "digest?"},
		},
		// Endpoints for device capabilities
		// DELETE /api/v1/capabilities/{deviceId}
		{
			Methods: []string{fasthttp.MethodDelete},
			Route:   route + "/capabilities/{deviceId}",
			Version: self.Version,
			// Handler:    self.deleteDevice, //TODO: Update delete flow here
			Parameters: []string{"deviceId?"},
		},
		// PUT /api/v1/capabilities/{deviceId}
		{
			Methods:    []string{fasthttp.MethodPut},
			Route:      route + "/capabilities/{deviceId}",
			Version:    self.Version,
			Handler:    self.updateDeviceCapabilities,
			Parameters: []string{"deviceId?"},
		},
		// POST /api/v1/deployments/{deploymentId}/status
		{
			Methods:    []string{fasthttp.MethodPost},
			Route:      route + "/deployments/{deploymentId}/status",
			Version:    self.Version,
			Handler:    self.onDeploymentStatusUpdate,
			Parameters: []string{"deploymentId?"},
		},
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

	// Extract deviceId from URL parameters
	deviceId := request.Parameters["__deviceId"]
	if deviceId == "" {
		return problemResponse(margoStdSbiAPI.NewInvalidRequest(
            "deviceId path parameter is required",
            "/api/v1/capabilities/{deviceId}"))
	}

	deviceSpiffeId, err := ExtractPeerSpiffeID(request)
	if err != nil {
        return problemResponse(margoStdSbiAPI.NewInvalidRequest(
            fmt.Sprintf("failed to extract device spiffeId: %s", err.Error()),
            "/api/v1/capabilities/{deviceId}"))
    }

	// Parse request body using the correct DeviceCapabilities type
	var capabilities margoStdSbiAPI.DeviceCapabilitiesManifest
	 if err := json.Unmarshal(request.Body, &capabilities); err != nil {
        return problemResponse(margoStdSbiAPI.NewInvalidRequest(
            fmt.Sprintf("failed to parse device capabilities: %s", err.Error()),
            "/api/v1/capabilities/{deviceId}"))
    }

	// Validate required fields
	if capabilities.Properties.Id == "" {
        return problemResponse(margoStdSbiAPI.NewSemanticError(
            "device ID in properties is required",
            fmt.Sprintf("/api/v1/capabilities/%s", deviceId)))
    }

	// Validate deviceId matches the one in properties
	if capabilities.Properties.Id != deviceId {
		return problemResponse(margoStdSbiAPI.NewSemanticError(
			"device ID mismatch",
			fmt.Sprintf("/api/v1/capabilities/%s", deviceId)))
	}

	// Call DeviceManager to update capabilities
	// deviceid is just for residing in properties. For identity, MIAF related identity needs to be used.
	err = self.DeviceManager.UpdateDeviceCapabilities(pCtx, deviceSpiffeId, capabilities)
	 if err != nil {
        return problemResponse(margoStdSbiAPI.NewInternalError(
            fmt.Sprintf("failed to update device capabilities: %s", err.Error()),
            fmt.Sprintf("/api/v1/capabilities/%s", deviceId)))
    }

	return v1alpha2.COAResponse{
		State:       v1alpha2.Created,
		Body:        []byte(`{"message": "Device capabilities updated successfully"}`),
		ContentType: "application/json",
	}
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

	deviceClientId, err := ExtractPeerSpiffeID(request)
	if err != nil {
        return problemResponse(margoStdSbiAPI.NewInvalidRequest(
            fmt.Sprintf("failed to extract device spiffeId: %s", err.Error()),
            "/api/v1/deployments/{deploymentId}/status"))
    }

	deploymentId := request.Parameters["__deploymentId"]
 	if deploymentId == "" {
        return problemResponse(margoStdSbiAPI.NewInvalidRequest(
            "deploymentId path parameter is required",
            "/api/v1/deployments/{deploymentId}/status"))
    }

	deviceVendorLogger.InfofCtx(pCtx, "V (MargoDeviceVendor): onDeploymentStatusUpdate, method: %s, %s", request.Method, string(request.Body))
	// Parse request
	var statusReq margoStdSbiAPI.DeploymentStatusManifest
	if err := json.Unmarshal(request.Body, &statusReq); err != nil {
        return problemResponse(margoStdSbiAPI.NewInvalidRequest(
            fmt.Sprintf("failed to parse request: %s", err.Error()),
            fmt.Sprintf("/api/v1/deployments/%s/status", deploymentId)))
    }

	if err := self.validateStatusUpdateRequest(statusReq); err != nil {
		 return problemResponse(margoStdSbiAPI.NewSemanticError(
            err.Error(),
            fmt.Sprintf("/api/v1/deployments/%s/status", deploymentId)))
	}

	if err := self.DeviceManager.OnDeploymentStatus(pCtx, deviceClientId, deploymentId, string(statusReq.Status.State)); err != nil {
		 return problemResponse(margoStdSbiAPI.NewInternalError(
            fmt.Sprintf("failed to update deployment status: %s", err.Error()),
            fmt.Sprintf("/api/v1/deployments/%s/status", deploymentId)))
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
		return problemResponse(margoStdSbiAPI.NewInternalError(
            "failed to extract request headers",
            "/api/v1/deployments"))
	}

	deviceVendorLogger.InfofCtx(pCtx, "V (MargoDeviceVendor): getDesiredManifest, parsedHeaders, method: sign(%v)", headers)

	if accept := headers["accept"]; accept != "application/vnd.margo.manifest.v1+json" {
		return problemResponse(margoStdSbiAPI.NewServerCannotGenerateResponse(
            "Accept header must be application/vnd.margo.manifest.v1+json",
            "/api/v1/deployments"))
	}

	deviceClientId, err := ExtractPeerSpiffeID(request)
	if err != nil {
		return problemResponse(margoStdSbiAPI.NewInternalError(
            "failed to extract device spiffeId",
            "/api/v1/deployments"))
	}

	deviceVendorLogger.InfofCtx(pCtx, "Processing request for deviceClientId: %s", deviceClientId)

	// Fix: Use lowercase header key
	digest := headers["if-none-match"]
	deviceVendorLogger.DebugfCtx(pCtx, "If-None-Match digest: %s", digest)

	shouldReplaceBundle, _, manifest, err := self.DeviceManager.ShouldReplaceBundle(pCtx, deviceClientId, &digest)
	if err != nil {
        return problemResponse(margoStdSbiAPI.NewInternalError(
            fmt.Sprintf("failed to get desired states: %s", err.Error()),
            "/api/v1/deployments"))
    }

	if manifest == nil {
		deviceVendorLogger.ErrorfCtx(pCtx, "Manifest is nil for device %s", deviceClientId)
		return problemResponse(margoStdSbiAPI.NewInternalError(
            "manifest is nil",
            "/api/v1/deployments"))
	}

	// SPEC-COMPLIANT: Compute ETag as digest of the manifest JSON
	var etag string
	manifestVersionInt := uint64(manifest.ManifestVersion)

	if manifest.Bundle == nil {
		// Empty bundle: Compute digest of the manifest JSON (per spec)
		manifestJSON, err := json.Marshal(manifest)
		if err != nil {
			deviceVendorLogger.ErrorfCtx(pCtx, "Failed to marshal manifest for digest: %v", err)
			return problemResponse(margoStdSbiAPI.NewInternalError(fmt.Sprintf("failed to compute manifest digest: %s", err.Error()),
    "/api/v1/deployments"))
		}

		// Compute SHA-256 digest of the manifest JSON
		hash := sha256.Sum256(manifestJSON)
		etag = fmt.Sprintf("\"sha256:%x\"", hash)

		deviceVendorLogger.InfofCtx(pCtx, "Returning empty manifest for device %s - Version: %d, ETag: %s",
			deviceClientId, manifestVersionInt, etag)
	} else {
		if manifest.Bundle.Digest == nil {
			deviceVendorLogger.ErrorfCtx(pCtx, "Manifest bundle digest is nil for device %s", deviceClientId)
			return problemResponse(margoStdSbiAPI.NewInternalError("manifest bundle digest is nil",
    "/api/v1/deployments"))
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
		return problemResponse(margoStdSbiAPI.NewInternalError(fmt.Sprintf("failed to marshal manifest: %s", err.Error()),
    "/api/v1/deployments"))
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
		return problemResponse(margoStdSbiAPI.NewInternalError(
    "failed to extract request headers",
    "/api/v1/bundles/{digest}"))
	}

	// Validate Accept header (406 Not Acceptable)
	acceptedTypes := []string{
		"application/vnd.margo.bundle.v1+tar+gzip",
		"application/octet-stream",
		"*/*",
	}
	accept := headers["accept"]
	if accept != "" {
		validAccept := slices.Contains(acceptedTypes, accept)
		if !validAccept {
			return problemResponse(margoStdSbiAPI.NewServerCannotGenerateResponse(
    "Accept header must be application/vnd.margo.bundle.v1+tar+gzip",
    "/api/v1/bundles/{digest}"))
		}
	}

	deviceClientId, err := ExtractPeerSpiffeID(request)
	if err != nil {
		return problemResponse(margoStdSbiAPI.NewInvalidRequest(
    fmt.Sprintf("failed to extract device spiffeId: %s", err.Error()),
    "/api/v1/bundles/{digest}"))
	}

	requestedDigest := request.Parameters["__digest"]
	if requestedDigest == "" {
		return problemResponse(margoStdSbiAPI.NewInvalidRequest(
        "digest path parameter is required",
        "/api/v1/bundles/{digest}"))
	}

	// Extract If-None-Match header from client
	clientETag := headers["if-none-match"]

	// Get bundle from database
	path, manifest, err := self.DeviceManager.GetBundle(pCtx, deviceClientId, &requestedDigest)
	if err != nil {
		return problemResponse(margoStdSbiAPI.NewInvalidBundle(
        fmt.Sprintf("bundle not found for digest %s", requestedDigest),
        fmt.Sprintf("/api/v1/bundles/%s", requestedDigest)))
	}
	if path == "" || manifest == nil {
		return problemResponse(margoStdSbiAPI.NewInvalidBundle(
    fmt.Sprintf("bundle not found for digest %s", requestedDigest),
    fmt.Sprintf("/api/v1/bundles/%s", requestedDigest)))
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
		return problemResponse(margoStdSbiAPI.NewInternalError(
    fmt.Sprintf("failed to read bundle: %s", err.Error()),
    fmt.Sprintf("/api/v1/bundles/%s", requestedDigest)))
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
		return problemResponse(margoStdSbiAPI.NewInvalidBundle(
    fmt.Sprintf("digest mismatch: requested %s, actual %s", requestedDigest, actualDigest),
    fmt.Sprintf("/api/v1/bundles/%s", requestedDigest)))
	}

	deviceVendorLogger.InfofCtx(pCtx,
		"Serving bundle for device %s with verified digest %s (%d bytes)",
		deviceClientId, actualDigest, len(bundleData))

	// Return with proper headers
	return createSuccessResponseWithHeaders(
		span,
		"application/vnd.margo.bundle.v1+tar+gzip",
		map[string]string{
			"Content-Type":  "application/vnd.margo.bundle.v1+tar+gzip",
			"Cache-Control": "public, max-age=31536000, immutable",
			"ETag":          fmt.Sprintf("\"%s\"", actualDigest), // Quoted ETag
			"Vary":          "Accept-Encoding",
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
		return problemResponse(margoStdSbiAPI.NewInternalError(
    "failed to extract request headers",
    "/api/v1/deployments/{deploymentId}/{digest}"))
	}

	// Validate Accept header (406 Not Acceptable)
	if accept := headers["accept"]; accept != "" && accept != "application/yaml" && accept != "*/*" {
		return problemResponse(margoStdSbiAPI.NewServerCannotGenerateResponse(
    "Accept header must be application/yaml",
    "/api/v1/deployments/{deploymentId}/{digest}"))
	}

	// AFTER — no deviceId needed (deploymentId is sufficient to look up deployment)
	// remove deviceClientId entirely

	deploymentId := request.Parameters["__deploymentId"]
	if deploymentId == "" {
		return problemResponse(margoStdSbiAPI.NewInvalidRequest(
    "deploymentId path parameter is required",
    "/api/v1/deployments/{deploymentId}/{digest}"))
	}

	requestedDigest := request.Parameters["__digest"]
	if requestedDigest == "" {
		return problemResponse(margoStdSbiAPI.NewInvalidRequest(
    "digest path parameter is required",
    "/api/v1/deployments/{deploymentId}/{digest}"))
	}

	// Extract If-None-Match header from client
	clientETag := headers["if-none-match"]

	// Get deployment from database
	deployment, err := self.DeviceManager.Database.GetDeployment(pCtx, deploymentId)
	if err != nil {
		return problemResponse(margoStdSbiAPI.NewDeploymentNotFound(
    fmt.Sprintf("deployment %s not found: %s", deploymentId, err.Error()),
    fmt.Sprintf("/api/v1/deployments/%s/%s", deploymentId, requestedDigest)))
	}
	if deployment == nil {
		return problemResponse(margoStdSbiAPI.NewDeploymentNotFound(
    fmt.Sprintf("deployment %s not found", deploymentId),
    fmt.Sprintf("/api/v1/deployments/%s/%s", deploymentId, requestedDigest)))
	}

	var yamlContent []byte
	if len(deployment.DesiredState.RawYAML) > 0 {
		yamlContent = deployment.DesiredState.RawYAML
	} else {
		deviceVendorLogger.WarnfCtx(pCtx,
			"RawYAML missing for deployment %s — falling back to dynamic marshal", deploymentId)

		// Marshal to YAML (this is the "exact bytes" that will be sent)

		yamlContent, err = yaml.Marshal(deployment.DesiredState.AppDeploymentManifest)
		if err != nil {
			return problemResponse(margoStdSbiAPI.NewInternalError(
    fmt.Sprintf("failed to marshal deployment: %s", err.Error()),
    fmt.Sprintf("/api/v1/deployments/%s/%s", deploymentId, requestedDigest)))
		}
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
		return problemResponse(margoStdSbiAPI.NewDeploymentNotFound(
    fmt.Sprintf("digest mismatch: requested %s, actual %s", requestedDigest, actualDigest),
    fmt.Sprintf("/api/v1/deployments/%s/%s", deploymentId, requestedDigest)))
	}

	deviceVendorLogger.InfofCtx(pCtx,
		"Serving deployment %s with verified digest %s (%d bytes)",
		deploymentId, actualDigest, len(yamlContent))

	// Return with proper headers
	return createSuccessResponseWithHeaders(
		span,
		"application/yaml",
		map[string]string{
			"Content-Type":  "application/yaml",
			"Cache-Control": "public, max-age=31536000, immutable",
			"ETag":          fmt.Sprintf("\"%s\"", actualDigest), // Quoted ETag
			"Vary":          "Accept-Encoding",
		},
		v1alpha2.OK,
		&yamlContent,
	)
}

func (self *DeviceAgentVendor) validateStatusUpdateRequest(req margoStdSbiAPI.DeploymentStatusManifest) error {
	// validate the request fields

	if req.DeploymentId == "" {
		return fmt.Errorf("invalid deployment id: %s", req.DeploymentId)
	}

	if req.Status.State == "" ||
		(req.Status.State != margoStdSbiAPI.DeploymentStatusManifestStatusStateFailed &&
			req.Status.State != margoStdSbiAPI.DeploymentStatusManifestStatusStateInstalled &&
			req.Status.State != margoStdSbiAPI.DeploymentStatusManifestStatusStateInstalling &&
			req.Status.State != margoStdSbiAPI.DeploymentStatusManifestStatusStatePending &&
			req.Status.State != margoStdSbiAPI.DeploymentStatusManifestStatusStateRemoved &&
			req.Status.State != margoStdSbiAPI.DeploymentStatusManifestStatusStateRemoving) {
		// TODO: it is better if these validations are generated by the openapi tool
		return fmt.Errorf("invalid state: %s", req.Status.State)
	}
	return nil
}

// Create a utility function for consistent header parsing
func ParseRequestHeaders(ctx context.Context) (map[string]string, error) {
	headers := make(map[string]string)
	if httpReq, ok := ctx.Value(v1alpha2.COAFastHTTPContextKey).(*fasthttp.RequestCtx); ok {
		for _, key := range httpReq.Request.Header.PeekKeys() {
			value := httpReq.Request.Header.Peek(string(key))
			headers[strings.ToLower(string(key))] = string(value)
		}
		return headers, nil
	}
	return nil, nil
}

// ExtractTLSCertificates extracts the client (peer) certificates
// from the mTLS connection embedded in the COARequest context.
//
// Returns (clientSpiffeID, serverSpiffeID, error).
func ExtractPeerSpiffeID(request v1alpha2.COARequest) (string, error) {
	fhCtx, ok := request.Context.Value(v1alpha2.COAFastHTTPContextKey).(*fasthttp.RequestCtx)
	if !ok || fhCtx == nil {
		return "", fmt.Errorf("fasthttp context not available in request")
	}

	tlsState := fhCtx.TLSConnectionState()
	if tlsState == nil {
		return "", fmt.Errorf("TLS connection state is nil — connection may not be over mTLS")
	}

	// ----------------------------------------------------------------
	// Extract client certificate (peer certificate presented during mTLS handshake).
	// ----------------------------------------------------------------
	if len(tlsState.PeerCertificates) == 0 {
		return "", fmt.Errorf("no client certificate presented by peer")
	}
	clientCert := tlsState.PeerCertificates[0] // leaf is always index 0

	clientSpiffeID, err := parser.ParseSpiffeIdFromX509Svid(clientCert.Raw) // replace with parsed SPIFFE ID from clientCert
	if err != nil {
		return "", fmt.Errorf("failed to parse client spiffeId, err: %w", err)
	}

	return clientSpiffeID, nil
}
