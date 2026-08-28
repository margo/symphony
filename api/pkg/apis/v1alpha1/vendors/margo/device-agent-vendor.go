package margo

import (
    "context"
    "crypto/sha256"
    "encoding/json"
    "fmt"
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
            Methods:    []string{fasthttp.MethodDelete},
            Route:      route + "/capabilities/{deviceId}",
            Version:    self.Version,
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
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "deviceId is required", v1alpha2.BadRequest),
			"Missing deviceId parameter", v1alpha2.BadRequest)
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
	if capabilities.Properties.Id != deviceId {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "device ID mismatch", v1alpha2.BadRequest),
			"Device ID in URL does not match device ID in capabilities", v1alpha2.BadRequest)
	}

	// Call DeviceManager to update capabilities
	err := self.DeviceManager.UpdateDeviceCapabilities(pCtx, deviceId, capabilities)
	if err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to update device capabilities", v1alpha2.InternalError)
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


	deploymentId := request.Parameters["__deploymentId"]
	if deploymentId == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "deploymentId is required", v1alpha2.BadRequest),
			"Missing deploymentId parameter", v1alpha2.BadRequest)
	}


	deviceVendorLogger.InfofCtx(pCtx, "V (MargoDeviceVendor): onDeploymentStatusUpdate, method: %s, %s", request.Method, string(request.Body))
	// Parse request
	var statusReq margoStdSbiAPI.DeploymentStatusManifest
	if err := json.Unmarshal(request.Body, &statusReq); err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to parse the request", v1alpha2.BadRequest)
	}

	if err := self.validateStatusUpdateRequest(statusReq); err != nil {
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to update deployment status", v1alpha2.BadRequest)
	}

	// Temporary workaround: Extract deviceId from request body if available, otherwise use empty string( need to figureout with MIAF)
	deviceId := ""
	if statusReq.DeviceId != nil {
    	deviceId = string(*statusReq.DeviceId)
	}
	if err := self.DeviceManager.OnDeploymentStatus(pCtx, deviceId, deploymentId, string(statusReq.Status.State)); err != nil {
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


	// deviceId from query param or TODO: from mTLS SPIFFE ID
	// TODO: MIAF SUP — extract deviceId from mTLS client certificate SPIFFE ID
	deviceId := request.Parameters["deviceId"] // temporary PoC workaround

	deviceVendorLogger.InfofCtx(pCtx, "Processing request for deviceClientId: %s", deviceId)



	// Fix: Use lowercase header key
	digest := headers["if-none-match"]
	deviceVendorLogger.DebugfCtx(pCtx, "If-None-Match digest: %s", digest)

	shouldReplaceBundle, _, manifest, err := self.DeviceManager.ShouldReplaceBundle(pCtx, deviceId, &digest)
	if err != nil {
		deviceVendorLogger.ErrorfCtx(pCtx, "ShouldReplaceBundle failed for device %s: %v", deviceId, err)
		return createErrorResponse2(deviceVendorLogger, span, err, "Failed to get the desired states", v1alpha2.InternalError)
	}

	if manifest == nil {
		deviceVendorLogger.ErrorfCtx(pCtx, "Manifest is nil for device %s", deviceId)
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
			deviceId, manifestVersionInt, etag)
	} else {
		if manifest.Bundle.Digest == nil {
			deviceVendorLogger.ErrorfCtx(pCtx, "Manifest bundle digest is nil for device %s", deviceId)
			return createErrorResponse2(deviceVendorLogger, span,
				v1alpha2.NewCOAError(nil, "manifest bundle digest is nil", v1alpha2.InternalError),
				"Internal server error", v1alpha2.InternalError)
		}

		// Bundle with deployments: Use bundle digest as ETag
		etag = fmt.Sprintf("\"%s\"", *manifest.Bundle.Digest)

		deviceVendorLogger.InfofCtx(pCtx, "Returning bundle manifest for device %s - Version: %d, Digest: %s, Deployments: %d",
			deviceId, manifestVersionInt, *manifest.Bundle.Digest, len(manifest.Deployments))
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
		deviceVendorLogger.InfofCtx(pCtx, "Bundle not modified for device %s, returning 304 - ETag: %s", deviceId, etag)

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

	deviceVendorLogger.InfofCtx(pCtx, "Returning new manifest for device %s - ETag: %s", deviceId, etag)

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
	// AFTER — PoC: deviceId from query param (TODO: MIAF — from mTLS SPIFFE ID)
	deviceId := request.Parameters["deviceId"] // query param workaround
	if deviceId == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "deviceId is required", v1alpha2.BadRequest),
			"Missing deviceId parameter", v1alpha2.BadRequest)
	}

	requestedDigest := request.Parameters["__digest"]
	if requestedDigest == "" {
		return createErrorResponse2(deviceVendorLogger, span,
			v1alpha2.NewCOAError(nil, "digest is required", v1alpha2.BadRequest),
			"Missing digest parameter", v1alpha2.BadRequest)
	}


	//Extract If-None-Match header from client
	clientETag := headers["if-none-match"]

	// Get bundle from database
	path, manifest, err := self.DeviceManager.GetBundle(pCtx, deviceId, &requestedDigest)
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
				deviceId, serverETag)

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
			deviceId, requestedDigest, actualDigest)

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
		deviceId, actualDigest, len(bundleData))

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

	// AFTER — no deviceId needed (deploymentId is sufficient to look up deployment)
// remove deviceClientId entirely
	
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
	if httpReq, ok := ctx.Value((v1alpha2.COAFastHTTPContextKey)).(*fasthttp.RequestCtx); ok {
		for _, key := range httpReq.Request.Header.PeekKeys() {
			value := httpReq.Request.Header.Peek(string(key))
			headers[strings.ToLower(string(key))] = string(value)
		}
		return headers, nil
	}
	return nil, nil
}

