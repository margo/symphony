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
			Route:   route + "/wfm/states",
			Version: o.Version,
			Handler: o.pollDesiredState,
		},
	}
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
		return createErrorResponse(deviceVendorLogger, span, err, "Failed to parse the request", v1alpha2.BadRequest)
	}

	// Call MargoManager to sync state
	desiredStates, err := c.DeviceManager.PollDesiredState(pCtx, deviceId, syncReq)
	if err != nil {
		return createErrorResponse(deviceVendorLogger, span, err, "Failed to sync state", v1alpha2.InternalError)
	}

	// Create success response
	return createSuccessResponse(span, v1alpha2.Accepted, &desiredStates)
}
