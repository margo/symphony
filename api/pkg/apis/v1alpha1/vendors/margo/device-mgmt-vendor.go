package margo

import (
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/managers/margo"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/observability"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/pubsub"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/vendors"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	"github.com/valyala/fasthttp"
)

var deviceMgmtVendorLogger = logger.NewLogger("coa.runtime")

type DeviceMgmtVendor struct {
	vendors.Vendor
	DeviceManager *margo.DeviceManager
}

func (self *DeviceMgmtVendor) GetInfo() vendors.VendorInfo {
	return vendors.VendorInfo{
		Version:  self.Vendor.Version,
		Name:     "MargoDeviceMgmtVendor",
		Producer: "Margo",
	}
}

func (self *DeviceMgmtVendor) Init(config vendors.VendorConfig, factories []managers.IManagerFactroy, providers map[string]map[string]providers.IProvider, pubsubProvider pubsub.IPubSubProvider) error {
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
		return v1alpha2.NewCOAError(nil, "margo device manager is not supplied", v1alpha2.MissingConfig)
	}
	return nil
}

func (self *DeviceMgmtVendor) GetEndpoints() []v1alpha2.Endpoint {
	route := DeviceMgmtInterfaceDefaultBaseURL
	if self.Route != "" {
		route = self.Route
	}
	return []v1alpha2.Endpoint{
		{
			Methods: []string{fasthttp.MethodGet},
			Route:   route + "/devices",
			Version: self.Version,
			Handler: self.listDevices,
		},
	}
}

// Handler for GET /devices/
func (self *DeviceMgmtVendor) listDevices(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Device Mgmt Vendor",
		request.Context,
		&map[string]string{
			"method": "listDevices",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	deviceMgmtVendorLogger.InfofCtx(pCtx, "V (MargoDeviceMgmtVendor): listDevices, method: %s", request.Method)

	// Call DeviceManager to list the devices
	devices, err := self.DeviceManager.ListDevices(pCtx)
	if err != nil {
		return createErrorResponse2(deviceMgmtVendorLogger, span, err, "Failed to report device capabilities", v1alpha2.InternalError)
	}

	return createSuccessResponse(span, v1alpha2.OK, &devices)
}
