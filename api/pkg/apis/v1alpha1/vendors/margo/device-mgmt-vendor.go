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
	"github.com/kr/pretty"
	"github.com/margo/sandbox/non-standard/generatedCode/wfm/nbi"
	"github.com/margo/sandbox/shared-lib/constraints"
	"github.com/margo/sandbox/shared-lib/pointers"
	//"github.com/margo/sandbox/standard/generatedCode/wfm/sbi"

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

    appPkgId := request.Parameters["appPackageId"]
    devices, err := self.DeviceManager.ListDevices(pCtx)
    if err != nil {
        return createErrorResponse2(deviceMgmtVendorLogger, span, err, "Failed to list devices", v1alpha2.InternalError)
    }

    if appPkgId == "" {
        deviceMgmtVendorLogger.InfoCtx(pCtx, "V (MargoDeviceMgmtVendor): appPkgId is empty, returning all devices")
        return createSuccessResponse(span, v1alpha2.OK, &devices)
    }

    deviceMgmtVendorLogger.InfofCtx(pCtx, "V (MargoDeviceMgmtVendor): marking devices for appPkgId: %s", appPkgId)

    appPkgRow, err := self.DeviceManager.Database.GetAppPackage(pCtx, appPkgId)
    if err != nil {
        deviceMgmtVendorLogger.ErrorfCtx(pCtx, "V (MargoDeviceMgmtVendor): failed to get app package by appPkgId: %s, err: %s", appPkgId, err.Error())
        return createErrorResponse2(deviceMgmtVendorLogger, span, err, "Failed to get application package for checking eligible devices", v1alpha2.InternalError)
    }

    dc := constraints.New()
    for i, d := range devices.Items {
        // Capabilities are stored in the DB (SBI layer), not in NBI DeviceSpec
        devCapPtr, err := self.DeviceManager.GetDeviceCapabilities(pCtx, *d.Id)
        if err != nil || devCapPtr == nil {
            deviceMgmtVendorLogger.WarnfCtx(pCtx, "V (MargoDeviceMgmtVendor): failed to get capabilities for device %s, marking unknown: %v", *d.Id, err)
            devices.Items[i].Eligible = pointers.Ptr(nbi.Unknown)
            continue
        }
        devCap := *devCapPtr

        deviceMgmtVendorLogger.InfofCtx(pCtx, "V (MargoDeviceMgmtVendor): device capabilities: %s", pretty.Sprint(devCap))

        eligible := true
        unknown := true

        if len(appPkgRow.AppDescription.DeploymentProfiles) > 0 {
			// DeviceConstraints removed from AppDeploymentProfile in new NBI spec.
			// Eligibility based on characteristics matching is not yet implemented.
			// Mark as unknown until characteristics-based eligibility is supported.
			deviceMgmtVendorLogger.InfoCtx(pCtx, "V (MargoDeviceMgmtVendor): DeviceConstraints removed from NBI spec, skipping constraint check")
			_ = dc
			_ = eligible
		}


        if unknown {
            deviceMgmtVendorLogger.InfoCtx(pCtx, "V (MargoDeviceMgmtVendor): device eligibility unknown (no constraints to evaluate)")
            devices.Items[i].Eligible = pointers.Ptr(nbi.Unknown)
            continue
        }

        if eligible {
            devices.Items[i].Eligible = pointers.Ptr(nbi.True)
            continue
        }
        devices.Items[i].Eligible = pointers.Ptr(nbi.False)
    }

    return createSuccessResponse(span, v1alpha2.OK, &devices)
}

