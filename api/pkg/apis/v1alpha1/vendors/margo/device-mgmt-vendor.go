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
	"github.com/margo/sandbox/standard/generatedCode/wfm/sbi"

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
	// Call DeviceManager to list the devices
	devices, err := self.DeviceManager.ListDevices(pCtx)
	if err != nil {
		return createErrorResponse2(deviceMgmtVendorLogger, span, err, "Failed to report device capabilities", v1alpha2.InternalError)
	}

	if appPkgId == "" {
		deviceMgmtVendorLogger.InfoCtx(pCtx, "V (MargoDeviceMgmtVendor): appPkgId is empty, returning all devices")
		return createSuccessResponse(span, v1alpha2.OK, &devices)
	}

	deviceMgmtVendorLogger.InfofCtx(pCtx, "V (MargoDeviceMgmtVendor): marking devices for appPkgId: %s", appPkgId)

	// getting app package here by appId
	appPkgRow, err := self.DeviceManager.Database.GetAppPackage(pCtx, appPkgId)
	if err != nil {
		deviceMgmtVendorLogger.ErrorfCtx(pCtx, "V (MargoDeviceMgmtVendor): failed to get app package by appPkgId: %s, err: %s", appPkgId, err.Error())
		return createErrorResponse2(deviceMgmtVendorLogger, span, err, "Failed to get application package for checking eligible devices", v1alpha2.InternalError)
	}

	deviceMgmtVendorLogger.InfofCtx(pCtx, "V (MargoDeviceMgmtVendor): Printing application package here: %s", pretty.Sprint(appPkgRow))

	dc := constraints.New()
	for i, d := range devices.Items {

		devCap, err := ConvertAtoB[any, sbi.DeviceCapabilitiesManifest](d.Spec.Capabilities)
		if err != nil {
			deviceMgmtVendorLogger.ErrorfCtx(pCtx, "V (MargoDeviceMgmtVendor): failed to convert device capabilities, err: %s, rawCapabilities: %s", err.Error(), pretty.Sprint(d.Spec.Capabilities))
			return createErrorResponse2(deviceMgmtVendorLogger, span, err, "Failed to convert device capabilities for checking eligible devices", v1alpha2.InternalError)
		}

		deviceMgmtVendorLogger.InfofCtx(pCtx, "V (MargoDeviceMgmtVendor): printing converted device Capabilities here, : %s", pretty.Sprint(devCap))
		eligible := true
		for _, dp := range appPkgRow.AppDescription.DeploymentProfiles {

			if dp.DeviceConstraints == nil {
				// Deployment profile does not have device constraints present
				// no need to check anything
				deviceMgmtVendorLogger.InfoCtx(pCtx, "V (MargoDeviceMgmtVendor): app deployment profile is nil, no need to check")
				continue
			}

			devCons, err := ConvertAtoB[nbi.DeviceConstraints, sbi.DeviceConstraints](*dp.DeviceConstraints)
			if err != nil {
				deviceMgmtVendorLogger.ErrorfCtx(pCtx, "V (MargoDeviceMgmtVendor): failed to convert device constraints, err: %s, rawDeviceConstraints: %s", err.Error(), pretty.Sprint(*dp.DeviceConstraints))
				return createErrorResponse2(deviceMgmtVendorLogger, span, err, "Failed to convert device constraints for checking eligible devices", v1alpha2.InternalError)
			}
			deviceMgmtVendorLogger.InfofCtx(pCtx, "V (MargoDeviceMgmtVendor): printing converted device constraints here, : %s", pretty.Sprint(devCons))

			ok, reason, err := dc.IsDeviceEligible(devCap, devCons)
			if err != nil {
				deviceMgmtVendorLogger.ErrorfCtx(pCtx, "V (MargoDeviceMgmtVendor): failed to check device eligibility, err: %s", err.Error())
				return createErrorResponse2(deviceMgmtVendorLogger, span, err, "Failed to check for eligible devices", v1alpha2.InternalError)
			}

			if !ok {
				eligible = false
				deviceMgmtVendorLogger.WarnfCtx(pCtx, "V (MargoDeviceMgmtVendor): device is not eligible, checking for next, deviceId : %s, reason: %s", *d.Id, reason)
				break
			}

		}
		if eligible {
			deviceMgmtVendorLogger.InfoCtx(pCtx, "V (MargoDeviceMgmtVendor): device is eligible, checking for next, deviceId : %s", *d.Id)

			devices.Items[i].Eligible = pointers.Ptr(nbi.True)
			continue
		}
		devices.Items[i].Eligible = pointers.Ptr(nbi.False)
	}
	return createSuccessResponse(span, v1alpha2.OK, &devices)
}
