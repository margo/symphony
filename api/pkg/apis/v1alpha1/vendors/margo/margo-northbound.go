package margo

import (
	"encoding/json"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/managers/margo"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/managers/solutions"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/managers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/observability"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/pubsub"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/vendors"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	"github.com/kr/pretty"
	margoAPIModels "github.com/margo/dev-repo/sdk/api/wfm/northbound/models"
	"github.com/valyala/fasthttp"
)

var margoLog = logger.NewLogger("coa.runtime")

type MargoNorthboundVendor struct {
	vendors.Vendor
	MargoManager     *margo.MargoManager
	SolutionsManager *solutions.SolutionsManager
}

func (o *MargoNorthboundVendor) GetInfo() vendors.VendorInfo {
	return vendors.VendorInfo{
		Version:  o.Vendor.Version,
		Name:     "MargoNorthbound",
		Producer: "Margo",
	}
}

func (e *MargoNorthboundVendor) Init(config vendors.VendorConfig, factories []managers.IManagerFactroy, providers map[string]map[string]providers.IProvider, pubsubProvider pubsub.IPubSubProvider) error {
	err := e.Vendor.Init(config, factories, providers, pubsubProvider)
	if err != nil {
		return err
	}
	for _, m := range e.Managers {
		switch c := m.(type) {
		case *margo.MargoManager:
			e.MargoManager = c
		case *solutions.SolutionsManager:
			e.SolutionsManager = c
		}
	}
	if e.MargoManager == nil {
		return v1alpha2.NewCOAError(nil, "margo manager is not supplied", v1alpha2.MissingConfig)
	}
	if e.SolutionsManager == nil {
		return v1alpha2.NewCOAError(nil, "solutions manager is not supplied", v1alpha2.MissingConfig)
	}
	return nil
}

func (o *MargoNorthboundVendor) GetEndpoints() []v1alpha2.Endpoint {
	route := "margo/northbound/v1"
	if o.Route != "" {
		route = o.Route
	}
	return []v1alpha2.Endpoint{
		{
			Methods: []string{fasthttp.MethodPost},
			Route:   route + "/app-pkgs",
			Version: o.Version,
			Handler: o.onboardAppPkg,
		},
		{
			Methods:    []string{fasthttp.MethodGet},
			Route:      route + "/app-pkgs",
			Version:    o.Version,
			Handler:    o.listAppPkgs,
			Parameters: []string{"id?", "name?", "type?"},
		},
		{
			Methods:    []string{fasthttp.MethodDelete},
			Route:      route + "/app-pkgs",
			Version:    o.Version,
			Handler:    o.deleteAppPkg,
			Parameters: []string{"id?"},
		},
	}
}

func (c *MargoNorthboundVendor) onboardAppPkg(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Northbound Vendor",
		request.Context,
		&map[string]string{
			"method": "onboardAppPkg",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	margoLog.InfofCtx(pCtx, "V (MargoNorthboundVendor): onboardAppPkg, method: %s, %s", request.Method, string(request.Body))

	// Parse request
	var appPkgReq margoAPIModels.AppPkgOnboardingReq
	if err := json.Unmarshal(request.Body, &appPkgReq); err != nil {
		return createErrorResponse(span, err, "Failed to parse the request", v1alpha2.BadRequest)
	}

	// Onboard app package
	appPkg, err := c.MargoManager.OnboardAppPkg(pCtx, appPkgReq)
	if err != nil {
		return createErrorResponse(span, err, "Failed to onboard the app", v1alpha2.InternalError)
	}

	// Create success response
	return createSuccessResponse(span, v1alpha2.Accepted, appPkg)
}

func (c *MargoNorthboundVendor) listAppPkgs(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Northbound Vendor",
		request.Context,
		&map[string]string{
			"method": "listAppPkgs",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	margoLog.InfofCtx(pCtx, "V (MargoNorthboundVendor): listAppPkgs, method: %s", request.Method)

	appPkgs, err := c.MargoManager.ListAppPkgs(pCtx)
	if err != nil {
		return createErrorResponse(span, err, "Failed to list app packages", v1alpha2.InternalError)
	}

	return createSuccessResponse(span, v1alpha2.OK, appPkgs)
}

func (c *MargoNorthboundVendor) getAppPkg(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Northbound Vendor",
		request.Context,
		&map[string]string{
			"method": "getAppPkg",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	margoLog.InfofCtx(pCtx, "V (MargoNorthboundVendor): getAppPkg, method: %s", request.Method)

	pkgId := request.Parameters["id"]
	appPkg, err := c.MargoManager.GetAppPkg(pCtx, pkgId)
	if err != nil {
		return createErrorResponse(span, err, "Failed to get the app package", v1alpha2.InternalError)
	}

	return createSuccessResponse(span, v1alpha2.OK, appPkg)
}

func (c *MargoNorthboundVendor) deleteAppPkg(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Northbound Vendor",
		request.Context,
		&map[string]string{
			"method": "deleteAppPkg",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	pkgId := request.Parameters["__id"]
	margoLog.InfofCtx(pCtx, "V (MargoNorthboundVendor): deleteAppPkg, method: %s, metadata: %s, path: %s, parameters: %s", request.Method,
		pretty.Sprint(request.Metadata), pretty.Sprint(request.Route), pretty.Sprint(request.Parameters), "pkgId", pkgId)
	resp, err := c.MargoManager.DeleteAppPkg(pCtx, pkgId)
	if err != nil {
		return createErrorResponse(span, err, "Failed to delete the app package", v1alpha2.InternalError)
	}

	return createSuccessResponse(span, v1alpha2.Accepted, resp)
}
