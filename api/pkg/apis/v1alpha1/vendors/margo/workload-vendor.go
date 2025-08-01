package margo

import (
	"encoding/json"
	"fmt"

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
	margoNonStdAPI "github.com/margo/dev-repo/non-standard/generatedCode/wfm/nbi"
	"github.com/valyala/fasthttp"
)

var workloadVendorLogger = logger.NewLogger("coa.runtime")

type WorkloadVendor struct {
	vendors.Vendor
	AppPkgManager     *margo.AppPkgManager
	DeploymentManager *margo.DeploymentManager
	SolutionsManager  *solutions.SolutionsManager
}

func (o *WorkloadVendor) GetInfo() vendors.VendorInfo {
	return vendors.VendorInfo{
		Version:  o.Vendor.Version,
		Name:     "MargoWorkloadVendor",
		Producer: "Margo",
	}
}

func (e *WorkloadVendor) Init(config vendors.VendorConfig, factories []managers.IManagerFactroy, providers map[string]map[string]providers.IProvider, pubsubProvider pubsub.IPubSubProvider) error {
	err := e.Vendor.Init(config, factories, providers, pubsubProvider)
	if err != nil {
		return err
	}
	for _, m := range e.Managers {
		switch c := m.(type) {
		case *margo.AppPkgManager:
			e.AppPkgManager = c
		case *margo.DeploymentManager:
			e.DeploymentManager = c
		case *solutions.SolutionsManager:
			e.SolutionsManager = c
		}
	}
	if e.AppPkgManager == nil {
		return v1alpha2.NewCOAError(nil, "margo app pkg manager is not supplied", v1alpha2.MissingConfig)
	}
	if e.AppPkgManager == nil {
		return v1alpha2.NewCOAError(nil, "margo deployment manager is not supplied", v1alpha2.MissingConfig)
	}
	if e.SolutionsManager == nil {
		return v1alpha2.NewCOAError(nil, "solutions manager is not supplied", v1alpha2.MissingConfig)
	}
	return nil
}

func (o *WorkloadVendor) GetEndpoints() []v1alpha2.Endpoint {
	route := WorkloadMgmtDefaultBaseURL
	if o.Route != "" {
		route = o.Route
	}
	return []v1alpha2.Endpoint{
		{
			Methods: []string{fasthttp.MethodPost},
			Route:   route + "/app-packages",
			Version: o.Version,
			Handler: o.onboardAppPkg,
		},
		{
			Methods:    []string{fasthttp.MethodGet},
			Route:      route + "/app-packages",
			Version:    o.Version,
			Handler:    o.listAppPkgs,
			Parameters: []string{"id?", "name?", "type?"},
		},
		{
			Methods:    []string{fasthttp.MethodDelete},
			Route:      route + "/app-packages",
			Version:    o.Version,
			Handler:    o.deleteAppPkg,
			Parameters: []string{"id?"},
		},
		{
			Methods: []string{fasthttp.MethodPost},
			Route:   route + "/app-deployments",
			Version: o.Version,
			Handler: o.createDeployment,
		},
		{
			Methods:    []string{fasthttp.MethodGet},
			Route:      route + "/app-deployments",
			Version:    o.Version,
			Handler:    o.listDeployments,
			Parameters: []string{"id?", "type?"},
		},
		{
			Methods:    []string{fasthttp.MethodDelete},
			Route:      route + "/app-deployments",
			Version:    o.Version,
			Handler:    o.deleteDeployment,
			Parameters: []string{"id?"},
		},
	}
}

func (c *WorkloadVendor) onboardAppPkg(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Workload Vendor",
		request.Context,
		&map[string]string{
			"method": "onboardAppPkg",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	workloadVendorLogger.InfofCtx(pCtx, "V (AppPkgMgmt): onboardAppPkg, method: %s, %s", request.Method, string(request.Body))

	// Parse request
	var appPkgReq margoNonStdAPI.ApplicationPackageRequest
	if err := json.Unmarshal(request.Body, &appPkgReq); err != nil {
		return createErrorResponse(workloadVendorLogger, span, err, "Failed to parse the request", v1alpha2.BadRequest)
	}

	// Onboard app package
	appPkg, err := c.AppPkgManager.OnboardAppPkg(pCtx, appPkgReq)
	if err != nil {
		return createErrorResponse(workloadVendorLogger, span, err, "Failed to onboard the app", v1alpha2.InternalError)
	}

	// Create success response
	return createSuccessResponse(span, v1alpha2.Accepted, appPkg)
}

func (c *WorkloadVendor) listAppPkgs(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Workload Vendor",
		request.Context,
		&map[string]string{
			"method": "listAppPkgs",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	workloadVendorLogger.InfofCtx(pCtx, "V (AppPkgMgmt): listAppPkgs, method: %s", request.Method)

	appPkgs, err := c.AppPkgManager.ListAppPkgs(pCtx)
	if err != nil {
		return createErrorResponse(workloadVendorLogger, span, err, "Failed to list app packages", v1alpha2.InternalError)
	}

	fmt.Println("-------", pretty.Sprint(appPkgs), "---------------------------------------")
	return createSuccessResponse(span, v1alpha2.OK, appPkgs)
}

func (c *WorkloadVendor) getAppPkg(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Workload Vendor",
		request.Context,
		&map[string]string{
			"method": "getAppPkg",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	workloadVendorLogger.InfofCtx(pCtx, "V (AppPkgMgmt): getAppPkg, method: %s", request.Method)

	pkgId := request.Parameters["id"]
	appPkg, err := c.AppPkgManager.GetAppPkg(pCtx, pkgId)
	if err != nil {
		return createErrorResponse(workloadVendorLogger, span, err, "Failed to get the app package", v1alpha2.InternalError)
	}

	return createSuccessResponse(span, v1alpha2.OK, appPkg)
}

func (c *WorkloadVendor) deleteAppPkg(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Workload Vendor",
		request.Context,
		&map[string]string{
			"method": "deleteAppPkg",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	pkgId := request.Parameters["__id"]
	workloadVendorLogger.InfofCtx(pCtx, "V (AppPkgMgmt): deleteAppPkg, method: %s, metadata: %s, path: %s, parameters: %s", request.Method,
		pretty.Sprint(request.Metadata), pretty.Sprint(request.Route), pretty.Sprint(request.Parameters), "pkgId", pkgId)
	resp, err := c.AppPkgManager.DeleteAppPkg(pCtx, pkgId)
	if err != nil {
		return createErrorResponse(workloadVendorLogger, span, err, "Failed to delete the app package", v1alpha2.InternalError)
	}

	return createSuccessResponse(span, v1alpha2.Accepted, resp)
}

func (c *WorkloadVendor) createDeployment(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Workload Vendor",
		request.Context,
		&map[string]string{
			"method": "createDeployment",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	workloadVendorLogger.InfofCtx(pCtx, "V (WorkloadMgmt): createDeployment, method: %s, %s", request.Method, string(request.Body))

	// Parse request
	var deploymentReq margoNonStdAPI.ApplicationDeploymentRequest
	if err := json.Unmarshal(request.Body, &deploymentReq); err != nil {
		return createErrorResponse(workloadVendorLogger, span, err, "Failed to parse the request", v1alpha2.BadRequest)
	}

	// Onboard app package
	appPkg, err := c.DeploymentManager.CreateDeployment(pCtx, deploymentReq)
	if err != nil {
		return createErrorResponse(workloadVendorLogger, span, err, "Failed to create the app deployment", v1alpha2.InternalError)
	}

	// Create success response
	return createSuccessResponse(span, v1alpha2.Accepted, appPkg)
}

func (c *WorkloadVendor) listDeployments(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Workload Vendor",
		request.Context,
		&map[string]string{
			"method": "listDeployments",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	workloadVendorLogger.InfofCtx(pCtx, "V (WorkloadMgmt): listDeployments, method: %s", request.Method)

	deployments, err := c.DeploymentManager.ListDeployments(pCtx)
	if err != nil {
		return createErrorResponse(workloadVendorLogger, span, err, "Failed to list app deployments", v1alpha2.InternalError)
	}

	return createSuccessResponse(span, v1alpha2.OK, deployments)
}

func (c *WorkloadVendor) deleteDeployment(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Workload Vendor",
		request.Context,
		&map[string]string{
			"method": "deleteDeployment",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	pkgId := request.Parameters["__id"]
	workloadVendorLogger.InfofCtx(pCtx, "V (WorkloadMgmt): deleteDeployment, method: %s, metadata: %s, path: %s, parameters: %s", request.Method,
		pretty.Sprint(request.Metadata), pretty.Sprint(request.Route), pretty.Sprint(request.Parameters), "pkgId", pkgId)
	resp, err := c.DeploymentManager.DeleteDeployment(pCtx, pkgId)
	if err != nil {
		return createErrorResponse(workloadVendorLogger, span, err, "Failed to delete the app deployment", v1alpha2.InternalError)
	}

	return createSuccessResponse(span, v1alpha2.Accepted, resp)
}
