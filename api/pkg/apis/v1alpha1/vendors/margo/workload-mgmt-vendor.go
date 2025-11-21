package margo

import (
	"encoding/json"

	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/managers/catalogs"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/managers/margo"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/managers/solutioncontainers"
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

var workloadMgmtVendorLogger = logger.NewLogger("coa.runtime")

type WorkloadMgmtVendor struct {
	vendors.Vendor
	AppPkgManager            *margo.AppPkgManager
	DeploymentManager        *margo.DeploymentManager
	SolutionsManager         *solutions.SolutionsManager
	SolutionContainerManager *solutioncontainers.SolutionContainersManager
	CatalogsManager          *catalogs.CatalogsManager
}

func (self *WorkloadMgmtVendor) GetInfo() vendors.VendorInfo {
	return vendors.VendorInfo{
		Version:  self.Vendor.Version,
		Name:     "MargoWorkloadVendor",
		Producer: "Margo",
	}
}

func (self *WorkloadMgmtVendor) Init(config vendors.VendorConfig, factories []managers.IManagerFactroy, providers map[string]map[string]providers.IProvider, pubsubProvider pubsub.IPubSubProvider) error {
	err := self.Vendor.Init(config, factories, providers, pubsubProvider)
	if err != nil {
		return err
	}
	for _, m := range self.Managers {
		switch c := m.(type) {
		case *margo.AppPkgManager:
			self.AppPkgManager = c
		case *margo.DeploymentManager:
			self.DeploymentManager = c
		case *solutions.SolutionsManager:
			self.SolutionsManager = c
		case *catalogs.CatalogsManager:
			self.CatalogsManager = c
		case *solutioncontainers.SolutionContainersManager:
			self.SolutionContainerManager = c
		}
	}
	if self.AppPkgManager == nil {
		return v1alpha2.NewCOAError(nil, "margo app pkg manager is not supplied", v1alpha2.MissingConfig)
	}
	if self.DeploymentManager == nil {
		return v1alpha2.NewCOAError(nil, "margo deployment manager is not supplied", v1alpha2.MissingConfig)
	}
	if self.SolutionsManager == nil {
		return v1alpha2.NewCOAError(nil, "solutions manager is not supplied", v1alpha2.MissingConfig)
	}
	if self.CatalogsManager == nil {
		return v1alpha2.NewCOAError(nil, "catalogs manager is not supplied", v1alpha2.MissingConfig)
	}
	if self.SolutionContainerManager == nil {
		return v1alpha2.NewCOAError(nil, "solutions container manager is not supplied", v1alpha2.MissingConfig)
	}
	return nil
}

func (self *WorkloadMgmtVendor) GetEndpoints() []v1alpha2.Endpoint {
	route := WorkloadMgmtInterfaceDefaultBaseURL
	if self.Route != "" {
		route = self.Route
	}
	return []v1alpha2.Endpoint{
		{
			Methods: []string{fasthttp.MethodPost},
			Route:   route + "/app-packages",
			Version: self.Version,
			Handler: self.onboardAppPkg,
		},
		{
			Methods:    []string{fasthttp.MethodGet},
			Route:      route + "/app-packages",
			Version:    self.Version,
			Handler:    self.listAppPkgs,
			Parameters: []string{"id?", "name?", "type?"},
		},
		{
			Methods:    []string{fasthttp.MethodDelete},
			Route:      route + "/app-packages",
			Version:    self.Version,
			Handler:    self.deleteAppPkg,
			Parameters: []string{"id?"},
		},
		{
			Methods: []string{fasthttp.MethodPost},
			Route:   route + "/app-deployments",
			Version: self.Version,
			Handler: self.createDeployment,
		},
		{
			Methods:    []string{fasthttp.MethodGet},
			Route:      route + "/app-deployments",
			Version:    self.Version,
			Handler:    self.listDeployments,
			Parameters: []string{"id?", "type?"},
		},
		{
			Methods:    []string{fasthttp.MethodDelete},
			Route:      route + "/app-deployments",
			Version:    self.Version,
			Handler:    self.deleteDeployment,
			Parameters: []string{"id?"},
		},
	}
}

func (self *WorkloadMgmtVendor) onboardAppPkg(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Workload Vendor",
		request.Context,
		&map[string]string{
			"method": "onboardAppPkg",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	workloadMgmtVendorLogger.InfofCtx(pCtx, "V (AppPkgMgmt): onboardAppPkg, method: %s, %s", request.Method, string(request.Body))

	// Parse request
	var appPkgReq margoNonStdAPI.ApplicationPackageManifestRequest
	if err := json.Unmarshal(request.Body, &appPkgReq); err != nil {
		return createErrorResponse(workloadMgmtVendorLogger, span, err, "Failed to parse the request", v1alpha2.BadRequest)
	}

	// Onboard app package
	appPkg, err := self.AppPkgManager.OnboardAppPkg(pCtx, appPkgReq, self.SolutionsManager, self.SolutionContainerManager, self.CatalogsManager)
	if err != nil {
		return createErrorResponse(workloadMgmtVendorLogger, span, err, "Failed to onboard the app", v1alpha2.InternalError)
	}

	// catalogState, solutionState, solutionContainerState, err := c.AppPkgManager.ConvertApplicationDescriptionToSymphony(pCtx, *appPkg, margo.ApplicationDescription{}, nil)
	// if err != nil {
	// 	return createErrorResponse(workloadVendorLogger, span, err, "Failed to convert margo app to symphony objects", v1alpha2.InternalError)
	// }

	// if err := c.CatalogsManager.UpsertState(pCtx, catalogState.ObjectMeta.Name, *catalogState); err != nil {
	// 	return createErrorResponse(workloadVendorLogger, span, err, "Failed to store the catalog", v1alpha2.InternalError)
	// }
	// if err := c.SolutionsManager.UpsertState(pCtx, solutionState.ObjectMeta.Name, *solutionState); err != nil {
	// 	return createErrorResponse(workloadVendorLogger, span, err, "Failed to store the solution", v1alpha2.InternalError)
	// }

	// if err := c.SolutionContainerManager.UpsertState(pCtx, solutionContainerState.ObjectMeta.Name, *solutionContainerState); err != nil {
	// 	return createErrorResponse(workloadVendorLogger, span, err, "Failed to store the solution container", v1alpha2.InternalError)
	// }

	// Create success response
	return createSuccessResponse(span, v1alpha2.Accepted, appPkg)
}

func (self *WorkloadMgmtVendor) listAppPkgs(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Workload Vendor",
		request.Context,
		&map[string]string{
			"method": "listAppPkgs",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	workloadMgmtVendorLogger.InfofCtx(pCtx, "V (AppPkgMgmt): listAppPkgs, method: %s", request.Method)

	appPkgs, err := self.AppPkgManager.ListAppPkgs(pCtx)
	if err != nil {
		return createErrorResponse(workloadMgmtVendorLogger, span, err, "Failed to list app packages", v1alpha2.InternalError)
	}

	return createSuccessResponse(span, v1alpha2.OK, appPkgs)
}

func (self *WorkloadMgmtVendor) getAppPkg(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Workload Vendor",
		request.Context,
		&map[string]string{
			"method": "getAppPkg",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	workloadMgmtVendorLogger.InfofCtx(pCtx, "V (AppPkgMgmt): getAppPkg, method: %s", request.Method)

	pkgId := request.Parameters["id"]
	appPkg, err := self.AppPkgManager.GetAppPkg(pCtx, pkgId)
	if err != nil {
		return createErrorResponse(workloadMgmtVendorLogger, span, err, "Failed to get the app package", v1alpha2.InternalError)
	}

	return createSuccessResponse(span, v1alpha2.OK, appPkg)
}

func (self *WorkloadMgmtVendor) deleteAppPkg(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Workload Vendor",
		request.Context,
		&map[string]string{
			"method": "deleteAppPkg",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	pkgId := request.Parameters["__id"]
	workloadMgmtVendorLogger.InfofCtx(pCtx, "V (AppPkgMgmt): deleteAppPkg, method: %s, metadata: %s, path: %s, parameters: %s", request.Method,
		pretty.Sprint(request.Metadata), pretty.Sprint(request.Route), pretty.Sprint(request.Parameters), "pkgId", pkgId)
	err := self.AppPkgManager.DeleteAppPkg(pCtx, pkgId)
	if err != nil {
		return createErrorResponse(workloadMgmtVendorLogger, span, err, "Failed to delete the app package", v1alpha2.InternalError)
	}

	return createSuccessResponse(span, v1alpha2.Accepted, (*byte)(nil))
}

func (self *WorkloadMgmtVendor) createDeployment(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Workload Vendor",
		request.Context,
		&map[string]string{
			"method": "createDeployment",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	workloadMgmtVendorLogger.InfofCtx(pCtx, "V (WorkloadMgmt): createDeployment, method: %s, %s", request.Method, string(request.Body))

	// Parse request
	var deploymentReq margoNonStdAPI.ApplicationDeploymentManifestRequest
	if err := json.Unmarshal(request.Body, &deploymentReq); err != nil {
		return createErrorResponse(workloadMgmtVendorLogger, span, err, "Failed to parse the request", v1alpha2.BadRequest)
	}

	existingAppPkg, err := self.AppPkgManager.GetAppPkg(pCtx, deploymentReq.Spec.AppPackageRef.Id)
	if err != nil {
		return createErrorResponse(workloadMgmtVendorLogger, span, err, "referenced app package doesn't exist", v1alpha2.BadRequest)
	}

	// Onboard app package
	appPkg, err := self.DeploymentManager.CreateDeployment(pCtx, deploymentReq, *existingAppPkg)
	if err != nil {
		return createErrorResponse(workloadMgmtVendorLogger, span, err, "Failed to create the app deployment", v1alpha2.InternalError)
	}

	// Create success response
	return createSuccessResponse(span, v1alpha2.Accepted, appPkg)
}

func (self *WorkloadMgmtVendor) listDeployments(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Workload Vendor",
		request.Context,
		&map[string]string{
			"method": "listDeployments",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	workloadMgmtVendorLogger.InfofCtx(pCtx, "V (WorkloadMgmt): listDeployments, method: %s", request.Method)

	deployments, err := self.DeploymentManager.ListDeployments(pCtx)
	if err != nil {
		return createErrorResponse(workloadMgmtVendorLogger, span, err, "Failed to list app deployments", v1alpha2.InternalError)
	}

	return createSuccessResponse(span, v1alpha2.OK, &deployments)
}

func (self *WorkloadMgmtVendor) deleteDeployment(request v1alpha2.COARequest) v1alpha2.COAResponse {
	pCtx, span := observability.StartSpan("Margo Workload Vendor",
		request.Context,
		&map[string]string{
			"method": "deleteDeployment",
			"route":  request.Route,
			"verb":   request.Method,
		})
	defer span.End()

	deploymentId := request.Parameters["__id"]
	workloadMgmtVendorLogger.InfofCtx(pCtx, "V (WorkloadMgmt): deleteDeployment, method: %s, metadata: %s, path: %s, parameters: %s", request.Method,
		pretty.Sprint(request.Metadata), pretty.Sprint(request.Route), pretty.Sprint(request.Parameters), "pkgId", deploymentId)
	resp, err := self.DeploymentManager.DeleteDeployment(pCtx, deploymentId)
	if err != nil {
		return createErrorResponse(workloadMgmtVendorLogger, span, err, "Failed to delete the app deployment", v1alpha2.InternalError)
	}

	return createSuccessResponse(span, v1alpha2.Accepted, resp)
}
