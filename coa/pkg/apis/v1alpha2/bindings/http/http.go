/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 * SPDX-License-Identifier: MIT
 */

package http

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	v1alpha2 "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/margo/mis/trustbundle"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/certs"
	autogen "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/certs/autogen"
	localfile "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/certs/localfile"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/pubsub"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/utils"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger/contexts"
	routing "github.com/fasthttp/router"
	"github.com/margo/sandbox/shared-lib/mis/mtls"
	"github.com/margo/sandbox/shared-lib/mis/parser"
	"github.com/margo/sandbox/shared-lib/mis/validators"

	"github.com/valyala/fasthttp"
)

// MiddlewareConfig configures a HTTP middleware.
type MiddlewareConfig struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
}

type CertProviderConfig struct {
	Type   string                    `json:"type"`
	Config providers.IProviderConfig `json:"config"`
}

// HttpBindingConfig configures a HttpBinding.
type HttpBindingConfig struct {
	Port         int                `json:"port"`
	Pipeline     []MiddlewareConfig `json:"pipeline"`
	TLS          bool               `json:"tls"`
	MTLS         bool               `json:"mtls"`
	MIAF         *MIAF              `json:"miaf"`
	CertProvider CertProviderConfig `json:"certProvider"`
}

// TrustBundle represents the operator provided SPIFFE trust bundle configuration.
type TrustBundle struct {
	URI  string `json:"uri"`
	Path string `json:"path"`
}

// MIS represents the Margo Identity Service configuration.
type MIS struct {
	Endpoint      string      `json:"endpoint"`
	CAPath        string      `json:"caPath"`
	CacheInterval int         `json:"cacheInterval"`
	TrustDomain   string      `json:"trustDomain"`
	TrustBundle   TrustBundle `json:"trustBundle"`
}

// MIAF represents the Margo Identity and Authorization Framework configuration.
type MIAF struct {
	MIS       *MIS   `json:"mis"`
	AuthzPath string `json:"authzPath"` // path to json file containing allowed client's spiffe id
}

// ToMIAFInput converts an http.MIAF config object into a parser.MIAFInput
// suitable for passing to parser.ParseMIAFConfig.
func (miaf *MIAF) ToMIAFInput() parser.MIAFInput {
	mis := miaf.MIS

	// Map TrustBundle only when at least one field is populated,
	// matching parser.ParseMIAFConfig's nil-check on MISInput.TrustBundle.
	var trustBundle *parser.TrustBundleInput
	if mis.TrustBundle.URI != "" || mis.TrustBundle.Path != "" {
		trustBundle = &parser.TrustBundleInput{
			URI:  mis.TrustBundle.URI,
			Path: mis.TrustBundle.Path,
		}
	}

	return parser.MIAFInput{
		X509: parser.MIAFx509Input{},
		MIS: parser.MISInput{
			Endpoint:    mis.Endpoint,
			CAPath:      mis.CAPath,
			TrustDomain: mis.TrustDomain,
			TrustBundle: trustBundle,
		},
		AuthzPath: miaf.AuthzPath,
	}
}

// HttpBinding provides service endpoints as a fasthttp web server
type HttpBinding struct {
	CertProvider      certs.ICertProvider
	ParsedMIAFConfig  *parser.ParsedMIAFConfig
	server            *fasthttp.Server
	pipeline          Pipeline
	errChan           chan error
	trustBundleCacher trustbundle.TrustMaterialCacherIfc
}

// ValidateMIAFConfig validates the MIAF configuration based on the following rules:
//  1. MIS must not be nil.
//  2. mis.endpoint and mis.caPath must be present together (both or neither).
//  3. if trustBundle.uri is present, mis.endpoint & mis.caPath must be present.
//  4. if neither trustBundle.path nor endpoint is configured, fail.
//     (trustBundle.path makes endpoint+caPath+trustBundle.uri optional;
//     without it, endpoint+caPath are required and trustBundle.uri is optional)
//  5. trustDomain is required when endpoint+caPath are absent (static trust bundle mode).
func ValidateMIAFConfig(miaf *MIAF) error {
	if miaf == nil {
		return v1alpha2.NewCOAError(nil, "MIAF config is required but missing", v1alpha2.BadConfig)
	}

	mis := miaf.MIS
	if mis == nil {
		return v1alpha2.NewCOAError(nil, "MIAF.MIS is required but missing", v1alpha2.BadConfig)
	}

	hasEndpoint := mis.Endpoint != ""
	hasCAPath := mis.CAPath != ""
	hasTrustBundlePath := mis.TrustBundle.Path != ""
	hasTrustBundleURI := mis.TrustBundle.URI != ""

	// Rule 2: endpoint and caPath must be present together
	if hasEndpoint != hasCAPath {
		return v1alpha2.NewCOAError(nil, "MIS endpoint and caPath must be configured together", v1alpha2.BadConfig)
	}

	// Rule 3: if trustBundle.uri is present, endpoint & caPath must be present
	if hasTrustBundleURI && (!hasEndpoint || !hasCAPath) {
		return v1alpha2.NewCOAError(nil, "MIS endpoint and caPath are required when trustBundle.uri is specified", v1alpha2.BadConfig)
	}

	// Rule 4: if neither trustBundle.path nor endpoint is configured, fail
	if !hasTrustBundlePath && !hasEndpoint {
		return v1alpha2.NewCOAError(nil, "either trustBundle.path or MIS endpoint+caPath must be configured", v1alpha2.BadConfig)
	}

	// Rule 5: trustDomain is required in static trust bundle mode (no endpoint+caPath)
	if hasTrustBundlePath && !hasEndpoint && mis.TrustDomain == "" {
		return v1alpha2.NewCOAError(nil, "MIS trustDomain is required when using static trust bundle (trustBundle.path) without endpoint+caPath", v1alpha2.BadConfig)
	}

	return nil
}

// Launch fasthttp server
func (h *HttpBinding) Launch(config HttpBindingConfig, endpoints []v1alpha2.Endpoint, pubsubProvider pubsub.IPubSubProvider) error {
	handler := h.useRouter(endpoints)
	var err error
	h.pipeline, err = BuildPipeline(config, pubsubProvider)
	if err != nil {
		return err
	}

	// Initialize error channel for the server goroutine
	h.errChan = make(chan error, 1)

	if config.TLS {
		switch config.CertProvider.Type {
		case "certs.autogen":
			h.CertProvider = &autogen.AutoGenCertProvider{}
		case "certs.localfile":
			h.CertProvider = &localfile.LocalCertFileProvider{}
		default:
			return v1alpha2.NewCOAError(nil, fmt.Sprintf("cert provider type '%s' is not recognized", config.CertProvider.Type), v1alpha2.BadConfig)
		}
		err = h.CertProvider.Init(config.CertProvider.Config)
		if err != nil {
			return err
		}
	}

	// For MIAF
	if config.MTLS {
		switch config.CertProvider.Type {
		case "certs.localfile":
			h.CertProvider = &localfile.LocalCertFileProvider{}
		default:
			return v1alpha2.NewCOAError(nil, fmt.Sprintf("cert provider type '%s' is not recognized or allowed for mTLS", config.CertProvider.Type), v1alpha2.BadConfig)
		}
		err = h.CertProvider.Init(config.CertProvider.Config)
		if err != nil {
			return err
		}

		// localhost is a placeholder
		cert, key, err := h.CertProvider.GetCert("localhost")
		if err != nil {
			return err
		}

		// Validate Cert And Key
		if ok, err := validators.ValidateX509SVID(cert, validators.PrincipalWFM); !ok {
			return fmt.Errorf("failed to validate SVID, err: %w", err)
		}

		if err := validators.ValidatePrivateKey(key); err != nil {
			return fmt.Errorf("failed to validate private key, err: %w", err)
		}
		// assign validated items here
		h.ParsedMIAFConfig.X509.CertPEM = cert
		h.ParsedMIAFConfig.X509.KeyPEM = key

		// Now setup ways to obtain trustbundle, and a cache which can be accessed here
		ccfg := trustbundle.TrustMaterialCacherConfig{
			MISEndpoint:     h.ParsedMIAFConfig.MIS.Endpoint,
			MISCAPem:        h.ParsedMIAFConfig.MIS.CAPEM,
			TrustBundleURI:  h.ParsedMIAFConfig.MIS.TrustBundle.URI,
			TrustBundleJSON: h.ParsedMIAFConfig.MIS.TrustBundle.BundleJSON,
			TrustDomain:     h.ParsedMIAFConfig.MIS.TrustDomain,
		}
		tbc := trustbundle.New(ccfg)
		// this starts the trust bundle cacher
		err = tbc.Start()
		if err != nil {
			return fmt.Errorf("failed to get and cache trustbundle/trustdomain, err: %w", err)
		}

		h.trustBundleCacher = tbc

	}

	h.server = &fasthttp.Server{
		Handler: h.pipeline.Apply(handler),
	}

	go func() {
		var serverErr error
		if config.TLS {
			cert, key, err := h.CertProvider.GetCert("localhost") // TODO: user proper host/DNS name
			if err != nil {
				h.errChan <- v1alpha2.NewCOAError(nil, fmt.Sprintf("error getting TLS certificates: %s", err.Error()), v1alpha2.BadConfig)
				return
			}
			serverErr = h.server.ListenAndServeTLSEmbed(fmt.Sprintf(":%d", config.Port), cert, key)
		} else if config.MTLS {
			serverCert, err := parser.CertificateFromBytes(h.ParsedMIAFConfig.X509.CertPEM, h.ParsedMIAFConfig.X509.KeyPEM)
			if err != nil {
				h.errChan <- v1alpha2.NewCOAError(nil, fmt.Sprintf("error getting TLS certificates: %s", err.Error()), v1alpha2.BadConfig)
				return
			}
			tlsConfig, err := mtls.NewMTLSServerConfig(serverCert, mtls.VerifierConfig{
				GetOwnTrustDomain:   h.trustBundleCacher.GetTrustDomain,
				GetTrustBundleBytes: h.trustBundleCacher.GetTrustBundle,
				GetClientAllowList: func() []string {
					return h.ParsedMIAFConfig.AuthorizedSPIFFEIDs
				},
			})
			if err != nil {
				h.errChan <- v1alpha2.NewCOAError(nil, fmt.Sprintf("error getting mTLS config: %s", err.Error()), v1alpha2.BadConfig)
				return
			}

			ln, err := net.Listen("tcp4", fmt.Sprintf(":%d", config.Port))
			if err != nil {
				h.errChan <- v1alpha2.NewCOAError(nil, fmt.Sprintf("error creating tcp listner: %s", err.Error()), v1alpha2.BadConfig)
				return
			}

			lnTls := tls.NewListener(ln, tlsConfig)
			serverErr = h.server.Serve(lnTls)
		} else {
			serverErr = h.server.ListenAndServe(fmt.Sprintf(":%d", config.Port))
		}
		// Send all server errors to the channel
		// During normal shutdown, serverErr might be nil or a "server closed" type error
		if serverErr != nil {
			h.errChan <- v1alpha2.NewCOAError(nil, fmt.Sprintf("server error: %s", serverErr.Error()), v1alpha2.InternalError)
		}
	}()

	select {
	case err := <-h.errChan:
		if h.trustBundleCacher != nil {
			// Stopping caching mechanism for trust bundle
			h.trustBundleCacher.Stop()
		}
		httpLogger.ErrorCtx(context.Background(), "H (HttpBinding): Server error: %s", err.Error())
		return err
	case <-time.After(10 * time.Second):
		httpLogger.DebugCtx(context.Background(), "H (HttpBinding): Server started on port: %s", config.Port)
	}
	return nil
}

// Shutdown fasthttp server
func (h *HttpBinding) Shutdown(ctx context.Context) error {
	if err := h.pipeline.Shutdown(ctx); err != nil {
		return err
	}
	return h.server.ShutdownWithContext(ctx)
}

func (h *HttpBinding) useRouter(endpoints []v1alpha2.Endpoint) fasthttp.RequestHandler {
	router := h.getRouter(endpoints)
	return router.Handler
}

func (h *HttpBinding) getRouter(endpoints []v1alpha2.Endpoint) *routing.Router {
	router := routing.New()
	router.SaveMatchedRoutePath = true
	for _, e := range endpoints {
		path := fmt.Sprintf("/%s/%s", e.Version, e.Route)
		for _, p := range e.Parameters {
			path += "/{" + p + "}"
		}
		for _, m := range e.Methods {
			router.Handle(m, path, wrapAsHTTPHandler(e, e.Handler))
		}
	}
	return router
}

func composeCOARequestContext(reqCtx *fasthttp.RequestCtx, actCtx *contexts.ActivityLogContext, diagCtx *contexts.DiagnosticLogContext) context.Context {
	retCtx := context.TODO()
	if reqCtx != nil {
		retCtx = context.WithValue(retCtx, v1alpha2.COAFastHTTPContextKey, reqCtx)
	}
	if actCtx != nil {
		retCtx = context.WithValue(retCtx, contexts.ActivityLogContextKey, actCtx)
	}
	if diagCtx != nil {
		retCtx = context.WithValue(retCtx, contexts.DiagnosticLogContextKey, diagCtx)
	}
	return retCtx
}

func wrapAsHTTPHandler(endpoint v1alpha2.Endpoint, handler v1alpha2.COAHandler) fasthttp.RequestHandler {
	return func(reqCtx *fasthttp.RequestCtx) {
		actCtx := contexts.ParseActivityLogContextFromHttpRequestHeader(reqCtx)
		diagCtx := contexts.ParseDiagnosticLogContextFromHttpRequestHeader(reqCtx)
		ctx := composeCOARequestContext(reqCtx, actCtx, diagCtx)
		// patch correlation id if missing
		ctx = contexts.GenerateCorrelationIdToParentContextIfMissing(ctx)
		req := v1alpha2.COARequest{
			Body:    reqCtx.PostBody(),
			Route:   string(reqCtx.Request.URI().Path()),
			Method:  string(reqCtx.Method()),
			Context: ctx,
		}
		meta := reqCtx.Request.Header.Peek(v1alpha2.COAMetaHeader)
		if meta != nil {
			metaMap := make(map[string]string)
			json.Unmarshal(meta, &metaMap)
			req.Metadata = metaMap
		}
		req.Parameters = make(map[string]string)

		for _, p := range endpoint.Parameters {
			k := p
			if strings.HasSuffix(p, "?") {
				k = k[:len(p)-1]
			}
			v := reqCtx.UserValue(k)
			k = "__" + k
			if v == nil {
				req.Parameters[k] = "" // TODO: chance to report on missing required parameters
			} else {
				req.Parameters[k] = utils.FormatAsString(v)
			}
		}

		reqCtx.QueryArgs().VisitAll(func(key, value []byte) {
			req.Parameters[string(key)] = string(value)
		})

		resp := handler(req)

		if resp.State == v1alpha2.APIRedirect {
			reqCtx.Redirect(resp.RedirectUri, 308)
		} else {
			if len(resp.Metadata) != 0 {
				data, _ := json.Marshal(resp.Metadata)
				reqCtx.Response.Header.Set(v1alpha2.COAMetaHeader, string(data))
			}
			reqCtx.SetContentType(resp.ContentType)
			reqCtx.SetBody(resp.Body)
			reqCtx.SetStatusCode(toHttpState(resp.State))
		}
	}
}

func toHttpState(state v1alpha2.State) int {
	switch state {
	case v1alpha2.OK:
		return fasthttp.StatusOK
	case v1alpha2.Created:
		return fasthttp.StatusCreated
	case v1alpha2.Accepted:
		return fasthttp.StatusAccepted
	case v1alpha2.NotModified: // ← ADDED for Bundle 304 issue
		return fasthttp.StatusNotModified
	case v1alpha2.BadRequest:
		return fasthttp.StatusBadRequest
	case v1alpha2.Unauthorized:
		return fasthttp.StatusUnauthorized
	case v1alpha2.NotFound:
		return fasthttp.StatusNotFound
	case v1alpha2.MethodNotAllowed:
		return fasthttp.StatusMethodNotAllowed
	case v1alpha2.Conflict:
		return fasthttp.StatusConflict
	case v1alpha2.StatusUnprocessableEntity:
		return fasthttp.StatusUnprocessableEntity
	case v1alpha2.InternalError:
		return fasthttp.StatusInternalServerError
	default:
		return fasthttp.StatusInternalServerError
	}
}
