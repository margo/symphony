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
	"net/url"
	"slices"
	"strings"
	"time"

	v1alpha2 "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/margo/mis/cacher"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/certs"
	autogen "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/certs/autogen"
	localfile "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/certs/localfile"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers/pubsub"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/utils"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger/contexts"
	margoStdSbiAPI "github.com/margo/sandbox/standard/generatedCode/wfm/sbi"

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
	URI        string `json:"uri"`
	StaticPath string `json:"staticPath"`
}

// MIS represents the Margo Identity Service configuration.
type MIS struct {
	DiscoveryDocumentURL string      `json:"discoveryDocumentURL"`
	TrustAnchor          string      `json:"trustAnchor"`
	TrustPins            []string    `json:"trustPins"`
	CacheInterval        int         `json:"cacheInterval"`
	TrustBundle          TrustBundle `json:"trustBundle"`
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
	if mis.TrustBundle.URI != "" || mis.TrustBundle.StaticPath != "" {
		trustBundle = &parser.TrustBundleInput{
			URI:        mis.TrustBundle.URI,
			StaticPath: mis.TrustBundle.StaticPath,
		}
	}

	return parser.MIAFInput{
		X509: parser.MIAFx509Input{},
		MIS: parser.MISInput{
			DiscoveryDocumentURL: mis.DiscoveryDocumentURL,
			TrustAnchorPath:      mis.TrustAnchor,
			TrustBundle:          trustBundle,
			TrustPins:            mis.TrustPins,
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
	trustBundleCacher cacher.TrustMaterialCacherIfc
	authClientCacher  cacher.AuthClientCatcherIfc
}

// ValidateMIAFConfig validates the MIAF configuration based on the following rules:
//  1. MIAF.AuthzPath and MIS.TrustBundle.StaticPath are always required.
//  2. MIS.DiscoveryDocumentURL and MIS.TrustBundle.URI are optional, but if present
//     they must be complete, valid absolute URLs (e.g. https://mis.margo.org/.well-known/margo).
//  3. If either DiscoveryDocumentURL or TrustBundle.URI is present, a trust anchor is required:
//     - if the URL host is an IP address, MIS.TrustPins must be present.
//     - if the URL host is a domain name, MIS.TrustAnchor must be present.
func ValidateMIAFConfig(miaf *MIAF) error {
	if miaf == nil {
		return v1alpha2.NewCOAError(nil, "MIAF config is required but missing", v1alpha2.BadConfig)
	}

	mis := miaf.MIS
	if mis == nil {
		return v1alpha2.NewCOAError(nil, "MIAF.MIS is required but missing", v1alpha2.BadConfig)
	}

	// Rule 1: required fields.
	if miaf.AuthzPath == "" {
		return v1alpha2.NewCOAError(nil,
			"MIAF.authzPath must always be configured",
			v1alpha2.BadConfig,
		)
	}
	if mis.TrustBundle.StaticPath == "" {
		return v1alpha2.NewCOAError(nil,
			"MIS.trustBundle.staticPath must always be configured",
			v1alpha2.BadConfig,
		)
	}

	// Rule 2: optional URLs, but must be valid when present.
	hasEndpoint := mis.DiscoveryDocumentURL != ""
	hasTrustBundleURI := mis.TrustBundle.URI != ""

	hosts := make([]string, 0)
	if hasEndpoint {
		host, err := validateAbsoluteURL(mis.DiscoveryDocumentURL)
		if err != nil {
			return v1alpha2.NewCOAError(nil,
				fmt.Sprintf("MIS.discoveryDocumentURL is invalid: %s", err.Error()),
				v1alpha2.BadConfig,
			)
		}
		hosts = append(hosts, host)
	}
	if hasTrustBundleURI {
		host, err := validateAbsoluteURL(mis.TrustBundle.URI)
		if err != nil {
			return v1alpha2.NewCOAError(nil,
				fmt.Sprintf("MIS.trustBundle.uri is invalid: %s", err.Error()),
				v1alpha2.BadConfig,
			)
		}
		hosts = append(hosts, host)
	}

	// Rule 3: a trust anchor is required when any URL is configured.
	if hasEndpoint || hasTrustBundleURI {
		hasCAPath := mis.TrustAnchor != ""
		// it contains trustpins & atleast 1 element is not empty
		hasTrustPins := len(mis.TrustPins) > 0 && mis.TrustPins[0] != ""

		// Check every configured URL host; an IP host requires trust pins.
		for _, host := range hosts {
			if net.ParseIP(host) != nil {
				if !hasTrustPins {
					return v1alpha2.NewCOAError(nil,
						"MIS.trustPins must be configured when the MIS URL uses an IP address",
						v1alpha2.BadConfig,
					)
				}
			} else if !hasCAPath {
				return v1alpha2.NewCOAError(nil,
					"MIS.trustAnchor must be configured when the MIS URL uses a domain name",
					v1alpha2.BadConfig,
				)
			}
		}
	}

	return nil
}

// validateAbsoluteURL checks that raw is a complete absolute https URL with a host
// and returns the host name (without port).
func validateAbsoluteURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL '%s': %w", raw, err)
	}
	if u.Scheme != "https" {
		return "", fmt.Errorf("URL '%s' must use the https scheme", raw)
	}
	if u.Host == "" {
		return "", fmt.Errorf("URL '%s' must include a host", raw)
	}
	return u.Hostname(), nil
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
	finalHandler := h.pipeline.Apply(handler)

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

		leaf, intermediate := parser.SplitLeafAndChain(cert)

		spiffeId, err := validateMIAFCertificate(leaf, key, intermediate)
		if err != nil {
			return err
		}

		td, err := parser.ParseTrustDomain(spiffeId)
		if err != nil {
			return fmt.Errorf("failed to parse trust domain from spiffeId, err: %w", err)
		}

		// assign validated items here
		h.ParsedMIAFConfig.X509.CertPEM = leaf
		h.ParsedMIAFConfig.X509.ChainPEM = intermediate
		h.ParsedMIAFConfig.X509.KeyPEM = key
		h.ParsedMIAFConfig.X509.SpiffeID = spiffeId
		h.ParsedMIAFConfig.MIS.TrustDomain = td

		// Now setup ways to obtain trustbundle, and a cache which can be accessed here
		ccfg := cacher.TrustMaterialCacherConfig{
			DiscoveryDocumentURL: h.ParsedMIAFConfig.MIS.DiscoveryDocumentURL,
			TrustPins:            h.ParsedMIAFConfig.MIS.TrustPins,
			MISTrustAnchor:       h.ParsedMIAFConfig.MIS.TrustAnchorPEM,
			TrustDomain:          h.ParsedMIAFConfig.MIS.TrustDomain,
			Interval:             config.MIAF.MIS.CacheInterval,
			Logger:               httpLogger,
		}

		if h.ParsedMIAFConfig.MIS.TrustBundle != nil {
			ccfg.TrustBundleURI = h.ParsedMIAFConfig.MIS.TrustBundle.URI
			ccfg.TrustBundleJSON = h.ParsedMIAFConfig.MIS.TrustBundle.BundleJSON
		}

		tbc := cacher.NewTrustMaterialCacher(ccfg)
		// this starts the trust bundle cacher
		err = tbc.Start()
		if err != nil {
			return fmt.Errorf("failed to get and cache trustbundle/trustdomain, err: %w", err)
		}

		h.trustBundleCacher = tbc

		err = validators.ValidateSpiffeIDWithTrustDomain(spiffeId, tbc.GetTrustDomain(), validators.PrincipalWFMClient)
		if err != nil {
			return fmt.Errorf("failed to validate spiffeId with trust domain, err: %w", err)
		}
		acc, err := cacher.NewAuthClientCacher(config.MIAF.AuthzPath, httpLogger)
		if err != nil {
			return fmt.Errorf("failed to get auth client cacher, err: %w", err)
		}

		err = acc.Start()
		if err != nil {
			return fmt.Errorf("failed to start auth client cacher, err: %w", err)
		}

		h.authClientCacher = acc
		finalHandler = h.mTLSAuthMiddleware(finalHandler)

	}

	h.server = &fasthttp.Server{
		Handler: h.pipeline.Apply(finalHandler),
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
			serverCert, err := parser.CertificateFromBytes(h.ParsedMIAFConfig.X509.CertPEM, h.ParsedMIAFConfig.X509.KeyPEM, h.ParsedMIAFConfig.X509.ChainPEM)
			if err != nil {
				h.errChan <- v1alpha2.NewCOAError(nil, fmt.Sprintf("error getting TLS certificates: %s", err.Error()), v1alpha2.BadConfig)
				return
			}
			tlsConfig, err := mtls.NewMTLSServerConfig(serverCert, mtls.VerifierConfig{
				GetOwnTrustDomain:   h.trustBundleCacher.GetTrustDomain,
				GetTrustBundleBytes: h.trustBundleCacher.GetTrustBundle,
				GetClientAllowList:  h.authClientCacher.GetAuthorizedClients,
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
			h.server.DisableKeepalive = true // disabling keep alives on server side as well
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
			// Stopping caching mechanism for trust bundle cacher
			h.trustBundleCacher.Stop()
		}

		if h.authClientCacher != nil {
			// Stopping caching mechanism for auth client cacher
			h.authClientCacher.Stop()
		}
		httpLogger.ErrorCtx(context.Background(), "H (HttpBinding): Server error:", err.Error())
		return err
	case <-time.After(10 * time.Second):
		httpLogger.DebugCtx(context.Background(), "H (HttpBinding): Server started on port: ", config.Port)
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

func (h *HttpBinding) mTLSAuthMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		fullPath := string(ctx.Path())

		httpLogger.DebugCtx(context.Background(), "H (mTLSAuthMiddleware): Incoming request", "path", fullPath)

		tlsConn, ok := ctx.Conn().(*tls.Conn)
		if !ok {
			httpLogger.WarnCtx(context.Background(), "H (mTLSAuthMiddleware): Connection is not a TLS connection", "path", fullPath)
			resp, _ := margoStdSbiAPI.NewNotAuthorized(
				"The request is not authorized by the WFM's local policy.",
				fullPath,
			).MarshalJSON()
			ctx.SetStatusCode(fasthttp.StatusUnauthorized)
			ctx.SetBody(resp)
			return
		}

		state := tlsConn.ConnectionState()
		if len(state.PeerCertificates) == 0 {
			httpLogger.WarnCtx(context.Background(), "H (mTLSAuthMiddleware): No peer certificates found in TLS connection", "path", fullPath)
			resp, _ := margoStdSbiAPI.NewNotAuthorized(
				"The request is not authorized by the WFM's local policy.",
				fullPath,
			).MarshalJSON()
			ctx.SetStatusCode(fasthttp.StatusUnauthorized)
			ctx.SetBody(resp)
			return
		}

		spiffeID, err := parser.ParseSpiffeIdFromX509Svid(state.PeerCertificates[0].Raw)
		if err != nil {
			httpLogger.ErrorCtx(context.Background(), "H (mTLSAuthMiddleware): Failed to parse SPIFFE ID from peer certificate", "path", fullPath, "error", err.Error())
			resp, _ := margoStdSbiAPI.NewNotAuthorized(
				"The request is not authorized by the WFM's local policy.",
				fullPath,
			).MarshalJSON()
			ctx.SetStatusCode(fasthttp.StatusUnauthorized)
			ctx.SetBody(resp)
			return
		}

		httpLogger.DebugCtx(context.Background(), "H (mTLSAuthMiddleware): Parsed SPIFFE ID from peer certificate", "path", fullPath, "spiffeID", spiffeID)

		allowList := h.authClientCacher.GetAuthorizedClients()
		authorized := slices.Contains(allowList, spiffeID)

		if !authorized {
			httpLogger.WarnCtx(context.Background(), "H (mTLSAuthMiddleware): SPIFFE ID not in authorized client list", "path", fullPath, "spiffeID", spiffeID)
			resp, _ := margoStdSbiAPI.NewNotAuthorized(
				"The request is not authorized by the WFM's local policy.",
				fullPath,
			).MarshalJSON()
			ctx.SetStatusCode(fasthttp.StatusUnauthorized)
			ctx.SetBody(resp)
			return
		}

		httpLogger.DebugCtx(context.Background(), "H (mTLSAuthMiddleware): Request authorized", "path", fullPath, "spiffeID", spiffeID)
		next(ctx)
	}
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

func validateMIAFCertificate(leaf, key, intermediate []byte) (string, error) {
	spiffeID, err := validators.ValidateX509SVID(leaf, validators.PrincipalWFM)
	if err != nil {
		return "", fmt.Errorf("failed to validate SVID, err: %w", err)
	}

	if err := validators.ValidatePrivateKey(key); err != nil {
		return "", fmt.Errorf("failed to validate private key, err: %w", err)
	}

	if err := validators.ValidateCertKeyPair(leaf, key); err != nil {
		return "", fmt.Errorf("failed to verify certificate against provided key, err: %w", err)
	}

	// as intermediates are optional
	if len(intermediate) > 0 {
		if err := validators.ValidateIntermediateCACertificates(intermediate); err != nil {
			return "", fmt.Errorf("validation failed for intermediate chains, err: %w", err)
		}
	}

	return spiffeID, nil
}
