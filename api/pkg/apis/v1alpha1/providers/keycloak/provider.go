package keycloak

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Nerzal/gocloak/v13"
	"github.com/eclipse-symphony/symphony/api/pkg/apis/v1alpha1/contexts"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/observability"
	observ_utils "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/observability/utils"
	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
)

var keycloakLogs = logger.NewLogger("coa.runtime")

type KeycloakProvider struct {
	Context     *contexts.ManagerContext
	client      *gocloak.GoCloak
	adminToken  *gocloak.JWT
	tokenExpiry time.Time
	config      KeycloakProviderConfig
	mu          sync.Mutex // To ensure thread-safe token refresh
}

type ClientConfig struct {
	ClientID                  string             `json:"client_id"`
	Name                      string             `json:"name"`
	Description               string             `json:"description"`
	Enabled                   bool               `json:"enabled"`
	AuthenticatorType         string             `json:"authenticator_type"`
	StandardFlowEnabled       bool               `json:"standard_flow_enabled"`
	ServiceAccountsEnabled    bool               `json:"service_accounts_enabled"`
	DirectAccessGrantsEnabled bool               `json:"direct_access_grants_enabled"`
	Attributes                *map[string]string `json:"attributes,omitempty"`
}

type ClientResult struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	ClientUUID   string `json:"client_uuid"`
	TokenUrl     string `json:"token_url"`
}

type TokenData struct {
	AccessToken  string                 `json:"access_token"`
	RefreshToken string                 `json:"refresh_token"`
	ExpiresIn    int                    `json:"expires_in"`
	TokenType    string                 `json:"token_type"`
	Claims       map[string]interface{} `json:"claims,omitempty"`
}

func (self *KeycloakProvider) Init(config providers.IProviderConfig) error {
	ctx, span := observability.StartSpan("Android ADB Provider", context.TODO(), &map[string]string{
		"method": "Init",
	})
	var err error = nil
	defer observ_utils.CloseSpanWithError(span, &err)
	defer observ_utils.EmitUserDiagnosticsLogs(ctx, &err)

	keycloakLogs.InfoCtx(ctx, "  P (Keycloak Provider): Init()")

	err = self.setup(ctx, config)
	if err != nil {
		keycloakLogs.ErrorfCtx(ctx, "  P (Keycloak Provider): failure: %+v", err)
		return errors.New("expected correct KeycloakProviderConfig")
	}

	return nil
}

type KeycloakProviderConfig struct {
	KeycloakURL string `json:"keycloakURL"`
	Realm       string `json:"realm"`
	AdminUser   string `json:"adminUsername"`
	AdminPass   string `json:"adminPassword"`
}

func toKeycloakProviderConfig(config providers.IProviderConfig) (KeycloakProviderConfig, error) {
	ret := KeycloakProviderConfig{}
	data, err := json.Marshal(config)
	if err != nil {
		return ret, err
	}
	err = json.Unmarshal(data, &ret)
	if err != nil {
		return ret, err
	}

	if ret.AdminUser == "" {
		return ret, fmt.Errorf("keycloak admin user cannot be empty, please check the config that you passed")
	}
	if ret.AdminPass == "" {
		return ret, fmt.Errorf("keycloak admin password cannot be empty, please check the config that you passed")
	}
	if ret.KeycloakURL == "" {
		return ret, fmt.Errorf("keycloak url cannot be empty, please check the config that you passed")
	}
	if ret.Realm == "" {
		return ret, fmt.Errorf("keycloak realm cannot be empty, please check the config that you passed")
	}

	return ret, err
}

// setup initializes a KeycloakProvider with admin credentials
func (self *KeycloakProvider) setup(ctx context.Context, config providers.IProviderConfig) error {
	parsedConfig, err := toKeycloakProviderConfig(config)
	if err != nil {
		keycloakLogs.Errorf("  P (Keycloak Provider): expected KeycloakProviderConfig: %+v", err)
		return err
	}

	client := gocloak.NewClient(parsedConfig.KeycloakURL)
	token, err := client.LoginAdmin(ctx, parsedConfig.AdminPass, parsedConfig.AdminPass, parsedConfig.Realm)
	if err != nil {
		return fmt.Errorf("failed to get admin token: %w", err)
	}

	self.client = client
	self.config = parsedConfig
	self.adminToken = token
	self.tokenExpiry = time.Now().Add(time.Duration(token.ExpiresIn) * time.Second)

	return nil
}

func (self *KeycloakProvider) GetTokenURL() string {
	return self.config.KeycloakURL
}

// refreshTokenIfNeeded ensures the admin token is valid and refreshes it if necessary
func (self *KeycloakProvider) refreshTokenIfNeeded(ctx context.Context) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	// Check if the token is still valid
	if time.Now().Before(self.tokenExpiry) {
		return nil
	}

	// Refresh the token
	token, err := self.client.LoginAdmin(ctx, self.config.AdminUser, self.config.AdminPass, self.config.Realm)
	if err != nil {
		return fmt.Errorf("failed to refresh admin token: %w", err)
	}

	self.adminToken = token
	self.tokenExpiry = time.Now().Add(time.Duration(token.ExpiresIn) * time.Second)
	return nil
}

// CreateClientWithClaims creates a client and adds claims as protocol mappers
func (self *KeycloakProvider) CreateClientWithClaims(ctx context.Context, clientConfig ClientConfig, claims map[string]interface{}) (*ClientResult, error) {
	// Ensure the admin token is valid before making the API call
	if err := self.refreshTokenIfNeeded(ctx); err != nil {
		return nil, err
	}

	client := gocloak.Client{
		ClientID:                     gocloak.StringP(clientConfig.ClientID),
		Name:                         gocloak.StringP(clientConfig.Name),
		Description:                  gocloak.StringP(clientConfig.Description),
		Enabled:                      gocloak.BoolP(clientConfig.Enabled),
		ClientAuthenticatorType:      gocloak.StringP(clientConfig.AuthenticatorType),
		StandardFlowEnabled:          gocloak.BoolP(clientConfig.StandardFlowEnabled),
		ServiceAccountsEnabled:       gocloak.BoolP(clientConfig.ServiceAccountsEnabled),
		DirectAccessGrantsEnabled:    gocloak.BoolP(clientConfig.DirectAccessGrantsEnabled),
		Protocol:                     gocloak.StringP("openid-connect"),
		Attributes:                   clientConfig.Attributes,
		AuthorizationServicesEnabled: gocloak.BoolP(true),
	}

	clientUUID, err := self.client.CreateClient(ctx, self.adminToken.AccessToken, self.config.Realm, client)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	for claimName, claimValue := range claims {
		err = self.createClaimMapper(ctx, clientUUID, claimName, claimValue)
		if err != nil {
			return nil, fmt.Errorf("failed to create claim mapper for %s: %w", claimName, err)
		}
	}

	clientSecret, err := self.client.GetClientSecret(ctx, self.adminToken.AccessToken, self.config.Realm, clientUUID)
	if err != nil {
		return nil, fmt.Errorf("failed to get client secret: %w", err)
	}

	return &ClientResult{
		ClientID:     clientConfig.ClientID,
		ClientSecret: *clientSecret.Value,
		ClientUUID:   clientUUID,
		TokenUrl:     self.GetTokenURL(),
	}, nil
}

// createClaimMapper creates a protocol mapper for a client
func (self *KeycloakProvider) createClaimMapper(ctx context.Context, clientUUID, claimName string, claimValue interface{}) error {
	mapper := gocloak.ProtocolMapperRepresentation{
		Name:           gocloak.StringP(fmt.Sprintf("%s-mapper", claimName)),
		Protocol:       gocloak.StringP("openid-connect"),
		ProtocolMapper: gocloak.StringP("oidc-hardcoded-claim-mapper"),
		Config: &map[string]string{
			"claim.name":                claimName,
			"claim.value":               fmt.Sprintf("%v", claimValue),
			"jsonType.label":            "String",
			"id.token.claim":            "true",
			"access.token.claim":        "true",
			"userinfo.token.claim":      "true",
			"introspection.token.claim": "true",
		},
	}

	_, err := self.client.CreateClientProtocolMapper(ctx, self.adminToken.AccessToken, self.config.Realm, clientUUID, mapper)
	return err
}

// GetTokenWithClaims retrieves a token and optionally updates claims
func (self *KeycloakProvider) GetTokenWithClaims(ctx context.Context, clientID, clientSecret string, additionalClaims map[string]interface{}) (*TokenData, error) {
	// Ensure the admin token is valid before making the API call
	if err := self.refreshTokenIfNeeded(ctx); err != nil {
		return nil, err
	}

	if len(additionalClaims) > 0 {
		err := self.updateClientClaims(ctx, clientID, additionalClaims)
		if err != nil {
			return nil, fmt.Errorf("failed to update client claims: %w", err)
		}
	}

	token, err := self.client.LoginClient(ctx, clientID, clientSecret, self.config.Realm)
	if err != nil {
		return nil, fmt.Errorf("failed to get token: %w", err)
	}

	claims, err := self.parseTokenClaims(token.AccessToken)
	if err != nil {
		return nil, fmt.Errorf("failed to parse token claims: %w", err)
	}

	return &TokenData{
		AccessToken:  token.AccessToken,
		RefreshToken: token.RefreshToken,
		ExpiresIn:    token.ExpiresIn,
		TokenType:    token.TokenType,
		Claims:       claims,
	}, nil
}

// parseTokenClaims decodes and parses claims from a JWT
func (self *KeycloakProvider) parseTokenClaims(accessToken string) (map[string]interface{}, error) {
	parts := strings.Split(accessToken, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid JWT format")
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("failed to decode payload: %w", err)
	}

	var claims map[string]interface{}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("failed to unmarshal claims: %w", err)
	}

	return claims, nil
}

func (self *KeycloakProvider) updateClientClaims(ctx context.Context, clientID string, claims map[string]interface{}) error {
	// Ensure the admin token is valid before making the API call
	if err := self.refreshTokenIfNeeded(ctx); err != nil {
		return err
	}

	// Get client UUID
	clients, err := self.client.GetClients(ctx, self.adminToken.AccessToken, self.config.Realm, gocloak.GetClientsParams{
		ClientID: &clientID,
	})
	if err != nil {
		return fmt.Errorf("failed to get client: %w", err)
	}

	if len(clients) == 0 {
		return fmt.Errorf("client %s not found", clientID)
	}

	clientUUID := *clients[0].ID

	// Get existing mappers
	mappers, err := self.client.GetClientScopeProtocolMappers(ctx, self.adminToken.AccessToken, self.config.Realm, clientUUID)
	if err != nil {
		return fmt.Errorf("failed to get protocol mappers: %w", err)
	}

	// Track existing claims
	existingClaims := make(map[string]string) // claimName -> mapperID
	for _, mapper := range mappers {
		if mapper.ProtocolMappersConfig != nil {
			existingClaims[*mapper.ProtocolMappersConfig.ClaimName] = *mapper.ProtocolMappersConfig.IDTokenClaim
		}
	}

	// Update or create claims
	for claimName, claimValue := range claims {
		err := self.createClaimMapper(ctx, clientUUID, claimName, claimValue)
		if err != nil {
			return fmt.Errorf("failed to update claim %s: %w", claimName, err)
		}
	}

	return nil
}
