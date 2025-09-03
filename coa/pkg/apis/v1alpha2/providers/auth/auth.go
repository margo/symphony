package auth

import (
	"context"
	"time"

	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/providers"
)

// AuthProvider defines the generic interface for authentication providers
type IAuthProvider interface {
	Init(config providers.IProviderConfig)

	// Authenticate user with credentials
	Authenticate(ctx context.Context, credentials AuthCredentials) (*AuthResult, error)

	// Validate an existing token
	ValidateToken(ctx context.Context, token string) (*TokenInfo, error)

	// Refresh an expired token
	RefreshToken(ctx context.Context, refreshToken string) (*AuthResult, error)

	// Get provider metadata/capabilities
	GetProviderInfo() ProviderInfo

	// not needed, but added to keep it simple for keycloak
	GetAuthTokenUrl() string

	// to add any entity, this is like signup
	AddNewAuthUser() error

	// to remove any entity, this is like removing a user
	RemoveAuthUser(id string) error

	// Cleanup resources
	Close() error
}

// AuthCredentials represents authentication input
type AuthCredentials struct {
	Username     string                 `json:"username,omitempty"`
	Password     string                 `json:"password,omitempty"`
	ClientID     string                 `json:"client_id,omitempty"`
	ClientSecret string                 `json:"client_secret,omitempty"`
	Token        string                 `json:"token,omitempty"`
	Claims       map[string]interface{} `json:"claims,omitempty"`
}

// AuthResult represents authentication output
type AuthResult struct {
	AccessToken  string                 `json:"access_token"`
	RefreshToken string                 `json:"refresh_token,omitempty"`
	TokenType    string                 `json:"token_type"`
	ExpiresIn    int                    `json:"expires_in"`
	Claims       map[string]interface{} `json:"claims,omitempty"`
	UserInfo     *UserInfo              `json:"user_info,omitempty"`
}

// TokenInfo represents token validation result
type TokenInfo struct {
	Valid     bool                   `json:"valid"`
	Claims    map[string]interface{} `json:"claims,omitempty"`
	ExpiresAt time.Time              `json:"expires_at"`
	UserInfo  *UserInfo              `json:"user_info,omitempty"`
}

// UserInfo represents user information
type UserInfo struct {
	ID       string            `json:"id"`
	Username string            `json:"username"`
	Email    string            `json:"email,omitempty"`
	Groups   []string          `json:"groups,omitempty"`
	Roles    []string          `json:"roles,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ProviderInfo represents provider capabilities
type ProviderInfo struct {
	Name         string   `json:"name"`
	Version      string   `json:"version"`
	Type         string   `json:"type"`
	Capabilities []string `json:"capabilities"`
}
