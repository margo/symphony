package margo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/eclipse-symphony/symphony/coa/pkg/logger"
	"gopkg.in/yaml.v2"

	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	observ_utils "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/observability/utils"
	margoNonStdAPI "github.com/margo/dev-repo/non-standard/generatedCode/wfm/nbi"
	"go.opentelemetry.io/otel/trace"
)

// Helper method for error responses
func createErrorResponse(logger logger.Logger, span trace.Span, err error, message string, errorType v1alpha2.State) v1alpha2.COAResponse {
	logger.InfofCtx(context.Background(), "err: %s, msg: %s", err.Error(), message)
	errResp := &margoNonStdAPI.ErrorResponse{
		ErrorCode: err.Error(),
		RequestId: "",
		Details: &map[string]interface{}{
			"message": message,
		},
		Timestamp: time.Now().UTC(),
	}

	respBytes, _ := json.Marshal(errResp)
	coaErr := v1alpha2.NewCOAError(err, message, errorType)

	response := v1alpha2.COAResponse{
		State: v1alpha2.GetErrorState(coaErr),
		Body:  respBytes,
	}

	return observ_utils.CloseSpanWithCOAResponse(span, response)
}

// Helper method for error responses
func createErrorResponse2(logger logger.Logger, span trace.Span, err error, message string, errorType v1alpha2.State) v1alpha2.COAResponse {
	logger.ErrorfCtx(context.Background(), "err: %s, msg: %s", err.Error(), message)

	// ertype := errorType.String()
	// erMesg := err.Error()
	errResp := struct {
		Error string
	}{
		Error: err.Error(),
	}

	respBytes, _ := json.Marshal(errResp)
	coaErr := v1alpha2.NewCOAError(err, message, errorType)

	response := v1alpha2.COAResponse{
		State: v1alpha2.GetErrorState(coaErr),
		Body:  respBytes,
	}

	return observ_utils.CloseSpanWithCOAResponse(span, response)
}

func createSuccessResponse[T any](span trace.Span, state v1alpha2.State, data *T) v1alpha2.COAResponse {
	// response := struct {
	// 	Data      *T        `json:"data,omitempty"`
	// 	RequestId string    `json:"requestId"`
	// 	Timestamp time.Time `json:"timestamp"`
	// }{
	// 	Data:      data,
	// 	RequestId: "",
	// 	Timestamp: time.Now().UTC(),
	// }
	// // ... rest unchanged

	respBytes, _ := json.Marshal(data)

	coaResponse := v1alpha2.COAResponse{
		State:       state,
		Body:        respBytes,
		ContentType: "application/json",
	}

	return observ_utils.CloseSpanWithCOAResponse(span, coaResponse)
}

// Supported content types
const (
	ContentTypeMargoManifest      = "application/vnd.margo.manifest.v1+json"
	ContentTypeYAML               = "application/yaml"
	ContentTypeJSON               = "application/json"
	ContentTypeMargoBundleTarGzip = "application/vnd.margo.bundle.v1+tar+gzip"
)

// ContentSerializer handles different content type serialization
type ContentSerializer interface {
	Serialize(data interface{}) ([]byte, error)
	Validate() error
}

// JSONSerializer handles JSON serialization
type JSONSerializer struct {
	ContentType string
}

func (j JSONSerializer) Serialize(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func (j JSONSerializer) Validate() error {
	validTypes := []string{ContentTypeJSON, ContentTypeMargoManifest}
	for _, validType := range validTypes {
		if j.ContentType == validType {
			return nil
		}
	}
	return fmt.Errorf("unsupported JSON content type: %s", j.ContentType)
}

// YAMLSerializer handles YAML serialization
type YAMLSerializer struct{}

func (y YAMLSerializer) Serialize(data interface{}) ([]byte, error) {
	return yaml.Marshal(data)
}

func (y YAMLSerializer) Validate() error {
	return nil
}

// BinarySerializer handles binary content (for tar+gzip)
type BinarySerializer struct{}

func (b BinarySerializer) Serialize(data interface{}) ([]byte, error) {
	// For binary content, data should already be []byte
	if bytes, ok := data.([]byte); ok {
		return bytes, nil
	}
	if bytesPtr, ok := data.(*[]byte); ok && bytesPtr != nil {
		return *bytesPtr, nil
	}
	return nil, fmt.Errorf("binary content must be []byte or *[]byte, got %T", data)
}

func (b BinarySerializer) Validate() error {
	return nil
}

// getSerializer returns appropriate serializer for content type
func getSerializer(contentType string) (ContentSerializer, error) {
	switch contentType {
	case ContentTypeJSON, ContentTypeMargoManifest:
		return JSONSerializer{ContentType: contentType}, nil
	case ContentTypeYAML:
		return YAMLSerializer{}, nil
	case ContentTypeMargoBundleTarGzip:
		return BinarySerializer{}, nil
	default:
		return nil, fmt.Errorf("unsupported content type: %s", contentType)
	}
}

// ResponseBuilder helps build COA responses with method chaining
type ResponseBuilder struct {
	span        trace.Span
	contentType string
	metadata    map[string]string
	state       v1alpha2.State
	data        interface{}
	err         error
}

// NewResponseBuilder creates a new response builder
func NewResponseBuilder(span trace.Span) *ResponseBuilder {
	return &ResponseBuilder{
		span:     span,
		metadata: make(map[string]string),
		state:    v1alpha2.BadRequest, // Default state
	}
}

// WithContentType sets the content type
func (rb *ResponseBuilder) WithContentType(contentType string) *ResponseBuilder {
	rb.contentType = contentType
	return rb
}

// WithMetadata sets metadata (replaces existing)
func (rb *ResponseBuilder) WithMetadata(metadata map[string]string) *ResponseBuilder {
	if metadata != nil {
		rb.metadata = metadata
	}
	return rb
}

// AddMetadata adds a single metadata entry
func (rb *ResponseBuilder) AddMetadata(key, value string) *ResponseBuilder {
	if rb.metadata == nil {
		rb.metadata = make(map[string]string)
	}
	rb.metadata[key] = value
	return rb
}

// WithState sets the state
func (rb *ResponseBuilder) WithState(state v1alpha2.State) *ResponseBuilder {
	rb.state = state
	return rb
}

// WithData sets the response data
func (rb *ResponseBuilder) WithData(data interface{}) *ResponseBuilder {
	rb.data = data
	return rb
}

// Build creates the final COA response
func (rb *ResponseBuilder) Build() (v1alpha2.COAResponse, error) {
	if rb.err != nil {
		return v1alpha2.COAResponse{}, rb.err
	}

	// Get appropriate serializer
	serializer, err := getSerializer(rb.contentType)
	if err != nil {
		return v1alpha2.COAResponse{}, fmt.Errorf("failed to get serializer: %w", err)
	}

	// Validate content type
	if err := serializer.Validate(); err != nil {
		return v1alpha2.COAResponse{}, fmt.Errorf("content type validation failed: %w", err)
	}

	// Serialize data
	var respBytes []byte
	if rb.data != nil {
		respBytes, err = serializer.Serialize(rb.data)
		if err != nil {
			return v1alpha2.COAResponse{}, fmt.Errorf("failed to serialize data: %w", err)
		}
	}

	// Ensure metadata is not nil
	if rb.metadata == nil {
		rb.metadata = make(map[string]string)
	}

	coaResponse := v1alpha2.COAResponse{
		State:       rb.state,
		Body:        respBytes,
		ContentType: rb.contentType,
		Metadata:    rb.metadata,
	}

	return observ_utils.CloseSpanWithCOAResponse(rb.span, coaResponse), nil
}

// Improved main function with better error handling
func createSuccessResponseWithHeaders[T any](
	span trace.Span,
	contentType string,
	metadata map[string]string,
	state v1alpha2.State,
	data *T,
) v1alpha2.COAResponse {

	builder := NewResponseBuilder(span).
		WithContentType(contentType).
		WithMetadata(metadata).
		WithState(state)

	if data != nil {
		builder = builder.WithData(*data)
	}

	resp, _ := builder.Build()
	return resp
}

// Alternative simpler version if you want to keep the original signature
func createSuccessResponseWithHeadersSimple[T any](
	span trace.Span,
	contentType string,
	metadata map[string]string,
	state v1alpha2.State,
	data *T,
) (v1alpha2.COAResponse, error) {

	// Initialize metadata if nil
	if metadata == nil {
		metadata = make(map[string]string)
	}

	// Validate and get serializer
	serializer, err := getSerializer(contentType)
	if err != nil {
		return v1alpha2.COAResponse{}, err
	}

	// Serialize data
	var respBytes []byte
	if data != nil {
		respBytes, err = serializer.Serialize(*data)
		if err != nil {
			return v1alpha2.COAResponse{}, fmt.Errorf("failed to serialize response data: %w", err)
		}
	}

	coaResponse := v1alpha2.COAResponse{
		State:       state,
		Body:        respBytes,
		ContentType: contentType,
		Metadata:    metadata,
	}

	return observ_utils.CloseSpanWithCOAResponse(span, coaResponse), nil
}
