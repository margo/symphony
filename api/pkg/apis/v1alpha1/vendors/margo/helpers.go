package margo

import (
	"context"
	"encoding/json"
	"time"

	"github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
	observ_utils "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2/observability/utils"
	margoAPIModels "github.com/margo/dev-repo/non-standard/generatedCode/models"
	"go.opentelemetry.io/otel/trace"
)

// Helper method for error responses
func createErrorResponse(span trace.Span, err error, message string, errorType v1alpha2.State) v1alpha2.COAResponse {
	margoLog.InfofCtx(context.Background(), "err: %s, msg: %s", err.Error(), message)
	errResp := &margoAPIModels.ErrorResponse{
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

func createSuccessResponse[T any](span trace.Span, state v1alpha2.State, data *T) v1alpha2.COAResponse {
	response := struct {
		Data      *T        `json:"data,omitempty"`
		RequestId string    `json:"requestId"`
		Timestamp time.Time `json:"timestamp"`
	}{
		Data:      data,
		RequestId: "",
		Timestamp: time.Now().UTC(),
	}
	// ... rest unchanged

	respBytes, _ := json.Marshal(response)

	coaResponse := v1alpha2.COAResponse{
		State:       state,
		Body:        respBytes,
		ContentType: "application/json",
	}

	return observ_utils.CloseSpanWithCOAResponse(span, coaResponse)
}
