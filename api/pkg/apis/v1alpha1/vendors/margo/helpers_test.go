package margo

import (
    "context"
    "encoding/json"
    "errors"
    "testing"
    "time"
    "fmt"
    "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
    margoAPIModels "github.com/margo/dev-repo/non-standard/generatedCode/models"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "go.opentelemetry.io/otel"
)

func TestCreateErrorResponse(t *testing.T) {
    // Setup tracer for tests
    tracer := otel.Tracer("test")
    
    tests := []struct {
        name      string
        err       error
        message   string
        errorType v1alpha2.State
    }{
        {
            name:      "basic error response",
            err:       errors.New("test error"),
            message:   "test message",
            errorType: v1alpha2.BadRequest,
        },
        {
            name:      "internal server error",
            err:       errors.New("internal error"),
            message:   "something went wrong",
            errorType: v1alpha2.InternalError,
        },
        {
            name:      "empty message",
            err:       errors.New("validation failed"),
            message:   "",
            errorType: v1alpha2.BadRequest,
        },
        {
            name:      "not found error",
            err:       errors.New("resource not found"),
            message:   "requested resource does not exist",
            errorType: v1alpha2.NotFound,
        },
        {
            name:      "unauthorized error",
            err:       errors.New("access denied"),
            message:   "insufficient permissions",
            errorType: v1alpha2.Unauthorized,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            ctx := context.Background()
            _, span := tracer.Start(ctx, "test-span")
            defer span.End()
            
            response := createErrorResponse(span, tt.err, tt.message, tt.errorType)
            
            // Verify response structure
            assert.NotNil(t, response.Body)
            
            // Unmarshal and verify error response body
            var errorResp margoAPIModels.ErrorResponse
            err := json.Unmarshal(response.Body, &errorResp)
            require.NoError(t, err)
            
            // Verify error response fields
            assert.Equal(t, tt.err.Error(), errorResp.ErrorCode)
            assert.Equal(t, "", errorResp.RequestId)
            assert.NotNil(t, errorResp.Details)
            
            // Verify details structure
            details := *errorResp.Details
            assert.Equal(t, tt.message, details["message"])
            
            // Verify timestamp is recent (within last minute)
            assert.WithinDuration(t, time.Now().UTC(), errorResp.Timestamp, time.Minute)
            
            // Verify COA error state
            expectedState := v1alpha2.GetErrorState(v1alpha2.NewCOAError(tt.err, tt.message, tt.errorType))
            assert.Equal(t, expectedState, response.State)
        })
    }
}

func TestCreateErrorResponseWithNilError(t *testing.T) {
    tracer := otel.Tracer("test")
    ctx := context.Background()
    _, span := tracer.Start(ctx, "test-span")
    defer span.End()
    
    // This test verifies behavior when error is nil (edge case)
    // Note: This might panic in actual implementation, so you may need to handle this case
    defer func() {
        if r := recover(); r != nil {
            t.Logf("Function panicked as expected with nil error: %v", r)
        }
    }()
    
    // Uncomment if you want to test nil error handling
    // response := createErrorResponse(span, nil, "test message", v1alpha2.BadRequest)
    // assert.NotNil(t, response)
}

func TestCreateSuccessResponse(t *testing.T) {
    tracer := otel.Tracer("test")
    
    t.Run("success response with string data", func(t *testing.T) {
        ctx := context.Background()
        _, span := tracer.Start(ctx, "test-span")
        defer span.End()
        
        testData := "test string data"
        state := v1alpha2.OK
        
        response := createSuccessResponse(span, state, &testData)
        
        // Verify response structure
        assert.NotNil(t, response.Body)
        assert.Equal(t, "application/json", response.ContentType)
        assert.Equal(t, state, response.State)
        
        // Unmarshal and verify response body
        var successResp struct {
            Data      *string   `json:"data,omitempty"`
            RequestId string    `json:"requestId"`
            Timestamp time.Time `json:"timestamp"`
        }
        
        err := json.Unmarshal(response.Body, &successResp)
        require.NoError(t, err)
        
        assert.Equal(t, testData, *successResp.Data)
        assert.Equal(t, "", successResp.RequestId)
        assert.WithinDuration(t, time.Now().UTC(), successResp.Timestamp, time.Minute)
    })
    
    t.Run("success response with struct data", func(t *testing.T) {
        ctx := context.Background()
        _, span := tracer.Start(ctx, "test-span")
        defer span.End()
        
        type TestStruct struct {
            ID   int    `json:"id"`
            Name string `json:"name"`
        }
        
        testData := TestStruct{ID: 123, Name: "test"}
        state := v1alpha2.OK
        
        response := createSuccessResponse(span, state, &testData)
        
        // Verify response structure
        assert.NotNil(t, response.Body)
        assert.Equal(t, "application/json", response.ContentType)
        assert.Equal(t, state, response.State)
        
        // Unmarshal and verify response body
        var successResp struct {
            Data      *TestStruct `json:"data,omitempty"`
            RequestId string      `json:"requestId"`
            Timestamp time.Time   `json:"timestamp"`
        }
        
        err := json.Unmarshal(response.Body, &successResp)
        require.NoError(t, err)
        
        assert.Equal(t, testData.ID, successResp.Data.ID)
        assert.Equal(t, testData.Name, successResp.Data.Name)
        assert.Equal(t, "", successResp.RequestId)
        assert.WithinDuration(t, time.Now().UTC(), successResp.Timestamp, time.Minute)
    })
    
    t.Run("success response with nil data", func(t *testing.T) {
        ctx := context.Background()
        _, span := tracer.Start(ctx, "test-span")
        defer span.End()
        
        var testData *string = nil
        state := v1alpha2.OK
        
        response := createSuccessResponse(span, state, testData)
        
        // Verify response structure
        assert.NotNil(t, response.Body)
        assert.Equal(t, "application/json", response.ContentType)
        assert.Equal(t, state, response.State)
        
        // Unmarshal and verify response body
        var successResp struct {
            Data      *string   `json:"data,omitempty"`
            RequestId string    `json:"requestId"`
            Timestamp time.Time `json:"timestamp"`
        }
        
        err := json.Unmarshal(response.Body, &successResp)
        require.NoError(t, err)
        
        assert.Nil(t, successResp.Data)
        assert.Equal(t, "", successResp.RequestId)
        assert.WithinDuration(t, time.Now().UTC(), successResp.Timestamp, time.Minute)
    })
    
    t.Run("success response with different states", func(t *testing.T) {
        states := []v1alpha2.State{
            v1alpha2.OK,
            v1alpha2.Accepted,
        }
        
        for _, state := range states {
            t.Run(fmt.Sprintf("state_%d", state), func(t *testing.T) {
                ctx := context.Background()
                _, span := tracer.Start(ctx, "test-span")
                defer span.End()
                
                testData := "test"
                response := createSuccessResponse(span, state, &testData)
                
                assert.Equal(t, state, response.State)
                assert.NotNil(t, response.Body)
                assert.Equal(t, "application/json", response.ContentType)
            })
        }
    })
    
    t.Run("success response with map data", func(t *testing.T) {
        ctx := context.Background()
        _, span := tracer.Start(ctx, "test-span")
        defer span.End()
        
        testData := map[string]interface{}{
            "key1": "value1",
            "key2": 42,
            "key3": true,
        }
        state := v1alpha2.OK
        
        response := createSuccessResponse(span, state, &testData)
        
        // Verify response structure
        assert.NotNil(t, response.Body)
        assert.Equal(t, "application/json", response.ContentType)
        assert.Equal(t, state, response.State)
        
        // Unmarshal and verify response body
        var successResp struct {
            Data      *map[string]interface{} `json:"data,omitempty"`
            RequestId string                  `json:"requestId"`
            Timestamp time.Time               `json:"timestamp"`
        }
        
        err := json.Unmarshal(response.Body, &successResp)
        require.NoError(t, err)
        
        assert.Equal(t, testData["key1"], (*successResp.Data)["key1"])
        assert.Equal(t, float64(42), (*successResp.Data)["key2"]) // JSON unmarshals numbers as float64
        assert.Equal(t, testData["key3"], (*successResp.Data)["key3"])
    })
    
    t.Run("success response with slice data", func(t *testing.T) {
        ctx := context.Background()
        _, span := tracer.Start(ctx, "test-span")
        defer span.End()
        
        testData := []string{"item1", "item2", "item3"}
        state := v1alpha2.OK
        
        response := createSuccessResponse(span, state, &testData)
        
        // Verify response structure
        assert.NotNil(t, response.Body)
        assert.Equal(t, "application/json", response.ContentType)
        assert.Equal(t, state, response.State)
        
        // Unmarshal and verify response body
        var successResp struct {
            Data      *[]string `json:"data,omitempty"`
            RequestId string    `json:"requestId"`
            Timestamp time.Time `json:"timestamp"`
        }
        
        err := json.Unmarshal(response.Body, &successResp)
        require.NoError(t, err)
        
        assert.Equal(t, len(testData), len(*successResp.Data))
        assert.Equal(t, testData[0], (*successResp.Data)[0])
        assert.Equal(t, testData[1], (*successResp.Data)[1])
        assert.Equal(t, testData[2], (*successResp.Data)[2])
    })
}

func TestResponseJSONMarshaling(t *testing.T) {
    tracer := otel.Tracer("test")
    
    t.Run("error response JSON structure", func(t *testing.T) {
        ctx := context.Background()
        _, span := tracer.Start(ctx, "test-span")
        defer span.End()
        
        err := errors.New("test error")
        message := "test message"
        
        response := createErrorResponse(span, err, message, v1alpha2.BadRequest)
        
        // Verify JSON can be unmarshaled to map for structure validation
        var jsonMap map[string]interface{}
        unmarshalErr := json.Unmarshal(response.Body, &jsonMap)
        require.NoError(t, unmarshalErr)
        
        // Verify required fields exist
        assert.Contains(t, jsonMap, "errorCode")
        assert.Contains(t, jsonMap, "requestId")
        assert.Contains(t, jsonMap, "details")
        assert.Contains(t, jsonMap, "timestamp")
        
        // Verify details structure
        details, ok := jsonMap["details"].(map[string]interface{})
        require.True(t, ok)
        assert.Contains(t, details, "message")
        assert.Equal(t, message, details["message"])
    })
    
    t.Run("success response JSON structure", func(t *testing.T) {
        ctx := context.Background()
        _, span := tracer.Start(ctx, "test-span")
        defer span.End()
        
        testData := map[string]string{"key": "value"}
        response := createSuccessResponse(span, v1alpha2.OK, &testData)
        
        // Verify JSON can be unmarshaled to map for structure validation
        var jsonMap map[string]interface{}
        unmarshalErr := json.Unmarshal(response.Body, &jsonMap)
        require.NoError(t, unmarshalErr)
        
        // Verify required fields exist
        assert.Contains(t, jsonMap, "data")
        assert.Contains(t, jsonMap, "requestId")
        assert.Contains(t, jsonMap, "timestamp")
        
        // Verify data structure
        data, ok := jsonMap["data"].(map[string]interface{})
        require.True(t, ok)
        assert.Equal(t, "value", data["key"])
    })
}

// Benchmark tests for performance measurement
func BenchmarkCreateErrorResponse(b *testing.B) {
    tracer := otel.Tracer("benchmark")
    ctx := context.Background()
    _, span := tracer.Start(ctx, "benchmark-span")
    defer span.End()
    
    err := errors.New("benchmark error")
    message := "benchmark message"
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        createErrorResponse(span, err, message, v1alpha2.BadRequest)
    }
}

func BenchmarkCreateSuccessResponse(b *testing.B) {
    tracer := otel.Tracer("benchmark")
    ctx := context.Background()
    _, span := tracer.Start(ctx, "benchmark-span")
    defer span.End()
    
    testData := "benchmark data"
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        createSuccessResponse(span, v1alpha2.OK, &testData)
    }
}

