// api/pkg/apis/v1alpha1/vendors/margo/problem_response.go
package margo

import (
    "net/http"

    "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
    sbi "github.com/margo/sandbox/standard/generatedCode/wfm/sbi"
)

// problemToCOAState maps HTTP status to COA state.
func problemToCOAState(httpStatus int) v1alpha2.State {
    switch httpStatus {
    case http.StatusBadRequest:
        return v1alpha2.BadRequest
    case http.StatusUnprocessableEntity:
        return v1alpha2.BadRequest
    case http.StatusForbidden:
        return v1alpha2.Forbidden
    case http.StatusNotFound:
        return v1alpha2.NotFound
    case http.StatusNotAcceptable:
        return v1alpha2.NotAcceptable
    case http.StatusConflict:
        return v1alpha2.Conflict
    default:
        return v1alpha2.InternalError
    }
}

// problemResponse converts a *sbi.ProblemDetail into a COAResponse.
func problemResponse(pd *sbi.ProblemDetail) v1alpha2.COAResponse {
    body, _ := pd.MarshalJSON()
    return v1alpha2.COAResponse{
        State:       problemToCOAState(pd.Status),
        Body:        body,
        ContentType: sbi.ProblemContentType,
    }
}
