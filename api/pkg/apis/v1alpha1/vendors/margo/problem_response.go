// api/pkg/apis/v1alpha1/vendors/margo/problem_response.go
package margo

import (
    "github.com/eclipse-symphony/symphony/coa/pkg/apis/v1alpha2"
    sbi "github.com/margo/sandbox/standard/generatedCode/wfm/sbi"
)

// problemResponse converts a *sbi.ProblemDetail into a COAResponse.
func problemResponse(pd *sbi.ProblemDetail) v1alpha2.COAResponse {
    body, _ := pd.MarshalJSON()
    return v1alpha2.COAResponse{
        State:       v1alpha2.GetHttpStatus(pd.Status),
        Body:        body,
        ContentType: sbi.ProblemContentType,
    }
}
