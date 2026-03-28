package witnesstests

import (
	"testing"

	"github.com/openfga/openfga/tests/check"
	"github.com/openfga/openfga/tests/listobjects"
	"github.com/openfga/openfga/tests/listusers"
)

// TestConformance runs OpenFGA's full matrix test suite (Check, ListObjects,
// ListUsers) against the witness server to verify that the audit interceptors
// do not alter any API behaviour.
func TestConformance(t *testing.T) {
	client, _ := startTestServer(t)

	t.Run("Check", func(t *testing.T) {
		check.RunAllTests(t, client)
	})
	t.Run("ListObjects", func(t *testing.T) {
		listobjects.RunAllTests(t, client)
	})
	t.Run("ListUsers", func(t *testing.T) {
		listusers.RunAllTests(t, client)
	})
}
