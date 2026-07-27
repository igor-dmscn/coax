// This file is in package cable_test, not cable, because the conformance harness
// imports cable and a test in package cable could not import it back.
package cable_test

import (
	"testing"

	"go-cable/cable"
	"go-cable/cable/pubsubtest"
)

func TestMemoryPubSubConformance(t *testing.T) {
	pubsubtest.Run(t, func(t *testing.T) cable.PubSub {
		ps := cable.NewMemoryPubSub()
		t.Cleanup(func() { ps.Close() })
		return ps
	})
}
