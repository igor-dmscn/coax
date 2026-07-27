// This file is in package coax_test, not cable, because the conformance harness
// imports cable and a test in package cable could not import it back.
package coax_test

import (
	"testing"

	"github.com/igor-dmscn/coax-claude-impl/coax"
	"github.com/igor-dmscn/coax-claude-impl/coax/pubsubtest"
)

func TestMemoryPubSubConformance(t *testing.T) {
	pubsubtest.Run(t, func(t *testing.T) coax.PubSub {
		ps := coax.NewMemoryPubSub()
		t.Cleanup(func() { ps.Close() })
		return ps
	})
}
