package ember

import (
	"testing"
)

// These are build-only tests to verify the client compiles correctly.
// Integration tests require a running ember server and are in a separate
// test suite (see CI configuration).

func TestDialInvalidAddress(t *testing.T) {
	// Dial with gRPC lazy connections won't fail immediately,
	// but we can verify the client is created without panic.
	c, err := Dial("localhost:0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer c.Close()
}

func TestDialWithPassword(t *testing.T) {
	c, err := Dial("localhost:0", WithPassword("secret"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer c.Close()
}
