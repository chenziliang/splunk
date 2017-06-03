package splunk

import (
	"testing"
)

func TestRestClient(t *testing.T) {
	splunkURI := "https://localhost:8089"
	sr, _ := NewRestClient(splunkURI, "", 10, 120)

	_, err := sr.Login("admin", "admin")
	if err != nil {
		t.Errorf("Failed to get session key from %s, error=%s", splunkURI, err)
	}
}
