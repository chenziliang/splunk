package splunk

import (
	"encoding/json"
	"testing"
	"time"
)

func TestHttpEventAsyncClient(t *testing.T) {
	e := "i love you"
	ee, _ := json.Marshal(&e)
	e2 := map[string]string{
		"1": "2",
	}
	ee2, _ := json.Marshal(&e2)

	event := &Event{
		Index:      "main",
		Source:     "descartes",
		Sourcetype: "descartes:event",
		Host:       "localhost",
		Timestamp:  time.Now().UTC().Unix(),
		Data:       []string{string(ee), string(ee2)},
	}

	events := []*Event{event}

	serverURIs := []string{"https://localhost:8088"}
	tokens := []string{"810E895F-820A-4591-BB7F-0B1D805924FB"}
	client, err := NewHttpEventAsyncClient(serverURIs, tokens)
	if err != nil {
		t.Errorf("Failed to create client, error=%s", err)
	}

	err = client.WriteEvents(events)
	if err != nil {
		t.Errorf("Failed to index data from %s, error=%s", serverURIs, err)
	}
}
