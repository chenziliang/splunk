package splunk

import (
	"math/rand"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/pquerna/ffjson/ffjson"
)

var headers = map[string]string{"Content-Type": "application/json", "Connection": "keep-alive"}

type Event struct {
	Source     string      `json:"source" binding:"required"`
	Sourcetype string      `json:"sourcetype" binding:"required"`
	Host       string      `json:"host" binding:"required"`
	Timestamp  int64       `json:"time" binding:"required"`
	Index      string      `json:"index" binding:"required"`
	Data       interface{} `json:"event" binding:"required"`
}

type HttpEventClient struct {
	clients         []*RestClient
	retries         int
	httpPayloadSize int
}

// NewHttpEventClient
// serverURIs: ["https://indexer1:8088", "https://indexer2:8088",...],
// tokens":    [uuid1,uuid2,...]
func NewHttpEventClient(serverURIs []string, tokens []string) (*HttpEventClient, error) {
	if len(serverURIs) != len(tokens) && len(tokens) != 1 {
		return nil, errors.New("serverURI and token number is not matched")
	}

	if len(tokens) == 1 {
		// shared one token
		for i := 0; i < len(serverURIs)-1; i++ {
			tokens = append(tokens, tokens[0])
		}
	}

	var clients []*RestClient
	for idx := range serverURIs {
		client, _ := NewRestClient(serverURIs[idx], tokens[idx], 2, 120)
		clients = append(clients, client)
	}

	return &HttpEventClient{
		clients: clients,
		retries: 3,
	}, nil
}

func (hec *HttpEventClient) WithRetries(retries int) *HttpEventClient {
	hec.retries = retries
	return hec
}

func (hec *HttpEventClient) WithHttpPayloadSize(maxBytes int) *HttpEventClient {
	hec.httpPayloadSize = maxBytes
	return hec
}

func (hec *HttpEventClient) WriteEvents(events []*Event) error {
	var err error
	allData := hec.prepareData(events)
	for _, data := range allData {
		for i := 0; i < hec.retries; i++ {
			err = hec.doWriteEvents(data)
			if err == nil {
				break
			}
		}
	}
	return err
}

func (hec *HttpEventClient) doWriteEvents(data []byte) error {
	start := 0
	if len(hec.clients) > 1 {
		rand.Seed(time.Now().UTC().Unix())
		start = rand.Intn(len(hec.clients))
	}

	var err error
	for i := 0; i < len(hec.clients); i++ {
		client := hec.clients[start]
		_, err = client.Request("/services/collector", "POST", headers, data, 1)
		if err == nil {
			return nil
		}

		errStr := err.Error()
		if strings.Contains(errStr, "No data") || strings.Contains(errStr, "invalid-event-number") {
			return err
		}
		start = (start + 1) % len(hec.clients)
	}
	return err
}

func (hec *HttpEventClient) prepareData(events []*Event) [][]byte {
	allData := make([][]byte, 0, 1)
	var data []byte
	for _, event := range events {
		if event.Data == nil {
			continue
		}

		if event.Index == "" || event.Index == "default" {
			event.Index = "main"
		}

		d, _ := ffjson.Marshal(event)
		if len(data)+len(d)+1 > hec.httpPayloadSize {
			if data != nil {
				allData = append(allData, data)
				data = nil
			}
		}
		data = append(data, d...)
		data = append(data, '\n')
	}

	if data != nil {
		allData = append(allData, data)
	}
	return allData
}
