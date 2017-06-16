package splunk

import (
	"math/rand"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/pquerna/ffjson/ffjson"
)

var (
	hecHeaders = map[string]string{
		"Content-Type": "application/json",
		"Connection":   "keep-alive",
	}
)

type Event struct {
	Source     string      `json:"source" binding:"required"`
	Sourcetype string      `json:"sourcetype" binding:"required"`
	Host       string      `json:"host" binding:"required"`
	Timestamp  int64       `json:"time" binding:"required"`
	Index      string      `json:"index" binding:"required"`
	Data       interface{} `json:"event" binding:"required"`
}

type httpEventClient struct {
	clients         []*RestClient
	retries         int
	httpPayloadSize int
}

// newhttpEventClient
// serverURIs: ["https://indexer1:8088", "https://indexer2:8088",...],
// tokens":    [uuid1,uuid2,...]
func newHttpEventClient(serverURIs, tokens []string) (*httpEventClient, error) {
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

	return &httpEventClient{
		clients: clients,
		retries: 3,
	}, nil
}

func (hec *httpEventClient) WithRetries(retries int) *httpEventClient {
	hec.retries = retries
	return hec
}

func (hec *httpEventClient) WithHttpPayloadSize(maxBytes int) *httpEventClient {
	hec.httpPayloadSize = maxBytes
	return hec
}

func getHeaders(headers map[string]string) map[string]string {
	if headers == nil {
		return hecHeaders
	}

	h := make(map[string]string, len(hecHeaders)+len(headers))
	for k, v := range hecHeaders {
		h[k] = v
	}

	for k, v := range headers {
		h[k] = v
	}

	return h
}

func (hec *httpEventClient) doWriteEvents(data []byte, headers map[string]string) ([]byte, error) {
	return hec.post("/services/collector", data, headers)
}

func (hec *httpEventClient) post(uri string, data []byte, headers map[string]string) ([]byte, error) {
	start := 0
	if len(hec.clients) > 1 {
		rand.Seed(time.Now().UTC().UnixNano())
		start = rand.Intn(len(hec.clients))
	}

	var (
		err  error
		resp []byte
	)

	h := getHeaders(headers)

	for i := 0; i < len(hec.clients); i++ {
		client := hec.clients[start]
		resp, err = client.Request(uri, "POST", h, data, 1)
		if err == nil {
			return resp, nil
		}

		errStr := err.Error()
		if strings.Contains(errStr, "No data") || strings.Contains(errStr, "invalid-event-number") {
			return nil, err
		}
		start = (start + 1) % len(hec.clients)
	}

	return nil, err
}

func (hec *httpEventClient) prepareData(events []*Event) [][]byte {
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
