package splunk

import (
	"strconv"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/pquerna/ffjson/ffjson"
)

type HttpEventSyncClient struct {
	*httpEventClient
	headers map[string]string
}

type AckID int

type ackResponse struct {
	Acks map[string]bool `json:"acks" binding:"required"`
}

type postResponse struct {
	AckID AckID `json:"ackId" binding:"required"`
}

// NewHttpEventClient
// serverURI: "https://indexer1:8088"
// token":    uuid1
func NewHttpEventSyncClient(serverURI, token string) (*HttpEventSyncClient, error) {
	httpClient, err := newHttpEventClient([]string{serverURI}, []string{token})
	if err != nil {
		return nil, err
	}

	return &HttpEventSyncClient{
		httpEventClient: httpClient,
		headers:         map[string]string{"X-Splunk-Request-Channel": uuid.New().String()},
	}, nil
}

func (hec *HttpEventSyncClient) WithChannelID(channelID string) *HttpEventSyncClient {
	hec.headers["X-Splunk-Request-Channel"] = channelID
	return hec
}

func extractAckID(resp []byte) (AckID, error) {
	var r postResponse
	err := ffjson.Unmarshal(resp, &r)
	if err != nil {
		return AckID(0), err
	}

	if r.AckID != 0 {
		return r.AckID, nil
	}

	return AckID(0), errors.New("Failed to extract ack ID")
}

func (hec *HttpEventSyncClient) WriteEvents(events []*Event) ([]AckID, error) {
	var (
		err    error
		ackIDs []AckID
		id     AckID
		resp   []byte
	)

	allData := hec.prepareData(events)
	for _, data := range allData {
		for i := 0; i < hec.retries; i++ {
			resp, err = hec.doWriteEvents(data, hec.headers)
			if err == nil {
				id, err = extractAckID(resp)
				if err == nil {
					ackIDs = append(ackIDs, id)
					break
				}
			}
		}

		// if there is err, we error out
		if err != nil {
			return nil, err
		}
	}

	return ackIDs, nil
}

func unAcked(ackResp *ackResponse) ([]AckID, error) {
	var unAckIDs []AckID
	for id, acked := range ackResp.Acks {
		if acked {
			continue
		}

		ackID, err := strconv.Atoi(id)
		if err != nil {
			return nil, err
		}

		unAckIDs = append(unAckIDs, AckID(ackID))
	}

	return unAckIDs, nil
}

// Poll polls the status of event acks and return un-acked AckIDs
func (hec *HttpEventSyncClient) Poll(ackIDs []AckID) ([]AckID, error) {
	m := map[string][]AckID{
		"acks": ackIDs,
	}

	data, err := ffjson.Marshal(m)
	if err != nil {
		return ackIDs, err
	}

	resp, err := hec.post("/services/collector/ack", data, hec.headers)
	if err != nil {
		return ackIDs, err
	}

	var ackResp ackResponse
	err = ffjson.Unmarshal(resp, &ackResp)
	if err != nil {
		return ackIDs, err
	}

	unAcks, err := unAcked(&ackResp)
	if err != nil {
		return ackIDs, err
	}

	return unAcks, nil
}
