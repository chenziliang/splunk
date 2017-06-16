package splunk

type HttpEventAsyncClient struct {
	*httpEventClient
}

func NewHttpEventAsyncClient(serverURIs, tokens []string) (*HttpEventAsyncClient, error) {
	httpClient, err := newHttpEventClient(serverURIs, tokens)
	if err != nil {
		return nil, err
	}

	return &HttpEventAsyncClient{
		httpEventClient: httpClient,
	}, nil
}

func (hec *HttpEventAsyncClient) WriteEvents(events []*Event) error {
	var err error

	allData := hec.prepareData(events)
	for _, data := range allData {
		for i := 0; i < hec.retries; i++ {
			_, err = hec.doWriteEvents(data, nil)
			if err == nil {
				break
			}
		}

		if err != nil {
			return err
		}
	}

	return nil
}
