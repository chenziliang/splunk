package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/chenziliang/splunk"
)

func getEvents(sourcetype, transaction string, batchSize int) []*splunk.Event {
	data := map[string]interface{}{"cf_origin": "firehose", "deployment": "cf", "event_type": "ValueMetric", "ip": "192.168.16.26", "job": "doppler", "job_index": "5a634d0b-bbc5-47c4-9450-a43f44a7fd30", "name": "messageRouter.numberOfFirehoseSinks", "origin": "DopplerServer", "unit": "sinks", "value": 1, "transaction": transaction}

	var events []*splunk.Event
	for i := 0; i < batchSize; i++ {
		event := &splunk.Event{
			Timestamp:  time.Now().UnixNano(),
			Source:     "hec-client",
			Sourcetype: sourcetype,
			Host:       "localhost",
			Data:       data,
		}
		events = append(events, event)
	}
	return events
}

func (p *HecPerf) asyncPost(idx, totalEvents int) {
	client, err := splunk.NewHttpEventAsyncClient([]string{p.hecURI}, []string{p.hecToken})
	if err != nil {
		fmt.Printf("workerId=%d Failed tto create client, err=%+v\n", idx, err)
		return
	}

	var sent int

LOOP:
	for {
		events := getEvents("hec:async", p.transaction, p.batchSize)
		err := client.WriteEvents(events)
		if err != nil {
			fmt.Printf("workerId=%d Failed to write events, error=%+v\n", idx, err)
			continue
		}

		sent += len(events)
		if sent >= totalEvents {
			fmt.Printf("workerId=%d done With total_events=%d\n", idx, totalEvents)
			break LOOP
		}
	}
	p.workers.Done()
}

func (p *HecPerf) syncPost(idx, totalEvents int) {
	client, err := splunk.NewHttpEventSyncClient(p.hecURI, p.hecToken)
	if err != nil {
		fmt.Printf("workerId=%d Failed tto create client, err=%+v\n", idx, err)
		return
	}

	var (
		sent              int
		outstandingAckIDs []splunk.AckID
	)

OUTLOOP:
	for {
		events := getEvents("hec:sync", p.transaction, p.batchSize)
		ids, err := client.WriteEvents(events)
		if err != nil {
			fmt.Printf("workerId=%d Failed to write events, error=%+v\n", idx, err)
			continue
		}

		sent += len(events)
		outstandingAckIDs = append(outstandingAckIDs, ids...)
		if len(outstandingAckIDs)%p.pollLimit != 0 {
			continue
		}

		pollId := uuid.New().String()
		fmt.Printf("workerId=%d pollId=%s Poll outstandingACKs=%d\n", idx, pollId, len(outstandingAckIDs))
		ticker := time.NewTicker(p.pollInterval)

	LOOP:
		for {
			select {
			case <-ticker.C:
				unacked, err := client.Poll(outstandingAckIDs)
				if err != nil {
					fmt.Printf("workerId=%d pollId=%s Failed to poll acks, error=%+v", idx, pollId, err)
					continue
				}

				outstandingAckIDs = unacked
				fmt.Printf("workerId=%d pollId=%s outstandingACKs=%d\n", idx, pollId, len(outstandingAckIDs))
				if len(outstandingAckIDs) == 0 {
					break LOOP
				}
			}
		}

		if sent >= totalEvents {
			fmt.Printf("workerId=%d done With total_events=%d\n", idx, totalEvents)
			break OUTLOOP
		}
	}
	p.workers.Done()
}

type HecPerf struct {
	hecURI       string
	hecToken     string
	concurrency  int
	batchSize    int
	pollLimit    int
	pollInterval time.Duration
	totalEvents  int
	syncMode     bool
	transaction  string
	workers      sync.WaitGroup
}

func NewHecPerf(hecURI, hecToken string) *HecPerf {
	return &HecPerf{
		hecURI:       hecURI,
		hecToken:     hecToken,
		concurrency:  1,
		batchSize:    1000,
		pollLimit:    10000,
		pollInterval: time.Second,
		totalEvents:  1000000,
		syncMode:     true,
		transaction:  uuid.New().String(),
	}
}

func (p *HecPerf) WithConcurrency(concurrency int) *HecPerf {
	p.concurrency = concurrency
	return p
}

func (p *HecPerf) WithBatchSize(batchSize int) *HecPerf {
	p.batchSize = batchSize
	return p
}

func (p *HecPerf) WithPollLimit(pollLimit int) *HecPerf {
	p.pollLimit = pollLimit
	return p
}

func (p *HecPerf) WithPollInterval(pollInterval time.Duration) *HecPerf {
	p.pollInterval = pollInterval
	return p
}

func (p *HecPerf) WithTotalEvents(totalEvents int) *HecPerf {
	p.totalEvents = totalEvents
	return p
}

func (p *HecPerf) WithSyncMode(syncMode bool) *HecPerf {
	p.syncMode = syncMode
	return p
}

func (p *HecPerf) writeMeta(event *splunk.Event) error {
	if p.syncMode {
		event.Sourcetype = "hec:sync"
		client, err := splunk.NewHttpEventSyncClient(p.hecURI, p.hecToken)
		if err != nil {
			fmt.Printf("Failed to create client, err=%+v\n", err)
			return err
		}

		_, err = client.WriteEvents([]*splunk.Event{event})
		if err != nil {
			fmt.Printf("Failed to write transaction info, err=%+v\n", err)
		}
		return err
	} else {
		event.Sourcetype = "hec:async"
		client, err := splunk.NewHttpEventAsyncClient([]string{p.hecURI}, []string{p.hecToken})
		if err != nil {
			fmt.Printf("Failed to create client, err=%+v\n", err)
			return err
		}

		err = client.WriteEvents([]*splunk.Event{event})
		if err != nil {
			fmt.Printf("Failed to write transaction info, err=%+v\n", err)
		}
		return err
	}
}

func (p *HecPerf) writeTxn(meta map[string]interface{}) error {
	data := map[string]interface{}{
		"concurrency":   p.concurrency,
		"batch-size":    p.batchSize,
		"poll-limit":    p.pollLimit,
		"poll-interval": int(p.pollInterval / time.Millisecond),
		"total-events":  p.totalEvents,
		"sync-mode":     p.syncMode,
		"transaction":   p.transaction,
		"time":          time.Now().UnixNano(),
	}

	for k, v := range meta {
		data[k] = v
	}

	event := &splunk.Event{
		Timestamp: time.Now().Unix(),
		Source:    "hec-client",
		Host:      "localhost",
		Data:      data,
	}

	return p.writeMeta(event)
}

func (p *HecPerf) Start() error {
	fmt.Printf("Start producing txn=%s total-events=%d concurrency=%d poll-limit=%d sync-mode=%t\n",
		p.transaction, p.totalEvents, p.concurrency, p.pollLimit, p.syncMode)
	start := time.Now().UnixNano()

	var err error
	defer func() {
		if err != nil {
			return
		}

		cost := time.Now().UnixNano() - start
		eps := int64(p.totalEvents) * int64(1000000000) / cost
		fmt.Printf("End of producing txn=%s total-events=%d concurrency=%d poll-limit=%d sync-mode=%t took=%d nanoseconds eps=%d\n",
			p.transaction, p.totalEvents, p.concurrency, p.pollLimit, p.syncMode, cost, eps)

		data := map[string]interface{}{
			"cost": cost,
			"eps":  eps,
		}

		p.writeTxn(data)
	}()

	err = p.writeTxn(nil)
	if err != nil {
		return err
	}

	fun := p.syncPost
	if !p.syncMode {
		fun = p.asyncPost
	}

	shared := p.totalEvents / p.concurrency
	for i := 0; i < p.concurrency; i++ {
		p.workers.Add(1)
		go fun(i, shared)
	}

	p.workers.Wait()
	return nil
}

func main() {
	hecURI := kingpin.Flag("hec-uri", "HEC Server URI, for example: https://localhost:8088").Required().String()
	hecToken := kingpin.Flag("hec-token", "HEC input token").Required().String()
	concurrency := kingpin.Flag("concurrency", "How many concurrent HEC post").Default("1").Int()
	batchSize := kingpin.Flag("batch-size", "How many events per post").Default("1000").Int()
	pollLimit := kingpin.Flag("poll-limit", "After sending how many events, it begins to poll").Default("10000").Int()
	pollInterval := kingpin.Flag("poll-interval", "How many seconds to wait before each poll").Default("1s").Duration()
	totalEvents := kingpin.Flag("total-events", "After sending how many events, it stops").Default("10000000").Int()
	syncMode := kingpin.Flag("sync-mode", "sync or async HEC mode").Default("true").Bool()

	kingpin.Parse()

	if *totalEvents / *concurrency < *pollLimit {
		fmt.Printf("total-events / concurrency should be greater than poll-limit\n")
		return
	}

	perf := NewHecPerf(*hecURI, *hecToken)
	perf.WithConcurrency(*concurrency).
		WithBatchSize(*batchSize).
		WithPollLimit(*pollLimit).
		WithPollInterval(*pollInterval).
		WithTotalEvents(*totalEvents).
		WithSyncMode(*syncMode)

	perf.Start()
}
