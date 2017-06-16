package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/chenziliang/splunk"
)

func getEvents(sourcetype string) []*splunk.Event {
	data := map[string]interface{}{"cf_origin": "firehose", "deployment": "cf", "event_type": "ValueMetric", "ip": "192.168.16.26", "job": "doppler", "job_index": "5a634d0b-bbc5-47c4-9450-a43f44a7fd30", "name": "messageRouter.numberOfFirehoseSinks", "origin": "DopplerServer", "unit": "sinks", "value": 1}

	var events []*splunk.Event
	for i := 0; i < 1000; i++ {
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

func (p *hecPerf) asyncPost(idx, totalEvents int) {
	client, err := splunk.NewHttpEventAsyncClient([]string{p.hecURI}, []string{p.hecToken})
	if err != nil {
		fmt.Printf("workerId=%d Failed tto create client, err=%+v\n", idx, err)
		return
	}

	var sent int

LOOP:
	for {
		events := getEvents("hec:async")
		err := client.WriteEvents(events)
		if err != nil {
			fmt.Printf("workerId=%d Failed to write events, error=%+v\n", idx, err)
			continue
		}

		sent += len(events)
		if sent >= totalEvents {
			fmt.Printf("workerId=%d done with total_events=%d\n", idx, totalEvents)
			break LOOP
		}
	}
	p.workers.Done()
}

func (p *hecPerf) syncPost(idx, totalEvents int) {
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
		events := getEvents("hec:sync")
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
			fmt.Printf("workerId=%d done with total_events=%d\n", idx, totalEvents)
			break OUTLOOP
		}
	}
	p.workers.Done()
}

type hecPerf struct {
	hecURI       string
	hecToken     string
	concurrency  int
	pollLimit    int
	pollInterval time.Duration
	totalEvents  int
	syncMode     bool
	workers      sync.WaitGroup
}

func newHecPerf(hecURI, hecToken string, concurrency, totalEvents, pollLimit int, pollInterval time.Duration, syncMode bool) *hecPerf {
	return &hecPerf{
		hecURI:       hecURI,
		hecToken:     hecToken,
		concurrency:  concurrency,
		pollLimit:    pollLimit,
		pollInterval: pollInterval,
		totalEvents:  totalEvents,
		syncMode:     syncMode,
	}
}

func (p *hecPerf) Start() {
	fun := p.syncPost
	if !p.syncMode {
		fun = p.asyncPost
	}

	shared := p.totalEvents / p.concurrency
	for i := 0; i < p.concurrency-1; i++ {
		p.workers.Add(1)
		go fun(i, shared)
	}

	go fun(p.concurrency, shared)
	p.workers.Wait()
}

func main() {
	hecURI := kingpin.Flag("hec-uri", "HEC Server URI, for example: https://localhost:8088").Required().String()
	hecToken := kingpin.Flag("hec-token", "HEC input token").Required().String()
	concurrency := kingpin.Flag("concurrency", "How many concurrent HEC post").Default("1").Int()
	pollLimit := kingpin.Flag("poll-limit", "After sending how many events, it begins to poll").Default("10000").Int()
	pollInterval := kingpin.Flag("poll-interval", "How many seconds to wait before each poll").Default("1s").Duration()
	totalEvents := kingpin.Flag("total-events", "After sending how many events, it stops").Default("10000000").Int()
	syncMode := kingpin.Flag("sync-mode", "sync or async HEC mode").Default("true").Bool()

	kingpin.Parse()

	if *totalEvents / *concurrency < *pollLimit {
		fmt.Printf("total-events / concurrency should be greater than poll-limit\n")
		return
	}

	perf := newHecPerf(*hecURI, *hecToken, *concurrency, *totalEvents, *pollLimit, *pollInterval, *syncMode)
	fmt.Printf("Start producing total_events=%d concurrency=%d poll_limit=%d sync_mode=%t\n",
		*totalEvents, *concurrency, *pollLimit, *syncMode)
	start := time.Now().UnixNano()
	perf.Start()
	cost := time.Now().UnixNano() - start
	fmt.Printf("End of producing total_events=%d concurrency=%d poll-limit=%d sync_mode=%t took=%d nanoseconds eps=%d\n",
		*totalEvents, *concurrency, *pollLimit, *syncMode, cost, int64(*totalEvents)*int64(1000000000)/cost)
}
