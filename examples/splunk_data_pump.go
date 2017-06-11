package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chenziliang/splunk"
	"github.com/pquerna/ffjson/ffjson"
	"gopkg.in/alecthomas/kingpin.v2"
)

type SplunkDataPump struct {
	clients     []*splunk.HttpEventClient
	workersWG   sync.WaitGroup
	totalEvents int64
	batchCount  int
	eps         int
	index       string
	source      string
	sourcetype  string
	rawEvent    interface{}
	cancelChan  chan struct{}
	started     int32
}

func NewSplunkDataPump(uris string, tokens string, concurrency int, rawEvent interface{}) (*SplunkDataPump, error) {
	var clients []*splunk.HttpEventClient
	uriArray := strings.Split(uris, ",")
	tokenArray := strings.Split(tokens, ",")
	for i := 0; i < concurrency; i++ {
		client, err := splunk.NewHttpEventClient(uriArray, tokenArray)
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}

	return &SplunkDataPump{
		clients:     clients,
		totalEvents: -1,
		batchCount:  100,
		eps:         -1,
		index:       "main",
		source:      "hec:client",
		sourcetype:  "hec:perf",
		rawEvent:    rawEvent,
		started:     0,
	}, nil
}

func (dp *SplunkDataPump) WithIndex(index string) *SplunkDataPump {
	dp.index = index
	return dp
}

func (dp *SplunkDataPump) WithSourcetype(sourcetype string) *SplunkDataPump {
	dp.sourcetype = sourcetype
	return dp
}

func (dp *SplunkDataPump) WithSource(source string) *SplunkDataPump {
	dp.source = source
	return dp
}

func (dp *SplunkDataPump) WithBatchCount(batchCount int) *SplunkDataPump {
	dp.batchCount = batchCount
	return dp
}

func (dp *SplunkDataPump) WithTotalEvents(totalEvents int64) *SplunkDataPump {
	dp.totalEvents = totalEvents
	return dp
}

func (dp *SplunkDataPump) WithEps(eps int) *SplunkDataPump {
	dp.eps = eps
	return dp
}

func (dp *SplunkDataPump) doIndexEvents(events chan *splunk.Event, idx int) {
	var batches []*splunk.Event

	for {
		select {
		case <-dp.cancelChan:
			return
		case event := <-events:
			batches = append(batches, event)
			if len(batches) > dp.batchCount {
				dp.clients[idx].WriteEvents(batches)
				batches = batches[:0]
			}
		}
	}
}

func (dp *SplunkDataPump) concurrency() int64 {
	return int64(len(dp.clients))
}

func (dp *SplunkDataPump) indexEvents(idx int) {
	if dp.eps < 0 {
		dp.indexEventsAsFastAsPossible(idx)
		return
	}

	durationPerEvent := time.Duration(int64(time.Second) / int64(dp.eps))
	ticker := time.NewTicker(durationPerEvent)
	tickerChan := ticker.C
	eventsChan := make(chan *splunk.Event, dp.eps+1)

	go dp.doIndexEvents(eventsChan, idx)

	shared := int64(-1)
	if dp.totalEvents > 0 {
		if idx == 0 {
			shared = dp.totalEvents%dp.concurrency() + dp.totalEvents/dp.concurrency()
		} else {
			shared = dp.totalEvents / dp.concurrency()
		}
	}

	startTime := time.Now().UnixNano()
	eventSent := int64(0)
	LOOP:
	for {
		select {
		case <-dp.cancelChan:
			break LOOP
		case t := <-tickerChan:
			event := &splunk.Event{
				Index:      dp.index,
				Sourcetype: dp.sourcetype,
				Source:     dp.source,
				Data:       dp.rawEvent,
				Timestamp:  t.UnixNano(),
			}
			eventsChan <- event

			eventSent += 1
			if shared > 0 && eventSent >= shared {
				break LOOP
			}

			if eventSent%100000 == 0 {
				duration := time.Now().UnixNano() - startTime
				fmt.Printf("Generated %d events in %d nano-seconds, actual_eps=%d, required_eps=%d\n",
					eventSent, duration, eventSent*1000000000/duration, dp.eps)
			}
		}
	}

	duration := time.Now().UnixNano() - startTime
	fmt.Printf("Done with generation. Generated %d events in %d nano-seconds, actual_eps=%d, required_eps=%d\n",
		eventSent, duration, eventSent*1000000000/duration, dp.eps)

	ticker.Stop()
	dp.workersWG.Done()
}

func (dp *SplunkDataPump) indexEventsAsFastAsPossible(idx int) {
	eventsChan := make(chan *splunk.Event, 100000)
	go dp.doIndexEvents(eventsChan, idx)

	shared := int64(-1)
	if dp.totalEvents > 0 {
		if idx == 0 {
			shared = dp.totalEvents%dp.concurrency() + dp.totalEvents/dp.concurrency()
		} else {
			shared = dp.totalEvents / dp.concurrency()
		}
	}

	eventSent := int64(0)
	startTime := time.Now().UnixNano()

LOOP:
	for {
		event := &splunk.Event{
			Index:      dp.index,
			Sourcetype: dp.sourcetype,
			Source:     dp.source,
			Data:       dp.rawEvent,
			Timestamp:  time.Now().UnixNano(),
		}

		select {
		case eventsChan <- event:
			eventSent += 1

			if shared > 0 && eventSent >= shared {
				break LOOP
			}

			if eventSent%100000 == 0 {
				duration := time.Now().UnixNano() - startTime
				fmt.Printf("Generated %d events in %d nano-seconds, actual_eps=%d, required_eps=%d\n",
					eventSent, duration, eventSent*1000000000/duration, dp.eps)
			}

		case <-dp.cancelChan:
			break LOOP
		}
	}

	duration := time.Now().UnixNano() - startTime
	fmt.Printf("Done with generation. Generated %d events in %d nano-seconds, actual_eps=%d, required_eps=%d\n",
		eventSent, duration, eventSent*1000000000/duration, dp.eps)
	dp.workersWG.Done()
}

func (dp *SplunkDataPump) Start() {
	if !atomic.CompareAndSwapInt32(&dp.started, 0, 1) {
		return
	}

	dp.cancelChan = make(chan struct{})
	for i := 0; i < len(dp.clients); i++ {
		dp.workersWG.Add(1)
		go dp.indexEvents(i)
	}
}

func (dp *SplunkDataPump) Stop() {
	if !atomic.CompareAndSwapInt32(&dp.started, 1, 0) {
		return
	}

	close(dp.cancelChan)
	dp.workersWG.Wait()
}

func main() {
	uris := kingpin.Flag("uris", "Splunk HEC URI, separated by ','").Required().String()
	tokens := kingpin.Flag("tokens", "Splunk HEC tokens corresponding to uris, separated by ','").Required().String()
	concurrency := kingpin.Flag("concurrency", "concurrency. Number of goroutines to do http request").Default("1").Int()
	batchCount := kingpin.Flag("batch", "Batch count").Default("1").Int()
	totalEvents := kingpin.Flag("total", "Total event count").Default("-1").Int64()
	eps := kingpin.Flag("eps", "Avarage event per second").Default("100").Int()
	index := kingpin.Flag("index", "Splunk index").Default("main").String()
	sourcetype := kingpin.Flag("sourcetype", "Event sourcetype").Default("hec:perf").String()
	source := kingpin.Flag("source", "Event source").Default("hec:client").String()
	event := kingpin.Flag("event", "Raw event").Required().String()
	format := kingpin.Flag("format", "Event format (plaintext or json)").Default("plaintext").String()

	kingpin.Parse()

	var rawEvent interface{}
	if *format == "json" {
		ffjson.Unmarshal([]byte(*event), &rawEvent)
	} else {
		rawEvent = event
	}

	dp, err := NewSplunkDataPump(*uris, *tokens, *concurrency, rawEvent)
	if err != nil {
		return
	}

	dp.WithIndex(*index).
		WithSource(*source).
		WithSourcetype(*sourcetype).
		WithEps(*eps).
		WithBatchCount(*batchCount).
		WithTotalEvents(*totalEvents)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	dp.Start()
	<-signalCh
	dp.Stop()
}
