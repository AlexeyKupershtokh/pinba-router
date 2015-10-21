package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/url"
	"time"

	"github.com/golang/protobuf/proto"
	client "github.com/influxdb/influxdb/client/v2"
	"github.com/mkevac/gopinba/Pinba"
)

var (
	inAddr = flag.String("in", "0.0.0.0:30002", "incoming socket")
)

func main() {
	flag.Parse()

	addr, err := net.ResolveUDPAddr("udp4", *inAddr)
	if err != nil {
		log.Fatalf("Can't resolve address: '%v'", err)
	}

	sock, err := net.ListenUDP("udp4", addr)
	if err != nil {
		log.Fatalf("Can't open UDP socket: '%v'", err)
	}

	log.Printf("Start listening on udp://%v\n", *inAddr)

	defer sock.Close()

	u, _ := url.Parse("http://localhost:8086")
	c := client.NewClient(client.Config{
		URL:      u,
		Username: "username",
		Password: "password",
	})

	sinkChan := make(chan Pinba.Request)
	bpc := client.BatchPointsConfig{
		Precision: "us",
		Database:  "pinba",
	}
	sink := NewInfluxDBSink(100, sinkChan, c, bpc)
	go sink.run()
	for {
		var buf = make([]byte, 65536)
		rlen, _, err := sock.ReadFromUDP(buf)
		if err != nil {
			log.Fatalf("Error on sock.ReadFrom, %v", err)
		}
		if rlen == 0 {
			continue
		}

		request := &Pinba.Request{}
		proto.Unmarshal(buf[0:rlen], request)
		log.Printf("%v", request)
		sinkChan <- *request
	}
}

type InfluxDBSink struct {
	bufferLen         int
	input             chan Pinba.Request
	aggregated        chan []Pinba.Request
	aggregator        Aggregator
	client            client.Client
	BatchPointsConfig client.BatchPointsConfig
}

func NewInfluxDBSink(bufferLen int, input chan Pinba.Request, client client.Client, batchPointsConfig client.BatchPointsConfig) *InfluxDBSink {
	log.Printf("Sink create")
	ch := make(chan []Pinba.Request)
	aggregator := NewAggregator(bufferLen, input, ch)
	sink := &InfluxDBSink{
		bufferLen:         bufferLen,
		input:             input,
		aggregated:        ch,
		aggregator:        *aggregator,
		client:            client,
		BatchPointsConfig: batchPointsConfig,
	}
	return sink
}

func (sink *InfluxDBSink) run() {
	log.Printf("Sink run")
	go sink.aggregator.run()
	for {
		log.Printf("Sink wait for aggregated")
		batchRequest := <-sink.aggregated
		log.Printf("Sink got aggregated")
		bp, err := client.NewBatchPoints(sink.BatchPointsConfig)
		if err != nil {
			log.Fatal(err)
		}
		for _, req := range batchRequest {
			fields := make(map[string]interface{})
			log.Printf("%v", req)
			fields["request_time"] = req.GetRequestTime()
			fields["ru_utime"] = req.GetRuUtime()
			fields["ru_stime"] = req.GetRuStime()
			tags := make(map[string]string)
			tags["server_name"] = req.GetServerName()
			tags["hostname"] = req.GetHostname()
			tags["script_name"] = req.GetScriptName()
			point := client.NewPoint("request", tags, fields, time.Now())
			bp.AddPoint(point)
		}
		err = sink.client.Write(bp)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func WriteRequest(request Pinba.Request) error {
	return fmt.Errorf("not implemented")
}

type Aggregator struct {
	n      int
	buf    []Pinba.Request
	input  chan Pinba.Request
	output chan []Pinba.Request
}

func NewAggregator(n int, input chan Pinba.Request, output chan []Pinba.Request) *Aggregator {
	return &Aggregator{
		n:      n,
		input:  input,
		output: output,
	}
}

func (aggregator *Aggregator) run() {
	timerChannel := make(<-chan time.Time)
	realOutput := aggregator.output
	var output chan []Pinba.Request
	for {
		select {
		case output <- aggregator.buf:
			log.Println("case output <- aggregator.buf")
			timerChannel = nil
			aggregator.buf = nil
			output = nil
		case request := <-aggregator.input:
			log.Println("case request := <-aggregator.input")
			if len(aggregator.buf) == 0 {
				timerChannel = time.After(time.Second)
			}
			aggregator.buf = append(aggregator.buf, request)
			if len(aggregator.buf) >= aggregator.n {
				log.Printf("len(aggregator.buf) = %v, aggregator.n = %v", len(aggregator.buf), aggregator.n)
				output = realOutput
			}
		case <-timerChannel:
			log.Println("case <-timerChannel")
			timerChannel = nil
			output = realOutput
		}
	}
}
