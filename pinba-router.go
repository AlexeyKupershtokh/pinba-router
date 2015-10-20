package PinbaRouter

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/golang/protobuf/proto"
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
		fmt.Printf("%v", request)
	}
}
