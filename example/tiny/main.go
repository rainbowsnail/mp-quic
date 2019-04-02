package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/lucas-clemente/quic-go/h2quic"
	"github.com/lucas-clemente/quic-go/internal/utils"
)

var (
	listen = flag.String("listen", ":6121", "listen")
	size   = flag.Int("size", 0, "size")
	buf    []byte
)

func handle(resp http.ResponseWriter, req *http.Request) {
	_, err := resp.Write(buf)
	if err != nil {
		log.Fatal(err)
	}
}

func startServer() {
	http.HandleFunc("/", handle)
	h2quic.ListenAndServe(*listen, "bin/fullchain.pem", "bin/privkey.pem", nil)
}

func main() {
	flag.Parse()
	utils.SetLogLevel(utils.LogLevelInfo)
	buf = make([]byte, *size)
	startServer()
}
