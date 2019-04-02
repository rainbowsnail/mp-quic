package main

import (
	"log"
	"net/http"

	"github.com/lucas-clemente/quic-go/h2quic"
	"github.com/lucas-clemente/quic-go/internal/utils"
)

var (
	buf [2048]byte
)

func handle(resp http.ResponseWriter, req *http.Request) {
	_, err := resp.Write(buf[:])
	if err != nil {
		log.Fatal(err)
	}
}

func startServer() {
	http.HandleFunc("/", handle)
	h2quic.ListenAndServe(":6121", "example/fullchain.pem", "example/privkey.pem", nil)
}

func main() {
	utils.SetLogLevel(utils.LogLevelInfo)
	startServer()
}
