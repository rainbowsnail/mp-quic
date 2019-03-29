package main

import (
	"bytes"
	"crypto/tls"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/lucas-clemente/quic-go"
	"github.com/lucas-clemente/quic-go/h2quic"
	"github.com/lucas-clemente/quic-go/internal/utils"
)

var (
	buf [268435456]byte
)

func handle(resp http.ResponseWriter, req *http.Request) {
	_, err := resp.Write(buf[:])
	if err != nil {
		log.Fatal(err)
	}
}

func startServer(wg *sync.WaitGroup) {
	http.HandleFunc("/", handle)
	h2quic.ListenAndServe(":6121", "example/fullchain.pem", "example/privkey.pem", nil)
	wg.Add(1)
}

func testRequest() {
	client := &http.Client{
		Transport: &h2quic.RoundTripper{
			QuicConfig:      &quic.Config{CreatePaths: true},
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Get("https://127.0.0.1:6121")
	if err != nil {
		log.Fatal(err)
	}
	body := &bytes.Buffer{}
	_, err = io.Copy(body, resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(body.Len())
}

func main() {
	utils.SetLogLevel(utils.LogLevelInfo)

	wg := &sync.WaitGroup{}
	go startServer(wg)

	testRequest()

	wg.Wait()
}
