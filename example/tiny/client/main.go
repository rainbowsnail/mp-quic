package main

import (
	"bytes"
	"crypto/tls"
	"io"
	"log"
	"net/http"

	"github.com/lucas-clemente/quic-go"
	"github.com/lucas-clemente/quic-go/h2quic"
	"github.com/lucas-clemente/quic-go/internal/utils"
)

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
	defer resp.Body.Close()

	body := &bytes.Buffer{}
	_, err = io.Copy(body, resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(body.Len())
}

func main() {
	utils.SetLogLevel(utils.LogLevelInfo)
	testRequest()
}
