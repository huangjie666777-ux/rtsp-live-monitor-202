package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"rtsprelay202/media"
	"rtsprelay202/rtsp"
)

func main() {
	port := flag.Int("port", 8554, "TCP port to listen on")
	file := flag.String("file", "assets/demo.ulaw", "8kHz mono PCMU (u-law) audio file")
	flag.Parse()

	src, err := media.LoadSource(*file)
	if err != nil {
		log.Fatalf("load media: %v", err)
	}
	fmt.Printf("loaded %s: %d samples, duration %.3fs\n", *file, src.Samples(), src.Duration())

	srv := rtsp.NewServer(src)
	addr := fmt.Sprintf(":%d", *port)
	log.Printf("RTSP listening on %s, resources /demo and /live/<name>", addr)
	if err := srv.ListenAndServe(addr); err != nil {
		log.Printf("server stopped: %v", err)
		os.Exit(1)
	}
}
