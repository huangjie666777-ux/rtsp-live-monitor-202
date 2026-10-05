package rtsp

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"
)

func req(s string) *Request {
	r, err := readRequest(bufio.NewReader(strings.NewReader(s)))
	if err != nil {
		panic(err)
	}
	return r
}

func TestParseBasic(t *testing.T) {
	r := req("OPTIONS rtsp://x/demo RTSP/1.0\r\nCSeq: 3\r\nUser-Agent: t\r\n\r\n")
	if r.Method != "OPTIONS" || r.CSeq() != "3" || r.Header("user-agent") != "t" {
		t.Fatalf("bad parse: %+v", r)
	}
}

func TestPipelined(t *testing.T) {
	data := "OPTIONS rtsp://x/demo RTSP/1.0\r\nCSeq: 1\r\n\r\n" +
		"DESCRIBE rtsp://x/demo RTSP/1.0\r\nCSeq: 2\r\n\r\n"
	br := bufio.NewReader(strings.NewReader(data))
	r1, err := readRequest(br)
	if err != nil || r1.Method != "OPTIONS" {
		t.Fatalf("r1: %v %+v", err, r1)
	}
	r2, err := readRequest(br)
	if err != nil || r2.Method != "DESCRIBE" || r2.CSeq() != "2" {
		t.Fatalf("r2: %v %+v", err, r2)
	}
}

func TestHalfPacket(t *testing.T) {
	full := "PLAY rtsp://x/demo RTSP/1.0\r\nCSeq: 9\r\n\r\n"
	pr, pw := io.Pipe()
	go func() {
		for _, c := range []byte(full) {
			pw.Write([]byte{c})
			time.Sleep(time.Millisecond)
		}
	}()
	r, err := readRequest(bufio.NewReader(pr))
	if err != nil || r.Method != "PLAY" || r.CSeq() != "9" {
		t.Fatalf("half packet: %v %+v", err, r)
	}
}

func TestSkipsInterleavedFrames(t *testing.T) {
	frame := []byte{'$', 1, 0, 4, 0x80, 0xC8, 0x00, 0x06}
	data := append(frame, []byte("OPTIONS rtsp://x/demo RTSP/1.0\r\nCSeq: 5\r\n\r\n")...)
	data = append(data, frame...)
	data = append(data, []byte("TEARDOWN rtsp://x/demo RTSP/1.0\r\nCSeq: 6\r\n\r\n")...)
	br := bufio.NewReader(bytes.NewReader(data))
	r1, err := readRequest(br)
	if err != nil || r1.CSeq() != "5" {
		t.Fatalf("r1: %v %+v", err, r1)
	}
	r2, err := readRequest(br)
	if err != nil || r2.Method != "TEARDOWN" {
		t.Fatalf("r2: %v %+v", err, r2)
	}
}

func TestContentLengthBody(t *testing.T) {
	body := "announce-body"
	r := req("SETUP rtsp://x/demo RTSP/1.0\r\nCSeq: 1\r\nContent-Length: " +
		strconv.Itoa(len(body)) + "\r\n\r\n" + body)
	if string(r.Body) != body {
		t.Fatalf("body: %q", r.Body)
	}
}

func TestLimits(t *testing.T) {
	huge := "OPTIONS rtsp://x/demo RTSP/1.0\r\nCSeq: 1\r\nX: " + strings.Repeat("a", maxHeaderBytes) + "\r\n\r\n"
	if _, err := readRequest(bufio.NewReader(strings.NewReader(huge))); err == nil {
		t.Fatal("expected header size limit error")
	}
	bad := "OPTIONS rtsp://x/demo RTSP/1.0\r\nCSeq: 1\r\nContent-Length: 99999999\r\n\r\n"
	if _, err := readRequest(bufio.NewReader(strings.NewReader(bad))); err == nil {
		t.Fatal("expected body size limit error")
	}
	oversizeFrame := []byte{'$', 0, 0xff, 0xff}
	if _, err := readRequest(bufio.NewReader(bytes.NewReader(oversizeFrame))); err == nil {
		t.Fatal("expected frame size limit error")
	}
}
