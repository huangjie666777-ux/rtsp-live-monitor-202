package main

// Demo RTSP TCP client: exercises OPTIONS, DESCRIBE, SETUP, PLAY, PAUSE,
// seek (Range), PLAY-while-playing rejection, pipelined and split requests,
// interleaved RTCP skipping, and TEARDOWN, printing received RTP packets.

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pion/rtp"
)

var cseq int

func send(conn net.Conn, format string, args ...any) {
	cseq++
	req := fmt.Sprintf(format, args...)
	if !strings.Contains(req, "CSeq:") {
		head := fmt.Sprintf("CSeq: %d\r\n", cseq)
		req = strings.Replace(req, "\r\n", "\r\n"+head, 1)
	}
	fmt.Printf(">> %s\n", strings.ReplaceAll(strings.ReplaceAll(req, "\r\n", " | "), "\n", " "))
	io.WriteString(conn, req)
}

type response struct {
	status  string
	headers map[string]string
	body    string
}

// readMessage reads either an RTSP response or an interleaved '$' frame.
// Interleaved frames are returned via the frames channel by the caller loop.
func readResponse(r *bufio.Reader, frames chan<- []byte) (*response, error) {
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == '$' {
			hdr := make([]byte, 3)
			if _, err := io.ReadFull(r, hdr); err != nil {
				return nil, err
			}
			n := int(hdr[1])<<8 | int(hdr[2])
			payload := make([]byte, n)
			if _, err := io.ReadFull(r, payload); err != nil {
				return nil, err
			}
			if frames != nil {
				frames <- append([]byte{hdr[0]}, payload...)
			}
			continue
		}
		line, err := readLine(r, b)
		if err != nil {
			return nil, err
		}
		resp := &response{status: line, headers: map[string]string{}}
		for {
			b, err := r.ReadByte()
			if err != nil {
				return nil, err
			}
			hl, err := readLine(r, b)
			if err != nil {
				return nil, err
			}
			if hl == "" {
				break
			}
			k, v, _ := strings.Cut(hl, ":")
			resp.headers[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
		}
		if cl := resp.headers["content-length"]; cl != "" {
			n, _ := strconv.Atoi(cl)
			body := make([]byte, n)
			if _, err := io.ReadFull(r, body); err != nil {
				return nil, err
			}
			resp.body = string(body)
		}
		fmt.Printf("<< %s (CSeq=%s)\n", resp.status, resp.headers["cseq"])
		return resp, nil
	}
}

func readLine(r *bufio.Reader, first byte) (string, error) {
	var sb strings.Builder
	sb.WriteByte(first)
	for {
		c, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		if c == 0x0a {
			return strings.TrimSuffix(sb.String(), "\r"), nil
		}
		sb.WriteByte(c)
	}
}

// collectRTP drains interleaved RTP frames for d and prints stats.
func collectRTP(frames <-chan []byte, d time.Duration, label string) (count int, lastSeq int, lastTS uint32) {
	deadline := time.After(d)
	start := time.Now()
	for {
		select {
		case f := <-frames:
			ch := f[0]
			var p rtp.Packet
			if err := p.Unmarshal(f[1:]); err != nil {
				fmt.Printf("   [ch %d] unmarshal error: %v\n", ch, err)
				continue
			}
			if count == 0 {
				fmt.Printf("   first RTP: ch=%d pt=%d seq=%d ts=%d ssrc=%#x payload=%dB (after %v)\n",
					ch, p.PayloadType, p.SequenceNumber, p.Timestamp, p.SSRC, len(p.Payload), time.Since(start).Round(time.Millisecond))
			}
			count++
			lastSeq = int(p.SequenceNumber)
			lastTS = p.Timestamp
		case <-deadline:
			fmt.Printf("   [%s] received %d RTP packets in %v, last seq=%d ts=%d\n", label, count, d, lastSeq, lastTS)
			return
		}
	}
}

func main() {
	addr := "127.0.0.1:8554"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	r := bufio.NewReader(conn)
	frames := make(chan []byte, 1024)
	respCh := make(chan *response, 16)
	// Background reader: continuously dispatches responses and interleaved
	// frames so RTP flow is never blocked by request/response sequencing.
	go func() {
		for {
			resp, err := readResponse(r, frames)
			if err != nil {
				close(respCh)
				return
			}
			respCh <- resp
		}
	}()
	nextResp := func() *response {
		select {
		case resp, ok := <-respCh:
			if !ok {
				fmt.Println("connection closed by server")
				os.Exit(1)
			}
			return resp
		case <-time.After(3 * time.Second):
			fmt.Println("timeout waiting for response")
			os.Exit(1)
		}
		return nil
	}
	uri := "rtsp://" + addr + "/demo"

	fmt.Println("=== 1. OPTIONS + DESCRIBE pipelined in one write ===")
	cseq++
	cseq++
	pipelined := fmt.Sprintf("OPTIONS %s RTSP/1.0\r\nCSeq: %d\r\n\r\n", uri, cseq-1) +
		fmt.Sprintf("DESCRIBE %s RTSP/1.0\r\nCSeq: %d\r\nAccept: application/sdp\r\n\r\n", uri, cseq)
	fmt.Printf(">> (two requests in one TCP write)\n")
	io.WriteString(conn, pipelined)
	nextResp()
	desc := nextResp()
	fmt.Printf("--- SDP ---\n%s-----------\n", desc.body)

	fmt.Println("=== 2. SETUP split into two TCP writes (half packet) ===")
	cseq++
	half := fmt.Sprintf("SETUP %s/trackID=0 RTSP/1.0\r\nCSeq: %d\r\nTrans", uri, cseq)
	io.WriteString(conn, half)
	time.Sleep(100 * time.Millisecond)
	io.WriteString(conn, "port: RTP/AVP/TCP;unicast;interleaved=0-1\r\n\r\n")
	fmt.Printf(">> (SETUP sent in two halves)\n")
	setup := nextResp()
	session := setup.headers["session"]
	fmt.Printf("   session=%s transport=%s\n", session, setup.headers["transport"])

	fmt.Println("=== 3. PLAY, receive ~1s of RTP ===")
	send(conn, "PLAY %s RTSP/1.0\r\nSession: %s\r\n\r\n", uri, session)
	play := nextResp()
	fmt.Printf("   Range=%s\n   RTP-Info=%s\n", play.headers["range"], play.headers["rtp-info"])
	collectRTP(frames, 1*time.Second, "initial play")

	fmt.Println("=== 4. PLAY while playing (must be rejected) ===")
	send(conn, "PLAY %s RTSP/1.0\r\nSession: %s\r\n\r\n", uri, session)
	nextResp()

	fmt.Println("=== 5. PAUSE, expect silence ===")
	send(conn, "PAUSE %s RTSP/1.0\r\nSession: %s\r\n\r\n", uri, session)
	nextResp()
	n, _, _ := collectRTP(frames, 400*time.Millisecond, "after pause")
	fmt.Printf("   packets during pause: %d\n", n)

	fmt.Println("=== 6. Resume PLAY (no Range), no gap ===")
	send(conn, "PLAY %s RTSP/1.0\r\nSession: %s\r\n\r\n", uri, session)
	play2 := nextResp()
	fmt.Printf("   Range=%s RTP-Info=%s\n", play2.headers["range"], play2.headers["rtp-info"])
	collectRTP(frames, 500*time.Millisecond, "resumed")

	fmt.Println("=== 7. Seek: PLAY Range:npt=2.007- (aligns down to 2.000) ===")
	send(conn, "PAUSE %s RTSP/1.0\r\nSession: %s\r\n\r\n", uri, session)
	nextResp()
	send(conn, "PLAY %s RTSP/1.0\r\nSession: %s\r\nRange: npt=2.007-\r\n\r\n", uri, session)
	play3 := nextResp()
	fmt.Printf("   Range=%s RTP-Info=%s\n", play3.headers["range"], play3.headers["rtp-info"])
	collectRTP(frames, 500*time.Millisecond, "after seek")

	fmt.Println("=== 8. Invalid range must be rejected ===")
	send(conn, "PAUSE %s RTSP/1.0\r\nSession: %s\r\n\r\n", uri, session)
	nextResp()
	send(conn, "PLAY %s RTSP/1.0\r\nSession: %s\r\nRange: npt=99-\r\n\r\n", uri, session)
	nextResp()

	fmt.Println("=== 9. Wrong session must be rejected ===")
	send(conn, "PLAY %s RTSP/1.0\r\nSession: deadbeef\r\n\r\n", uri)
	nextResp()

	fmt.Println("=== 10. Interleaved RTCP from client is skipped ===")
	rtcp := []byte{'$', 1, 0, 8, 0x80, 0xC8, 0x00, 0x06, 1, 2, 3, 4}
	conn.Write(rtcp)
	send(conn, "OPTIONS %s RTSP/1.0\r\n\r\n", uri)
	nextResp()

	fmt.Println("=== 11. TEARDOWN ===")
	send(conn, "TEARDOWN %s RTSP/1.0\r\nSession: %s\r\n\r\n", uri, session)
	nextResp()
	fmt.Println("demo done")
}
