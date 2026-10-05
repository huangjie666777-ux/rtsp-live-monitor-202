package main

// Live demo: one RTSP publisher (ANNOUNCE/SETUP mode=record/RECORD, then
// interleaved RTP) and two concurrent listeners (DESCRIBE/SETUP/PLAY) on
// rtsp://<addr>/live/<name>. Listener 1 also exercises PAUSE/resume.
// Finally the publisher sends TEARDOWN and both listener connections are
// closed by the server.

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
)

const announceSDP = "v=0\r\n" +
	"o=- 0 0 IN IP4 127.0.0.1\r\n" +
	"s=live mic\r\n" +
	"t=0 0\r\n" +
	"m=audio 0 RTP/AVP 0\r\n" +
	"c=IN IP4 0.0.0.0\r\n" +
	"a=rtpmap:0 PCMU/8000/1\r\n" +
	"a=control:trackID=0\r\n"

type client struct {
	name   string
	conn   net.Conn
	r      *bufio.Reader
	cseq   int
	frames chan []byte
	respCh chan map[string]string
}

func dial(addr, name string) *client {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	c := &client{
		name:   name,
		conn:   conn,
		r:      bufio.NewReader(conn),
		frames: make(chan []byte, 256),
		respCh: make(chan map[string]string, 16),
	}
	go c.readLoop()
	return c
}

func (c *client) readLoop() {
	for {
		b, err := c.r.ReadByte()
		if err != nil {
			close(c.respCh)
			return
		}
		if b == '$' {
			hdr := make([]byte, 3)
			if _, err := io.ReadFull(c.r, hdr); err != nil {
				close(c.respCh)
				return
			}
			n := int(hdr[1])<<8 | int(hdr[2])
			payload := make([]byte, n)
			if _, err := io.ReadFull(c.r, payload); err != nil {
				close(c.respCh)
				return
			}
			select {
			case c.frames <- append([]byte{hdr[0]}, payload...):
			default:
			}
			continue
		}
		resp := map[string]string{}
		line, err := readLine(c.r, b)
		if err != nil {
			close(c.respCh)
			return
		}
		resp[":status"] = line
		for {
			b, err := c.r.ReadByte()
			if err != nil {
				close(c.respCh)
				return
			}
			hl, err := readLine(c.r, b)
			if err != nil {
				close(c.respCh)
				return
			}
			if hl == "" {
				break
			}
			k, v, _ := strings.Cut(hl, ":")
			resp[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
		}
		if cl := resp["content-length"]; cl != "" {
			n, _ := strconv.Atoi(cl)
			body := make([]byte, n)
			if _, err := io.ReadFull(c.r, body); err != nil {
				close(c.respCh)
				return
			}
			resp[":body"] = string(body)
		}
		c.respCh <- resp
	}
}

func readLine(r *bufio.Reader, first byte) (string, error) {
	var sb strings.Builder
	sb.WriteByte(first)
	for {
		ch, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		if ch == 0x0a {
			return strings.TrimSuffix(sb.String(), "\r"), nil
		}
		sb.WriteByte(ch)
	}
}

func (c *client) request(format string, args ...any) map[string]string {
	c.cseq++
	req := fmt.Sprintf(format, args...)
	if !strings.Contains(req, "CSeq:") {
		req = strings.Replace(req, "\r\n", fmt.Sprintf("\r\nCSeq: %d\r\n", c.cseq), 1)
	}
	fmt.Printf("[%s] >> %s\n", c.name, strings.ReplaceAll(strings.TrimRight(req, "\r\n"), "\r\n", " | "))
	io.WriteString(c.conn, req)
	select {
	case resp, ok := <-c.respCh:
		if !ok {
			fmt.Printf("[%s] connection closed by server\n", c.name)
			return nil
		}
		fmt.Printf("[%s] << %s\n", c.name, resp[":status"])
		return resp
	case <-time.After(3 * time.Second):
		fmt.Printf("[%s] timeout waiting for response\n", c.name)
		os.Exit(1)
		return nil
	}
}

// loadAudio reads the demo u-law clip; falls back to a synthetic square
// wave in u-law (0x7F/0xFF) when the file is unavailable.
func loadAudio() []byte {
	for _, p := range []string{"assets/demo.ulaw", "../assets/demo.ulaw"} {
		if data, err := os.ReadFile(p); err == nil && len(data) > 0 {
			fmt.Printf("publisher: loaded %s (%d samples)\n", p, len(data))
			return data
		}
	}
	data := make([]byte, 16000)
	for i := range data {
		if (i/80)%2 == 0 {
			data[i] = 0x7F
		} else {
			data[i] = 0xFF
		}
	}
	fmt.Println("publisher: using synthetic u-law square wave")
	return data
}

// publisher streams 160-byte PCMU payloads as interleaved RTP, 20ms apart.
func publisher(addr, name string, audio []byte, stop <-chan struct{}) {
	c := dial(addr, "pub")
	uri := "rtsp://" + addr + "/live/" + name
	c.request("ANNOUNCE %s RTSP/1.0\r\nContent-Type: application/sdp\r\nContent-Length: %d\r\n\r\n%s",
		uri, len(announceSDP), announceSDP)
	setup := c.request("SETUP %s/trackID=0 RTSP/1.0\r\nTransport: RTP/AVP/TCP;unicast;interleaved=0-1;mode=\"record\"\r\n\r\n", uri)
	sess := setup["session"]
	c.request("RECORD %s RTSP/1.0\r\nSession: %s\r\n\r\n", uri, sess)
	fmt.Println("publisher: RECORD ok, streaming...")

	var seq uint16 = 7
	var ts uint32 = 123456
	const ssrc = 0xCAFECAFE
	off := 0
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			c.request("TEARDOWN %s RTSP/1.0\r\nSession: %s\r\n\r\n", uri, sess)
			fmt.Println("publisher: TEARDOWN sent")
			c.conn.Close()
			return
		case <-ticker.C:
			end := off + 160
			if end > len(audio) {
				off = 0
				end = 160
			}
			pkt := &rtp.Packet{
				Header:  rtp.Header{Version: 2, PayloadType: 0, SequenceNumber: seq, Timestamp: ts, SSRC: ssrc},
				Payload: audio[off:end],
			}
			off = end
			seq++
			ts += 160
			raw, _ := pkt.Marshal()
			frame := append([]byte{'$', 0, byte(len(raw) >> 8), byte(len(raw))}, raw...)
			if _, err := c.conn.Write(frame); err != nil {
				return
			}
		}
	}
}

// listener subscribes and validates the RTP stream for d.
func listener(addr, name, label string, rtpCh, rtcpCh int, d time.Duration, pauseTest bool, done *atomic.Int32) {
	defer done.Add(1)
	c := dial(addr, label)
	uri := "rtsp://" + addr + "/live/" + name
	desc := c.request("DESCRIBE %s RTSP/1.0\r\nAccept: application/sdp\r\n\r\n", uri)
	if desc == nil {
		return
	}
	fmt.Printf("[%s] SDP: %s\n", label, strings.ReplaceAll(strings.TrimSpace(desc[":body"]), "\r\n", " / "))
	setup := c.request("SETUP %s/trackID=0 RTSP/1.0\r\nTransport: RTP/AVP/TCP;unicast;interleaved=%d-%d\r\n\r\n",
		uri, rtpCh, rtcpCh)
	sess := setup["session"]
	// Live sources must reject Range.
	c.request("PLAY %s RTSP/1.0\r\nSession: %s\r\nRange: npt=5-\r\n\r\n", uri, sess)
	c.request("PLAY %s RTSP/1.0\r\nSession: %s\r\n\r\n", uri, sess)

	collect := func(d time.Duration) (n int, ok bool) {
		ok = true
		deadline := time.After(d)
		var lastSeq int = -1
		var lastTS uint32
		for {
			select {
			case f := <-c.frames:
				var p rtp.Packet
				if err := p.Unmarshal(f[1:]); err != nil {
					fmt.Printf("[%s] bad RTP: %v\n", label, err)
					continue
				}
				if int(f[0]) != rtpCh {
					fmt.Printf("[%s] unexpected channel %d\n", label, f[0])
					ok = false
				}
				if len(p.Payload) != 160 {
					fmt.Printf("[%s] bad payload size %d\n", label, len(p.Payload))
					ok = false
				}
				if lastSeq >= 0 {
					if int(p.SequenceNumber) != (lastSeq+1)&0xFFFF {
						fmt.Printf("[%s] seq gap: %d -> %d\n", label, lastSeq, p.SequenceNumber)
						ok = false
					}
					if p.Timestamp-lastTS != 160 {
						fmt.Printf("[%s] ts jump: %d -> %d\n", label, lastTS, p.Timestamp)
						ok = false
					}
				} else {
					fmt.Printf("[%s] first RTP: ch=%d seq=%d ts=%d ssrc=%#x\n",
						label, f[0], p.SequenceNumber, p.Timestamp, p.SSRC)
				}
				lastSeq = int(p.SequenceNumber)
				lastTS = p.Timestamp
				n++
			case <-deadline:
				return n, ok
			}
		}
	}

	n1, ok1 := collect(d)
	fmt.Printf("[%s] received %d packets, stream valid=%v\n", label, n1, ok1)

	if pauseTest {
		c.request("PAUSE %s RTSP/1.0\r\nSession: %s\r\n\r\n", uri, sess)
		// Drain anything in flight, then confirm silence.
		time.Sleep(100 * time.Millisecond)
		for len(c.frames) > 0 {
			<-c.frames
		}
		n, _ := collect(300 * time.Millisecond)
		fmt.Printf("[%s] packets during pause: %d\n", label, n)
		c.request("PLAY %s RTSP/1.0\r\nSession: %s\r\n\r\n", uri, sess)
		n2, ok2 := collect(1 * time.Second)
		fmt.Printf("[%s] after resume: %d packets, stream valid=%v\n", label, n2, ok2)
	}
	c.conn.Close()
}

func main() {
	addr := "127.0.0.1:8554"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	name := "mic"
	if len(os.Args) > 2 {
		name = os.Args[2]
	}
	audio := loadAudio()

	stop := make(chan struct{})
	go publisher(addr, name, audio, stop)
	time.Sleep(300 * time.Millisecond) // let RECORD complete

	var done atomic.Int32
	go listener(addr, name, "listener-A", 0, 1, 2*time.Second, true, &done)
	go listener(addr, name, "listener-B", 2, 3, 3*time.Second, false, &done)

	for done.Load() < 2 {
		time.Sleep(200 * time.Millisecond)
	}
	close(stop)
	time.Sleep(300 * time.Millisecond)
	fmt.Println("live demo done")
}
