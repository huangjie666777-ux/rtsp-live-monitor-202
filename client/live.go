package main

// Live demo: one publisher (ANNOUNCE/SETUP record/RECORD, then interleaved
// RTP) and two listeners (DESCRIBE/SETUP/PLAY) on the same server. Both
// listeners must receive every published payload with independent SSRC,
// continuous sequence numbers and +160 timestamps.

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/pion/rtp"
)

type liveConn struct {
	conn   net.Conn
	frames chan []byte
	respCh chan *response
}

func dialLive(addr string) *liveConn {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	lc := &liveConn{conn: conn, frames: make(chan []byte, 256), respCh: make(chan *response, 16)}
	r := bufio.NewReader(conn)
	go func() {
		for {
			resp, err := readResponse(r, lc.frames)
			if err != nil {
				close(lc.respCh)
				return
			}
			lc.respCh <- resp
		}
	}()
	return lc
}

func (lc *liveConn) next() *response {
	select {
	case resp, ok := <-lc.respCh:
		if !ok {
			fmt.Println("connection closed by server")
			return nil
		}
		return resp
	case <-time.After(3 * time.Second):
		fmt.Println("timeout waiting for response")
		os.Exit(1)
	}
	return nil
}

const livePackets = 100 // 2s of 20ms packets

// pubSender writes livePackets RTP packets (160-byte PCMU payloads) on the
// publisher's interleaved channel 0, one every 20ms. Payload byte value is
// the packet index, so listeners can verify content.
func pubSender(conn net.Conn, done chan<- struct{}) {
	seq, ts := uint16(0), uint32(0)
	for i := 0; i < livePackets; i++ {
		payload := make([]byte, 160)
		for j := range payload {
			payload[j] = byte(i)
		}
		pkt := &rtp.Packet{
			Header:  rtp.Header{Version: 2, PayloadType: 0, SequenceNumber: seq, Timestamp: ts, SSRC: 0x11223344},
			Payload: payload,
		}
		raw, err := pkt.Marshal()
		if err != nil {
			panic(err)
		}
		hdr := []byte{'$', 0, byte(len(raw) >> 8), byte(len(raw))}
		if _, err := conn.Write(append(hdr, raw...)); err != nil {
			fmt.Println("publisher write:", err)
			break
		}
		seq++
		ts += 160
		time.Sleep(20 * time.Millisecond)
	}
	done <- struct{}{}
}

// collectLive verifies a listener's RTP stream and reports stats.
func collectLive(lc *liveConn, label string, expect int) {
	var count, bad int
	var lastSeq, firstTS, lastTS int
	var ssrc uint32
	deadline := time.After(4 * time.Second)
	for count < expect {
		select {
		case f, ok := <-lc.frames:
			if !ok {
				fmt.Printf("   [%s] frames channel closed after %d packets\n", label, count)
				return
			}
			var p rtp.Packet
			if err := p.Unmarshal(f[1:]); err != nil {
				fmt.Printf("   [%s] unmarshal: %v\n", label, err)
				bad++
				continue
			}
			if count == 0 {
				ssrc = p.SSRC
				firstTS = int(p.Timestamp)
			}
			if len(p.Payload) != 160 || p.Payload[0] != byte(count) {
				bad++
			}
			if count > 0 && int(p.SequenceNumber) != lastSeq+1 {
				bad++
			}
			lastSeq = int(p.SequenceNumber)
			lastTS = int(p.Timestamp)
			count++
		case <-deadline:
			fmt.Printf("   [%s] timeout after %d packets\n", label, count)
			return
		}
	}
	fmt.Printf("   [%s] received %d/%d packets, ssrc=%#x seq ok, ts %d..%d (+%d/pkt), payload mismatches=%d\n",
		label, count, expect, ssrc, firstTS, lastTS, (lastTS-firstTS)/max(count-1, 1), bad)
}

func liveDemo(addr string) {
	uri := "rtsp://" + addr + "/live/devmic1"

	fmt.Println("=== PUBLISHER: ANNOUNCE /live/devmic1 ===")
	pub := dialLive(addr)
	sdp := "v=0\r\n" +
		"o=- 0 0 IN IP4 0.0.0.0\r\n" +
		"s=device mic\r\n" +
		"t=0 0\r\n" +
		"m=audio 0 RTP/AVP 0\r\n" +
		"c=IN IP4 0.0.0.0\r\n" +
		"a=rtpmap:0 PCMU/8000/1\r\n" +
		"a=control:trackID=0\r\n"
	send(pub.conn, "ANNOUNCE %s RTSP/1.0\r\nContent-Type: application/sdp\r\nContent-Length: %d\r\n\r\n%s", uri, len(sdp), sdp)
	pub.next()

	fmt.Println("=== PUBLISHER: SETUP mode=record + RECORD ===")
	send(pub.conn, "SETUP %s/trackID=0 RTSP/1.0\r\nTransport: RTP/AVP/TCP;unicast;interleaved=0-1;mode=\"record\"\r\n\r\n", uri)
	setup := pub.next()
	pubSess := setup.headers["session"]
	fmt.Printf("   session=%s transport=%s\n", pubSess, setup.headers["transport"])
	send(pub.conn, "RECORD %s RTSP/1.0\r\nSession: %s\r\n\r\n", uri, pubSess)
	pub.next()

	listen := func(tag string, ch int) *liveConn {
		lc := dialLive(addr)
		send(lc.conn, "DESCRIBE %s RTSP/1.0\r\nAccept: application/sdp\r\n\r\n", uri)
		desc := lc.next()
		fmt.Printf("=== %s: DESCRIBE ok, SDP %d bytes ===\n", tag, len(desc.body))
		send(lc.conn, "SETUP %s/trackID=0 RTSP/1.0\r\nTransport: RTP/AVP/TCP;unicast;interleaved=%d-%d\r\n\r\n", uri, ch, ch+1)
		st := lc.next()
		sess := st.headers["session"]
		send(lc.conn, "PLAY %s RTSP/1.0\r\nSession: %s\r\n\r\n", uri, sess)
		pl := lc.next()
		fmt.Printf("   %s PLAY: Range=%s RTP-Info=%s\n", tag, pl.headers["range"], pl.headers["rtp-info"])
		return lc
	}
	l1 := listen("LISTENER-1", 0)
	l2 := listen("LISTENER-2", 2)

	fmt.Printf("=== PUBLISHER: sending %d RTP packets (20ms each) ===\n", livePackets)
	done := make(chan struct{})
	go pubSender(pub.conn, done)

	res := make(chan struct{}, 2)
	go func() { collectLive(l1, "listener-1 ch0", livePackets); res <- struct{}{} }()
	go func() { collectLive(l2, "listener-2 ch2", livePackets); res <- struct{}{} }()
	<-done
	<-res
	<-res

	fmt.Println("=== PUBLISHER: TEARDOWN (listeners must be disconnected) ===")
	send(pub.conn, "TEARDOWN %s RTSP/1.0\r\nSession: %s\r\n\r\n", uri, pubSess)
	pub.next()
	for i, lc := range []*liveConn{l1, l2} {
		if _, ok := <-lc.respCh; !ok {
			fmt.Printf("   listener-%d connection closed by server (expected)\n", i+1)
		} else {
			fmt.Printf("   listener-%d NOT closed (unexpected)\n", i+1)
		}
	}
	fmt.Println("live demo done")
}
