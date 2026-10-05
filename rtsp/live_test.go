package rtsp

import (
	"bufio"
	"strings"
	"testing"
	"time"
)

func TestParseLivePath(t *testing.T) {
	cases := []struct {
		path  string
		name  string
		track bool
		ok    bool
	}{
		{"/live/mic1", "mic1", false, true},
		{"/live/mic1/trackID=0", "mic1", true, true},
		{"/live/A_b-9", "", false, false},                      // dash not allowed
		{"/live/" + strings.Repeat("a", 33), "", false, false}, // too long
		{"/live/", "", false, false},
		{"/demo", "", false, false},
		{"/live/mic1/trackID=1", "", false, false},
	}
	for _, tc := range cases {
		name, track, ok := parseLivePath(tc.path)
		if name != tc.name || track != tc.track || ok != tc.ok {
			t.Errorf("parseLivePath(%q) = %q,%v,%v want %q,%v,%v",
				tc.path, name, track, ok, tc.name, tc.track, tc.ok)
		}
	}
}

func TestValidAnnounceSDP(t *testing.T) {
	good := "v=0\r\nm=audio 0 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000/1\r\na=control:trackID=0\r\n"
	if !validAnnounceSDP([]byte(good)) {
		t.Fatal("valid SDP rejected")
	}
	bad := []string{
		"v=0\r\n", // no media
		"m=audio 0 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000/1\r\n",                         // no control
		"m=audio 0 RTP/AVP 0\r\na=control:trackID=0\r\n",                            // no rtpmap
		"m=audio 0 RTP/AVP 8\r\na=rtpmap:0 PCMU/8000/1\r\na=control:trackID=0\r\n",  // payload not 0
		"m=audio 0 RTP/AVP 0\r\na=rtpmap:0 PCMU/16000/1\r\na=control:trackID=0\r\n", // wrong rate
		good + "m=audio 0 RTP/AVP 0\r\n",                                            // two tracks
	}
	for i, b := range bad {
		if validAnnounceSDP([]byte(b)) {
			t.Errorf("bad SDP %d accepted", i)
		}
	}
}

func TestRegistryRemoveDoesNotClobberNewSource(t *testing.T) {
	reg := newLiveRegistry()
	old := newLiveSource("mic", reg)
	if !reg.reserve("mic", old) {
		t.Fatal("reserve failed")
	}
	if reg.reserve("mic", newLiveSource("mic", reg)) {
		t.Fatal("duplicate reserve allowed")
	}
	reg.remove("mic", old)
	if reg.get("mic") != nil {
		t.Fatal("remove did not delete owner")
	}
	// Re-publish under the same name, then stale cleanup must not delete it.
	fresh := newLiveSource("mic", reg)
	if !reg.reserve("mic", fresh) {
		t.Fatal("re-reserve failed")
	}
	reg.remove("mic", old)
	if reg.get("mic") != fresh {
		t.Fatal("stale cleanup removed the new source")
	}
}

func TestBroadcastDropsSlowListenerOnly(t *testing.T) {
	reg := newLiveRegistry()
	src := newLiveSource("mic", reg)
	src.setRecording()
	mkListener := func() *liveListener {
		c := &clientConn{closed: make(chan struct{})}
		l := newLiveListener(src, c, newSession())
		src.addListener(l)
		return l
	}
	slow := mkListener()
	fast := mkListener()
	// Drain the fast listener so its queue never fills.
	received := make(chan []byte, 2*liveQueueSize)
	go func() {
		for p := range fast.queue {
			received <- p
		}
	}()
	payload := make([]byte, 160)
	for i := 0; i < liveQueueSize+10; i++ {
		src.broadcast(payload)
		// The fast listener keeps up and receives every packet.
		select {
		case <-received:
		case <-time.After(time.Second):
			t.Fatalf("fast listener got only %d packets", i)
		}
	}
	// Slow listener (never drained) must have been dropped and stopped.
	select {
	case <-slow.stop:
	default:
		t.Fatal("slow listener not stopped")
	}
	src.close()
	select {
	case <-fast.stop:
	case <-time.After(time.Second):
		t.Fatal("source close did not stop listener")
	}
}

func TestReadMessageFramesAndRequests(t *testing.T) {
	wire := "$\x02\x00\x04abcdOPTIONS rtsp://h/live/x RTSP/1.0\r\nCSeq: 1\r\n\r\n"
	r := bufio.NewReader(strings.NewReader(wire))
	req, fr, err := readMessage(r)
	if err != nil || fr == nil || fr.channel != 2 || string(fr.payload) != "abcd" {
		t.Fatalf("frame: req=%v fr=%+v err=%v", req, fr, err)
	}
	req, fr, err = readMessage(r)
	if err != nil || req == nil || fr != nil || req.Method != "OPTIONS" {
		t.Fatalf("request: req=%+v fr=%+v err=%v", req, fr, err)
	}
}
