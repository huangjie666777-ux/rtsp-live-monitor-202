package rtsp

import (
	"testing"
	"time"
)

func TestLiveNameValidation(t *testing.T) {
	valid := []string{"a", "mic", "Mic_01", "A2345678901234567890123456789012"}
	for _, n := range valid {
		if !validLiveName(n) {
			t.Errorf("name %q should be valid", n)
		}
	}
	invalid := []string{"", "has-dash", "has.dot", "has/slash", "A23456789012345678901234567890123", "\u9ea6"}
	for _, n := range invalid {
		if validLiveName(n) {
			t.Errorf("name %q should be invalid", n)
		}
	}
}

func TestLiveNamePath(t *testing.T) {
	name, track, ok := liveName("/live/mic")
	if !ok || track || name != "mic" {
		t.Fatalf("/live/mic -> %q %v %v", name, track, ok)
	}
	name, track, ok = liveName("/live/mic/trackID=0")
	if !ok || !track || name != "mic" {
		t.Fatalf("/live/mic/trackID=0 -> %q %v %v", name, track, ok)
	}
	for _, p := range []string{"/live/", "/live/mic/trackID=1", "/live/a/b", "/demo"} {
		if _, _, ok := liveName(p); ok {
			t.Fatalf("%s should not parse as live path", p)
		}
	}
}

func TestAnnounceSDP(t *testing.T) {
	good := "v=0\r\no=- 0 0 IN IP4 0.0.0.0\r\ns=x\r\nt=0 0\r\n" +
		"m=audio 0 RTP/AVP 0\r\nc=IN IP4 0.0.0.0\r\n" +
		"a=rtpmap:0 PCMU/8000/1\r\na=control:trackID=0\r\n"
	if err := announceSDP([]byte(good)); err != nil {
		t.Fatalf("good SDP rejected: %v", err)
	}
	cases := map[string]string{
		"two tracks":      good + "m=audio 0 RTP/AVP 0\r\n",
		"wrong payload":   "m=audio 0 RTP/AVP 8\r\na=rtpmap:8 PCMA/8000\r\na=control:trackID=0\r\n",
		"wrong rate":      "m=audio 0 RTP/AVP 0\r\na=rtpmap:0 PCMU/16000/1\r\na=control:trackID=0\r\n",
		"wrong control":   "m=audio 0 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000/1\r\na=control:trackID=1\r\n",
		"missing rtpmap":  "m=audio 0 RTP/AVP 0\r\na=control:trackID=0\r\n",
		"missing control": "m=audio 0 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000/1\r\n",
	}
	for name, sdp := range cases {
		if err := announceSDP([]byte(sdp)); err == nil {
			t.Errorf("%s: should be rejected", name)
		}
	}
}

func TestRegistryExclusiveAndStaleCleanup(t *testing.T) {
	r := newRegistry()
	a := newLiveSource("mic")
	b := newLiveSource("mic")
	if !r.register("mic", a) {
		t.Fatal("first register failed")
	}
	if r.register("mic", b) {
		t.Fatal("second publisher on same name must be rejected")
	}
	// Stale cleanup of a non-owner must not remove the current source.
	r.unregister("mic", b)
	if r.get("mic") != a {
		t.Fatal("stale unregister removed the live source")
	}
	r.unregister("mic", a)
	if r.get("mic") != nil {
		t.Fatal("owner unregister should free the name")
	}
	// The name can be published again.
	if !r.register("mic", b) {
		t.Fatal("re-publish after release failed")
	}
}

func TestBroadcastDropsSlowSubscriber(t *testing.T) {
	src := newLiveSource("mic")
	fast := newLiveSub()
	slow := newLiveSub()
	src.subscribe(fast)
	src.subscribe(slow)

	payload := make([]byte, 160)
	// Overflow the slow subscriber: fill its queue without draining,
	// while the fast one keeps up.
	for i := 0; i < liveQueueLen+10; i++ {
		for len(fast.queue) > 0 {
			<-fast.queue
		}
		src.broadcast(payload)
	}
	select {
	case <-slow.dead:
	default:
		t.Fatal("slow subscriber should have been dropped")
	}
	// The fast subscriber kept receiving without blocking the publisher.
	select {
	case <-fast.dead:
		t.Fatal("fast subscriber must not be dropped")
	default:
	}
	if got := len(fast.queue); got == 0 {
		t.Fatal("fast subscriber got no payloads")
	}
}

func TestSourceCloseDropsSubscribers(t *testing.T) {
	src := newLiveSource("mic")
	sub := newLiveSub()
	src.subscribe(sub)
	src.close()
	select {
	case <-sub.dead:
	case <-time.After(time.Second):
		t.Fatal("subscriber not dropped on source close")
	}
	if src.subscribe(newLiveSub()) {
		t.Fatal("subscribe after close must fail")
	}
}
