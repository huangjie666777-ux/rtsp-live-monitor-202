package rtsp

import (
	"sync"
	"sync/atomic"

	"github.com/pion/rtp"

	"rtsprelay202/media"
)

// liveQueueLen bounds how many undelivered packets one listener may
// accumulate before it is considered too slow and dropped.
const liveQueueLen = 64

// liveSource is one published live stream. The publisher pushes raw
// 160-sample PCMU payloads; each subscriber gets its own queue so a
// slow listener never blocks the publisher or the other listeners.
type liveSource struct {
	name string

	recording atomic.Bool // set once RECORD succeeds

	mu     sync.Mutex
	subs   map[*liveSub]struct{}
	closed bool
}

func newLiveSource(name string) *liveSource {
	return &liveSource{name: name, subs: map[*liveSub]struct{}{}}
}

// broadcast fans one payload out to all subscribers. A subscriber with
// a full queue is dropped; the publisher is never blocked.
func (ls *liveSource) broadcast(payload []byte) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.closed {
		return
	}
	for sub := range ls.subs {
		select {
		case sub.queue <- payload:
		default:
			sub.kill()
		}
	}
}

func (ls *liveSource) subscribe(sub *liveSub) bool {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.closed {
		return false
	}
	ls.subs[sub] = struct{}{}
	return true
}

func (ls *liveSource) unsubscribe(sub *liveSub) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	delete(ls.subs, sub)
}

// close drops every subscriber; their connections are closed by their
// writer goroutines.
func (ls *liveSource) close() {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.closed {
		return
	}
	ls.closed = true
	for sub := range ls.subs {
		sub.kill()
	}
}

// liveSub is one listener's delivery queue.
type liveSub struct {
	queue chan []byte
	dead  chan struct{} // closed when the sub is dropped (slow or source gone)
	once  sync.Once
}

func newLiveSub() *liveSub {
	return &liveSub{
		queue: make(chan []byte, liveQueueLen),
		dead:  make(chan struct{}),
	}
}

func (s *liveSub) kill() {
	s.once.Do(func() { close(s.dead) })
}

// liveWriter packetizes queued payloads for one listener and writes
// them as interleaved RTP on the listener's connection. Each listener
// gets its own SSRC, continuous sequence numbers and a timestamp that
// advances by 160 per packet.
type liveWriter struct {
	sess *Session
	conn *clientConn
	sub  *liveSub
	src  *liveSource

	stop chan struct{}
	done chan struct{}
}

func newLiveWriter(s *Session, c *clientConn, src *liveSource) *liveWriter {
	return &liveWriter{
		sess: s,
		conn: c,
		sub:  newLiveSub(),
		src:  src,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
}

// attach registers the subscriber; must be called before run.
func (w *liveWriter) attach() bool { return w.src.subscribe(w.sub) }

func (w *liveWriter) run() {
	defer close(w.done)
	defer w.src.unsubscribe(w.sub)
	for {
		select {
		case <-w.stop:
			return
		case <-w.conn.closed:
			return
		case <-w.sub.dead:
			// Dropped as too slow, or the source went away:
			// disconnect only this listener.
			w.conn.close()
			return
		case payload := <-w.sub.queue:
			if !w.send(payload) {
				return
			}
		}
	}
}

func (w *liveWriter) send(payload []byte) bool {
	s := w.sess
	s.mu.Lock()
	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    payloadTypePCMU,
			SequenceNumber: s.seq,
			Timestamp:      s.rtpTime,
			SSRC:           s.ssrc,
		},
		Payload: payload,
	}
	s.seq++
	s.rtpTime += media.SamplesPerTick
	s.sentSamples += media.SamplesPerTick
	channel := s.rtpChannel
	s.mu.Unlock()

	raw, err := pkt.Marshal()
	if err != nil {
		return false
	}
	if err := w.conn.writeInterleaved(channel, raw); err != nil {
		// Write timeout or broken connection: drop only this listener.
		w.conn.close()
		return false
	}
	return true
}

func (w *liveWriter) stopAndWait() {
	select {
	case <-w.stop:
	default:
		close(w.stop)
	}
	<-w.done
}

// liveSessionCleanup detaches the listener writer of s, if any. Pending
// queued packets are discarded, so a resumed PLAY only delivers audio
// that arrives afterwards.
func liveSessionCleanup(s *Session) {
	s.mu.Lock()
	w := s.liveW
	s.liveW = nil
	s.mu.Unlock()
	if w != nil {
		w.stopAndWait()
	}
}
