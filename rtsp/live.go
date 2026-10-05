package rtsp

import (
	"sync"

	"github.com/pion/rtp"
)

// liveQueueSize is the maximum number of undelivered packets buffered per
// listener. A full queue (or a write timeout) drops only that listener.
const liveQueueSize = 64

// liveRegistry maps stream names to their live source. Each name is owned
// by at most one publisher at a time.
type liveRegistry struct {
	mu      sync.Mutex
	sources map[string]*liveSource
}

func newLiveRegistry() *liveRegistry {
	return &liveRegistry{sources: map[string]*liveSource{}}
}

// reserve claims name for src. Returns false when the name is taken.
func (r *liveRegistry) reserve(name string, src *liveSource) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sources[name]; ok {
		return false
	}
	r.sources[name] = src
	return true
}

func (r *liveRegistry) get(name string) *liveSource {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sources[name]
}

// remove deletes name only if it is still owned by src, so cleanup of an
// old publisher session can never remove a newer source with the same name.
func (r *liveRegistry) remove(name string, src *liveSource) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sources[name] == src {
		delete(r.sources, name)
	}
}

// liveSource is one published live stream: it receives 160-byte PCMU
// payloads from its publisher and fans them out to listeners.
type liveSource struct {
	name string
	reg  *liveRegistry

	mu        sync.Mutex
	recording bool // RECORD succeeded: listeners may attach
	closed    bool
	listeners map[*liveListener]struct{}
}

func newLiveSource(name string, reg *liveRegistry) *liveSource {
	return &liveSource{
		name:      name,
		reg:       reg,
		listeners: map[*liveListener]struct{}{},
	}
}

func (s *liveSource) setRecording() {
	s.mu.Lock()
	s.recording = true
	s.mu.Unlock()
}

func (s *liveSource) isRecording() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recording
}

// broadcast queues one payload copy on every listener. A listener whose
// queue is full is disconnected; everyone else is unaffected.
func (s *liveSource) broadcast(payload []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	for l := range s.listeners {
		select {
		case l.queue <- payload:
		default:
			// Slow listener: drop it without blocking the publisher.
			delete(s.listeners, l)
			l.stopAsync()
		}
	}
}

func (s *liveSource) addListener(l *liveListener) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.recording {
		return false
	}
	s.listeners[l] = struct{}{}
	return true
}

func (s *liveSource) removeListener(l *liveListener) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.listeners, l)
}

// close detaches the source from the registry (if it still owns the name)
// and disconnects all listeners.
func (s *liveSource) close() {
	s.reg.remove(s.name, s)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	listeners := make([]*liveListener, 0, len(s.listeners))
	for l := range s.listeners {
		listeners = append(listeners, l)
	}
	s.listeners = map[*liveListener]struct{}{}
	s.mu.Unlock()
	for _, l := range listeners {
		l.stopAsync()
	}
}

// liveListener delivers queued payloads to one PLAYING live session,
// packetizing RTP with the session's own SSRC, sequence and timestamp.
type liveListener struct {
	src  *liveSource
	conn *clientConn
	sess *Session

	queue chan []byte
	stop  chan struct{}
	done  chan struct{}
	once  sync.Once
}

func newLiveListener(src *liveSource, c *clientConn, sess *Session) *liveListener {
	return &liveListener{
		src:   src,
		conn:  c,
		sess:  sess,
		queue: make(chan []byte, liveQueueSize),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
}

func (l *liveListener) run() {
	defer close(l.done)
	for {
		select {
		case <-l.stop:
			return
		case <-l.conn.closed:
			return
		case payload := <-l.queue:
			if !l.send(payload) {
				return
			}
		}
	}
}

// send packetizes one 160-sample payload and writes the interleaved frame.
// A write error or timeout disconnects only this listener.
func (l *liveListener) send(payload []byte) bool {
	s := l.sess
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
	s.rtpTime += uint32(len(payload))
	s.sentSamples += uint64(len(payload))
	channel := s.rtpChannel
	s.mu.Unlock()

	raw, err := pkt.Marshal()
	if err != nil {
		return false
	}
	if err := l.conn.writeInterleaved(channel, raw); err != nil {
		l.conn.close()
		return false
	}
	return true
}

// stopAsync asks the delivery loop to stop and closes the connection
// without waiting (safe to call while holding the source lock).
func (l *liveListener) stopAsync() {
	l.once.Do(func() { close(l.stop) })
	l.conn.close()
}

// stopAndWait stops delivery, waits for the loop to exit and drops any
// packets still queued, so a resumed PLAY only receives new audio.
func (l *liveListener) stopAndWait() {
	l.once.Do(func() { close(l.stop) })
	<-l.done
	for {
		select {
		case <-l.queue:
		default:
			return
		}
	}
}
