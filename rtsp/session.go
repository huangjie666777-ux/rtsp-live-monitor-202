package rtsp

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
)

type sessionState int

const (
	stateInit sessionState = iota
	stateReady
	statePlaying
)

type sessionKind int

const (
	kindDemo      sessionKind = iota // /demo file playback
	kindListener                     // /live/<name> listener
	kindPublisher                    // /live/<name> publisher
)

func (s sessionState) String() string {
	switch s {
	case stateReady:
		return "READY"
	case statePlaying:
		return "PLAYING"
	}
	return "INIT"
}

// Session is a per-connection exclusive RTSP session.
type Session struct {
	ID string

	mu          sync.Mutex
	kind        sessionKind
	state       sessionState
	rtpChannel  int
	rtcpChannel int

	ssrc        uint32
	seq         uint16
	rtpTime     uint32
	sentSamples uint64 // total samples packetized; drives the RTP timestamp

	pos     int // file cursor in samples
	stream  *streamer
	live    *liveSource // publisher: owned source; listener: subscribed source
	liveW   *liveWriter // listener only
	onClose func()
}

func newSession() *Session {
	var b [8]byte
	_, _ = rand.Read(b[:])
	var seed [4]byte
	_, _ = rand.Read(seed[:])
	return &Session{
		ID:   fmt.Sprintf("%016x", binary.BigEndian.Uint64(b[:])),
		ssrc: binary.BigEndian.Uint32(seed[:]),
	}
}

func (s *Session) State() sessionState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}
