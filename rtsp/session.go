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
	stateRecording
)

func (s sessionState) String() string {
	switch s {
	case stateReady:
		return "READY"
	case statePlaying:
		return "PLAYING"
	case stateRecording:
		return "RECORDING"
	}
	return "INIT"
}

// sessionMode distinguishes a media-receiving session (demo file or live
// listener) from a live publishing session.
type sessionMode int

const (
	modePlay sessionMode = iota
	modeRecord
)

// Session is a per-connection exclusive RTSP session.
type Session struct {
	ID string

	mu          sync.Mutex
	state       sessionState
	mode        sessionMode
	rtpChannel  int
	rtcpChannel int

	ssrc        uint32
	seq         uint16
	rtpTime     uint32
	sentSamples uint64 // total samples packetized; drives the RTP timestamp

	pos    int // file cursor in samples
	stream *streamer

	// Live publishing/listening state (nil for /demo sessions).
	live     *liveSource   // source this session publishes to or listens on
	listener *liveListener // non-nil while a live PLAY is delivering
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
