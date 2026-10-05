package rtsp

import (
	"time"

	"github.com/pion/rtp"

	"rtsprelay202/media"
)

const (
	payloadTypePCMU = 0
	tickInterval    = 20 * time.Millisecond
)

// streamer packetizes the media source into RTP and writes interleaved
// '$' frames on the session's TCP connection, one packet every 20ms.
type streamer struct {
	sess *Session
	conn *clientConn
	src  *media.Source
	stop chan struct{}
	done chan struct{}
}

func newStreamer(s *Session, c *clientConn, src *media.Source) *streamer {
	return &streamer{
		sess: s,
		conn: c,
		src:  src,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
}

func (st *streamer) run() {
	defer close(st.done)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-st.stop:
			return
		case <-st.conn.closed:
			return
		case <-ticker.C:
			if !st.sendPacket() {
				return
			}
		}
	}
}

// sendPacket sends one 20ms RTP packet. Returns false when streaming must
// stop (end of file, write error/timeout or closed connection).
func (st *streamer) sendPacket() bool {
	s := st.sess
	s.mu.Lock()
	payload := st.src.Chunk(s.pos)
	if len(payload) == 0 {
		// End of file: behave like PAUSE, keep position at the end.
		s.state = stateReady
		s.stream = nil
		s.mu.Unlock()
		close(st.stop)
		return false
	}
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
	s.pos += len(payload)
	channel := s.rtpChannel
	s.mu.Unlock()

	raw, err := pkt.Marshal()
	if err != nil {
		return false
	}
	if err := st.conn.writeInterleaved(channel, raw); err != nil {
		st.conn.close()
		return false
	}
	return true
}

func (st *streamer) stopAndWait() {
	select {
	case <-st.stop:
	default:
		close(st.stop)
	}
	<-st.done
}
