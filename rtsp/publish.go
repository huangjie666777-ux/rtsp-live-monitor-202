package rtsp

import (
	"regexp"
	"strings"

	"github.com/pion/rtp"

	"rtsprelay202/media"
)

// liveNameRe restricts stream names to 1-32 letters, digits or underscores.
var liveNameRe = regexp.MustCompile(`^[A-Za-z0-9_]{1,32}$`)

// parseLivePath extracts the stream name from a /live/<name>[...] request
// path. track reports whether the /trackID=0 suffix is present.
func parseLivePath(path string) (name string, track bool, ok bool) {
	rest, found := strings.CutPrefix(path, "/live/")
	if !found {
		return "", false, false
	}
	if strings.HasSuffix(rest, "/"+trackControl) {
		rest = strings.TrimSuffix(rest, "/"+trackControl)
		track = true
	}
	if !liveNameRe.MatchString(rest) {
		return "", false, false
	}
	return rest, track, true
}

// validAnnounceSDP reports whether the SDP describes exactly one audio
// track: trackID=0, 8kHz mono PCMU, RTP payload type 0.
func validAnnounceSDP(body []byte) bool {
	var mediaSections, controlOK, rtpmapOK, payloadOK int
	for _, line := range strings.Split(strings.ReplaceAll(string(body), "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "m="):
			mediaSections++
			fields := strings.Fields(line)
			if len(fields) >= 4 && fields[0] == "m=audio" {
				for _, f := range fields[3:] {
					if f == "0" {
						payloadOK++
					}
				}
			}
		case line == "a=control:"+trackControl:
			controlOK++
		case strings.HasPrefix(line, "a=rtpmap:0 "):
			if strings.EqualFold(strings.TrimPrefix(line, "a=rtpmap:0 "), "PCMU/8000/1") {
				rtpmapOK++
			}
		}
	}
	return mediaSections == 1 && controlOK == 1 && rtpmapOK == 1 && payloadOK == 1
}

// handleAnnounce registers a pending publisher for /live/<name>.
func (c *clientConn) handleAnnounce(req *Request, name string) error {
	if c.pub != nil {
		return c.fail(req, 455, "Method Not Valid in This State")
	}
	if !validAnnounceSDP(req.Body) {
		return c.fail(req, 400, "Bad Request")
	}
	src := newLiveSource(name, c.srv.live)
	if !c.srv.live.reserve(name, src) {
		return c.fail(req, 409, "Conflict")
	}
	c.pub = src
	return c.respond(req, 200, "OK", nil, "")
}

// handleRecord opens the stream for listeners; from now on interleaved
// RTP frames from this connection are distributed.
func (c *clientConn) handleRecord(req *Request) error {
	s := c.sess
	if s == nil || req.Header("session") != s.ID {
		return c.fail(req, 454, "Session Not Found")
	}
	s.mu.Lock()
	if s.mode != modeRecord || s.state != stateReady || s.live == nil {
		s.mu.Unlock()
		return c.fail(req, 455, "Method Not Valid in This State")
	}
	s.state = stateRecording
	src := s.live
	s.mu.Unlock()

	src.setRecording()
	return c.respond(req, 200, "OK", nil, "")
}

// handleFrame processes one interleaved '$' frame from a connection. On a
// recording publisher connection the RTP payload is parsed and broadcast;
// RTCP is skipped and anything invalid is silently dropped. On all other
// connections frames are ignored.
func (c *clientConn) handleFrame(fr *frame) {
	s := c.sess
	if s == nil {
		return
	}
	s.mu.Lock()
	recording := s.mode == modeRecord && s.state == stateRecording
	rtpCh, rtcpCh := s.rtpChannel, s.rtcpChannel
	src := s.live
	s.mu.Unlock()
	if !recording || src == nil {
		return
	}
	ch := int(fr.channel)
	if ch == rtcpCh {
		return // RTCP from the publisher is ignored
	}
	if ch != rtpCh {
		return
	}
	var pkt rtp.Packet
	if err := pkt.Unmarshal(fr.payload); err != nil {
		return // invalid RTP: drop
	}
	if pkt.Version != 2 || len(pkt.Payload) != media.SamplesPerTick {
		return // only 160-byte (20ms) PCMU payloads are accepted
	}
	src.broadcast(pkt.Payload)
}
