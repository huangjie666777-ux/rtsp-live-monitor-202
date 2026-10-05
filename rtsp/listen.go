package rtsp

import (
	"fmt"
	"strings"
)

// liveSourceFor returns the named source only when it is open for
// listeners, i.e. its publisher has completed RECORD.
func (s *Server) liveSourceFor(name string) *liveSource {
	src := s.live.get(name)
	if src == nil || !src.recording.Load() {
		return nil
	}
	return src
}

// handleLiveDescribe serves SDP for a live stream. Live streams have no
// known duration, so the range is open-ended.
func (c *clientConn) handleLiveDescribe(req *Request, name string) error {
	if !validLiveName(name) || c.srv.liveSourceFor(name) == nil {
		return c.fail(req, 404, "Not Found")
	}
	body := fmt.Sprintf(
		"v=0\r\n"+
			"o=- 0 0 IN IP4 0.0.0.0\r\n"+
			"s=rtsprelay202 live %s\r\n"+
			"t=0 0\r\n"+
			"a=range:npt=0-\r\n"+
			"m=audio 0 RTP/AVP 0\r\n"+
			"c=IN IP4 0.0.0.0\r\n"+
			"a=rtpmap:0 PCMU/8000/1\r\n"+
			"a=control:%s\r\n",
		name, trackControl)
	return c.respond(req, 200, "OK", map[string]string{
		"Content-Base": req.URI,
	}, body)
}

// handleLiveSetup prepares a listener session on a client-chosen pair of
// interleaved channels.
func (c *clientConn) handleLiveSetup(req *Request, name string) error {
	src := c.srv.liveSourceFor(name)
	if !validLiveName(name) || src == nil {
		return c.fail(req, 404, "Not Found")
	}
	if c.sess != nil && c.sess.State() == statePlaying {
		return c.fail(req, 459, "Aggregate Operation Not Allowed")
	}
	rtpCh, rtcpCh, mode, ok := parseTransport(req.Header("transport"))
	if !ok || modeRecord(mode) {
		return c.fail(req, 461, "Unsupported Transport")
	}

	if c.sess == nil {
		c.sess = newSession()
	}
	s := c.sess
	s.mu.Lock()
	s.kind = kindListener
	s.live = src
	s.rtpChannel, s.rtcpChannel = rtpCh, rtcpCh
	s.state = stateReady
	s.mu.Unlock()

	return c.respond(req, 200, "OK", map[string]string{
		"Transport": fmt.Sprintf("RTP/AVP/TCP;unicast;interleaved=%d-%d", rtpCh, rtcpCh),
	}, "")
}

// handleLivePlay attaches the listener to the live source. The PLAY
// response is sent before the first RTP packet. Range is meaningless on
// a live source and always rejected.
func (c *clientConn) handleLivePlay(req *Request, s *Session) error {
	if req.Header("range") != "" {
		return c.fail(req, 457, "Invalid Range")
	}
	s.mu.Lock()
	if s.state != stateReady {
		s.mu.Unlock()
		return c.fail(req, 455, "Method Not Valid in This State")
	}
	src := s.live
	s.state = statePlaying
	w := newLiveWriter(s, c, src)
	s.liveW = w
	seq, ts := s.seq, s.rtpTime
	s.mu.Unlock()

	if src == nil || !src.recording.Load() || !w.attach() {
		s.mu.Lock()
		s.state = stateReady
		s.liveW = nil
		s.mu.Unlock()
		return c.fail(req, 404, "Not Found")
	}

	err := c.respond(req, 200, "OK", map[string]string{
		"Range": "npt=0-",
		"RTP-Info": fmt.Sprintf("url=%s;seq=%d;rtptime=%d",
			strings.TrimSuffix(req.URI, "/"+trackControl)+"/"+trackControl, seq, ts),
	}, "")
	if err != nil {
		return err
	}
	go w.run()
	return nil
}
