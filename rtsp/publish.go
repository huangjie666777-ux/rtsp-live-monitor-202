package rtsp

import (
	"fmt"
	"strings"

	"github.com/pion/rtp"

	"rtsprelay202/media"
)

// announceSDP validates an ANNOUNCE body: exactly one audio track with
// RTP payload type 0 (PCMU), 8 kHz mono, controlled as trackID=0.
func announceSDP(body []byte) error {
	sdp := strings.ReplaceAll(string(body), "\r\n", "\n")
	mediaSections := 0
	payloadOK, rtpmapOK, controlOK := false, false, false
	for _, line := range strings.Split(sdp, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "m="):
			mediaSections++
			f := strings.Fields(line)
			// m=audio <port> RTP/AVP 0
			if len(f) == 4 && f[0] == "m=audio" && f[2] == "RTP/AVP" && f[3] == "0" {
				payloadOK = true
			}
		case strings.HasPrefix(line, "a=rtpmap:"):
			f := strings.Fields(line)
			if len(f) == 2 && f[0] == "a=rtpmap:0" &&
				(f[1] == "PCMU/8000" || f[1] == "PCMU/8000/1") {
				rtpmapOK = true
			}
		case strings.HasPrefix(line, "a=control:"):
			if line == "a=control:"+trackControl {
				controlOK = true
			}
		}
	}
	if mediaSections != 1 || !payloadOK || !rtpmapOK || !controlOK {
		return fmt.Errorf("need exactly one m=audio RTP/AVP 0 track with PCMU/8000/1 and a=control:%s", trackControl)
	}
	return nil
}

// handleAnnounce registers a new live source for /live/<name>.
func (c *clientConn) handleAnnounce(req *Request, name string) error {
	if !validLiveName(name) {
		return c.fail(req, 400, "Bad Request")
	}
	if ct := req.Header("content-type"); ct != "" &&
		!strings.EqualFold(strings.TrimSpace(ct), "application/sdp") {
		return c.fail(req, 415, "Unsupported Media Type")
	}
	if err := announceSDP(req.Body); err != nil {
		return c.fail(req, 400, "Bad Request")
	}
	// A re-ANNOUNCE on the same connection replaces the previous source.
	c.releasePublisher()
	src := newLiveSource(name)
	if !c.srv.live.register(name, src) {
		return c.fail(req, 409, "Conflict")
	}
	c.pubName = name
	c.pubSrc = src
	return c.respond(req, 200, "OK", nil, "")
}

// handlePublishSetup handles SETUP .../live/<name>/trackID=0 with
// mode="record" after a successful ANNOUNCE on this connection.
func (c *clientConn) handlePublishSetup(req *Request, name string) error {
	if c.pubSrc == nil || c.pubName != name {
		return c.fail(req, 400, "Bad Request")
	}
	rtpCh, rtcpCh, mode, ok := parseTransport(req.Header("transport"))
	if !ok {
		return c.fail(req, 461, "Unsupported Transport")
	}
	if !modeRecord(mode) {
		return c.fail(req, 461, "Unsupported Transport")
	}
	if c.sess == nil {
		c.sess = newSession()
	}
	s := c.sess
	s.mu.Lock()
	s.kind = kindPublisher
	s.live = c.pubSrc
	s.rtpChannel, s.rtcpChannel = rtpCh, rtcpCh
	s.state = stateReady
	s.mu.Unlock()

	return c.respond(req, 200, "OK", map[string]string{
		"Transport": fmt.Sprintf("RTP/AVP/TCP;unicast;interleaved=%d-%d;mode=\"record\"", rtpCh, rtcpCh),
	}, "")
}

func modeRecord(mode string) bool {
	m := strings.Trim(strings.ToLower(strings.TrimSpace(mode)), "\"")
	return m == "record"
}

// handleRecord switches the publisher session into recording; from the
// 200 response on, listeners may attach and incoming RTP is distributed.
func (c *clientConn) handleRecord(req *Request) error {
	s := c.sess
	if s == nil || req.Header("session") != s.ID {
		return c.fail(req, 454, "Session Not Found")
	}
	s.mu.Lock()
	if s.kind != kindPublisher || s.state != stateReady {
		s.mu.Unlock()
		return c.fail(req, 455, "Method Not Valid in This State")
	}
	s.state = statePlaying
	src := s.live
	s.mu.Unlock()

	if err := c.respond(req, 200, "OK", nil, ""); err != nil {
		return err
	}
	// Open the stream to listeners only after RECORD succeeded.
	src.recording.Store(true)
	return nil
}

// handleFrame processes one interleaved '$' frame from a publisher
// connection. RTP on the negotiated channel is parsed (CSRC, extension
// headers and padding handled by the RTP parser) and only intact
// 160-byte PCMU payloads are distributed; anything else is dropped.
// RTCP and frames on other channels are skipped.
func (c *clientConn) handleFrame(f *Frame) {
	s := c.sess
	if s == nil {
		return
	}
	s.mu.Lock()
	publisher := s.kind == kindPublisher && s.state == statePlaying
	rtpCh := s.rtpChannel
	src := s.live
	s.mu.Unlock()
	if !publisher || src == nil || int(f.Channel) != rtpCh {
		return
	}
	var pkt rtp.Packet
	if err := pkt.Unmarshal(f.Payload); err != nil {
		return // malformed RTP: drop
	}
	if pkt.Version != 2 || pkt.PayloadType != payloadTypePCMU {
		return
	}
	if len(pkt.Payload) != media.SamplesPerTick {
		return // only full 20ms (160 sample) payloads are accepted
	}
	src.broadcast(pkt.Payload)
}

// releasePublisher unregisters and closes the live source owned by this
// connection, if any. Safe to call multiple times; never removes a newer
// source registered under the same name.
func (c *clientConn) releasePublisher() {
	if c.pubSrc != nil {
		c.srv.live.unregister(c.pubName, c.pubSrc)
		c.pubSrc.close()
		c.pubSrc = nil
		c.pubName = ""
	}
}
