package rtsp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"rtsprelay202/media"
)

const (
	resourcePath = "/demo"
	trackControl = "trackID=0"
	writeTimeout = 5 * time.Second
	serverName   = "rtsprelay202/1.0"
)

// Server is an RTSP 1.0 server over TCP (interleaved RTP/AVP/TCP only).
type Server struct {
	src  *media.Source
	live *liveRegistry
}

func NewServer(src *media.Source) *Server {
	return &Server{src: src, live: newLiveRegistry()}
}

func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		c := newClientConn(conn, s)
		go c.serve()
	}
}

// clientConn serializes all writes (responses and '$' frames) and owns the
// per-connection session.
type clientConn struct {
	conn    net.Conn
	srv     *Server
	writeMu sync.Mutex
	closed  chan struct{}
	closeMu sync.Once
	sess    *Session
	pub     *liveSource // announced on this connection, nil otherwise
}

func newClientConn(conn net.Conn, srv *Server) *clientConn {
	return &clientConn{conn: conn, srv: srv, closed: make(chan struct{})}
}

func (c *clientConn) close() {
	c.closeMu.Do(func() {
		close(c.closed)
		if c.conn != nil {
			c.conn.Close()
		}
	})
}

func (c *clientConn) serve() {
	defer func() {
		c.teardownSession()
		c.close()
	}()
	r := bufio.NewReader(c.conn)
	for {
		req, fr, err := readMessage(r)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
				log.Printf("[%s] read: %v", c.conn.RemoteAddr(), err)
			}
			return
		}
		if fr != nil {
			c.handleFrame(fr)
			continue
		}
		if err := c.handle(req); err != nil {
			log.Printf("[%s] handle %s: %v", c.conn.RemoteAddr(), req.Method, err)
			return
		}
	}
}

// writeRaw writes with a deadline; responses and '$' frames are serialized
// through writeMu so message boundaries are never interleaved.
func (c *clientConn) writeRaw(p []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_, err := c.conn.Write(p)
	return err
}

func (c *clientConn) writeInterleaved(channel int, payload []byte) error {
	hdr := []byte{'$', byte(channel), byte(len(payload) >> 8), byte(len(payload))}
	return c.writeRaw(append(hdr, payload...))
}

func (c *clientConn) respond(req *Request, code int, reason string, extra map[string]string, body string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "RTSP/1.0 %d %s\r\n", code, reason)
	if cs := req.CSeq(); cs != "" {
		fmt.Fprintf(&b, "CSeq: %s\r\n", cs)
	}
	fmt.Fprintf(&b, "Server: %s\r\n", serverName)
	if c.sess != nil {
		fmt.Fprintf(&b, "Session: %s\r\n", c.sess.ID)
	}
	for k, v := range extra {
		fmt.Fprintf(&b, "%s: %s\r\n", k, v)
	}
	if body != "" {
		fmt.Fprintf(&b, "Content-Type: application/sdp\r\nContent-Length: %d\r\n", len(body))
	}
	b.WriteString("\r\n")
	b.WriteString(body)
	return c.writeRaw([]byte(b.String()))
}

func (c *clientConn) fail(req *Request, code int, reason string) error {
	return c.respond(req, code, reason, nil, "")
}

const publicMethods = "OPTIONS, DESCRIBE, SETUP, PLAY, PAUSE, RECORD, ANNOUNCE, TEARDOWN"

// uriPath extracts the path of an rtsp://host[:port]/path URI.
func uriPath(uri string) (string, bool) {
	u := strings.TrimPrefix(uri, "rtsp://")
	if u == uri {
		return "", false
	}
	if i := strings.IndexByte(u, '/'); i >= 0 {
		return u[i:], true
	}
	return "", false
}

func (c *clientConn) handle(req *Request) error {
	if req.CSeq() == "" {
		return c.fail(req, 400, "Bad Request")
	}
	path, ok := uriPath(req.URI)
	if !ok {
		return c.fail(req, 404, "Not Found")
	}
	if name, track, isLive := parseLivePath(path); isLive {
		return c.handleLive(req, name, track)
	}
	if path != resourcePath && path != resourcePath+"/"+trackControl {
		return c.fail(req, 404, "Not Found")
	}
	switch req.Method {
	case "OPTIONS":
		return c.respond(req, 200, "OK", map[string]string{
			"Public": publicMethods,
		}, "")
	case "DESCRIBE":
		return c.handleDescribe(req)
	case "SETUP":
		return c.handleSetup(req)
	case "PLAY":
		return c.handlePlay(req)
	case "PAUSE":
		return c.handlePause(req)
	case "TEARDOWN":
		return c.handleTeardown(req)
	default:
		return c.fail(req, 405, "Method Not Allowed")
	}
}

func (c *clientConn) handleDescribe(req *Request) error {
	dur := c.srv.src.Duration()
	body := fmt.Sprintf(
		"v=0\r\n"+
			"o=- 0 0 IN IP4 0.0.0.0\r\n"+
			"s=rtsprelay202 demo\r\n"+
			"t=0 0\r\n"+
			"a=range:npt=0-%.3f\r\n"+
			"m=audio 0 RTP/AVP 0\r\n"+
			"c=IN IP4 0.0.0.0\r\n"+
			"a=rtpmap:0 PCMU/8000/1\r\n"+
			"a=control:%s\r\n",
		dur, trackControl)
	return c.respond(req, 200, "OK", map[string]string{
		"Content-Base": req.URI,
	}, body)
}

func (c *clientConn) handleSetup(req *Request) error {
	if c.sess != nil && c.sess.State() == statePlaying {
		return c.fail(req, 459, "Aggregate Operation Not Allowed")
	}
	tr := req.Header("transport")
	if tr == "" {
		return c.fail(req, 400, "Bad Request")
	}
	spec := strings.Split(tr, ",")[0]
	parts := strings.Split(spec, ";")
	if !strings.EqualFold(strings.TrimSpace(parts[0]), "RTP/AVP/TCP") {
		return c.fail(req, 461, "Unsupported Transport")
	}
	rtpCh, rtcpCh := -1, -1
	unicast := false
	for _, p := range parts[1:] {
		p = strings.TrimSpace(p)
		if strings.EqualFold(p, "unicast") {
			unicast = true
		}
		if v, ok := strings.CutPrefix(strings.ToLower(p), "interleaved="); ok {
			fmt.Sscanf(v, "%d-%d", &rtpCh, &rtcpCh)
		}
	}
	if !unicast || rtpCh < 0 || rtcpCh < 0 || rtpCh == rtcpCh || rtpCh > 255 || rtcpCh > 255 {
		return c.fail(req, 461, "Unsupported Transport")
	}

	if c.sess == nil {
		c.sess = newSession()
	}
	s := c.sess
	s.mu.Lock()
	s.rtpChannel, s.rtcpChannel = rtpCh, rtcpCh
	s.state = stateReady
	s.mu.Unlock()

	return c.respond(req, 200, "OK", map[string]string{
		"Transport": fmt.Sprintf("RTP/AVP/TCP;unicast;interleaved=%d-%d", rtpCh, rtcpCh),
	}, "")
}

// parseRange parses "npt=x-" (or "npt=x-y"); returns start seconds.
func parseRange(h string) (float64, bool) {
	v, ok := strings.CutPrefix(strings.TrimSpace(h), "npt=")
	if !ok {
		return 0, false
	}
	start, _, _ := strings.Cut(v, "-")
	f, err := strconv.ParseFloat(strings.TrimSpace(start), 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}

func (c *clientConn) handlePlay(req *Request) error {
	s := c.sess
	if s == nil || req.Header("session") != s.ID {
		return c.fail(req, 454, "Session Not Found")
	}
	s.mu.Lock()
	if s.state != stateReady {
		s.mu.Unlock()
		return c.fail(req, 455, "Method Not Valid in This State")
	}

	dur := c.srv.src.Duration()
	pos := s.pos
	if rh := req.Header("range"); rh != "" {
		start, ok := parseRange(rh)
		if !ok || start < 0 || start >= dur {
			s.mu.Unlock()
			return c.fail(req, 457, "Invalid Range")
		}
		// Align down to a 20ms (160-sample) boundary; seeking only moves
		// the file cursor, RTP sequence/timestamp keep running.
		pos = int(start*media.SampleRate) / media.SamplesPerTick * media.SamplesPerTick
	}
	if pos >= c.srv.src.Samples() {
		// Previously played to the end: replay from the beginning.
		pos = 0
	}
	s.pos = pos
	s.state = statePlaying
	st := newStreamer(s, c, c.srv.src)
	s.stream = st
	seq, ts := s.seq, s.rtpTime
	s.mu.Unlock()

	startSec := float64(pos) / media.SampleRate
	err := c.respond(req, 200, "OK", map[string]string{
		"Range": fmt.Sprintf("npt=%.3f-%.3f", startSec, dur),
		"RTP-Info": fmt.Sprintf("url=%s;seq=%d;rtptime=%d",
			strings.TrimSuffix(req.URI, "/"+trackControl)+"/"+trackControl, seq, ts),
	}, "")
	if err != nil {
		return err
	}
	// First RTP packet is sent strictly after the PLAY response.
	go st.run()
	return nil
}

func (c *clientConn) handlePause(req *Request) error {
	s := c.sess
	if s == nil || req.Header("session") != s.ID {
		return c.fail(req, 454, "Session Not Found")
	}
	s.mu.Lock()
	if s.state != statePlaying {
		s.mu.Unlock()
		return c.fail(req, 455, "Method Not Valid in This State")
	}
	st := s.stream
	s.stream = nil
	s.state = stateReady
	s.mu.Unlock()

	// Respond first, then stop packets; the sample position is kept.
	if err := c.respond(req, 200, "OK", nil, ""); err != nil {
		return err
	}
	if st != nil {
		st.stopAndWait()
	}
	return nil
}

func (c *clientConn) handleTeardown(req *Request) error {
	s := c.sess
	if s == nil || req.Header("session") != s.ID {
		return c.fail(req, 454, "Session Not Found")
	}
	c.teardownSession()
	return c.respond(req, 200, "OK", nil, "")
}

func (c *clientConn) teardownSession() {
	if s := c.sess; s != nil {
		s.mu.Lock()
		st := s.stream
		s.stream = nil
		l := s.listener
		s.listener = nil
		src := s.live
		mode := s.mode
		s.state = stateInit
		s.mu.Unlock()
		if st != nil {
			st.stopAndWait()
		}
		if l != nil {
			src.removeListener(l)
			l.stopAndWait()
		}
		if mode == modeRecord && src != nil {
			// Publisher gone: remove the source and disconnect its
			// listeners. remove() only deletes the registry entry if
			// this source still owns the name.
			src.close()
		}
		c.sess = nil
	}
	if c.pub != nil {
		// Announced but never RECORDed (or already closed above).
		c.pub.close()
		c.pub = nil
	}
}

// handleLive routes requests for /live/<name> resources.
func (c *clientConn) handleLive(req *Request, name string, track bool) error {
	switch req.Method {
	case "OPTIONS":
		return c.respond(req, 200, "OK", map[string]string{
			"Public": publicMethods,
		}, "")
	case "ANNOUNCE":
		if track {
			return c.fail(req, 404, "Not Found")
		}
		return c.handleAnnounce(req, name)
	case "DESCRIBE":
		return c.handleLiveDescribe(req, name)
	case "SETUP":
		return c.handleLiveSetup(req, name, track)
	case "RECORD":
		return c.handleRecord(req)
	case "PLAY":
		return c.handleLivePlay(req)
	case "PAUSE":
		return c.handleLivePause(req)
	case "TEARDOWN":
		return c.handleTeardown(req)
	default:
		return c.fail(req, 405, "Method Not Allowed")
	}
}

func (c *clientConn) handleLiveDescribe(req *Request, name string) error {
	src := c.srv.live.get(name)
	if src == nil || !src.isRecording() {
		// Listening opens only after RECORD succeeded.
		return c.fail(req, 404, "Not Found")
	}
	body := "v=0\r\n" +
		"o=- 0 0 IN IP4 0.0.0.0\r\n" +
		"s=rtsprelay202 live " + name + "\r\n" +
		"t=0 0\r\n" +
		"a=range:npt=0-\r\n" +
		"m=audio 0 RTP/AVP 0\r\n" +
		"c=IN IP4 0.0.0.0\r\n" +
		"a=rtpmap:0 PCMU/8000/1\r\n" +
		"a=control:" + trackControl + "\r\n"
	return c.respond(req, 200, "OK", map[string]string{
		"Content-Base": req.URI,
	}, body)
}

// parseTransportSpec parses the first Transport spec, requiring
// RTP/AVP/TCP, unicast and two distinct interleaved channels.
func parseTransportSpec(tr string) (rtpCh, rtcpCh int, mode string, ok bool) {
	spec := strings.Split(tr, ",")[0]
	parts := strings.Split(spec, ";")
	if !strings.EqualFold(strings.TrimSpace(parts[0]), "RTP/AVP/TCP") {
		return 0, 0, "", false
	}
	rtpCh, rtcpCh = -1, -1
	unicast := false
	for _, p := range parts[1:] {
		p = strings.TrimSpace(p)
		if strings.EqualFold(p, "unicast") {
			unicast = true
		}
		if v, is := strings.CutPrefix(strings.ToLower(p), "interleaved="); is {
			fmt.Sscanf(v, "%d-%d", &rtpCh, &rtcpCh)
		}
		if v, is := strings.CutPrefix(strings.ToLower(p), "mode="); is {
			mode = strings.Trim(strings.ToLower(v), "\"")
		}
	}
	if !unicast || rtpCh < 0 || rtcpCh < 0 || rtpCh == rtcpCh || rtpCh > 255 || rtcpCh > 255 {
		return 0, 0, "", false
	}
	return rtpCh, rtcpCh, mode, true
}

func (c *clientConn) handleLiveSetup(req *Request, name string, track bool) error {
	if c.sess != nil && c.sess.State() != stateInit && c.sess.State() != stateReady {
		return c.fail(req, 459, "Aggregate Operation Not Allowed")
	}
	tr := req.Header("transport")
	if tr == "" {
		return c.fail(req, 400, "Bad Request")
	}
	rtpCh, rtcpCh, mode, ok := parseTransportSpec(tr)
	if !ok {
		return c.fail(req, 461, "Unsupported Transport")
	}

	var src *liveSource
	sessMode := modePlay
	if mode == "record" {
		// Publisher: must have ANNOUNCEd this exact name first.
		if !track || c.pub == nil || c.pub.name != name {
			return c.fail(req, 400, "Bad Request")
		}
		src = c.pub
		sessMode = modeRecord
	} else {
		// Listener: the source must already be recording.
		src = c.srv.live.get(name)
		if src == nil || !src.isRecording() {
			return c.fail(req, 404, "Not Found")
		}
	}

	if c.sess == nil {
		c.sess = newSession()
	}
	s := c.sess
	s.mu.Lock()
	s.mode = sessMode
	s.live = src
	s.rtpChannel, s.rtcpChannel = rtpCh, rtcpCh
	s.state = stateReady
	s.mu.Unlock()

	transport := fmt.Sprintf("RTP/AVP/TCP;unicast;interleaved=%d-%d", rtpCh, rtcpCh)
	if sessMode == modeRecord {
		transport += ";mode=\"record\""
	}
	return c.respond(req, 200, "OK", map[string]string{
		"Transport": transport,
	}, "")
}

func (c *clientConn) handleLivePlay(req *Request) error {
	s := c.sess
	if s == nil || req.Header("session") != s.ID {
		return c.fail(req, 454, "Session Not Found")
	}
	if req.Header("range") != "" {
		return c.fail(req, 457, "Invalid Range") // live sources have no seek
	}
	s.mu.Lock()
	if s.mode != modePlay || s.live == nil || s.state != stateReady {
		s.mu.Unlock()
		return c.fail(req, 455, "Method Not Valid in This State")
	}
	l := newLiveListener(s.live, c, s)
	if !s.live.addListener(l) {
		s.mu.Unlock()
		return c.fail(req, 404, "Not Found")
	}
	s.listener = l
	s.state = statePlaying
	seq, ts := s.seq, s.rtpTime
	s.mu.Unlock()

	err := c.respond(req, 200, "OK", map[string]string{
		"Range": "npt=0-",
		"RTP-Info": fmt.Sprintf("url=%s;seq=%d;rtptime=%d",
			strings.TrimSuffix(req.URI, "/"+trackControl)+"/"+trackControl, seq, ts),
	}, "")
	if err != nil {
		return err
	}
	// The PLAY response is sent strictly before the first RTP packet.
	go l.run()
	return nil
}

func (c *clientConn) handleLivePause(req *Request) error {
	s := c.sess
	if s == nil || req.Header("session") != s.ID {
		return c.fail(req, 454, "Session Not Found")
	}
	s.mu.Lock()
	if s.mode != modePlay || s.state != statePlaying {
		s.mu.Unlock()
		return c.fail(req, 455, "Method Not Valid in This State")
	}
	l := s.listener
	s.listener = nil
	src := s.live
	s.state = stateReady
	s.mu.Unlock()

	// Respond first, then stop delivery and drop any queued packets, so a
	// resumed PLAY only receives newly published audio.
	if err := c.respond(req, 200, "OK", nil, ""); err != nil {
		return err
	}
	if l != nil {
		src.removeListener(l)
		l.stopAndWait()
	}
	return nil
}
