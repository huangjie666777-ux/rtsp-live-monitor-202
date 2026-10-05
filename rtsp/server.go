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
	demoPath     = "/demo"
	livePrefix   = "/live/"
	trackControl = "trackID=0"
	writeTimeout = 5 * time.Second
	serverName   = "rtsprelay202/1.0"
)

// Server is an RTSP 1.0 server over TCP (interleaved RTP/AVP/TCP only).
// It serves the /demo file stream and any number of /live/<name>
// published streams.
type Server struct {
	src  *media.Source
	live *registry
}

func NewServer(src *media.Source) *Server {
	return &Server{src: src, live: newRegistry()}
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

	// Publisher state: live source announced on this connection.
	pubName string
	pubSrc  *liveSource
}

func newClientConn(conn net.Conn, srv *Server) *clientConn {
	return &clientConn{conn: conn, srv: srv, closed: make(chan struct{})}
}

func (c *clientConn) close() {
	c.closeMu.Do(func() {
		close(c.closed)
		c.conn.Close()
	})
}

func (c *clientConn) serve() {
	defer func() {
		c.teardownSession()
		c.releasePublisher()
		c.close()
	}()
	r := bufio.NewReader(c.conn)
	for {
		req, frame, err := readMessage(r)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) {
				log.Printf("[%s] read: %v", c.conn.RemoteAddr(), err)
			}
			return
		}
		if frame != nil {
			// Interleaved RTP from a publisher is distributed;
			// anything else (e.g. RTCP) is skipped.
			c.handleFrame(frame)
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

// requestPath extracts the path of an rtsp://host[:port]/... URI.
func requestPath(uri string) (string, bool) {
	u := strings.TrimPrefix(uri, "rtsp://")
	if u == uri {
		return "", false
	}
	i := strings.IndexByte(u, '/')
	if i < 0 {
		return "", false
	}
	return u[i:], true
}

// liveName returns the stream name for /live/<name> (track=false) or
// /live/<name>/trackID=0 (track=true) paths.
func liveName(path string) (name string, track bool, ok bool) {
	rest, found := strings.CutPrefix(path, livePrefix)
	if !found || rest == "" {
		return "", false, false
	}
	if !strings.Contains(rest, "/") {
		return rest, false, true
	}
	base, suffix, _ := strings.Cut(rest, "/")
	if base == "" || suffix != trackControl {
		return "", false, false
	}
	return base, true, true
}

func (c *clientConn) handle(req *Request) error {
	if req.CSeq() == "" {
		return c.fail(req, 400, "Bad Request")
	}
	path, ok := requestPath(req.URI)
	if !ok {
		return c.fail(req, 404, "Not Found")
	}
	name, _, isLive := liveName(path)

	switch req.Method {
	case "OPTIONS":
		return c.respond(req, 200, "OK", map[string]string{
			"Public": "OPTIONS, DESCRIBE, SETUP, PLAY, PAUSE, TEARDOWN, ANNOUNCE, RECORD",
		}, "")
	case "ANNOUNCE":
		if !isLive || strings.Contains(strings.TrimPrefix(path, livePrefix), "/") {
			return c.fail(req, 404, "Not Found")
		}
		return c.handleAnnounce(req, name)
	case "DESCRIBE":
		if path == demoPath {
			return c.handleDescribe(req)
		}
		if isLive && !strings.Contains(strings.TrimPrefix(path, livePrefix), "/") {
			return c.handleLiveDescribe(req, name)
		}
		return c.fail(req, 404, "Not Found")
	case "SETUP":
		if path == demoPath+"/"+trackControl {
			return c.handleSetup(req)
		}
		if isLive && strings.HasSuffix(path, "/"+trackControl) {
			if c.pubSrc != nil && c.pubName == name {
				return c.handlePublishSetup(req, name)
			}
			return c.handleLiveSetup(req, name)
		}
		return c.fail(req, 404, "Not Found")
	case "PLAY":
		return c.handlePlay(req)
	case "PAUSE":
		return c.handlePause(req)
	case "TEARDOWN":
		return c.handleTeardown(req)
	case "RECORD":
		return c.handleRecord(req)
	default:
		return c.fail(req, 405, "Method Not Allowed")
	}
}

// requireSession returns the connection session when the request carries
// a matching Session header. Control methods (PLAY/PAUSE/TEARDOWN/RECORD)
// must not run without it.
func (c *clientConn) requireSession(req *Request) *Session {
	s := c.sess
	if s == nil || req.Header("session") != s.ID {
		return nil
	}
	return s
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

// parseTransport parses one RTP/AVP/TCP;unicast;interleaved=a-b[;mode=...]
// transport spec.
func parseTransport(tr string) (rtpCh, rtcpCh int, mode string, ok bool) {
	if tr == "" {
		return 0, 0, "", false
	}
	spec := strings.Split(tr, ",")[0]
	parts := strings.Split(spec, ";")
	if !strings.EqualFold(strings.TrimSpace(parts[0]), "RTP/AVP/TCP") {
		return 0, 0, "", false
	}
	rtpCh, rtcpCh = -1, -1
	unicast := false
	for _, p := range parts[1:] {
		p = strings.TrimSpace(p)
		switch {
		case strings.EqualFold(p, "unicast"):
			unicast = true
		case strings.HasPrefix(strings.ToLower(p), "interleaved="):
			v := p[len("interleaved="):]
			fmt.Sscanf(v, "%d-%d", &rtpCh, &rtcpCh)
		case strings.HasPrefix(strings.ToLower(p), "mode="):
			mode = p[len("mode="):]
		}
	}
	if !unicast || rtpCh < 0 || rtcpCh < 0 || rtpCh == rtcpCh || rtpCh > 255 || rtcpCh > 255 {
		return 0, 0, "", false
	}
	return rtpCh, rtcpCh, mode, true
}

func (c *clientConn) handleSetup(req *Request) error {
	if c.sess != nil && c.sess.State() == statePlaying {
		return c.fail(req, 459, "Aggregate Operation Not Allowed")
	}
	rtpCh, rtcpCh, _, ok := parseTransport(req.Header("transport"))
	if !ok {
		return c.fail(req, 461, "Unsupported Transport")
	}

	if c.sess == nil {
		c.sess = newSession()
	}
	s := c.sess
	s.mu.Lock()
	s.kind = kindDemo
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
	s := c.requireSession(req)
	if s == nil {
		return c.fail(req, 454, "Session Not Found")
	}
	s.mu.Lock()
	kind := s.kind
	s.mu.Unlock()
	if kind == kindListener {
		return c.handleLivePlay(req, s)
	}
	if kind != kindDemo {
		return c.fail(req, 455, "Method Not Valid in This State")
	}
	return c.handleDemoPlay(req, s)
}

func (c *clientConn) handleDemoPlay(req *Request, s *Session) error {
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
	s := c.requireSession(req)
	if s == nil {
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

	// Respond first, then stop packets.
	if err := c.respond(req, 200, "OK", nil, ""); err != nil {
		return err
	}
	if st != nil {
		st.stopAndWait()
	}
	// Listener: drop the subscription and any queued packets, so a
	// resumed PLAY only delivers newly arrived audio.
	liveSessionCleanup(s)
	return nil
}

func (c *clientConn) handleTeardown(req *Request) error {
	s := c.requireSession(req)
	if s == nil {
		return c.fail(req, 454, "Session Not Found")
	}
	c.teardownSession()
	return c.respond(req, 200, "OK", nil, "")
}

func (c *clientConn) teardownSession() {
	s := c.sess
	if s == nil {
		return
	}
	s.mu.Lock()
	st := s.stream
	s.stream = nil
	s.state = stateInit
	s.mu.Unlock()
	if st != nil {
		st.stopAndWait()
	}
	liveSessionCleanup(s)
	if s.kind == kindPublisher {
		c.releasePublisher()
	}
	c.sess = nil
}
