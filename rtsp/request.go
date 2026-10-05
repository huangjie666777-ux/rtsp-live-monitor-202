package rtsp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	maxRequestLine = 4096
	maxHeaderBytes = 64 * 1024
	maxBodyBytes   = 1 << 20
	maxFrameBytes  = 64 * 1024
)

// Request is a parsed RTSP request.
type Request struct {
	Method  string
	URI     string
	Proto   string
	Headers map[string]string
	Body    []byte
}

func (r *Request) Header(name string) string {
	return r.Headers[strings.ToLower(name)]
}

func (r *Request) CSeq() string { return r.Header("cseq") }

// readRequest reads one RTSP request, transparently skipping interleaved
// '$' binary frames (RTP/RTCP over TCP). Half packets, pipelined requests
// and Content-Length bodies are handled without breaking message boundaries.
func readRequest(r *bufio.Reader) (*Request, error) {
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == '$' {
			if err := skipInterleavedFrame(r); err != nil {
				return nil, err
			}
			continue
		}
		return readRequestHead(r, b)
	}
}

// skipInterleavedFrame consumes one '$' <channel> <len:2> <payload> frame.
func skipInterleavedFrame(r *bufio.Reader) error {
	hdr := make([]byte, 3)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return err
	}
	n := int(hdr[1])<<8 | int(hdr[2])
	if n > maxFrameBytes {
		return fmt.Errorf("interleaved frame too large: %d", n)
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return err
}

func readRequestHead(r *bufio.Reader, first byte) (*Request, error) {
	line, err := readLine(r, first)
	if err != nil {
		return nil, err
	}
	parts := strings.SplitN(line, " ", 3)
	if len(parts) != 3 || !strings.HasPrefix(parts[2], "RTSP/") {
		return nil, fmt.Errorf("malformed request line: %q", line)
	}
	req := &Request{Method: parts[0], URI: parts[1], Proto: parts[2], Headers: map[string]string{}}

	total := 0
	for {
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		hline, err := readLine(r, b)
		if err != nil {
			return nil, err
		}
		total += len(hline) + 2
		if total > maxHeaderBytes {
			return nil, fmt.Errorf("headers too large")
		}
		if hline == "" {
			break
		}
		k, v, ok := strings.Cut(hline, ":")
		if !ok {
			return nil, fmt.Errorf("malformed header: %q", hline)
		}
		req.Headers[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
	}

	if cl := req.Header("content-length"); cl != "" {
		n, err := strconv.Atoi(cl)
		if err != nil || n < 0 || n > maxBodyBytes {
			return nil, fmt.Errorf("bad Content-Length: %q", cl)
		}
		req.Body = make([]byte, n)
		if _, err := io.ReadFull(r, req.Body); err != nil {
			return nil, err
		}
	}
	return req, nil
}

// readLine reads until LF (a preceding CR is stripped). first is the
// already-consumed first byte of the line.
func readLine(r *bufio.Reader, first byte) (string, error) {
	var sb strings.Builder
	sb.WriteByte(first)
	for {
		if sb.Len() > maxRequestLine {
			return "", fmt.Errorf("line too long")
		}
		c, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		if c == 0x0a {
			return strings.TrimSuffix(sb.String(), ""), nil
		}
		sb.WriteByte(c)
	}
}
