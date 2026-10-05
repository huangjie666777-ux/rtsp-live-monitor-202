# rtsprelay202 — RTSP 1.0 PCMU audio server

A minimal RTSP 1.0 backend (Go 1.27.1, Pion RTP 1.8.25) that streams
`assets/demo.ulaw` (8 kHz mono PCMU, RTP payload type 0) over
RTP/AVP/TCP interleaved connections. Built for device interop testing.

## Layout

- `main.go` — CLI entry (`-port`, `-file`), loads media, starts server.
- `rtsp/request.go` — request parser: CRLF lines, `Content-Length` bodies,
  CSeq extraction, half-packet and pipelined-request handling, `$` binary
  frame skipping (client RTCP), and size limits (request line 4 KiB,
  headers 64 KiB, body 1 MiB, interleaved frame 64 KiB).
- `rtsp/server.go` — connection loop and method handlers
  (OPTIONS / DESCRIBE / SETUP / PLAY / PAUSE / TEARDOWN). All writes
  (responses and `$` frames) are serialized through one mutex with a 5 s
  write deadline.
- `rtsp/session.go` — per-connection exclusive session: random 64-bit hex
  Session id, random SSRC, running RTP sequence/timestamp counters.
- `rtsp/stream.go` — 20 ms ticker sender: 160 samples per RTP v2 packet,
  payload type 0, continuous sequence numbers, timestamp = sent samples.
- `media/source.go` — the 8 kHz mono PCMU clip (64000 samples = 8.000 s).
- `client/main.go` — demo TCP client used for the interop check below.
- `rtsp/request_test.go` — parser unit tests (half packets, pipelining,
  interleaved frames, limits).

## Behavior

- Resource `rtsp://<host>:<port>/demo`, single track `trackID=0`.
- SDP (`DESCRIBE`) advertises `m=audio 0 RTP/AVP 0`, `a=rtpmap:0 PCMU/8000/1`
  and the clip duration via `a=range:npt=0-8.000`.
- Only `RTP/AVP/TCP;unicast;interleaved=<rtp>-<rtcp>` with two distinct
  client-chosen channels is accepted (`461` otherwise).
- `SETUP` creates a random per-connection Session. Wrong/unknown Session
  ids get `454`; methods invalid in the current state get `455`
  (including PLAY while playing).
- `PLAY` without `Range` resumes at the paused position. `Range: npt=x-`
  seeks: x must be finite, >= 0 and < duration (`457` otherwise) and is
  aligned down to a 20 ms boundary. The response carries the aligned
  `Range` and `RTP-Info` (url/seq/rtptime). Seeking only moves the file
  cursor — SSRC, sequence and timestamp keep running. The first RTP
  packet is sent strictly after the PLAY response.
- `PAUSE` replies first, then stops packets; the next sample position is
  kept, so resuming has no gap or overlap. End of file acts like PAUSE;
  playing again restarts from 0.
- `TEARDOWN`, disconnect, or a write timeout stops the sender and frees
  the session. Clients are fully independent.

## Build and run

    go build ./...
    go test ./...
    go build -o bin/rtspserver .
    ./bin/rtspserver -port 8554          # default -file assets/demo.ulaw

## Demo client

    go build -o bin/rtspdemo ./client
    ./bin/rtspdemo 127.0.0.1:8554

The demo exercises, in order: pipelined OPTIONS+DESCRIBE in one TCP
write, a SETUP split across two writes (half packet), PLAY with ~50
RTP packets/s on the client's interleaved channel 0, PLAY-while-playing
rejection (455), PAUSE silence, gapless resume, seek with
`Range: npt=2.007-` aligned to 2.000, invalid range (457), wrong session
(454), client-to-server RTCP `$` frame skipping, and TEARDOWN.
