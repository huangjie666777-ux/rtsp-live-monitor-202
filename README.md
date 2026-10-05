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

## Live publishing and listening (`/live/<name>`)

Device microphone audio can be published in real time and fanned out to
any number of diagnostic listeners. `/demo` file playback is unaffected.

### Publishing

- `ANNOUNCE rtsp://<host>:<port>/live/<name>` with an `application/sdp`
  body. `<name>` is 1-32 letters, digits or underscores; a second
  publisher with the same name gets `409 Conflict`. The SDP must describe
  exactly one track: `m=audio ... RTP/AVP 0`, `a=rtpmap:0 PCMU/8000/1`,
  `a=control:trackID=0` (`400` otherwise).
- `SETUP .../live/<name>/trackID=0` with
  `Transport: RTP/AVP/TCP;unicast;interleaved=<rtp>-<rtcp>;mode="record"`
  (two distinct channels, `461` otherwise).
- `RECORD` opens the stream for listeners. From then on the publisher
  writes interleaved `$` frames on the negotiated RTP channel: RTP v2
  packets (CSRC, extension headers and padding are parsed) with exactly
  160 bytes of PCMU payload (20 ms). Invalid frames are dropped, RTCP is
  skipped, and RTSP control requests on the same connection keep working.
  Nothing is written to disk or transcoded.
- `TEARDOWN` or disconnect removes the source and closes all its
  listener connections; the name can immediately be re-published. Stale
  cleanup of an old session never removes a newer source with the same
  name (`rtsp/live.go` `liveRegistry.remove` compares ownership).

### Listening

- `DESCRIBE` / `SETUP` / `PLAY` on `/live/<name>` work only after
  `RECORD` succeeded (`404` before). Listeners pick their own interleaved
  channels. The `PLAY` response is sent strictly before the first RTP
  packet; live sources reject `Range` with `457`.
- Every listener gets an independent SSRC, continuous sequence numbers
  and timestamps incrementing by 160 per packet; payloads are forwarded
  unchanged, packet for packet.
- `PAUSE` stops delivery and drops queued packets; resuming `PLAY` only
  receives newly published audio.
- Each listener has a 64-packet queue. A full queue or a write timeout
  disconnects only that slow listener — the publisher and other
  listeners are never blocked.

### Session requirement

PLAY / PAUSE / RECORD / TEARDOWN now require a matching `Session`
header; requests without one (or with an unknown id) get
`454 Session Not Found`.

### Code map (live path)

- `rtsp/request.go` — `readMessage` splits the connection into RTSP
  requests and interleaved `$` frames (`readRequest` kept for tests).
- `rtsp/publish.go` — name/SDP validation, `ANNOUNCE`/`RECORD` handlers,
  publisher frame parsing and broadcast entry point.
- `rtsp/live.go` — `liveRegistry` (name ownership), `liveSource`
  (recording state, fan-out, teardown) and `liveListener` (64-packet
  queue, per-listener RTP packetizer).
- `rtsp/session.go` — session mode (play/record) and live attachments.
- `rtsp/server.go` — `/live/<name>` routing, listener
  DESCRIBE/SETUP/PLAY/PAUSE, session-required fix, cleanup.
- `rtsp/live_test.go` — unit tests for the above.

### Live demo client

    go build -o bin/rtspdemo ./client
    ./bin/rtspdemo -live 127.0.0.1:8554

Runs one publisher (ANNOUNCE/SETUP record/RECORD, 100 RTP packets) and
two listeners on different interleaved channels; both verify payload
content, sequence continuity and +160 timestamps, then the publisher
TEARDOWN closes both listener connections.
