# rtsprelay202 — RTSP 1.0 PCMU audio server

A minimal RTSP 1.0 backend (Go 1.27.1, Pion RTP 1.8.25) that streams
`assets/demo.ulaw` (8 kHz mono PCMU, RTP payload type 0) over
RTP/AVP/TCP interleaved connections, and relays live 8 kHz mono PCMU
audio from one publisher to any number of listeners on
`rtsp://<host>:<port>/live/<name>`. Built for device interop testing.

## Layout

- `main.go` — CLI entry (`-port`, `-file`), loads media, starts server.
- `rtsp/request.go` — request parser: CRLF lines, `Content-Length` bodies,
  CSeq extraction, half-packet and pipelined-request handling, `$` binary
  frame parsing (publisher RTP / client RTCP), and size limits (request
  line 4 KiB, headers 64 KiB, body 1 MiB, interleaved frame 64 KiB).
- `rtsp/server.go` — connection loop and method dispatch (OPTIONS /
  DESCRIBE / SETUP / PLAY / PAUSE / TEARDOWN / ANNOUNCE / RECORD), path
  routing for `/demo` and `/live/<name>`, transport parsing. All writes
  (responses and `$` frames) are serialized through one mutex with a 5 s
  write deadline.
- `rtsp/session.go` — per-connection exclusive session: random 64-bit hex
  Session id, random SSRC, running RTP sequence/timestamp counters, and
  the session kind (demo / listener / publisher).
- `rtsp/stream.go` — 20 ms ticker sender for the /demo file: 160 samples
  per RTP v2 packet, payload type 0, continuous sequence numbers,
  timestamp = sent samples.
- `rtsp/registry.go` — live source registry: name validation
  (`^[A-Za-z0-9_]{1,32}$`), one publisher per name, stale-cleanup-safe
  unregister (pointer compare) so an old session never removes a newer
  source with the same name.
- `rtsp/live.go` — live distribution: `liveSource` fans raw 160-byte PCMU
  payloads out to per-listener queues (capacity 64). A full queue drops
  only that slow listener; the publisher and other listeners never block.
  `liveWriter` packetizes per listener with its own SSRC, continuous
  sequence numbers and a timestamp advancing by 160 per packet.
- `rtsp/publish.go` — publisher side: ANNOUNCE SDP validation (exactly
  one `m=audio ... RTP/AVP 0` track, `a=rtpmap:0 PCMU/8000[/1]`,
  `a=control:trackID=0`), SETUP with `mode="record"`, RECORD, and the
  interleaved-frame pump that parses RTP v2 (CSRC, extension headers and
  padding handled by the Pion parser), accepts only 160-byte PCMU
  payloads, drops invalid frames and skips RTCP. Nothing is stored or
  transcoded.
- `rtsp/listen.go` — listener side: DESCRIBE / SETUP / PLAY for live
  sources (only after RECORD succeeded), Range rejection.
- `media/source.go` — the 8 kHz mono PCMU clip (64000 samples = 8.000 s).
- `client/main.go` — /demo interop client.
- `client/live/main.go` — live demo: one publisher + two listeners.
- `rtsp/request_test.go`, `rtsp/live_test.go` — parser, registry,
  SDP-validation and slow-subscriber unit tests.

## Behavior

### /demo (file playback)

- Resource `rtsp://<host>:<port>/demo`, single track `trackID=0`.
- SDP advertises `m=audio 0 RTP/AVP 0`, `a=rtpmap:0 PCMU/8000/1` and the
  clip duration via `a=range:npt=0-8.000`.
- Only `RTP/AVP/TCP;unicast;interleaved=<rtp>-<rtcp>` with two distinct
  client-chosen channels is accepted (`461` otherwise).
- `PLAY` without `Range` resumes at the paused position.
  `Range: npt=x-` seeks (aligned down to 20 ms, `457` when invalid).
  SSRC, sequence and timestamp keep running across seeks. The first RTP
  packet is sent strictly after the PLAY response.
- `PAUSE` replies first, then stops packets; resume is gapless. End of
  file acts like PAUSE; playing again restarts from 0.

### /live/<name> (publish)

- `name` must match `^[A-Za-z0-9_]{1,32}$`; one publisher holds a name
  at a time (`409` when taken).
- `ANNOUNCE` with an `application/sdp` body describing exactly one
  `m=audio ... RTP/AVP 0` track with `a=rtpmap:0 PCMU/8000/1` and
  `a=control:trackID=0` (`400` otherwise).
- `SETUP .../live/<name>/trackID=0` with
  `Transport: RTP/AVP/TCP;unicast;interleaved=<a>-<b>;mode="record"`
  (two distinct channels, `461` otherwise).
- `RECORD` starts distribution; listeners may attach only after RECORD
  succeeded. The publisher then sends RTP as interleaved `$` frames on
  the negotiated RTP channel. Frames are parsed as RTP v2 (CSRC,
  extension headers and padding handled); only packets with a 160-byte
  PCMU payload are forwarded, anything else is dropped. RTCP frames are
  skipped, and interleaved RTSP control requests keep working.
- Publisher `TEARDOWN` or disconnect removes the source and closes all
  its listener connections; the name can be published again immediately.
  Cleanup of an old session never removes a newer same-named source.

### /live/<name> (listen)

- `DESCRIBE` returns SDP with an open-ended `a=range:npt=0-`.
- `SETUP` accepts any free pair of interleaved channels per listener.
- `PLAY` rejects `Range` (`457`, live sources are not seekable); the
  200 response precedes the first RTP packet. Every listener gets its
  own SSRC, continuous sequence numbers and timestamps advancing by 160
  per packet; payloads are forwarded unchanged.
- `PAUSE` stops delivery and drops all queued packets, so a resumed
  `PLAY` only receives audio that arrives afterwards.
- Each listener has a 64-packet queue. A full queue or a write timeout
  (5 s) disconnects only that slow listener — the publisher and the
  other listeners are never blocked.

### Sessions

- `SETUP` creates a random per-connection Session. Control methods
  (`PLAY`, `PAUSE`, `TEARDOWN`, `RECORD`) require a matching
  `Session` header — a missing or wrong id gets `454`; methods invalid
  in the current state get `455` (including PLAY while playing).

## Build and run

    go build ./...
    go test ./...
    go build -o bin/rtspserver .
    ./bin/rtspserver -port 8554          # default -file assets/demo.ulaw

## Demo clients

    go build -o bin/rtspdemo ./client
    ./bin/rtspdemo 127.0.0.1:8554        # /demo playback interop check

    go build -o bin/rtsplive ./client/live
    ./bin/rtsplive 127.0.0.1:8554 mic    # live publish + 2 listeners

The /demo client exercises pipelined OPTIONS+DESCRIBE, a SETUP split
across two writes, PLAY, PLAY-while-playing rejection (455), PAUSE
silence, gapless resume, seek alignment, invalid range (457), wrong
session (454), client-to-server RTCP `$` frame skipping, and TEARDOWN.

The live client publishes `assets/demo.ulaw` in a loop via
ANNOUNCE/SETUP(mode=record)/RECORD plus interleaved RTP, then attaches
two listeners on their own channel pairs. It verifies per-listener SSRC,
sequence continuity and 160-step timestamps, Range rejection (457),
PAUSE silence, resume-on-new-audio, and finally publisher TEARDOWN.

