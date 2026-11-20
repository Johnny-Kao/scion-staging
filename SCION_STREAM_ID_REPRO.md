# SCION Gateway — Stream ID reuse integration reproducer

Public, minimal reproduction on the upstream source without production-code changes.

**Upstream baseline:** `scionproto/scion@c7fe6e4dd1b14848892b2e916eb6ac387945bb08` (2026-11-20).  
**Status:** reproducer only; this branch is not a proposed fix, PR, or issue.

## Reproduce

Use Go 1.26.4 or later on a checkout of this branch:

```sh
go test ./gateway/dataplane -run '^TestResearchIntegratedRealEncodersPathChange$' -count=5 -v
go test -race ./gateway/dataplane -run '^TestResearchIntegratedRealEncodersPathChange$' -count=3
go vet ./gateway/dataplane
```

The test deliberately **passes when it reproduces a fault**; PASS does not mean gateway behavior is correct.

## What the test demonstrates

1. Unmodified `Session.SetPaths()` replaces senders through normal calls, alternating valid test path MTUs. `NewStreamID()` allocates IDs normally, without clock mocking or forced values.
2. The sender/encoder produces real SIG frames from checksum-valid IPv4/UDP packets and the mock `PacketConn` captures those exact bytes.
3. The in-memory network drops the old sender's final packet fragment and delivers its first fragment followed by a new sender's complete SIG frames after a naturally observed duplicate ID.
4. The unmodified receiver writes a reassembly result mixing old and new datagram bytes instead of the intact new packet.
5. A separate receiver with **distinct** old/new Stream IDs correctly delivers the intact new packet.

The problem is associated with `gateway/dataplane/encoder.go:NewStreamID()`, which uses the low **16 bits** of a timestamp although SIG carries a **20-bit** Stream ID. Related prior discussion: [scionproto/scion#2950](https://github.com/scionproto/scion/issues/2950).

## Evidence boundary

- Repeated path changes in this test are deliberately **high-frequency stress**, not representative field telemetry.
- Network transport and loss are emulated in memory. This is an integration reproduction using real sender and receiver code, **not** a physical SCION deployment or confirmed customer incident.
- The malformed packet is observed at the gateway TUN-write boundary; downstream host acceptance and security implications were not assessed.
- The test does not force a colliding ID; it observes automatic reuse and selects two captured real sender streams with the same ID.
- No fix, generator patch, private research notes, or unrelated optimizations are included.

## Published code

Only `gateway/dataplane/research_stream_id_repro_test.go` is added relative to upstream, plus this branch-specific reproduction README. The existing upstream test helpers supply a normal mock path and mock network connection. There are **no production source edits**.
