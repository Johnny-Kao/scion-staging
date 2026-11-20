// Research-only integration reproducer: upstream Session.Write -> sender encoder ->
// captured PacketConn wire frames -> actual receiver worker. No injected SIG headers.
package dataplane

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/layers"
	"github.com/scionproto/scion/pkg/private/mocks/net/mock_net"
	"github.com/scionproto/scion/pkg/snet"
	"github.com/scionproto/scion/private/ringbuf"
)

// publicReproTun captures packets written at the gateway TUN boundary.
type publicReproTun struct{ packets [][]byte }

func (t *publicReproTun) Read(p []byte) (int, error) { return 0, nil }
func (t *publicReproTun) Write(p []byte) (int, error) {
	t.packets = append(t.packets, bytes.Clone(p))
	return len(p), nil
}
func (t *publicReproTun) Close() error { return nil }

func researchCaptureFrames(t *testing.T, ch <-chan []byte, id uint32, count int) [][]byte {
	t.Helper()
	frames := make([][]byte, 0, count)
	for len(frames) < count {
		select {
		case frame := <-ch:
			got := binary.BigEndian.Uint32(frame[4:8]) & 0xfffff
			seq := binary.BigEndian.Uint64(frame[8:16])
			if got != id || seq != uint64(len(frames)) {
				t.Fatalf("unexpected actual frame: id=%d want %d seq=%d want %d", got, id, seq, len(frames))
			}
			frames = append(frames, frame)
		case <-time.After(2 * time.Second):
			t.Fatalf("sender did not emit %d real frames for ID %d (got %d)", count, id, len(frames))
		}
	}
	return frames
}

func researchDeliverWireFrame(w *worker, wire []byte) {
	frames := make(ringbuf.EntryList, 1)
	if n := newFrameBufs(frames); n != 1 {
		panic("pool empty")
	}
	f := frames[0].(*frameBuf)
	copy(f.raw, wire)
	f.frameLen = len(wire)
	w.processFrame(context.Background(), f)
}

func TestResearchIntegratedRealEncodersPathChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	ch := make(chan []byte, 64)
	conn := mock_net.NewMockPacketConn(ctrl)
	conn.EXPECT().LocalAddr().Return(&snet.UDPAddr{Host: &net.UDPAddr{IP: net.IP{192, 168, 1, 1}}}).AnyTimes()
	conn.EXPECT().WriteTo(gomock.Any(), gomock.Any()).DoAndReturn(func(frame []byte, _ any) (int, error) {
		ch <- bytes.Clone(frame)
		return len(frame), nil
	}).AnyTimes()
	sess := &Session{SessionID: 1, DataPlaneConn: conn}
	defer sess.Close()
	makePacket := func(n int, marker byte) gopacket.Packet {
		ip := &layers.IPv4{
			Version: 4, IHL: 5, TTL: 64, Protocol: layers.IPProtocolUDP,
			SrcIP: net.IP{10, 0, 0, 1}, DstIP: net.IP{10, 0, 0, 2},
		}
		udp := &layers.UDP{SrcPort: 2048, DstPort: 2049}
		if err := udp.SetNetworkLayerForChecksum(ip); err != nil {
			t.Fatal(err)
		}
		buf := gopacket.NewSerializeBuffer()
		if err := gopacket.SerializeLayers(buf, gopacket.SerializeOptions{FixLengths: true, ComputeChecksums: true},
			ip, udp, gopacket.Payload(bytes.Repeat([]byte{marker}, n-28))); err != nil {
			t.Fatal(err)
		}
		if len(buf.Bytes()) != n {
			t.Fatalf("serialized UDP packet size=%d, want %d", len(buf.Bytes()), n)
		}
		return gopacket.NewPacket(buf.Bytes(), layers.LayerTypeIPv4, gopacket.DecodeOptions{Lazy: true, NoCopy: true})
	}
	type oldStream struct {
		frame []byte
		step  int
	}
	seen := make(map[uint32]oldStream)
	var oldWire []byte
	var newWire [][]byte
	var repeatedID uint32
	var initialStep, duplicateStep int
	var found bool
	const maximum = 4096
	for step := 0; step < maximum; step++ {
		mtu := uint16(200)
		if step%2 == 1 {
			mtu = 202
		}
		if err := sess.SetPaths([]snet.Path{createMockPath(ctrl, mtu)}); err != nil {
			t.Fatal(err)
		}
		id := sess.senders[0].encoder.streamID // produced by actual upstream NewStreamID
		if previously, exists := seen[id]; exists {
			repeatedID = id
			oldWire = previously.frame
			initialStep, duplicateStep = previously.step, step
			sess.Write(makePacket(320, 0xb2))
			// 320 bytes over 200/202 MTU always needs three real SIG frames.
			newWire = researchCaptureFrames(t, ch, id, 3)
			found = true
			break
		}
		sess.Write(makePacket(160, 0xa1))
		// One real fragmented IPv4 packet, split across two sender frames.
		captured := researchCaptureFrames(t, ch, id, 2)
		seen[id] = oldStream{frame: captured[0], step: step}
		// Network loss: the second fragment of this old sender is intentionally not forwarded.
	}
	if !found {
		t.Fatalf("no repeated naturally generated ID in %d path transitions", maximum)
	}
	if len(oldWire) < 40 || len(newWire) < 2 {
		t.Fatal("invalid actual encoded frames")
	}
	oldLen := int(binary.BigEndian.Uint16(oldWire[18:20]))
	if oldLen != 160 {
		t.Fatalf("old IP length %d", oldLen)
	}
	oldFirstPayload := len(oldWire) - 16
	if oldFirstPayload >= oldLen {
		t.Fatalf("old packet unexpectedly not fragmented; payload=%d", oldFirstPayload)
	}
	tun := &publicReproTun{}
	receiver := &worker{rlists: make(map[int]*reassemblyList), tunIO: tun}
	researchDeliverWireFrame(receiver, oldWire)
	if len(tun.packets) != 0 {
		t.Fatal("incomplete original packet unexpectedly delivered")
	}
	for _, frame := range newWire {
		researchDeliverWireFrame(receiver, frame)
	}
	if len(tun.packets) == 0 {
		t.Fatalf("no receiver packets produced: ID %d, old fragment bytes=%d", repeatedID, oldFirstPayload)
	}
	bad := false
	for _, packet := range tun.packets {
		if !bytes.Equal(packet, makePacket(320, 0xb2).Data()) && !bytes.Equal(packet, makePacket(160, 0xa1).Data()) {
			bad = true
		}
	}
	if !bad {
		t.Fatalf("no corrupted packet despite collision; %d packets", len(tun.packets))
	}
	t.Logf("REAL ENCODER WIRES: repeated ID %d at path updates %d -> %d; first old fragment payload %d/%d bytes, new real frames=%d, receiver delivered %d packets including mixed corrupted packet", repeatedID, initialStep, duplicateStep, oldFirstPayload, oldLen, len(newWire), len(tun.packets))
	// Control: old incomplete stream under a distinct ID must not contaminate newly
	// encoded frames, and receiver must deliver the complete new IPv4 packet.
	controlTun := &publicReproTun{}
	controlWorker := &worker{rlists: make(map[int]*reassemblyList), tunIO: controlTun}
	controlOld := bytes.Clone(oldWire)
	alternative := (repeatedID + 1) & 0xfffff
	binary.BigEndian.PutUint32(controlOld[4:8], alternative)
	researchDeliverWireFrame(controlWorker, controlOld)
	for _, frame := range newWire {
		researchDeliverWireFrame(controlWorker, frame)
	}
	expected := makePacket(320, 0xb2).Data()
	if len(controlTun.packets) != 1 || !bytes.Equal(controlTun.packets[0], expected) {
		t.Fatalf("different-ID isolation control failed: %d outputs", len(controlTun.packets))
	}
}
