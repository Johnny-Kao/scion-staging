package dataplane

import (
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/scionproto/scion/pkg/snet"
)

func TestSessionCloseTerminal(t *testing.T) {
	ctrl := gomock.NewController(t)
	frames := make(chan []byte, 8)
	session := createSession(t, ctrl, frames)
	path0 := createMockPath(ctrl, 200)
	path1 := createMockPath(ctrl, 202)
	require.NoError(t, session.SetPaths([]snet.Path{path0}))
	session.Close()
	session.Close() // repeated close is safe
	require.Error(t, session.SetPaths([]snet.Path{path1}))
	sendPackets(t, session, 22, 1)
	select {
	case <-frames:
		t.Fatal("session sent a packet submitted after Close")
	case <-time.After(40 * time.Millisecond):
	}
}

func TestSessionCloseConcurrentSetPaths(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := createSession(t, ctrl, make(chan []byte, 8))
	path0 := createMockPath(ctrl, 200)
	path1 := createMockPath(ctrl, 202)
	require.NoError(t, session.SetPaths([]snet.Path{path0}))
	ready := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(ready)
		for i := 0; i < 1000; i++ {
			if err := session.SetPaths([]snet.Path{path1}); err != nil {
				return
			}
			if err := session.SetPaths([]snet.Path{path0}); err != nil {
				return
			}
		}
	}()
	<-ready
	session.Close()
	wg.Wait()
	require.Error(t, session.SetPaths([]snet.Path{path0}))
}
