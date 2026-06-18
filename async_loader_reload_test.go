package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestLoader builds a loader with no real API/cache dependencies. Callers
// override loadCompaniesFn/loadUsersFn to observe load runs.
func newTestLoader(t *testing.T) *AsyncInitialLoader {
	t.Helper()
	return NewAsyncInitialLoader(nil, nil, nil, nil, nil, NewSchematicLogger(), time.Minute, DefaultAsyncLoaderConfig())
}

func waitIdle(t *testing.T, al *AsyncInitialLoader) {
	t.Helper()
	require.Eventually(t, func() bool { return !al.loadingActive() }, time.Second, time.Millisecond, "load run did not finish")
}

// TestStartAsyncLoadingOnlyRunsOnce verifies the once-guard: OnConnectionReady
// calls StartAsyncLoading on every reconnect, but only the first call may
// trigger a bulk load — reconnects rely on replay instead.
func TestStartAsyncLoadingOnlyRunsOnce(t *testing.T) {
	ctx := context.Background()
	al := newTestLoader(t)

	var companyLoads, userLoads int32
	al.loadCompaniesFn = func(context.Context) error { atomic.AddInt32(&companyLoads, 1); return nil }
	al.loadUsersFn = func(context.Context) error { atomic.AddInt32(&userLoads, 1); return nil }

	al.StartAsyncLoading(ctx)
	waitIdle(t, al)
	al.StartAsyncLoading(ctx)
	al.StartAsyncLoading(ctx)
	waitIdle(t, al)

	assert.Equal(t, int32(1), atomic.LoadInt32(&companyLoads), "StartAsyncLoading must load only once")
	assert.Equal(t, int32(1), atomic.LoadInt32(&userLoads), "StartAsyncLoading must load only once")
}

// TestReloadForcesFreshLoad is the regression test for the dead reload path:
// Reload must trigger a real bulk load even after the initial load has run.
func TestReloadForcesFreshLoad(t *testing.T) {
	ctx := context.Background()
	al := newTestLoader(t)

	var companyLoads, userLoads int32
	al.loadCompaniesFn = func(context.Context) error { atomic.AddInt32(&companyLoads, 1); return nil }
	al.loadUsersFn = func(context.Context) error { atomic.AddInt32(&userLoads, 1); return nil }

	al.StartAsyncLoading(ctx)
	waitIdle(t, al)

	al.Reload(ctx)
	waitIdle(t, al)
	al.Reload(ctx)
	waitIdle(t, al)

	// Initial load + two reloads = three runs.
	assert.Equal(t, int32(3), atomic.LoadInt32(&companyLoads))
	assert.Equal(t, int32(3), atomic.LoadInt32(&userLoads))
}

// TestReloadSkippedWhileLoadInProgress verifies overlapping runs are guarded so
// two loads never write the same caches concurrently.
func TestReloadSkippedWhileLoadInProgress(t *testing.T) {
	ctx := context.Background()
	al := newTestLoader(t)

	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(1)
	var startedOnce sync.Once
	var companyLoads int32
	al.loadCompaniesFn = func(context.Context) error {
		atomic.AddInt32(&companyLoads, 1)
		startedOnce.Do(started.Done)
		<-release // block until the test lets the run finish
		return nil
	}
	al.loadUsersFn = func(context.Context) error { return nil }

	al.Reload(ctx)          // first run begins and blocks in loadCompaniesFn
	started.Wait()          // ensure the run is in progress
	al.Reload(ctx)          // second run must be skipped while the first is active
	assert.True(t, al.loadingActive())

	close(release)
	waitIdle(t, al)
	assert.Equal(t, int32(1), atomic.LoadInt32(&companyLoads), "overlapping reload must be skipped")
}
