package numeric

import (
	"sync"
	"testing"
	"time"
)

// 验证并发 Close：第二个 Close 必须等到计算真正停止、关闭处理落盘后才返回。
func TestConcurrentCloseWaitsForShutdown(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	computeStarted := make(chan struct{})
	releaseCompute := make(chan struct{})
	s.compute = func(id uint64, inputs []int64, seed int64, canceled func() bool) (int64, int64, string, bool) {
		close(computeStarted)
		<-releaseCompute
		return 1, 1, "", true
	}
	if _, err := s.Submit(SubmitRequest{Submitter: "a", Values: []int64{1}}); err != nil {
		t.Fatal(err)
	}
	<-computeStarted

	firstDone := make(chan struct{})
	go func() {
		if err := s.Close(); err != nil {
			t.Errorf("first Close: %v", err)
		}
		close(firstDone)
	}()
	// 等第一次 Close 已开始关闭（closed 置位）但计算尚未退出。
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.mu.Lock()
		c := s.closed
		s.mu.Unlock()
		if c {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first Close did not start")
		}
		time.Sleep(time.Millisecond)
	}

	secondDone := make(chan struct{})
	go func() {
		if err := s.Close(); err != nil {
			t.Errorf("second Close: %v", err)
		}
		close(secondDone)
	}()

	select {
	case <-secondDone:
		t.Fatal("second Close returned before computation stopped")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseCompute)
	<-firstDone
	select {
	case <-secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("second Close did not return after shutdown completed")
	}

	// 完全关闭后再次 Close 应立即返回。
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("post-shutdown Close blocked")
	}

	j, err := s.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != StatusFailed {
		t.Fatalf("interrupted job status = %s, want %s", j.Status, StatusFailed)
	}

	// 重新打开不受旧对象影响。
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	j2, err := s2.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if j2.Status != StatusFailed {
		t.Fatalf("reopened job status = %s, want %s", j2.Status, StatusFailed)
	}
}

// 空闲归档的并发 Close 不应互相等待。
func TestConcurrentCloseIdle(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	wg.Wait()
}
