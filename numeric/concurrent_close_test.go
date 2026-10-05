package numeric

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestConcurrentCloseAllWaitForShutdown 覆盖同一份归档被多处同时关闭的情形。
//
// 旧实现里，第一次 Close 把 closed 置位后，后发的 Close 仅凭 closed==true 就
// 立即成功返回——此刻 worker 尚未退出，运行中的作业仍在补写中断结果。本测试
// 让计算在观察到取消后停在一个由测试控制的“收尾”阶段：若任何一次 Close 在
// 该阶段结束前返回，就说明它过早完成。所有 Close 都必须等到 worker 退出、
// 运行作业已按“计算被中断”失败落盘后才返回，且每次返回都是可安全重开目录
// 的时刻。
func TestConcurrentCloseAllWaitForShutdown(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	started := make(chan struct{})
	finishing := make(chan struct{})
	release := make(chan struct{})
	var finishOnce sync.Once
	s.compute = func(_ uint64, in []int64, seed int64, canceled func() bool) (int64, int64, string, bool) {
		close(started)
		for !canceled() {
			time.Sleep(time.Millisecond)
		}
		// 已观察到关闭造成的取消，但在测试放行前不返回：模拟计算退出与
		// worker 裁决落盘之间的窗口。旧对象此刻仍占着计算位置。
		finishOnce.Do(func() { close(finishing) })
		<-release
		return computeResult(in, seed, canceled)
	}

	j1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1, 2}})
	// 排队作业：重复关闭不能启动、失败或取消它。
	j2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "queued", Values: []int64{10, 20}})
	<-started

	const n = 8
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	returned := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Close(); err != nil {
				errCh <- err
				return
			}
			returned <- struct{}{}
		}()
	}

	// 等到运行中的计算已观察到取消、但尚未退出。
	<-finishing

	// 留出足够时间让全部并发 Close 都进入关闭路径；旧实现下后发调用会在
	// 这一窗口内立即返回。
	select {
	case <-returned:
		t.Fatal("某次 Close 在运行中作业的中断处理结束前就提前返回")
	case <-time.After(200 * time.Millisecond):
	}

	// 此刻旧对象仍未完成关闭：作业仍是运行态，任何调用方都还不应据此重开。
	if g, err := s.Get(j1.ID); err != nil || g.Status != StatusRunning {
		status := "<nil>"
		if err == nil {
			status = string(g.Status)
		}
		t.Fatalf("关闭收尾期间作业状态=%s err=%v，want running", status, err)
	}

	// 放行计算收尾：worker 裁决为“计算被中断”并落盘、退出，所有 Close 随后返回。
	close(release)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("并发 Close 互相等待，未能全部结束")
	}
	close(errCh)
	for e := range errCh {
		t.Errorf("concurrent close: %v", e)
	}
	if len(returned) != n {
		t.Fatalf("应有 %d 次 Close 成功返回，got %d", n, len(returned))
	}

	// 任意一次 Close 返回后都可安全重开：运行作业为中断失败、原因可查、
	// 无成功归档与实际计算输入；排队作业原样保留并继续按序处理。
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	g1, err := s2.Get(j1.ID)
	if err != nil {
		t.Fatalf("get j1: %v", err)
	}
	if g1.Status != StatusFailed || !strings.Contains(g1.FailureReason, "计算被中断") {
		t.Fatalf("j1 status=%s reason=%q，want 计算被中断失败", g1.Status, g1.FailureReason)
	}
	if g1.Archive != nil || len(g1.EffectiveValues) != 0 {
		t.Fatalf("中断失败不应留下成功归档或实际输入，got archive=%v effective=%v",
			g1.Archive, g1.EffectiveValues)
	}

	g2 := waitStatus(t, s2, j2.ID, StatusSucceeded)
	if g2.Archive == nil || g2.Archive.Sum != 30 {
		t.Fatalf("排队作业重开后应正常成功（总和 30），got status=%s", g2.Status)
	}
}

// TestConcurrentCloseIdleStore 没有运行中作业的空闲归档被并发关闭：每次调用
// 都正常返回、互不卡死；完全关闭后的再次 Close 立即成功。
func TestConcurrentCloseIdleStore(t *testing.T) {
	s, _ := openTestStore(t)

	const n = 8
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Close(); err != nil {
				t.Errorf("idle close: %v", err)
			}
		}()
	}
	wg.Wait()

	if err := s.Close(); err != nil {
		t.Fatalf("close after fully closed: %v", err)
	}
	if _, err := s.Submit(SubmitRequest{Submitter: "a", Values: []int64{1}}); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("submit after close: %v", err)
	}
}
