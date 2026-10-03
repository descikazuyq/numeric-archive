package numeric

import (
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// setClock 是可随意拨动（含回拨）的测试时钟，用于模拟本机时钟异常。
type setClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *setClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *setClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// blockingCompute 占用计算位置，直到 Close 置位取消标记后返回失败；
// started 在首次进入计算时关闭，用于确认作业已在运行。
func blockingCompute(started chan<- struct{}) func(uint64, []int64, int64, func() bool) (int64, int64, string, bool) {
	var once sync.Once
	return func(_ uint64, _ []int64, _ int64, canceled func() bool) (int64, int64, string, bool) {
		once.Do(func() { close(started) })
		for !canceled() {
			time.Sleep(time.Millisecond)
		}
		return 0, 0, "aborted", false
	}
}

// 时钟回拨后重新打开归档：调度顺序、按提交人列举的顺序都必须仍是
// 接受先后（作业号顺序），记录上的提交时间只用于范围过滤。
func TestReopenPreservesAcceptanceOrder(t *testing.T) {
	dir := t.TempDir()
	clock := &setClock{t: time.Date(2026, 10, 3, 9, 0, 0, 0, time.UTC)}
	s, err := Open(dir, WithClock(clock))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	started := make(chan struct{})
	s.compute = blockingCompute(started)

	// 占用本地唯一的计算位置，让后续作业留在队列里。
	if _, err := s.Submit(SubmitRequest{Submitter: "bob", Values: []int64{1}}); err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	<-started

	t1005 := time.Date(2026, 10, 3, 10, 5, 0, 0, time.UTC)
	t1000 := time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)

	clock.set(t1005)
	ja, err := s.Submit(SubmitRequest{Submitter: "alice", Values: []int64{2}})
	if err != nil {
		t.Fatalf("submit A: %v", err)
	}
	// 时钟回拨：后接受的乙反而带着更早的提交时间。
	clock.set(t1000)
	jb, err := s.Submit(SubmitRequest{Submitter: "alice", Values: []int64{3}})
	if err != nil {
		t.Fatalf("submit B: %v", err)
	}
	// 提交时间完全相同的丙，接受顺序同样必须保留。
	jc, err := s.Submit(SubmitRequest{Submitter: "alice", Values: []int64{4}})
	if err != nil {
		t.Fatalf("submit C: %v", err)
	}
	if jb.QueuedAt != jc.QueuedAt {
		t.Fatalf("测试前提：乙丙提交时间应相同，got %v vs %v", jb.QueuedAt, jc.QueuedAt)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 重新打开：甲、乙、丙都仍在排队，由新 worker 继续处理。
	s2, err := Open(dir, WithClock(newManualClock()))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	a2 := waitStatus(t, s2, ja.ID, StatusSucceeded)
	b2 := waitStatus(t, s2, jb.ID, StatusSucceeded)
	c2 := waitStatus(t, s2, jc.ID, StatusSucceeded)
	if !a2.StartedAt.Before(b2.StartedAt) || !b2.StartedAt.Before(c2.StartedAt) {
		t.Fatalf("重新打开后执行顺序应为甲→乙→丙，got 甲@%v 乙@%v 丙@%v",
			a2.StartedAt, b2.StartedAt, c2.StartedAt)
	}

	// 不限范围：按实际提交顺序返回。
	all, err := s2.List("alice", time.Time{}, time.Time{})
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 3 || all[0].ID != ja.ID || all[1].ID != jb.ID || all[2].ID != jc.ID {
		t.Fatalf("列举顺序应为甲、乙、丙，got %v", listIDs(all))
	}

	// 范围只覆盖 10:00：只能查到乙、丙（按接受顺序）。
	onlyBC, err := s2.List("alice", t1000, t1000)
	if err != nil {
		t.Fatalf("list 10:00: %v", err)
	}
	if len(onlyBC) != 2 || onlyBC[0].ID != jb.ID || onlyBC[1].ID != jc.ID {
		t.Fatalf("10:00 范围内应只有乙、丙，got %v", listIDs(onlyBC))
	}

	// 范围同时覆盖两个时间：先甲后乙丙，即使甲的记录时间更晚。
	both, err := s2.List("alice", t1000, t1005)
	if err != nil {
		t.Fatalf("list 10:00-10:05: %v", err)
	}
	if len(both) != 3 || both[0].ID != ja.ID || both[1].ID != jb.ID || both[2].ID != jc.ID {
		t.Fatalf("范围内列举应先甲后乙丙，got %v", listIDs(both))
	}

	// 恢复不得改写提交时间，也不得重新分配作业号。
	if !a2.QueuedAt.Equal(t1005) || !b2.QueuedAt.Equal(t1000) || !c2.QueuedAt.Equal(t1000) {
		t.Fatalf("恢复后提交时间被改写：甲@%v 乙@%v 丙@%v", a2.QueuedAt, b2.QueuedAt, c2.QueuedAt)
	}
	if a2.ID != ja.ID || b2.ID != jb.ID || c2.ID != jc.ID {
		t.Fatalf("恢复后作业号发生变化：%d/%d/%d", a2.ID, b2.ID, c2.ID)
	}
}

// 上次关闭时仍在运行的上游在恢复时变为失败：等待它的作业及更下游
// 必须在本次打开完成时级联失败，即使下游记录的提交时间更早。
func TestReopenCascadeWithRolledBackClock(t *testing.T) {
	dir := t.TempDir()
	clock := &setClock{t: time.Date(2026, 10, 3, 10, 5, 0, 0, time.UTC)}
	s, err := Open(dir, WithClock(clock))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	started := make(chan struct{})
	s.compute = blockingCompute(started)

	up, err := s.Submit(SubmitRequest{Submitter: "alice", Values: []int64{1}})
	if err != nil {
		t.Fatalf("submit up: %v", err)
	}
	<-started // up 正在运行，磁盘记录为 running

	// 时钟回拨：下游记录的提交时间早于上游。
	clock.set(time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC))
	mid, err := s.Submit(SubmitRequest{Submitter: "alice", Values: []int64{2}, Dependencies: []uint64{up.ID}})
	if err != nil {
		t.Fatalf("submit mid: %v", err)
	}
	clock.set(time.Date(2026, 10, 3, 9, 55, 0, 0, time.UTC))
	down, err := s.Submit(SubmitRequest{Submitter: "alice", Values: []int64{3}, Dependencies: []uint64{mid.ID}})
	if err != nil {
		t.Fatalf("submit down: %v", err)
	}

	// 模拟进程中断（不 Close）：磁盘上 up 仍是 running，mid/down 仍在排队。
	// 直接重新打开同一目录，恢复应把 up 标记为失败并沿链条级联。
	s2, err := Open(dir, WithClock(newManualClock()))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()
	defer func() { _ = s.Close() }()

	up2 := waitStatus(t, s2, up.ID, StatusFailed)
	mid2 := waitStatus(t, s2, mid.ID, StatusFailed)
	down2 := waitStatus(t, s2, down.ID, StatusFailed)

	if !strings.Contains(up2.FailureReason, "计算被中断") {
		t.Fatalf("上游失败原因应说明计算被中断，got %q", up2.FailureReason)
	}
	// 直接阻断者与根因作业号必须沿链条保留。
	if mid2.BlockerID != up.ID {
		t.Fatalf("mid 的根因作业号应为 %d，got %d", up.ID, mid2.BlockerID)
	}
	if !strings.Contains(mid2.FailureReason, "直接上游作业 "+itoa(up.ID)) {
		t.Fatalf("mid 的失败详情应指出直接阻断者 %d，got %q", up.ID, mid2.FailureReason)
	}
	if down2.BlockerID != up.ID {
		t.Fatalf("down 的根因作业号应为 %d，got %d", up.ID, down2.BlockerID)
	}
	if !strings.Contains(down2.FailureReason, "直接上游作业 "+itoa(mid.ID)) {
		t.Fatalf("down 的失败详情应指出直接阻断者 %d，got %q", mid.ID, down2.FailureReason)
	}
	if !strings.Contains(down2.FailureReason, "根因为作业 "+itoa(up.ID)) {
		t.Fatalf("down 的失败详情应保留根因作业号 %d，got %q", up.ID, down2.FailureReason)
	}

	// 级联后的列举仍按接受顺序。
	all, err := s2.List("alice", time.Time{}, time.Time{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 3 || all[0].ID != up.ID || all[1].ID != mid.ID || all[2].ID != down.ID {
		t.Fatalf("列举顺序应为上游→中游→下游，got %v", listIDs(all))
	}
}

func listIDs(jobs []*Job) []uint64 {
	ids := make([]uint64, 0, len(jobs))
	for _, j := range jobs {
		ids = append(ids, j.ID)
	}
	return ids
}

func itoa(id uint64) string {
	return strconv.FormatUint(id, 10)
}
