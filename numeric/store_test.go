package numeric

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// manualClock 是确定性时钟：每次取值自动推进，保证提交时间严格可排序。
type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func newManualClock() *manualClock {
	return &manualClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(time.Microsecond)
	return c.t
}

func openTestStore(t *testing.T) (*Store, *manualClock) {
	t.Helper()
	clock := newManualClock()
	s, err := Open(t.TempDir(), WithClock(clock))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, clock
}

func waitStatus(t *testing.T, s *Store, id uint64, want Status) *Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		j, err := s.Get(id)
		if err != nil {
			t.Fatalf("get %d: %v", id, err)
		}
		if j.Status == want {
			return j
		}
		if isTerminal(j.Status) && want != j.Status {
			t.Fatalf("job %d reached terminal %s (reason=%q), want %s",
				id, j.Status, j.FailureReason, want)
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %d status=%s, want %s (reason=%q)", id, j.Status, want, j.FailureReason)
		}
		time.Sleep(time.Millisecond)
	}
}

func isTerminal(st Status) bool {
	return st == StatusSucceeded || st == StatusFailed || st == StatusCanceled
}

func mustSubmit(t *testing.T, s *Store, req SubmitRequest) *Job {
	t.Helper()
	j, err := s.Submit(req)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return j
}

// gateCompute 安装计算钩子：block(id) 让指定作业停在“计算中”，
// release(id) 放行；未 block 的作业直接完成。started 记录进入计算的作业号，
// order 记录实际完成计算的顺序，计算本身仍由真实 computeResult 完成。
func gateCompute(t *testing.T, s *Store) (
	started chan uint64,
	block func(ids ...uint64),
	release func(id uint64),
	order *[]uint64,
	omu *sync.Mutex,
) {
	t.Helper()
	started = make(chan uint64, 64)
	var bmu sync.Mutex
	gates := make(map[uint64]chan struct{})
	var ord []uint64
	omu = &sync.Mutex{}
	block = func(ids ...uint64) {
		bmu.Lock()
		defer bmu.Unlock()
		for _, id := range ids {
			gates[id] = make(chan struct{})
		}
	}
	release = func(id uint64) {
		bmu.Lock()
		ch, ok := gates[id]
		bmu.Unlock()
		if ok {
			close(ch)
		}
	}
	s.compute = func(id uint64, inputs []int64, seed int64, canceled func() bool) (int64, int64, string, bool) {
		started <- id
		bmu.Lock()
		ch := gates[id]
		bmu.Unlock()
		if ch != nil {
			select {
			case <-ch:
			case <-s.stopCh: // Close 时自动放行，真实计算随后观察到取消而中止
			}
		}
		omu.Lock()
		ord = append(ord, id)
		omu.Unlock()
		return computeResult(inputs, seed, canceled)
	}
	return started, block, release, &ord, omu
}

func waitStarted(t *testing.T, started <-chan uint64, want uint64) {
	t.Helper()
	select {
	case id := <-started:
		if id != want {
			t.Fatalf("expected job %d to start, got %d", want, id)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("job %d did not start", want)
	}
}

func TestSubmitRejectsEmptySequence(t *testing.T) {
	s, _ := openTestStore(t)
	_, err := s.Submit(SubmitRequest{Submitter: "a", RequestID: "r1", Values: nil})
	if !errors.Is(err, ErrEmptySequence) {
		t.Fatalf("err=%v, want ErrEmptySequence", err)
	}
	_, err = s.Submit(SubmitRequest{Submitter: "a", RequestID: "r1", Values: []int64{}})
	if !errors.Is(err, ErrEmptySequence) {
		t.Fatalf("err=%v, want ErrEmptySequence", err)
	}
	// 不产生任何记录。
	if got, _ := s.List("a", time.Time{}, time.Time{}); len(got) != 0 {
		t.Fatalf("empty submit created records: %d", len(got))
	}
}

func TestSubmitDependencyMustExist(t *testing.T) {
	s, _ := openTestStore(t)
	_, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "r1", Values: []int64{1},
		HasDependency: true, DependencyID: 999,
	})
	if !errors.Is(err, ErrDependencyNotFound) {
		t.Fatalf("err=%v, want ErrDependencyNotFound", err)
	}
	if _, err := s.Get(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected dependency submit left a record: %v", err)
	}
}

func TestBasicSuccessAndArchive(t *testing.T) {
	s, _ := openTestStore(t)
	j := mustSubmit(t, s, SubmitRequest{
		Submitter: "alice", RequestID: "req-1", Values: []int64{1, 2, 3, -4}, Seed: 7,
	})
	if j.ID != 1 || j.Status != StatusQueued {
		t.Fatalf("initial view: %+v", j)
	}
	done := waitStatus(t, s, 1, StatusSucceeded)
	a := done.Archive
	if a == nil {
		t.Fatal("succeeded job has no archive")
	}
	if a.Sum != 2 || a.SumOfSquares != 30 {
		t.Fatalf("results sum=%d sq=%d, want 2,30", a.Sum, a.SumOfSquares)
	}
	wantEffective := []int64{1, 2, 3, -4}
	if len(a.EffectiveValues) != 4 || a.EffectiveValues[3] != -4 {
		t.Fatalf("effective=%v", a.EffectiveValues)
	}
	if a.InputsDigest != inputsDigestHex(wantEffective, 7) ||
		a.ResultDigest != resultDigestHex(wantEffective, 7, 2, 30) ||
		a.Checksum != checksumHex(wantEffective, 7, 2, 30, a.Log, a.ResultDigest) {
		t.Fatal("archive digests/checksum mismatch")
	}
	if a.Submitter != "alice" || a.RequestID != "req-1" || a.Seed != 7 {
		t.Fatal("original params not archived")
	}
	if a.CompletedAt.IsZero() {
		t.Fatal("CompletedAt missing")
	}
}

func TestDigestIndependentOfIDAndTime(t *testing.T) {
	s1, c1 := openTestStore(t)
	s2, _ := openTestStore(t)
	_ = c1
	mustSubmit(t, s1, SubmitRequest{Submitter: "a", Values: []int64{5, -2}, Seed: 3})
	// 在另一个目录、不同时间、不同作业号下提交完全相同的有效输入与种子。
	time.Sleep(5 * time.Millisecond)
	mustSubmit(t, s2, SubmitRequest{Submitter: "b", Values: []int64{5, -2}, Seed: 3})
	a := waitStatus(t, s1, 1, StatusSucceeded).Archive
	b := waitStatus(t, s2, 1, StatusSucceeded).Archive
	if a.ResultDigest != b.ResultDigest || a.Checksum != b.Checksum || a.InputsDigest != b.InputsDigest {
		t.Fatal("digests/checksum must be independent of submitter, job id and time")
	}
	if a.Log != b.Log {
		t.Fatal("log must be identical for identical effective inputs+seed")
	}
	if a.CompletedAt.Equal(b.CompletedAt) {
		// 极小概率；用不同目录时钟时几乎不可能相等。
		// CompletedAt 允许不同，不参与摘要。
	}
}

func TestIdempotentReplayReturnsSameJob(t *testing.T) {
	s, _ := openTestStore(t)
	req := SubmitRequest{Submitter: "a", RequestID: "dup", Values: []int64{1, 2}, Seed: 4}
	first := mustSubmit(t, s, req)
	waitStatus(t, s, first.ID, StatusSucceeded)
	again := mustSubmit(t, s, req)
	if again.ID != first.ID || again.Status != StatusSucceeded {
		t.Fatalf("replay returned id=%d status=%s, want %d succeeded", again.ID, again.Status, first.ID)
	}
	if got, _ := s.List("a", time.Time{}, time.Time{}); len(got) != 1 {
		t.Fatalf("replay created extra records: %d", len(got))
	}
}

func TestIdempotencyConflictKeepsOriginal(t *testing.T) {
	s, _ := openTestStore(t)
	orig := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "k", Values: []int64{1, 2}, Seed: 0})
	waitStatus(t, s, orig.ID, StatusSucceeded)

	variants := []SubmitRequest{
		{Submitter: "a", RequestID: "k", Values: []int64{2, 1}, Seed: 0}, // 次序变化
		{Submitter: "a", RequestID: "k", Values: []int64{1, 2}, Seed: 9}, // 种子变化
		{Submitter: "a", RequestID: "k", Values: []int64{1, 2, 3}, Seed: 0},
	}
	for i, v := range variants {
		j, err := s.Submit(v)
		if !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("variant %d: err=%v, want conflict", i, err)
		}
		if j == nil || j.ID != orig.ID {
			t.Fatalf("variant %d: conflict must also return original job", i)
		}
	}
	// 依赖变化也是冲突：先造一个可引用的作业 5。
	dep := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "dep", Values: []int64{1}})
	waitStatus(t, s, dep.ID, StatusSucceeded)
	_, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "k", Values: []int64{1, 2}, Seed: 0,
		HasDependency: true, DependencyID: dep.ID,
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("dependency change err=%v, want conflict", err)
	}
	// 原记录结果不变。
	got := waitStatus(t, s, orig.ID, StatusSucceeded)
	if got.Archive == nil || got.Archive.Sum != 3 {
		t.Fatal("original record altered after conflicts")
	}
}

func TestRequestIDScopedBySubmitter(t *testing.T) {
	s, _ := openTestStore(t)
	a := mustSubmit(t, s, SubmitRequest{Submitter: "alice", RequestID: "x", Values: []int64{1}})
	b := mustSubmit(t, s, SubmitRequest{Submitter: "bob", RequestID: "x", Values: []int64{2}})
	if a.ID == b.ID {
		t.Fatal("same request id across submitters must create separate jobs")
	}
	waitStatus(t, s, a.ID, StatusSucceeded)
	waitStatus(t, s, b.ID, StatusSucceeded)
}

func TestConcurrentDuplicateSubmit(t *testing.T) {
	s, _ := openTestStore(t)
	const n = 32
	var wg sync.WaitGroup
	ids := make([]uint64, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			j, err := s.Submit(SubmitRequest{
				Submitter: "a", RequestID: "race", Values: []int64{1, 2, 3},
			})
			errs[i] = err
			if j != nil {
				ids[i] = j.ID
			}
		}(i)
	}
	close(start)
	wg.Wait()
	uniq := map[uint64]struct{}{}
	for i := range ids {
		if errs[i] != nil {
			t.Fatalf("submit %d: %v", i, errs[i])
		}
		uniq[ids[i]] = struct{}{}
	}
	if len(uniq) != 1 {
		t.Fatalf("created %d jobs under one idempotency key: %v", len(uniq), ids)
	}
	waitStatus(t, s, ids[0], StatusSucceeded)
	if got, _ := s.List("a", time.Time{}, time.Time{}); len(got) != 1 {
		t.Fatalf("expected exactly 1 record, got %d", len(got))
	}
}

func TestFIFOExecutionOrder(t *testing.T) {
	s, _ := openTestStore(t)
	_, _, _, order, omu := gateCompute(t, s)
	jobs := []uint64{}
	for i := 0; i < 4; i++ {
		j := mustSubmit(t, s, SubmitRequest{
			Submitter: "a", RequestID: "", Values: []int64{int64(i + 1)},
		})
		jobs = append(jobs, j.ID)
	}
	for _, id := range jobs {
		waitStatus(t, s, id, StatusSucceeded)
	}
	omu.Lock()
	defer omu.Unlock()
	if len(*order) != 4 {
		t.Fatalf("order=%v", *order)
	}
	for i, id := range jobs {
		if (*order)[i] != id {
			t.Fatalf("execution order %v, want %v", *order, jobs)
		}
	}
}

func TestDependencyAppendsDependencySum(t *testing.T) {
	s, _ := openTestStore(t)
	base := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1, 2}, Seed: 0})
	waitStatus(t, s, base.ID, StatusSucceeded) // sum=3
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{10}, Seed: 0,
		HasDependency: true, DependencyID: base.ID,
	})
	d := waitStatus(t, s, down.ID, StatusSucceeded)
	if got := d.Archive.EffectiveValues; len(got) != 2 || got[0] != 10 || got[1] != 3 {
		t.Fatalf("effective=%v, want [10 3]", got)
	}
	if d.Archive.Sum != 13 || d.Archive.SumOfSquares != 109 {
		t.Fatalf("sum=%d sq=%d, want 13,109", d.Archive.Sum, d.Archive.SumOfSquares)
	}
	// 与“直接提交有效输入”的作业拥有相同结果摘要与校验值。
	direct := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{10, 3}, Seed: 0})
	dd := waitStatus(t, s, direct.ID, StatusSucceeded)
	if d.Archive.ResultDigest != dd.Archive.ResultDigest ||
		d.Archive.Checksum != dd.Archive.Checksum {
		t.Fatal("dependency-appended input must digest identically to the expanded input")
	}
}

func TestDependencyWaitDoesNotBlockRunnableJobs(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)

	first := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}}) // id 1 占住位置
	waitStarted(t, started, first.ID)

	waiting := mustSubmit(t, s, SubmitRequest{ // id 2 等 id 1 的结果
		Submitter: "a", Values: []int64{2},
		HasDependency: true, DependencyID: first.ID,
	})
	runnable := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{3}}) // id 3 无依赖

	// 唯一位置正被 id 1 占用：id 2 说明在等依赖结果，id 3 说明在等计算位置。
	if j, _ := s.Get(waiting.ID); j.WaitReason != WaitDependency {
		t.Fatalf("waiting job reason=%q, want WaitDependency", j.WaitReason)
	}
	if j, _ := s.Get(runnable.ID); j.WaitReason != WaitSlot || j.Status != StatusQueued {
		t.Fatalf("runnable job status=%s reason=%q, want queued/WaitSlot", j.Status, j.WaitReason)
	}

	// 上游取消：id 2 必须失败并指出阻断者，而 id 3 必须照常被排空，
	// 证明等待依赖的作业没有把队列堵死。
	if _, err := s.Cancel(first.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	release(first.ID)
	failed := waitStatus(t, s, waiting.ID, StatusFailed)
	if failed.BlockerID != first.ID {
		t.Fatalf("blocker=%d want %d", failed.BlockerID, first.ID)
	}
	done := waitStatus(t, s, runnable.ID, StatusSucceeded)
	if done.Archive.Sum != 3 {
		t.Fatalf("independent job result=%d, want 3", done.Archive.Sum)
	}
}

func TestDependencyChainFIFOWhenRunnable(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, order, omu := gateCompute(t, s)
	block(1)

	head := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}}) // id 1
	waitStarted(t, started, head.ID)
	chain1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{2}}) // id 2
	middle := mustSubmit(t, s, SubmitRequest{                                     // id 3 等 id 2
		Submitter: "a", Values: []int64{3},
		HasDependency: true, DependencyID: chain1.ID,
	})
	tail := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{4}}) // id 4

	// id 3 等 id 2 的依赖结果；id 4 等计算位置。
	if j, _ := s.Get(middle.ID); j.WaitReason != WaitDependency {
		t.Fatalf("middle reason=%q want WaitDependency", j.WaitReason)
	}
	if j, _ := s.Get(tail.ID); j.WaitReason != WaitSlot {
		t.Fatalf("tail reason=%q want WaitSlot", j.WaitReason)
	}
	release(head.ID) // 之后严格按“可运行作业的提交顺序”：2 → 3 → 4
	waitStatus(t, s, tail.ID, StatusSucceeded)
	m := waitStatus(t, s, middle.ID, StatusSucceeded)
	// id 3 的有效输入末尾追加 id 2 的总和 2。
	if got := m.Archive.EffectiveValues; len(got) != 2 || got[0] != 3 || got[1] != 2 {
		t.Fatalf("middle effective=%v, want [3 2]", got)
	}
	omu.Lock()
	defer omu.Unlock()
	want := []uint64{1, 2, 3, 4}
	if len(*order) != 4 {
		t.Fatalf("order=%v", *order)
	}
	for i := range want {
		if (*order)[i] != want[i] {
			t.Fatalf("execution order %v, want %v", *order, want)
		}
	}
}

func TestCanceledQueuedDependencyDoesNotStallQueue(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)

	head := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}}) // id 1 运行中
	waitStarted(t, started, head.ID)
	dep := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{2}}) // id 2 排队
	child := mustSubmit(t, s, SubmitRequest{                                   // id 3 等 id 2
		Submitter: "a", Values: []int64{3},
		HasDependency: true, DependencyID: dep.ID,
	})
	late := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{4}}) // id 4

	// 在 id 1 仍运行时取消排队中的 id 2：id 3 立即失败，不挡 id 4。
	if _, err := s.Cancel(dep.ID); err != nil {
		t.Fatalf("cancel queued dep: %v", err)
	}
	c := waitStatus(t, s, child.ID, StatusFailed)
	if c.BlockerID != dep.ID {
		t.Fatalf("child blocker=%d want %d", c.BlockerID, dep.ID)
	}
	release(head.ID)
	l := waitStatus(t, s, late.ID, StatusSucceeded)
	if l.Archive.Sum != 4 {
		t.Fatalf("late job sum=%d want 4", l.Archive.Sum)
	}
}

func TestDependencyAppendedSumCausesOverflowFailure(t *testing.T) {
	s, _ := openTestStore(t)
	// 依赖作业自身合法：两个 2.1e9 的平方和 8.82e18 < MaxInt64，
	// 但总和为 4.2e9（其平方 1.764e19 已越界）。
	v := int64(2100000000)
	base := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{v, v}})
	b := waitStatus(t, s, base.ID, StatusSucceeded)
	if b.Archive.Sum != 2*v {
		t.Fatalf("base sum=%d want %d", b.Archive.Sum, 2*v)
	}
	// 下游自身仅一个 0，但追加的依赖总和 4.2e9 会让平方和越界。
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{0},
		HasDependency: true, DependencyID: base.ID,
	})
	f := waitStatus(t, s, down.ID, StatusFailed)
	if !strings.Contains(f.FailureReason, "平方和") {
		t.Fatalf("reason=%q want 平方和溢出", f.FailureReason)
	}
	if f.Archive != nil {
		t.Fatal("overflow after dependency append must leave no success archive")
	}
}

func TestCancelNotFound(t *testing.T) {
	s, _ := openTestStore(t)
	if _, err := s.Cancel(777); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err=%v want ErrNotFound", err)
	}
}

func TestWaitSlotReason(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)
	first := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	waitStarted(t, started, first.ID)
	second := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{2}})
	if j, _ := s.Get(second.ID); j.WaitReason != WaitSlot {
		t.Fatalf("reason=%q, want WaitSlot", j.WaitReason)
	}
	release(first.ID)
	waitStatus(t, s, first.ID, StatusSucceeded)
	waitStatus(t, s, second.ID, StatusSucceeded)
}

func TestDependencyFailureCascades(t *testing.T) {
	s, _ := openTestStore(t)
	big := int64(3037000500)
	bad := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{big}})
	waitStatus(t, s, bad.ID, StatusFailed)

	mid := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{1},
		HasDependency: true, DependencyID: bad.ID,
	})
	far := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{2},
		HasDependency: true, DependencyID: mid.ID,
	})
	m := waitStatus(t, s, mid.ID, StatusFailed)
	f := waitStatus(t, s, far.ID, StatusFailed)
	if m.BlockerID != bad.ID || f.BlockerID != bad.ID {
		t.Fatalf("blockers mid=%d far=%d, want root %d", m.BlockerID, f.BlockerID, bad.ID)
	}
	if m.Archive != nil || f.Archive != nil {
		t.Fatal("blocked jobs must not carry success archives")
	}
	if !strings.Contains(f.FailureReason, "作业") || !strings.Contains(m.FailureReason, "阻断") {
		t.Fatalf("failure reasons must identify blocker: mid=%q far=%q", m.FailureReason, f.FailureReason)
	}
}

func TestCancelQueuedAndRunningAndCascade(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)

	first := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	waitStarted(t, started, first.ID)

	mid := mustSubmit(t, s, SubmitRequest{ // 等 first 的结果
		Submitter: "a", Values: []int64{2},
		HasDependency: true, DependencyID: first.ID,
	})
	far := mustSubmit(t, s, SubmitRequest{ // 等 mid
		Submitter: "a", Values: []int64{3},
		HasDependency: true, DependencyID: mid.ID,
	})
	independent := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{9}})
	// 此时唯一位置被 id 1 占用，mid/far 等依赖、independent 等位置。

	// 取消运行中的 first。
	cj, err := s.Cancel(first.ID)
	if err != nil || cj.Status != StatusCanceled {
		t.Fatalf("cancel running: %v %+v", err, cj)
	}
	// 重复取消仍成功。
	if again, err := s.Cancel(first.ID); err != nil || again.Status != StatusCanceled {
		t.Fatalf("repeat cancel: %v %+v", err, again)
	}
	release(first.ID) // 放行计算，成功结果必须被丢弃

	mm := waitStatus(t, s, mid.ID, StatusFailed)
	ff := waitStatus(t, s, far.ID, StatusFailed)
	// independent 未被等待依赖的 mid/far 堵在后面，照常成功。
	ind := waitStatus(t, s, independent.ID, StatusSucceeded)
	if ind.Archive.Sum != 9 {
		t.Fatalf("independent sum=%d want 9", ind.Archive.Sum)
	}
	if mm.BlockerID != first.ID || ff.BlockerID != first.ID {
		t.Fatalf("cascade blockers %d,%d want root %d", mm.BlockerID, ff.BlockerID, first.ID)
	}
	if mm.Archive != nil || ff.Archive != nil {
		t.Fatal("canceled job and dependents must never archive success")
	}
	if !strings.Contains(mm.FailureReason, "取消") || !strings.Contains(ff.FailureReason, "阻断") {
		t.Fatalf("failure reasons must explain the blocker: mid=%q far=%q", mm.FailureReason, ff.FailureReason)
	}

	// 已成功的作业不能取消，结果保持。
	if _, err := s.Cancel(independent.ID); !errors.Is(err, ErrNotCancellable) {
		t.Fatalf("cancel succeeded: %v, want ErrNotCancellable", err)
	}
	got, _ := s.Get(independent.ID)
	if got.Status != StatusSucceeded || got.Archive == nil {
		t.Fatal("succeeded result changed after rejected cancel")
	}
	// 已失败的作业同样不能取消，原因保持。
	if _, err := s.Cancel(mm.ID); !errors.Is(err, ErrNotCancellable) {
		t.Fatalf("cancel failed: %v", err)
	}
	got2, _ := s.Get(mm.ID)
	if got2.Status != StatusFailed || got2.FailureReason != mm.FailureReason {
		t.Fatal("failed record changed after rejected cancel")
	}
}

func TestCancelQueuedBeforeRun(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)
	first := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	waitStarted(t, started, first.ID)
	second := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{2}})
	if j, _ := s.Get(second.ID); j.WaitReason != WaitSlot {
		t.Fatalf("reason=%q want WaitSlot", j.WaitReason)
	}
	if _, err := s.Cancel(second.ID); err != nil {
		t.Fatalf("cancel queued: %v", err)
	}
	release(first.ID)
	waitStatus(t, s, first.ID, StatusSucceeded)
	j, _ := s.Get(second.ID)
	if j.Status != StatusCanceled || j.Archive != nil {
		t.Fatalf("queued cancel: status=%s archive=%v", j.Status, j.Archive)
	}
}

func TestOverflowFailureLeavesNoSuccess(t *testing.T) {
	s, _ := openTestStore(t)
	big := int64(3037000500)
	j := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{big, 1, 2}})
	failed := waitStatus(t, s, j.ID, StatusFailed)
	if !strings.Contains(failed.FailureReason, "平方和") {
		t.Fatalf("reason=%q", failed.FailureReason)
	}
	if failed.Archive != nil || len(failed.EffectiveValues) != 0 {
		t.Fatal("overflow failure must not retain success result or effective input")
	}
	// 磁盘上同样不能是成功记录。
	s2, err := Open(s.Dir())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, _ := s2.Get(j.ID)
	if got.Status != StatusFailed || got.Archive != nil {
		t.Fatalf("persisted status=%s archive=%v", got.Status, got.Archive)
	}
}

func TestSumOverflowReasonViaHook(t *testing.T) {
	s, _ := openTestStore(t)
	s.compute = func(_ uint64, _ []int64, _ int64, _ func() bool) (int64, int64, string, bool) {
		return 0, 0, "总和超出有符号 64 位整数范围", false
	}
	j := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	failed := waitStatus(t, s, j.ID, StatusFailed)
	if !strings.Contains(failed.FailureReason, "总和") || failed.Archive != nil {
		t.Fatalf("reason=%q archive=%v", failed.FailureReason, failed.Archive)
	}
}

func TestListBySubmitterAndTimeRange(t *testing.T) {
	s, clock := openTestStore(t)
	_ = clock
	a1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	a2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{2}})
	b1 := mustSubmit(t, s, SubmitRequest{Submitter: "b", Values: []int64{3}})
	waitStatus(t, s, b1.ID, StatusSucceeded)

	got, err := s.List("a", time.Time{}, time.Time{})
	if err != nil || len(got) != 2 || got[0].ID != a1.ID || got[1].ID != a2.ID {
		t.Fatalf("list a: %v err=%v", ids(got), err)
	}
	// 时间范围两端包含。
	q1, _ := s.Get(a1.ID)
	q2, _ := s.Get(a2.ID)
	got, err = s.List("a", q1.QueuedAt, q2.QueuedAt)
	if err != nil || len(got) != 2 {
		t.Fatalf("inclusive range: %v err=%v", ids(got), err)
	}
	got, _ = s.List("a", q2.QueuedAt, q2.QueuedAt)
	if len(got) != 1 || got[0].ID != a2.ID {
		t.Fatalf("single-point range: %v", ids(got))
	}
	// 起始晚于结束拒绝查询。
	if _, err := s.List("a", q2.QueuedAt, q1.QueuedAt); !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatalf("err=%v want ErrInvalidTimeRange", err)
	}
	// 提交先后排列（跨提交人也按 id）。
	all, _ := s.List("b", time.Time{}, time.Time{})
	if len(all) != 1 || all[0].ID != b1.ID {
		t.Fatalf("list b: %v", ids(all))
	}
}

func ids(js []*Job) []uint64 {
	out := make([]uint64, len(js))
	for i, j := range js {
		out[i] = j.ID
	}
	return out
}

func TestGetNotFound(t *testing.T) {
	s, _ := openTestStore(t)
	if _, err := s.Get(12345); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err=%v", err)
	}
	if _, err := s.Cancel(12345); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cancel err=%v", err)
	}
}

func TestReturnedViewIsImmutable(t *testing.T) {
	s, _ := openTestStore(t)
	j := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1, 2}, Seed: 5})
	waitStatus(t, s, j.ID, StatusSucceeded)

	v1, _ := s.Get(j.ID)
	v1.Values[0] = 999
	v1.Archive.Sum = 999
	v1.Archive.EffectiveValues[0] = 999
	v1.Archive.Checksum = "tampered"
	v1.Status = StatusFailed

	v2, _ := s.Get(j.ID)
	if v2.Status != StatusSucceeded || v2.Archive.Sum != 3 ||
		v2.Archive.Checksum == "tampered" || v2.Archive.EffectiveValues[0] != 1 ||
		v2.Values[0] != 1 {
		t.Fatalf("mutating returned view changed stored record: %+v", v2)
	}
}
