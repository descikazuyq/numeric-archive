package numeric

import (
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- 测试辅助 ----

func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func setPaused(s *Store, b bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused = b
	if !b {
		select {
		case s.resume <- struct{}{}:
		default:
		}
	}
}

func setGate(s *Store, g chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.computeGate = g
}

func waitStatus(t *testing.T, s *Store, id int64, want JobStatus) *Job {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last *Job
	for time.Now().Before(deadline) {
		j, err := s.Get(id)
		if err != nil {
			t.Fatalf("Get(%d): %v", id, err)
		}
		last = j
		if j.Status == want {
			return j
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("job %d status = %q, want %q", id, last.Status, want)
	return nil
}

func submit(t *testing.T, s *Store, p SubmitParams) *Job {
	t.Helper()
	j, err := s.Submit(p)
	if err != nil {
		t.Fatalf("Submit(%q/%q): %v", p.Submitter, p.ReqNo, err)
	}
	return j
}

// ---- 提交与计算 ----

func TestSubmitComputesSumAndSquareSum(t *testing.T) {
	s := newStore(t)
	j := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "r1", Sequence: []int64{1, 2, 3}, Seed: 42})
	if j.ID != 1 {
		t.Fatalf("job id = %d, want 1", j.ID)
	}
	if j.Status != StatusQueued {
		t.Fatalf("new job status = %q, want queued", j.Status)
	}
	got := waitStatus(t, s, j.ID, StatusSucceeded)
	if got.Archive == nil {
		t.Fatal("succeeded job has no archive")
	}
	a := got.Archive
	if a.Sum != 6 || a.SumOfSquares != 14 || a.Count != 3 {
		t.Fatalf("result = sum %d, sq %d, count %d; want 6/14/3", a.Sum, a.SumOfSquares, a.Count)
	}
	if len(a.Inputs) != 3 || a.Inputs[0] != 1 || a.Inputs[2] != 3 {
		t.Fatalf("inputs = %v", a.Inputs)
	}
	if a.Seed != 42 || a.Submitter != "alice" || a.ReqNo != "r1" {
		t.Fatalf("archive raw params mismatch: %+v", a)
	}
	if a.Checksum == "" {
		t.Fatal("empty checksum")
	}
	if len(a.ComputationLog) != 3 {
		t.Fatalf("computation log len = %d, want 3", len(a.ComputationLog))
	}
}

func TestEmptySequenceRejectedNoRecord(t *testing.T) {
	s := newStore(t)
	_, err := s.Submit(SubmitParams{Submitter: "alice", ReqNo: "empty", Sequence: []int64{}})
	if !errors.Is(err, ErrEmptySequence) {
		t.Fatalf("err = %v, want ErrEmptySequence", err)
	}
	if _, err := s.Get(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(1) err = %v, want ErrNotFound", err)
	}
	list, err := s.List("alice", time.Time{}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("list len = %d, want 0", len(list))
	}
}

func TestOverflowFailsWithoutArchive(t *testing.T) {
	s := newStore(t)
	// 平方和溢出（和未溢出）。
	j := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "of", Sequence: []int64{math.MaxInt64}})
	got := waitStatus(t, s, j.ID, StatusFailed)
	if got.Archive != nil {
		t.Fatal("failed job has archive")
	}
	if got.FailureReason == "" {
		t.Fatal("failure reason empty")
	}
	t.Logf("failure reason: %s", got.FailureReason)

	// 求和溢出。
	j2 := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "of2", Sequence: []int64{math.MaxInt64, 1}})
	got2 := waitStatus(t, s, j2.ID, StatusFailed)
	if got2.Archive != nil || got2.FailureReason == "" {
		t.Fatalf("sum overflow not handled: archive=%v reason=%q", got2.Archive, got2.FailureReason)
	}
}

// ---- 幂等 ----

func TestIdempotentReplayReturnsSameJob(t *testing.T) {
	s := newStore(t)
	p := SubmitParams{Submitter: "alice", ReqNo: "k", Sequence: []int64{1, 2}, Seed: 7}
	j1 := submit(t, s, p)
	j2, err := s.Submit(p)
	if err != nil {
		t.Fatal(err)
	}
	if j1.ID != j2.ID {
		t.Fatalf("replay created job %d, want %d", j2.ID, j1.ID)
	}

	// 整数次序变化 → 冲突。
	pOrder := p
	pOrder.Sequence = []int64{2, 1}
	if _, err := s.Submit(pOrder); !errors.Is(err, ErrConflict) {
		t.Fatalf("order change err = %v, want ErrConflict", err)
	}
	// 种子变化 → 冲突。
	pSeed := p
	pSeed.Seed = 8
	if _, err := s.Submit(pSeed); !errors.Is(err, ErrConflict) {
		t.Fatalf("seed change err = %v, want ErrConflict", err)
	}
	// 依赖变化 → 冲突。
	dep := int64(1)
	pDep := p
	pDep.DependencyID = &dep
	if _, err := s.Submit(pDep); !errors.Is(err, ErrConflict) {
		t.Fatalf("dependency change err = %v, want ErrConflict", err)
	}
	// 不同提交人可以使用相同请求号。
	j3 := submit(t, s, SubmitParams{Submitter: "bob", ReqNo: "k", Sequence: []int64{9}})
	if j3.ID == j1.ID {
		t.Fatalf("different submitter reused job id %d", j3.ID)
	}
	// 冲突后原记录不变。
	got, err := s.Get(j1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Seed != 7 || len(got.Sequence) != 2 {
		t.Fatalf("original record changed after conflict: %+v", got)
	}
}

func TestConcurrentIdempotencyCreatesOneJob(t *testing.T) {
	s := newStore(t)
	p := SubmitParams{Submitter: "alice", ReqNo: "race", Sequence: []int64{1, 2, 3}}
	const n = 50
	var wg sync.WaitGroup
	ids := make(chan int64, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, err := s.Submit(p)
			if err == nil {
				ids <- j.ID
			}
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[int64]bool{}
	for id := range ids {
		seen[id] = true
	}
	if len(seen) != 1 {
		t.Fatalf("created %d distinct jobs, want 1", len(seen))
	}
}

// ---- 取消 ----

func TestCancelQueued(t *testing.T) {
	s := newStore(t)
	setPaused(s, true)
	defer setPaused(s, false)

	j := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "c", Sequence: []int64{1}})
	if j.Status != StatusQueued || j.QueueState != "waiting_slot" {
		t.Fatalf("queued view = %+v", j)
	}
	if err := s.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(j.ID)
	if got.Status != StatusCancelled {
		t.Fatalf("status = %q, want cancelled", got.Status)
	}
	// 重复取消已取消的作业仍成功。
	if err := s.Cancel(j.ID); err != nil {
		t.Fatalf("re-cancel: %v", err)
	}
	// 取消不存在的作业。
	if err := s.Cancel(999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cancel missing err = %v", err)
	}
	// 取消已成功的作业返回不能取消。
	j3 := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "ok", Sequence: []int64{1}})
	setPaused(s, false)
	waitStatus(t, s, j3.ID, StatusSucceeded)
	if err := s.Cancel(j3.ID); !errors.Is(err, ErrCannotCancel) {
		t.Fatalf("cancel succeeded err = %v, want ErrCannotCancel", err)
	}
}

func TestCancelRunningDiscardsResult(t *testing.T) {
	s := newStore(t)
	gate := make(chan struct{})
	setGate(s, gate)
	j := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "cr", Sequence: []int64{1, 2}})
	waitStatus(t, s, j.ID, StatusRunning)

	if err := s.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	close(gate)
	setGate(s, nil)

	// 等待调度器完成丢弃。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.Get(j.ID)
		if got.Status == StatusCancelled && got.Archive == nil {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	got, _ := s.Get(j.ID)
	t.Fatalf("after release: status=%q archive=%v", got.Status, got.Archive)
}

// ---- 依赖 ----

func TestDependencyAddsSumToInputs(t *testing.T) {
	s := newStore(t)
	dep := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "dep", Sequence: []int64{1, 2}, Seed: 1})
	waitStatus(t, s, dep.ID, StatusSucceeded)

	depID := dep.ID
	j := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "use", Sequence: []int64{10}, DependencyID: &depID})
	got := waitStatus(t, s, j.ID, StatusSucceeded)
	a := got.Archive
	// 实际输入 = [10, 3]，和 = 13，平方和 = 109。
	if len(a.Inputs) != 2 || a.Inputs[0] != 10 || a.Inputs[1] != 3 {
		t.Fatalf("inputs = %v, want [10 3]", a.Inputs)
	}
	if a.Sum != 13 || a.SumOfSquares != 109 {
		t.Fatalf("sum=%d sq=%d, want 13/109", a.Sum, a.SumOfSquares)
	}
}

func TestDependencyMissingRejectedNoRecord(t *testing.T) {
	s := newStore(t)
	bad := int64(99)
	_, err := s.Submit(SubmitParams{Submitter: "alice", ReqNo: "x", Sequence: []int64{1}, DependencyID: &bad})
	if !errors.Is(err, ErrDepNotFound) {
		t.Fatalf("err = %v, want ErrDepNotFound", err)
	}
	if _, err := s.Get(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(1) = %v, want ErrNotFound", err)
	}
}

func TestDependencyFailureBlocksDownstream(t *testing.T) {
	s := newStore(t)
	dep := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "dep", Sequence: []int64{math.MaxInt64, 1}})
	waitStatus(t, s, dep.ID, StatusFailed)

	depID := dep.ID
	j := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "use", Sequence: []int64{10}, DependencyID: &depID})
	got := waitStatus(t, s, j.ID, StatusFailed)
	if got.Archive != nil {
		t.Fatal("blocked downstream has archive")
	}
	if got.FailureReason == "" || !strings.Contains(got.FailureReason, "1") {
		t.Fatalf("reason = %q, should name blocking job 1", got.FailureReason)
	}

	// 级联到更下游：C 失败 → B 被 C 阻断 → A 被 B 阻断。
	c := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "c", Sequence: []int64{math.MaxInt64, 1}})
	waitStatus(t, s, c.ID, StatusFailed)
	bID := c.ID
	b := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "b", Sequence: []int64{1}, DependencyID: &bID})
	aID := b.ID
	a := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "a", Sequence: []int64{1}, DependencyID: &aID})

	gb := waitStatus(t, s, b.ID, StatusFailed)
	ga := waitStatus(t, s, a.ID, StatusFailed)
	if !strings.Contains(gb.FailureReason, "3") {
		t.Fatalf("B reason = %q, should name job 3", gb.FailureReason)
	}
	if !strings.Contains(ga.FailureReason, "4") {
		t.Fatalf("A reason = %q, should name job 4", ga.FailureReason)
	}
}

func TestDependencyCancelledBlocksDownstream(t *testing.T) {
	s := newStore(t)
	setPaused(s, true)

	dep := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "dep", Sequence: []int64{1}})
	depID := dep.ID
	j := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "use", Sequence: []int64{10}, DependencyID: &depID})

	// 排队时说明等待原因。
	gotDep, _ := s.Get(dep.ID)
	if gotDep.QueueState != "waiting_slot" {
		t.Fatalf("dep queue state = %q", gotDep.QueueState)
	}
	gotJ, _ := s.Get(j.ID)
	if gotJ.QueueState != "waiting_dependency" || gotJ.WaitingFor == nil || *gotJ.WaitingFor != dep.ID {
		t.Fatalf("dependent view = %+v", gotJ)
	}

	if err := s.Cancel(dep.ID); err != nil {
		t.Fatal(err)
	}
	gotJ2, _ := s.Get(j.ID)
	if gotJ2.Status != StatusFailed || !strings.Contains(gotJ2.FailureReason, "1") {
		t.Fatalf("after cancel: status=%q reason=%q", gotJ2.Status, gotJ2.FailureReason)
	}
	setPaused(s, false)
}

func TestCancelRunningBlocksDownstream(t *testing.T) {
	s := newStore(t)
	gate := make(chan struct{})
	setGate(s, gate)
	dep := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "dep", Sequence: []int64{1, 2}})
	waitStatus(t, s, dep.ID, StatusRunning)

	depID := dep.ID
	j := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "use", Sequence: []int64{10}, DependencyID: &depID})
	if err := s.Cancel(dep.ID); err != nil {
		t.Fatal(err)
	}
	close(gate)
	setGate(s, nil)

	got := waitStatus(t, s, j.ID, StatusFailed)
	if got.Archive != nil || !strings.Contains(got.FailureReason, "1") {
		t.Fatalf("downstream: status=%q archive=%v reason=%q", got.Status, got.Archive, got.FailureReason)
	}
}

func TestDependencyWaitDoesNotBlockRunnableJobs(t *testing.T) {
	s := newStore(t)
	// 链：C(无依赖) → B(依赖C) → A(依赖B)，另有 D(无依赖)。
	// C 被 gate 卡住时，B、A 等待依赖；释放后全部应按 C、B、A、D 顺序成功，
	// 调度器不能卡在链首等待依赖而不扫描后面的可运行作业。
	gate := make(chan struct{})
	setGate(s, gate)
	c := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "c", Sequence: []int64{1}})
	bID := c.ID
	b := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "b", Sequence: []int64{2}, DependencyID: &bID})
	aID := b.ID
	a := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "a", Sequence: []int64{3}, DependencyID: &aID})
	d := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "d", Sequence: []int64{4}})

	waitStatus(t, s, c.ID, StatusRunning)
	for _, id := range []int64{b.ID, a.ID} {
		g, _ := s.Get(id)
		if g.QueueState != "waiting_dependency" {
			t.Fatalf("job %d queue state = %q, want waiting_dependency", id, g.QueueState)
		}
	}
	gd, _ := s.Get(d.ID)
	if gd.QueueState != "waiting_slot" {
		t.Fatalf("job 4 queue state = %q, want waiting_slot", gd.QueueState)
	}

	close(gate)
	setGate(s, nil)

	ga := waitStatus(t, s, a.ID, StatusSucceeded)
	gd2 := waitStatus(t, s, d.ID, StatusSucceeded)
	// C 和 =1；B 实际输入 [2,1] 和 =3；A 实际输入 [3,3] 和 =6；D 和 =4。
	if ga.Archive.Sum != 6 {
		t.Fatalf("A sum = %d, want 6", ga.Archive.Sum)
	}
	if gd2.Archive.Sum != 4 {
		t.Fatalf("D sum = %d, want 4", gd2.Archive.Sum)
	}
	gb, _ := s.Get(b.ID)
	if gb.Archive.Sum != 3 {
		t.Fatalf("B sum = %d, want 3", gb.Archive.Sum)
	}
}

// ---- 查询 ----

func TestListBySubmitterAndTimeRange(t *testing.T) {
	s := newStore(t)
	base := time.Now()
	j1 := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "1", Sequence: []int64{1}})
	time.Sleep(2 * time.Millisecond)
	j2 := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "2", Sequence: []int64{2}})
	time.Sleep(2 * time.Millisecond)
	submit(t, s, SubmitParams{Submitter: "bob", ReqNo: "3", Sequence: []int64{3}})

	// 按提交人过滤。
	list, err := s.List("alice", base.Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].ID != j1.ID || list[1].ID != j2.ID {
		t.Fatalf("alice list = %+v", list)
	}
	listBob, _ := s.List("bob", base.Add(-time.Hour), time.Now().Add(time.Hour))
	if len(listBob) != 1 || listBob[0].ID != 3 {
		t.Fatalf("bob list = %+v", listBob)
	}

	// 包含两个端点。
	listExact, err := s.List("alice", j1.SubmittedAt, j2.SubmittedAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(listExact) != 2 {
		t.Fatalf("exact-range list len = %d, want 2", len(listExact))
	}

	// 起始晚于结束 → 拒绝。
	if _, err := s.List("alice", time.Now().Add(time.Hour), base); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("reversed range err = %v, want ErrInvalidRange", err)
	}
}

// ---- 校验值确定性与不可变性 ----

func TestChecksumDeterministicAndDataImmutable(t *testing.T) {
	s := newStore(t)
	j1 := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "1", Sequence: []int64{1, 2, 3}, Seed: 5})
	waitStatus(t, s, j1.ID, StatusSucceeded)
	j2 := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "2", Sequence: []int64{1, 2, 3}, Seed: 5})
	waitStatus(t, s, j2.ID, StatusSucceeded)
	a1, _ := s.Get(j1.ID)
	a2, _ := s.Get(j2.ID)
	// 相同有效输入与种子 → 相同结果摘要与校验值，与作业号、时间无关。
	if a1.Archive.Checksum != a2.Archive.Checksum {
		t.Fatalf("checksums differ: %s vs %s", a1.Archive.Checksum, a2.Archive.Checksum)
	}
	if a1.Archive.Sum != a2.Archive.Sum || a1.Archive.SumOfSquares != a2.Archive.SumOfSquares {
		t.Fatal("result summaries differ")
	}
	// 种子不同 → 校验值不同。
	j3 := submit(t, s, SubmitParams{Submitter: "alice", ReqNo: "3", Sequence: []int64{1, 2, 3}, Seed: 6})
	waitStatus(t, s, j3.ID, StatusSucceeded)
	a3, _ := s.Get(j3.ID)
	if a3.Archive.Checksum == a1.Archive.Checksum {
		t.Fatal("different seed produced same checksum")
	}

	// 修改查询返回的数据不能改变已保存记录。
	got, _ := s.Get(j1.ID)
	got.Archive.Sum = 999
	got.Archive.Checksum = "tampered"
	got.Sequence[0] = 999
	got2, _ := s.Get(j1.ID)
	if got2.Archive.Sum != 6 || got2.Archive.Checksum == "tampered" || got2.Sequence[0] != 1 {
		t.Fatalf("stored record mutated: sum=%d checksum=%s seq=%v",
			got2.Archive.Sum, got2.Archive.Checksum, got2.Sequence)
	}
}

// ---- 持久化与恢复 ----

func TestPersistenceReopenContinues(t *testing.T) {
	dir := t.TempDir()
	s1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	j1 := submit(t, s1, SubmitParams{Submitter: "alice", ReqNo: "r1", Sequence: []int64{1, 2, 3}, Seed: 9})
	waitStatus(t, s1, j1.ID, StatusSucceeded)
	before, _ := s1.Get(j1.ID)

	setPaused(s1, true)
	j2 := submit(t, s1, SubmitParams{Submitter: "alice", ReqNo: "r2", Sequence: []int64{5}})
	submit(t, s1, SubmitParams{Submitter: "alice", ReqNo: "r3", Sequence: []int64{7}})
	if err := s1.Cancel(j2.ID); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	// 幂等请求号继续生效。
	replay, err := s2.Submit(SubmitParams{Submitter: "alice", ReqNo: "r1", Sequence: []int64{1, 2, 3}, Seed: 9})
	if err != nil {
		t.Fatal(err)
	}
	if replay.ID != j1.ID {
		t.Fatalf("replay id = %d, want %d", replay.ID, j1.ID)
	}
	// 完成结果与归档不丢失，校验值不变。
	got, _ := s2.Get(j1.ID)
	if got.Status != StatusSucceeded || got.Archive == nil {
		t.Fatalf("job1 after reopen: status=%q archive=%v", got.Status, got.Archive)
	}
	if got.Archive.Sum != before.Archive.Sum || got.Archive.Checksum != before.Archive.Checksum {
		t.Fatal("archive changed across reopen")
	}
	// 取消状态保留。
	got2, _ := s2.Get(j2.ID)
	if got2.Status != StatusCancelled {
		t.Fatalf("job2 after reopen: %q", got2.Status)
	}
	// 排队作业继续处理。
	j4 := submit(t, s2, SubmitParams{Submitter: "alice", ReqNo: "r4", Sequence: []int64{4}})
	got4 := waitStatus(t, s2, j4.ID, StatusSucceeded)
	if got4.Archive.Sum != 4 {
		t.Fatalf("queued job after reopen sum = %d, want 4", got4.Archive.Sum)
	}
}

func TestInterruptedRunningJobMarkedFailed(t *testing.T) {
	dir := t.TempDir()
	s1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	setGate(s1, gate)
	j := submit(t, s1, SubmitParams{Submitter: "alice", ReqNo: "run", Sequence: []int64{1, 2}})
	waitStatus(t, s1, j.ID, StatusRunning)
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	got := waitStatus(t, s2, j.ID, StatusFailed)
	if got.Archive != nil {
		t.Fatal("interrupted job has archive")
	}
	if !strings.Contains(got.FailureReason, "中断") {
		t.Fatalf("reason = %q, should mention interruption", got.FailureReason)
	}

	// 中断作业的下游不能继续计算。
	depID := j.ID
	d := submit(t, s2, SubmitParams{Submitter: "alice", ReqNo: "down", Sequence: []int64{1}, DependencyID: &depID})
	gotD := waitStatus(t, s2, d.ID, StatusFailed)
	if gotD.Archive != nil || !strings.Contains(gotD.FailureReason, "1") {
		t.Fatalf("downstream: archive=%v reason=%q", gotD.Archive, gotD.FailureReason)
	}
}

// ---- 其他 ----

func TestUnknownJob(t *testing.T) {
	s := newStore(t)
	if _, err := s.Get(42); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}
