package numeric

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// 多依赖端到端：原始 [10]，两个上游总和依次为 3 和 -2，
// 实际输入必须是 [10, 3, -2] → 总和 11、平方和 113，
// 不按上游完成先后改变。
func TestMultiDependencyAppendsInListOrder(t *testing.T) {
	s, _ := openTestStore(t)
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1, 2}})  // 和 3
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1, -3}}) // 和 -2
	waitStatus(t, s, u1.ID, StatusSucceeded)
	waitStatus(t, s, u2.ID, StatusSucceeded)

	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{10},
		Dependencies: []uint64{u2.ID, u1.ID}, // 故意与提交次序不同
	})
	d := waitStatus(t, s, down.ID, StatusSucceeded)
	want := []int64{10, -2, 3}
	if got := d.Archive.EffectiveValues; len(got) != len(want) {
		t.Fatalf("effective=%v want %v", got, want)
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("effective=%v want %v（必须按列表顺序追加，与上游完成先后无关）", got, want)
			}
		}
	}
	if d.Archive.Sum != 11 || d.Archive.SumOfSquares != 113 {
		t.Fatalf("sum=%d sq=%d, want 11,113", d.Archive.Sum, d.Archive.SumOfSquares)
	}
	// 详情保存依赖顺序。
	if len(d.Dependencies) != 2 || d.Dependencies[0] != u2.ID || d.Dependencies[1] != u1.ID {
		t.Fatalf("dependencies=%v, want [%d %d]", d.Dependencies, u2.ID, u1.ID)
	}
	if d.Archive.Dependencies[0] != u2.ID || d.Archive.Dependencies[1] != u1.ID {
		t.Fatalf("archive dependencies=%v", d.Archive.Dependencies)
	}
	// 与“直接提交相同实际输入”的作业摘要/校验值一致；上游作业号不参与。
	direct := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{10, -2, 3}})
	dd := waitStatus(t, s, direct.ID, StatusSucceeded)
	if d.Archive.ResultDigest != dd.Archive.ResultDigest ||
		d.Archive.Checksum != dd.Archive.Checksum ||
		d.Archive.InputsDigest != dd.Archive.InputsDigest ||
		d.Archive.Log != dd.Archive.Log {
		t.Fatal("multi-dependency digest/checksum/log must match expanded input; upstream ids must not participate")
	}
}

func TestMultiDependencyRejectsInvalidList(t *testing.T) {
	s, _ := openTestStore(t)
	u := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u", Values: []int64{1}})
	waitStatus(t, s, u.ID, StatusSucceeded)

	cases := []struct {
		name string
		deps []uint64
		want error
	}{
		{"zero", []uint64{0}, ErrInvalidDependency},
		{"duplicate", []uint64{u.ID, u.ID}, ErrInvalidDependency},
		{"missing", []uint64{u.ID, 999}, ErrDependencyNotFound},
	}
	for _, c := range cases {
		_, err := s.Submit(SubmitRequest{
			Submitter: "a", RequestID: "bad-" + c.name, Values: []int64{1},
			Dependencies: c.deps,
		})
		if !errors.Is(err, c.want) {
			t.Fatalf("%s: err=%v want %v", c.name, err, c.want)
		}
	}
	// 单依赖方式与非空列表同时启用 → 拒绝。
	_, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "both", Values: []int64{1},
		HasDependency: true, DependencyID: u.ID, Dependencies: []uint64{u.ID},
	})
	if !errors.Is(err, ErrInvalidDependency) {
		t.Fatalf("both modes: err=%v want ErrInvalidDependency", err)
	}
	// 拒绝不产生记录，也不占用幂等请求号：同号合法提交仍创建新作业。
	if got, _ := s.List("a", time.Time{}, time.Time{}); len(got) != 1 {
		t.Fatalf("rejected submits left records: %d", len(got))
	}
	ok := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "both", Values: []int64{7}})
	if ok.ID == u.ID {
		t.Fatal("rejected submit consumed the idempotency request number")
	}
	waitStatus(t, s, ok.ID, StatusSucceeded)
}

func TestMultiDependencyAlreadyFailedUpstreamFailsImmediately(t *testing.T) {
	s, _ := openTestStore(t)
	big := int64(3037000500)
	bad := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{big}})
	waitStatus(t, s, bad.ID, StatusFailed)
	canceled := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "c", Values: []int64{2}})
	if _, err := s.Cancel(canceled.ID); err != nil {
		t.Fatal(err)
	}

	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{1},
		Dependencies: []uint64{canceled.ID, bad.ID}, // 列表中最靠前的是 canceled
	})
	d, _ := s.Get(down.ID)
	if d.Status != StatusFailed {
		t.Fatalf("status=%s want failed immediately", d.Status)
	}
	if d.BlockerID != canceled.ID {
		t.Fatalf("blocker=%d want earliest-in-list %d", d.BlockerID, canceled.ID)
	}
	if !strings.Contains(d.FailureReason, "直接上游作业") || !strings.Contains(d.FailureReason, "阻断") {
		t.Fatalf("reason=%q must name direct upstream and 阻断", d.FailureReason)
	}
	if d.Archive != nil {
		t.Fatal("immediate failure must carry no archive")
	}
}

func TestMultiDependencyPendingListAndSlotReason(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)

	head := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	waitStarted(t, started, head.ID)
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{2}})
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{3}})
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{4},
		Dependencies: []uint64{u1.ID, u2.ID},
	})
	later := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{5}})

	d, _ := s.Get(down.ID)
	if d.WaitReason != WaitDependency {
		t.Fatalf("reason=%q want WaitDependency", d.WaitReason)
	}
	if len(d.PendingDependencies) != 2 || d.PendingDependencies[0] != u1.ID || d.PendingDependencies[1] != u2.ID {
		t.Fatalf("pending=%v want [%d %d] in submit order", d.PendingDependencies, u1.ID, u2.ID)
	}
	// 等待多上游的作业不挡后面的可运行作业。
	l, _ := s.Get(later.ID)
	if l.WaitReason != WaitSlot {
		t.Fatalf("later job reason=%q want WaitSlot", l.WaitReason)
	}

	// 堵住 u2：u1 成功后 down 仍排队，待列出项应只剩 u2。
	block(u2.ID)
	release(head.ID)
	waitStatus(t, s, head.ID, StatusSucceeded)
	waitStatus(t, s, u1.ID, StatusSucceeded)
	d, _ = s.Get(down.ID)
	if d.WaitReason != WaitDependency || len(d.PendingDependencies) != 1 || d.PendingDependencies[0] != u2.ID {
		t.Fatalf("after u1: reason=%q pending=%v, want WaitDependency/[%d]", d.WaitReason, d.PendingDependencies, u2.ID)
	}
	// u2 也成功后，依赖全部就绪：排队原因变为等待计算位置（作业也可能已开始运行）。
	release(u2.ID)
	waitStatus(t, s, u2.ID, StatusSucceeded)
	d, _ = s.Get(down.ID)
	if d.Status == StatusQueued {
		if d.WaitReason != WaitSlot || len(d.PendingDependencies) != 0 {
			t.Fatalf("after all upstreams: reason=%q pending=%v, want WaitSlot/empty", d.WaitReason, d.PendingDependencies)
		}
	} else if d.Status != StatusRunning && d.Status != StatusSucceeded {
		t.Fatalf("after all upstreams: status=%s reason=%q", d.Status, d.WaitReason)
	}
	waitStatus(t, s, later.ID, StatusSucceeded)
	waitStatus(t, s, down.ID, StatusSucceeded)
}

func TestMultiDependencyCascadePicksEarliestFailedUpstream(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)

	head := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}}) // id 1 占位置
	waitStarted(t, started, head.ID)
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{2}}) // id 2
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{3}}) // id 3
	down := mustSubmit(t, s, SubmitRequest{                                   // id 4 等 2、3
		Submitter: "a", Values: []int64{4},
		Dependencies: []uint64{u1.ID, u2.ID},
	})
	child := mustSubmit(t, s, SubmitRequest{ // id 5 等 id 4
		Submitter: "a", Values: []int64{5},
		Dependencies: []uint64{down.ID},
	})

	// 取消列表中靠后的 u2：down 立即失败，直接阻断者为 u2，根因也是 u2。
	if _, err := s.Cancel(u2.ID); err != nil {
		t.Fatal(err)
	}
	d := waitStatus(t, s, down.ID, StatusFailed)
	if d.BlockerID != u2.ID {
		t.Fatalf("down blocker=%d want %d", d.BlockerID, u2.ID)
	}
	c := waitStatus(t, s, child.ID, StatusFailed)
	if c.BlockerID != u2.ID {
		t.Fatalf("child blocker=%d want root %d", c.BlockerID, u2.ID)
	}
	// 此后 u1 再失败也不能改写已确定的原因。
	if _, err := s.Cancel(u1.ID); err != nil {
		t.Fatal(err)
	}
	d2, _ := s.Get(down.ID)
	if d2.BlockerID != u2.ID {
		t.Fatalf("blocker rewritten to %d after later upstream change", d2.BlockerID)
	}
	release(head.ID)
	waitStatus(t, s, head.ID, StatusSucceeded)
}

func TestCancelMergedJobDoesNotCancelUpstreams(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	waitStarted(t, started, u1.ID)
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{2}})
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{3},
		Dependencies: []uint64{u1.ID, u2.ID},
	})
	// down 仍在排队时取消：只影响它自己，不取消上游。
	if _, err := s.Cancel(down.ID); err != nil {
		t.Fatal(err)
	}
	if g, _ := s.Get(down.ID); g.Status != StatusCanceled {
		t.Fatalf("down=%s want canceled", g.Status)
	}
	release(u1.ID)
	waitStatus(t, s, u1.ID, StatusSucceeded)
	waitStatus(t, s, u2.ID, StatusSucceeded)
	for _, id := range []uint64{u1.ID, u2.ID} {
		if g, _ := s.Get(id); g.Status != StatusSucceeded {
			t.Fatalf("upstream %d status=%s, want succeeded (cancel must not cascade upward)", id, g.Status)
		}
	}
}

func TestMultiDependencyIdempotencyContentAndOrder(t *testing.T) {
	s, _ := openTestStore(t)
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{1}})
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{2}})
	waitStatus(t, s, u1.ID, StatusSucceeded)
	waitStatus(t, s, u2.ID, StatusSucceeded)

	orig := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "k", Values: []int64{1}, Seed: 0,
		Dependencies: []uint64{u1.ID, u2.ID},
	})
	// 单依赖方式与只含同一作业号的列表视为相同内容：
	// 先以单依赖方式提交，再以列表 [同一作业号] 重放 → 同一作业。
	single := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "ks", Values: []int64{1}, Seed: 0,
		HasDependency: true, DependencyID: u1.ID,
	})
	singleReplay := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "ks", Values: []int64{1}, Seed: 0,
		Dependencies: []uint64{u1.ID},
	})
	if singleReplay.ID != single.ID {
		t.Fatalf("single-dep vs list[same] must be identical content: %d vs %d", singleReplay.ID, single.ID)
	}
	// 次序变化 → 冲突。
	if _, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "k", Values: []int64{1}, Seed: 0,
		Dependencies: []uint64{u2.ID, u1.ID},
	}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatal("order change must conflict")
	}
	// 内容变化 → 冲突。
	if _, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "k", Values: []int64{1}, Seed: 0,
		Dependencies: []uint64{u1.ID},
	}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatal("list change must conflict")
	}
	// 完全相同 → 原作业。
	same := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "k", Values: []int64{1}, Seed: 0,
		Dependencies: []uint64{u1.ID, u2.ID},
	})
	if same.ID != orig.ID {
		t.Fatalf("same content replayed id=%d want %d", same.ID, orig.ID)
	}
	// 原记录内容不变。
	g, _ := s.Get(orig.ID)
	if len(g.Dependencies) != 2 || g.Dependencies[0] != u1.ID || g.Dependencies[1] != u2.ID {
		t.Fatalf("original record altered: %v", g.Dependencies)
	}
}

func TestMultiDependencyConcurrentDuplicate(t *testing.T) {
	s, _ := openTestStore(t)
	u := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u", Values: []int64{1}})
	waitStatus(t, s, u.ID, StatusSucceeded)

	const n = 16
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
				Submitter: "a", RequestID: "race", Values: []int64{1},
				Dependencies: []uint64{u.ID},
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
		t.Fatalf("created %d jobs under one idempotency key", len(uniq))
	}
}

func TestMultiDependencyViewIsDeepCopy(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)
	u := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	waitStarted(t, started, u.ID)
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{2},
		Dependencies: []uint64{u.ID},
	})
	v1, _ := s.Get(down.ID)
	v1.Dependencies[0] = 999
	v1.PendingDependencies[0] = 998
	v2, _ := s.Get(down.ID)
	if v2.Dependencies[0] != u.ID || v2.PendingDependencies[0] != u.ID {
		t.Fatalf("mutating returned view changed stored record: deps=%v pending=%v",
			v2.Dependencies, v2.PendingDependencies)
	}
	release(u.ID)
	waitStatus(t, s, u.ID, StatusSucceeded)
	waitStatus(t, s, down.ID, StatusSucceeded)
}

func TestMultiDependencyReopenPreservesAndResumes(t *testing.T) {
	dir := t.TempDir()
	clock := newManualClock()
	s, err := Open(dir, WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	// 堵住唯一计算位置，保证重开前所有作业都停留在排队状态。
	started, block, _, _, _ := gateCompute(t, s)
	block(1)
	head := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	waitStarted(t, started, head.ID)
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{1, 2}}) // 和 3
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{3}})    // 和 3
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{10},
		Dependencies: []uint64{u2.ID, u1.ID},
	})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	// head 中断失败；u1、u2 继续；down 最后成功，输入为 [10, 3, 3]。
	waitStatus(t, s2, head.ID, StatusFailed)
	d := waitStatus(t, s2, down.ID, StatusSucceeded)
	want := []int64{10, 3, 3}
	if len(d.Archive.EffectiveValues) != 3 {
		t.Fatalf("effective=%v want %v", d.Archive.EffectiveValues, want)
	}
	for i := range want {
		if d.Archive.EffectiveValues[i] != want[i] {
			t.Fatalf("effective=%v want %v", d.Archive.EffectiveValues, want)
		}
	}
	if d.Archive.Sum != 16 || d.Archive.SumOfSquares != 118 {
		t.Fatalf("sum=%d sq=%d want 16,118", d.Archive.Sum, d.Archive.SumOfSquares)
	}
	if len(d.Dependencies) != 2 || d.Dependencies[0] != u2.ID || d.Dependencies[1] != u1.ID {
		t.Fatalf("dependencies after reopen=%v", d.Dependencies)
	}
	// 幂等关系保留。
	replay := mustSubmit(t, s2, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{10},
		Dependencies: []uint64{u2.ID, u1.ID},
	})
	if replay.ID != down.ID {
		t.Fatalf("idempotency after reopen: id=%d want %d", replay.ID, down.ID)
	}
}

func TestMultiDependencyReopenInterruptedRunBlocksDownstream(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// id 1 运行中（中断）；id 2、3 都等 id 1；id 4 等 id 2。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "r1",
		values: []int64{1}, dependencies: nil,
		queuedAt:  base,
		startedAt: base.Add(time.Second),
		status:    StatusRunning,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "r2",
		values: []int64{2}, dependencies: []uint64{1},
		queuedAt: base.Add(2 * time.Second),
		status:   StatusQueued,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "r3",
		values: []int64{3}, dependencies: []uint64{1},
		queuedAt: base.Add(3 * time.Second),
		status:   StatusQueued,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 4, submitter: "a", requestID: "r4",
		values: []int64{4}, dependencies: []uint64{2},
		queuedAt: base.Add(4 * time.Second),
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	j1, _ := s.Get(1)
	if j1.Status != StatusFailed || !strings.Contains(j1.FailureReason, "中断") {
		t.Fatalf("interrupted: status=%s reason=%q", j1.Status, j1.FailureReason)
	}
	j2 := waitStatus(t, s, 2, StatusFailed)
	j3 := waitStatus(t, s, 3, StatusFailed)
	j4 := waitStatus(t, s, 4, StatusFailed)
	if j2.BlockerID != 1 || j3.BlockerID != 1 || j4.BlockerID != 1 {
		t.Fatalf("blockers %d,%d,%d want root 1", j2.BlockerID, j3.BlockerID, j4.BlockerID)
	}
	if !strings.Contains(j2.FailureReason, "作业 1") || !strings.Contains(j3.FailureReason, "作业 1") {
		t.Fatalf("reasons must name blocking job: j2=%q j3=%q", j2.FailureReason, j3.FailureReason)
	}
}

// 旧格式记录（单依赖字段、无 dependencies）重开后仍可读、可恢复，
// 且已有成功归档的摘要与校验值不变。
func TestLegacyRecordsRemainReadableAndStable(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "r1",
		values:          []int64{1, 2},
		effectiveValues: []int64{1, 2},
		queuedAt:        base,
		startedAt:       base.Add(time.Second),
		status:          StatusSucceeded,
		archive: &Archive{
			JobID: 1, Submitter: "a", RequestID: "r1", Seed: 0,
			Values: []int64{1, 2}, HasDependency: false,
			EffectiveValues: []int64{1, 2},
			InputsDigest:    inputsDigestHex([]int64{1, 2}, 0),
			Sum:             3, SumOfSquares: 5,
			ResultDigest: resultDigestHex([]int64{1, 2}, 0, 3, 5),
			Log:          buildLog([]int64{1, 2}, 0, 3, 5),
			Checksum:     checksumHex([]int64{1, 2}, 0, 3, 5, buildLog([]int64{1, 2}, 0, 3, 5), resultDigestHex([]int64{1, 2}, 0, 3, 5)),
			CompletedAt:  base.Add(2 * time.Second),
		},
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "r2",
		values:   []int64{3},
		queuedAt: base.Add(3 * time.Second),
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	j1, _ := s.Get(1)
	if j1.Status != StatusSucceeded || j1.Archive == nil {
		t.Fatalf("legacy success record broken: %s", j1.Status)
	}
	if j1.Archive.Sum != 3 || j1.Archive.Checksum != checksumHex([]int64{1, 2}, 0, 3, 5, j1.Archive.Log, j1.Archive.ResultDigest) {
		t.Fatal("legacy archive checksum changed")
	}
	if len(j1.Dependencies) != 0 {
		t.Fatalf("legacy no-dep job dependencies=%v want empty", j1.Dependencies)
	}
	// 旧格式排队作业继续被处理。
	j2 := waitStatus(t, s, 2, StatusSucceeded)
	if j2.Archive.Sum != 3 {
		t.Fatalf("legacy queued job result=%d want 3", j2.Archive.Sum)
	}
}
