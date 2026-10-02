package numeric

import (
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestMultipleDependenciesWorkedExample 覆盖需求中的算例：
// 原始输入 [10]，两个上游总和依次为 3 和 -2，实际输入 [10 3 -2]，
// 总和 11、平方和 113。
func TestMultipleDependenciesWorkedExample(t *testing.T) {
	s, _ := openTestStore(t)
	up1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1, 2}}) // sum 3
	up2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{-2}})   // sum -2
	waitStatus(t, s, up1.ID, StatusSucceeded)
	waitStatus(t, s, up2.ID, StatusSucceeded)

	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{10},
		Dependencies: []uint64{up1.ID, up2.ID},
	})
	d := waitStatus(t, s, down.ID, StatusSucceeded)
	if got := d.Archive.EffectiveValues; !reflect.DeepEqual(got, []int64{10, 3, -2}) {
		t.Fatalf("effective=%v, want [10 3 -2]", got)
	}
	if d.Archive.Sum != 11 || d.Archive.SumOfSquares != 113 {
		t.Fatalf("sum=%d sq=%d, want 11,113", d.Archive.Sum, d.Archive.SumOfSquares)
	}
	// 与直接提交展开后输入的作业摘要、日志、校验值完全一致。
	direct := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{10, 3, -2}})
	dd := waitStatus(t, s, direct.ID, StatusSucceeded)
	if d.Archive.InputsDigest != dd.Archive.InputsDigest ||
		d.Archive.ResultDigest != dd.Archive.ResultDigest ||
		d.Archive.Checksum != dd.Archive.Checksum ||
		d.Archive.Log != dd.Archive.Log {
		t.Fatal("multi-dependency input must digest identically to the expanded input")
	}
}

// TestDependencyListOrderControlsAppend 证明追加次序只取决于列表次序，
// 而非上游完成先后：物理上 id 较小的上游先完成，但列表把它排在第二位时，
// 其总和必须第二个追加；两个次序的输入摘要必须不同。
func TestDependencyListOrderControlsAppend(t *testing.T) {
	s, _ := openTestStore(t)
	first := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{-2}})    // id 1，先完成，sum -2
	second := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1, 2}}) // id 2，后完成，sum 3
	waitStatus(t, s, first.ID, StatusSucceeded)
	waitStatus(t, s, second.ID, StatusSucceeded)

	// 列表次序 [2,1] 与完成先后 [1,2] 相反：实际输入必须是 [10 3 -2]。
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{10},
		Dependencies: []uint64{second.ID, first.ID},
	})
	d := waitStatus(t, s, down.ID, StatusSucceeded)
	if got := d.Archive.EffectiveValues; !reflect.DeepEqual(got, []int64{10, 3, -2}) {
		t.Fatalf("effective=%v, want [10 3 -2]（按列表次序，无视完成先后）", got)
	}
	if !reflect.DeepEqual(d.Dependencies, []uint64{2, 1}) {
		t.Fatalf("detail dependencies=%v, want [2 1]", d.Dependencies)
	}
	if !reflect.DeepEqual(d.Archive.Dependencies, []uint64{2, 1}) {
		t.Fatalf("archive dependencies=%v, want [2 1]", d.Archive.Dependencies)
	}

	// 反过来的列表产生不同实际输入（总和可能相同，但输入摘要必须不同）。
	rev := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{10},
		Dependencies: []uint64{first.ID, second.ID},
	})
	r := waitStatus(t, s, rev.ID, StatusSucceeded)
	if got := r.Archive.EffectiveValues; !reflect.DeepEqual(got, []int64{10, -2, 3}) {
		t.Fatalf("effective=%v, want [10 -2 3]", got)
	}
	if r.Archive.InputsDigest == d.Archive.InputsDigest ||
		r.Archive.ResultDigest == d.Archive.ResultDigest {
		t.Fatal("dependency list order must change the effective input digest")
	}
}

// TestSubmitRejectsInvalidDependencyList 覆盖零、重复、混用两种方式，
// 并确认拒绝不留记录、不占用幂等请求号。
func TestSubmitRejectsInvalidDependencyList(t *testing.T) {
	s, _ := openTestStore(t)
	up := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	waitStatus(t, s, up.ID, StatusSucceeded)

	cases := []struct {
		name string
		req  SubmitRequest
		want error
	}{
		{"zero", SubmitRequest{Submitter: "a", RequestID: "z", Values: []int64{1}, Dependencies: []uint64{up.ID, 0}}, ErrInvalidDependencyList},
		{"only zero", SubmitRequest{Submitter: "a", Values: []int64{1}, Dependencies: []uint64{0}}, ErrInvalidDependencyList},
		{"duplicate", SubmitRequest{Submitter: "a", Values: []int64{1}, Dependencies: []uint64{up.ID, up.ID}}, ErrInvalidDependencyList},
		{"zero and duplicate", SubmitRequest{Submitter: "a", Values: []int64{1}, Dependencies: []uint64{0, up.ID, 0, up.ID}}, ErrInvalidDependencyList},
		{"mixed forms", SubmitRequest{Submitter: "a", Values: []int64{1}, HasDependency: true, DependencyID: up.ID, Dependencies: []uint64{up.ID}}, ErrInvalidDependencyList},
		{"missing in list", SubmitRequest{Submitter: "a", Values: []int64{1}, Dependencies: []uint64{up.ID, 4242}}, ErrDependencyNotFound},
	}
	for _, c := range cases {
		_, err := s.Submit(c.req)
		if !errors.Is(err, c.want) {
			t.Fatalf("%s: err=%v, want %v", c.name, err, c.want)
		}
		if c.want == ErrInvalidDependencyList && !strings.Contains(err.Error(), "作业号") {
			t.Fatalf("%s: error must name invalid/duplicate ids: %q", c.name, err.Error())
		}
	}
	// 所有被拒绝的提交都不留下记录（只有 up 一条）。
	if got, _ := s.List("a", time.Time{}, time.Time{}); len(got) != 1 {
		t.Fatalf("rejected submits left records: %d", len(got))
	}
	// 幂等请求号未被占用：同一请求号随后可正常提交。
	j, err := s.Submit(SubmitRequest{Submitter: "a", RequestID: "z", Values: []int64{1}})
	if err != nil {
		t.Fatalf("request id must not be consumed by rejected submit: %v", err)
	}
	waitStatus(t, s, j.ID, StatusSucceeded)

	// 空列表沿用无依赖行为；空列表 + 单依赖方式沿用单依赖行为。
	empty := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{5}, Dependencies: nil})
	e := waitStatus(t, s, empty.ID, StatusSucceeded)
	if len(e.Dependencies) != 0 || e.Archive.Sum != 5 {
		t.Fatalf("empty list must behave like no dependency: %+v", e)
	}
	single := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{5},
		HasDependency: true, DependencyID: up.ID, Dependencies: []uint64{},
	})
	sg := waitStatus(t, s, single.ID, StatusSucceeded)
	if !reflect.DeepEqual(sg.Dependencies, []uint64{up.ID}) {
		t.Fatalf("single dependency normalized list=%v", sg.Dependencies)
	}
}

// TestPendingDependenciesListAndSlotTransition 检查排队详情按提交次序列出
// 未成功归档的直接上游；全部可用后原因转为等待计算位置。
func TestPendingDependenciesListAndSlotTransition(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1, 2)

	a := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}}) // id 1 运行中
	waitStarted(t, started, a.ID)
	b := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{2}}) // id 2 排队（放行后也会停住）
	// 列表次序 [2,1]：两个上游都未成功，详情必须按该次序列出。
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{10},
		Dependencies: []uint64{b.ID, a.ID},
	})
	j, _ := s.Get(down.ID)
	if j.WaitReason != WaitDependency || !reflect.DeepEqual(j.PendingDependencies, []uint64{2, 1}) {
		t.Fatalf("pending=%v reason=%q, want [2 1]/WaitDependency", j.PendingDependencies, j.WaitReason)
	}

	release(a.ID) // 1 先成功；2 仍被门控住，此时只剩 2 未就绪
	waitStatus(t, s, a.ID, StatusSucceeded)
	deadline := time.Now().Add(5 * time.Second)
	for {
		j, _ = s.Get(down.ID)
		if reflect.DeepEqual(j.PendingDependencies, []uint64{2}) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending after first upstream ready: %v", j.PendingDependencies)
		}
		time.Sleep(time.Millisecond)
	}
	release(b.ID)
	waitStatus(t, s, b.ID, StatusSucceeded)
	done := waitStatus(t, s, down.ID, StatusSucceeded)
	if !reflect.DeepEqual(done.Archive.EffectiveValues, []int64{10, 2, 1}) {
		t.Fatalf("effective=%v, want [10 2 1]（列表次序 2 然后 1）", done.Archive.EffectiveValues)
	}
}

// TestMultiDependencyImmediateFailurePicksFirstBlocked 提交时多个上游已失败：
// 立即失败，直接阻断者选列表最靠前者，并指出阻断链条根因；后来的状态变化
// 不能改写已确定的原因。
func TestMultiDependencyImmediateFailurePicksFirstBlocked(t *testing.T) {
	s, _ := openTestStore(t)
	big := int64(3037000500)
	root := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{big}}) // id 1 失败（根因）
	waitStatus(t, s, root.ID, StatusFailed)
	mid := mustSubmit(t, s, SubmitRequest{ // id 2 因 id 1 失败，blocker=1
		Submitter: "a", Values: []int64{1},
		HasDependency: true, DependencyID: root.ID,
	})
	other := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{-7}}) // id 3 成功
	waitStatus(t, s, mid.ID, StatusFailed)
	waitStatus(t, s, other.ID, StatusSucceeded)

	// 列表 [3,2,1]：3 已成功，2、1 都已失败，直接阻断者取列表最靠前的 2，
	// 根因沿链条追溯到 1。
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{10},
		Dependencies: []uint64{other.ID, mid.ID, root.ID},
	})
	d := waitStatus(t, s, down.ID, StatusFailed)
	if d.BlockerID != root.ID {
		t.Fatalf("blocker=%d, want root %d", d.BlockerID, root.ID)
	}
	if !strings.Contains(d.FailureReason, "作业 2") {
		t.Fatalf("reason must name the direct upstream 2: %q", d.FailureReason)
	}
	if !strings.Contains(d.FailureReason, "第 2/3 项") {
		t.Fatalf("reason must give the list position: %q", d.FailureReason)
	}
	if !strings.Contains(d.FailureReason, "根因") || !strings.Contains(d.FailureReason, "作业 1") {
		t.Fatalf("reason must identify root cause 1: %q", d.FailureReason)
	}
	if d.Archive != nil {
		t.Fatal("failed job must not carry success archive")
	}

	// 换个列表次序 [1,2]：直接阻断者应是列表最靠前的 1（根因也是 1）。
	down2 := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{10},
		Dependencies: []uint64{root.ID, mid.ID},
	})
	d2 := waitStatus(t, s, down2.ID, StatusFailed)
	if d2.BlockerID != 1 || !strings.Contains(d2.FailureReason, "第 1/2 项") {
		t.Fatalf("down2 blocker=%d reason=%q, want direct blocker 1 at position 1/2", d2.BlockerID, d2.FailureReason)
	}
}

// TestMultiDependencyFailureFreezesReason 等待期间一个上游（非列表最前者）
// 先失败：不必等待其余上游，本作业立即失败；之后另一个上游成功，原因不变。
func TestMultiDependencyFailureFreezesReason(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)

	running := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}}) // id 1 运行中
	waitStarted(t, started, running.ID)
	queued := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{2}}) // id 2 排队
	down := mustSubmit(t, s, SubmitRequest{                                       // id 3 等 [1,2]
		Submitter: "a", Values: []int64{10},
		Dependencies: []uint64{running.ID, queued.ID},
	})
	independent := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{9}}) // id 4

	// id 1 仍在运行时取消排队的 id 2：id 3 必须立刻以 id 2 为阻断者失败，
	// 不等待 id 1，也不挡住 id 4。
	if _, err := s.Cancel(queued.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	d := waitStatus(t, s, down.ID, StatusFailed)
	if d.BlockerID != queued.ID || !strings.Contains(d.FailureReason, "作业 2") ||
		!strings.Contains(d.FailureReason, "取消") || !strings.Contains(d.FailureReason, "第 2/2 项") {
		t.Fatalf("down reason=%q blocker=%d, want direct blocker 2 (canceled, position 2/2)", d.FailureReason, d.BlockerID)
	}
	frozen := d.FailureReason

	release(running.ID)
	waitStatus(t, s, running.ID, StatusSucceeded)
	ind := waitStatus(t, s, independent.ID, StatusSucceeded)
	if ind.Archive.Sum != 9 {
		t.Fatalf("independent job blocked behind multi-dependency waiter: sum=%d", ind.Archive.Sum)
	}
	again, _ := s.Get(down.ID)
	if again.Status != StatusFailed || again.FailureReason != frozen || again.BlockerID != queued.ID {
		t.Fatalf("reason rewritten after later upstream success: %q (was %q)", again.FailureReason, frozen)
	}
}

// TestMultiDependencyCascadeToDownstream 一个上游失败，其作业及更下游立即失败，
// 根因沿多依赖链条传递。
func TestMultiDependencyCascadeToDownstream(t *testing.T) {
	s, _ := openTestStore(t)
	big := int64(3037000500)
	root := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{big}}) // id 1 失败
	waitStatus(t, s, root.ID, StatusFailed)
	up := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}, Dependencies: []uint64{root.ID}}) // id 2
	mid := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{2}, Dependencies: []uint64{up.ID}})  // id 3 等 id 2
	far := waitStatus(t, s, mid.ID, StatusFailed)
	if far.BlockerID != root.ID {
		t.Fatalf("mid blocker=%d want root 1", far.BlockerID)
	}
}

// TestCancelMergeJobOnlyAffectsItsDownstream 取消一个等待多个上游的合并作业：
// 只取消它自己及其下游，其余上游照常成功。
func TestCancelMergeJobOnlyAffectsItsDownstream(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)

	holder := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{0}}) // id 1 占住位置
	waitStarted(t, started, holder.ID)
	upA := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}}) // id 2
	upB := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{2}}) // id 3
	merge := mustSubmit(t, s, SubmitRequest{                                   // id 4 等 [2,3]
		Submitter: "a", Values: []int64{10},
		Dependencies: []uint64{upA.ID, upB.ID},
	})
	down := mustSubmit(t, s, SubmitRequest{ // id 5 等 id 4
		Submitter: "a", Values: []int64{20},
		Dependencies: []uint64{merge.ID},
	})

	if _, err := s.Cancel(merge.ID); err != nil {
		t.Fatalf("cancel merge: %v", err)
	}
	d := waitStatus(t, s, down.ID, StatusFailed)
	if d.BlockerID != merge.ID {
		t.Fatalf("downstream blocker=%d want %d", d.BlockerID, merge.ID)
	}
	// 两个上游都还在排队，未被连带取消。
	if g, _ := s.Get(upA.ID); g.Status != StatusQueued {
		t.Fatalf("upstream A status=%s, want queued", g.Status)
	}
	if g, _ := s.Get(upB.ID); g.Status != StatusQueued {
		t.Fatalf("upstream B status=%s, want queued", g.Status)
	}

	release(holder.ID)
	a := waitStatus(t, s, upA.ID, StatusSucceeded)
	b := waitStatus(t, s, upB.ID, StatusSucceeded)
	if a.Archive.Sum != 1 || b.Archive.Sum != 2 {
		t.Fatalf("upstreams must still complete: A=%d B=%d", a.Archive.Sum, b.Archive.Sum)
	}
	mg, _ := s.Get(merge.ID)
	if mg.Status != StatusCanceled || mg.Archive != nil {
		t.Fatalf("merge job: status=%s archive=%v", mg.Status, mg.Archive)
	}
}

// TestMultiDependencyIdempotency 列表内容与次序都属于提交内容。
func TestMultiDependencyIdempotency(t *testing.T) {
	s, _ := openTestStore(t)
	up1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	up2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{2}})
	waitStatus(t, s, up1.ID, StatusSucceeded)
	waitStatus(t, s, up2.ID, StatusSucceeded)

	req := SubmitRequest{
		Submitter: "a", RequestID: "m", Values: []int64{10}, Seed: 5,
		Dependencies: []uint64{up1.ID, up2.ID},
	}
	first := mustSubmit(t, s, req)
	waitStatus(t, s, first.ID, StatusSucceeded)

	// 完全相同（列表内容与次序一致）→ 原作业。
	if j, err := s.Submit(req); err != nil || j.ID != first.ID {
		t.Fatalf("identical replay: id=%d err=%v", idOrZero(j), err)
	}
	// 列表次序变化 → 冲突。
	if j, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "m", Values: []int64{10}, Seed: 5,
		Dependencies: []uint64{up2.ID, up1.ID},
	}); !errors.Is(err, ErrIdempotencyConflict) || j == nil || j.ID != first.ID {
		t.Fatalf("reordered list: j=%+v err=%v", j, err)
	}
	// 列表成员变化 → 冲突。
	if _, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "m", Values: []int64{10}, Seed: 5,
		Dependencies: []uint64{up1.ID},
	}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("shortened list err=%v, want conflict", err)
	}
	// 单依赖方式与只含同一作业号的列表等价（列表 → 单依赖）。
	one := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "one", Values: []int64{10},
		Dependencies: []uint64{up1.ID},
	})
	waitStatus(t, s, one.ID, StatusSucceeded)
	if j, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "one", Values: []int64{10},
		HasDependency: true, DependencyID: up1.ID,
	}); err != nil || j.ID != one.ID {
		t.Fatalf("single-dep form must equal one-element list: id=%d err=%v", idOrZero(j), err)
	}
	// 反方向：先用单依赖提交，再用等价列表重放。
	two := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "two", Values: []int64{10},
		HasDependency: true, DependencyID: up2.ID,
	})
	waitStatus(t, s, two.ID, StatusSucceeded)
	if j, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "two", Values: []int64{10},
		Dependencies: []uint64{up2.ID},
	}); err != nil || j.ID != two.ID {
		t.Fatalf("list replay of single-dep: id=%d err=%v", idOrZero(j), err)
	}
	// 不同作业号不等价。
	if _, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "two", Values: []int64{10},
		Dependencies: []uint64{up1.ID},
	}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different dependency id must conflict: %v", err)
	}
	// 原记录结果不变。
	g := waitStatus(t, s, first.ID, StatusSucceeded)
	if !reflect.DeepEqual(g.Archive.EffectiveValues, []int64{10, 1, 2}) {
		t.Fatalf("original record altered: %v", g.Archive.EffectiveValues)
	}
}

func idOrZero(j *Job) uint64 {
	if j == nil {
		return 0
	}
	return j.ID
}

// TestConcurrentDuplicateMultiDependencySubmit 并发重复提交多依赖作业只建一个。
func TestConcurrentDuplicateMultiDependencySubmit(t *testing.T) {
	s, _ := openTestStore(t)
	up := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	waitStatus(t, s, up.ID, StatusSucceeded)

	const n = 24
	var wg sync.WaitGroup
	ids := make([]uint64, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			j, err := s.Submit(SubmitRequest{
				Submitter: "a", RequestID: "race-multi", Values: []int64{10},
				Dependencies: []uint64{up.ID},
			})
			if err != nil {
				t.Errorf("submit %d: %v", i, err)
				return
			}
			ids[i] = j.ID
		}(i)
	}
	close(start)
	wg.Wait()
	uniq := map[uint64]struct{}{}
	for _, id := range ids {
		uniq[id] = struct{}{}
	}
	if len(uniq) != 1 {
		t.Fatalf("created %d jobs: %v", len(uniq), ids)
	}
	waitStatus(t, s, ids[0], StatusSucceeded)
}

// TestReturnedDependencyListsAreCopies 修改返回的列表不得影响记录。
func TestReturnedDependencyListsAreCopies(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)
	up := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	waitStarted(t, started, up.ID)
	in := []uint64{up.ID}
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{10}, Dependencies: in,
	})
	in[0] = 999 // 调用方事后修改入参切片
	v1, _ := s.Get(down.ID)
	v1.Dependencies[0] = 998
	v1.PendingDependencies[0] = 997
	release(up.ID)
	done := waitStatus(t, s, down.ID, StatusSucceeded)
	done.Archive.Dependencies[0] = 996
	done.Archive.EffectiveValues[0] = 995
	g, _ := s.Get(down.ID)
	if !reflect.DeepEqual(g.Dependencies, []uint64{1}) ||
		!reflect.DeepEqual(g.Archive.Dependencies, []uint64{1}) {
		t.Fatalf("stored dependency list mutated: detail=%v archive=%v", g.Dependencies, g.Archive.Dependencies)
	}
	if g.Archive.EffectiveValues[0] != 10 {
		t.Fatalf("stored effective values mutated: %v", g.Archive.EffectiveValues)
	}
}

// TestReopenPreservesMultiDependencies 关闭重开后多依赖关系与幂等关系保留，
// 排队作业按恢复后的上游状态继续等待并完成，实际输入次序不变。
func TestReopenPreservesMultiDependencies(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	started, block, release, _, _ := gateCompute(t, s)
	block(1)
	up1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{1, 2}}) // id 1 sum 3
	waitStarted(t, started, up1.ID)
	up2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{-2}}) // id 2
	down := mustSubmit(t, s, SubmitRequest{                                                      // id 3 等 [2,1]
		Submitter: "a", RequestID: "d", Values: []int64{10},
		Dependencies: []uint64{up2.ID, up1.ID},
	})
	if _, err := s.Cancel(up2.ID); err != nil { // id 2 取消：重开后 id 3 应立即失败
		t.Fatal(err)
	}
	waitStatus(t, s, down.ID, StatusFailed)
	release(up1.ID)
	waitStatus(t, s, up1.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	g, _ := s2.Get(down.ID)
	if g.Status != StatusFailed || g.BlockerID != up2.ID {
		t.Fatalf("down after reopen: status=%s blocker=%d", g.Status, g.BlockerID)
	}
	if !reflect.DeepEqual(g.Dependencies, []uint64{2, 1}) {
		t.Fatalf("dependency order lost across reopen: %v", g.Dependencies)
	}
	// 幂等关系保留：相同内容重放返回原作业；改次序冲突。
	if j, err := s2.Submit(SubmitRequest{
		Submitter: "a", RequestID: "d", Values: []int64{10},
		Dependencies: []uint64{up2.ID, up1.ID},
	}); err != nil || j.ID != down.ID {
		t.Fatalf("idempotency lost across reopen: id=%d err=%v", idOrZero(j), err)
	}
	if _, err := s2.Submit(SubmitRequest{
		Submitter: "a", RequestID: "d", Values: []int64{10},
		Dependencies: []uint64{up1.ID, up2.ID},
	}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("reordered list after reopen err=%v", err)
	}

	// 新的排队多依赖作业在重开后继续等待并完成，次序正确。
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s3.Close() }()
	fresh := mustSubmit(t, s3, SubmitRequest{
		Submitter: "a", Values: []int64{10},
		Dependencies: []uint64{up1.ID},
	})
	f := waitStatus(t, s3, fresh.ID, StatusSucceeded)
	if !reflect.DeepEqual(f.Archive.EffectiveValues, []int64{10, 3}) {
		t.Fatalf("resumed effective input=%v, want [10 3]", f.Archive.EffectiveValues)
	}
}

// TestReopenInterruptedMultiUpstreamBlocksDownstream 运行中被中断的作业继续
// 阻断未结束的多依赖下游；多个直接上游都失败时按列表次序选择，根因追溯到
// 被中断的作业。
func TestReopenInterruptedMultiUpstreamBlocksDownstream(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{ // id 1：上次关闭时仍在运行
		id: 1, submitter: "a", requestID: "r1",
		values:    []int64{1},
		queuedAt:  base,
		startedAt: base.Add(time.Second),
		status:    StatusRunning,
	})
	writeSyntheticRecord(t, dir, &storedJob{ // id 2：依赖 1
		id: 2, submitter: "a", requestID: "r2",
		values:       []int64{2},
		dependencies: []uint64{1},
		queuedAt:     base.Add(2 * time.Second),
		status:       StatusQueued,
	})
	writeSyntheticRecord(t, dir, &storedJob{ // id 3：多依赖 [2,1]
		id: 3, submitter: "a", requestID: "r3",
		values:       []int64{3},
		dependencies: []uint64{2, 1},
		queuedAt:     base.Add(3 * time.Second),
		status:       StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	j1, _ := s.Get(1)
	if j1.Status != StatusFailed {
		t.Fatalf("interrupted upstream status=%s", j1.Status)
	}
	j2 := waitStatus(t, s, 2, StatusFailed)
	j3 := waitStatus(t, s, 3, StatusFailed)
	if j2.BlockerID != 1 {
		t.Fatalf("j2 blocker=%d want 1", j2.BlockerID)
	}
	// id 3 列表中 id 2 最靠前（直接阻断者），根因追溯到被中断的 id 1。
	if j3.BlockerID != 1 || !strings.Contains(j3.FailureReason, "作业 2") ||
		!strings.Contains(j3.FailureReason, "作业 1") {
		t.Fatalf("j3 blocker=%d reason=%q, want root 1 via direct 2", j3.BlockerID, j3.FailureReason)
	}
}

// TestLegacyRecordsStillReadable 旧格式（无 dependencies 字段、旧归档无该字段）
// 记录仍可读取：单依赖关系被还原，已有成功归档的摘要与校验值不变。
func TestLegacyRecordsStillReadable(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)

	// id 1：旧格式成功归档（无依赖）。
	eff1 := []int64{1, 2, 3}
	a1 := &Archive{
		JobID: 1, Submitter: "a", RequestID: "leg-1", Seed: 4,
		Values:          append([]int64(nil), eff1...),
		EffectiveValues: append([]int64(nil), eff1...),
		InputsDigest:    inputsDigestHex(eff1, 4),
		Sum:             6, SumOfSquares: 14,
		ResultDigest: resultDigestHex(eff1, 4, 6, 14),
		CompletedAt:  base.Add(time.Minute),
	}
	a1.Log = buildLog(eff1, 4, 6, 14)
	a1.Checksum = checksumHex(eff1, 4, 6, 14, a1.Log, a1.ResultDigest)
	wantChecksum := a1.Checksum
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "leg-1", seed: 4,
		values:   append([]int64(nil), eff1...),
		queuedAt: base, finishedAt: a1.CompletedAt,
		status:          StatusSucceeded,
		effectiveValues: append([]int64(nil), eff1...),
		archive:         a1,
	})

	// id 2：旧格式排队单依赖记录（has_dependency=true，无 dependencies 字段）。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "leg-2",
		values:        []int64{10},
		hasDependency: true, dependencyID: 1,
		// 注意：dependencies 刻意留空，encodeRecord 不会写出该字段。
		queuedAt: base.Add(2 * time.Second),
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	g1, _ := s.Get(1)
	if g1.Status != StatusSucceeded || g1.Archive == nil || g1.Archive.Checksum != wantChecksum {
		t.Fatalf("legacy success archive not intact: %+v checksum want %s", g1, wantChecksum)
	}
	g2 := waitStatus(t, s, 2, StatusSucceeded)
	if !reflect.DeepEqual(g2.Dependencies, []uint64{1}) {
		t.Fatalf("legacy single dependency not restored: %v", g2.Dependencies)
	}
	if !reflect.DeepEqual(g2.Archive.EffectiveValues, []int64{10, 6}) {
		t.Fatalf("legacy dependency effective=%v, want [10 6]", g2.Archive.EffectiveValues)
	}
}
