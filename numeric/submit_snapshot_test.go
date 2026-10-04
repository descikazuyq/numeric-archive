package numeric

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// 参数快照回归：Submit 在接受时即保存整数序列与有序依赖列表的拷贝，
// 调用方随后改写自己持有的切片（改值、调换、替换作业号）都不能影响
// 已接受作业的保存参数、等待的上游、最终计算内容与成功归档。

func equalInt64s(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalUint64s(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// 已接受但因上游未完成而排队的作业：调用方改写原整数序列、并调换/替换
// 原依赖列表中的作业号（替换进去的号是真实存在的作业）之后，按号查询仍应
// 看到接受时的原始次序；上游全部成功归档后，本作业仍按原列表顺序追加各上游
// 总和完成计算，归档对应原始参数，不混入改写后的数据。
func TestSubmitSnapshotsParamsWhileQueuedBehindDependencies(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)

	head := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}}) // id 1 占住计算位置
	waitStarted(t, started, head.ID)
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{2}}) // id 2，总和 2
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{3}}) // id 3，总和 3

	values := []int64{10, 20}
	deps := []uint64{u1.ID, u2.ID}
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "snap", Values: values, Seed: 7,
		Dependencies: deps,
	})

	// 接受之后调用方改写自己持有的切片：改整数、调换并替换依赖作业号。
	// head.ID 真实存在，但也不能让 down 转而等待它或采用它的结果。
	values[0] = -999
	values[1] = 888
	deps[0] = head.ID
	deps[1] = u1.ID

	// 排队期间按号查询：原始整数次序、依赖次序与待完成上游列表都保持接受时的内容。
	q, err := s.Get(down.ID)
	if err != nil {
		t.Fatal(err)
	}
	if q.Status != StatusQueued || q.WaitReason != WaitDependency {
		t.Fatalf("status=%s reason=%q, want queued/WaitDependency", q.Status, q.WaitReason)
	}
	if !equalInt64s(q.Values, []int64{10, 20}) {
		t.Fatalf("values=%v, want [10 20]（接受时的快照）", q.Values)
	}
	if !equalUint64s(q.Dependencies, []uint64{u1.ID, u2.ID}) {
		t.Fatalf("dependencies=%v, want [%d %d]", q.Dependencies, u1.ID, u2.ID)
	}
	if !equalUint64s(q.PendingDependencies, []uint64{u1.ID, u2.ID}) {
		t.Fatalf("pending=%v, want [%d %d]（替换进去的 %d 不能成为等待对象）",
			q.PendingDependencies, u1.ID, u2.ID, head.ID)
	}

	// 放行后原先指定的上游依次成功，down 按原列表顺序追加各上游总和。
	release(head.ID)
	waitStatus(t, s, head.ID, StatusSucceeded)
	waitStatus(t, s, u1.ID, StatusSucceeded)
	waitStatus(t, s, u2.ID, StatusSucceeded)
	d := waitStatus(t, s, down.ID, StatusSucceeded)

	wantEffective := []int64{10, 20, 2, 3} // 原始序列 + u1、u2 的总和，不含 head 的总和
	if !equalInt64s(d.EffectiveValues, wantEffective) {
		t.Fatalf("effective=%v, want %v", d.EffectiveValues, wantEffective)
	}
	a := d.Archive
	if a == nil {
		t.Fatal("succeeded job has no archive")
	}
	if !equalInt64s(a.Values, []int64{10, 20}) || !equalInt64s(a.EffectiveValues, wantEffective) {
		t.Fatalf("archive values=%v effective=%v, want [10 20] / %v", a.Values, a.EffectiveValues, wantEffective)
	}
	if !equalUint64s(a.Dependencies, []uint64{u1.ID, u2.ID}) {
		t.Fatalf("archive dependencies=%v, want [%d %d]", a.Dependencies, u1.ID, u2.ID)
	}
	if a.Sum != 35 || a.SumOfSquares != 513 {
		t.Fatalf("sum=%d sq=%d, want 35,513", a.Sum, a.SumOfSquares)
	}
	// 摘要、日志与校验值对应接受时的输入与种子，与改写后的数据无关。
	if a.InputsDigest != inputsDigestHex(wantEffective, 7) ||
		a.ResultDigest != resultDigestHex(wantEffective, 7, 35, 513) ||
		a.Log != buildLog(wantEffective, 7, 35, 513) ||
		a.Checksum != checksumHex(wantEffective, 7, 35, 513, a.Log, a.ResultDigest) {
		t.Fatal("archive digest/log/checksum must correspond to the accepted-time inputs")
	}
	if strings.Contains(a.Log, "-999") || strings.Contains(a.Log, "888") {
		t.Fatalf("log must not contain caller-mutated values: %q", a.Log)
	}
}

// 只交换整数位置、总和与平方和恰好不变的情形：原始次序与实际输入不能被交换，
// 摘要与校验值仍对应接受时的输入次序（摘要本身对次序敏感）。
func TestSubmitSnapshotsPermutedValuesWithSameSum(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)

	head := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	waitStarted(t, started, head.ID)

	values := []int64{3, -1, 2}
	j := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: values, Seed: 5})
	// 交换为首尾互换：总和 4、平方和 14 不变，但次序不同。
	values[0] = 2
	values[2] = 3

	q, _ := s.Get(j.ID)
	if !equalInt64s(q.Values, []int64{3, -1, 2}) {
		t.Fatalf("values=%v, want accepted-time order [3 -1 2]", q.Values)
	}

	release(head.ID)
	waitStatus(t, s, head.ID, StatusSucceeded)
	d := waitStatus(t, s, j.ID, StatusSucceeded)

	orig := []int64{3, -1, 2}
	permuted := []int64{2, -1, 3}
	if !equalInt64s(d.Archive.EffectiveValues, orig) {
		t.Fatalf("effective=%v, want %v（次序不能被交换）", d.Archive.EffectiveValues, orig)
	}
	if d.Archive.Sum != 4 || d.Archive.SumOfSquares != 14 {
		t.Fatalf("sum=%d sq=%d, want 4,14", d.Archive.Sum, d.Archive.SumOfSquares)
	}
	if d.Archive.InputsDigest != inputsDigestHex(orig, 5) ||
		d.Archive.ResultDigest != resultDigestHex(orig, 5, 4, 14) ||
		d.Archive.Checksum != checksumHex(orig, 5, 4, 14, d.Archive.Log, d.Archive.ResultDigest) {
		t.Fatal("digests/checksum must correspond to the accepted-time input order")
	}
	if d.Archive.InputsDigest == inputsDigestHex(permuted, 5) {
		t.Fatal("inputs digest must be order-sensitive: matched the permuted sequence")
	}
	if !strings.Contains(d.Archive.Log, "input[0]=3\n") {
		t.Fatalf("log must record accepted-time order: %q", d.Archive.Log)
	}
}

// 无依赖的合法非空序列同样遵守快照规则：不需要等待上游也不能遗漏保障。
func TestSubmitSnapshotsValuesWithoutDependencies(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)

	head := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{9}})
	waitStarted(t, started, head.ID)

	values := []int64{4, -1}
	j := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: values, Seed: 2})
	values[0] = 100
	values[1] = 200

	q, _ := s.Get(j.ID)
	if !equalInt64s(q.Values, []int64{4, -1}) {
		t.Fatalf("values=%v, want [4 -1]", q.Values)
	}

	release(head.ID)
	waitStatus(t, s, head.ID, StatusSucceeded)
	d := waitStatus(t, s, j.ID, StatusSucceeded)

	want := []int64{4, -1}
	if !equalInt64s(d.Archive.EffectiveValues, want) {
		t.Fatalf("effective=%v, want %v", d.Archive.EffectiveValues, want)
	}
	if d.Archive.Sum != 3 || d.Archive.SumOfSquares != 17 {
		t.Fatalf("sum=%d sq=%d, want 3,17", d.Archive.Sum, d.Archive.SumOfSquares)
	}
	if d.Archive.InputsDigest != inputsDigestHex(want, 2) ||
		d.Archive.Checksum != checksumHex(want, 2, 3, 17, d.Archive.Log, d.Archive.ResultDigest) {
		t.Fatal("no-dependency archive must correspond to accepted-time values")
	}
}

// 快照规则与幂等行为联动：用接受时的原始内容重放仍返回原作业；
// 用改写后的内容（即使总和、平方和相同）重放则按既有规则返回幂等冲突，
// 原作业参数与成功归档保持不变，两种请求都不新增作业。
func TestSubmitSnapshotIdempotencyReplayAndConflict(t *testing.T) {
	s, _ := openTestStore(t)
	up := mustSubmit(t, s, SubmitRequest{Submitter: "up", Values: []int64{5}})  // 总和 5
	up2 := mustSubmit(t, s, SubmitRequest{Submitter: "up", Values: []int64{7}}) // 另一个真实存在的作业
	waitStatus(t, s, up.ID, StatusSucceeded)
	waitStatus(t, s, up2.ID, StatusSucceeded)

	values := []int64{1, 2}
	deps := []uint64{up.ID}
	orig := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "k", Values: values, Seed: 3,
		Dependencies: deps,
	})
	// 接受后调用方改写切片：整数换序（总和/平方和不变）、把依赖替换成另一个
	// 真实存在的作业号。
	values[0] = 2
	values[1] = 1
	deps[0] = up2.ID

	// 用接受时的原始内容重放 → 返回原作业及其当前状态。
	replay := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "k", Values: []int64{1, 2}, Seed: 3,
		Dependencies: []uint64{up.ID},
	})
	if replay.ID != orig.ID {
		t.Fatalf("replay with original content got id=%d, want %d", replay.ID, orig.ID)
	}

	// 用改写后的内容重放 → 幂等冲突并返回原作业；完整整数次序与依赖次序
	// 都参与比较，不能因计算结果相同而把改写后的请求当成原请求。
	conflicts := []SubmitRequest{
		{Submitter: "a", RequestID: "k", Values: []int64{2, 1}, Seed: 3, Dependencies: []uint64{up.ID}}, // 整数换序，和与平方和相同
		{Submitter: "a", RequestID: "k", Values: values, Seed: 3, Dependencies: deps},                   // 调用方改写后的切片
		{Submitter: "a", RequestID: "k", Values: []int64{1, 2}, Seed: 3},                                // 依赖被去掉
	}
	for i, c := range conflicts {
		j, err := s.Submit(c)
		if !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("conflict %d: err=%v, want ErrIdempotencyConflict", i, err)
		}
		if j == nil || j.ID != orig.ID {
			t.Fatalf("conflict %d: must also return original job, got %+v", i, j)
		}
	}

	// 两种请求都不新增作业。
	if got, _ := s.List("a", time.Time{}, time.Time{}); len(got) != 1 {
		t.Fatalf("replay/conflict created extra records: %d", len(got))
	}

	// 原作业的参数与成功归档保持接受时的内容：有效输入 [1, 2, 5]。
	d := waitStatus(t, s, orig.ID, StatusSucceeded)
	if !equalInt64s(d.Values, []int64{1, 2}) || !equalUint64s(d.Dependencies, []uint64{up.ID}) {
		t.Fatalf("original params altered: values=%v deps=%v", d.Values, d.Dependencies)
	}
	want := []int64{1, 2, 5}
	if !equalInt64s(d.Archive.EffectiveValues, want) {
		t.Fatalf("effective=%v, want %v", d.Archive.EffectiveValues, want)
	}
	if d.Archive.Sum != 8 || d.Archive.SumOfSquares != 30 {
		t.Fatalf("sum=%d sq=%d, want 8,30", d.Archive.Sum, d.Archive.SumOfSquares)
	}
	if d.Archive.InputsDigest != inputsDigestHex(want, 3) ||
		d.Archive.Checksum != checksumHex(want, 3, 8, 30, d.Archive.Log, d.Archive.ResultDigest) {
		t.Fatal("original archive must remain bound to the accepted-time inputs")
	}
}
