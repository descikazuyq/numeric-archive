package numeric

import (
	"errors"
	"testing"
	"time"
)

// 作业接受后调用方仍持有提交时给出的整数切片与依赖切片；这些切片后来被
// 改写（改值、交换次序、替换成确实存在的其他作业号）只能影响调用方自己的
// 数据，不能改变作业保存的参数、等待的上游与最终计算内容。
//
// 本文件围绕这项既有规则补充回归保障：Submit 在接受时即深拷贝两份切片
// （store.go 中 values/dependencies 的落盘与内存记录都来自独立副本），
// 幂等比较同样以保存的副本为准，而不是调用方手里那块随时会变的内存。

// assertInt64s 按位置比较两个整数序列。
func assertInt64s(t *testing.T, name string, got, want []int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s=%v, want %v", name, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s=%v, want %v（必须保持接受时的完整次序）", name, got, want)
		}
	}
}

// assertUint64s 按位置比较两个作业号序列。
func assertUint64s(t *testing.T, name string, got, want []uint64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s=%v, want %v", name, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s=%v, want %v（必须保持接受时的依赖次序）", name, got, want)
		}
	}
}

// 重点场景：作业已接受、但因上游尚未完成仍在排队。调用方在此期间改写原整数
// 序列、调换并替换依赖列表中的作业号后，按作业号查询仍应看到接受时的原始
// 次序，待完成的上游列表只反映原先指定的上游；替换进去的作业号即使确实存在，
// 也不能让本作业转而等待它。原上游全部成功后，本作业仍按原列表顺序追加各上游
// 的总和，结果未越界时正常完成。
func TestSubmitCopiesSlicesBeforeJobRuns(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)

	head := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}}) // id 1 占住计算位置
	waitStarted(t, started, head.ID)
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1, 2}})  // id 2，总和 3
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1, -3}}) // id 3，总和 -2
	// 替换依赖时使用的“确实存在”的作业：它绝不能因此变成 down 的上游。
	extra := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{77}}) // id 4，总和 77

	values := []int64{10, 20, -5}
	deps := []uint64{u1.ID, u2.ID}
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: values, Dependencies: deps,
	}) // id 5，排队等待 2、3

	// 接受返回后，调用方改写自己手里的两块内存：整数全部换掉，依赖列表
	// 既调换次序又把其中一个替换成已存在的 extra。
	values[0], values[1], values[2] = 999, -888, 0
	deps[0], deps[1] = extra.ID, u1.ID

	// 排队视图必须仍是接受时的内容：原整数次序、原依赖次序、原待完成上游。
	g, _ := s.Get(down.ID)
	if g.Status != StatusQueued || g.WaitReason != WaitDependency {
		t.Fatalf("status=%s reason=%q, want queued/WaitDependency", g.Status, g.WaitReason)
	}
	assertInt64s(t, "queued values", g.Values, []int64{10, 20, -5})
	assertUint64s(t, "queued dependencies", g.Dependencies, []uint64{u1.ID, u2.ID})
	assertUint64s(t, "pending dependencies", g.PendingDependencies, []uint64{u1.ID, u2.ID})

	// 排队期间用“接受时的内容”重放（即使复用的正是刚被改写又改回的同一切片）
	// 仍返回原作业及其当前状态，不新增作业。
	values[0], values[1], values[2] = 10, 20, -5
	deps[0], deps[1] = u1.ID, u2.ID
	replay := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: values, Dependencies: deps,
	})
	if replay.ID != down.ID || replay.Status != StatusQueued {
		t.Fatalf("equal-content replay while queued: id=%d status=%s, want %d queued",
			replay.ID, replay.Status, down.ID)
	}
	// 用调用方改写后的内容重放：既有幂等冲突，同时返回原作业，原作业仍排队、
	// 参数不变，也不新增记录。
	conflict, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "down",
		Values:       []int64{999, -888, 0},
		Dependencies: []uint64{extra.ID, u1.ID},
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("mutated replay err=%v, want ErrIdempotencyConflict", err)
	}
	if conflict == nil || conflict.ID != down.ID || conflict.Status != StatusQueued {
		t.Fatalf("conflict must return original queued job, got %+v", conflict)
	}
	assertUint64s(t, "dependencies after conflict", conflict.Dependencies, []uint64{u1.ID, u2.ID})
	assertUint64s(t, "pending after conflict", conflict.PendingDependencies, []uint64{u1.ID, u2.ID})
	if got, _ := s.List("a", time.Time{}, time.Time{}); len(got) != 5 {
		t.Fatalf("replay/conflict must not create jobs: list len=%d want 5", len(got))
	}

	// 原上游全部成功归档后，down 按原列表顺序 [u1,u2] 追加 3、-2 后计算；
	// extra 从未成为上游，其总和 77 不得混入。
	release(head.ID)
	waitStatus(t, s, u1.ID, StatusSucceeded)
	waitStatus(t, s, u2.ID, StatusSucceeded)
	waitStatus(t, s, extra.ID, StatusSucceeded)
	done := waitStatus(t, s, down.ID, StatusSucceeded)

	assertInt64s(t, "archive original values", done.Values, []int64{10, 20, -5})
	assertInt64s(t, "archive values copy", done.Archive.Values, []int64{10, 20, -5})
	assertUint64s(t, "archive dependencies", done.Dependencies, []uint64{u1.ID, u2.ID})
	assertUint64s(t, "archive dependency copy", done.Archive.Dependencies, []uint64{u1.ID, u2.ID})
	wantEffective := []int64{10, 20, -5, 3, -2}
	assertInt64s(t, "effective values", done.EffectiveValues, wantEffective)
	assertInt64s(t, "archive effective values", done.Archive.EffectiveValues, wantEffective)
	// 10+20-5+3-2 = 26；100+400+25+9+4 = 538。
	if done.Archive.Sum != 26 || done.Archive.SumOfSquares != 538 {
		t.Fatalf("sum=%d sq=%d, want 26,538", done.Archive.Sum, done.Archive.SumOfSquares)
	}
	for _, v := range done.Archive.EffectiveValues {
		if v == 77 {
			t.Fatalf("replacement job's sum leaked into effective input: %v", done.Archive.EffectiveValues)
		}
	}
	// 摘要、日志、校验值全部对应接受时的实际输入，而不是调用方改写后的数据。
	if done.Archive.InputsDigest != inputsDigestHex(wantEffective, 0) ||
		done.Archive.ResultDigest != resultDigestHex(wantEffective, 0, 26, 538) ||
		done.Archive.Log != buildLog(wantEffective, 0, 26, 538) ||
		done.Archive.Checksum != checksumHex(wantEffective, 0, 26, 538,
			done.Archive.Log, done.Archive.ResultDigest) {
		t.Fatal("archive digest/log/checksum must correspond to accepted inputs, not caller mutations")
	}
	// 成功后再用改写内容重放仍是冲突，原归档保持不变。
	if _, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "down",
		Values:       []int64{999, -888, 0},
		Dependencies: []uint64{extra.ID, u1.ID},
	}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("post-success mutated replay err=%v, want conflict", err)
	}
	again := waitStatus(t, s, down.ID, StatusSucceeded)
	assertInt64s(t, "values after post-success conflict", again.Archive.Values, []int64{10, 20, -5})
	assertInt64s(t, "effective after post-success conflict", again.Archive.EffectiveValues, wantEffective)
}

// 无依赖的合法非空序列同样在接受时拷贝：它不需要等待上游，不能因此漏掉
// 保障——排队视图与成功归档都只认接受时的序列。
func TestSubmitCopiesValuesWithoutDependency(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)
	head := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	waitStarted(t, started, head.ID)

	values := []int64{5, -2, 4}
	j := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: values})
	values[0], values[1], values[2] = 9, 9, 9 // 接受后立即改写调用方切片

	g, _ := s.Get(j.ID)
	if g.Status != StatusQueued || g.WaitReason != WaitSlot {
		t.Fatalf("status=%s reason=%q, want queued/WaitSlot", g.Status, g.WaitReason)
	}
	assertInt64s(t, "queued values", g.Values, []int64{5, -2, 4})

	release(head.ID)
	done := waitStatus(t, s, j.ID, StatusSucceeded)
	wantEffective := []int64{5, -2, 4}
	assertInt64s(t, "stored values", done.Values, wantEffective)
	assertInt64s(t, "archive values", done.Archive.Values, wantEffective)
	assertInt64s(t, "effective values", done.Archive.EffectiveValues, wantEffective)
	// 5-2+4 = 7；25+4+16 = 45。
	if done.Archive.Sum != 7 || done.Archive.SumOfSquares != 45 {
		t.Fatalf("sum=%d sq=%d, want 7,45", done.Archive.Sum, done.Archive.SumOfSquares)
	}
	if done.Archive.InputsDigest != inputsDigestHex(wantEffective, 0) {
		t.Fatal("inputs digest must correspond to accepted sequence")
	}
}

// 只交换整数位置、总和与平方和恰好不变的情形：[2,-3] 与 [-3,2] 的总和
// （-1）与平方和（13）完全相同，但原始次序、实际输入、摘要与校验值仍必须
// 对应接受时的输入，不能被调用方交换。
func TestSubmitValuesSwapWithIdenticalTotalsDoesNotChangeAcceptedInput(t *testing.T) {
	s, _ := openTestStore(t)
	values := []int64{2, -3}
	j := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "k", Values: values, Seed: 11})
	values[0], values[1] = -3, 2 // 调用方事后交换；总和、平方和不变

	done := waitStatus(t, s, j.ID, StatusSucceeded)
	want := []int64{2, -3}
	swapped := []int64{-3, 2}
	assertInt64s(t, "stored values", done.Values, want)
	assertInt64s(t, "effective values", done.Archive.EffectiveValues, want)
	if done.Archive.Sum != -1 || done.Archive.SumOfSquares != 13 {
		t.Fatalf("sum=%d sq=%d, want -1,13", done.Archive.Sum, done.Archive.SumOfSquares)
	}
	// 数值结果相同，但摘要、日志、校验值绑定完整次序：必须对应接受时的 [2,-3]，
	// 而不是交换后的 [-3,2]。
	if done.Archive.InputsDigest != inputsDigestHex(want, 11) ||
		done.Archive.InputsDigest == inputsDigestHex(swapped, 11) {
		t.Fatal("inputs digest must encode accepted order, not swapped order")
	}
	if done.Archive.ResultDigest != resultDigestHex(want, 11, -1, 13) ||
		done.Archive.ResultDigest == resultDigestHex(swapped, 11, -1, 13) {
		t.Fatal("result digest must encode accepted order despite identical totals")
	}
	wantLog := buildLog(want, 11, -1, 13)
	if done.Archive.Log != wantLog || done.Archive.Log == buildLog(swapped, 11, -1, 13) {
		t.Fatalf("log=%q must list accepted order", done.Archive.Log)
	}
	if done.Archive.Checksum != checksumHex(want, 11, -1, 13, wantLog, done.Archive.ResultDigest) {
		t.Fatal("checksum must correspond to accepted-order inputs")
	}
}

// 幂等比较的是保存下来的完整整数次序与依赖次序：用接受时的内容重放返回原
// 作业；用改写后的内容重放一律冲突——即使改写只交换次序、计算结果（总和、
// 平方和乃至实际输入数值）恰好完全相同。两种请求都不新增作业，原作业的参数
// 与成功归档保持不变。
func TestIdempotencyComparesAcceptedOrderNotComputedResults(t *testing.T) {
	s, _ := openTestStore(t)
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{1, 2}}) // 总和 3
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{3}})    // 总和 3
	waitStatus(t, s, u1.ID, StatusSucceeded)
	waitStatus(t, s, u2.ID, StatusSucceeded)

	values := []int64{7, -2} // 交换后 [-2,7] 与原次序总和、平方和相同
	deps := []uint64{u1.ID, u2.ID}
	orig := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "k", Values: values, Dependencies: deps,
	})
	done := waitStatus(t, s, orig.ID, StatusSucceeded)
	// 实际输入 [7,-2,3,3]：总和 11，平方和 71；交换整数或调换两个总和相同的
	// 上游都不会改变这些数值，但次序属于提交内容。
	wantEffective := []int64{7, -2, 3, 3}
	assertInt64s(t, "accepted effective", done.Archive.EffectiveValues, wantEffective)
	if done.Archive.Sum != 11 || done.Archive.SumOfSquares != 71 {
		t.Fatalf("sum=%d sq=%d, want 11,71", done.Archive.Sum, done.Archive.SumOfSquares)
	}

	// 调用方事后改写自己持有的切片，不影响原作业。
	values[0], values[1] = -2, 7
	deps[0], deps[1] = u2.ID, u1.ID

	// 接受时内容的重放（全新切片）→ 原作业、当前状态。
	same := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "k", Values: []int64{7, -2},
		Dependencies: []uint64{u1.ID, u2.ID},
	})
	if same.ID != orig.ID || same.Status != StatusSucceeded {
		t.Fatalf("equal-content replay: id=%d status=%s, want %d succeeded",
			same.ID, same.Status, orig.ID)
	}

	// 只交换整数位置：实际输入 [-2,7,3,3] 的总和、平方和与原输入相同，仍冲突。
	j, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "k", Values: []int64{-2, 7},
		Dependencies: []uint64{u1.ID, u2.ID},
	})
	if !errors.Is(err, ErrIdempotencyConflict) || j == nil || j.ID != orig.ID {
		t.Fatalf("values-order swap despite identical totals must conflict: %v %+v", err, j)
	}
	// 只交换依赖次序：两个上游总和都是 3，追加部分数值完全相同，仍冲突。
	j, err = s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "k", Values: []int64{7, -2},
		Dependencies: []uint64{u2.ID, u1.ID},
	})
	if !errors.Is(err, ErrIdempotencyConflict) || j == nil || j.ID != orig.ID {
		t.Fatalf("dependency-order swap despite identical sums must conflict: %v %+v", err, j)
	}
	// 两种冲突请求都不能新增作业：归档里只有 u1、u2、orig 三个作业。
	if got, _ := s.List("a", time.Time{}, time.Time{}); len(got) != 3 {
		t.Fatalf("conflicting replays created records: list len=%d want 3", len(got))
	}
	// 原作业的参数与成功归档保持不变。
	g := waitStatus(t, s, orig.ID, StatusSucceeded)
	assertInt64s(t, "original values", g.Values, []int64{7, -2})
	assertUint64s(t, "original dependencies", g.Dependencies, []uint64{u1.ID, u2.ID})
	assertInt64s(t, "original effective", g.Archive.EffectiveValues, wantEffective)
	if g.Archive.Sum != 11 || g.Archive.SumOfSquares != 71 {
		t.Fatalf("original archive altered: sum=%d sq=%d", g.Archive.Sum, g.Archive.SumOfSquares)
	}
}
