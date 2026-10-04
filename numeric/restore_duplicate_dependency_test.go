package numeric

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 规格示例：原始序列 [10]，上游作业总和为 3，依赖列表把该上游的作业号保存了
// 两次，实际输入 [10,3,3]，总和 16、平方和 118，日志、摘要与校验值全套自洽。
// 重新打开归档时仍不能视为合法成功：提交时不允许同一份作业重复引用同一个直接
// 上游，恢复时同样不接受。本作业改判为失败（自身记录有误，BlockerID 为 0），
// 失败原因指出依赖列表重复与首次再次出现的上游作业号；被重复引用的上游保留
// 自己的结果；仍在等待这份结果的下游按既有规则失败并沿链条保留根因作业号。
func TestReopenDuplicateDependencyFailsSelfConsistentSuccess(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	up := &storedJob{
		id: 1, submitter: "a", requestID: "up",
		values:          []int64{3},
		effectiveValues: []int64{3},
		queuedAt:        base, finishedAt: base.Add(time.Second),
		status: StatusSucceeded,
	}
	up.archive = newArchive(up, []int64{3}, 3, 9, up.finishedAt)
	writeSyntheticRecord(t, dir, up)

	// 依赖列表重复引用作业 1；归档数值与这份伪造的实际输入完全自洽。
	eff := []int64{10, 3, 3}
	sum, sumSq, _, ok := computeResult(eff, 0, nil)
	if !ok || sum != 16 || sumSq != 118 {
		t.Fatalf("spec example inputs must give sum=16 sq=118, got %d,%d ok=%v", sum, sumSq, ok)
	}
	down := &storedJob{
		id: 2, submitter: "a", requestID: "down",
		values:          []int64{10},
		dependencies:    []uint64{1, 1},
		effectiveValues: eff,
		queuedAt:        base.Add(2 * time.Second), finishedAt: base.Add(3 * time.Second),
		status: StatusSucceeded,
	}
	down.archive = newArchive(down, eff, sum, sumSq, down.finishedAt)
	writeSyntheticRecord(t, dir, down)

	// 仍在等待作业 2 结果的下游。
	waiter := &storedJob{
		id: 3, submitter: "a", requestID: "waiter",
		values: []int64{9}, dependencies: []uint64{2},
		queuedAt: base.Add(4 * time.Second),
		status:   StatusQueued,
	}
	writeSyntheticRecord(t, dir, waiter)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 被重复引用的上游保留自己的结果。
	gUp, err := s.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if gUp.Status != StatusSucceeded || gUp.Archive == nil || gUp.Archive.Sum != 3 {
		t.Fatalf("被重复引用的上游须保留自己的结果: status=%s archive=%v", gUp.Status, gUp.Archive)
	}

	// 重复依赖记录改判失败：自身记录有误，BlockerID 为 0，不再返回归档与实际输入。
	gDown, err := s.Get(2)
	if err != nil {
		t.Fatal(err)
	}
	if gDown.Status != StatusFailed || gDown.BlockerID != 0 {
		t.Fatalf("重复依赖记录须失败且 BlockerID 为 0: status=%s blocker=%d", gDown.Status, gDown.BlockerID)
	}
	if !strings.Contains(gDown.FailureReason, "依赖列表重复") ||
		!strings.Contains(gDown.FailureReason, "作业 1") {
		t.Fatalf("原因须说明依赖列表重复并指出首次再次出现的上游作业号 1: %q", gDown.FailureReason)
	}
	if gDown.Archive != nil || gDown.EffectiveValues != nil {
		t.Fatalf("失败记录不得返回成功归档与实际输入: archive=%v effective=%v",
			gDown.Archive, gDown.EffectiveValues)
	}
	// 原作业号、提交参数与依赖次序保留。
	if len(gDown.Dependencies) != 2 || gDown.Dependencies[0] != 1 || gDown.Dependencies[1] != 1 ||
		len(gDown.Values) != 1 || gDown.Values[0] != 10 {
		t.Fatalf("原提交参数与依赖次序须保留: deps=%v values=%v", gDown.Dependencies, gDown.Values)
	}

	// 等待它的下游按既有规则失败：直接上游为 2，根因沿链条保留为 2
	// （作业 2 自身无根因，以它自己为根因）。
	gWaiter := waitStatus(t, s, 3, StatusFailed)
	if gWaiter.BlockerID != 2 {
		t.Fatalf("waiter blocker=%d want 2", gWaiter.BlockerID)
	}
	if !strings.Contains(gWaiter.FailureReason, "直接上游作业 2") {
		t.Fatalf("waiter 原因须指出直接上游 2: %q", gWaiter.FailureReason)
	}

	// 按提交人列举与按作业号读取得到相同状态和原因。
	listed, err := s.List("a", time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[uint64]*Job, len(listed))
	for _, lj := range listed {
		byID[lj.ID] = lj
	}
	if byID[2] == nil || byID[2].Status != StatusFailed || byID[2].FailureReason != gDown.FailureReason ||
		byID[2].BlockerID != 0 {
		t.Fatalf("列举与读取的归因不一致: %+v", byID[2])
	}

	// 正常可写的归档目录保存这一失败结果：落盘记录与查询一致。
	data, err := os.ReadFile(filepath.Join(dir, jobFileName(2)))
	if err != nil {
		t.Fatal(err)
	}
	var rec jobRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Status != StatusFailed || rec.BlockerID != 0 || rec.FailureReason != gDown.FailureReason {
		t.Fatalf("落盘记录须保存相同状态与原因: status=%s blocker=%d reason=%q",
			rec.Status, rec.BlockerID, rec.FailureReason)
	}

	// 无关记录不受影响：归档正常打开，新作业照常提交与调度。
	fresh := mustSubmit(t, s, SubmitRequest{Submitter: "b", RequestID: "fresh", Values: []int64{1, 2}})
	if g := waitStatus(t, s, fresh.ID, StatusSucceeded); g.Archive == nil || g.Archive.Sum != 3 {
		t.Fatalf("新作业须照常完成: %+v", g)
	}
}

// 重复的作业号隔着其他上游出现（非相邻）同样识别；首次再次出现的作业号按
// 保存的依赖顺序判断。
func TestReopenNonAdjacentDuplicateDependencyFails(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	mkUp := func(id uint64, values []int64, sum, sumSq int64) {
		up := &storedJob{
			id: id, submitter: "a", requestID: "up" + itoa(id),
			values:          values,
			effectiveValues: append([]int64(nil), values...),
			queuedAt:        base, finishedAt: base.Add(time.Second),
			status: StatusSucceeded,
		}
		up.archive = newArchive(up, up.effectiveValues, sum, sumSq, up.finishedAt)
		writeSyntheticRecord(t, dir, up)
	}
	mkUp(1, []int64{3}, 3, 9)
	mkUp(2, []int64{5}, 5, 25)

	// 依赖顺序 [1, 2, 1]：首次再次出现的是作业 1；追加部分 [3,5,3] 与上游
	// 总和逐项吻合，全套字段自洽。
	eff := []int64{10, 3, 5, 3}
	sum, sumSq, _, ok := computeResult(eff, 0, nil)
	if !ok {
		t.Fatal("inputs overflow")
	}
	down := &storedJob{
		id: 3, submitter: "a", requestID: "down",
		values:          []int64{10},
		dependencies:    []uint64{1, 2, 1},
		effectiveValues: eff,
		queuedAt:        base.Add(2 * time.Second), finishedAt: base.Add(3 * time.Second),
		status: StatusSucceeded,
	}
	down.archive = newArchive(down, eff, sum, sumSq, down.finishedAt)
	writeSyntheticRecord(t, dir, down)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	g, err := s.Get(3)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusFailed || g.BlockerID != 0 {
		t.Fatalf("非相邻重复同样失败且 BlockerID 为 0: status=%s blocker=%d", g.Status, g.BlockerID)
	}
	if !strings.Contains(g.FailureReason, "依赖列表重复") ||
		!strings.Contains(g.FailureReason, "作业 1") {
		t.Fatalf("原因须指出首次再次出现的作业号 1: %q", g.FailureReason)
	}
	for _, id := range []uint64{1, 2} {
		if gu, _ := s.Get(id); gu.Status != StatusSucceeded {
			t.Fatalf("上游 %d 须保留成功结果: %s", id, gu.Status)
		}
	}
}

// 原排队的重复依赖记录重开后立即失败，不会开始计算（不会把同一份上游总和
// 追加两次后继续）。
func TestReopenDuplicateDependencyQueuedNeverRuns(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	up := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "up", Values: []int64{3}})
	waitStatus(t, s, up.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 合成一条排队记录：依赖列表把上游保存了两次。若漏查，worker 会把上游
	// 总和追加两次后以 [10,3,3] 继续计算。
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: up.ID + 100, submitter: "a", requestID: "queued",
		values: []int64{10}, dependencies: []uint64{up.ID, up.ID},
		queuedAt: base,
		status:   StatusQueued,
	})

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	g, err := s2.Get(up.ID + 100)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusFailed || g.BlockerID != 0 {
		t.Fatalf("排队重复依赖记录须直接失败且 BlockerID 为 0: status=%s blocker=%d", g.Status, g.BlockerID)
	}
	if !strings.Contains(g.FailureReason, "依赖列表重复") ||
		!strings.Contains(g.FailureReason, "作业 "+itoa(up.ID)) {
		t.Fatalf("原因须说明重复并指出上游作业号: %q", g.FailureReason)
	}
	if !g.StartedAt.IsZero() || g.EffectiveValues != nil {
		t.Fatalf("原排队作业不得开始计算: started=%v effective=%v", g.StartedAt, g.EffectiveValues)
	}
	gUp, _ := s2.Get(up.ID)
	if gUp.Status != StatusSucceeded {
		t.Fatalf("上游须保留成功结果: %s", gUp.Status)
	}
}

// 唯一性按作业号判断而不是按上游总和判断：两个不同上游都得到 3 时，按保存
// 次序各追加一个 3 仍然合法，重开后保持原顺序与计算结果。
func TestReopenDistinctUpstreamsWithSameSumStayValid(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{1, 2}})
	waitStatus(t, s, u1.ID, StatusSucceeded) // 和 3
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{3}})
	waitStatus(t, s, u2.ID, StatusSucceeded) // 和 3
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{10},
		Dependencies: []uint64{u1.ID, u2.ID},
	})
	done := waitStatus(t, s, down.ID, StatusSucceeded)
	if done.Archive.Sum != 16 || done.Archive.SumOfSquares != 118 {
		t.Fatalf("sum=%d sq=%d want 16,118", done.Archive.Sum, done.Archive.SumOfSquares)
	}
	wantEffective := append([]int64(nil), done.EffectiveValues...)
	wantChecksum := done.Archive.Checksum
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	g, err := s2.Get(down.ID)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusSucceeded || g.Archive == nil {
		t.Fatalf("同和的不同上游不得被合并或拒绝: %s", g.Status)
	}
	if g.Archive.Checksum != wantChecksum || g.Archive.Sum != 16 || g.Archive.SumOfSquares != 118 {
		t.Fatalf("重开后结果与校验值须保持不变: %+v", g.Archive)
	}
	if len(g.EffectiveValues) != len(wantEffective) {
		t.Fatalf("实际输入须保持 [10,3,3]: %v", g.EffectiveValues)
	}
	for i := range wantEffective {
		if g.EffectiveValues[i] != wantEffective[i] {
			t.Fatalf("实际输入须保持 [10,3,3]: %v", g.EffectiveValues)
		}
	}
	if len(g.Dependencies) != 2 || g.Dependencies[0] != u1.ID || g.Dependencies[1] != u2.ID {
		t.Fatalf("依赖次序须保持: %v", g.Dependencies)
	}
}

// 已失败或已取消的记录即使依赖列表重复也保留既有终态与原因，不被改写。
func TestReopenDuplicateDependencyKeepsExistingTerminalState(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	up := &storedJob{
		id: 1, submitter: "a", requestID: "up",
		values:          []int64{3},
		effectiveValues: []int64{3},
		queuedAt:        base, finishedAt: base.Add(time.Second),
		status: StatusSucceeded,
	}
	up.archive = newArchive(up, []int64{3}, 3, 9, up.finishedAt)
	writeSyntheticRecord(t, dir, up)

	failed := &storedJob{
		id: 2, submitter: "a", requestID: "failed",
		values: []int64{10}, dependencies: []uint64{1, 1},
		queuedAt: base.Add(2 * time.Second), finishedAt: base.Add(3 * time.Second),
		status: StatusFailed, failureReason: "原有失败原因", blockerID: 7,
	}
	writeSyntheticRecord(t, dir, failed)
	canceled := &storedJob{
		id: 3, submitter: "a", requestID: "canceled",
		values: []int64{10}, dependencies: []uint64{1, 1},
		queuedAt: base.Add(4 * time.Second), finishedAt: base.Add(5 * time.Second),
		status: StatusCanceled,
	}
	writeSyntheticRecord(t, dir, canceled)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	gFailed, err := s.Get(2)
	if err != nil {
		t.Fatal(err)
	}
	if gFailed.Status != StatusFailed || gFailed.FailureReason != "原有失败原因" || gFailed.BlockerID != 7 {
		t.Fatalf("已失败记录须保留既有终态与原因: status=%s reason=%q blocker=%d",
			gFailed.Status, gFailed.FailureReason, gFailed.BlockerID)
	}
	gCanceled, err := s.Get(3)
	if err != nil {
		t.Fatal(err)
	}
	if gCanceled.Status != StatusCanceled {
		t.Fatalf("已取消记录须保留取消状态: %s", gCanceled.Status)
	}
}
