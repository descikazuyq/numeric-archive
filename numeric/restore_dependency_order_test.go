package numeric

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// assertDependencyOrderFailure 校验“依赖违反先后关系”记录重开后的统一形态：
// 失败、BlockerID 为 0、无成功归档与实际输入，原因说明依赖违反先后关系并
// 指出保存的依赖顺序中第一个不合法的上游作业号、说明它不能作为本作业的
// 上游；按作业号读取与按提交人列举一致，落盘记录同样已改写。
func assertDependencyOrderFailure(t *testing.T, s *Store, id, badID uint64) {
	t.Helper()
	g, err := s.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusFailed {
		t.Fatalf("job %d must fail on out-of-order dependency, got %s", id, g.Status)
	}
	if g.BlockerID != 0 {
		t.Fatalf("out-of-order dependency is the job's own error: job %d blocker=%d want 0",
			id, g.BlockerID)
	}
	if g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("job %d must not return archive/effective inputs: archive=%v effective=%v",
			id, g.Archive, g.EffectiveValues)
	}
	if !strings.Contains(g.FailureReason, "依赖违反先后关系") ||
		!strings.Contains(g.FailureReason, "作业 "+itoa(badID)) ||
		!strings.Contains(g.FailureReason, "不能作为本作业的直接上游") {
		t.Fatalf("job %d reason=%q must state ordering violation and name illegal upstream %d",
			id, g.FailureReason, badID)
	}
	if g.FinishedAt.IsZero() {
		t.Fatalf("job %d failed record must carry a finished time", id)
	}

	listed, err := s.List(g.Submitter, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, lj := range listed {
		if lj.ID != id {
			continue
		}
		found = true
		if lj.Status != StatusFailed || lj.BlockerID != 0 ||
			lj.Archive != nil || lj.EffectiveValues != nil ||
			lj.FailureReason != g.FailureReason {
			t.Fatalf("listed job %d disagrees with Get: status=%s blocker=%d reason=%q",
				id, lj.Status, lj.BlockerID, lj.FailureReason)
		}
	}
	if !found {
		t.Fatalf("job %d missing from submitter listing", id)
	}

	data, err := os.ReadFile(filepath.Join(s.Dir(), jobFileName(id)))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, `"status": "failed"`) {
		t.Fatalf("job %d on-disk record must be failed:\n%s", id, text)
	}
	// blocker 为 0 时按 omitempty 不落盘；出现任何 blocker_id 都说明根因错误。
	if strings.Contains(text, `"blocker_id"`) {
		t.Fatalf("job %d on-disk record must have no blocker root:\n%s", id, text)
	}
	if strings.Contains(text, `"archive"`) {
		t.Fatalf("job %d on-disk record must not retain archive:\n%s", id, text)
	}
}

// 规格示例：作业 7 保存了对作业 7 自身的依赖，原始序列 [0]，实际输入 [0,0]，
// 总和、平方和、日志、摘要与校验值全部正确——重开后仍必须判失败，原因指出
// 作业 7 不能作为自己的上游，BlockerID 为 0。等待它的下游按既有规则失败，
// 根因就是作业 7；引用链上的已归档中间作业同样改判并保留同一根因；无关作业
// 照常成功。
func TestReopenRejectsSelfConsistentSelfDependency(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 7：原始 [0]，依赖保存为自己 [7]，实际输入 [0,0]（sum=0、sq=0，
	// 全套字段自洽）。
	self := syntheticSucceededRecord(t, dir, 7,
		[]int64{0}, []int64{0, 0}, []uint64{7}, base)
	if self.archive.Sum != 0 || self.archive.SumOfSquares != 0 {
		t.Fatalf("self-dep example must be self-consistent: %+v", self.archive)
	}
	// 作业 8：排队等待作业 7。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 8, submitter: "a", requestID: "child",
		values: []int64{20}, dependencies: []uint64{7},
		queuedAt: base.Add(8 * time.Second),
		status:   StatusQueued,
	})
	// 作业 9：已成功归档、直接上游为 7，追加值等于作业 7 的总和 0，全套自洽。
	archived := syntheticSucceededRecord(t, dir, 9,
		[]int64{1}, []int64{1, 0}, []uint64{7}, base)
	if archived.archive.Sum != 1 || archived.archive.SumOfSquares != 1 {
		t.Fatalf("archived downstream must be self-consistent: %+v", archived.archive)
	}
	// 作业 10：排队等待作业 9，验证根因跨已归档中间作业保留。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 10, submitter: "a", requestID: "grandchild",
		values: []int64{30}, dependencies: []uint64{9},
		queuedAt: base.Add(10 * time.Second),
		status:   StatusQueued,
	})
	// 作业 11：无关排队作业，重开后应照常完成。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 11, submitter: "b", requestID: "indep",
		values:   []int64{1, 2, 3},
		queuedAt: base.Add(11 * time.Second),
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	assertDependencyOrderFailure(t, s, 7, 7)
	// 原记录已有完成时间，改判时保留，不改写时间信息。
	g7, _ := s.Get(7)
	if !g7.FinishedAt.Equal(base.Add(8 * time.Second)) {
		t.Fatalf("job 7 finished time must be preserved: got %v want %v",
			g7.FinishedAt, base.Add(8*time.Second))
	}
	if !g7.StartedAt.Equal(base.Add(7*time.Second+time.Millisecond)) ||
		!g7.QueuedAt.Equal(base.Add(7*time.Second)) {
		t.Fatalf("job 7 existing time info must be preserved: queued=%s started=%s",
			g7.QueuedAt, g7.StartedAt)
	}

	// 排队下游：直接上游 7、根因 7（问题记录自身 BlockerID 为 0）。
	child := waitStatus(t, s, 8, StatusFailed)
	if child.BlockerID != 7 {
		t.Fatalf("child blocker=%d want root 7 (the self-dependent record itself)",
			child.BlockerID)
	}
	if !strings.Contains(child.FailureReason, "直接上游作业 7") ||
		!strings.Contains(child.FailureReason, "依赖违反先后关系") {
		t.Fatalf("child must name direct upstream 7 and carry its reason: %q",
			child.FailureReason)
	}
	if child.Archive != nil || !child.StartedAt.IsZero() {
		t.Fatalf("blocked child must never compute: started=%s archive=%v",
			child.StartedAt, child.Archive)
	}

	// 已归档下游 9：成功结果不可用，直接上游 7，根因 7。
	g9, _ := s.Get(9)
	if g9.Status != StatusFailed || g9.BlockerID != 7 || g9.Archive != nil {
		t.Fatalf("archived downstream 9 must fail with root 7: %+v", g9)
	}
	if !strings.Contains(g9.FailureReason, "直接上游作业 7") ||
		!strings.Contains(g9.FailureReason, "成功结果不可用") {
		t.Fatalf("job 9 reason must name unavailable upstream 7: %q", g9.FailureReason)
	}

	// 排队的作业 10：直接上游 9，根因仍为 7。
	grand := waitStatus(t, s, 10, StatusFailed)
	if grand.BlockerID != 7 {
		t.Fatalf("grandchild blocker=%d want root 7", grand.BlockerID)
	}
	if !strings.Contains(grand.FailureReason, "直接上游作业 9") ||
		!strings.Contains(grand.FailureReason, "阻断根因为作业 7") {
		t.Fatalf("grandchild must name direct upstream 9 and root 7: %q",
			grand.FailureReason)
	}

	// 无关作业照常处理。
	indep := waitStatus(t, s, 11, StatusSucceeded)
	if indep.Archive.Sum != 6 || indep.Archive.SumOfSquares != 14 {
		t.Fatalf("unrelated job result=%d,%d want 6,14",
			indep.Archive.Sum, indep.Archive.SumOfSquares)
	}

	// 再次重开：失败结果已落盘，状态、原因与根因保持稳定。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	assertDependencyOrderFailure(t, s2, 7, 7)
	for _, want := range []struct{ id, root uint64 }{
		{8, 7}, {9, 7}, {10, 7},
	} {
		g, _ := s2.Get(want.id)
		if g.Status != StatusFailed || g.BlockerID != want.root {
			t.Fatalf("after second reopen job %d status=%s blocker=%d want failed/%d",
				want.id, g.Status, g.BlockerID, want.root)
		}
	}
}

// 排队记录引用自己时重开即失败：不能开始计算（无开始时间），补记完成时间，
// 等待它的下游随之失败，根因为该记录自身。
func TestReopenQueuedSelfDependencyFailsWithoutComputing(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "self",
		values: []int64{10}, dependencies: []uint64{2},
		queuedAt: base.Add(2 * time.Second),
		status:   StatusQueued,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "child",
		values: []int64{20}, dependencies: []uint64{2},
		queuedAt: base.Add(3 * time.Second),
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	assertDependencyOrderFailure(t, s, 2, 2)
	g2, _ := s.Get(2)
	if !g2.StartedAt.IsZero() {
		t.Fatalf("self-dependent queued job must never start computing, started=%s",
			g2.StartedAt)
	}
	if g2.FinishedAt.IsZero() {
		t.Fatalf("queued job re-judged as failed must get a finished time")
	}
	if !g2.QueuedAt.Equal(base.Add(2 * time.Second)) {
		t.Fatalf("queued-at time must be preserved: %v", g2.QueuedAt)
	}

	child := waitStatus(t, s, 3, StatusFailed)
	if child.BlockerID != 2 || !strings.Contains(child.FailureReason, "直接上游作业 2") {
		t.Fatalf("child blocker=%d reason=%q want root 2 named as direct upstream",
			child.BlockerID, child.FailureReason)
	}
}

// 引用作业号比自己大的作业（即使该作业确实存在且已经成功、全套数值自洽）
// 同样改判失败；被引用的有效上游保留自己的成功结果，不被判成失败。
func TestReopenRejectsDependencyOnFutureJob(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 5：[5] → 总和 5，合法且成功。
	future := syntheticSucceededRecord(t, dir, 5, []int64{5}, []int64{5}, nil, base)
	if future.archive.Sum != 5 {
		t.Fatalf("future job sum=%d want 5", future.archive.Sum)
	}
	// 作业 3：原始 [10]，保存了对作业 5 的依赖，实际输入 [10,5]
	//（sum=15、sq=125，全套自洽）。提交作业 3 时作业 5 尚不存在，不可能合法。
	bad := syntheticSucceededRecord(t, dir, 3,
		[]int64{10}, []int64{10, 5}, []uint64{5}, base)
	if bad.archive.Sum != 15 || bad.archive.SumOfSquares != 125 {
		t.Fatalf("future-dep record must be self-consistent: %+v", bad.archive)
	}
	// 作业 4：排队等待作业 5——记录同样违反先后关系，不能继续等待。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 4, submitter: "a", requestID: "waitfuture",
		values: []int64{11}, dependencies: []uint64{5},
		queuedAt: base.Add(4 * time.Second),
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	assertDependencyOrderFailure(t, s, 3, 5)
	assertDependencyOrderFailure(t, s, 4, 5)

	// 被引用的有效作业保留成功结果。
	g5, _ := s.Get(5)
	if g5.Status != StatusSucceeded || g5.Archive == nil || g5.Archive.Sum != 5 {
		t.Fatalf("referenced future job must keep its own success: %+v", g5)
	}
}

// 多个违反先后关系的引用同时存在时，原因指出保存的依赖顺序中第一个不合法的
// 作业号（不按作业号大小挑）；依赖列表同时重复时保留既有的重复失败说明，
// 不因先后检查改写。
func TestReopenOutOfOrderReasonNamesFirstIllegalInSavedOrder(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 1 合法存在且成功。
	syntheticSucceededRecord(t, dir, 1, []int64{1, 2}, []int64{1, 2}, nil, base)
	// 作业 6：依赖保存顺序 [1, 9, 8]——1 合法，9 是第一个不合法的（8 也不合法，
	// 但在列表中更靠后；9、8 的记录甚至不存在，先后判断先于缺失判断）。
	syntheticSucceededRecord(t, dir, 6,
		[]int64{10}, []int64{10, 3, 9, 8}, []uint64{1, 9, 8}, base)
	// 作业 7：依赖 [10, 7, 11]——10 先不合法，自引用 7 虽在列表中但更靠后。
	syntheticSucceededRecord(t, dir, 7,
		[]int64{20}, []int64{20, 10, 0, 11}, []uint64{10, 7, 11}, base)
	// 作业 8：依赖 [9, 9]——重复检查优先，保留重复依赖失败说明。
	syntheticSucceededRecord(t, dir, 8,
		[]int64{30}, []int64{30, 9, 9}, []uint64{9, 9}, base)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	g6, _ := s.Get(6)
	if g6.Status != StatusFailed || g6.BlockerID != 0 ||
		!strings.Contains(g6.FailureReason, "作业 9") ||
		strings.Contains(g6.FailureReason, "作业 8") {
		t.Fatalf("job 6 must name first illegal dep 9 (not 8): %q", g6.FailureReason)
	}
	g7, _ := s.Get(7)
	if g7.Status != StatusFailed || g7.BlockerID != 0 ||
		!strings.Contains(g7.FailureReason, "作业 10") {
		t.Fatalf("job 7 must name first illegal dep 10 in saved order: %q",
			g7.FailureReason)
	}
	g8, _ := s.Get(8)
	if g8.Status != StatusFailed || g8.BlockerID != 0 ||
		!strings.Contains(g8.FailureReason, "依赖列表重复") ||
		strings.Contains(g8.FailureReason, "依赖违反先后关系") {
		t.Fatalf("job 8 must keep duplicate-dependency reason: %q", g8.FailureReason)
	}
}

// 已经失败或取消的记录即使保存了引用自己/后来作业的依赖，也保留既有终态、
// 原因与根因，不被恢复阶段的先后检查改写。
func TestReopenOutOfOrderKeepsExistingTerminalRecords(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	failedAt := base.Add(2 * time.Second)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "f",
		values: []int64{10}, dependencies: []uint64{2}, // 引用自己
		queuedAt: failedAt, finishedAt: failedAt,
		status:        StatusFailed,
		failureReason: "既有的失败原因",
		blockerID:     9,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "c",
		values: []int64{11}, dependencies: []uint64{99}, // 引用后来作业
		queuedAt: base.Add(3 * time.Second), finishedAt: base.Add(4 * time.Second),
		status: StatusCanceled,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	gf, _ := s.Get(2)
	if gf.Status != StatusFailed || gf.BlockerID != 9 || gf.FailureReason != "既有的失败原因" {
		t.Fatalf("existing failed record must be preserved: status=%s blocker=%d reason=%q",
			gf.Status, gf.BlockerID, gf.FailureReason)
	}
	gc, _ := s.Get(3)
	if gc.Status != StatusCanceled || gc.BlockerID != 0 || gc.FailureReason != "" {
		t.Fatalf("existing canceled record must be preserved: status=%s blocker=%d reason=%q",
			gc.Status, gc.BlockerID, gc.FailureReason)
	}
}

// 先后只看作业号，不要求依赖列表按作业号排序：合法的无序列表（各项互异且都
// 小于本作业号）仍按原次序追加各上游总和，重开后保持成功与依赖次序。
func TestReopenUnsortedButEarlierDependenciesRemainLegal(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	syntheticSucceededRecord(t, dir, 1, []int64{1}, []int64{1}, nil, base) // 和 1
	syntheticSucceededRecord(t, dir, 3, []int64{3}, []int64{3}, nil, base) // 和 3
	syntheticSucceededRecord(t, dir, 4, []int64{4}, []int64{4}, nil, base) // 和 4
	// 作业 5：依赖顺序 [3,1,4]——无序但都小于 5，追加 [3,1,4]：
	// sum=10+3+1+4=18，sq=100+9+1+16=126。
	down := syntheticSucceededRecord(t, dir, 5,
		[]int64{10}, []int64{10, 3, 1, 4}, []uint64{3, 1, 4}, base)
	if down.archive.Sum != 18 || down.archive.SumOfSquares != 126 {
		t.Fatalf("legal unsorted record must be self-consistent: %+v", down.archive)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	g, err := s.Get(5)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusSucceeded || g.Archive == nil {
		t.Fatalf("distinct smaller out-of-order upstreams must stay legal: %s", g.Status)
	}
	if g.Archive.Sum != 18 || g.Archive.SumOfSquares != 126 {
		t.Fatalf("legal result changed: %+v", g.Archive)
	}
	if len(g.Dependencies) != 3 || g.Dependencies[0] != 3 ||
		g.Dependencies[1] != 1 || g.Dependencies[2] != 4 {
		t.Fatalf("dependency order must be preserved: %v", g.Dependencies)
	}
}

// 旧的单依赖记录（只有 has_dependency/dependency_id）适用同一规则：保存了
// 更大作业号作为唯一上游时，即使数值全套自洽也改判失败。
func TestReopenLegacySingleDependencyOutOfOrderFails(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 3 存在且成功，总和 3。
	syntheticSucceededRecord(t, dir, 3, []int64{3}, []int64{3}, nil, base)
	// 作业 2：旧格式单依赖记录，dependency_id 指向后来的作业 3，实际输入
	// [10,3]（sum=13、sq=109，全套自洽），顶层与归档都不写 dependencies。
	eff := []int64{10, 3}
	down := &storedJob{
		id: 2, submitter: "a", requestID: "legacy",
		values:       []int64{10},
		dependencies: []uint64{3},
		queuedAt:     base.Add(2 * time.Second), finishedAt: base.Add(3 * time.Second),
		status: StatusSucceeded,
	}
	arc := newArchive(down, eff, 13, 109, down.finishedAt)
	arc.Dependencies = nil // 旧归档无此字段
	r := jobRecord{
		Version: recordVersion,
		ID:      2, Submitter: down.submitter, RequestID: down.requestID,
		Seed: down.seed, Values: []int64{10},
		HasDependency: true, DependencyID: 3,
		// Dependencies 刻意留空（omitempty 后落盘无此字段）。
		QueuedAt: down.queuedAt, FinishedAt: down.finishedAt,
		Status:          StatusSucceeded,
		EffectiveValues: append([]int64(nil), eff...),
		Archive:         arc,
	}
	data, err := json.MarshalIndent(&r, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, jobFileName(2)), data, 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	assertDependencyOrderFailure(t, s, 2, 3)

	// 被引用的作业 3 不受影响。
	g3, _ := s.Get(3)
	if g3.Status != StatusSucceeded || g3.Archive == nil {
		t.Fatalf("referenced job 3 must stay succeeded: %s", g3.Status)
	}
}
