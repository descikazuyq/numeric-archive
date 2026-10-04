package numeric

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// syntheticSucceededRecord 直接在磁盘上构造一条“成功”记录：依赖列表与实际
// 输入由调用方给定（可以故意违反提交时的唯一性校验），归档的摘要、日志与
// 校验值按给定实际输入与结果经 newArchive 全套生成，因此记录在数值层面完全
// 自洽——用于验证恢复逻辑不能只凭结果自洽接受记录。
func syntheticSucceededRecord(t *testing.T, dir string, id uint64, values, eff []int64, deps []uint64, base time.Time) *storedJob {
	t.Helper()
	sum, sumSq, _, ok := computeResult(eff, 0, nil)
	if !ok {
		t.Fatalf("synthetic eff %v overflows", eff)
	}
	j := &storedJob{
		id: id, submitter: "a", requestID: "j" + itoa(id),
		values: values, dependencies: deps,
		queuedAt:   base.Add(time.Duration(id) * time.Second),
		startedAt:  base.Add(time.Duration(id)*time.Second + time.Millisecond),
		finishedAt: base.Add(time.Duration(id+1) * time.Second),
		status:     StatusSucceeded,
	}
	j.effectiveValues = append([]int64(nil), eff...)
	j.archive = newArchive(j, eff, sum, sumSq, j.finishedAt)
	writeSyntheticRecord(t, dir, j)
	return j
}

// assertDuplicateDependencyFailure 校验重复依赖记录重开后的统一形态：失败、
// BlockerID 为 0、无成功归档与实际输入，原因说明依赖列表重复并指出首次再次
// 出现的上游作业号；按作业号读取与按提交人列举一致，落盘记录同样已改写。
func assertDuplicateDependencyFailure(t *testing.T, s *Store, id, repeatID uint64) {
	t.Helper()
	g, err := s.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusFailed {
		t.Fatalf("job %d must fail on duplicated dependency, got %s", id, g.Status)
	}
	if g.BlockerID != 0 {
		t.Fatalf("duplicated dependency is the job's own error: job %d blocker=%d want 0", id, g.BlockerID)
	}
	if g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("job %d must not return archive/effective inputs: archive=%v effective=%v",
			id, g.Archive, g.EffectiveValues)
	}
	if !strings.Contains(g.FailureReason, "依赖列表重复") ||
		!strings.Contains(g.FailureReason, "作业 "+itoa(repeatID)) {
		t.Fatalf("job %d reason=%q must state duplicated dependency list and name first repeated upstream %d",
			id, g.FailureReason, repeatID)
	}
	if g.FinishedAt.IsZero() {
		t.Fatalf("job %d failed record must keep a finished time", id)
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

// 规格示例：上游总和为 3，依赖列表把同一上游保存两次，实际输入 [10,3,3]，
// 总和 16、平方和 118，摘要、日志、校验值全部自洽——重开后仍必须判失败，
// 不能作为合法成功归档。被重复引用的上游保留自己的结果；等待该作业的下游
// 按既有规则失败，根因作业号就是这份重复记录自身。
func TestReopenRejectsSelfConsistentDuplicateDependency(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 1：[1,2] → 总和 3。
	up := syntheticSucceededRecord(t, dir, 1, []int64{1, 2}, []int64{1, 2}, nil, base)
	if up.archive.Sum != 3 {
		t.Fatalf("upstream sum=%d want 3", up.archive.Sum)
	}
	// 作业 2：原始 [10]，依赖列表重复保存作业 1 两次，实际输入 [10,3,3]
	// （总和 16、平方和 118，全套字段自洽）。
	dup := syntheticSucceededRecord(t, dir, 2, []int64{10}, []int64{10, 3, 3}, []uint64{1, 1}, base)
	if dup.archive.Sum != 16 || dup.archive.SumOfSquares != 118 {
		t.Fatalf("dup result=%d,%d want 16,118", dup.archive.Sum, dup.archive.SumOfSquares)
	}
	// 作业 3：仍在排队等待作业 2 的下游。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "child",
		values: []int64{20}, dependencies: []uint64{2},
		queuedAt: base.Add(3 * time.Second),
		status:   StatusQueued,
	})
	// 作业 4：与重复记录无关的排队作业，重开后应照常完成。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 4, submitter: "b", requestID: "indep",
		values:   []int64{1, 2, 3},
		queuedAt: base.Add(4 * time.Second),
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	assertDuplicateDependencyFailure(t, s, 2, 1)

	// 被重复引用的上游仍保留自己的成功结果。
	gUp, _ := s.Get(1)
	if gUp.Status != StatusSucceeded || gUp.Archive == nil || gUp.Archive.Sum != 3 {
		t.Fatalf("duplicated-but-valid upstream must keep its result: status=%s archive=%v",
			gUp.Status, gUp.Archive)
	}

	// 下游按既有依赖失败规则处理：直接上游是作业 2，根因也是作业 2
	//（重复记录自身 BlockerID 为 0，根因不另指他人）。
	child := waitStatus(t, s, 3, StatusFailed)
	if child.BlockerID != 2 {
		t.Fatalf("child blocker=%d want root 2 (the duplicate record itself)", child.BlockerID)
	}
	if !strings.Contains(child.FailureReason, "直接上游作业 2") ||
		!strings.Contains(child.FailureReason, "依赖列表重复") {
		t.Fatalf("child must name direct upstream 2 and carry its duplicate reason: %q",
			child.FailureReason)
	}
	if child.Archive != nil || !child.StartedAt.IsZero() {
		t.Fatalf("blocked child must never compute: started=%s archive=%v",
			child.StartedAt, child.Archive)
	}

	// 无关记录继续正常查询与调度。
	indep := waitStatus(t, s, 4, StatusSucceeded)
	if indep.Archive.Sum != 6 || indep.Archive.SumOfSquares != 14 {
		t.Fatalf("unrelated job result=%d,%d want 6,14", indep.Archive.Sum, indep.Archive.SumOfSquares)
	}

	// 再次重开：失败结果已保存在目录中，状态、原因与根因保持稳定，归档仍可用。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	assertDuplicateDependencyFailure(t, s3, 2, 1)
	gChild2 := waitStatus(t, s3, 3, StatusFailed)
	if gChild2.BlockerID != 2 {
		t.Fatalf("after second reopen child blocker=%d want 2", gChild2.BlockerID)
	}
}

// 重复引用不要求相邻：[a,b,a] 中作业 a 隔着 b 再次出现，失败原因指出首次
// 再次出现的 a；被引用的两个上游都保持成功。
func TestReopenRejectsNonAdjacentDuplicateDependency(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 1 总和 3，作业 2 总和 5。
	syntheticSucceededRecord(t, dir, 1, []int64{1, 2}, []int64{1, 2}, nil, base)
	syntheticSucceededRecord(t, dir, 2, []int64{5}, []int64{5}, nil, base)
	// 作业 3：依赖 [1,2,1]，实际输入 [10,3,5,3]（sum=21, sq=143，全套自洽）。
	dup := syntheticSucceededRecord(t, dir, 3,
		[]int64{10}, []int64{10, 3, 5, 3}, []uint64{1, 2, 1}, base)
	if dup.archive.Sum != 21 || dup.archive.SumOfSquares != 143 {
		t.Fatalf("dup result=%d,%d want 21,143", dup.archive.Sum, dup.archive.SumOfSquares)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	assertDuplicateDependencyFailure(t, s, 3, 1)
	for _, id := range []uint64{1, 2} {
		g, _ := s.Get(id)
		if g.Status != StatusSucceeded || g.Archive == nil {
			t.Fatalf("valid upstream %d must stay succeeded: %s", id, g.Status)
		}
	}
}

// 排队记录的依赖列表重复时，重开后直接失败，不能开始计算（无开始时间、
// 无归档与实际输入）；等待它的下游随之失败，根因为该重复记录自身。
func TestReopenQueuedDuplicateDependencyFailsWithoutComputing(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	syntheticSucceededRecord(t, dir, 1, []int64{1, 2}, []int64{1, 2}, nil, base)
	// 作业 2：排队，依赖列表重复 [1,1]。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "dup",
		values: []int64{10}, dependencies: []uint64{1, 1},
		queuedAt: base.Add(2 * time.Second),
		status:   StatusQueued,
	})
	// 作业 3：排队等待作业 2。
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

	assertDuplicateDependencyFailure(t, s, 2, 1)
	g2, _ := s.Get(2)
	if !g2.StartedAt.IsZero() {
		t.Fatalf("duplicated queued job must never start computing, started=%s", g2.StartedAt)
	}

	child := waitStatus(t, s, 3, StatusFailed)
	if child.BlockerID != 2 || !strings.Contains(child.FailureReason, "直接上游作业 2") {
		t.Fatalf("child blocker=%d reason=%q want root 2 named as direct upstream",
			child.BlockerID, child.FailureReason)
	}
}

// 唯一性按作业号判断，而非按上游总和：两个不同上游都得到 3，各引用一次时
// 实际输入同样是 [10,3,3]，仍属合法记录，重开后保持成功，次序与结果不变。
func TestReopenEqualSumDistinctUpstreamsRemainLegal(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 两个不同作业号，总和都为 3。
	syntheticSucceededRecord(t, dir, 1, []int64{1, 2}, []int64{1, 2}, nil, base)
	syntheticSucceededRecord(t, dir, 2, []int64{0, 3}, []int64{0, 3}, nil, base)
	// 作业 3：依赖 [1,2]（作业号不同），追加 [3,3] 合法。
	down := syntheticSucceededRecord(t, dir, 3,
		[]int64{10}, []int64{10, 3, 3}, []uint64{1, 2}, base)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	g, err := s.Get(3)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusSucceeded || g.Archive == nil {
		t.Fatalf("distinct upstreams with equal sums must stay legal: %s", g.Status)
	}
	if g.Archive.Sum != 16 || g.Archive.SumOfSquares != 118 ||
		g.Archive.Checksum != down.archive.Checksum {
		t.Fatalf("legal multi-dependency result changed: %+v", g.Archive)
	}
	if len(g.Dependencies) != 2 || g.Dependencies[0] != 1 || g.Dependencies[1] != 2 {
		t.Fatalf("dependency order changed: %v", g.Dependencies)
	}
}

// 已经失败或取消的记录即使保存了重复依赖列表，也保留既有终态、原因与根因，
// 不被恢复阶段的唯一性检查改写。
func TestReopenDuplicateDependencyKeepsExistingTerminalRecords(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	syntheticSucceededRecord(t, dir, 1, []int64{1, 2}, []int64{1, 2}, nil, base)
	failedAt := base.Add(2 * time.Second)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "f",
		values: []int64{10}, dependencies: []uint64{1, 1},
		queuedAt: failedAt, finishedAt: failedAt,
		status:        StatusFailed,
		failureReason: "既有的失败原因",
		blockerID:     9,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "c",
		values: []int64{11}, dependencies: []uint64{1, 1},
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
