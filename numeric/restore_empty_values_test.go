package numeric

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeRawJSONRecord 把一段原始 JSON 直接写成作业记录文件，用于构造
// encodeRecord 不会产出的形态（values 字段缺省、空数组等）。
func writeRawJSONRecord(t *testing.T, dir string, id uint64, jsonText string) {
	t.Helper()
	if err := writeFileAtomic(dir, jobFileName(id), []byte(jsonText), 0o600); err != nil {
		t.Fatal(err)
	}
}

// assertEmptyValuesFailure 校验空原始序列排队记录重开后的统一形态：失败、
// BlockerID 为 0、无成功归档与实际输入、从未进入运行，原因明确说明原始整数
// 序列为空；原作业号、提交人、请求号、种子、提交时间与依赖顺序保持不变；
// 按作业号读取与按提交人列举一致，落盘记录同样已改写。
func assertEmptyValuesFailure(t *testing.T, s *Store, id uint64, wantQueuedAt time.Time, wantDeps []uint64) {
	t.Helper()
	g, err := s.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusFailed {
		t.Fatalf("job %d must fail on empty original values, got %s", id, g.Status)
	}
	if g.BlockerID != 0 {
		t.Fatalf("empty original values is the job's own error: job %d blocker=%d want 0", id, g.BlockerID)
	}
	if !strings.Contains(g.FailureReason, "原始整数序列为空") {
		t.Fatalf("job %d reason=%q must state the original integer sequence is empty", id, g.FailureReason)
	}
	if g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("job %d must not return archive/effective inputs: archive=%v effective=%v",
			id, g.Archive, g.EffectiveValues)
	}
	if !g.StartedAt.IsZero() {
		t.Fatalf("job %d must never start computing, started=%s", id, g.StartedAt)
	}
	if g.FinishedAt.IsZero() {
		t.Fatalf("job %d failed record must carry a finished time", id)
	}
	if !g.QueuedAt.Equal(wantQueuedAt) {
		t.Fatalf("job %d queued time changed: %s want %s", id, g.QueuedAt, wantQueuedAt)
	}
	if len(g.Values) != 0 {
		t.Fatalf("job %d original values must stay empty, got %v", id, g.Values)
	}
	if len(g.Dependencies) != len(wantDeps) {
		t.Fatalf("job %d dependencies changed: %v want %v", id, g.Dependencies, wantDeps)
	}
	for i := range wantDeps {
		if g.Dependencies[i] != wantDeps[i] {
			t.Fatalf("job %d dependency order changed: %v want %v", id, g.Dependencies, wantDeps)
		}
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
			lj.FailureReason != g.FailureReason || !lj.FinishedAt.Equal(g.FinishedAt) {
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
	if !strings.Contains(text, `"finished_at"`) {
		t.Fatalf("job %d on-disk record must carry a finished time:\n%s", id, text)
	}
}

// 无依赖的排队记录原始序列为空（values 缺省、为 null 或为空数组）时，重开后
// 直接失败——不能继续计算得到总和、平方和均为 0 的成功归档；同目录其他原始
// 序列有效的作业照常调度计算，单条无效记录不影响归档打开。
func TestReopenQueuedEmptyValuesFailsWithoutComputing(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 1：values 字段缺省。
	writeRawJSONRecord(t, dir, 1, `{
  "version": "numeric-job-v1",
  "id": 1,
  "submitter": "a",
  "request_id": "missing",
  "seed": 7,
  "queued_at": "`+base.Add(time.Second).Format(time.RFC3339Nano)+`",
  "status": "queued"
}`)
	// 作业 2：values 为 null（encodeRecord 对空序列的既有写法）。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "null",
		seed:     8,
		queuedAt: base.Add(2 * time.Second),
		status:   StatusQueued,
	})
	// 作业 3：values 为空数组。
	writeRawJSONRecord(t, dir, 3, `{
  "version": "numeric-job-v1",
  "id": 3,
  "submitter": "b",
  "request_id": "empty-array",
  "seed": 9,
  "values": [],
  "queued_at": "`+base.Add(3*time.Second).Format(time.RFC3339Nano)+`",
  "status": "queued"
}`)
	// 作业 4：原始序列有效的排队作业，重开后应照常完成。
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

	assertEmptyValuesFailure(t, s, 1, base.Add(time.Second), nil)
	assertEmptyValuesFailure(t, s, 2, base.Add(2*time.Second), nil)
	assertEmptyValuesFailure(t, s, 3, base.Add(3*time.Second), nil)
	for _, id := range []uint64{1, 2, 3} {
		g, _ := s.Get(id)
		if g.Seed != int64(6+id) {
			t.Fatalf("job %d seed changed: %d", id, g.Seed)
		}
	}

	// 无关的有效作业继续遵守已有调度与计算行为。
	indep := waitStatus(t, s, 4, StatusSucceeded)
	if indep.Archive.Sum != 6 || indep.Archive.SumOfSquares != 14 {
		t.Fatalf("unrelated job result=%d,%d want 6,14", indep.Archive.Sum, indep.Archive.SumOfSquares)
	}

	// 再次重开：失败结果已保存在目录中，状态、原因与根因保持稳定。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	assertEmptyValuesFailure(t, s2, 1, base.Add(time.Second), nil)
	assertEmptyValuesFailure(t, s2, 2, base.Add(2*time.Second), nil)
	assertEmptyValuesFailure(t, s2, 3, base.Add(3*time.Second), nil)
}

// 带合法依赖的排队记录原始序列为空时，同样直接失败：不能凭成功上游追加的
// 总和补出原始参数后继续计算；被引用的有效上游保留自己的结果。
func TestReopenQueuedEmptyValuesWithDependencyFails(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 1：[1,2] → 总和 3，成功归档。
	up := syntheticSucceededRecord(t, dir, 1, []int64{1, 2}, []int64{1, 2}, nil, base)
	if up.archive.Sum != 3 {
		t.Fatalf("upstream sum=%d want 3", up.archive.Sum)
	}
	// 作业 2：排队，原始序列为空，合法依赖作业 1。若凭上游总和补出输入，
	// 会以 [3] 算出总和 3、平方和 9 的“成功”结果——必须拒绝。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "empty",
		dependencies: []uint64{1},
		queuedAt:     base.Add(2 * time.Second),
		status:       StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	assertEmptyValuesFailure(t, s, 2, base.Add(2*time.Second), []uint64{1})

	// 被引用的有效上游保留自己的成功结果，不因无效记录而改变。
	gUp, _ := s.Get(1)
	if gUp.Status != StatusSucceeded || gUp.Archive == nil || gUp.Archive.Sum != 3 {
		t.Fatalf("valid upstream must keep its result: status=%s archive=%v", gUp.Status, gUp.Archive)
	}
}

// 等待空序列记录结果的下游按既有依赖失败规则被阻断：原因指出直接阻断它的
// 上游，根因沿依赖链保留为空序列记录自身的作业号；再下游沿链条继承同一根因。
func TestReopenQueuedEmptyValuesBlocksDownstream(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 1：排队，原始序列为空。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "empty",
		queuedAt: base.Add(time.Second),
		status:   StatusQueued,
	})
	// 作业 2：排队等待作业 1。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "child",
		values:       []int64{20},
		dependencies: []uint64{1},
		queuedAt:     base.Add(2 * time.Second),
		status:       StatusQueued,
	})
	// 作业 3：排队等待作业 2（空序列记录的再下游）。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "grandchild",
		values:       []int64{30},
		dependencies: []uint64{2},
		queuedAt:     base.Add(3 * time.Second),
		status:       StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	assertEmptyValuesFailure(t, s, 1, base.Add(time.Second), nil)

	child := waitStatus(t, s, 2, StatusFailed)
	if child.BlockerID != 1 {
		t.Fatalf("child blocker=%d want root 1 (the empty-values record itself)", child.BlockerID)
	}
	if !strings.Contains(child.FailureReason, "直接上游作业 1") ||
		!strings.Contains(child.FailureReason, "原始整数序列为空") {
		t.Fatalf("child must name direct upstream 1 and carry its empty-values reason: %q",
			child.FailureReason)
	}
	if child.Archive != nil || !child.StartedAt.IsZero() {
		t.Fatalf("blocked child must never compute: started=%s archive=%v", child.StartedAt, child.Archive)
	}

	grand := waitStatus(t, s, 3, StatusFailed)
	if grand.BlockerID != 1 {
		t.Fatalf("grandchild blocker=%d want root 1 preserved along the chain", grand.BlockerID)
	}
	if !strings.Contains(grand.FailureReason, "直接上游作业 2") ||
		!strings.Contains(grand.FailureReason, "阻断根因为作业 1") {
		t.Fatalf("grandchild must name direct upstream 2 and root 1: %q", grand.FailureReason)
	}
}

// 空序列指没有任何原始整数：[0]、[0,0] 以及包含负数或重复整数的非空序列
// 仍是合法输入，不按总和是否为 0 判断有效性，重开后照常计算成功。
func TestReopenQueuedZeroValuedSequencesRemainValid(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "zero",
		values:   []int64{0},
		queuedAt: base.Add(time.Second),
		status:   StatusQueued,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "zeros",
		values:   []int64{0, 0},
		queuedAt: base.Add(2 * time.Second),
		status:   StatusQueued,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "neg-dup",
		values:   []int64{-2, 2, -2},
		queuedAt: base.Add(3 * time.Second),
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	g1 := waitStatus(t, s, 1, StatusSucceeded)
	if g1.Archive.Sum != 0 || g1.Archive.SumOfSquares != 0 {
		t.Fatalf("[0] result=%d,%d want 0,0", g1.Archive.Sum, g1.Archive.SumOfSquares)
	}
	g2 := waitStatus(t, s, 2, StatusSucceeded)
	if g2.Archive.Sum != 0 || g2.Archive.SumOfSquares != 0 {
		t.Fatalf("[0,0] result=%d,%d want 0,0", g2.Archive.Sum, g2.Archive.SumOfSquares)
	}
	g3 := waitStatus(t, s, 3, StatusSucceeded)
	if g3.Archive.Sum != -2 || g3.Archive.SumOfSquares != 12 {
		t.Fatalf("[-2,2,-2] result=%d,%d want -2,12", g3.Archive.Sum, g3.Archive.SumOfSquares)
	}
}

// 已经失败或取消的记录即使原始序列为空，也保留既有终态、原因与根因，
// 不被恢复阶段的空序列检查改写。
func TestReopenEmptyValuesKeepsExistingTerminalRecords(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	failedAt := base.Add(2 * time.Second)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "f",
		queuedAt: failedAt, finishedAt: failedAt,
		status:        StatusFailed,
		failureReason: "既有的失败原因",
		blockerID:     9,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "c",
		queuedAt: base.Add(3 * time.Second), finishedAt: base.Add(4 * time.Second),
		status: StatusCanceled,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	gf, _ := s.Get(1)
	if gf.Status != StatusFailed || gf.BlockerID != 9 || gf.FailureReason != "既有的失败原因" {
		t.Fatalf("existing failed record must be preserved: status=%s blocker=%d reason=%q",
			gf.Status, gf.BlockerID, gf.FailureReason)
	}
	gc, _ := s.Get(2)
	if gc.Status != StatusCanceled || gc.BlockerID != 0 || gc.FailureReason != "" {
		t.Fatalf("existing canceled record must be preserved: status=%s blocker=%d reason=%q",
			gc.Status, gc.BlockerID, gc.FailureReason)
	}
}
