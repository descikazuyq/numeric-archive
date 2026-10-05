package numeric

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeRecordValuesVariant 把记录写到磁盘，并按 variant 控制原始整数序列字段的
// 落盘形态：
//   - "missing"：直接省略 values 字段；
//   - "null"：显式写为 null；
//   - "empty"：写为空数组 []。
//
// 对排队记录而言，三者在语义上都是“没有任何原始整数”，重开后必须与提交入口
// 拒绝空序列遵守同一项输入要求。
func writeRecordValuesVariant(t *testing.T, dir string, j *storedJob, variant string) {
	t.Helper()
	data, err := encodeRecord(j)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	switch variant {
	case "missing":
		delete(fields, "values")
	case "null":
		fields["values"] = json.RawMessage("null")
	case "empty":
		fields["values"] = json.RawMessage("[]")
	default:
		t.Fatalf("unknown variant %q", variant)
	}
	out, err := json.MarshalIndent(fields, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, jobFileName(j.id)), out, 0o600); err != nil {
		t.Fatal(err)
	}
}

// assertEmptySequenceFailure 校验空序列排队记录重开后的统一形态：失败、
// BlockerID 为 0、从未开始运行、无成功归档与实际输入、原因明确说明原始整数
// 序列为空；原作业号、提交人、请求号、种子、提交时间与依赖顺序保留，并补上
// 完成时间。按作业号读取与按提交人列举一致，落盘记录同样已改写。
func assertEmptySequenceFailure(t *testing.T, s *Store, want *storedJob) *Job {
	t.Helper()
	g, err := s.Get(want.id)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusFailed {
		t.Fatalf("job %d with empty original sequence must fail on reopen, got %s",
			want.id, g.Status)
	}
	if g.BlockerID != 0 {
		t.Fatalf("empty sequence is the job's own invalid input: job %d blocker=%d want 0",
			want.id, g.BlockerID)
	}
	if !strings.Contains(g.FailureReason, "原始整数序列为空") {
		t.Fatalf("job %d reason=%q must state the original integer sequence is empty",
			want.id, g.FailureReason)
	}
	if g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("job %d must not return archive/effective inputs: archive=%v effective=%v",
			want.id, g.Archive, g.EffectiveValues)
	}
	if !g.StartedAt.IsZero() {
		t.Fatalf("job %d must never start computing, started=%s", want.id, g.StartedAt)
	}
	if g.FinishedAt.IsZero() {
		t.Fatalf("job %d must have a finished time when it enters the failed state", want.id)
	}
	// 原作业号、提交人、请求号、种子、提交时间与原有依赖顺序全部保留。
	if g.ID != want.id || g.Submitter != want.submitter || g.RequestID != want.requestID ||
		g.Seed != want.seed || !g.QueuedAt.Equal(want.queuedAt) {
		t.Fatalf("job %d identity fields altered: %+v", want.id, g)
	}
	if len(g.Dependencies) != len(want.dependencies) {
		t.Fatalf("job %d dependency order altered: %v, want %v",
			want.id, g.Dependencies, want.dependencies)
	}
	for i := range want.dependencies {
		if g.Dependencies[i] != want.dependencies[i] {
			t.Fatalf("job %d dependency order altered: %v, want %v",
				want.id, g.Dependencies, want.dependencies)
		}
	}
	// 原始序列字段仍为空：不能凭任何来源补出原始参数。
	if len(g.Values) != 0 {
		t.Fatalf("job %d must not fabricate original values, got %v", want.id, g.Values)
	}

	listed, err := s.List(want.submitter, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, lj := range listed {
		if lj.ID != want.id {
			continue
		}
		found = true
		if lj.Status != StatusFailed || lj.BlockerID != 0 ||
			lj.Archive != nil || lj.EffectiveValues != nil ||
			lj.FailureReason != g.FailureReason ||
			!lj.QueuedAt.Equal(want.queuedAt) || len(lj.Dependencies) != len(want.dependencies) {
			t.Fatalf("listed job %d disagrees with Get: status=%s blocker=%d reason=%q",
				want.id, lj.Status, lj.BlockerID, lj.FailureReason)
		}
	}
	if !found {
		t.Fatalf("job %d missing from submitter listing", want.id)
	}

	data, err := os.ReadFile(filepath.Join(s.Dir(), jobFileName(want.id)))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, `"status": "failed"`) {
		t.Fatalf("job %d on-disk record must be failed:\n%s", want.id, text)
	}
	// blocker 为 0 时按 omitempty 不落盘；出现任何 blocker_id 都说明根因错误。
	if strings.Contains(text, `"blocker_id"`) {
		t.Fatalf("job %d on-disk record must have no blocker root:\n%s", want.id, text)
	}
	if strings.Contains(text, `"archive"`) {
		t.Fatalf("job %d on-disk record must not retain archive:\n%s", want.id, text)
	}
	if !strings.Contains(text, "原始整数序列为空") {
		t.Fatalf("job %d on-disk record must persist the empty-sequence reason:\n%s",
			want.id, text)
	}
	return g
}

// 规格示例：缺省、null 与空数组三种落盘形态的无依赖排队记录，重开后都直接
// 失败——无依赖时不能把空输入算出的总和、平方和 0 当作成功归档。
func TestReopenQueuedEmptySequenceVariantsFailWithoutComputing(t *testing.T) {
	for _, variant := range []string{"missing", "null", "empty"} {
		t.Run(variant, func(t *testing.T) {
			dir := t.TempDir()
			base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
			want := &storedJob{
				id: 1, submitter: "a", requestID: "empty",
				seed: 42, queuedAt: base,
				status: StatusQueued,
			}
			writeRecordValuesVariant(t, dir, want, variant)

			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			assertEmptySequenceFailure(t, s, want)

			// 再次重开：失败状态与原因已保存在原记录中，形态保持稳定。
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s2, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s2.Close()
			assertEmptySequenceFailure(t, s2, want)
		})
	}
}

// 带依赖的空序列排队记录不能开始运行，也不能只用成功上游的追加总和补出原始
// 参数完成计算；被引用的有效上游保留自己的状态与结果。
func TestReopenQueuedEmptySequenceWithDependenciesDoesNotUseUpstreamSums(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 1：[1,2] → 总和 3 的成功上游。
	up := syntheticSucceededRecord(t, dir, 1, []int64{1, 2}, []int64{1, 2}, nil, base)
	if up.archive.Sum != 3 {
		t.Fatalf("upstream sum=%d want 3", up.archive.Sum)
	}
	// 作业 2：排队、依赖 [1]，原始序列为空（null）。若错误地用追加值计算，
	// 会得到实际输入 [3]、总和 3、平方和 9 的“成功”。
	want := &storedJob{
		id: 2, submitter: "a", requestID: "empty-dep",
		seed: 7, dependencies: []uint64{1},
		queuedAt: base.Add(2 * time.Second),
		status:   StatusQueued,
	}
	writeRecordValuesVariant(t, dir, want, "null")

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	g := assertEmptySequenceFailure(t, s, want)
	if len(g.Dependencies) != 1 || g.Dependencies[0] != 1 {
		t.Fatalf("dependency order must be preserved: %v", g.Dependencies)
	}

	// 有效上游不被这份无效记录影响。
	gUp, _ := s.Get(1)
	if gUp.Status != StatusSucceeded || gUp.Archive == nil ||
		gUp.Archive.Sum != 3 || gUp.Archive.SumOfSquares != 5 {
		t.Fatalf("valid upstream must keep its result: status=%s archive=%v",
			gUp.Status, gUp.Archive)
	}
}

// 空序列记录重开判失败后，仍在排队等待它的下游按既有依赖失败规则被阻断：
// 直接阻断者是该空序列作业，根因沿链条保留为这份记录自身（其 BlockerID 为
// 0）；更下游同样不能继续计算或宣称成功。
func TestReopenQueuedEmptySequenceBlocksDownstreamWithRoot(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 1：空序列排队记录（无依赖）。
	empty := &storedJob{
		id: 1, submitter: "a", requestID: "empty",
		queuedAt: base,
		status:   StatusQueued,
	}
	writeRecordValuesVariant(t, dir, empty, "empty")
	// 作业 2：直接下游，排队等待作业 1。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "child",
		values: []int64{20}, dependencies: []uint64{1},
		queuedAt: base.Add(2 * time.Second),
		status:   StatusQueued,
	})
	// 作业 3：更下游，排队等待作业 2。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "grandchild",
		values: []int64{30}, dependencies: []uint64{2},
		queuedAt: base.Add(3 * time.Second),
		status:   StatusQueued,
	})
	// 作业 4：与空序列链无关的排队作业，重开后照常成功。
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

	assertEmptySequenceFailure(t, s, empty)

	child := waitStatus(t, s, 2, StatusFailed)
	if child.BlockerID != 1 {
		t.Fatalf("child blocker=%d want root 1 (the empty-sequence record itself)",
			child.BlockerID)
	}
	if !strings.Contains(child.FailureReason, "直接上游作业 1") ||
		!strings.Contains(child.FailureReason, "原始整数序列为空") {
		t.Fatalf("child must name direct upstream 1 and carry the empty-sequence reason: %q",
			child.FailureReason)
	}
	if child.Archive != nil || child.EffectiveValues != nil || !child.StartedAt.IsZero() {
		t.Fatalf("blocked child must never compute: started=%s archive=%v effective=%v",
			child.StartedAt, child.Archive, child.EffectiveValues)
	}

	grand := waitStatus(t, s, 3, StatusFailed)
	if grand.BlockerID != 1 {
		t.Fatalf("grandchild blocker=%d want root 1 along the chain", grand.BlockerID)
	}
	if !strings.Contains(grand.FailureReason, "直接上游作业 2") ||
		!strings.Contains(grand.FailureReason, "阻断根因为作业 1") {
		t.Fatalf("grandchild must name direct upstream 2 and root 1: %q",
			grand.FailureReason)
	}
	if grand.Archive != nil || !grand.StartedAt.IsZero() {
		t.Fatalf("blocked grandchild must never compute: started=%s archive=%v",
			grand.StartedAt, grand.Archive)
	}

	// 无关作业继续按既有调度与计算行为成功。
	indep := waitStatus(t, s, 4, StatusSucceeded)
	if indep.Archive.Sum != 6 || indep.Archive.SumOfSquares != 14 {
		t.Fatalf("unrelated job result=%d,%d want 6,14",
			indep.Archive.Sum, indep.Archive.SumOfSquares)
	}

	// 再次重开：空序列记录与整条阻断链的状态、原因、根因保持稳定。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	assertEmptySequenceFailure(t, s2, empty)
	for _, want := range []struct {
		id      uint64
		blocker uint64
	}{{2, 1}, {3, 1}} {
		g := waitStatus(t, s2, want.id, StatusFailed)
		if g.BlockerID != want.blocker || g.Archive != nil {
			t.Fatalf("after second reopen job %d blocker=%d archive=%v want blocker %d",
				want.id, g.BlockerID, g.Archive, want.blocker)
		}
	}
}

// 空序列指没有任何原始整数：[0]、[0,0] 及包含负数或重复整数的非空序列仍是
// 合法输入，不能按总和是否为 0 判断有效性。
func TestReopenQueuedNonEmptyZeroLikeSequencesRemainLegal(t *testing.T) {
	cases := []struct {
		name       string
		values     []int64
		sum        int64
		sumSquares int64
	}{
		{"single zero", []int64{0}, 0, 0},
		{"two zeros", []int64{0, 0}, 0, 0},
		{"negatives summing zero", []int64{-3, 3}, 0, 18},
		{"repeated negatives", []int64{-1, -1}, -2, 2},
		{"zero with negatives and repeats", []int64{0, -2, -2, 3}, -1, 17},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
			writeSyntheticRecord(t, dir, &storedJob{
				id: 1, submitter: "a", requestID: "nonempty",
				values:   tc.values,
				queuedAt: base,
				status:   StatusQueued,
			})

			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			g := waitStatus(t, s, 1, StatusSucceeded)
			if g.Archive == nil {
				t.Fatalf("non-empty sequence %v must be a legal input", tc.values)
			}
			if g.Archive.Sum != tc.sum || g.Archive.SumOfSquares != tc.sumSquares {
				t.Fatalf("non-empty sequence %v result=%d,%d want %d,%d",
					tc.values, g.Archive.Sum, g.Archive.SumOfSquares, tc.sum, tc.sumSquares)
			}
			if len(g.EffectiveValues) != len(tc.values) {
				t.Fatalf("effective inputs=%v, want %v", g.EffectiveValues, tc.values)
			}
			for i := range tc.values {
				if g.EffectiveValues[i] != tc.values[i] {
					t.Fatalf("effective inputs=%v, want %v", g.EffectiveValues, tc.values)
				}
			}
		})
	}
}

// 已经失败或取消的记录即使原始序列字段为空，也继续按各自既有恢复规则处理：
// 保留原有状态、原因、根因与完成时间，不被空序列检查改写。
func TestReopenTerminalRecordsWithEmptyValuesKeepExistingState(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	failedAt := base.Add(2 * time.Second)
	failed := &storedJob{
		id: 2, submitter: "a", requestID: "f",
		queuedAt: base, finishedAt: failedAt,
		status:        StatusFailed,
		failureReason: "既有的失败原因",
		blockerID:     9,
	}
	writeRecordValuesVariant(t, dir, failed, "empty") // values 为 []，状态为 failed
	canceledAt := base.Add(4 * time.Second)
	canceled := &storedJob{
		id: 3, submitter: "a", requestID: "c",
		queuedAt: base.Add(3 * time.Second), finishedAt: canceledAt,
		status: StatusCanceled,
	}
	writeRecordValuesVariant(t, dir, canceled, "null") // values 为 null，状态为 canceled

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
	if !gf.FinishedAt.Equal(failedAt) {
		t.Fatalf("existing failed record finished time must be preserved: %s, want %s",
			gf.FinishedAt, failedAt)
	}
	gc, _ := s.Get(3)
	if gc.Status != StatusCanceled || gc.BlockerID != 0 || gc.FailureReason != "" {
		t.Fatalf("existing canceled record must be preserved: status=%s blocker=%d reason=%q",
			gc.Status, gc.BlockerID, gc.FailureReason)
	}
	if !gc.FinishedAt.Equal(canceledAt) {
		t.Fatalf("existing canceled record finished time must be preserved: %s, want %s",
			gc.FinishedAt, canceledAt)
	}
}
