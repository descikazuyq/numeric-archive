package numeric

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// assertDependencyInputsFailure 重开归档后作业必须呈现为依赖输入对应失败：
// 状态失败、查询与列举不再返回成功归档与实际输入，原作业号、提交人、
// 请求号、原始序列、种子与依赖顺序保留，失败原因指出无法对应或无法
// 使用的直接上游作业号并说明成功结果不可用，落盘记录同样已改写为失败。
// wantBlocker 为必须保留的阻断根因作业号：因直接上游结果失效时沿链条
// 保留最初根因；仅本作业保存的追加输入不符（上游全部有效）时必须为 0。
func assertDependencyInputsFailure(t *testing.T, s *Store, j *Job, wantDep uint64, keyword string, wantBlocker uint64) {
	t.Helper()
	g, err := s.Get(j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusFailed {
		t.Fatalf("job %d must fail closed, got %s", j.ID, g.Status)
	}
	if g.BlockerID != wantBlocker {
		t.Fatalf("job %d blocker=%d want root %d (reason=%q)", j.ID, g.BlockerID, wantBlocker, g.FailureReason)
	}
	if g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("job %d must not carry archive/effective inputs: archive=%v effective=%v",
			j.ID, g.Archive, g.EffectiveValues)
	}
	if g.ID != j.ID || g.Submitter != j.Submitter || g.RequestID != j.RequestID || g.Seed != j.Seed {
		t.Fatalf("identity changed: got id=%d submitter=%q request=%q seed=%d",
			g.ID, g.Submitter, g.RequestID, g.Seed)
	}
	if len(g.Values) != len(j.Values) {
		t.Fatalf("original values changed: %v, want %v", g.Values, j.Values)
	}
	for i := range j.Values {
		if g.Values[i] != j.Values[i] {
			t.Fatalf("original values changed: %v, want %v", g.Values, j.Values)
		}
	}
	if len(g.Dependencies) != len(j.Dependencies) {
		t.Fatalf("dependency order changed: %v, want %v", g.Dependencies, j.Dependencies)
	}
	for i := range j.Dependencies {
		if g.Dependencies[i] != j.Dependencies[i] {
			t.Fatalf("dependency order changed: %v, want %v", g.Dependencies, j.Dependencies)
		}
	}
	if !strings.Contains(g.FailureReason, "作业 "+itoa(wantDep)) ||
		!strings.Contains(g.FailureReason, keyword) ||
		!strings.Contains(g.FailureReason, "成功结果不可用") {
		t.Fatalf("job %d reason=%q must name upstream %d with %q and state success results unusable",
			j.ID, g.FailureReason, wantDep, keyword)
	}
	// 按提交人列举时同样不返回成功归档与实际输入。
	listed, err := s.List(j.Submitter, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, lj := range listed {
		if lj.ID == j.ID {
			found = true
			if lj.Status != StatusFailed || lj.Archive != nil || lj.EffectiveValues != nil {
				t.Fatalf("listed job %d must be failed without archive: %s", j.ID, lj.Status)
			}
		}
	}
	if !found {
		t.Fatalf("job %d missing from submitter listing", j.ID)
	}
	// 落盘记录同样已改写为失败。
	data, err := os.ReadFile(filepath.Join(s.Dir(), jobFileName(j.ID)))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"status": "failed"`) {
		t.Fatal("on-disk record must be rewritten as failed")
	}
}

// 原始序列 [10]，两个上游总和分别为 3 和 -2，依赖列表先引用总和为 -2 的
// 作业：正确的实际输入是 [10,-2,3]。记录写成 [10,3,-2] 时总和仍为 11、
// 平方和仍为 113，摘要、日志与校验值也都与这份错误输入自洽，重开后仍
// 必须判定该成功结果不可用；上游自身通过校验，不受下游错误影响。
func TestReopenRejectsSwappedDependencySums(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{1, 2}})  // 和 3
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{1, -3}}) // 和 -2
	waitStatus(t, s, u1.ID, StatusSucceeded)
	waitStatus(t, s, u2.ID, StatusSucceeded)
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{10}, Seed: 9,
		Dependencies: []uint64{u2.ID, u1.ID}, // 先引用总和为 -2 的作业
	})
	done := waitStatus(t, s, down.ID, StatusSucceeded)
	if done.Archive.Sum != 11 || done.Archive.SumOfSquares != 113 {
		t.Fatalf("real result=%d,%d", done.Archive.Sum, done.Archive.SumOfSquares)
	}
	before, _ := s.Get(down.ID)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 追加部分次序调换：[10,3,-2] 与 [10,-2,3] 的总和、平方和完全相同。
	rewriteRecordEffectiveInputs(t, dir, down.ID, []int64{10, 3, -2})

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	// 第一个对不上的位置对应依赖列表中的 u2。
	assertDependencyInputsFailure(t, s2, before, u2.ID, "无法对应", 0)

	// 上游自身通过校验，不因下游追加值错误而改动。
	for _, id := range []uint64{u1.ID, u2.ID} {
		g, err := s2.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if g.Status != StatusSucceeded || g.Archive == nil {
			t.Fatalf("upstream %d must stay succeeded: %s", id, g.Status)
		}
	}
	// 无关作业继续正常查询和计算。
	next := mustSubmit(t, s2, SubmitRequest{Submitter: "b", Values: []int64{1, 2, 3}})
	nd := waitStatus(t, s2, next.ID, StatusSucceeded)
	if nd.Archive.Sum != 6 || nd.Archive.SumOfSquares != 14 {
		t.Fatalf("unrelated job result=%d,%d", nd.Archive.Sum, nd.Archive.SumOfSquares)
	}
}

// 负数、零以及不同上游恰好同和都按位置判断：正确的记录（含同和上游、
// 零和、负和）重开后保持原结果、摘要与校验值；追加值与对应位置上游
// 总和不符的记录失败。
func TestReopenDependencyAppendedInputsByPosition(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{1, 2}})   // 和 3
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{4, -1}})  // 和 3（同和）
	u3 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u3", Values: []int64{0, 0}})   // 和 0
	u4 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u4", Values: []int64{-2, -3}}) // 和 -5
	for _, id := range []uint64{u1.ID, u2.ID, u3.ID, u4.ID} {
		waitStatus(t, s, id, StatusSucceeded)
	}
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{7},
		Dependencies: []uint64{u2.ID, u1.ID, u3.ID, u4.ID},
	})
	done := waitStatus(t, s, down.ID, StatusSucceeded)
	// 实际输入 [7,3,3,0,-5]：sum=8，sq=49+9+9+0+25=92。
	wantEff := []int64{7, 3, 3, 0, -5}
	for i := range wantEff {
		if done.Archive.EffectiveValues[i] != wantEff[i] {
			t.Fatalf("effective=%v want %v", done.Archive.EffectiveValues, wantEff)
		}
	}
	if done.Archive.Sum != 8 || done.Archive.SumOfSquares != 92 {
		t.Fatalf("result=%d,%d want 8,92", done.Archive.Sum, done.Archive.SumOfSquares)
	}
	wantChecksum := done.Archive.Checksum
	before, _ := s.Get(down.ID)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 篡改副本：把 u4 的追加位置写成 5 而非 -5（绝对值相同，符号不同）。
	d := t.TempDir()
	copyArchive(t, dir, d)
	bad := append([]int64(nil), wantEff...)
	bad[4] = 5
	rewriteRecordEffectiveInputs(t, d, down.ID, bad)
	sBad, err := Open(d)
	if err != nil {
		t.Fatal(err)
	}
	defer sBad.Close()
	assertDependencyInputsFailure(t, sBad, before, u4.ID, "无法对应", 0)

	// 原始归档重开：同和上游、零和、负和都按位置对应，结果与校验值不变。
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
		t.Fatalf("valid dependent archive must survive reopen: %s", g.Status)
	}
	if g.Archive.Sum != 8 || g.Archive.SumOfSquares != 92 || g.Archive.Checksum != wantChecksum {
		t.Fatalf("archive changed across reopen: %+v", g.Archive)
	}
	for i := range wantEff {
		if g.EffectiveValues[i] != wantEff[i] {
			t.Fatalf("effective=%v want %v", g.EffectiveValues, wantEff)
		}
	}
}

// 上游在本次打开中因已有校验被判失败时，已经归档成功的下游也不能继续使用
// 它：下游及其更下游的成功记录一并改判失败，失败原因指出无法使用的直接
// 上游作业号；仍在排队等待失效作业的下游沿用既有依赖失败处理。
func TestReopenInvalidatedUpstreamFailsSucceededDependents(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	up := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "up", Values: []int64{2, -3}}) // 和 -1
	waitStatus(t, s, up.ID, StatusSucceeded)
	mid := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "mid", Values: []int64{5},
		Dependencies: []uint64{up.ID},
	})
	waitStatus(t, s, mid.ID, StatusSucceeded) // 实际输入 [5,-1]
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{1},
		Dependencies: []uint64{mid.ID},
	})
	waitStatus(t, s, down.ID, StatusSucceeded) // 实际输入 [1,4]
	midBefore, _ := s.Get(mid.ID)
	downBefore, _ := s.Get(down.ID)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 篡改上游的数值结果（全套字段自洽）：重开后上游被既有复算校验判失败。
	rewriteRecordNumbers(t, dir, up.ID, 8, 13)
	// 仍在排队等待 mid 的作业：沿用既有依赖失败处理级联失败。
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 100, submitter: "a", requestID: "queued",
		values: []int64{9}, dependencies: []uint64{mid.ID},
		queuedAt: base,
		status:   StatusQueued,
	})

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	gUp, _ := s2.Get(up.ID)
	if gUp.Status != StatusFailed {
		t.Fatalf("forged upstream: %s, want failed", gUp.Status)
	}
	// 已成功归档的 mid/down 也不能继续使用失效上游。
	assertDependencyInputsFailure(t, s2, midBefore, up.ID, "无法使用", up.ID)
	assertDependencyInputsFailure(t, s2, downBefore, mid.ID, "无法使用", up.ID)
	// 排队等待失效作业的下游按既有依赖失败处理：直接上游为 mid，根因沿
	// 链条保留为最初校验失败的 up。
	gq := waitStatus(t, s2, 100, StatusFailed)
	if gq.BlockerID != up.ID {
		t.Fatalf("queued downstream blocker=%d want root %d", gq.BlockerID, up.ID)
	}
	if !strings.Contains(gq.FailureReason, "直接上游作业 "+itoa(mid.ID)) ||
		!strings.Contains(gq.FailureReason, "阻断根因为作业 "+itoa(up.ID)) {
		t.Fatalf("queued downstream must name direct upstream %d and root %d: %q",
			mid.ID, up.ID, gq.FailureReason)
	}
}

// 直接上游的记录在归档目录中不存在时，依赖它的成功记录同样不可用。
func TestReopenMissingUpstreamRecordFailsDependent(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	up := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "up", Values: []int64{4}})
	waitStatus(t, s, up.ID, StatusSucceeded)
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{1},
		Dependencies: []uint64{up.ID},
	})
	waitStatus(t, s, down.ID, StatusSucceeded)
	before, _ := s.Get(down.ID)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(filepath.Join(dir, jobFileName(up.ID))); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	assertDependencyInputsFailure(t, s2, before, up.ID, "无法使用", up.ID)
}

// 旧格式单依赖记录（无 dependencies 字段）按同一含义核对追加值：
// 追加值不等于唯一上游总和时失败；正确记录保持原结果（既有测试覆盖）。
func TestReopenLegacySingleDependencyWrongAppendedSum(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	up := &storedJob{
		id: 1, submitter: "a", requestID: "up",
		values:          []int64{2, -3},
		effectiveValues: []int64{2, -3},
		queuedAt:        base, finishedAt: base.Add(time.Second),
		status: StatusSucceeded,
	}
	up.archive = newArchive(up, []int64{2, -3}, -1, 13, up.finishedAt)
	writeSyntheticRecord(t, dir, up)

	// 唯一上游总和为 -1，追加值却写成 3：全套摘要与这份错误输入自洽。
	eff := []int64{5, 3}
	down := &storedJob{
		id: 2, submitter: "a", requestID: "down",
		values:          []int64{5},
		dependencies:    []uint64{1},
		effectiveValues: eff,
		queuedAt:        base.Add(2 * time.Second), finishedAt: base.Add(3 * time.Second),
		status: StatusSucceeded,
	}
	arc := newArchive(down, eff, 8, 34, down.finishedAt)
	arc.Dependencies = nil // 旧归档无此字段
	r := buildLegacyRecord(t, down, arc, eff)
	if err := os.WriteFile(filepath.Join(dir, jobFileName(2)), r, 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	gUp, _ := s.Get(1)
	if gUp.Status != StatusSucceeded {
		t.Fatalf("upstream must stay succeeded: %s", gUp.Status)
	}
	g, err := s.Get(2)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusFailed || g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("legacy record with wrong appended sum must fail: %s", g.Status)
	}
	if !strings.Contains(g.FailureReason, "作业 1") ||
		!strings.Contains(g.FailureReason, "成功结果不可用") {
		t.Fatalf("reason=%q must name upstream 1 and state success results unusable", g.FailureReason)
	}
	if len(g.Dependencies) != 1 || g.Dependencies[0] != 1 || len(g.Values) != 1 || g.Values[0] != 5 {
		t.Fatalf("original params must be preserved: deps=%v values=%v", g.Dependencies, g.Values)
	}
}

// 正确的单依赖、多依赖归档重开后保持原结果、摘要与校验值（依赖输入对应
// 检查不改变既有有效记录）。
func TestReopenKeepsCorrectDependencyArchives(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{1, 2}})
	waitStatus(t, s, u1.ID, StatusSucceeded)
	single := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "single", Values: []int64{10},
		HasDependency: true, DependencyID: u1.ID,
	})
	waitStatus(t, s, single.ID, StatusSucceeded)
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{-4, 1}})
	waitStatus(t, s, u2.ID, StatusSucceeded)
	multi := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "multi", Values: []int64{7},
		Dependencies: []uint64{u2.ID, single.ID},
	})
	waitStatus(t, s, multi.ID, StatusSucceeded)
	singleDone, _ := s.Get(single.ID)
	multiDone, _ := s.Get(multi.ID)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for _, want := range []*Job{singleDone, multiDone} {
		g, err := s2.Get(want.ID)
		if err != nil {
			t.Fatal(err)
		}
		if g.Status != StatusSucceeded || g.Archive == nil {
			t.Fatalf("job %d must stay succeeded: %s", want.ID, g.Status)
		}
		if g.Archive.Sum != want.Archive.Sum || g.Archive.SumOfSquares != want.Archive.SumOfSquares ||
			g.Archive.Checksum != want.Archive.Checksum || g.Archive.InputsDigest != want.Archive.InputsDigest ||
			g.Archive.ResultDigest != want.Archive.ResultDigest || g.Archive.Log != want.Archive.Log {
			t.Fatalf("job %d archive changed across reopen", want.ID)
		}
	}
}
