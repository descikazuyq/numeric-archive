package numeric

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// assertDependencyInputFailure 重开归档后作业必须呈现为依赖追加输入失败：
// 状态失败、查询与列举不再返回成功归档与实际输入，原作业号、提交人、
// 请求号、原始序列、种子与依赖顺序保留，失败原因指出无法对应或无法
// 使用的直接上游作业号并说明成功结果不可用，落盘记录同样已改写为失败。
func assertDependencyInputFailure(t *testing.T, s *Store, dir string, id uint64,
	wantValues []int64, wantDeps []uint64, badIDs ...uint64) {
	t.Helper()
	g, err := s.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusFailed {
		t.Fatalf("job %d must fail closed, got %s", id, g.Status)
	}
	if g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("job %d must not carry archive/effective inputs: archive=%v effective=%v",
			id, g.Archive, g.EffectiveValues)
	}
	if g.ID != id {
		t.Fatalf("original job id changed: got %d, want %d", g.ID, id)
	}
	if len(g.Values) != len(wantValues) {
		t.Fatalf("original values changed: %v, want %v", g.Values, wantValues)
	}
	for i := range wantValues {
		if g.Values[i] != wantValues[i] {
			t.Fatalf("original values changed: %v, want %v", g.Values, wantValues)
		}
	}
	if len(g.Dependencies) != len(wantDeps) {
		t.Fatalf("dependency order changed: %v, want %v", g.Dependencies, wantDeps)
	}
	for i := range wantDeps {
		if g.Dependencies[i] != wantDeps[i] {
			t.Fatalf("dependency order changed: %v, want %v", g.Dependencies, wantDeps)
		}
	}
	if !strings.Contains(g.FailureReason, "成功结果不可用") {
		t.Fatalf("job %d reason=%q must state success results unusable", id, g.FailureReason)
	}
	for _, bad := range badIDs {
		if !strings.Contains(g.FailureReason, "作业") ||
			!strings.Contains(g.FailureReason, itoa(bad)) {
			t.Fatalf("job %d reason=%q must name unusable upstream job %d", id, g.FailureReason, bad)
		}
	}

	// 按提交人列举时同样不返回成功归档与实际输入。
	list, err := s.List(g.Submitter, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, lj := range list {
		if lj.ID == id {
			found = true
			if lj.Status != StatusFailed || lj.Archive != nil || lj.EffectiveValues != nil {
				t.Fatalf("listed job %d must be failed without archive: %s", id, lj.Status)
			}
		}
	}
	if !found {
		t.Fatalf("job %d missing from submitter listing", id)
	}

	// 可正常写入时，落盘记录也已改写为失败。
	data, err := os.ReadFile(filepath.Join(dir, jobFileName(id)))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"status": "failed"`) {
		t.Fatalf("on-disk record of job %d must be rewritten as failed", id)
	}
}

// 任务示例：原始序列 [10]，两个上游总和分别为 3 与 -2，依赖列表先引用
// 总和为 -2 的作业，再引用总和为 3 的作业，正确的实际输入是 [10,-2,3]。
// 记录被写成 [10,3,-2] 时，即使总和仍为 11、平方和仍为 113，输入摘要、
// 结果摘要、日志与校验值都与这份错误输入自洽，重开后仍必须判定失败；
// 两个上游自身通过校验，不因此改动。
func TestReopenRejectsReorderedAppendedSums(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	upPos := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "pos", Values: []int64{1, 2}}) // sum=3
	upNeg := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "neg", Values: []int64{-2}})   // sum=-2
	waitStatus(t, s, upPos.ID, StatusSucceeded)
	waitStatus(t, s, upNeg.ID, StatusSucceeded)
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{10},
		Dependencies: []uint64{upNeg.ID, upPos.ID}, // 先 -2 后 3
	})
	done := waitStatus(t, s, down.ID, StatusSucceeded)
	if done.Archive.Sum != 11 || done.Archive.SumOfSquares != 113 {
		t.Fatalf("real result=%d,%d, want 11,113", done.Archive.Sum, done.Archive.SumOfSquares)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 追加部分被调换为 [10,3,-2]：总和、平方和不变，全套摘要按错误输入自洽。
	dirBad := t.TempDir()
	copyArchive(t, dir, dirBad)
	rewriteRecordEffectiveInputs(t, dirBad, down.ID, []int64{10, 3, -2})

	s2, err := Open(dirBad)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	assertDependencyInputFailure(t, s2, dirBad, down.ID, []int64{10},
		[]uint64{upNeg.ID, upPos.ID}, upNeg.ID, upPos.ID)

	// 上游自身通过校验，不因下游追加值错误而改动。
	for _, up := range []uint64{upPos.ID, upNeg.ID} {
		g, err := s2.Get(up)
		if err != nil {
			t.Fatal(err)
		}
		if g.Status != StatusSucceeded || g.Archive == nil {
			t.Fatalf("upstream %d must stay succeeded, got %s", up, g.Status)
		}
	}

	// 未篡改的归档重开后保持原结果、摘要与校验值。
	s3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	g, err := s3.Get(down.ID)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusSucceeded || g.Archive == nil {
		t.Fatalf("correct dependent archive must survive reopen: %s", g.Status)
	}
	if g.Archive.Sum != 11 || g.Archive.SumOfSquares != 113 ||
		g.Archive.Checksum != done.Archive.Checksum ||
		g.Archive.InputsDigest != done.Archive.InputsDigest ||
		g.Archive.ResultDigest != done.Archive.ResultDigest {
		t.Fatalf("archive changed across reopen: %+v", g.Archive)
	}
	if len(g.EffectiveValues) != 3 || g.EffectiveValues[0] != 10 ||
		g.EffectiveValues[1] != -2 || g.EffectiveValues[2] != 3 {
		t.Fatalf("effective values=%v, want [10 -2 3]", g.EffectiveValues)
	}
}

// 负数、零以及不同上游恰好具有相同总和时都按位置判断：追加值等于对应位置
// 上游总和的记录保持成功，不等于的（即使换成另一个总和相同的上游的值）
// 失败；不能要求上游总和互不相同。
func TestReopenDependencyAppendedSumsPositional(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{2, 3}})  // sum=5
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{5}})     // sum=5（与 u1 相同）
	u3 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u3", Values: []int64{0}})     // sum=0
	u4 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u4", Values: []int64{-7, 1}}) // sum=-6
	for _, id := range []uint64{u1.ID, u2.ID, u3.ID, u4.ID} {
		waitStatus(t, s, id, StatusSucceeded)
	}
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{-4},
		Dependencies: []uint64{u1.ID, u2.ID, u3.ID, u4.ID},
	})
	done := waitStatus(t, s, down.ID, StatusSucceeded)
	// 实际输入 [-4, 5, 5, 0, -6]：sum=0，sq=16+25+25+0+36=102。
	if done.Archive.Sum != 0 || done.Archive.SumOfSquares != 102 {
		t.Fatalf("real result=%d,%d, want 0,102", done.Archive.Sum, done.Archive.SumOfSquares)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 追加部分位置正确（含相同总和与零）：重开后保持原结果与校验值。
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	g, err := s2.Get(down.ID)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusSucceeded || g.Archive == nil ||
		g.Archive.Checksum != done.Archive.Checksum {
		t.Fatalf("positional-correct archive must survive reopen: %s", g.Status)
	}

	// 把 u4 的追加位置写成别的值（其余不变），只有该位置对不上。
	dirBad := t.TempDir()
	copyArchive(t, dir, dirBad)
	rewriteRecordEffectiveInputs(t, dirBad, down.ID, []int64{-4, 5, 5, 0, 6})
	s3, err := Open(dirBad)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	assertDependencyInputFailure(t, s3, dirBad, down.ID, []int64{-4},
		[]uint64{u1.ID, u2.ID, u3.ID, u4.ID}, u4.ID)
}

// 上游在本次打开中因已有校验被判失败时，已经归档成功的依赖作业也不能继续
// 使用它：同样改判为失败；仍在排队等待失效作业的下游沿用既有依赖失败处理，
// 无关作业继续正常查询和计算。
func TestReopenFailedUpstreamInvalidatesSucceededDependent(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	up := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "up", Values: []int64{2, -3}}) // sum=-1
	waitStatus(t, s, up.ID, StatusSucceeded)
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{5},
		Dependencies: []uint64{up.ID},
	})
	doneDown := waitStatus(t, s, down.ID, StatusSucceeded) // 实际输入 [5,-1]：sum=4
	if doneDown.Archive.Sum != 4 {
		t.Fatalf("dependent result=%d, want 4", doneDown.Archive.Sum)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 篡改上游的实际输入使其在重开时被既有校验判失败；下游记录本身自洽。
	rewriteRecordEffectiveInputs(t, dir, up.ID, []int64{-3, 2})
	// 再补一个仍在排队等待下游的排队作业，以及一个无关作业。
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 90, submitter: "a", requestID: "grand",
		values: []int64{7}, dependencies: []uint64{down.ID},
		queuedAt: base,
		status:   StatusQueued,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 91, submitter: "b", requestID: "unrelated",
		values: []int64{8}, queuedAt: base,
		status: StatusQueued,
	})

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	gUp, err := s2.Get(up.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gUp.Status != StatusFailed {
		t.Fatalf("tampered upstream must fail existing checks, got %s", gUp.Status)
	}
	// 下游记录自身全套自洽，但上游已不可用，成功结果必须失效。
	assertDependencyInputFailure(t, s2, dir, down.ID, []int64{5}, []uint64{up.ID}, up.ID)

	// 仍在排队等待失效作业的下游沿用既有依赖失败处理级联失败，
	// 直接阻断者是已失效的下游作业本身。
	gGrand := waitStatus(t, s2, 90, StatusFailed)
	if gGrand.BlockerID != down.ID || !strings.Contains(gGrand.FailureReason, "作业 2") {
		t.Fatalf("queued downstream must cascade: blocker=%d reason=%q",
			gGrand.BlockerID, gGrand.FailureReason)
	}

	// 无关作业继续正常计算。
	gUnrelated := waitStatus(t, s2, 91, StatusSucceeded)
	if gUnrelated.Archive.Sum != 8 {
		t.Fatalf("unrelated job result=%d, want 8", gUnrelated.Archive.Sum)
	}
}

// 直接上游在归档中不存在（记录被移除）或恢复后不是成功状态时，成功记录
// 同样不能使用它，改判为失败并指出该上游作业号。
func TestReopenMissingOrUnsucceededUpstream(t *testing.T) {
	t.Run("missing upstream record", func(t *testing.T) {
		dir := t.TempDir()
		base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
		eff := []int64{5, 9}
		down := &storedJob{
			id: 2, submitter: "a", requestID: "down",
			values: []int64{5}, dependencies: []uint64{1}, // 作业 1 不存在
			effectiveValues: eff,
			queuedAt:        base, finishedAt: base.Add(time.Second),
			status: StatusSucceeded,
		}
		down.archive = newArchive(down, eff, 14, 106, down.finishedAt)
		writeSyntheticRecord(t, dir, down)

		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		assertDependencyInputFailure(t, s, dir, 2, []int64{5}, []uint64{1}, 1)
	})

	t.Run("upstream canceled", func(t *testing.T) {
		dir := t.TempDir()
		base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
		writeSyntheticRecord(t, dir, &storedJob{
			id: 1, submitter: "a", requestID: "up",
			values: []int64{9}, queuedAt: base, finishedAt: base.Add(time.Second),
			status: StatusCanceled,
		})
		eff := []int64{5, 9}
		down := &storedJob{
			id: 2, submitter: "a", requestID: "down",
			values: []int64{5}, dependencies: []uint64{1},
			effectiveValues: eff,
			queuedAt:        base.Add(2 * time.Second), finishedAt: base.Add(3 * time.Second),
			status: StatusSucceeded,
		}
		down.archive = newArchive(down, eff, 14, 106, down.finishedAt)
		writeSyntheticRecord(t, dir, down)

		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		assertDependencyInputFailure(t, s, dir, 2, []int64{5}, []uint64{1}, 1)
		// 上游的取消状态不受影响。
		g, err := s.Get(1)
		if err != nil {
			t.Fatal(err)
		}
		if g.Status != StatusCanceled {
			t.Fatalf("canceled upstream changed: %s", g.Status)
		}
	})
}

// 正确的单依赖、多依赖归档以及旧格式单依赖记录重开后保持原有结果、
// 摘要与校验值；无依赖作业继续遵守已有检查。
func TestReopenValidDependencyArchivesUnchanged(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{4}})     // sum=4
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{-1, 2}}) // sum=1
	waitStatus(t, s, u1.ID, StatusSucceeded)
	waitStatus(t, s, u2.ID, StatusSucceeded)
	single := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "single", Values: []int64{2},
		HasDependency: true, DependencyID: u1.ID,
	})
	multi := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "multi", Values: []int64{3},
		Dependencies: []uint64{u2.ID, u1.ID},
	})
	noDep := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "plain", Values: []int64{6, -6}})
	doneSingle := waitStatus(t, s, single.ID, StatusSucceeded)
	doneMulti := waitStatus(t, s, multi.ID, StatusSucceeded)
	doneNoDep := waitStatus(t, s, noDep.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for id, want := range map[uint64]*Job{
		single.ID: doneSingle, multi.ID: doneMulti, noDep.ID: doneNoDep,
	} {
		g, err := s2.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if g.Status != StatusSucceeded || g.Archive == nil {
			t.Fatalf("job %d must stay succeeded, got %s", id, g.Status)
		}
		if g.Archive.Sum != want.Archive.Sum ||
			g.Archive.SumOfSquares != want.Archive.SumOfSquares ||
			g.Archive.InputsDigest != want.Archive.InputsDigest ||
			g.Archive.ResultDigest != want.Archive.ResultDigest ||
			g.Archive.Log != want.Archive.Log ||
			g.Archive.Checksum != want.Archive.Checksum {
			t.Fatalf("job %d archive changed across reopen: %+v", id, g.Archive)
		}
	}
}
