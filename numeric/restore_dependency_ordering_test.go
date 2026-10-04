package numeric

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// assertIllegalOrderingFailure 校验“依赖先后关系不合法”记录重开后的统一形态：
// 失败、BlockerID 为 0、无成功归档与实际输入，原因指出保存的依赖顺序中第一个
// 不合法的作业号并说明它不能作为本作业的上游；按作业号读取与按提交人列举一致，
// 落盘记录同样已改写。
func assertIllegalOrderingFailure(t *testing.T, s *Store, id, badID, selfID uint64) {
	t.Helper()
	g, err := s.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusFailed {
		t.Fatalf("job %d must fail on illegal dependency ordering, got %s", id, g.Status)
	}
	if g.BlockerID != 0 {
		t.Fatalf("illegal ordering is the job's own error: job %d blocker=%d want 0", id, g.BlockerID)
	}
	if g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("job %d must not return archive/effective inputs: archive=%v effective=%v",
			id, g.Archive, g.EffectiveValues)
	}
	if !strings.Contains(g.FailureReason, "依赖先后关系不合法") ||
		!strings.Contains(g.FailureReason, "作业 "+itoa(badID)) ||
		!strings.Contains(g.FailureReason, "不能作为本作业的上游") {
		t.Fatalf("job %d reason=%q must name first illegal upstream %d (self=%d) as unable to precede it",
			id, g.FailureReason, badID, selfID)
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

// 规格示例：作业 7 保存了对作业 7 自身的依赖，原始序列 [0]，实际输入 [0,0]，
// 总和、平方和、日志、摘要和校验值全部自洽——重开后仍必须判失败。等待它的
// 下游按既有依赖失败规则处理，根因沿合法依赖链保留为作业 7；被引用者就是
// 它自己，不能把任何其他有效作业判成失败。
func TestReopenRejectsSelfConsistentSelfDependency(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 7：依赖 [7]（引用自己），实际输入 [0,0]（sum=0、sq=0，全套自洽）。
	self := syntheticSucceededRecord(t, dir, 7, []int64{0}, []int64{0, 0}, []uint64{7}, base)
	if self.archive.Sum != 0 || self.archive.SumOfSquares != 0 {
		t.Fatalf("self-dep premise: sum=%d sq=%d want 0,0", self.archive.Sum, self.archive.SumOfSquares)
	}
	// 作业 8：合法排队下游，等待作业 7。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 8, submitter: "a", requestID: "child",
		values: []int64{20}, dependencies: []uint64{7},
		queuedAt: base.Add(8 * time.Second),
		status:   StatusQueued,
	})
	// 作业 9：更下游，只依赖作业 8，根因应沿合法链条保留为作业 7。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 9, submitter: "a", requestID: "grandchild",
		values: []int64{30}, dependencies: []uint64{8},
		queuedAt: base.Add(9 * time.Second),
		status:   StatusQueued,
	})
	// 作业 10：无关排队作业，重开后照常完成。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 10, submitter: "b", requestID: "indep",
		values:   []int64{1, 2, 3},
		queuedAt: base.Add(10 * time.Second),
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	assertIllegalOrderingFailure(t, s, 7, 7, 7)

	// 原始提交内容与已有时间信息保留；成功记录已有完成时间，不重新填写。
	g7, _ := s.Get(7)
	if len(g7.Values) != 1 || g7.Values[0] != 0 || len(g7.Dependencies) != 1 || g7.Dependencies[0] != 7 {
		t.Fatalf("self-dep record must keep original submission content: values=%v deps=%v",
			g7.Values, g7.Dependencies)
	}
	if !g7.QueuedAt.Equal(base.Add(7*time.Second)) ||
		!g7.StartedAt.Equal(base.Add(7*time.Second+time.Millisecond)) ||
		!g7.FinishedAt.Equal(base.Add(8*time.Second)) {
		t.Fatalf("self-dep record must keep existing timestamps: queued=%s started=%s finished=%s",
			g7.QueuedAt, g7.StartedAt, g7.FinishedAt)
	}

	// 下游按既有依赖失败规则处理：直接上游 7，根因也是 7；不能继续等待或产生归档。
	child := waitStatus(t, s, 8, StatusFailed)
	if child.BlockerID != 7 {
		t.Fatalf("child blocker=%d want root 7 (the illegal record itself)", child.BlockerID)
	}
	if !strings.Contains(child.FailureReason, "直接上游作业 7") ||
		!strings.Contains(child.FailureReason, "依赖先后关系不合法") {
		t.Fatalf("child must name direct upstream 7 and carry its ordering reason: %q",
			child.FailureReason)
	}
	if child.Archive != nil || !child.StartedAt.IsZero() {
		t.Fatalf("blocked child must never compute: started=%s archive=%v",
			child.StartedAt, child.Archive)
	}
	// 根因沿合法依赖链保留：作业 9 的直接上游是 8，根因仍是 7。
	grand := waitStatus(t, s, 9, StatusFailed)
	if grand.BlockerID != 7 {
		t.Fatalf("grandchild blocker=%d want root 7 preserved along legal chain", grand.BlockerID)
	}
	if !strings.Contains(grand.FailureReason, "直接上游作业 8") ||
		!strings.Contains(grand.FailureReason, "根因为作业 7") {
		t.Fatalf("grandchild must name direct upstream 8 and root 7: %q", grand.FailureReason)
	}

	// 无关作业照常成功。
	indep := waitStatus(t, s, 10, StatusSucceeded)
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
	assertIllegalOrderingFailure(t, s2, 7, 7, 7)
	for _, id := range []uint64{8, 9} {
		g := waitStatus(t, s2, id, StatusFailed)
		if g.BlockerID != 7 {
			t.Fatalf("after second reopen job %d blocker=%d want 7", id, g.BlockerID)
		}
	}
}

// 成功记录引用后来的作业（依赖作业号大于自己），即使被引用作业确实存在且已经
// 成功、追加值与全套数值字段都自洽，重开后仍改判失败，BlockerID 为 0；
// 被引用的有效上游保留自己的结果，等待该错误记录的下游级联失败并以它为根因。
func TestReopenRejectsSucceededRecordReferencingLaterJob(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 1 总和 3，作业 5 总和 5。
	syntheticSucceededRecord(t, dir, 1, []int64{1, 2}, []int64{1, 2}, nil, base)
	syntheticSucceededRecord(t, dir, 5, []int64{5}, []int64{5}, nil, base)
	// 作业 3：依赖 [5]（后来作业），实际输入 [10,5]，sum=15、sq=125，全套自洽。
	bad := syntheticSucceededRecord(t, dir, 3, []int64{10}, []int64{10, 5}, []uint64{5}, base)
	if bad.archive.Sum != 15 || bad.archive.SumOfSquares != 125 {
		t.Fatalf("premise result=%d,%d want 15,125", bad.archive.Sum, bad.archive.SumOfSquares)
	}
	// 作业 6：排队等待作业 3。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 6, submitter: "a", requestID: "child",
		values: []int64{20}, dependencies: []uint64{3},
		queuedAt: base.Add(6 * time.Second),
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	assertIllegalOrderingFailure(t, s, 3, 5, 3)

	// 被引用的后来作业与无关上游都保留成功结果，不被判成失败。
	g1, _ := s.Get(1)
	if g1.Status != StatusSucceeded || g1.Archive == nil || g1.Archive.Sum != 3 {
		t.Fatalf("valid job 1 must keep its result: %+v", g1.Archive)
	}
	g5, _ := s.Get(5)
	if g5.Status != StatusSucceeded || g5.Archive == nil || g5.Archive.Sum != 5 {
		t.Fatalf("referenced later job 5 exists and succeeded; it must keep its result: %+v", g5.Archive)
	}

	child := waitStatus(t, s, 6, StatusFailed)
	if child.BlockerID != 3 || !strings.Contains(child.FailureReason, "直接上游作业 3") {
		t.Fatalf("child blocker=%d reason=%q want root 3 named as direct upstream",
			child.BlockerID, child.FailureReason)
	}
}

// 多个违反先后关系的引用时，失败原因指出保存的依赖顺序中第一个不合法作业号：
// 作业 7 的依赖为 [1,9,5,8]，1 合法，9 是第一个作业号不小于 7 的上游；
// 被引用的 8、9 都确实存在且成功，仍不能接受。
func TestReopenIllegalOrderingNamesFirstBadDependencyInSavedOrder(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	syntheticSucceededRecord(t, dir, 1, []int64{1, 2}, []int64{1, 2}, nil, base)
	syntheticSucceededRecord(t, dir, 5, []int64{5}, []int64{5}, nil, base)
	syntheticSucceededRecord(t, dir, 8, []int64{8}, []int64{8}, nil, base)
	syntheticSucceededRecord(t, dir, 9, []int64{9}, []int64{9}, nil, base)
	// 作业 7：依赖次序 [1,9,5,8]，追加各上游总和 [3,9,5,8]，全套自洽。
	syntheticSucceededRecord(t, dir, 7,
		[]int64{10}, []int64{10, 3, 9, 5, 8}, []uint64{1, 9, 5, 8}, base)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	assertIllegalOrderingFailure(t, s, 7, 9, 7)
	// 被引用的作业全部保留自己的成功结果。
	for _, id := range []uint64{1, 5, 8, 9} {
		g, _ := s.Get(id)
		if g.Status != StatusSucceeded || g.Archive == nil {
			t.Fatalf("referenced job %d must stay succeeded: status=%s", id, g.Status)
		}
	}
}

// 排队记录引用后来作业或自己时，重开后立即失败：不能开始计算（无开始时间）、
// 补上完成时间、落盘改写，也不能继续等待后来作业——即使后来作业随后成功。
// 等待该错误记录的合法下游随之级联失败；无关排队作业照常完成。
func TestReopenQueuedIllegalDependencyFailsWithoutWaiting(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 2：排队引用后来的作业 3（作业 3 本身也在排队）。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "forward",
		values: []int64{10}, dependencies: []uint64{3},
		queuedAt: base.Add(2 * time.Second),
		status:   StatusQueued,
	})
	// 作业 3：无依赖排队，重开后应照常计算成功——作业 2 不能等它。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "later",
		values:   []int64{5},
		queuedAt: base.Add(3 * time.Second),
		status:   StatusQueued,
	})
	// 作业 5：排队引用自己。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 5, submitter: "a", requestID: "self",
		values: []int64{50}, dependencies: []uint64{5},
		queuedAt: base.Add(5 * time.Second),
		status:   StatusQueued,
	})
	// 作业 6：合法排队下游，等待作业 5。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 6, submitter: "a", requestID: "child",
		values: []int64{60}, dependencies: []uint64{5},
		queuedAt: base.Add(6 * time.Second),
		status:   StatusQueued,
	})
	// 作业 7：无关排队作业。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 7, submitter: "b", requestID: "indep",
		values:   []int64{1, 2, 3},
		queuedAt: base.Add(7 * time.Second),
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	assertIllegalOrderingFailure(t, s, 2, 3, 2)
	g2, _ := s.Get(2)
	if !g2.StartedAt.IsZero() {
		t.Fatalf("forward-referencing queued job must never start computing, started=%s", g2.StartedAt)
	}
	if g2.FinishedAt.IsZero() {
		t.Fatalf("forward-referencing queued job must be given a finished time when rejudged")
	}
	// 后来作业随后成功，作业 2 也不得翻案。
	later := waitStatus(t, s, 3, StatusSucceeded)
	if later.Archive.Sum != 5 {
		t.Fatalf("later job sum=%d want 5", later.Archive.Sum)
	}
	g2b, _ := s.Get(2)
	if g2b.Status != StatusFailed {
		t.Fatalf("job 2 must stay failed after job 3 succeeded, got %s", g2b.Status)
	}

	assertIllegalOrderingFailure(t, s, 5, 5, 5)
	child := waitStatus(t, s, 6, StatusFailed)
	if child.BlockerID != 5 || !strings.Contains(child.FailureReason, "直接上游作业 5") {
		t.Fatalf("child blocker=%d reason=%q want root 5 named as direct upstream",
			child.BlockerID, child.FailureReason)
	}
	if !child.StartedAt.IsZero() || child.Archive != nil {
		t.Fatalf("blocked child must never compute: started=%s archive=%v",
			child.StartedAt, child.Archive)
	}

	indep := waitStatus(t, s, 7, StatusSucceeded)
	if indep.Archive.Sum != 6 {
		t.Fatalf("unrelated job sum=%d want 6", indep.Archive.Sum)
	}
}

// 已经失败或取消的记录即使保存了违反先后关系的依赖，也保留既有终态、原因与
// 根因，不被恢复阶段的先后检查改写。
func TestReopenIllegalOrderingKeepsExistingTerminalRecords(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	syntheticSucceededRecord(t, dir, 1, []int64{1}, []int64{1}, nil, base)
	failedAt := base.Add(2 * time.Second)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "f",
		values: []int64{10}, dependencies: []uint64{5}, // 引用后来作业
		queuedAt: failedAt, finishedAt: failedAt,
		status:        StatusFailed,
		failureReason: "既有的失败原因",
		blockerID:     9,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "c",
		values: []int64{11}, dependencies: []uint64{3}, // 引用自己
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

// 同时违反唯一性与先后关系时，保留现有的重复依赖失败说明（重复检查先行）。
func TestReopenDuplicateCheckTakesPrecedenceOverOrdering(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	syntheticSucceededRecord(t, dir, 9, []int64{9}, []int64{9}, nil, base)
	// 作业 3：依赖 [9,9]——既重复引用，作业号 9 又大于自己；原因必须是重复说明。
	syntheticSucceededRecord(t, dir, 3,
		[]int64{10}, []int64{10, 9, 9}, []uint64{9, 9}, base)

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
		t.Fatalf("job 3 must fail with blocker 0: status=%s blocker=%d", g.Status, g.BlockerID)
	}
	if !strings.Contains(g.FailureReason, "依赖列表重复") ||
		strings.Contains(g.FailureReason, "依赖先后关系不合法") {
		t.Fatalf("duplicate reason must be preserved over ordering reason: %q", g.FailureReason)
	}
}

// 旧的单依赖记录（落盘只有 has_dependency/dependency_id）适用同一规则：
// dependency_id 等于或大于本作业号时同样改判失败。
func TestReopenLegacySingleDependencyObeysOrderingRule(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 6：旧格式成功记录，单依赖指向自己（实际输入 [0,0]，全套自洽）。
	j6 := &storedJob{
		id: 6, submitter: "a", requestID: "legacy-self",
		values:     []int64{0},
		queuedAt:   base.Add(6 * time.Second),
		startedAt:  base.Add(6*time.Second + time.Millisecond),
		finishedAt: base.Add(7 * time.Second),
		status:     StatusSucceeded,
	}
	eff6 := []int64{0, 0}
	sum, sumSq, _, ok := computeResult(eff6, 0, nil)
	if !ok {
		t.Fatal("eff overflows")
	}
	a6 := newArchive(&storedJob{
		id: 6, submitter: "a", requestID: "legacy-self",
		values: []int64{0}, dependencies: []uint64{6},
	}, eff6, sum, sumSq, j6.finishedAt)
	r6 := jobRecord{
		Version: recordVersion,
		ID:      6, Submitter: "a", RequestID: "legacy-self",
		// Dependencies 刻意留空（omitempty 后落盘无此字段）。
		HasDependency: true, DependencyID: 6,
		QueuedAt: j6.queuedAt, StartedAt: j6.startedAt, FinishedAt: j6.finishedAt,
		Status:          StatusSucceeded,
		EffectiveValues: append([]int64(nil), eff6...),
		Archive:         a6,
	}
	writeRawRecord(t, dir, 6, &r6)

	// 作业 7：旧格式排队记录，单依赖指向后来的作业 8。
	r7 := jobRecord{
		Version: recordVersion,
		ID:      7, Submitter: "a", RequestID: "legacy-forward",
		Values: []int64{0},
		// Dependencies 刻意留空。
		HasDependency: true, DependencyID: 8,
		QueuedAt: base.Add(7 * time.Second),
		Status:   StatusQueued,
	}
	writeRawRecord(t, dir, 7, &r7)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	assertIllegalOrderingFailure(t, s, 6, 6, 6)
	assertIllegalOrderingFailure(t, s, 7, 8, 7)
	g7, _ := s.Get(7)
	// 旧格式字段恢复后依赖列表恰含一个作业号，原始内容保留。
	if len(g7.Dependencies) != 1 || g7.Dependencies[0] != 8 ||
		len(g7.Values) != 1 || g7.Values[0] != 0 || !g7.HasDependency || g7.DependencyID != 8 {
		t.Fatalf("legacy queued record must keep the reconstructed single dependency: %+v", g7)
	}
}

// 不要求依赖列表按作业号排序：所有直接上游作业号都小于自己时，乱序列表
// （如 [3,1,2]）仍属合法，重开后保持成功，追加仍按保存的原次序，结果不变。
func TestReopenUnsortedButEarlierDependenciesRemainLegal(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	syntheticSucceededRecord(t, dir, 1, []int64{1}, []int64{1}, nil, base) // sum 1
	syntheticSucceededRecord(t, dir, 2, []int64{2}, []int64{2}, nil, base) // sum 2
	syntheticSucceededRecord(t, dir, 3, []int64{3}, []int64{3}, nil, base) // sum 3
	// 作业 5：依赖次序 [3,1,2]（乱序但作业号都小于 5），追加 [3,1,2]。
	down := syntheticSucceededRecord(t, dir, 5,
		[]int64{10}, []int64{10, 3, 1, 2}, []uint64{3, 1, 2}, base)

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
		t.Fatalf("unsorted but earlier upstreams must stay legal: %s", g.Status)
	}
	if g.Archive.Sum != down.archive.Sum || g.Archive.SumOfSquares != down.archive.SumOfSquares ||
		g.Archive.Checksum != down.archive.Checksum {
		t.Fatalf("legal unsorted result changed: got %+v want %+v", g.Archive, down.archive)
	}
	if len(g.Dependencies) != 3 || g.Dependencies[0] != 3 || g.Dependencies[1] != 1 || g.Dependencies[2] != 2 {
		t.Fatalf("dependency order must be preserved: %v", g.Dependencies)
	}
}

// writeRawRecord 直接把给定记录结构序列化落盘，用于构造旧格式单依赖记录。
func writeRawRecord(t *testing.T, dir string, id uint64, r *jobRecord) {
	t.Helper()
	out, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(dir, jobFileName(id), out, 0o600); err != nil {
		t.Fatal(err)
	}
}
