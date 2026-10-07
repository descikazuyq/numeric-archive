package numeric

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 本文件覆盖“保存状态已经是失败或取消的记录，重开后只清除残留的成功归档与
// 实际参与计算的输入、维持原终态”的恢复规则。这些记录可能由旧版本写出或被
// 改动：状态字段已是 failed/canceled，却仍带着 effective_values、archive
// （只带其中之一或两者皆有），且归档可以全套自洽——读取必须维持“只有成功
// 作业才提供完整归档”的约定，不能把残留成功结果带出给调用方。

// residualEffective / residualArchive 位掩码控制终态记录里残留哪些成功产物。
const (
	residualNone      = 0
	residualEffective = 1 << iota
	residualArchive
)

// writeResidualTerminalRecord 把一条保存状态为终态（failed/canceled）却带残留
// 成功产物的记录写入 dir/name。residual 控制残留实际输入与归档的组合；归档按
// eff、j.seed 与既有计算规则构造，数值、摘要、日志与校验值全部自洽——用来
// 证明“自洽残留归档”也不能让终态记录翻案为成功。
func writeResidualTerminalRecord(t *testing.T, dir, name string, j *storedJob, eff []int64, residual int) {
	t.Helper()
	sum, sumSq, _, ok := computeResult(eff, j.seed, nil)
	if !ok {
		t.Fatalf("residual effective %v overflows", eff)
	}
	if residual&residualEffective != 0 {
		j.effectiveValues = append([]int64(nil), eff...)
	}
	if residual&residualArchive != 0 {
		j.archive = newArchive(j, eff, sum, sumSq, j.finishedAt)
	}
	data, err := encodeRecord(j)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(dir, name, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// assertNoSuccessData 校验按作业号读取与按提交人列举都不返回成功归档与实际
// 输入，且两处状态一致；返回 Get 视图供进一步断言原终态信息。
func assertNoSuccessData(t *testing.T, s *Store, id uint64) *Job {
	t.Helper()
	g, err := s.Get(id)
	if err != nil {
		t.Fatalf("get %d: %v", id, err)
	}
	if g.Archive != nil {
		t.Fatalf("terminal job %d must not return a success archive: %+v", id, g.Archive)
	}
	if g.EffectiveValues != nil {
		t.Fatalf("terminal job %d must not return effective inputs: %v", id, g.EffectiveValues)
	}
	listed, err := s.List(g.Submitter, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	for _, lj := range listed {
		if lj.ID != id {
			continue
		}
		if lj.Status != g.Status || lj.FailureReason != g.FailureReason ||
			lj.BlockerID != g.BlockerID || lj.Archive != nil || lj.EffectiveValues != nil {
			t.Fatalf("listed job %d disagrees with Get after sanitization: "+
				"list=%s/%q/%d archive=%v effective=%v; get=%s/%q/%d",
				id, lj.Status, lj.FailureReason, lj.BlockerID, lj.Archive, lj.EffectiveValues,
				g.Status, g.FailureReason, g.BlockerID)
		}
		return g
	}
	t.Fatalf("terminal job %d missing from submitter listing", id)
	return nil
}

// assertOnDiskNoSuccessData 校验落盘记录已清除归档与实际输入，但仍是原终态。
func assertOnDiskNoSuccessData(t *testing.T, dir, fileName string, wantStatus Status) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, fileName))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if strings.Contains(text, `"archive"`) {
		t.Fatalf("on-disk record must not retain archive:\n%s", text)
	}
	if strings.Contains(text, `"effective_values"`) {
		t.Fatalf("on-disk record must not retain effective inputs:\n%s", text)
	}
	want := `"status": "` + string(wantStatus) + `"`
	if !strings.Contains(text, want) {
		t.Fatalf("on-disk record must keep status %s:\n%s", wantStatus, text)
	}
}

// 失败或取消的终态记录无论只残留实际输入、只残留归档还是两者都有，重开后都
// 只呈现原终态：成功归档与实际输入均为空；作业号、提交人、请求号、原始整数
// 及次序、种子、有序依赖与已有时间保持不变；失败原因与阻断根因不被改写；
// Get、List 与落盘记录一致，再次重开保持稳定。
func TestReopenTerminalRecordsSanitizeResidualSuccessData(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	values := []int64{1, 2, 3, 4} // 原始提交参数（所有记录相同）
	// 带依赖记录的残留实际输入也按“原始序列 + 每个直接上游一个追加位置”的
	// 最强自洽形态构造（上游 9 实际不存在也无所谓——终态记录不复核归档，
	// 只丢弃），证明即使归档与参数、依赖数量全部对应也不翻案。
	specs := []struct {
		id        uint64
		submitter string
		requestID string
		status    Status
		residual  int
		eff       []int64
		deps      []uint64
		reason    string
		blocker   uint64
	}{
		{1, "a", "f-eff", StatusFailed, residualEffective, []int64{1, 2, 3, 4}, nil, "既有的失败原因 1", 0},
		{2, "a", "c-eff", StatusCanceled, residualEffective, []int64{1, 2, 3, 4}, nil, "", 0},
		{3, "a", "f-arc", StatusFailed, residualArchive, []int64{1, 2, 3, 4}, nil, "既有的失败原因 3", 7},
		{4, "b", "c-arc", StatusCanceled, residualArchive, []int64{1, 2, 3, 4}, nil, "", 0},
		{5, "b", "f-both", StatusFailed, residualEffective | residualArchive, []int64{1, 2, 3, 4, 42}, []uint64{9}, "既有的失败原因 5", 7},
		{6, "b", "c-both", StatusCanceled, residualEffective | residualArchive, []int64{1, 2, 3, 4, 42}, []uint64{9}, "", 0},
	}
	for _, sp := range specs {
		j := &storedJob{
			id: sp.id, submitter: sp.submitter, requestID: sp.requestID,
			seed: 11, values: append([]int64(nil), values...), dependencies: sp.deps,
			queuedAt:      base.Add(time.Duration(sp.id) * time.Second),
			startedAt:     base.Add(time.Duration(sp.id)*time.Second + time.Millisecond),
			finishedAt:    base.Add(time.Duration(sp.id+1) * time.Second),
			status:        sp.status,
			failureReason: sp.reason,
			blockerID:     sp.blocker,
		}
		writeResidualTerminalRecord(t, dir, jobFileName(sp.id), j, sp.eff, sp.residual)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, sp := range specs {
		g := assertNoSuccessData(t, s, sp.id)
		if g.Status != sp.status {
			t.Fatalf("job %d status changed: %s want %s", sp.id, g.Status, sp.status)
		}
		if g.FailureReason != sp.reason || g.BlockerID != sp.blocker {
			t.Fatalf("job %d failure info changed: reason=%q blocker=%d want %q/%d",
				sp.id, g.FailureReason, g.BlockerID, sp.reason, sp.blocker)
		}
		if g.Submitter != sp.submitter || g.RequestID != sp.requestID || g.Seed != 11 {
			t.Fatalf("job %d identity/seed changed: %q %q seed=%d",
				sp.id, g.Submitter, g.RequestID, g.Seed)
		}
		// 原始整数序列属于提交参数，必须原样保留，不能被当作实际输入清空。
		if len(g.Values) != len(values) {
			t.Fatalf("job %d original values changed: %v", sp.id, g.Values)
		}
		for i := range values {
			if g.Values[i] != values[i] {
				t.Fatalf("job %d original values/order changed at %d: %v", sp.id, i, g.Values)
			}
		}
		if len(g.Dependencies) != len(sp.deps) {
			t.Fatalf("job %d dependencies changed: %v want %v", sp.id, g.Dependencies, sp.deps)
		}
		for i := range sp.deps {
			if g.Dependencies[i] != sp.deps[i] {
				t.Fatalf("job %d dependency order changed: %v want %v",
					sp.id, g.Dependencies, sp.deps)
			}
		}
		wantQueued := base.Add(time.Duration(sp.id) * time.Second)
		wantStarted := base.Add(time.Duration(sp.id)*time.Second + time.Millisecond)
		wantFinished := base.Add(time.Duration(sp.id+1) * time.Second)
		if !g.QueuedAt.Equal(wantQueued) || !g.StartedAt.Equal(wantStarted) ||
			!g.FinishedAt.Equal(wantFinished) {
			t.Fatalf("job %d timestamps changed: queued=%s started=%s finished=%s want %s/%s/%s",
				sp.id, g.QueuedAt, g.StartedAt, g.FinishedAt, wantQueued, wantStarted, wantFinished)
		}
		assertOnDiskNoSuccessData(t, dir, jobFileName(sp.id), sp.status)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 再次重开：清理已落盘，状态、原因与根因保持稳定，不会出现任何残留回潮。
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for _, sp := range specs {
		g := assertNoSuccessData(t, s2, sp.id)
		if g.Status != sp.status || g.FailureReason != sp.reason || g.BlockerID != sp.blocker {
			t.Fatalf("job %d terminal info drifted on second reopen: %s %q %d",
				sp.id, g.Status, g.FailureReason, g.BlockerID)
		}
		assertOnDiskNoSuccessData(t, dir, jobFileName(sp.id), sp.status)
	}
}

// 终态记录中的残留归档即使数值、摘要、日志与校验值全部自洽，也不能成为把
// 作业恢复为成功的依据：失败仍是原来的失败（原因与根因不变），归档不返回；
// 同目录同参数的合法成功记录照常成功，证明这不是“校验不过”才被拒绝。
func TestReopenResidualSelfConsistentArchiveStaysTerminal(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	eff := []int64{2, -3} // 总和 -1、平方和 13
	failedAt := base.Add(2 * time.Second)
	j := &storedJob{
		id: 1, submitter: "a", requestID: "self-consistent",
		seed: 5, values: append([]int64(nil), eff...),
		queuedAt: base.Add(time.Second), startedAt: failedAt, finishedAt: failedAt,
		status:        StatusFailed,
		failureReason: "原始失败原因：终态记录保持此原因",
		blockerID:     4,
	}
	writeResidualTerminalRecord(t, dir, jobFileName(1), j, eff,
		residualEffective|residualArchive)
	// 同参数、同种子的合法成功记录（作业 2），其校验值必然与残留归档一致。
	ok := syntheticSucceededRecord(t, dir, 2, eff, eff, nil, base.Add(10*time.Second))

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	g := assertNoSuccessData(t, s, 1)
	if g.Status != StatusFailed || g.BlockerID != 4 ||
		g.FailureReason != "原始失败原因：终态记录保持此原因" {
		t.Fatalf("failed record must keep its terminal state and reason: %s %q blocker=%d",
			g.Status, g.FailureReason, g.BlockerID)
	}
	if !strings.Contains(g.FailureReason, "原始失败原因") ||
		strings.Contains(g.FailureReason, "归档") {
		t.Fatalf("failed job must not be re-reasoned as an archive failure: %q", g.FailureReason)
	}
	assertOnDiskNoSuccessData(t, dir, jobFileName(1), StatusFailed)

	// 对照：自洽归档挂在成功状态下照常接受，且与被清理的残留归档本应同值。
	g2, _ := s.Get(2)
	if g2.Status != StatusSucceeded || g2.Archive == nil {
		t.Fatalf("valid succeeded record must still restore with archive: %s", g2.Status)
	}
	if g2.Archive.Sum != -1 || g2.Archive.SumOfSquares != 13 {
		t.Fatalf("control job results=%d,%d want -1,13", g2.Archive.Sum, g2.Archive.SumOfSquares)
	}
	if ok.archive.Checksum != g2.Archive.Checksum {
		t.Fatal("residual archive was self-consistent; checksums must match")
	}
}

// 非空请求号仍属于原终态作业：同一提交人再次提交相同内容时返回原终态详情，
// 同样不含成功归档与实际输入，不重算（开始时间保持）、不创建另一份作业。
func TestReopenTerminalResidualIdempotentReplayStaysTerminal(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	values := []int64{1, 2, 3, 4}
	failedAt := base.Add(2 * time.Second)
	jf := &storedJob{
		id: 1, submitter: "a", requestID: "idem-f",
		seed: 6, values: append([]int64(nil), values...),
		queuedAt: base.Add(time.Second), startedAt: failedAt, finishedAt: failedAt,
		status:        StatusFailed,
		failureReason: "终态失败原因",
	}
	writeResidualTerminalRecord(t, dir, jobFileName(1), jf, values,
		residualEffective|residualArchive)
	jc := &storedJob{
		id: 2, submitter: "a", requestID: "idem-c",
		seed: 6, values: append([]int64(nil), values...),
		queuedAt: base.Add(3 * time.Second), finishedAt: base.Add(4 * time.Second),
		status: StatusCanceled,
	}
	writeResidualTerminalRecord(t, dir, jobFileName(2), jc, values,
		residualEffective|residualArchive)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	replay, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "idem-f", Values: append([]int64(nil), values...), Seed: 6,
	})
	if err != nil {
		t.Fatalf("idempotent replay of failed job must not error: %v", err)
	}
	if replay.ID != 1 || replay.Status != StatusFailed ||
		replay.Archive != nil || replay.EffectiveValues != nil {
		t.Fatalf("replay must return the sanitized failed job: id=%d status=%s archive=%v eff=%v",
			replay.ID, replay.Status, replay.Archive, replay.EffectiveValues)
	}
	if !replay.StartedAt.Equal(failedAt) {
		t.Fatalf("replay must not recompute: started=%s want %s", replay.StartedAt, failedAt)
	}

	replayC, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "idem-c", Values: append([]int64(nil), values...), Seed: 6,
	})
	if err != nil || replayC.ID != 2 || replayC.Status != StatusCanceled ||
		replayC.Archive != nil || replayC.EffectiveValues != nil {
		t.Fatalf("canceled replay must return the sanitized canceled job: %v %+v", err, replayC)
	}

	// 重放不创建另一份作业：目录中该提交人仍只有原来两条终态记录。
	if listed, _ := s.List("a", time.Time{}, time.Time{}); len(listed) != 2 {
		t.Fatalf("replay must not create records: %d jobs", len(listed))
	}
	// 编号未被重放消耗：下一个新请求取得 3 并正常计算，终态旧作业不受影响。
	fresh := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{9}})
	if fresh.ID != 3 {
		t.Fatalf("idempotent replay consumed an id: new job id=%d want 3", fresh.ID)
	}
	waitStatus(t, s, 3, StatusSucceeded)
	stillFailed := assertNoSuccessData(t, s, 1)
	if stillFailed.Status != StatusFailed || stillFailed.FailureReason != "终态失败原因" {
		t.Fatalf("old failed job changed after new submit: %s %q",
			stillFailed.Status, stillFailed.FailureReason)
	}
}

// 等待残留终态作业的下游继续遵守已有阻断规则，不能利用残留结果开始计算：
// 排队子作业与再下游都按既有规则失败，根因沿依赖链保留为残留记录自身；
// 子作业从未进入运行、没有任何成功产物。
func TestReopenTerminalResidualDoesNotUnblockDownstream(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 1：失败终态，却带着与 [5] 全套自洽的成功归档——若按残留结果放行，
	// 作业 2 会把总和 5 追加到 [10] 后算出“成功”结果。必须阻断。
	j1 := &storedJob{
		id: 1, submitter: "a", requestID: "root",
		seed: 0, values: []int64{5},
		queuedAt: base.Add(time.Second), startedAt: base.Add(2 * time.Second),
		finishedAt:    base.Add(2 * time.Second),
		status:        StatusFailed,
		failureReason: "根因失败",
	}
	writeResidualTerminalRecord(t, dir, jobFileName(1), j1, []int64{5},
		residualEffective|residualArchive)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "child",
		values:       []int64{10},
		dependencies: []uint64{1},
		queuedAt:     base.Add(3 * time.Second),
		status:       StatusQueued,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "grandchild",
		values:       []int64{20},
		dependencies: []uint64{2},
		queuedAt:     base.Add(4 * time.Second),
		status:       StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	root := assertNoSuccessData(t, s, 1)
	if root.Status != StatusFailed || root.FailureReason != "根因失败" {
		t.Fatalf("residual root must keep its failure: %s %q", root.Status, root.FailureReason)
	}
	child := waitStatus(t, s, 2, StatusFailed)
	if child.BlockerID != 1 || !strings.Contains(child.FailureReason, "直接上游作业 1") {
		t.Fatalf("child must be blocked by job 1: blocker=%d reason=%q",
			child.BlockerID, child.FailureReason)
	}
	if child.Archive != nil || child.EffectiveValues != nil || !child.StartedAt.IsZero() {
		t.Fatalf("child must never compute from residual results: started=%s archive=%v eff=%v",
			child.StartedAt, child.Archive, child.EffectiveValues)
	}
	grand := waitStatus(t, s, 3, StatusFailed)
	if grand.BlockerID != 1 ||
		!strings.Contains(grand.FailureReason, "直接上游作业 2") ||
		!strings.Contains(grand.FailureReason, "阻断根因为作业 1") {
		t.Fatalf("grandchild must preserve root 1 along the chain: blocker=%d reason=%q",
			grand.BlockerID, grand.FailureReason)
	}
	if grand.Archive != nil || !grand.StartedAt.IsZero() {
		t.Fatalf("grandchild must never compute: started=%s archive=%v",
			grand.StartedAt, grand.Archive)
	}
}

// 原始整数序列属于提交参数：已经失败或取消的记录即使原始序列为空（且残留了
// 实际输入/归档），也保留空参数与原终态，不补造参数、不换成“原始整数序列
// 为空”的新失败原因，取消记录也不因此改判为失败。
func TestReopenTerminalResidualWithEmptyOriginalValuesKeepsTerminal(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	failedAt := base.Add(2 * time.Second)
	jf := &storedJob{
		id: 1, submitter: "a", requestID: "f-empty",
		queuedAt: base.Add(time.Second), startedAt: failedAt, finishedAt: failedAt,
		status:        StatusFailed,
		failureReason: "既有的失败原因",
		blockerID:     3,
	}
	// 原始 values 为空，却残留了实际输入 [7] 与全套自洽归档。
	writeResidualTerminalRecord(t, dir, jobFileName(1), jf, []int64{7},
		residualEffective|residualArchive)
	jc := &storedJob{
		id: 2, submitter: "a", requestID: "c-empty",
		queuedAt: base.Add(3 * time.Second), finishedAt: base.Add(4 * time.Second),
		status: StatusCanceled,
	}
	writeResidualTerminalRecord(t, dir, jobFileName(2), jc, []int64{7},
		residualEffective)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	gf := assertNoSuccessData(t, s, 1)
	if gf.Status != StatusFailed || gf.BlockerID != 3 ||
		gf.FailureReason != "既有的失败原因" {
		t.Fatalf("failed record must be preserved: %s blocker=%d reason=%q",
			gf.Status, gf.BlockerID, gf.FailureReason)
	}
	if strings.Contains(gf.FailureReason, "原始整数序列为空") {
		t.Fatalf("existing failure must not gain the empty-values reason: %q", gf.FailureReason)
	}
	if len(gf.Values) != 0 {
		t.Fatalf("empty original values must stay empty, got %v", gf.Values)
	}
	gc := assertNoSuccessData(t, s, 2)
	if gc.Status != StatusCanceled || gc.BlockerID != 0 || gc.FailureReason != "" {
		t.Fatalf("canceled record must stay canceled: %s blocker=%d reason=%q",
			gc.Status, gc.BlockerID, gc.FailureReason)
	}
	if len(gc.Values) != 0 {
		t.Fatalf("empty original values must stay empty, got %v", gc.Values)
	}
}

// 清理落盘维持既有尽力而为语义：目录暂时不可写时打开仍成功，内存中的查询
// 结果已不含残留；写入恢复后再次打开，残留才从落盘记录中清除。
func TestReopenTerminalSanitizationPersistsBestEffort(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	j := &storedJob{
		id: 1, submitter: "a", requestID: "ro",
		seed: 0, values: []int64{1, 2},
		queuedAt: base.Add(time.Second), startedAt: base.Add(2 * time.Second),
		finishedAt:    base.Add(2 * time.Second),
		status:        StatusFailed,
		failureReason: "既有的失败原因",
	}
	writeResidualTerminalRecord(t, dir, jobFileName(1), j, []int64{1, 2},
		residualEffective|residualArchive)

	// 目录只读：清理无法原子替换原记录，但打开不能因此失败。
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("open must succeed even when sanitization cannot persist: %v", err)
	}
	g := assertNoSuccessData(t, s, 1) // 内存查询结果已清理
	if g.FailureReason != "既有的失败原因" {
		t.Fatalf("reason changed: %q", g.FailureReason)
	}
	// 落盘仍是旧内容（含残留）——尽力而为，不影响打开。
	data, err := os.ReadFile(filepath.Join(dir, jobFileName(1)))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"archive"`) {
		t.Fatal("read-only directory must leave the residual record untouched on disk")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 写入恢复后再次打开：这一趟清理成功落盘。
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	g2 := assertNoSuccessData(t, s2, 1)
	if g2.Status != StatusFailed || g2.FailureReason != "既有的失败原因" {
		t.Fatalf("terminal state must be stable: %s %q", g2.Status, g2.FailureReason)
	}
	assertOnDiskNoSuccessData(t, dir, jobFileName(1), StatusFailed)
}

// 没有残留的失败、取消记录与合法成功记录不触发改写：终态记录保持原内容，
// 成功记录保持带完整归档的成功状态。
func TestReopenCleanTerminalAndSucceededRecordsAreNotRewritten(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	failedAt := base.Add(2 * time.Second)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "f-clean",
		seed: 1, values: []int64{1},
		queuedAt: base.Add(time.Second), startedAt: failedAt, finishedAt: failedAt,
		status:        StatusFailed,
		failureReason: "既有的失败原因",
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "c-clean",
		seed: 2, values: []int64{2},
		queuedAt: base.Add(3 * time.Second), finishedAt: base.Add(4 * time.Second),
		status: StatusCanceled,
	})
	syntheticSucceededRecord(t, dir, 3, []int64{1, 2}, []int64{1, 2}, nil, base.Add(5*time.Second))

	// 把三份记录的修改时间拨到过去：恢复若进行了任何写回，mtime 必然前进。
	old := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, id := range []uint64{1, 2, 3} {
		if err := os.Chtimes(filepath.Join(dir, jobFileName(id)), old, old); err != nil {
			t.Fatal(err)
		}
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, id := range []uint64{1, 2, 3} {
		fi, err := os.Stat(filepath.Join(dir, jobFileName(id)))
		if err != nil {
			t.Fatal(err)
		}
		if !fi.ModTime().Equal(old) {
			t.Fatalf("job %d record was rewritten despite needing no sanitization: mtime=%s",
				id, fi.ModTime())
		}
	}
	gf, _ := s.Get(1)
	if gf.Status != StatusFailed || gf.FailureReason != "既有的失败原因" || gf.Archive != nil {
		t.Fatalf("clean failed record must be used as-is: %s %q archive=%v",
			gf.Status, gf.FailureReason, gf.Archive)
	}
	gc, _ := s.Get(2)
	if gc.Status != StatusCanceled || gc.Archive != nil {
		t.Fatalf("clean canceled record must be used as-is: %s archive=%v", gc.Status, gc.Archive)
	}
	gs, _ := s.Get(3)
	if gs.Status != StatusSucceeded || gs.Archive == nil || gs.Archive.Sum != 3 {
		t.Fatalf("valid succeeded record must restore with its archive: %s %+v",
			gs.Status, gs.Archive)
	}
}

// 恢复出的终态记录保留读入时的文件名：清理写回同一份非默认命名的正式文件，
// 不按作业号另写默认命名记录；默认命名位置保持不存在。
func TestReopenTerminalResidualInSuffixedFileWritesBackInPlace(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	failedAt := base.Add(2 * time.Second)
	j := &storedJob{
		id: 1, submitter: "a", requestID: "suffixed",
		seed: 0, values: []int64{1, 2},
		queuedAt: base.Add(time.Second), startedAt: failedAt, finishedAt: failedAt,
		status:        StatusFailed,
		failureReason: "既有的失败原因",
	}
	const altName = "job-residual-0001.json"
	writeResidualTerminalRecord(t, dir, altName, j, []int64{1, 2},
		residualEffective|residualArchive)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	g := assertNoSuccessData(t, s, 1)
	if g.Status != StatusFailed || g.FailureReason != "既有的失败原因" {
		t.Fatalf("terminal state must be preserved: %s %q", g.Status, g.FailureReason)
	}
	assertOnDiskNoSuccessData(t, dir, altName, StatusFailed)
	if _, err := os.Stat(filepath.Join(dir, jobFileName(1))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sanitization must write back to %s, default-named file err=%v", altName, err)
	}
}
