package numeric

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 本文件覆盖“重新打开归档时清理已失败或已取消终态记录中残留的成功产物”：
// 只有成功作业才提供完整归档，保存状态已经是失败或取消、且记录能按现有规则
// 正常解析的作业，其记录里残留的实际参与计算输入与成功归档（即使数值、摘要、
// 日志与校验值全部自洽）都不能在重开后再被按作业号读取或按提交人列举带出。
// 清理只移除成功产物：终态、失败原因、阻断根因、作业号、提交人、请求号、
// 原始整数及次序、种子、有序依赖与已有时间一律保留。

// readOnDiskRecord 直接解析目录中某份记录文件，用于断言恢复改写真正落盘。
func readOnDiskRecord(t *testing.T, path string) *jobRecord {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var r jobRecord
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	return &r
}

// 规格主例：失败记录残留全套自洽归档与实际输入，取消记录分别残留“归档+实际
// 输入”“只有实际输入”“只有归档”。重开后四类记录都只呈现原终态：成功归档与
// 实际输入一律为空，残留的总和、平方和、日志、摘要与校验值都取不到；终态、
// 原因、根因与提交参数原样保留，清理结果写回各自的原记录文件。
func TestReopenTerminalRecordsDropResidualSuccessArtifacts(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	// 一份数值层面完全自洽的成功归档（[5,2] → 总和 7、平方和 29），把它
	// 残留到失败/取消记录上：即使全套自洽也不能成为恢复为成功的依据。
	selfConsistent := buildSucceededJob(t, 1, []int64{5}, []int64{5, 2}, []uint64{1}, base)
	if selfConsistent.archive.Sum != 7 || selfConsistent.archive.SumOfSquares != 29 {
		t.Fatalf("premise: residual archive sum=%d sq=%d want 7,29",
			selfConsistent.archive.Sum, selfConsistent.archive.SumOfSquares)
	}
	// 作业 1：合法成功记录（[2] → 2），供作业 2 声明有序依赖。
	syntheticSucceededRecord(t, dir, 1, []int64{2}, []int64{2}, nil, base)

	// 作业 2：失败（因上游问题失败的既有原因，根因作业 9），残留归档+实际输入。
	failedReason := "直接上游作业 9 已失败，阻断本作业继续计算（其失败原因：旧根因）"
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "f2", seed: 4,
		values:       []int64{5, -3},
		dependencies: []uint64{1},
		queuedAt:     base.Add(2 * time.Second),
		startedAt:    base.Add(2*time.Second + time.Millisecond),
		finishedAt:   base.Add(3 * time.Second),
		status:       StatusFailed, failureReason: failedReason, blockerID: 9,
		effectiveValues: []int64{5, -3, 2},
		archive:         selfConsistent.archive,
	})
	// 作业 3：取消，只残留实际输入。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "c3", seed: 6,
		values:          []int64{9},
		queuedAt:        base.Add(4 * time.Second),
		finishedAt:      base.Add(5 * time.Second),
		status:          StatusCanceled,
		effectiveValues: []int64{9},
	})
	// 作业 4：失败（无上游、BlockerID 0 的自身失败），只残留归档。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 4, submitter: "bob", requestID: "f4", seed: -1,
		values:     []int64{1, 2, 3},
		queuedAt:   base.Add(6 * time.Second),
		finishedAt: base.Add(7 * time.Second),
		status:     StatusFailed, failureReason: "平方和超出 int64 范围", blockerID: 0,
		archive: selfConsistent.archive,
	})
	// 作业 5：取消，既无归档也无实际输入（无残留——不应被改写，见另案）。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 5, submitter: "a", requestID: "c5",
		values:     []int64{8},
		queuedAt:   base.Add(8 * time.Second),
		finishedAt: base.Add(9 * time.Second),
		status:     StatusCanceled,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	g2 := mustGet(t, s, 2)
	if g2.Status != StatusFailed || g2.FailureReason != failedReason || g2.BlockerID != 9 {
		t.Fatalf("作业 2 终态/原因/根因被改动: status=%s reason=%q blocker=%d",
			g2.Status, g2.FailureReason, g2.BlockerID)
	}
	if g2.Archive != nil || g2.EffectiveValues != nil {
		t.Fatalf("作业 2 不得返回残留归档与实际输入: archive=%+v eff=%v",
			g2.Archive, g2.EffectiveValues)
	}
	assertInt64s(t, "job2 原始整数保留", g2.Values, []int64{5, -3})
	assertUint64s(t, "job2 有序依赖保留", g2.Dependencies, []uint64{1})
	if g2.Seed != 4 || g2.RequestID != "f2" || g2.Submitter != "a" {
		t.Fatalf("作业 2 提交参数被改动: %+v", g2)
	}
	if !g2.FinishedAt.Equal(base.Add(3*time.Second)) ||
		!g2.StartedAt.Equal(base.Add(2*time.Second+time.Millisecond)) ||
		!g2.QueuedAt.Equal(base.Add(2*time.Second)) {
		t.Fatalf("作业 2 已有时间被改动: queued=%s started=%s finished=%s",
			g2.QueuedAt, g2.StartedAt, g2.FinishedAt)
	}

	g3 := mustGet(t, s, 3)
	if g3.Status != StatusCanceled || g3.BlockerID != 0 || g3.FailureReason != "" {
		t.Fatalf("作业 3 取消状态被改判: %+v", g3)
	}
	if g3.Archive != nil || g3.EffectiveValues != nil {
		t.Fatalf("作业 3 不得返回残留实际输入: archive=%+v eff=%v",
			g3.Archive, g3.EffectiveValues)
	}

	g4 := mustGet(t, s, 4)
	if g4.Status != StatusFailed || g4.BlockerID != 0 ||
		g4.FailureReason != "平方和超出 int64 范围" {
		t.Fatalf("作业 4 失败判定被改写: %+v", g4)
	}
	if g4.Archive != nil || g4.EffectiveValues != nil {
		t.Fatalf("作业 4 不得返回残留归档: archive=%+v eff=%v",
			g4.Archive, g4.EffectiveValues)
	}
	assertInt64s(t, "job4 原始整数保留", g4.Values, []int64{1, 2, 3})

	g5 := mustGet(t, s, 5)
	if g5.Status != StatusCanceled || g5.Archive != nil || g5.EffectiveValues != nil {
		t.Fatalf("作业 5 应保持无残留的取消状态: %+v", g5)
	}

	// 按提交人与时间范围列举与按作业号读取一致：不带任何成功产物。
	listed, err := s.List("a", base.Add(1500*time.Millisecond), base.Add(9500*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[uint64]*Job{}
	for _, j := range listed {
		seen[j.ID] = j
		if j.Archive != nil || j.EffectiveValues != nil {
			t.Fatalf("列举中的作业 %d 带出了成功产物: archive=%+v eff=%v",
				j.ID, j.Archive, j.EffectiveValues)
		}
	}
	for _, id := range []uint64{2, 3, 5} {
		if seen[id] == nil || seen[id].Status != mustGet(t, s, id).Status {
			t.Fatalf("列举缺少或状态不一致的作业 %d: %+v", id, seen[id])
		}
	}
	if seen[4] != nil {
		t.Fatalf("作业 4 属于 bob，不应出现在 a 的列举中")
	}

	// 清理真正落盘：磁盘记录不再含归档与实际输入，终态与原因保留。
	for _, id := range []uint64{2, 3, 4} {
		r := readOnDiskRecord(t, filepath.Join(dir, jobFileName(id)))
		if r.Archive != nil || len(r.EffectiveValues) != 0 {
			t.Fatalf("作业 %d 磁盘记录仍有成功产物: archive=%+v eff=%v",
				id, r.Archive, r.EffectiveValues)
		}
	}
	r2 := readOnDiskRecord(t, filepath.Join(dir, jobFileName(2)))
	if r2.Status != StatusFailed || r2.FailureReason != failedReason || r2.BlockerID != 9 {
		t.Fatalf("作业 2 落盘终态被改写: status=%s reason=%q blocker=%d",
			r2.Status, r2.FailureReason, r2.BlockerID)
	}
	assertInt64s(t, "job2 落盘原始整数保留", r2.Values, []int64{5, -3})

	// 合法成功记录不受清理影响。
	g1 := mustGet(t, s, 1)
	if g1.Status != StatusSucceeded || g1.Archive == nil || g1.Archive.Sum != 2 {
		t.Fatalf("合法成功记录不应受影响: %+v", g1)
	}
}

// 再次打开仍然干净：上一次打开清理并落盘后，第二次打开读到的终态记录依旧
// 不含归档与实际输入，也不会触发任何新的改判。
func TestReopenCleanedTerminalRecordsStayCleanOnSecondOpen(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	a := buildSucceededJob(t, 1, []int64{7}, []int64{7}, nil, base)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "f2",
		values:   []int64{5},
		queuedAt: base, finishedAt: base.Add(time.Second),
		status: StatusFailed, failureReason: "旧失败原因",
		effectiveValues: []int64{5},
		archive:         a.archive,
	})
	s1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if g := mustGet(t, s1, 2); g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("首次打开即应清理: %+v", g)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	g := mustGet(t, s2, 2)
	if g.Status != StatusFailed || g.FailureReason != "旧失败原因" ||
		g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("第二次打开应仍是无成功产物的原失败状态: %+v", g)
	}
}

// 同一提交人用原非空请求号再次提交相同内容：返回原终态作业的当前详情，同样
// 不含成功归档与实际输入，不重算、不创建另一份作业；内容不同仍是原有的幂等
// 冲突。请求号仍属于原作业，新请求才取后续作业号。
func TestReplayTerminalRecordKeepsTerminalAndNoArtifacts(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	a := buildSucceededJob(t, 1, []int64{7}, []int64{7}, nil, base)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "f2", seed: 4,
		values:   []int64{5, -3},
		queuedAt: base, finishedAt: base.Add(time.Second),
		status: StatusFailed, failureReason: "旧失败原因", blockerID: 0,
		effectiveValues: []int64{5, -3},
		archive:         a.archive,
	})
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 相同内容 → 原终态详情，无错误。
	replay, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "f2", Values: []int64{5, -3}, Seed: 4,
	})
	if err != nil || replay.ID != 2 {
		t.Fatalf("幂等重放应返回原作业: id=%d err=%v", replayID(replay), err)
	}
	if replay.Status != StatusFailed || replay.FailureReason != "旧失败原因" ||
		replay.Archive != nil || replay.EffectiveValues != nil {
		t.Fatalf("重放返回必须是无成功产物的原终态详情: %+v", replay)
	}
	// 不另建作业、不重算：目录中仍只有作业 2 的记录，新请求取下一个作业号 3。
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || e.Name() == jobFileName(2) || strings.HasPrefix(e.Name(), ".tmp-") {
			continue
		}
		t.Fatalf("幂等重放不应产生新记录文件: %s", e.Name())
	}
	if g := mustGet(t, s, 2); g.Status != StatusFailed {
		t.Fatalf("重放后原作业不得被重算: %s", g.Status)
	}
	// 内容不同 → 原有的幂等冲突，返回的原作业视图同样干净。
	conflict, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "f2", Values: []int64{5}, Seed: 4,
	})
	if !errors.Is(err, ErrIdempotencyConflict) || conflict == nil || conflict.ID != 2 {
		t.Fatalf("内容不一致须返回幂等冲突: view=%+v err=%v", conflict, err)
	}
	if conflict.Archive != nil || conflict.EffectiveValues != nil {
		t.Fatalf("冲突返回的原作业视图不得带成功产物: %+v", conflict)
	}
	// 全新请求取未消耗的下一个作业号并正常计算。
	fresh := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	if fresh.ID != 3 {
		t.Fatalf("新作业应取得 3 号（重放不消耗作业号），got %d", fresh.ID)
	}
	waitStatus(t, s, 3, StatusSucceeded)
}

// 等待这些终态作业的下游继续遵守既有阻断规则，不能利用残留结果开始计算；
// 根因沿合法依赖链保留为终态作业自身。
func TestDownstreamOfCleanedTerminalJobsStillBlocked(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	a := buildSucceededJob(t, 1, []int64{7}, []int64{7}, nil, base)
	// 作业 1：失败但残留自洽归档；作业 2：取消但残留实际输入。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "f1",
		values:   []int64{7},
		queuedAt: base, finishedAt: base.Add(time.Second),
		status: StatusFailed, failureReason: "作业 1 的旧失败原因",
		effectiveValues: []int64{7},
		archive:         a.archive,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "c2",
		values:   []int64{4},
		queuedAt: base.Add(time.Second), finishedAt: base.Add(2 * time.Second),
		status:          StatusCanceled,
		effectiveValues: []int64{4},
	})
	// 作业 3 排队等待 1，作业 4 排队等待 3（多跳），作业 5 排队等待 2。
	for _, q := range []struct {
		id  uint64
		dep uint64
	}{{3, 1}, {4, 3}, {5, 2}} {
		writeSyntheticRecord(t, dir, &storedJob{
			id: q.id, submitter: "a", requestID: "q" + itoa(q.id),
			values:       []int64{1},
			dependencies: []uint64{q.dep},
			queuedAt:     base.Add(time.Duration(q.id) * time.Second),
			status:       StatusQueued,
		})
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	g3 := waitStatus(t, s, 3, StatusFailed)
	if g3.BlockerID != 1 || g3.Archive != nil || g3.EffectiveValues != nil ||
		!g3.StartedAt.IsZero() {
		t.Fatalf("作业 3 须按既有规则被失败上游阻断且从不计算: %+v", g3)
	}
	g4 := waitStatus(t, s, 4, StatusFailed)
	if g4.BlockerID != 1 {
		t.Fatalf("作业 4 的根因须沿链条保留为作业 1: blocker=%d", g4.BlockerID)
	}
	g5 := waitStatus(t, s, 5, StatusFailed)
	if g5.BlockerID != 2 {
		t.Fatalf("作业 5 须被已取消的作业 2 阻断: blocker=%d", g5.BlockerID)
	}
	// 上游终态记录本身的原因不被下游级联改写。
	if g1 := mustGet(t, s, 1); g1.FailureReason != "作业 1 的旧失败原因" || g1.Archive != nil {
		t.Fatalf("作业 1 原失败原因与清理结果须保留: %+v", g1)
	}
	if g2 := mustGet(t, s, 2); g2.Status != StatusCanceled || g2.Archive != nil {
		t.Fatalf("作业 2 取消状态须保留且无归档: %+v", g2)
	}
	// 阻断结果同样落盘。
	if b := onDiskBlocker(t, dir, 4); b != 1 {
		t.Fatalf("作业 4 落盘根因=%d want 1", b)
	}
}

// 原始整数序列属于提交参数：已失败记录即使原始序列为空，也只清理成功产物，
// 不补造参数、不清空（本来就空）其他字段，也不把终态改判成排队记录的
// “原始整数序列为空”失败。
func TestFailedRecordWithEmptyOriginalValuesKeepsTerminal(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	a := buildSucceededJob(t, 1, []int64{7}, []int64{7}, nil, base)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "f1",
		queuedAt: base, finishedAt: base.Add(time.Second),
		status: StatusFailed, failureReason: "计算被中断：归档上次关闭时作业仍在运行",
		effectiveValues: []int64{7, 8},
		archive:         a.archive,
	})
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	g := mustGet(t, s, 1)
	if g.Status != StatusFailed {
		t.Fatalf("终态必须保留: %s", g.Status)
	}
	if g.FailureReason != "计算被中断：归档上次关闭时作业仍在运行" {
		t.Fatalf("失败原因不能换成排队记录的空序列原因等新理由: %q", g.FailureReason)
	}
	if len(g.Values) != 0 {
		t.Fatalf("空原始序列属于提交参数，不应被补造: %v", g.Values)
	}
	if g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("残留成功产物必须清空: archive=%+v eff=%v", g.Archive, g.EffectiveValues)
	}
	r := readOnDiskRecord(t, filepath.Join(dir, jobFileName(1)))
	if r.Archive != nil || len(r.EffectiveValues) != 0 || len(r.Values) != 0 {
		t.Fatalf("落盘记录应无成功产物且不补造原始参数: %+v", r)
	}
}

// 没有任何残留的失败/取消记录与合法成功记录不触发恢复改写：磁盘字节保持
// 原样（不补写完成时间，不规范化任何字段）。
func TestCleanTerminalAndSucceededRecordsAreNotRewritten(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	syntheticSucceededRecord(t, dir, 1, []int64{2}, []int64{2}, nil, base)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "f2",
		values:   []int64{5},
		queuedAt: base.Add(2 * time.Second), finishedAt: base.Add(3 * time.Second),
		status: StatusFailed, failureReason: "旧失败原因",
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "c3",
		values:   []int64{9},
		queuedAt: base.Add(4 * time.Second), finishedAt: base.Add(5 * time.Second),
		status: StatusCanceled,
	})
	before := map[uint64][]byte{}
	for _, id := range []uint64{1, 2, 3} {
		data, err := os.ReadFile(filepath.Join(dir, jobFileName(id)))
		if err != nil {
			t.Fatal(err)
		}
		before[id] = data
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, id := range []uint64{1, 2, 3} {
		after, rerr := os.ReadFile(filepath.Join(dir, jobFileName(id)))
		if rerr != nil {
			t.Fatal(rerr)
		}
		if !bytes.Equal(before[id], after) {
			t.Fatalf("无残留的作业 %d 记录不应被恢复改写", id)
		}
	}
}

// 残留清理遵守“恢复出的记录写回读入时文件名”：非默认命名的终态记录在原
// 文件上被原子替换，不按作业号另写默认命名记录而同号旧记录原样保留。
func TestCleanedTerminalRecordWithSuffixedFileNameUpdatesSameFile(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	a := buildSucceededJob(t, 1, []int64{7}, []int64{7}, nil, base)
	j := &storedJob{
		id: 2, submitter: "a", requestID: "f2",
		values:   []int64{5},
		queuedAt: base, finishedAt: base.Add(time.Second),
		status: StatusFailed, failureReason: "旧失败原因",
		effectiveValues: []int64{5},
		archive:         a.archive,
		fileName:        suffixedRecordName(2),
	}
	data, err := encodeRecord(j)
	if err != nil {
		t.Fatal(err)
	}
	custom := suffixedRecordName(2)
	if err := writeFileAtomic(dir, custom, data, 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	g := mustGet(t, s, 2)
	if g.Status != StatusFailed || g.FailureReason != "旧失败原因" ||
		g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("非默认命名的失败记录应在保留终态的前提下清理: %+v", g)
	}
	r := readOnDiskRecord(t, filepath.Join(dir, custom))
	if r.Archive != nil || len(r.EffectiveValues) != 0 || r.Status != StatusFailed {
		t.Fatalf("清理必须写回原文件: %+v", r)
	}
	assertNoDefaultRecord(t, dir, 2)
}
