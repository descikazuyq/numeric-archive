package numeric

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 本文件覆盖“重新打开归档时清理排队记录中残留的成功产物”：排队状态必须继续
// 表示尚未取得本次计算的成功结果。保存状态为排队、原始参数与依赖合法的记录若
// 还带有实际参与计算的整数或旧成功归档，重开时先清除这些残留（只残留其中一种
// 或两者都有按同一规则处理），再按既有规则继续排队、等待与调度。残留归档中的
// 数值、日志、摘要与校验值即使完全自洽，也不能让作业直接成为成功，更不能作为
// 这次计算的输入来源。清理保留作业号、提交人、请求号、原始整数及次序、种子、
// 有序依赖与已有时间，不补完成时间，也不把原本合法的排队作业改判失败。

// writeQueuedRecordFile 把一条内存中的作业记录以指定文件名直接落盘（测试夹具）。
func writeQueuedRecordFile(t *testing.T, dir, name string, j *storedJob) {
	t.Helper()
	data, err := encodeRecord(j)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// 规格主例：原始序列为 [2,-3]、无依赖的排队作业即使残留一份 [9] 的旧成功结果
// （数值层面完全自洽：总和 9、平方和 81），重开后排队详情不返回该旧产物；真正
// 完成后得到的是此次计算生成的归档：实际输入 [2,-3]、总和 -1、平方和 13。
func TestReopenQueuedResidualArchiveIsNotReturnedAndNotReused(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	stale := buildSucceededJob(t, 9, []int64{9}, []int64{9}, nil, base)
	if stale.archive.Sum != 9 || stale.archive.SumOfSquares != 81 {
		t.Fatalf("premise: stale archive sum=%d sq=%d want 9,81",
			stale.archive.Sum, stale.archive.SumOfSquares)
	}
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "q1", seed: 4,
		values:          []int64{2, -3},
		queuedAt:        base,
		status:          StatusQueued,
		effectiveValues: []int64{9},
		archive:         stale.archive,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	done := waitStatus(t, s, 1, StatusSucceeded)
	if done.Archive == nil {
		t.Fatal("完成后必须有此次计算生成的归档")
	}
	if done.Archive.Sum != -1 || done.Archive.SumOfSquares != 13 {
		t.Fatalf("旧成功结果被当作本次结果: sum=%d sq=%d want -1,13",
			done.Archive.Sum, done.Archive.SumOfSquares)
	}
	assertInt64s(t, "实际输入必须只由原始整数构成", done.Archive.EffectiveValues, []int64{2, -3})
	assertInt64s(t, "详情中的实际输入同样是本次输入", done.EffectiveValues, []int64{2, -3})
	if done.Archive.JobID != 1 {
		t.Fatalf("归档必须属于本作业: job_id=%d", done.Archive.JobID)
	}

	// 再次打开：成功结果是本次归档，旧 [9] 结果永不复现。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	g := mustGet(t, s2, 1)
	if g.Status != StatusSucceeded || g.Archive.Sum != -1 || g.Archive.SumOfSquares != 13 {
		t.Fatalf("重开后应保留本次成功结果: %+v", g)
	}
}

// 排队等待期间，按作业号查看、按提交人列举、同一提交人以原请求号重复提交，返回
// 的详情都不含残留实际输入与成功归档；等待原因与待完成上游准确反映当前状态；
// 清理写回读入的正式记录。三种残留形态（两者都有、只有实际输入、只有归档）按
// 同一规则处理。为在作业真正运行前确定性地观察排队形态，目标作业等待一大批
// 低作业号的排队上游——沿用既有恢复测试的同一手法：每个上游都要“运行 + 完成”
// 两次原子落盘（含临时文件 fsync 与目录 fsync），上游数量保证下列断言全部发生
// 在目标变为可运行之前；关键的排队态断言放在最前。
func TestReopenQueuedRecordsDropResidualArtifactsWhileWaiting(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	const upstreams = 128
	depIDs := make([]uint64, 0, upstreams)
	for i := 1; i <= upstreams; i++ {
		id := uint64(i)
		u := &storedJob{
			id: id, submitter: "u", requestID: "u" + itoa(id),
			values:   []int64{1},
			queuedAt: base.Add(time.Duration(i) * time.Millisecond),
			status:   StatusQueued,
		}
		writeQueuedRecordFile(t, dir, jobFileName(id), u)
		depIDs = append(depIDs, id)
	}
	stale := buildSucceededJob(t, 9, []int64{9}, []int64{9}, nil, base)
	queuedAt := base.Add(time.Second)
	// 作业 upstreams+1：实际输入与旧归档都残留。
	writeSyntheticRecord(t, dir, &storedJob{
		id: upstreams + 1, submitter: "a", requestID: "q-both", seed: 4,
		values:          []int64{2, -3},
		dependencies:    depIDs,
		queuedAt:        queuedAt,
		status:          StatusQueued,
		effectiveValues: []int64{9},
		archive:         stale.archive,
	})
	// 作业 upstreams+2：只残留实际输入。
	writeSyntheticRecord(t, dir, &storedJob{
		id: upstreams + 2, submitter: "a", requestID: "q-eff", seed: 5,
		values:          []int64{5},
		dependencies:    depIDs,
		queuedAt:        queuedAt.Add(time.Millisecond),
		status:          StatusQueued,
		effectiveValues: []int64{9},
	})
	// 作业 upstreams+3：只残留旧归档。
	writeSyntheticRecord(t, dir, &storedJob{
		id: upstreams + 3, submitter: "a", requestID: "q-arc", seed: 6,
		values:       []int64{8},
		dependencies: depIDs,
		queuedAt:     queuedAt.Add(2 * time.Millisecond),
		status:       StatusQueued,
		archive:      stale.archive,
	})
	// 作业 upstreams+4：无依赖、两者都残留——排队原因为等待计算位置。
	writeSyntheticRecord(t, dir, &storedJob{
		id: upstreams + 4, submitter: "a", requestID: "q-slot", seed: 7,
		values:          []int64{7},
		queuedAt:        queuedAt.Add(3 * time.Millisecond),
		status:          StatusQueued,
		effectiveValues: []int64{9},
		archive:         stale.archive,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	bothID := uint64(upstreams + 1)
	effID := uint64(upstreams + 2)
	arcID := uint64(upstreams + 3)
	slotID := uint64(upstreams + 4)

	assertWaitingClean := func(id uint64, wantValues []int64, wantSeed int64, wantReq string, withDeps bool) {
		t.Helper()
		g := mustGet(t, s, id)
		if g.Status != StatusQueued || g.Archive != nil || g.EffectiveValues != nil {
			t.Fatalf("作业 %d 排队期间不得带出旧产物: %+v", id, g)
		}
		if !g.FinishedAt.IsZero() || !g.StartedAt.IsZero() {
			t.Fatalf("作业 %d 清理不得补完成时间或开始时间: started=%s finished=%s",
				id, g.StartedAt, g.FinishedAt)
		}
		assertInt64s(t, "原始整数保留", g.Values, wantValues)
		if g.Seed != wantSeed || g.RequestID != wantReq || g.Submitter != "a" || g.ID != id {
			t.Fatalf("作业 %d 提交参数被改动: %+v", id, g)
		}
		if withDeps {
			if g.WaitReason != WaitDependency || len(g.PendingDependencies) != upstreams {
				t.Fatalf("作业 %d 等待状态不正确: reason=%q pending=%d",
					id, g.WaitReason, len(g.PendingDependencies))
			}
			assertUint64s(t, "待完成上游保持依赖次序", g.PendingDependencies, depIDs)
			assertUint64s(t, "有序依赖保留", g.Dependencies, depIDs)
		} else {
			if g.WaitReason != WaitSlot || len(g.PendingDependencies) != 0 {
				t.Fatalf("作业 %d 应等待计算位置: reason=%q pending=%v",
					id, g.WaitReason, g.PendingDependencies)
			}
		}
	}
	assertWaitingClean(bothID, []int64{2, -3}, 4, "q-both", true)
	assertWaitingClean(effID, []int64{5}, 5, "q-eff", true)
	assertWaitingClean(arcID, []int64{8}, 6, "q-arc", true)
	assertWaitingClean(slotID, []int64{7}, 7, "q-slot", false)

	// 按提交人与时间范围列举同样干净。
	listed, err := s.List("a", queuedAt, queuedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 4 {
		t.Fatalf("列举数量=%d want 4", len(listed))
	}
	for _, j := range listed {
		if j.Archive != nil || j.EffectiveValues != nil || j.Status != StatusQueued {
			t.Fatalf("列举中的作业 %d 带出了旧产物: %+v", j.ID, j)
		}
	}

	// 同一提交人以原非空请求号重复提交：相同内容返回原排队详情（干净），
	// 内容不同仍是原有的幂等冲突，返回的原作业视图同样干净。
	replay, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "q-both", Values: []int64{2, -3}, Seed: 4,
		Dependencies: depIDs,
	})
	if err != nil || replay.ID != bothID {
		t.Fatalf("幂等重放应返回原作业: id=%d err=%v", replayID(replay), err)
	}
	if replay.Status != StatusQueued || replay.Archive != nil || replay.EffectiveValues != nil {
		t.Fatalf("重放返回必须是干净的排队详情: %+v", replay)
	}
	conflict, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "q-both", Values: []int64{2, -3}, Seed: 99,
		Dependencies: depIDs,
	})
	if !errors.Is(err, ErrIdempotencyConflict) || conflict == nil || conflict.ID != bothID {
		t.Fatalf("内容不一致须返回幂等冲突: view=%+v err=%v", conflict, err)
	}
	if conflict.Archive != nil || conflict.EffectiveValues != nil {
		t.Fatalf("冲突返回的原作业视图不得带旧产物: %+v", conflict)
	}

	// 清理已在恢复时落盘到各自的正式记录：排队状态、原始参数与依赖保留，
	// 不含归档与实际输入，也没有补上完成时间。
	for _, id := range []uint64{bothID, effID, arcID, slotID} {
		r := readOnDiskRecord(t, filepath.Join(dir, jobFileName(id)))
		if r.Status != StatusQueued || r.Archive != nil || len(r.EffectiveValues) != 0 {
			t.Fatalf("作业 %d 磁盘排队记录仍有旧产物: status=%s archive=%+v eff=%v",
				id, r.Status, r.Archive, r.EffectiveValues)
		}
		if !r.FinishedAt.IsZero() {
			t.Fatalf("作业 %d 清理不得补写完成时间: %s", id, r.FinishedAt)
		}
	}
	rb := readOnDiskRecord(t, filepath.Join(dir, jobFileName(bothID)))
	assertInt64s(t, "落盘排队记录保留原始整数", rb.Values, []int64{2, -3})
	if len(rb.Dependencies) != upstreams {
		t.Fatalf("落盘排队记录依赖被改动: %d", len(rb.Dependencies))
	}

	// 上游排空后按既有调度完成：实际输入只由原始整数与按依赖次序追加的
	// 成功上游总和构成（每个上游 [1] → 总和 1），与任何残留无关。
	both := waitStatus(t, s, bothID, StatusSucceeded)
	wantEffBoth := append([]int64{2, -3}, repeatInt64(1, upstreams)...)
	assertInt64s(t, "两者残留作业的实际输入", both.Archive.EffectiveValues, wantEffBoth)
	if both.Archive.Sum != -1+int64(upstreams) || both.Archive.SumOfSquares != 13+int64(upstreams) {
		t.Fatalf("两者残留作业结果=%d,%d want %d,%d",
			both.Archive.Sum, both.Archive.SumOfSquares, -1+int64(upstreams), 13+int64(upstreams))
	}
	eff := waitStatus(t, s, effID, StatusSucceeded)
	if eff.Archive.Sum != 5+int64(upstreams) || eff.Archive.SumOfSquares != 25+int64(upstreams) {
		t.Fatalf("只残留实际输入的作业结果=%d,%d want %d,%d",
			eff.Archive.Sum, eff.Archive.SumOfSquares, 5+int64(upstreams), 25+int64(upstreams))
	}
	arc := waitStatus(t, s, arcID, StatusSucceeded)
	if arc.Archive.Sum != 8+int64(upstreams) || arc.Archive.SumOfSquares != 64+int64(upstreams) {
		t.Fatalf("只残留归档的作业结果=%d,%d want %d,%d",
			arc.Archive.Sum, arc.Archive.SumOfSquares, 8+int64(upstreams), 64+int64(upstreams))
	}
	slot := waitStatus(t, s, slotID, StatusSucceeded)
	if slot.Archive.Sum != 7 || slot.Archive.SumOfSquares != 49 {
		t.Fatalf("无依赖作业结果=%d,%d want 7,49", slot.Archive.Sum, slot.Archive.SumOfSquares)
	}
}

// repeatInt64 返回 n 个 v 组成的切片（测试断言用）。
func repeatInt64(v int64, n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = v
	}
	return out
}

// 有依赖的排队作业残留了另一份实际输入与归档时，恢复后追加值仍取自上游当前
// 成功归档的总和，而不是残留输入里的旧追加值。为在目标运行前确定性地观察排队
// 形态，目标的成功上游（作业 blockers+1，总和 2）排在一大批低作业号排队作业
// 之后才会被调度，目标因此稳定处于等待依赖状态。
func TestReopenQueuedResidualInputsDontAffectDependencyAppend(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	const blockers = 128
	for i := 1; i <= blockers; i++ {
		id := uint64(i)
		writeQueuedRecordFile(t, dir, jobFileName(id), &storedJob{
			id: id, submitter: "u", values: []int64{1},
			queuedAt: base.Add(time.Duration(i) * time.Millisecond),
			status:   StatusQueued,
		})
	}
	upID := uint64(blockers + 1)
	targetID := upID + 1
	// 直接上游：重开时仍排队（无依赖，原始 [2]），待其运行后成功、总和 2。
	// 它排在低作业号排队作业之后，因此打开初期目标稳定处于等待依赖状态。
	writeQueuedRecordFile(t, dir, jobFileName(upID), &storedJob{
		id: upID, submitter: "u", requestID: "up", values: []int64{2},
		queuedAt: base.Add(time.Second),
		status:   StatusQueued,
	})
	// 目标排队依赖该上游，原始 [5]，残留实际输入 [5,9] 与一份自洽旧归档
	// （仿佛上游曾经给出总和 9）。
	stale := buildSucceededJob(t, 9, []int64{5, 9}, []int64{5, 9}, []uint64{1}, base)
	writeSyntheticRecord(t, dir, &storedJob{
		id: targetID, submitter: "a", requestID: "q2", seed: 0,
		values:          []int64{5},
		dependencies:    []uint64{upID},
		queuedAt:        base.Add(2 * time.Second),
		status:          StatusQueued,
		effectiveValues: []int64{5, 9},
		archive:         stale.archive,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 排队期间不返回残留；上游尚未完成，等待原因为等待依赖结果。
	g := mustGet(t, s, targetID)
	if g.Status != StatusQueued || g.WaitReason != WaitDependency ||
		g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("恢复后排队详情必须干净: %+v", g)
	}
	assertUint64s(t, "待完成上游指出尚未成功的直接上游", g.PendingDependencies, []uint64{upID})
	done := waitStatus(t, s, targetID, StatusSucceeded)
	assertInt64s(t, "追加值必须取自当前上游总和 2", done.Archive.EffectiveValues, []int64{5, 2})
	if done.Archive.Sum != 7 || done.Archive.SumOfSquares != 29 {
		t.Fatalf("结果=%d,%d want 7,29", done.Archive.Sum, done.Archive.SumOfSquares)
	}
}

// 排队残留清理写回读入时的那份正式记录：非默认文件名不导致另建记录；之后的
// 运行/成功状态更新也继续写回原文件。
func TestCleanedQueuedRecordWithSuffixedFileNameUpdatesSameFile(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	const upstreams = 128
	depIDs := make([]uint64, 0, upstreams)
	for i := 1; i <= upstreams; i++ {
		id := uint64(i)
		writeQueuedRecordFile(t, dir, jobFileName(id), &storedJob{
			id: id, submitter: "u", values: []int64{1},
			queuedAt: base.Add(time.Duration(i) * time.Millisecond),
			status:   StatusQueued,
		})
		depIDs = append(depIDs, id)
	}
	stale := buildSucceededJob(t, 9, []int64{9}, []int64{9}, nil, base)
	target := &storedJob{
		id: upstreams + 1, submitter: "a", requestID: "q", seed: 2,
		values:          []int64{5},
		dependencies:    depIDs,
		queuedAt:        base.Add(time.Second),
		status:          StatusQueued,
		effectiveValues: []int64{9},
		archive:         stale.archive,
		fileName:        suffixedRecordName(upstreams + 1),
	}
	custom := suffixedRecordName(upstreams + 1)
	writeQueuedRecordFile(t, dir, custom, target)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	id := uint64(upstreams + 1)
	g := mustGet(t, s, id)
	if g.Status != StatusQueued || g.WaitReason != WaitDependency ||
		g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("非默认命名的排队记录清理后应继续排队且干净: %+v", g)
	}
	r := readOnDiskRecord(t, filepath.Join(dir, custom))
	if r.Status != StatusQueued || r.Archive != nil || len(r.EffectiveValues) != 0 {
		t.Fatalf("清理必须写回读入的原文件: %+v", r)
	}
	assertNoDefaultRecord(t, dir, id)

	done := waitStatus(t, s, id, StatusSucceeded)
	if done.Archive.Sum != 5+int64(upstreams) {
		t.Fatalf("结果=%d want %d", done.Archive.Sum, 5+int64(upstreams))
	}
	// 成功归档仍写回原文件，没有同号默认命名副本。
	r2 := readOnDiskRecord(t, filepath.Join(dir, custom))
	if r2.Status != StatusSucceeded || r2.Archive == nil || r2.Archive.Sum != 5+int64(upstreams) {
		t.Fatalf("成功状态必须写回原文件: %+v", r2)
	}
	assertNoDefaultRecord(t, dir, id)
}

// 清理在排队记录自身合法性判定之前执行，但既有的改判规则保持不变：依赖列表重复、
// 引用不小于自己的上游、原始整数序列为空的排队记录，即使残留了完全自洽的旧成功
// 归档，仍按既有规则改判失败（BlockerID 为 0、补完成时间、无归档与实际输入）。
func TestReopenInvalidQueuedRecordsWithResidueStillFail(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	stale := buildSucceededJob(t, 9, []int64{9}, []int64{9}, nil, base)
	// 作业 1：依赖列表重复引用作业 2，残留归档与实际输入。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "dup",
		values:          []int64{1},
		dependencies:    []uint64{2, 2},
		queuedAt:        base,
		status:          StatusQueued,
		effectiveValues: []int64{1, 9, 9},
		archive:         stale.archive,
	})
	// 作业 2：原始整数序列为空，残留归档与实际输入。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "empty",
		queuedAt:        base.Add(time.Second),
		status:          StatusQueued,
		effectiveValues: []int64{9},
		archive:         stale.archive,
	})
	// 作业 3：引用自己（先后关系不合法），只残留实际输入。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "self",
		values:          []int64{1},
		dependencies:    []uint64{3},
		queuedAt:        base.Add(2 * time.Second),
		status:          StatusQueued,
		effectiveValues: []int64{1, 1},
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	cases := []struct {
		id      uint64
		wantSub string
	}{
		{1, "依赖列表重复"},
		{2, "原始整数序列为空"},
		{3, "依赖先后关系不合法"},
	}
	for _, c := range cases {
		g := waitStatus(t, s, c.id, StatusFailed)
		if !strings.Contains(g.FailureReason, c.wantSub) {
			t.Fatalf("作业 %d 失败原因=%q，应包含 %q", c.id, g.FailureReason, c.wantSub)
		}
		if g.BlockerID != 0 || g.Archive != nil || g.EffectiveValues != nil {
			t.Fatalf("作业 %d 改判形态不正确: %+v", c.id, g)
		}
		if g.FinishedAt.IsZero() {
			t.Fatalf("作业 %d 排队改判失败应补完成时间", c.id)
		}
		r := readOnDiskRecord(t, filepath.Join(dir, jobFileName(c.id)))
		if r.Status != StatusFailed || r.Archive != nil || len(r.EffectiveValues) != 0 {
			t.Fatalf("作业 %d 落盘记录不正确: %+v", c.id, r)
		}
	}
	// 作业 1 的原始参数仍保留。
	r1 := readOnDiskRecord(t, filepath.Join(dir, jobFileName(1)))
	assertUint64s(t, "作业 1 依赖列表原样保留在失败记录中", r1.Dependencies, []uint64{2, 2})
}

// 清理结果暂时无法保存时，归档仍按既有恢复规则打开：打开本身成功，内存中的排队
// 作业不携带旧产物（直接调用恢复清理函数做确定性验证）；磁盘上的旧记录暂时保留，
// 写入恢复后重新打开即完成清理并正常计算出本次结果。
func TestReopenQueuedSanitizePersistFailureBestEffort(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	stale := buildSucceededJob(t, 9, []int64{9}, []int64{9}, nil, base)
	j := &storedJob{
		id: 1, submitter: "a", requestID: "q1", seed: 4,
		values:          []int64{2, -3},
		queuedAt:        base,
		status:          StatusQueued,
		effectiveValues: []int64{9},
		archive:         stale.archive,
	}
	writeSyntheticRecord(t, dir, j)
	path := filepath.Join(dir, jobFileName(1))
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// 目录暂时不可写：清理无法落盘时不报错，内存中的作业已经干净且仍合法排队，
	// 磁盘字节暂时保持原样（尽力而为，不影响本次打开期间的查询结果）。
	makeArchiveReadOnly(t, dir)
	sanitizeRestoredQueuedJob(dir, j)
	if j.status != StatusQueued || j.archive != nil || len(j.effectiveValues) != 0 {
		t.Fatalf("清理失败时内存仍须干净且保持排队: status=%s archive=%+v eff=%v",
			j.status, j.archive, j.effectiveValues)
	}
	if !j.finishedAt.IsZero() {
		t.Fatalf("清理不得补完成时间: %s", j.finishedAt)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("清理无法保存时不应改动磁盘记录")
	}

	// 写入恢复后，按真实恢复路径重新读入记录再清理：残留被原子写回为干净的
	// 排队记录（非默认文件名也会写回原文件，本用例使用默认命名）。
	makeArchiveWritable(t, dir)
	reparsed, err := parseJobRecord(dir, jobFileName(1))
	if err != nil {
		t.Fatal(err)
	}
	if reparsed.archive == nil || len(reparsed.effectiveValues) == 0 {
		t.Fatalf("前提：重新读入应重新得到残留: archive=%v eff=%v",
			reparsed.archive, reparsed.effectiveValues)
	}
	sanitizeRestoredQueuedJob(dir, reparsed)
	r := readOnDiskRecord(t, path)
	if r.Status != StatusQueued || r.Archive != nil || len(r.EffectiveValues) != 0 {
		t.Fatalf("写入恢复后清理应落盘: %+v", r)
	}
	if !r.FinishedAt.IsZero() {
		t.Fatalf("清理不得补写完成时间: %s", r.FinishedAt)
	}
}

// 端到端：只读目录下打开带残留的排队记录，打开成功；关闭并恢复写入后重新打开，
// 残留被清理，作业按原始参数正常完成，旧 [9] 结果永不参与计算。
func TestReopenQueuedResidueUnderReadOnlyDirRecoversAfterWritableReopen(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	stale := buildSucceededJob(t, 9, []int64{9}, []int64{9}, nil, base)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "q1", seed: 4,
		values:          []int64{2, -3},
		queuedAt:        base,
		status:          StatusQueued,
		effectiveValues: []int64{9},
		archive:         stale.archive,
	})
	path := filepath.Join(dir, jobFileName(1))

	makeArchiveReadOnly(t, dir)
	s, err := Open(dir)
	if err != nil || s == nil {
		t.Fatalf("清理暂时无法保存时归档仍应正常打开: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// 只读期间没有任何改写落盘：旧残留仍在磁盘上。
	disk := readOnDiskRecord(t, path)
	if disk.Status != StatusQueued || disk.Archive == nil || len(disk.EffectiveValues) == 0 {
		t.Fatalf("只读打开期间磁盘记录应保持原样: %+v", disk)
	}

	makeArchiveWritable(t, dir)
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	done := waitStatus(t, s2, 1, StatusSucceeded)
	if done.Archive.Sum != -1 || done.Archive.SumOfSquares != 13 {
		t.Fatalf("恢复写入后应按原始参数重新计算: sum=%d sq=%d want -1,13",
			done.Archive.Sum, done.Archive.SumOfSquares)
	}
}

// 没有任何残留的排队记录不触发恢复清理（不产生一次无意义的重写）。
func TestCleanQueuedRecordIsNotRewrittenBySanitizer(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	j := &storedJob{
		id: 1, submitter: "a", requestID: "q1",
		values:   []int64{2, -3},
		queuedAt: base,
		status:   StatusQueued,
	}
	writeSyntheticRecord(t, dir, j)
	path := filepath.Join(dir, jobFileName(1))
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sanitizeRestoredQueuedJob(dir, j)
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("无残留的排队记录不应被清理改写")
	}
}
