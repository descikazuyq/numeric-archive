package numeric

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// 本文件覆盖“重新打开归档时依赖两种写法的换算”：记录与归档都保存有序依赖
// 列表时，非空列表就是实际的有序上游列表，旧式单依赖字段（HasDependency /
// DependencyID）的缺省、零值或残留值不能覆盖它，也不能单独成为归档失效的
// 理由；但两处表达的实际列表内容或次序不同仍按本作业自身归档有误改判失败。

// buildSucceededJob 在内存中构造一条数值层面完全自洽的成功记录（不落盘）：
// 归档的摘要、日志与校验值由 newArchive 按给定实际输入全套生成。
func buildSucceededJob(t *testing.T, id uint64, values, eff []int64, deps []uint64, base time.Time) *storedJob {
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
	return j
}

// writeSucceededRecordPatched 把自洽成功记录 j 序列化落盘，落盘前由 patch
// 调整记录结构（例如把归档的旧式单依赖字段改成缺省/残留值，或改写归档的
// 依赖列表），用于构造“列表正确但旧字段异常”或“记录与归档列表不一致”的
// 归档形态。
func writeSucceededRecordPatched(t *testing.T, dir string, j *storedJob, patch func(r *jobRecord)) {
	t.Helper()
	data, err := encodeRecord(j)
	if err != nil {
		t.Fatal(err)
	}
	var r jobRecord
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	patch(&r)
	writeRawRecord(t, dir, j.id, &r)
}

// 规格示例：作业 1 的总和为 2，作业 2 的总和为 -3，作业 3 的原始输入为 [4]、
// 依赖列表为 [2,1]，实际输入为 [4,-3,2]，总和为 3、平方和为 29。记录与归档
// 保存相同列表且其余校验全部成立时，即使旧字段缺失（作业 3）或残留另一个
// 作业号（作业 4，旧字段指向作业 1），重开后仍能读取这份成功结果，不改变
// 追加次序；详情及内嵌归档中的旧单依赖信息继续表示是否有依赖以及列表首项，
// 等待它的作业仍能按现有规则使用结果。
func TestReopenSucceededArchiveListWinsOverLegacyFields(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 1：[2] → 总和 2；作业 2：[-3] → 总和 -3。
	syntheticSucceededRecord(t, dir, 1, []int64{2}, []int64{2}, nil, base)
	syntheticSucceededRecord(t, dir, 2, []int64{-3}, []int64{-3}, nil, base)

	// 作业 3：记录与归档都保存列表 [2,1]，但旧式单依赖字段缺失（未填写）。
	j3 := buildSucceededJob(t, 3, []int64{4}, []int64{4, -3, 2}, []uint64{2, 1}, base)
	if j3.archive.Sum != 3 || j3.archive.SumOfSquares != 29 {
		t.Fatalf("premise result=%d,%d want 3,29", j3.archive.Sum, j3.archive.SumOfSquares)
	}
	wantChecksum3 := j3.archive.Checksum
	wantCompleted3 := j3.archive.CompletedAt
	writeSucceededRecordPatched(t, dir, j3, func(r *jobRecord) {
		r.HasDependency, r.DependencyID = false, 0
		r.Archive.HasDependency, r.Archive.DependencyID = false, 0
	})

	// 作业 4：同样的列表 [2,1] 与实际输入，但旧字段残留另一个作业号（指向 1）。
	j4 := buildSucceededJob(t, 4, []int64{4}, []int64{4, -3, 2}, []uint64{2, 1}, base)
	writeSucceededRecordPatched(t, dir, j4, func(r *jobRecord) {
		r.HasDependency, r.DependencyID = true, 1
		r.Archive.HasDependency, r.Archive.DependencyID = true, 1
	})

	// 作业 5：排队等待作业 3，重开后应能使用其总和 3 继续计算。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 5, submitter: "a", requestID: "child",
		values: []int64{10}, dependencies: []uint64{3},
		queuedAt: base.Add(5 * time.Second),
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, id := range []uint64{3, 4} {
		g := mustGet(t, s, id)
		if g.Status != StatusSucceeded || g.Archive == nil {
			t.Fatalf("job %d must stay succeeded with intact archive, got %s", id, g.Status)
		}
		// 追加次序不变：实际输入仍是 [4,-3,2]，结果仍是总和 3、平方和 29。
		assertInt64s(t, "effective", g.EffectiveValues, []int64{4, -3, 2})
		assertInt64s(t, "archive effective", g.Archive.EffectiveValues, []int64{4, -3, 2})
		if g.Archive.Sum != 3 || g.Archive.SumOfSquares != 29 {
			t.Fatalf("job %d result=%d,%d want 3,29", id, g.Archive.Sum, g.Archive.SumOfSquares)
		}
		// 依赖信息一致：列表保持 [2,1]，旧单依赖字段表示有依赖、首项为 2。
		assertUint64s(t, "dependencies", g.Dependencies, []uint64{2, 1})
		assertUint64s(t, "archive dependencies", g.Archive.Dependencies, []uint64{2, 1})
		if !g.HasDependency || g.DependencyID != 2 {
			t.Fatalf("job %d legacy fields: has=%v id=%d, want true,2 (first list item)",
				id, g.HasDependency, g.DependencyID)
		}
		if !g.Archive.HasDependency || g.Archive.DependencyID != 2 {
			t.Fatalf("job %d archive legacy fields: has=%v id=%d, want true,2",
				id, g.Archive.HasDependency, g.Archive.DependencyID)
		}
		// 按提交人列举与按作业号读取返回一致的依赖信息。
		listed := mustListJob(t, s, "a", id)
		assertUint64s(t, "listed dependencies", listed.Dependencies, []uint64{2, 1})
		assertUint64s(t, "listed archive dependencies", listed.Archive.Dependencies, []uint64{2, 1})
		if !listed.Archive.HasDependency || listed.Archive.DependencyID != 2 {
			t.Fatalf("job %d listed archive legacy fields: has=%v id=%d, want true,2",
				id, listed.Archive.HasDependency, listed.Archive.DependencyID)
		}
	}
	// 校验值与完成时间保持不变。
	g3 := mustGet(t, s, 3)
	if g3.Archive.Checksum != wantChecksum3 {
		t.Fatalf("job 3 checksum changed across reopen: %s want %s",
			g3.Archive.Checksum, wantChecksum3)
	}
	if !g3.Archive.CompletedAt.Equal(wantCompleted3) || !g3.FinishedAt.Equal(wantCompleted3) {
		t.Fatalf("job 3 completion time changed: archive=%s finished=%s want %s",
			g3.Archive.CompletedAt, g3.FinishedAt, wantCompleted3)
	}

	// 等待作业 3 的下游仍能使用其结果：实际输入 [10,3]，总和 13。
	child := waitStatus(t, s, 5, StatusSucceeded)
	assertInt64s(t, "child effective", child.Archive.EffectiveValues, []int64{10, 3})
	if child.Archive.Sum != 13 {
		t.Fatalf("child sum=%d want 13", child.Archive.Sum)
	}
}

// 兼容两种表示不能放宽真实依赖的检查：记录列表为 [2,1] 时，归档列表次序不同
// （[1,2]，作业 3）或内容不同（归档只有指向作业 1 的旧式单依赖，作业 4）都
// 仍按本作业自身归档有误改判失败——BlockerID 为 0，不返回成功归档或实际
// 输入，也不影响有效上游的结果；等待它的下游按既有规则级联失败。
func TestReopenSucceededArchiveListMismatchStillFails(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	syntheticSucceededRecord(t, dir, 1, []int64{2}, []int64{2}, nil, base)
	syntheticSucceededRecord(t, dir, 2, []int64{-3}, []int64{-3}, nil, base)

	// 作业 3：记录列表 [2,1]，归档列表 [1,2]——同样的集合，次序不同。
	j3 := buildSucceededJob(t, 3, []int64{4}, []int64{4, -3, 2}, []uint64{2, 1}, base)
	writeSucceededRecordPatched(t, dir, j3, func(r *jobRecord) {
		r.Archive.Dependencies = []uint64{1, 2}
		r.Archive.HasDependency, r.Archive.DependencyID = true, 1
	})

	// 作业 4：记录列表 [2,1]，归档没有列表、旧式单依赖指向作业 1——内容不同。
	j4 := buildSucceededJob(t, 4, []int64{4}, []int64{4, -3, 2}, []uint64{2, 1}, base)
	writeSucceededRecordPatched(t, dir, j4, func(r *jobRecord) {
		r.Archive.Dependencies = nil
		r.Archive.HasDependency, r.Archive.DependencyID = true, 1
	})

	// 作业 5：排队等待作业 3，应随其改判级联失败，根因为作业 3 自身。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 5, submitter: "a", requestID: "child",
		values: []int64{10}, dependencies: []uint64{3},
		queuedAt: base.Add(5 * time.Second),
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, id := range []uint64{3, 4} {
		g := mustGet(t, s, id)
		if g.Status != StatusFailed {
			t.Fatalf("job %d with mismatched archive dependency list must fail, got %s", id, g.Status)
		}
		if g.BlockerID != 0 {
			t.Fatalf("job %d mismatch is its own archive error: blocker=%d want 0", id, g.BlockerID)
		}
		if g.Archive != nil || g.EffectiveValues != nil {
			t.Fatalf("job %d must not return archive/effective inputs: archive=%v effective=%v",
				id, g.Archive, g.EffectiveValues)
		}
		if !strings.Contains(g.FailureReason, "归档不完整或校验值不一致") {
			t.Fatalf("job %d reason=%q must state archive intactness failure", id, g.FailureReason)
		}
		// 原始参数与保存的依赖列表保持原样。
		assertInt64s(t, "values kept", g.Values, []int64{4})
		assertUint64s(t, "dependencies kept", g.Dependencies, []uint64{2, 1})
	}

	// 有效上游的结果不受影响。
	for id, wantSum := range map[uint64]int64{1: 2, 2: -3} {
		g := mustGet(t, s, id)
		if g.Status != StatusSucceeded || g.Archive == nil || g.Archive.Sum != wantSum {
			t.Fatalf("valid upstream %d must keep its result: status=%s", id, g.Status)
		}
	}

	// 等待作业 3 的下游按既有规则失败，根因为作业 3 自身。
	child := waitStatus(t, s, 5, StatusFailed)
	if child.BlockerID != 3 || !strings.Contains(child.FailureReason, "直接上游作业 3") {
		t.Fatalf("child blocker=%d reason=%q want root 3 named as direct upstream",
			child.BlockerID, child.FailureReason)
	}
	if child.Archive != nil || !child.StartedAt.IsZero() {
		t.Fatalf("blocked child must never compute: started=%s archive=%v",
			child.StartedAt, child.Archive)
	}
}

// 旧式单依赖与只含同一作业号的列表表达同一内容：记录保存列表 [1]、归档只有
// 旧式单依赖字段（无 dependencies 字段）时，重开后仍保持成功；恢复后内嵌
// 归档的依赖列表与旧字段都按规范化形式返回。
func TestReopenLegacyOnlyArchiveMatchesSingleItemList(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	syntheticSucceededRecord(t, dir, 1, []int64{2}, []int64{2}, nil, base) // 总和 2

	// 作业 2：记录保存列表 [1]，实际输入 [4,2]；归档只写旧式单依赖字段。
	j2 := buildSucceededJob(t, 2, []int64{4}, []int64{4, 2}, []uint64{1}, base)
	writeSucceededRecordPatched(t, dir, j2, func(r *jobRecord) {
		r.Archive.Dependencies = nil
		r.Archive.HasDependency, r.Archive.DependencyID = true, 1
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	g := mustGet(t, s, 2)
	if g.Status != StatusSucceeded || g.Archive == nil {
		t.Fatalf("legacy-only archive equal to single-item list must stay succeeded: %s", g.Status)
	}
	if g.Archive.Sum != 6 || g.Archive.SumOfSquares != 20 {
		t.Fatalf("result=%d,%d want 6,20", g.Archive.Sum, g.Archive.SumOfSquares)
	}
	assertUint64s(t, "dependencies", g.Dependencies, []uint64{1})
	assertUint64s(t, "archive dependencies", g.Archive.Dependencies, []uint64{1})
	if !g.Archive.HasDependency || g.Archive.DependencyID != 1 {
		t.Fatalf("archive legacy fields: has=%v id=%d, want true,1",
			g.Archive.HasDependency, g.Archive.DependencyID)
	}
}
