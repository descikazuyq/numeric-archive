package numeric

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// 本文件覆盖“重新打开归档时清理排队记录中残留的成功产物”：
// 保存状态为排队、依赖列表与原始整数合法的记录，若还带有实际参与计算的
// 整数或成功归档，重开后必须清除——排队状态继续表示尚未取得本次成功结果，
// 不能让调用方读到一份看似可用的旧归档，旧产物也不能作为这次计算的输入。
// 清理只移除成功产物：不改状态、不补完成时间、不把合法排队作业改判失败。

// computeGate 是确定性的计算钩子：被 gate 的作业停在计算中直到 release，
// 其余作业直接由真实 computeResult 完成；seen 记录每个作业实际收到的输入，
// 用于断言实际输入只由原始整数与按依赖次序追加的上游总和构成。
type computeGate struct {
	mu    sync.Mutex
	gates map[uint64]chan struct{}
	seen  map[uint64][]int64
}

func newComputeGate() *computeGate {
	return &computeGate{
		gates: make(map[uint64]chan struct{}),
		seen:  make(map[uint64][]int64),
	}
}

func (g *computeGate) block(ids ...uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, id := range ids {
		g.gates[id] = make(chan struct{})
	}
}

func (g *computeGate) release(id uint64) {
	g.mu.Lock()
	ch := g.gates[id]
	g.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (g *computeGate) inputsOf(id uint64) []int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.seen[id]
}

// withComputeHook 在 Open 的 worker 启动前安装计算钩子，避免与 worker 之间
// 存在安装时序竞争；仅供包内测试使用。
func withComputeHook(hook func(id uint64, inputs []int64, seed int64, canceled func() bool) (int64, int64, string, bool)) Option {
	return func(s *Store) { s.compute = hook }
}

func (g *computeGate) hook(id uint64, inputs []int64, seed int64, canceled func() bool) (int64, int64, string, bool) {
	g.mu.Lock()
	g.seen[id] = append([]int64(nil), inputs...)
	ch := g.gates[id]
	g.mu.Unlock()
	if ch != nil {
		// 同时响应 Close 的中止信号，避免断言失败提前返回时 defer Close 永久等待。
		abort := make(chan struct{})
		go func() {
			for {
				if canceled() {
					select {
					case <-abort:
					default:
						close(abort)
					}
					return
				}
				select {
				case <-abort:
					return
				case <-time.After(time.Millisecond):
				}
			}
		}()
		select {
		case <-ch:
		case <-abort:
		}
		close(abort)
	}
	return computeResult(inputs, seed, canceled)
}

// assertQueuedViewClean 断言排队详情不含任何旧成功产物，且等待原因与待完成
// 上游准确反映当前状态；提交参数（原始整数次序、种子、请求号）保持不变，
// 清理没有补完成时间、也没有开始运行。
func assertQueuedViewClean(t *testing.T, j *Job, wait WaitReason, pending []uint64) {
	t.Helper()
	if j.Status != StatusQueued {
		t.Fatalf("job %d 应保持排队，got %s（原因 %q）", j.ID, j.Status, j.FailureReason)
	}
	if j.WaitReason != wait {
		t.Fatalf("job %d 等待原因=%q want %q", j.ID, j.WaitReason, wait)
	}
	if len(j.PendingDependencies) != len(pending) {
		t.Fatalf("job %d 待完成上游=%v want %v", j.ID, j.PendingDependencies, pending)
	}
	for i := range pending {
		if j.PendingDependencies[i] != pending[i] {
			t.Fatalf("job %d 待完成上游=%v want %v", j.ID, j.PendingDependencies, pending)
		}
	}
	if j.Archive != nil || j.EffectiveValues != nil {
		t.Fatalf("job %d 排队详情不得带出残留归档/实际输入: archive=%+v eff=%v",
			j.ID, j.Archive, j.EffectiveValues)
	}
	if !j.StartedAt.IsZero() || !j.FinishedAt.IsZero() {
		t.Fatalf("job %d 清理不得补时间: started=%s finished=%s",
			j.ID, j.StartedAt, j.FinishedAt)
	}
	if j.BlockerID != 0 || j.FailureReason != "" {
		t.Fatalf("job %d 合法排队作业不得被改判失败: blocker=%d reason=%q",
			j.ID, j.BlockerID, j.FailureReason)
	}
}

// 规格主例：原始序列 [2,-3]、无依赖的排队作业即使残留一份 [9] 的旧成功结果，
// 在它仍排队时按作业号、按提交人列举与幂等重放都看不到残留；完成后得到总和
// -1、平方和 13、实际输入 [2,-3]。同趟还覆盖“只残留实际输入”“只残留归档”
// 与“无残留不重写”三种形态，以及合法成功记录不受影响。
func TestReopenQueuedRecordsDropResidualSuccessArtifacts(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	// [9] → 总和 9、平方和 81 的一套完全自洽旧成功归档，作为残留来源。
	residual := buildSucceededJob(t, 9, []int64{9}, []int64{9}, nil, base)
	if residual.archive.Sum != 9 || residual.archive.SumOfSquares != 81 {
		t.Fatalf("premise: residual sum=%d sq=%d", residual.archive.Sum, residual.archive.SumOfSquares)
	}

	// 作业 1：占用计算位置的排队作业，重开后先停在计算中。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "blocker",
		values:   []int64{100, -50},
		queuedAt: base,
		status:   StatusQueued,
	})
	// 作业 2：[2,-3] 无依赖，同时残留实际输入 [9] 与 [9] 的成功归档。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "r2", seed: 4,
		values:          []int64{2, -3},
		queuedAt:        base.Add(2 * time.Second),
		status:          StatusQueued,
		effectiveValues: []int64{9},
		archive:         residual.archive,
	})
	// 作业 3：只残留实际输入。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "r3",
		values:          []int64{5},
		queuedAt:        base.Add(3 * time.Second),
		status:          StatusQueued,
		effectiveValues: []int64{5, 7},
	})
	// 作业 4：只残留成功归档。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 4, submitter: "a", requestID: "r4",
		values:   []int64{6},
		queuedAt: base.Add(4 * time.Second),
		status:   StatusQueued,
		archive:  residual.archive,
	})
	// 作业 5：无任何残留，恢复不应重写其磁盘字节。
	clean5 := &storedJob{
		id: 5, submitter: "a", requestID: "r5",
		values:   []int64{7},
		queuedAt: base.Add(5 * time.Second),
		status:   StatusQueued,
	}
	writeSyntheticRecord(t, dir, clean5)
	clean5Bytes, err := os.ReadFile(filepath.Join(dir, jobFileName(5)))
	if err != nil {
		t.Fatal(err)
	}
	// 作业 6：合法成功记录（[8] → 8），不受排队清理影响。
	syntheticSucceededRecord(t, dir, 6, []int64{8}, []int64{8}, nil, base)

	gate := newComputeGate()
	gate.block(1)
	s, err := Open(dir, withComputeHook(gate.hook))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	started := make(chan struct{})
	go func() {
		// gate 已在 worker 启动前安装；轮询确认作业 1 进入计算，此时 2~5 必仍排队。
		for {
			if g, _ := s.Get(1); g.Status == StatusRunning {
				close(started)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("作业 1 未进入运行")
	}

	for _, id := range []uint64{2, 3, 4, 5} {
		g := mustGet(t, s, id)
		assertQueuedViewClean(t, g, WaitSlot, nil)
	}
	g2 := mustGet(t, s, 2)
	if g2.Seed != 4 || g2.RequestID != "r2" || g2.Submitter != "a" {
		t.Fatalf("作业 2 提交参数被改动: %+v", g2)
	}
	assertInt64s(t, "job2 原始整数及次序保留", g2.Values, []int64{2, -3})
	if !g2.QueuedAt.Equal(base.Add(2 * time.Second)) {
		t.Fatalf("作业 2 提交时间被改动: %s", g2.QueuedAt)
	}

	// 按提交人+时间范围列举与按作业号读取一致：排队作业均无成功产物，
	// 顺序仍为提交先后，合法成功作业 6 保留归档。
	listed, err := s.List("a", time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 6 {
		t.Fatalf("列举数量=%d want 6", len(listed))
	}
	for i, j := range listed {
		if j.ID != uint64(i+1) {
			t.Fatalf("列举须按作业号升序: pos %d id %d", i, j.ID)
		}
		if j.ID == 6 {
			if j.Status != StatusSucceeded || j.Archive == nil || j.Archive.Sum != 8 {
				t.Fatalf("合法成功记录不受排队清理影响: %+v", j)
			}
			continue
		}
		if j.Archive != nil || j.EffectiveValues != nil {
			t.Fatalf("列举中的排队作业 %d 带出旧成功产物: archive=%+v eff=%v",
				j.ID, j.Archive, j.EffectiveValues)
		}
	}

	// 同一提交人以原非空请求号和相同内容重复提交：返回原排队详情，无错误、
	// 无成功产物；内容不同仍是原有的幂等冲突，冲突返回的视图同样干净。
	replay, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "r2", Values: []int64{2, -3}, Seed: 4,
	})
	if err != nil || replay.ID != 2 {
		t.Fatalf("幂等重放应返回原排队作业 2: id=%d err=%v", replayID(replay), err)
	}
	assertQueuedViewClean(t, replay, WaitSlot, nil)
	conflict, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "r2", Values: []int64{2, -3}, Seed: 5,
	})
	if !errors.Is(err, ErrIdempotencyConflict) || conflict == nil || conflict.ID != 2 {
		t.Fatalf("内容不同须返回幂等冲突: view=%+v err=%v", conflict, err)
	}
	if conflict.Archive != nil || conflict.EffectiveValues != nil {
		t.Fatalf("冲突视图不得带旧成功产物: archive=%+v eff=%v",
			conflict.Archive, conflict.EffectiveValues)
	}

	// 清理在排队期间已写回读入的正式记录：磁盘上 2/3/4 不再含归档与实际输入，
	// 状态仍是排队、未补完成时间；作业 5 无残留，磁盘字节保持不变。
	for _, id := range []uint64{2, 3, 4} {
		r := readOnDiskRecord(t, filepath.Join(dir, jobFileName(id)))
		if r.Archive != nil || len(r.EffectiveValues) != 0 {
			t.Fatalf("作业 %d 磁盘记录仍残留成功产物: archive=%+v eff=%v",
				id, r.Archive, r.EffectiveValues)
		}
		if r.Status != StatusQueued || !r.FinishedAt.IsZero() {
			t.Fatalf("作业 %d 不得被改判或补完成时间: status=%s finished=%s",
				id, r.Status, r.FinishedAt)
		}
	}
	if after, rerr := os.ReadFile(filepath.Join(dir, jobFileName(5))); rerr != nil {
		t.Fatal(rerr)
	} else if string(after) != string(clean5Bytes) {
		t.Fatal("无残留的排队记录不应被恢复改写")
	}

	// 放行后按既有调度处理：实际输入只由原始整数构成（无依赖），旧 [9] 归档
	// 不参与计算。
	gate.release(1)
	done1 := waitStatus(t, s, 1, StatusSucceeded)
	if done1.Archive.Sum != 50 || done1.Archive.SumOfSquares != 12500 {
		t.Fatalf("作业 1 结果=%d,%d want 50,12500", done1.Archive.Sum, done1.Archive.SumOfSquares)
	}
	done2 := waitStatus(t, s, 2, StatusSucceeded)
	if done2.Archive.Sum != -1 || done2.Archive.SumOfSquares != 13 {
		t.Fatalf("规格示例结果=%d,%d want -1,13", done2.Archive.Sum, done2.Archive.SumOfSquares)
	}
	assertInt64s(t, "job2 实际输入必须是 [2,-3]", done2.EffectiveValues, []int64{2, -3})
	assertInt64s(t, "job2 归档实际输入必须是 [2,-3]", done2.Archive.EffectiveValues, []int64{2, -3})
	assertInt64s(t, "job2 计算实际收到的输入", gate.inputsOf(2), []int64{2, -3})
	if done2.Archive.CompletedAt.IsZero() {
		t.Fatal("作业 2 真正成功后才应有完成时间")
	}
	if done2.Archive.Checksum == residual.archive.Checksum {
		t.Fatal("作业 2 的新归档不能沿用旧 [9] 归档的校验值")
	}
	done3 := waitStatus(t, s, 3, StatusSucceeded)
	if done3.Archive.Sum != 5 || done3.Archive.SumOfSquares != 25 {
		t.Fatalf("作业 3 结果=%d,%d want 5,25", done3.Archive.Sum, done3.Archive.SumOfSquares)
	}
	assertInt64s(t, "job3 实际输入", done3.EffectiveValues, []int64{5})
	done4 := waitStatus(t, s, 4, StatusSucceeded)
	if done4.Archive.Sum != 6 || done4.Archive.SumOfSquares != 36 {
		t.Fatalf("作业 4 结果=%d,%d want 6,36", done4.Archive.Sum, done4.Archive.SumOfSquares)
	}
	done5 := waitStatus(t, s, 5, StatusSucceeded)
	if done5.Archive.Sum != 7 || done5.Archive.SumOfSquares != 49 {
		t.Fatalf("作业 5 结果=%d,%d want 7,49", done5.Archive.Sum, done5.Archive.SumOfSquares)
	}

	// 再次打开：完成的作业呈现本次计算生成的归档，排队清理不留下任何旧痕迹。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	r2 := waitStatus(t, s2, 2, StatusSucceeded)
	if r2.Archive.Sum != -1 || r2.Archive.SumOfSquares != 13 {
		t.Fatalf("重开后作业 2 结果=%d,%d want -1,13", r2.Archive.Sum, r2.Archive.SumOfSquares)
	}
	assertInt64s(t, "重开后作业 2 实际输入", r2.EffectiveValues, []int64{2, -3})
}

// 等待成功上游的排队记录：等待原因仍是等待依赖、待完成上游列出该作业，残留
// 在排队期间不可见；上游成功后实际输入 = 原始整数 + 按依赖次序追加的上游
// 总和，旧残留（哪怕数值自洽）不参与。
func TestReopenQueuedDependentJobDropsResidueAndAwaitsUpstream(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 残留归档假装 [10,9] → 19,181（9 不是任何上游将得到的总和）。
	residual := buildSucceededJob(t, 2, []int64{10}, []int64{10, 9}, []uint64{1}, base)
	if residual.archive.Sum != 19 {
		t.Fatalf("premise residual sum=%d", residual.archive.Sum)
	}

	// 作业 1：[4] → 4，先占用计算位置停住，让 2/3 保持等待依赖。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "up",
		values:   []int64{4},
		queuedAt: base,
		status:   StatusQueued,
	})
	// 作业 2：依赖 1，残留实际输入与归档两者都有。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "child",
		values:          []int64{10},
		dependencies:    []uint64{1},
		queuedAt:        base.Add(2 * time.Second),
		status:          StatusQueued,
		effectiveValues: []int64{10, 9},
		archive:         residual.archive,
	})
	// 作业 3：依赖 1，只残留归档。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "child3",
		values:       []int64{11},
		dependencies: []uint64{1},
		queuedAt:     base.Add(3 * time.Second),
		status:       StatusQueued,
		archive:      residual.archive,
	})

	gate := newComputeGate()
	gate.block(1)
	s, err := Open(dir, withComputeHook(gate.hook))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	started := make(chan struct{})
	go func() {
		for {
			if g, _ := s.Get(1); g.Status == StatusRunning {
				close(started)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("上游未进入运行")
	}

	g2 := mustGet(t, s, 2)
	assertQueuedViewClean(t, g2, WaitDependency, []uint64{1})
	assertUint64s(t, "job2 有序依赖保留", g2.Dependencies, []uint64{1})
	g3 := mustGet(t, s, 3)
	assertQueuedViewClean(t, g3, WaitDependency, []uint64{1})

	// 幂等重放返回同样干净的等待依赖视图。
	replay, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "child", Values: []int64{10},
		Dependencies: []uint64{1},
	})
	if err != nil || replay.ID != 2 {
		t.Fatalf("重放应返回作业 2: id=%d err=%v", replayID(replay), err)
	}
	assertQueuedViewClean(t, replay, WaitDependency, []uint64{1})

	// 磁盘记录排队等待期间已无残留。
	for _, id := range []uint64{2, 3} {
		r := readOnDiskRecord(t, filepath.Join(dir, jobFileName(id)))
		if r.Archive != nil || len(r.EffectiveValues) != 0 || r.Status != StatusQueued {
			t.Fatalf("作业 %d 磁盘排队记录不得有成功产物: %+v", id, r)
		}
	}

	gate.release(1)
	up := waitStatus(t, s, 1, StatusSucceeded)
	if up.Archive.Sum != 4 {
		t.Fatalf("上游结果=%d want 4", up.Archive.Sum)
	}
	// [10,4] → 总和 14、平方和 116。
	done := waitStatus(t, s, 2, StatusSucceeded)
	if done.Archive.Sum != 14 || done.Archive.SumOfSquares != 116 {
		t.Fatalf("作业 2 结果=%d,%d want 14,116", done.Archive.Sum, done.Archive.SumOfSquares)
	}
	assertInt64s(t, "job2 实际输入必须是原始整数+上游总和 [10,4]",
		done.EffectiveValues, []int64{10, 4})
	assertInt64s(t, "job2 计算实际收到 [10,4]", gate.inputsOf(2), []int64{10, 4})
	// [11,4] → 总和 15、平方和 137。
	done3 := waitStatus(t, s, 3, StatusSucceeded)
	if done3.Archive.Sum != 15 || done3.Archive.SumOfSquares != 137 {
		t.Fatalf("作业 3 结果=%d,%d want 15,137", done3.Archive.Sum, done3.Archive.SumOfSquares)
	}
	assertInt64s(t, "job3 实际输入", done3.EffectiveValues, []int64{11, 4})
}

// 重开时已被失败上游阻断的排队记录：既有级联失败规则照常生效，结果详情不含
// 残留产物，根因与原因保持既有归因；幂等重放返回这份干净的失败详情。
func TestReopenQueuedResidueCascadeFailureStillDropsArtifacts(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	residual := buildSucceededJob(t, 2, []int64{10}, []int64{10, 4}, []uint64{1}, base)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "f1",
		values:     []int64{4},
		queuedAt:   base,
		finishedAt: base.Add(time.Second),
		status:     StatusFailed, failureReason: "平方和超出 int64 范围", blockerID: 0,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "child",
		values:          []int64{10},
		dependencies:    []uint64{1},
		queuedAt:        base.Add(2 * time.Second),
		status:          StatusQueued,
		effectiveValues: []int64{10, 4},
		archive:         residual.archive,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	g := waitStatus(t, s, 2, StatusFailed)
	if g.BlockerID != 1 || !strings.Contains(g.FailureReason, "直接上游作业 1") {
		t.Fatalf("作业 2 须按既有规则被失败上游阻断: blocker=%d reason=%q",
			g.BlockerID, g.FailureReason)
	}
	if g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("级联失败详情不得带残留产物: archive=%+v eff=%v", g.Archive, g.EffectiveValues)
	}
	if g.FinishedAt.IsZero() {
		t.Fatal("改判失败应补完成时间")
	}
	// 幂等重放返回同样的失败详情，不重算、不另建。
	replay, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "child", Values: []int64{10},
		Dependencies: []uint64{1},
	})
	if err != nil || replay.ID != 2 || replay.Status != StatusFailed ||
		replay.Archive != nil || replay.EffectiveValues != nil {
		t.Fatalf("重放应返回干净的原失败详情: view=%+v err=%v", replay, err)
	}
	r := readOnDiskRecord(t, filepath.Join(dir, jobFileName(2)))
	if r.Archive != nil || len(r.EffectiveValues) != 0 || r.Status != StatusFailed {
		t.Fatalf("落盘失败记录不得保留残留: %+v", r)
	}
}

// 排队记录使用非默认文件名恢复时，清理写回读入的那份正式文件，不按作业号
// 另建默认命名记录；随后继续排队、完成，状态更新始终写回原文件。
func TestReopenQueuedResidueWithSuffixedFileNameUpdatesSameFile(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	residual := buildSucceededJob(t, 9, []int64{9}, []int64{9}, nil, base)
	// 作业 1 占用计算位置，让使用非默认文件名的作业 2 在排队期间完成清理。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "blocker",
		values:   []int64{100, -50},
		queuedAt: base,
		status:   StatusQueued,
	})
	custom := suffixedRecordName(2)
	j := &storedJob{
		id: 2, submitter: "a", requestID: "r2", seed: 4,
		values:          []int64{2, -3},
		queuedAt:        base.Add(2 * time.Second),
		status:          StatusQueued,
		effectiveValues: []int64{9},
		archive:         residual.archive,
		fileName:        custom,
	}
	data, err := encodeRecord(j)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(dir, custom, data, 0o600); err != nil {
		t.Fatal(err)
	}

	gate := newComputeGate()
	gate.block(1)
	s, err := Open(dir, withComputeHook(gate.hook))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	started := make(chan struct{})
	go func() {
		for {
			if g, _ := s.Get(1); g.Status == StatusRunning {
				close(started)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("作业 1 未进入运行")
	}

	g := mustGet(t, s, 2)
	assertQueuedViewClean(t, g, WaitSlot, nil)
	assertInt64s(t, "原始整数保留", g.Values, []int64{2, -3})
	r := readOnDiskRecord(t, filepath.Join(dir, custom))
	if r.Archive != nil || len(r.EffectiveValues) != 0 || r.Status != StatusQueued {
		t.Fatalf("清理必须写回读入的原文件: %+v", r)
	}
	assertNoDefaultRecord(t, dir, 2)

	gate.release(1)
	waitStatus(t, s, 1, StatusSucceeded)
	done := waitStatus(t, s, 2, StatusSucceeded)
	if done.Archive.Sum != -1 || done.Archive.SumOfSquares != 13 {
		t.Fatalf("结果=%d,%d want -1,13", done.Archive.Sum, done.Archive.SumOfSquares)
	}
	r2 := readOnDiskRecord(t, filepath.Join(dir, custom))
	if r2.Status != StatusSucceeded || r2.Archive == nil ||
		len(r2.EffectiveValues) != 2 || r2.EffectiveValues[0] != 2 || r2.EffectiveValues[1] != -3 {
		t.Fatalf("成功更新也必须写回原文件: %+v", r2)
	}
	assertNoDefaultRecord(t, dir, 2)
}

// 清理结果暂时无法保存时，归档仍按既有恢复规则打开：本次查询不返回旧产物；
// 写入恢复并再次打开后，排队记录重新被清理并正常完成本次计算。
func TestReopenQueuedResidueSanitizePersistFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root：只读目录无法注入保存失败")
	}
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	residual := buildSucceededJob(t, 1, []int64{9}, []int64{9}, nil, base)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "r1",
		values:          []int64{3},
		queuedAt:        base,
		status:          StatusQueued,
		effectiveValues: []int64{9},
		archive:         residual.archive,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "r2",
		values:          []int64{2, -3},
		queuedAt:        base.Add(2 * time.Second),
		status:          StatusQueued,
		effectiveValues: []int64{9},
		archive:         residual.archive,
	})
	// 目录不可写：清理（以及随后的运行状态）无法落盘。
	makeArchiveReadOnly(t, dir)
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("清理落盘失败不应让打开失败: %v", err)
	}

	// 内存中的作业因运行状态无法保存会被保守置为失败，但无论排队还是失败，
	// 本次打开期间所有查询都不能返回旧产物。
	deadline := time.Now().Add(5 * time.Second)
	for _, id := range []uint64{1, 2} {
		for {
			g, gerr := s.Get(id)
			if gerr != nil {
				t.Fatal(gerr)
			}
			if g.Status == StatusFailed || g.Status == StatusQueued {
				if g.Archive != nil || g.EffectiveValues != nil {
					t.Fatalf("作业 %d 本次查询不得返回旧产物: archive=%+v eff=%v",
						id, g.Archive, g.EffectiveValues)
				}
			}
			if g.Status == StatusFailed {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("作业 %d 未进入预期状态: %s", id, g.Status)
			}
			time.Sleep(time.Millisecond)
		}
	}
	// 磁盘记录未被改动：仍带残留与排队状态。
	raw, err := os.ReadFile(filepath.Join(dir, jobFileName(2)))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"archive"`) || !strings.Contains(string(raw), `"status": "queued"`) {
		t.Fatalf("清理无法保存时磁盘记录应保持原样:\n%s", raw)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	makeArchiveWritable(t, dir)

	// 写入恢复后再次打开：排队记录被清理并按既有调度完成本次计算。
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	done := waitStatus(t, s2, 2, StatusSucceeded)
	if done.Archive.Sum != -1 || done.Archive.SumOfSquares != 13 {
		t.Fatalf("重开后结果=%d,%d want -1,13", done.Archive.Sum, done.Archive.SumOfSquares)
	}
	assertInt64s(t, "实际输入 [2,-3]", done.EffectiveValues, []int64{2, -3})
	onDisk := readOnDiskRecord(t, filepath.Join(dir, jobFileName(2)))
	if onDisk.Archive == nil || onDisk.Archive.Sum != -1 {
		t.Fatalf("落盘应为本轮新归档: %+v", onDisk.Archive)
	}
}

// 上次关闭时仍在运行的记录即使残留实际输入与成功归档，也仍按既有中断规则
// 改判失败（不利用残留成为成功），并补完成时间、清空产物；这是既有兼容规则。
func TestReopenInterruptedRunWithResidueStillFails(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	residual := buildSucceededJob(t, 1, []int64{9}, []int64{9}, nil, base)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "r1",
		values:          []int64{2, -3},
		queuedAt:        base,
		startedAt:       base.Add(time.Millisecond),
		status:          StatusRunning,
		effectiveValues: []int64{9},
		archive:         residual.archive,
	})
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	g := mustGet(t, s, 1)
	if g.Status != StatusFailed || !strings.Contains(g.FailureReason, "中断") || g.BlockerID != 0 {
		t.Fatalf("中断运行记录须按既有规则失败: %+v", g)
	}
	if g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("中断失败详情不得带残留产物: archive=%+v eff=%v", g.Archive, g.EffectiveValues)
	}
	if g.FinishedAt.IsZero() {
		t.Fatal("中断改判应补完成时间")
	}
	r := readOnDiskRecord(t, filepath.Join(dir, jobFileName(1)))
	if r.Status != StatusFailed || r.Archive != nil || len(r.EffectiveValues) != 0 {
		t.Fatalf("落盘中断记录不得保留残留: %+v", r)
	}
}
