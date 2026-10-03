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

func writeSyntheticRecord(t *testing.T, dir string, j *storedJob) {
	t.Helper()
	data, err := encodeRecord(j)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(dir, jobFileName(j.id), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestReopenPreservesTerminalRecordsAndArchive(t *testing.T) {
	dir := t.TempDir()
	clock := newManualClock()
	s, err := Open(dir, WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	started, block, release, _, _ := gateCompute(t, s)
	block(1)
	ok := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "ok", Values: []int64{1, 2, 3}, Seed: 4})
	waitStarted(t, started, ok.ID)
	cq := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "cq", Values: []int64{7}})
	if _, err := s.Cancel(cq.ID); err != nil { // 排队中取消
		t.Fatal(err)
	}
	big := int64(3037000500)
	bad := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "bad", Values: []int64{big}})
	release(ok.ID)
	waitStatus(t, s, ok.ID, StatusSucceeded)
	wantChecksum := waitStatus(t, s, ok.ID, StatusSucceeded).Archive.Checksum
	waitStatus(t, s, bad.ID, StatusFailed)
	if g, _ := s.Get(cq.ID); g.Status != StatusCanceled {
		t.Fatalf("cq=%s, want canceled", g.Status)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	g, err := s2.Get(ok.ID)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusSucceeded || g.Archive == nil {
		t.Fatalf("success record lost: %+v", g)
	}
	if g.Archive.Checksum != wantChecksum || g.Archive.Sum != 6 || g.Archive.SumOfSquares != 14 {
		t.Fatalf("archive changed across reopen: %+v", g.Archive)
	}
	if g.Archive.InputsDigest != inputsDigestHex([]int64{1, 2, 3}, 4) {
		t.Fatal("inputs digest mismatch after reopen")
	}
	b, _ := s2.Get(bad.ID)
	if b.Status != StatusFailed || !strings.Contains(b.FailureReason, "平方和") || b.Archive != nil {
		t.Fatalf("failure record wrong after reopen: %+v", b)
	}
	c, _ := s2.Get(cq.ID)
	if c.Status != StatusCanceled {
		t.Fatalf("cancel record wrong after reopen: %s", c.Status)
	}

	// 作业号续号，不复用。
	next := mustSubmit(t, s2, SubmitRequest{Submitter: "a", Values: []int64{1}})
	if next.ID <= cq.ID {
		t.Fatalf("new id %d should be greater than %d", next.ID, cq.ID)
	}
	waitStatus(t, s2, next.ID, StatusSucceeded)
}

func TestReopenIdempotencyKeySurvives(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	orig := mustSubmit(t, s, SubmitRequest{Submitter: "u", RequestID: "k1", Values: []int64{1, 2}, Seed: 3})
	waitStatus(t, s, orig.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	// 相同键相同内容 → 原作业。
	replay, err := s2.Submit(SubmitRequest{Submitter: "u", RequestID: "k1", Values: []int64{1, 2}, Seed: 3})
	if err != nil || replay.ID != orig.ID {
		t.Fatalf("replay after reopen: id=%d err=%v, want %d", replayID(replay), err, orig.ID)
	}
	// 内容变化 → 冲突，保留原记录。
	j, err := s2.Submit(SubmitRequest{Submitter: "u", RequestID: "k1", Values: []int64{2, 1}, Seed: 3})
	if !errors.Is(err, ErrIdempotencyConflict) || j == nil || j.ID != orig.ID {
		t.Fatalf("conflict after reopen: %+v %v", j, err)
	}
	// 不同提交人同请求号 → 新作业。
	other := mustSubmit(t, s2, SubmitRequest{Submitter: "v", RequestID: "k1", Values: []int64{1, 2}, Seed: 3})
	if other.ID == orig.ID {
		t.Fatal("request id must be scoped by submitter after reopen")
	}
}

func replayID(j *Job) uint64 {
	if j == nil {
		return 0
	}
	return j.ID
}

func TestReopenRecoversInterruptedRunAndBlocksDownstream(t *testing.T) {
	// 模拟上次进程在作业 1 运行中崩溃：直接留下 running 记录，
	// 以及一个排队等待它的作业 2、再下游作业 3。
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "r1",
		values:    []int64{1, 2},
		queuedAt:  base,
		startedAt: base.Add(time.Second),
		status:    StatusRunning,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "r2",
		values: []int64{3}, dependencies: []uint64{1},
		queuedAt: base.Add(2 * time.Second),
		status:   StatusQueued,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "r3",
		values: []int64{4}, dependencies: []uint64{2},
		queuedAt: base.Add(3 * time.Second),
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	j1, _ := s.Get(1)
	if j1.Status != StatusFailed || !strings.Contains(j1.FailureReason, "中断") {
		t.Fatalf("interrupted job: status=%s reason=%q", j1.Status, j1.FailureReason)
	}
	if j1.Archive != nil {
		t.Fatal("interrupted job must not carry an archive")
	}
	j2 := waitStatus(t, s, 2, StatusFailed)
	j3 := waitStatus(t, s, 3, StatusFailed)
	if j2.BlockerID != 1 || j3.BlockerID != 1 {
		t.Fatalf("downstream blockers %d,%d want root 1", j2.BlockerID, j3.BlockerID)
	}
	if !strings.Contains(j2.FailureReason, "作业 1") {
		t.Fatalf("j2 reason=%q must name blocking job 1", j2.FailureReason)
	}
}

func TestGracefulCloseInterruptsRunningJob(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	started, block, _, _, _ := gateCompute(t, s)
	block(1)
	j1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1, 2}})
	waitStarted(t, started, j1.ID)

	// Close 会让钩子解除并使计算观察到取消；作业以“中断”失败落盘。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// 重复 Close 安全。
	if err := s.Close(); err != nil {
		t.Fatalf("double close: %v", err)
	}
	// 关闭后拒绝新提交。
	if _, err := s.Submit(SubmitRequest{Submitter: "a", Values: []int64{1}}); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("submit after close: %v", err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	g, _ := s2.Get(j1.ID)
	if g.Status != StatusFailed || !strings.Contains(g.FailureReason, "中断") {
		t.Fatalf("graceful close recovery: status=%s reason=%q", g.Status, g.FailureReason)
	}
}

func TestReopenResumesQueuedJobs(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	started, block, _, _, _ := gateCompute(t, s)
	block(1)
	j1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1, 2}})
	j2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "later", Values: []int64{10, 20}})
	waitStarted(t, started, j1.ID)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重开：j1 中断失败，j2 仍排队并继续被处理。
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	g1, _ := s2.Get(j1.ID)
	if g1.Status != StatusFailed {
		t.Fatalf("j1=%s, want failed", g1.Status)
	}
	done := waitStatus(t, s2, j2.ID, StatusSucceeded)
	if done.Archive.Sum != 30 || done.Archive.SumOfSquares != 500 {
		t.Fatalf("resumed result=%d,%d", done.Archive.Sum, done.Archive.SumOfSquares)
	}
}

func TestReopenFailsClosedOnCorruptedSuccessArchive(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	j := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1, 2}})
	waitStatus(t, s, j.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 外部篡改校验值：成功状态不能继续对外宣称成功。
	p := filepath.Join(dir, jobFileName(j.ID))
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(string(data), `"checksum": "`, `"checksum": "f`, 1)
	if tampered == string(data) {
		t.Fatal("checksum field not found in record")
	}
	if err := os.WriteFile(p, []byte(tampered), 0o600); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	g, _ := s2.Get(j.ID)
	if g.Status != StatusFailed || g.Archive != nil {
		t.Fatalf("corrupted archive must fail closed, got %s archive=%v", g.Status, g.Archive)
	}
}

// fixedClock 返回固定时间，可在两次提交之间手动回拨，用于复现本机时钟回拨。
type fixedClock struct{ t time.Time }

func (c *fixedClock) Now() time.Time { return c.t }
func (c *fixedClock) set(t time.Time) {
	c.t = t
}

func atClock(h, m int) time.Time {
	return time.Date(2026, 5, 1, h, m, 0, 0, time.UTC)
}

// gateComputeOption 与 gateCompute 等价，但以 [Option] 形式在 [Open] 启动
// worker 之前装好计算钩子，避免重开后第一批排队作业在钩子装好前就跑完。
func gateComputeOption() (
	Option,
	chan uint64,
	func(ids ...uint64),
	func(id uint64),
	*[]uint64,
	*sync.Mutex,
) {
	started := make(chan uint64, 64)
	var bmu sync.Mutex
	gates := make(map[uint64]chan struct{})
	var ord []uint64
	omu := &sync.Mutex{}
	block := func(ids ...uint64) {
		bmu.Lock()
		defer bmu.Unlock()
		for _, id := range ids {
			gates[id] = make(chan struct{})
		}
	}
	release := func(id uint64) {
		bmu.Lock()
		ch, ok := gates[id]
		bmu.Unlock()
		if ok {
			close(ch)
		}
	}
	opt := func(s *Store) {
		s.compute = func(id uint64, inputs []int64, seed int64, canceled func() bool) (int64, int64, string, bool) {
			started <- id
			bmu.Lock()
			ch := gates[id]
			bmu.Unlock()
			if ch != nil {
				select {
				case <-ch:
				case <-s.stopCh: // Close 时自动放行，随后计算观察到取消而中止
				}
			}
			omu.Lock()
			ord = append(ord, id)
			omu.Unlock()
			return computeResult(inputs, seed, canceled)
		}
	}
	return opt, started, block, release, &ord, omu
}

func snapshotOrder(ord *[]uint64, omu *sync.Mutex) []uint64 {
	omu.Lock()
	defer omu.Unlock()
	return append([]uint64(nil), (*ord)...)
}

// 关闭重开后必须继续按“接受提交的先后”（作业号）排队，记录上的提交时间
// 早晚不能改变这个次序：时钟回拨让后提交的作业留下了更早的时间也不能插队；
// 提交时间完全相同的作业同样保持原接受顺序。
func TestReopenKeepsAcceptanceOrderAcrossClockRollback(t *testing.T) {
	dir := t.TempDir()
	clock := &fixedClock{t: atClock(8, 0)}
	s, err := Open(dir, WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	started, block, _, _, _ := gateCompute(t, s)
	block(1)
	head := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "head", Values: []int64{1}})
	waitStarted(t, started, head.ID)

	// 甲先接受（10:05）；时钟回拨后乙才接受（10:00），记录时间反而更早。
	clock.set(atClock(10, 5))
	jia := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "jia", Values: []int64{2}})
	clock.set(atClock(10, 0))
	yi := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "yi", Values: []int64{3}})
	// 丙、丁提交时间完全相同，须保持接受顺序。
	clock.set(atClock(10, 3))
	bing := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "bing", Values: []int64{4}})
	ding := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "ding", Values: []int64{5}})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	opt, _, _, _, ord, omu := gateComputeOption()
	s2, err := Open(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	waitStatus(t, s2, head.ID, StatusFailed) // 上次运行被中断
	wantOrder := []uint64{jia.ID, yi.ID, bing.ID, ding.ID}
	for _, id := range wantOrder {
		waitStatus(t, s2, id, StatusSucceeded)
	}
	gotOrder := snapshotOrder(ord, omu)
	if len(gotOrder) != len(wantOrder) {
		t.Fatalf("compute order=%v, want %v", gotOrder, wantOrder)
	}
	for i := range wantOrder {
		if gotOrder[i] != wantOrder[i] {
			t.Fatalf("compute order=%v, want %v（乙不得凭更早时间插队）", gotOrder, wantOrder)
		}
	}

	// 列举同样按接受先后，而非按记录时间递增。
	all, err := s2.List("a", atClock(10, 0), atClock(10, 5))
	if err != nil {
		t.Fatal(err)
	}
	gotIDs := idList(all)
	if len(gotIDs) != len(wantOrder) {
		t.Fatalf("list ids=%v, want %v", gotIDs, wantOrder)
	}
	for i := range wantOrder {
		if gotIDs[i] != wantOrder[i] {
			t.Fatalf("list order=%v, want %v", gotIDs, wantOrder)
		}
	}

	// 时间范围筛选仍以各记录保存的提交时间为准：只含 10:00 时只查到乙。
	onlyYi, err := s2.List("a", atClock(10, 0), atClock(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(onlyYi) != 1 || onlyYi[0].ID != yi.ID {
		t.Fatalf("10:00 range=%v, want only 乙(%d)", idList(onlyYi), yi.ID)
	}
	// 同时包含两个时间时先列甲再列乙。
	two, err := s2.List("a", atClock(10, 0), atClock(10, 5))
	if err != nil || two[0].ID != jia.ID || two[1].ID != yi.ID {
		t.Fatalf("[10:00,10:05]=%v err=%v, want 甲先于乙", idList(two), err)
	}
	// 同一时刻 10:03 的丙、丁仍按接受顺序列出。
	tie, err := s2.List("a", atClock(10, 3), atClock(10, 3))
	if err != nil {
		t.Fatal(err)
	}
	if len(tie) != 2 || tie[0].ID != bing.ID || tie[1].ID != ding.ID {
		t.Fatalf("10:03 range=%v, want 丙(%d),丁(%d)", idList(tie), bing.ID, ding.ID)
	}

	// 记录时间不被改写，作业号不被重新分配。
	gYi, _ := s2.Get(yi.ID)
	if !gYi.QueuedAt.Equal(atClock(10, 0)) {
		t.Fatalf("yi queuedAt=%s, 提交时间不得被改写", gYi.QueuedAt)
	}
	next := mustSubmit(t, s2, SubmitRequest{Submitter: "a", Values: []int64{9}})
	if next.ID <= ding.ID {
		t.Fatalf("new job id %d must be greater than %d", next.ID, ding.ID)
	}
	waitStatus(t, s2, next.ID, StatusSucceeded)
}

func idList(js []*Job) []uint64 {
	ids := make([]uint64, len(js))
	for i, j := range js {
		ids[i] = j.ID
	}
	return ids
}

// 恢复后等待依赖的作业仍被跳过：选取可运行作业时只考虑当下依赖已全部成功的
// 排队作业，等待者不会阻塞后面已经可运行的作业。
func TestReopenSkipsWaitingJobWithoutBlockingLaterRunnable(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	started, block, _, _, _ := gateCompute(t, s)
	block(1)
	head := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "head", Values: []int64{1}})
	waitStarted(t, started, head.ID)
	// up 无依赖；waiter 等待 up；ready 无依赖、提交更晚。
	up := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "up", Values: []int64{7}})
	waiter := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "waiter", Values: []int64{2},
		Dependencies: []uint64{up.ID},
	})
	ready := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "ready", Values: []int64{3}})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Open 启动 worker 前堵住 up：head 中断失败后 up 进入计算并停住，
	// waiter 仍等待依赖，此时可被选取的第一个排队作业必须是 ready，而非 waiter。
	opt, started2, block2, release2, _, _ := gateComputeOption()
	block2(up.ID)
	s2, err := Open(dir, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	waitStatus(t, s2, head.ID, StatusFailed)
	waitStarted(t, started2, up.ID)
	w, _ := s2.Get(waiter.ID)
	if w.Status != StatusQueued || w.WaitReason != WaitDependency ||
		len(w.PendingDependencies) != 1 || w.PendingDependencies[0] != up.ID {
		t.Fatalf("waiter after reopen: status=%s reason=%q pending=%v",
			w.Status, w.WaitReason, w.PendingDependencies)
	}
	// 直接验证调度选取：跳过等待中的 waiter，返回后面已可运行的 ready。
	s2.mu.Lock()
	pick := s2.pickRunnableLocked()
	s2.mu.Unlock()
	if pick == nil || pick.id != ready.ID {
		got := uint64(0)
		if pick != nil {
			got = pick.id
		}
		t.Fatalf("pickRunnable=%d, want ready %d（等待依赖的 waiter 必须被跳过）", got, ready.ID)
	}

	release2(up.ID)
	waitStatus(t, s2, up.ID, StatusSucceeded)
	// up 一旦成功，waiter 的依赖即就绪；它作业号更小，先于 ready 运行。
	waitStatus(t, s2, waiter.ID, StatusSucceeded)
	waitStatus(t, s2, ready.ID, StatusSucceeded)
}

// 上游因上次计算中断而失败时，即便下游记录的提交时间早于上游，等待它的作业
// 及更下游（含多依赖）也必须在本次打开完成时级联为失败：详情说明直接阻断者，
// 并保留沿链条传递的根因作业号。
func TestReopenCascadesInterruptedFailureRegardlessOfTimestamps(t *testing.T) {
	dir := t.TempDir()
	base := atClock(10, 5)
	// id 1 运行中（中断），记录时间 10:05；下游 id 2/3/4 的时间依次更早。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", requestID: "r1",
		values:    []int64{1},
		queuedAt:  base,
		startedAt: base.Add(time.Second),
		status:    StatusRunning,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "r2",
		values: []int64{2}, dependencies: []uint64{1},
		queuedAt: atClock(10, 0), // 早于上游
		status:   StatusQueued,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "r3",
		values: []int64{3}, dependencies: []uint64{2},
		queuedAt: atClock(9, 55), // 更早
		status:   StatusQueued,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 4, submitter: "a", requestID: "r4",
		values: []int64{4}, dependencies: []uint64{2, 3},
		queuedAt: atClock(9, 50),
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	j1, _ := s.Get(1)
	if j1.Status != StatusFailed || !strings.Contains(j1.FailureReason, "中断") {
		t.Fatalf("interrupted: status=%s reason=%q", j1.Status, j1.FailureReason)
	}
	j2 := waitStatus(t, s, 2, StatusFailed)
	j3 := waitStatus(t, s, 3, StatusFailed)
	j4 := waitStatus(t, s, 4, StatusFailed)
	if j2.BlockerID != 1 || j3.BlockerID != 1 || j4.BlockerID != 1 {
		t.Fatalf("blockers %d,%d,%d want root 1", j2.BlockerID, j3.BlockerID, j4.BlockerID)
	}
	// 直接阻断者按依赖列表顺序指出；根因作业号沿链条传递。
	if !strings.Contains(j2.FailureReason, "直接上游作业 1") {
		t.Fatalf("j2 reason=%q must name direct blocker 1", j2.FailureReason)
	}
	if !strings.Contains(j4.FailureReason, "直接上游作业 2") ||
		!strings.Contains(j4.FailureReason, "阻断根因为作业 1") {
		t.Fatalf("j4 reason=%q must name direct blocker 2 and root 1", j4.FailureReason)
	}
}

func TestNoSuccessWithoutArchiveOnDisk(t *testing.T) {
	// 磁盘上成功状态与归档共存于同一文件：成功作业落盘文件必须同时含两者。
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	j := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1, 2, 3}})
	waitStatus(t, s, j.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, jobFileName(j.ID)))
	if err != nil {
		t.Fatal(err)
	}
	str := string(data)
	if !strings.Contains(str, `"status": "succeeded"`) {
		t.Fatal("status field missing")
	}
	if !strings.Contains(str, `"checksum"`) || !strings.Contains(str, `"result_digest"`) ||
		!strings.Contains(str, `"effective_values"`) || !strings.Contains(str, `"log"`) {
		t.Fatal("succeeded record on disk must carry full archive")
	}
}
