package numeric

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

// tamperedSuccessRecord 构造一条“数值结果写错但日志、结果摘要与校验值都按
// 错误数字保持一致”的成功记录——即绕过旧完整性检查的唯一缺口。
func tamperedSuccessRecord(id uint64, values, effective []int64, seed, sum, sumSquares int64, at time.Time) *storedJob {
	j := &storedJob{
		id: id, submitter: "a", requestID: "r" + strconv.FormatUint(id, 10),
		seed: seed, values: values, effectiveValues: effective,
		queuedAt: at, startedAt: at.Add(time.Second), finishedAt: at.Add(2 * time.Second),
		status: StatusSucceeded,
	}
	j.archive = newArchive(j, effective, sum, sumSquares, j.finishedAt)
	return j
}

// 成功归档的总和/平方和必须是对归档保存的实际输入按既有规则求出的结果；
// 只让摘要、日志与校验值跟随错误数字保持一致不能再蒙混过关。
func TestReopenRejectsSuccessArchiveWithWrongResults(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 实际输入 [2,-3]：总和应为 -1、平方和应为 13。
	writeSyntheticRecord(t, dir, tamperedSuccessRecord(1, []int64{2, -3}, []int64{2, -3}, 0, 8, 13, base))
	writeSyntheticRecord(t, dir, tamperedSuccessRecord(2, []int64{2, -3}, []int64{2, -3}, 0, -1, 12, base.Add(time.Minute)))
	// 按原有规则总和溢出 int64：回绕后的数字不是有效结果。
	writeSyntheticRecord(t, dir, tamperedSuccessRecord(3,
		[]int64{math.MaxInt64, 1}, []int64{math.MaxInt64, 1}, 0, math.MinInt64, 1, base.Add(2*time.Minute)))
	// 依赖坏记录的排队作业：重开后按既有上游失败处理级联失败。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 4, submitter: "a", requestID: "r4",
		values: []int64{5}, dependencies: []uint64{1},
		queuedAt: base.Add(3 * time.Minute),
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, id := range []uint64{1, 2, 3} {
		g, err := s.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if g.Status != StatusFailed {
			t.Fatalf("job %d: tampered success kept, status=%s", id, g.Status)
		}
		if g.Archive != nil || g.EffectiveValues != nil {
			t.Fatalf("job %d: failed record must not expose archive/effective inputs", id)
		}
		if !strings.Contains(g.FailureReason, "数值结果不可用") {
			t.Fatalf("job %d: reason must say numeric results unusable, got %q", id, g.FailureReason)
		}
	}
	// 排队的下游沿用既有上游失败处理，不能拿着错误总和继续计算。
	j4 := waitStatus(t, s, 4, StatusFailed)
	if j4.BlockerID != 1 || !strings.Contains(j4.FailureReason, "作业 1") {
		t.Fatalf("downstream: blocker=%d reason=%q", j4.BlockerID, j4.FailureReason)
	}
}

// 合法归档不受新增数值核验影响：含依赖追加后的实际输入按既有规则复算一致，
// 重开后仍原样可用，摘要与校验值不变。
func TestReopenKeepsValidArchivesUnderResultCheck(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// id 1：实际输入 [2,-3]，总和 -1、平方和 13。
	up := tamperedSuccessRecord(1, []int64{2, -3}, []int64{2, -3}, 0, -1, 13, base)
	writeSyntheticRecord(t, dir, up)
	// id 2：依赖 id 1，实际输入已含追加的上游总和 -1（不能再追加一次）。
	down := &storedJob{
		id: 2, submitter: "a", requestID: "r2",
		values: []int64{5}, dependencies: []uint64{1},
		effectiveValues: []int64{5, -1},
		queuedAt:        base.Add(time.Minute), startedAt: base.Add(2 * time.Minute),
		finishedAt: base.Add(3 * time.Minute),
		status:     StatusSucceeded,
	}
	down.archive = newArchive(down, down.effectiveValues, 4, 26, down.finishedAt)
	writeSyntheticRecord(t, dir, down)
	wantChecksum := down.archive.Checksum

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	j1, _ := s.Get(1)
	if j1.Status != StatusSucceeded || j1.Archive == nil ||
		j1.Archive.Sum != -1 || j1.Archive.SumOfSquares != 13 {
		t.Fatalf("valid archive must survive: %s %+v", j1.Status, j1.Archive)
	}
	j2, _ := s.Get(2)
	if j2.Status != StatusSucceeded || j2.Archive == nil ||
		j2.Archive.Sum != 4 || j2.Archive.SumOfSquares != 26 {
		t.Fatalf("valid dependent archive must survive: %s %+v", j2.Status, j2.Archive)
	}
	if j2.Archive.Checksum != wantChecksum {
		t.Fatal("checksum must not be recomputed on reopen")
	}
}

