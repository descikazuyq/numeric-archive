package numeric

import (
	"errors"
	"os"
	"path/filepath"
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
		values: []int64{3}, hasDependency: true, dependencyID: 1,
		queuedAt: base.Add(2 * time.Second),
		status:   StatusQueued,
	})
	writeSyntheticRecord(t, dir, &storedJob{
		id: 3, submitter: "a", requestID: "r3",
		values: []int64{4}, hasDependency: true, dependencyID: 2,
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
