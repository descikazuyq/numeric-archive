package numeric

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 本文件为“作业号空间（uint64）耗尽后明确拒绝新作业”的行为提供回归保障。
// 已接受作业的最大编号达到上限时，归档本身不损坏：已有作业继续按原规则
// 查询、列举与处理；只有需要创建记录的新请求被 ErrJobIDExhausted 拒绝，
// 回绕得到的 0 或任何更小的数字都不会被当作可用编号，较小编号上的空缺
// 也不回头填补。还剩最后一个编号时合法请求正常取得该编号；该次提交保存
// 失败不消耗编号；已接受作业后来失败或取消也不释放编号。

// writeFailedRecord 直接向目录写入一条失败终态的正式记录（不参与计算），
// 用于构造“最大编号已抵上限”的归档。
func writeFailedRecord(t *testing.T, dir string, id uint64, submitter, requestID string, values []int64, seed int64) {
	t.Helper()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	j := &storedJob{
		id:            id,
		submitter:     submitter,
		requestID:     requestID,
		seed:          seed,
		values:        append([]int64(nil), values...),
		queuedAt:      at,
		finishedAt:    at,
		status:        StatusFailed,
		failureReason: "构造的既有失败记录",
	}
	data, err := encodeRecord(j)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(dir, jobFileName(id), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// 最大编号已达上限的归档仍能正常打开：已有作业可查询、可列举，幂等重放与
// 幂等冲突按原规则返回原作业；只有新请求被 ErrJobIDExhausted 拒绝，且拒绝
// 不产生记录、不登记请求号、查询与列举均不可见，已有记录不被改写。即使
// 较小编号全部空缺，也不回头补号，作业号 0 同样不会成为新作业的编号。
func TestJobIDExhaustedRejectsNewRequestsButArchiveStaysUsable(t *testing.T) {
	dir := t.TempDir()
	// 目录中只有最大编号一份记录：较小编号全部空缺，也不能回头补号。
	writeFailedRecord(t, dir, math.MaxUint64, "alice", "r1", []int64{3}, 2)
	diskBefore, err := os.ReadFile(filepath.Join(dir, jobFileName(math.MaxUint64)))
	if err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("archive with max job id must still open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// 已有作业按原规则可见。
	g, err := s.Get(math.MaxUint64)
	if err != nil || g.Status != StatusFailed || g.RequestID != "r1" {
		t.Fatalf("existing max-id job must be queryable: %+v err=%v", g, err)
	}

	// 新请求（空请求号）：拒绝，返回空作业与编号耗尽错误。
	j, err := s.Submit(SubmitRequest{Submitter: "bob", Values: []int64{1}})
	if !errors.Is(err, ErrJobIDExhausted) || j != nil {
		t.Fatalf("new request after exhaustion: job=%+v err=%v, want nil job and ErrJobIDExhausted", j, err)
	}
	// 新请求（不同提交人使用已存在的请求号，仍属新请求）：同样拒绝。
	j, err = s.Submit(SubmitRequest{Submitter: "bob", RequestID: "r1", Values: []int64{3}, Seed: 2})
	if !errors.Is(err, ErrJobIDExhausted) || j != nil {
		t.Fatalf("new request from different submitter: job=%+v err=%v, want nil job and ErrJobIDExhausted", j, err)
	}
	// 新请求（同一提交人首次使用的请求号）：同样拒绝。
	j, err = s.Submit(SubmitRequest{Submitter: "alice", RequestID: "r2", Values: []int64{9}})
	if !errors.Is(err, ErrJobIDExhausted) || j != nil {
		t.Fatalf("new request id: job=%+v err=%v, want nil job and ErrJobIDExhausted", j, err)
	}

	// 被拒绝的请求在任何视角都不可见：查询与列举都看不到，作业号 0 与
	// 回绕后的任何数字都没有成为新作业的编号，目录中没有新记录。
	if _, err := s.Get(0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("job id 0 must never be assigned: %v", err)
	}
	list, err := s.List("alice", time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != math.MaxUint64 {
		t.Fatalf("rejected submits visible via List(alice): %v", ids(list))
	}
	listBob, err := s.List("bob", time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listBob) != 0 {
		t.Fatalf("rejected submits visible via List(bob): %v", ids(listBob))
	}
	if countJobFiles(t, dir) != 1 || countTmpFiles(t, dir) != 0 {
		t.Fatalf("rejected submits must not create records: files=%d tmps=%d",
			countJobFiles(t, dir), countTmpFiles(t, dir))
	}
	s.mu.Lock()
	_, claimedR2 := s.idem[idemIdentity{"alice", "r2"}]
	_, claimedBob := s.idem[idemIdentity{"bob", "r1"}]
	s.mu.Unlock()
	if claimedR2 || claimedBob {
		t.Fatalf("rejected submits must not register request ids: r2=%v bob/r1=%v", claimedR2, claimedBob)
	}

	// 编号耗尽只限制创建新作业：相同提交人+相同请求号+相同内容仍返回原作业
	// 的当前详情，内容不同仍返回幂等冲突，都不能被编号耗尽错误替代。
	replay, err := s.Submit(SubmitRequest{Submitter: "alice", RequestID: "r1", Values: []int64{3}, Seed: 2})
	if err != nil || replay == nil || replay.ID != math.MaxUint64 || replay.Status != StatusFailed {
		t.Fatalf("idempotent replay must return original job: %+v err=%v", replay, err)
	}
	conflict, err := s.Submit(SubmitRequest{Submitter: "alice", RequestID: "r1", Values: []int64{4}, Seed: 2})
	if !errors.Is(err, ErrIdempotencyConflict) || conflict == nil || conflict.ID != math.MaxUint64 {
		t.Fatalf("idempotency conflict must not be replaced by exhaustion: %+v err=%v", conflict, err)
	}

	// 已有作业的参数、状态与落盘记录未被本次拒绝改写。
	g, err = s.Get(math.MaxUint64)
	if err != nil || g.Status != StatusFailed || g.Seed != 2 ||
		len(g.Values) != 1 || g.Values[0] != 3 {
		t.Fatalf("existing job altered by rejected submits: %+v err=%v", g, err)
	}
	diskAfter, err := os.ReadFile(filepath.Join(dir, jobFileName(math.MaxUint64)))
	if err != nil {
		t.Fatal(err)
	}
	if string(diskAfter) != string(diskBefore) {
		t.Fatal("existing on-disk record changed despite rejected submits")
	}
}

// 还剩最后一个编号时，合法的新请求正常取得最大作业号并按现有规则计算与
// 归档；只有这次提交被确认保存和接受之后，后续新提交才进入编号耗尽的拒绝
// 行为。重开归档后该状态保持：最大编号作业仍是带完整归档的成功记录，新
// 请求仍被拒绝。
func TestLastJobIDAcceptedNormallyThenExhausted(t *testing.T) {
	dir := t.TempDir()
	writeFailedRecord(t, dir, math.MaxUint64-1, "alice", "prev", []int64{1}, 0)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	last := mustSubmit(t, s, SubmitRequest{Submitter: "alice", RequestID: "last", Values: []int64{2, 3}, Seed: 5})
	if last.ID != math.MaxUint64 {
		t.Fatalf("last request got id=%d, want %d", last.ID, uint64(math.MaxUint64))
	}
	done := waitStatus(t, s, last.ID, StatusSucceeded)
	if done.Archive == nil || done.Archive.Sum != 5 || done.Archive.SumOfSquares != 13 {
		t.Fatalf("last job must compute and archive normally: %+v", done)
	}

	// 最后一个编号被接受之后，后续新提交才被拒绝。
	j, err := s.Submit(SubmitRequest{Submitter: "alice", Values: []int64{1}})
	if !errors.Is(err, ErrJobIDExhausted) || j != nil {
		t.Fatalf("submit after last id accepted: job=%+v err=%v, want nil job and ErrJobIDExhausted", j, err)
	}
	if _, err := s.Get(0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("job id 0 must never be assigned: %v", err)
	}
	list, err := s.List("alice", time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].ID != math.MaxUint64-1 || list[1].ID != math.MaxUint64 {
		t.Fatalf("list after exhaustion: %v", ids(list))
	}

	// 重开归档：最大编号的成功记录原样恢复，编号耗尽状态保持。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen archive at id limit: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	g, err := s2.Get(math.MaxUint64)
	if err != nil || g.Status != StatusSucceeded || g.Archive == nil || g.Archive.Sum != 5 {
		t.Fatalf("max-id job after reopen: %+v err=%v", g, err)
	}
	j, err = s2.Submit(SubmitRequest{Submitter: "alice", Values: []int64{1}})
	if !errors.Is(err, ErrJobIDExhausted) || j != nil {
		t.Fatalf("submit after reopen: job=%+v err=%v, want nil job and ErrJobIDExhausted", j, err)
	}
	// 幂等重放跨重开仍然有效，不被编号耗尽错误替代。
	replay, err := s2.Submit(SubmitRequest{Submitter: "alice", RequestID: "last", Values: []int64{2, 3}, Seed: 5})
	if err != nil || replay == nil || replay.ID != math.MaxUint64 || replay.Status != StatusSucceeded {
		t.Fatalf("idempotent replay after reopen: %+v err=%v", replay, err)
	}
}

// 最后一次编号的提交保存失败时，返回实际保存错误且不接受作业；编号不会
// 因为一次未接受的请求就永久耗尽——写入恢复后重新提交取得同一个最后编号，
// 此后新提交才进入拒绝行为。
func TestLastJobIDPersistFailureDoesNotExhaust(t *testing.T) {
	dir := t.TempDir()
	writeFailedRecord(t, dir, math.MaxUint64-1, "alice", "prev", []int64{1}, 0)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	req := SubmitRequest{Submitter: "alice", RequestID: "last", Values: []int64{7}, Seed: 1}
	injected := errors.New("injected save failure")
	s.persistFault = func(j *storedJob) error {
		if j.id == math.MaxUint64 {
			return injected
		}
		return nil
	}
	j, err := s.Submit(req)
	if !errors.Is(err, injected) || j != nil {
		t.Fatalf("last-id submit with save failure: job=%+v err=%v, want actual save error and nil job", j, err)
	}
	// 未被接受：查询与列举看不到，请求号未登记，编号未耗尽。
	if _, err := s.Get(math.MaxUint64); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unaccepted last-id submit visible via Get: %v", err)
	}
	s.mu.Lock()
	_, claimed := s.idem[idemIdentity{"alice", "last"}]
	exhausted := s.idsExhausted
	s.mu.Unlock()
	if claimed || exhausted {
		t.Fatalf("unaccepted submit must not register request id or exhaust ids: claimed=%v exhausted=%v",
			claimed, exhausted)
	}

	// 写入恢复后重新提交：取得同一个最后编号。
	s.persistFault = nil
	last := mustSubmit(t, s, req)
	if last.ID != math.MaxUint64 {
		t.Fatalf("retry after save recovery got id=%d, want %d", last.ID, uint64(math.MaxUint64))
	}
	waitStatus(t, s, last.ID, StatusSucceeded)
	j, err = s.Submit(SubmitRequest{Submitter: "alice", Values: []int64{1}})
	if !errors.Is(err, ErrJobIDExhausted) || j != nil {
		t.Fatalf("submit after last id accepted: job=%+v err=%v, want nil job and ErrJobIDExhausted", j, err)
	}
}

// 已经接受最后一个编号的作业后来失败或取消，也不释放它的编号：新请求仍被
// ErrJobIDExhausted 拒绝，不会把回绕后的数字当作可用编号。
func TestExhaustionNotRelievedByFailureOrCancel(t *testing.T) {
	dir := t.TempDir()
	writeFailedRecord(t, dir, math.MaxUint64-1, "alice", "prev", []int64{1}, 0)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// 让计算阻塞，便于在作业运行中取消它。
	release := make(chan struct{})
	s.compute = func(id uint64, in []int64, seed int64, canceled func() bool) (int64, int64, string, bool) {
		<-release
		return computeResult(in, seed, canceled)
	}
	last := mustSubmit(t, s, SubmitRequest{Submitter: "alice", RequestID: "last", Values: []int64{1, 2}})
	if last.ID != math.MaxUint64 {
		t.Fatalf("last request got id=%d, want %d", last.ID, uint64(math.MaxUint64))
	}
	waitStatus(t, s, last.ID, StatusRunning)
	if _, err := s.Cancel(last.ID); err != nil {
		t.Fatal(err)
	}
	close(release)
	canceled := waitStatus(t, s, last.ID, StatusCanceled)
	if canceled.ID != math.MaxUint64 {
		t.Fatalf("canceled job must keep its id: %+v", canceled)
	}

	// 取消不释放编号：新请求仍被拒绝。
	j, err := s.Submit(SubmitRequest{Submitter: "alice", Values: []int64{1}})
	if !errors.Is(err, ErrJobIDExhausted) || j != nil {
		t.Fatalf("cancel of last-id job must not release the id: job=%+v err=%v", j, err)
	}
	if _, err := s.Get(0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("job id 0 must never be assigned: %v", err)
	}
	list, err := s.List("alice", time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[1].ID != math.MaxUint64 || list[1].Status != StatusCanceled {
		t.Fatalf("list after cancel: %v", ids(list))
	}
}
