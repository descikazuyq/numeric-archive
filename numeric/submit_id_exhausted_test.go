package numeric

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 本文件覆盖“作业号（uint64）到达上限后不得继续分配编号”这一形态：
// 已接受作业的最大作业号达到 math.MaxUint64 时，归档仍正常打开、已有作业
// 照常查询/列举/处理，但任何需要创建作业的新请求都必须返回空作业与
// ErrJobIDsExhausted——编号不回绕为 0、不回头补没有记录的小编号；拒绝不留
// 任何痕迹，也不改变已有作业。幂等重放与幂等冲突不需要新编号，优先于耗尽
// 拒绝。最后一个编号可正常分配并接受，只有确认保存后才进入耗尽；最后一次
// 提交保存失败不消耗编号；已接受作业后来失败或取消不释放编号。

// maxJobID 是 uint64 可表示的最大作业号。
const maxJobID = ^uint64(0)

// assertSubmitBlockedByExhaustion 提交一次新请求并断言它被作业号耗尽错误
// 拒绝：返回空作业与非空错误，错误包装 ErrJobIDsExhausted，并明确给出
// 已达到的 uint64 上限编号。
func assertSubmitBlockedByExhaustion(t *testing.T, s *Store, req SubmitRequest) {
	t.Helper()
	j, err := s.Submit(req)
	if j != nil || err == nil {
		t.Fatalf("作业号耗尽后必须拒绝新请求：job=%+v err=%v", j, err)
	}
	if !errors.Is(err, ErrJobIDsExhausted) {
		t.Fatalf("错误必须包装 ErrJobIDsExhausted，got %v", err)
	}
	if !strings.Contains(err.Error(), "18446744073709551615") {
		t.Fatalf("错误必须明确说明已达到的 uint64 上限编号：%v", err)
	}
}

// assertExhaustedNewRequestNoTrace 断言耗尽拒绝不留任何痕迹：回绕后的作业号
// 0（以及 absentSmallIDs 中其他没有记录的小编号）查询不到、按提交人列举
// 看不到、目录中的正式记录数不变、没有残留临时文件、内存中的下一个编号仍是
// 耗尽标记 0、请求号未登记。absentSmallIDs 只列归档中确实不存在的编号。
func assertExhaustedNewRequestNoTrace(t *testing.T, s *Store, dir string, req SubmitRequest, wantFiles int, absentSmallIDs ...uint64) {
	t.Helper()
	absent := append([]uint64{0}, absentSmallIDs...)
	for _, id := range absent {
		if _, err := s.Get(id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("编号 %d 不能成为新作业编号：Get(%d)=%v", id, id, err)
		}
	}
	list, err := s.List(req.Submitter, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range list {
		for _, id := range absent {
			if j.ID == id {
				t.Fatalf("被拒绝请求不应出现在按提交人列举中：%v", ids(list))
			}
		}
	}
	if got := countJobFiles(t, dir); got != wantFiles {
		t.Fatalf("目录中的正式记录数变化：got %d want %d", got, wantFiles)
	}
	if got := countTmpFiles(t, dir); got != 0 {
		t.Fatalf("被拒绝请求不得残留临时文件：%d", got)
	}
	s.mu.Lock()
	nextID := s.nextID
	_, claimed := s.idem[idemIdentity{req.Submitter, req.RequestID}]
	s.mu.Unlock()
	if nextID != 0 {
		t.Fatalf("耗尽状态必须保持（nextID 为耗尽标记 0），got nextID=%d", nextID)
	}
	if claimed {
		t.Fatalf("拒绝不得登记提交人的请求号 %q", req.RequestID)
	}
}

// TestExhaustedArchiveOpensAndRejectsNewSubmission 核心场景：目录中只有一份
// 内容合法、作业号为 math.MaxUint64 的成功记录。归档必须正常打开，已有作业
// 继续可读；此后任何新请求（不同提交人、首次使用的请求号、空请求号）都得到
// 空作业与 ErrJobIDsExhausted，编号不回绕、不补号，拒绝不留任何痕迹，也不
// 改写已有记录。
func TestExhaustedArchiveOpensAndRejectsNewSubmission(t *testing.T) {
	dir := t.TempDir()
	last := succeededRecordInFile(t, maxJobID, "alice", "last-req", []int64{6}, restoredOccupiedBase)
	writeNamedSyntheticRecord(t, dir, jobFileName(maxJobID), last)

	s, err := Open(dir, WithClock(newManualClock()))
	if err != nil {
		t.Fatalf("最高作业号达到上限的合法归档仍必须正常打开：%v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	old, err := s.Get(maxJobID)
	if err != nil {
		t.Fatal(err)
	}
	if old.Status != StatusSucceeded || old.Archive == nil ||
		old.Archive.Sum != 6 || old.Archive.SumOfSquares != 36 {
		t.Fatalf("已有作业应按原规则恢复：%+v", old)
	}
	// 按提交人列举照常包含该作业。
	lst, err := s.List("alice", time.Time{}, time.Time{})
	if err != nil || len(lst) != 1 || lst[0].ID != maxJobID {
		t.Fatalf("已有作业应可按提交人列举：%v err=%v", ids(lst), err)
	}
	// 已成功作业不可取消，原有终态规则在耗尽归档中继续生效。
	if _, err := s.Cancel(maxJobID); !errors.Is(err, ErrNotCancellable) {
		t.Fatalf("已有作业的取消规则应保持不变：%v", err)
	}

	before, err := os.ReadFile(filepath.Join(dir, jobFileName(maxJobID)))
	if err != nil {
		t.Fatal(err)
	}

	// 不同提交人、首次使用的请求号、空请求号都属于新请求，一律耗尽拒绝。
	newReqs := []SubmitRequest{
		{Submitter: "bob", RequestID: "r1", Values: []int64{1}},
		{Submitter: "bob", Values: []int64{2}},
		{Submitter: "alice", RequestID: "first-use", Values: []int64{3}},
		{Submitter: "", RequestID: "x", Values: []int64{4}},
	}
	for _, req := range newReqs {
		assertSubmitBlockedByExhaustion(t, s, req)
		assertExhaustedNewRequestNoTrace(t, s, dir, req, 1, 1)
	}

	// 原有的输入与依赖合法性要求继续生效，且先于耗尽拒绝：空序列、依赖列表
	// 结构不合法、引用不存在上游的新请求分别得到原有错误，而不是耗尽错误。
	if _, err := s.Submit(SubmitRequest{Submitter: "bob", RequestID: "r2", Values: nil}); !errors.Is(err, ErrEmptySequence) {
		t.Fatalf("空序列必须保持原有拒绝，got %v", err)
	}
	if _, err := s.Submit(SubmitRequest{
		Submitter: "bob", RequestID: "r2", Values: []int64{1},
		Dependencies: []uint64{1, 1},
	}); !errors.Is(err, ErrInvalidDependency) {
		t.Fatalf("重复依赖必须保持原有拒绝，got %v", err)
	}
	if _, err := s.Submit(SubmitRequest{
		Submitter: "bob", RequestID: "r2", Values: []int64{1},
		Dependencies: []uint64{12345},
	}); !errors.Is(err, ErrDependencyNotFound) {
		t.Fatalf("引用不存在上游必须保持原有拒绝，got %v", err)
	}

	// 已有作业的参数、状态与成功归档逐字节不变。
	after, err := os.ReadFile(filepath.Join(dir, jobFileName(maxJobID)))
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("被拒绝提交不得改写已有作业记录：\nbefore=%s\nafter=%s", before, after)
	}
	g, _ := s.Get(maxJobID)
	if g.Status != StatusSucceeded || g.Archive == nil ||
		g.Archive.Sum != 6 || g.Archive.SumOfSquares != 36 ||
		g.Archive.Checksum != old.Archive.Checksum {
		t.Fatalf("已有作业的状态、结果与校验值不得因拒绝而改变：%+v", g)
	}

	// 重开归档：仍正常打开，新请求仍被耗尽拒绝，已有作业保持不变。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("耗尽归档重开不应失败：%v", err)
	}
	defer s2.Close()
	g2, _ := s2.Get(maxJobID)
	if g2.Status != StatusSucceeded || g2.Archive == nil ||
		g2.Archive.Checksum != old.Archive.Checksum {
		t.Fatalf("重开后已有作业应原样保留：%+v", g2)
	}
	assertSubmitBlockedByExhaustion(t, s2, SubmitRequest{Submitter: "carol", RequestID: "z", Values: []int64{9}})
	if _, err := s2.Get(0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("重开后作业号 0 同样不能存在：%v", err)
	}
}

// TestExhaustionDoesNotOverrideIdempotency 编号耗尽只限制创建新作业：相同
// 提交人用已有非空请求号提交相同内容仍返回原作业当前详情；内容不同仍按幂等
// 冲突返回原作业与错误；已失败作业的同内容重放同样返回原作业。三者都不被
// 耗尽错误替代，也不改动任何记录。
func TestExhaustionDoesNotOverrideIdempotency(t *testing.T) {
	dir := t.TempDir()
	writeNamedSyntheticRecord(t, dir, jobFileName(maxJobID),
		succeededRecordInFile(t, maxJobID, "alice", "last-req", []int64{6}, restoredOccupiedBase))
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	before, _ := os.ReadFile(filepath.Join(dir, jobFileName(maxJobID)))

	// 相同提交人+非空请求号+相同内容：返回原作业当前详情，无错误。
	replay := SubmitRequest{Submitter: "alice", RequestID: "last-req", Values: []int64{6}}
	j, err := s.Submit(replay)
	if err != nil || j == nil || j.ID != maxJobID || j.Status != StatusSucceeded {
		t.Fatalf("耗尽时幂等重放必须返回原作业：job=%+v err=%v", j, err)
	}
	if j.Archive == nil || j.Archive.Sum != 6 {
		t.Fatalf("幂等重放应返回原作业当前详情：%+v", j)
	}

	// 相同提交人+请求号但内容不同：仍是幂等冲突（同时返回原作业），不能被
	// 耗尽错误替代。
	conflictReq := SubmitRequest{Submitter: "alice", RequestID: "last-req", Values: []int64{6, 7}}
	cj, err := s.Submit(conflictReq)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("内容不同应返回幂等冲突，got %v", err)
	}
	if errors.Is(err, ErrJobIDsExhausted) {
		t.Fatalf("幂等冲突不得被耗尽错误替代：%v", err)
	}
	if cj == nil || cj.ID != maxJobID {
		t.Fatalf("幂等冲突仍须返回原作业视图：%+v", cj)
	}

	// 原记录字节不变，编号状态仍为耗尽。
	after, _ := os.ReadFile(filepath.Join(dir, jobFileName(maxJobID)))
	if string(after) != string(before) {
		t.Fatal("幂等重放/冲突不得改写已有记录")
	}

	// 已失败的已接受作业：同内容重放返回该失败作业本身，同样不报耗尽。
	dir2 := t.TempDir()
	big := int64(3037000500)
	failedRec := &storedJob{
		id: maxJobID, submitter: "a", requestID: "k", values: []int64{big},
		queuedAt:      restoredOccupiedBase,
		finishedAt:    restoredOccupiedBase.Add(time.Second),
		status:        StatusFailed,
		failureReason: "平方和超出有符号 64 位整数范围",
	}
	writeNamedSyntheticRecord(t, dir2, jobFileName(maxJobID), failedRec)
	s2, err := Open(dir2)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	again := SubmitRequest{Submitter: "a", RequestID: "k", Values: []int64{big}}
	rj, err := s2.Submit(again)
	if err != nil || rj == nil || rj.ID != maxJobID || rj.Status != StatusFailed {
		t.Fatalf("失败作业的同内容重放必须返回原作业而非耗尽错误：job=%+v err=%v", rj, err)
	}
}

// TestExistingQueuedJobStillProcessesWhenIDsExhausted 耗尽归档中未完成的排队
// 作业重开后继续按原规则计算与归档；它在跑的同时新请求仍被耗尽拒绝，两份
// 已有作业都按提交先后（作业号）正常列举。
func TestExistingQueuedJobStillProcessesWhenIDsExhausted(t *testing.T) {
	dir := t.TempDir()
	writeNamedSyntheticRecord(t, dir, jobFileName(1),
		queuedStoredJob(1, "alice", []int64{9}, restoredOccupiedBase))
	writeNamedSyntheticRecord(t, dir, jobFileName(maxJobID),
		succeededRecordInFile(t, maxJobID, "alice", "last-req", []int64{6},
			restoredOccupiedBase.Add(time.Second)))

	s, err := Open(dir, WithClock(newManualClock()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// 排队的小编号作业照常被唯一 worker 拾起、计算并归档。
	done := waitStatus(t, s, 1, StatusSucceeded)
	if done.Archive == nil || done.Archive.Sum != 9 || done.Archive.SumOfSquares != 81 {
		t.Fatalf("耗尽归档中的已有排队作业应照常完成：%+v", done)
	}
	// 它使用自己的默认命名文件；最大编号作业记录不受影响。
	if countJobFiles(t, dir) != 2 {
		t.Fatalf("目录应恰好有两份正式记录：%d", countJobFiles(t, dir))
	}

	assertSubmitBlockedByExhaustion(t, s, SubmitRequest{Submitter: "bob", RequestID: "n", Values: []int64{1}})
	assertExhaustedNewRequestNoTrace(t, s, dir,
		SubmitRequest{Submitter: "bob", RequestID: "n", Values: []int64{1}}, 2)

	lst, _ := s.List("alice", time.Time{}, time.Time{})
	if len(lst) != 2 || lst[0].ID != 1 || lst[1].ID != maxJobID {
		t.Fatalf("列举应按提交先后保留两份已有作业：%v", ids(lst))
	}
}

// TestLastJobIDAcceptedThenExhausted 还剩最后一个编号时，合法新请求正常取得
// math.MaxUint64 并按既有规则计算归档；只有这次提交确认接受之后，后续新
// 提交才进入耗尽拒绝，重开归档后同样保持耗尽。
func TestLastJobIDAcceptedThenExhausted(t *testing.T) {
	dir := t.TempDir()
	// 恢复出的最大作业号为 MaxUint64-1，因此下一个可分配编号恰好是上限。
	writeNamedSyntheticRecord(t, dir, jobFileName(maxJobID-1),
		succeededRecordInFile(t, maxJobID-1, "alice", "prev", []int64{5}, restoredOccupiedBase))

	s, err := Open(dir, WithClock(newManualClock()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	last := mustSubmit(t, s, SubmitRequest{Submitter: "bob", RequestID: "last", Values: []int64{7}})
	if last.ID != maxJobID {
		t.Fatalf("最后一个编号必须正常分配：got %d want %d", last.ID, maxJobID)
	}
	done := waitStatus(t, s, maxJobID, StatusSucceeded)
	if done.Archive == nil || done.Archive.Sum != 7 || done.Archive.SumOfSquares != 49 {
		t.Fatalf("取得最后编号的作业应按现有规则计算归档：%+v", done)
	}

	// 只有确认接受之后，后续新提交才进入耗尽拒绝。
	assertSubmitBlockedByExhaustion(t, s, SubmitRequest{Submitter: "carol", RequestID: "x", Values: []int64{1}})
	if _, err := s.Get(0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("耗尽后作业号 0 不能出现：%v", err)
	}

	// 前一个作业的记录保持完好。
	prev, _ := s.Get(maxJobID - 1)
	if prev.Status != StatusSucceeded || prev.Archive == nil || prev.Archive.Sum != 5 {
		t.Fatalf("已有作业不应受影响：%+v", prev)
	}

	// 重开：最大作业号恢复为 MaxUint64，仍然耗尽。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("含上限作业号的归档应正常重开：%v", err)
	}
	defer s2.Close()
	g, _ := s2.Get(maxJobID)
	if g.Status != StatusSucceeded || g.Archive == nil || g.Archive.Sum != 7 {
		t.Fatalf("取得最后编号的作业重开后应保留成功归档：%+v", g)
	}
	assertSubmitBlockedByExhaustion(t, s2, SubmitRequest{Submitter: "carol", RequestID: "y", Values: []int64{2}})
}

// TestLastJobIDPersistFailureKeepsIDReusable 最后一次提交的首份记录保存失败
// 时返回实际保存错误且不接受作业：最后编号不被消耗、请求号不登记；写入
// 恢复（含重开归档）后可以重新提交并取得同一个最后编号，正常计算归档，
// 之后才进入耗尽。
func TestLastJobIDPersistFailureKeepsIDReusable(t *testing.T) {
	dir := t.TempDir()
	writeNamedSyntheticRecord(t, dir, jobFileName(maxJobID-1),
		succeededRecordInFile(t, maxJobID-1, "alice", "prev", []int64{5}, restoredOccupiedBase))

	s, err := Open(dir, WithClock(newManualClock()))
	if err != nil {
		t.Fatal(err)
	}

	req := SubmitRequest{Submitter: "bob", RequestID: "last", Values: []int64{7}}

	// 注入故障：拟取得最后编号的作业首份（queued）记录无法保存。
	s.persistFault = func(j *storedJob) error {
		if j.id == maxJobID {
			return errors.New("numeric(测试故障): 模拟首份记录保存失败")
		}
		return nil
	}
	j, err := s.Submit(req)
	if err == nil || j != nil {
		t.Fatalf("最后编号提交保存失败时必须返回实际保存错误与空作业：job=%+v err=%v", j, err)
	}
	if errors.Is(err, ErrJobIDsExhausted) {
		t.Fatalf("保存失败必须返回实际保存错误，不能变成耗尽错误：%v", err)
	}

	// 未接受：最后编号仍可再取得，请求号未登记，没有正式记录或临时文件。
	if _, err := s.Get(maxJobID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("保存失败的提交不应可查询：%v", err)
	}
	s.mu.Lock()
	nextID := s.nextID
	_, claimed := s.idem[idemIdentity{"bob", "last"}]
	s.mu.Unlock()
	if nextID != maxJobID {
		t.Fatalf("保存失败不得消耗最后编号：nextID=%d want %d", nextID, maxJobID)
	}
	if claimed {
		t.Fatal("保存失败不得登记请求号")
	}
	if countJobFiles(t, dir) != 1 || countTmpFiles(t, dir) != 0 {
		t.Fatalf("保存失败不得留下记录：files=%d tmps=%d",
			countJobFiles(t, dir), countTmpFiles(t, dir))
	}

	// 写入恢复（关闭后重开，故障钩子不再存在）后重新提交，取得同一个最后编号。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, WithClock(newManualClock()))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	again := mustSubmit(t, s2, req)
	if again.ID != maxJobID {
		t.Fatalf("恢复后应重新取得同一个最后编号 %d，got %d", maxJobID, again.ID)
	}
	done := waitStatus(t, s2, maxJobID, StatusSucceeded)
	if done.Archive == nil || done.Archive.Sum != 7 || done.Archive.SumOfSquares != 49 {
		t.Fatalf("重新取得最后编号的作业应正常计算归档：%+v", done)
	}

	// 这次确认接受后，编号才真正耗尽。
	assertSubmitBlockedByExhaustion(t, s2, SubmitRequest{Submitter: "carol", RequestID: "z", Values: []int64{1}})
}

// TestAcceptedLastJobFailureOrCancelDoesNotReleaseID 已经接受的作业即使后来
// 失败或取消，也不释放它占掉的最后编号：后续新请求仍被耗尽拒绝，重开后亦然。
func TestAcceptedLastJobFailureOrCancelDoesNotReleaseID(t *testing.T) {
	t.Run("计算失败不释放编号", func(t *testing.T) {
		dir := t.TempDir()
		writeNamedSyntheticRecord(t, dir, jobFileName(maxJobID-1),
			succeededRecordInFile(t, maxJobID-1, "alice", "prev", []int64{5}, restoredOccupiedBase))
		s, err := Open(dir, WithClock(newManualClock()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })

		big := int64(3037000500)
		last := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "k", Values: []int64{big}})
		if last.ID != maxJobID {
			t.Fatalf("应取得最后编号 %d，got %d", maxJobID, last.ID)
		}
		failed := waitStatus(t, s, maxJobID, StatusFailed)
		if failed.Archive != nil || !strings.Contains(failed.FailureReason, "平方和") {
			t.Fatalf("作业应因溢出失败且无归档：%+v", failed)
		}
		// 失败终态不释放编号。
		assertSubmitBlockedByExhaustion(t, s, SubmitRequest{Submitter: "b", RequestID: "q", Values: []int64{1}})
	})

	t.Run("取消不释放编号", func(t *testing.T) {
		dir := t.TempDir()
		writeNamedSyntheticRecord(t, dir, jobFileName(maxJobID-1),
			succeededRecordInFile(t, maxJobID-1, "alice", "prev", []int64{5}, restoredOccupiedBase))
		s, err := Open(dir, WithClock(newManualClock()))
		if err != nil {
			t.Fatal(err)
		}
		started, block, release, _, _ := gateCompute(t, s)
		block(maxJobID)
		last := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "k", Values: []int64{1}})
		if last.ID != maxJobID {
			t.Fatalf("应取得最后编号 %d，got %d", maxJobID, last.ID)
		}
		waitStarted(t, started, maxJobID)
		canceled, err := s.Cancel(maxJobID)
		if err != nil || canceled.Status != StatusCanceled {
			t.Fatalf("运行中的作业应可取消：job=%+v err=%v", canceled, err)
		}
		release(maxJobID)

		// 取消终态不释放编号。
		assertSubmitBlockedByExhaustion(t, s, SubmitRequest{Submitter: "b", RequestID: "q", Values: []int64{1}})

		// 重开后：取消记录保留，编号仍耗尽。
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s2, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s2.Close()
		g, _ := s2.Get(maxJobID)
		if g.Status != StatusCanceled {
			t.Fatalf("取消记录应跨重开保留：%+v", g)
		}
		assertSubmitBlockedByExhaustion(t, s2, SubmitRequest{Submitter: "b", RequestID: "r", Values: []int64{2}})
	})
}
