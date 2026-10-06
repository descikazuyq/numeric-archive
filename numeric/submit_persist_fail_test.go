package numeric

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// errInjectedSubmitSave 是测试注入的“首条记录保存失败”错误，模拟临时记录
// 已写好却无法保存为正式记录（原子替换失败）的情形。
var errInjectedSubmitSave = errors.New("numeric(测试故障): 模拟首条记录保存失败")

// failSubmitRecord 注入只让指定提交人+请求号的“首条排队记录”落盘失败的故障，
// 其余作业与其他状态的写入照常成功。返回清除函数。
func failSubmitRecord(s *Store, submitter, requestID string) (clear func()) {
	s.persistFault = func(j *storedJob) error {
		if j.submitter == submitter && j.requestID == requestID && j.status == StatusQueued {
			return errInjectedSubmitSave
		}
		return nil
	}
	return func() { s.persistFault = nil }
}

// assertSubmitRejectedClean 断言一次保存失败的提交没有留下任何被接受的痕迹：
// 返回错误与空作业、按提交人列举只有既有作业、按作业号查不到新记录、
// 目录中也没有对应的正式记录或残留临时文件。
func assertSubmitRejectedClean(t *testing.T, s *Store, dir string, submitter string,
	existing int, nextID uint64) {
	t.Helper()
	list, err := s.List(submitter, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != existing {
		t.Fatalf("list after rejected submit: %d jobs, want %d", len(list), existing)
	}
	if _, err := s.Get(nextID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected submit left queryable job %d: %v", nextID, err)
	}
	if _, err := os.Stat(filepath.Join(dir, jobFileName(nextID))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected submit left formal record for job %d: %v", nextID, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			t.Fatalf("rejected submit left temp record %s", e.Name())
		}
	}
}

// 首条记录保存失败（临时记录已写好却无法保存为正式记录）时，提交必须返回实际
// 保存错误而不是一份已接受的作业：不登记作业号、不占用请求号、目录中不留正式
// 记录，已有上游的参数、状态与成功归档保持原样。写入恢复后同一提交人+请求号
// 可以原样重试，按最初指定的整数次序、种子与依赖接受并计算；此后现有幂等规则
// 才生效（同内容重放返回原作业，改内容返回冲突并保留原记录）。
func TestSubmitPersistFailureNotAcceptedThenExactRetry(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, WithClock(newManualClock()))
	if err != nil {
		t.Fatal(err)
	}

	up := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "up", Values: []int64{2, 3}, Seed: 1,
	})
	upDone := waitStatus(t, s, up.ID, StatusSucceeded) // 总和 5，归档完整
	if upDone.Archive == nil || upDone.Archive.Sum != 5 {
		t.Fatalf("upstream archive: %+v", upDone.Archive)
	}
	upChecksum := upDone.Archive.Checksum

	req := SubmitRequest{
		Submitter: "a", RequestID: "r1", Values: []int64{2, -3}, Seed: 7,
		Dependencies: []uint64{up.ID},
	}
	clear := failSubmitRecord(s, "a", "r1")
	j, err := s.Submit(req)
	clear()
	if !errors.Is(err, errInjectedSubmitSave) || j != nil {
		t.Fatalf("submit with failing save: view=%+v err=%v, want the actual save error", j, err)
	}
	// 未保存成功的提交不能被当成已接受：紧接着的同号提交同样走保存路径，
	// 不能命中任何“已接受请求”的幂等判定。
	clear = failSubmitRecord(s, "a", "r1")
	_, err = s.Submit(req)
	clear()
	if !errors.Is(err, errInjectedSubmitSave) {
		t.Fatalf("repeat submit while save fails: err=%v, want the actual save error", err)
	}

	assertSubmitRejectedClean(t, s, dir, "a", 1, 2)
	// 已有上游不能仅因这次提交失败而变成失败或取消，参数与归档保持原样。
	u, _ := s.Get(up.ID)
	if u.Status != StatusSucceeded || u.Archive == nil ||
		u.Archive.Sum != 5 || u.Archive.Checksum != upChecksum ||
		u.RequestID != "up" || u.Seed != 1 || len(u.Values) != 2 {
		t.Fatalf("upstream disturbed by rejected submit: %+v", u)
	}

	// 关闭重开：被拒绝的提交在磁盘上同样无迹可寻，幂等号未被占用。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	assertSubmitRejectedClean(t, s2, dir, "a", 1, 2)

	// 写入条件恢复后原样重试：按最初指定的整数次序、种子与依赖接受，
	// 追加上游总和后按现有规则计算。
	retry := mustSubmit(t, s2, req)
	if retry.ID != 2 || retry.Status != StatusQueued {
		t.Fatalf("exact retry: id=%d status=%s, want newly accepted job 2", retry.ID, retry.Status)
	}
	done := waitStatus(t, s2, retry.ID, StatusSucceeded)
	// 实际输入 = [2,-3] + 上游总和 5。
	wantEff := []int64{2, -3, 5}
	if len(done.EffectiveValues) != 3 {
		t.Fatalf("effective=%v, want %v", done.EffectiveValues, wantEff)
	}
	for i, v := range wantEff {
		if done.EffectiveValues[i] != v {
			t.Fatalf("effective=%v, want %v", done.EffectiveValues, wantEff)
		}
	}
	if done.Archive.Sum != 4 || done.Archive.SumOfSquares != 38 {
		t.Fatalf("sum=%d sq=%d, want 4,38", done.Archive.Sum, done.Archive.SumOfSquares)
	}
	if done.Seed != 7 || len(done.Dependencies) != 1 || done.Dependencies[0] != up.ID {
		t.Fatalf("retry must keep original seed and dependency: %+v", done)
	}

	// 同号请求真正被接受以后，现有幂等规则才生效。
	again := mustSubmit(t, s2, req)
	if again.ID != retry.ID || again.Status != StatusSucceeded {
		t.Fatalf("replay after acceptance: id=%d status=%s", again.ID, again.Status)
	}
	conflictReq := req
	conflictReq.Seed = 8
	cj, err := s2.Submit(conflictReq)
	if !errors.Is(err, ErrIdempotencyConflict) || cj == nil || cj.ID != retry.ID {
		t.Fatalf("changed content after acceptance: view=%+v err=%v, want conflict", cj, err)
	}
	if got, _ := s2.List("a", time.Time{}, time.Time{}); len(got) != 2 {
		t.Fatalf("conflict created extra records: %d", len(got))
	}
	// 上游结果全程保持不变。
	u2, _ := s2.Get(up.ID)
	if u2.Status != StatusSucceeded || u2.Archive == nil || u2.Archive.Checksum != upChecksum {
		t.Fatalf("upstream changed across retry: %+v", u2)
	}
}

// 保存失败的提交不占用请求号：写入恢复后同一提交人+请求号可以改交另一份合法
// 内容——不能报幂等冲突，不能返回一份失败作业冒充已接受请求，也不能沿用被
// 拒绝请求的输入或依赖。第一次提交 [2,-3]、种子 7 并依赖总和为 5 的上游，
// 保存失败后同号改交 [4]、种子 9 且不带依赖，应按新内容接受，最终归档总和
// 为 4、平方和为 16，实际输入只有 [4]，原来的上游结果保持不变。
func TestSubmitPersistFailureAllowsChangedContentResubmit(t *testing.T) {
	s, _ := openTestStore(t)

	up := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "up", Values: []int64{2, 3}, Seed: 1,
	})
	upDone := waitStatus(t, s, up.ID, StatusSucceeded) // 总和 5
	upChecksum := upDone.Archive.Checksum

	clear := failSubmitRecord(s, "a", "r1")
	j, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "r1", Values: []int64{2, -3}, Seed: 7,
		Dependencies: []uint64{up.ID},
	})
	clear()
	if !errors.Is(err, errInjectedSubmitSave) || j != nil {
		t.Fatalf("submit with failing save: view=%+v err=%v", j, err)
	}
	assertSubmitRejectedClean(t, s, s.Dir(), "a", 1, 2)

	// 同号改交另一份合法内容：前一次没有被接受，不能报请求号冲突。
	changed := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "r1", Values: []int64{4}, Seed: 9,
	})
	if changed.ID != 2 || changed.Status == StatusFailed {
		t.Fatalf("changed resubmit must be genuinely accepted: %+v", changed)
	}
	done := waitStatus(t, s, changed.ID, StatusSucceeded)
	if done.Archive == nil || done.Archive.Sum != 4 || done.Archive.SumOfSquares != 16 {
		t.Fatalf("archive=%+v, want sum 4 sq 16", done.Archive)
	}
	// 不能沿用被拒绝请求的输入或依赖：实际输入只有 [4]。
	if got := done.EffectiveValues; len(got) != 1 || got[0] != 4 {
		t.Fatalf("effective=%v, want [4] only", got)
	}
	if len(done.Dependencies) != 0 || done.HasDependency ||
		done.Seed != 9 || len(done.Values) != 1 || done.Values[0] != 4 {
		t.Fatalf("rejected request's params leaked into accepted job: %+v", done)
	}
	// 原来的上游结果保持不变。
	u, _ := s.Get(up.ID)
	if u.Status != StatusSucceeded || u.Archive == nil ||
		u.Archive.Sum != 5 || u.Archive.Checksum != upChecksum {
		t.Fatalf("upstream changed by resubmit: %+v", u)
	}

	// 接受之后幂等规则生效：同内容重放返回该作业当前详情，改内容冲突并保留它。
	replay := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "r1", Values: []int64{4}, Seed: 9,
	})
	if replay.ID != changed.ID || replay.Status != StatusSucceeded {
		t.Fatalf("replay: id=%d status=%s", replay.ID, replay.Status)
	}
	cj, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "r1", Values: []int64{2, -3}, Seed: 7,
		Dependencies: []uint64{up.ID},
	})
	if !errors.Is(err, ErrIdempotencyConflict) || cj == nil || cj.ID != changed.ID {
		t.Fatalf("original rejected content must now conflict: view=%+v err=%v", cj, err)
	}
	if got, _ := s.List("a", time.Time{}, time.Time{}); len(got) != 2 {
		t.Fatalf("conflict created extra records: %d", len(got))
	}
}

// 目录中的临时记录无法创建（目录不可写）时，提交同样必须返回实际保存错误，
// 不产生任何被接受的痕迹；写入恢复后同一提交人+请求号可原样重试并接受。
func TestSubmitPersistFailureReadOnlyDir(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, WithClock(newManualClock()))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	up := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "up", Values: []int64{2, 3}, Seed: 1,
	})
	upDone := waitStatus(t, s, up.ID, StatusSucceeded) // 总和 5
	upChecksum := upDone.Archive.Checksum

	req := SubmitRequest{
		Submitter: "a", RequestID: "r1", Values: []int64{2, -3}, Seed: 7,
		HasDependency: true, DependencyID: up.ID,
	}
	makeArchiveReadOnly(t, dir)
	j, err := s.Submit(req)
	if !errors.Is(err, fs.ErrPermission) || j != nil {
		t.Fatalf("submit with unwritable dir: view=%+v err=%v, want the actual save error", j, err)
	}
	// 首次未保存的提交不能被当成已接受：紧接着的重复请求同样失败。
	if j, err := s.Submit(req); !errors.Is(err, fs.ErrPermission) || j != nil {
		t.Fatalf("repeat submit while unwritable: view=%+v err=%v", j, err)
	}
	assertSubmitRejectedClean(t, s, dir, "a", 1, 2)
	u, _ := s.Get(up.ID)
	if u.Status != StatusSucceeded || u.Archive == nil || u.Archive.Checksum != upChecksum {
		t.Fatalf("upstream disturbed by rejected submit: %+v", u)
	}

	// 写入恢复：同一提交人+请求号原样重试，按现有规则追加上游总和后计算。
	makeArchiveWritable(t, dir)
	retry := mustSubmit(t, s, req)
	if retry.ID != 2 {
		t.Fatalf("retry id=%d, want 2", retry.ID)
	}
	done := waitStatus(t, s, retry.ID, StatusSucceeded)
	if got := done.EffectiveValues; len(got) != 3 || got[0] != 2 || got[1] != -3 || got[2] != 5 {
		t.Fatalf("effective=%v, want [2 -3 5]", got)
	}
	if done.Archive.Sum != 4 || done.Archive.SumOfSquares != 38 {
		t.Fatalf("sum=%d sq=%d, want 4,38", done.Archive.Sum, done.Archive.SumOfSquares)
	}
}

// 与“保存失败未被接受”不同：已接受作业后来计算失败仍保留自己的作业号和
// 请求号，终态为失败不是允许重新创建作业的理由——同号同内容重放返回该失败
// 作业的当前详情，改内容返回冲突并保留原记录。
func TestFailedTerminalJobKeepsRequestID(t *testing.T) {
	s, _ := openTestStore(t)
	big := int64(3037000500)
	req := SubmitRequest{Submitter: "a", RequestID: "rf", Values: []int64{big}, Seed: 3}
	j := mustSubmit(t, s, req)
	failed := waitStatus(t, s, j.ID, StatusFailed)
	if failed.Archive != nil {
		t.Fatalf("overflow job must fail without archive: %+v", failed)
	}

	// 同号同内容重放：返回该失败作业本身，不重新创建、不重新排队。
	replay := mustSubmit(t, s, req)
	if replay.ID != j.ID || replay.Status != StatusFailed ||
		replay.FailureReason != failed.FailureReason {
		t.Fatalf("replay of failed job: %+v, want the same failed job %d", replay, j.ID)
	}
	// 同号改内容：冲突并保留原失败记录。
	cj, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "rf", Values: []int64{1}, Seed: 3,
	})
	if !errors.Is(err, ErrIdempotencyConflict) || cj == nil || cj.ID != j.ID ||
		cj.Status != StatusFailed {
		t.Fatalf("changed content on failed job: view=%+v err=%v, want conflict", cj, err)
	}
	if got, _ := s.List("a", time.Time{}, time.Time{}); len(got) != 1 {
		t.Fatalf("failed job's request id must not allow recreation: %d records", len(got))
	}
	if _, err := s.Get(2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed terminal job must not free its request id: %v", err)
	}
	// 原失败记录保持原样。
	after, _ := s.Get(j.ID)
	if after.Status != StatusFailed || after.FailureReason != failed.FailureReason ||
		after.RequestID != "rf" || after.Seed != 3 {
		t.Fatalf("failed record altered: %+v", after)
	}
}
