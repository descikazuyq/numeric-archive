package numeric

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// 本文件为“提交入口先保存成功、才登记作业与非空请求号”这一行为补充回归
// 保障。合法请求（非空整数序列、合法种子与依赖、上游已成功并有完整归档）
// 在首份记录无法保存时必须被当作未接受：返回实际保存错误而非作业视图，
// 不占用作业号与请求号、不留正式记录、不影响已有作业；写入恢复后同一
// 提交人+请求号仍可原样重试或改交另一份合法内容。幂等规则只在请求真正
// 被接受以后生效——已接受作业后来计算失败不构成重建作业的理由。

// countTmpFiles 统计目录中原子写残留的临时记录数（保存失败时 writeFileAtomic
// 必须自行清理，不得把临时文件留在目录中）。
func countTmpFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), ".tmp-") {
			n++
		}
	}
	return n
}

// blockRenameWithDir 在目标正式记录路径放置一个非空目录，使“临时记录已
// 写好、却无法 rename 为正式记录”真实发生：临时文件仍可在归档目录中创建
// 和写盘，但替换目标被非空目录挡住（即便以 root 运行 rename 仍失败）。
// 返回清理函数。
func blockRenameWithDir(t *testing.T, dir string, id uint64) func() {
	t.Helper()
	target := filepath.Join(dir, jobFileName(id))
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatalf("mkdir rename blocker: %v", err)
	}
	if err := os.WriteFile(filepath.Join(target, "keep"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	return func() { _ = os.RemoveAll(target) }
}

// isRenameBlockedError 判断错误是否为 rename 到非空目录时的实际保存错误。
// Linux 对“文件 rename 到已存在的非空目录”返回 EEXIST（空目录则为 EISDIR）。
func isRenameBlockedError(err error) bool {
	return errors.Is(err, fs.ErrExist) ||
		errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EISDIR)
}

// 上游：总和为 5 的成功归档；本文件各用例在它之后提交依赖它的新作业。
func submitSumFiveUpstream(t *testing.T, s *Store) *Job {
	t.Helper()
	up := mustSubmit(t, s, SubmitRequest{
		Submitter: "alice", RequestID: "up", Values: []int64{5},
	})
	got := waitStatus(t, s, up.ID, StatusSucceeded)
	if got.Archive == nil || got.Archive.Sum != 5 || got.Archive.SumOfSquares != 25 {
		t.Fatalf("upstream archive wrong: %+v", got)
	}
	if len(got.Archive.EffectiveValues) != 1 || got.Archive.EffectiveValues[0] != 5 {
		t.Fatalf("upstream effective=%v want [5]", got.Archive.EffectiveValues)
	}
	return got
}

// assertUpstreamUnchanged 断言已有上游作业的参数、状态与成功归档在一次失败
// 提交前后完全保持原样（含逐字节落盘记录），不会仅因下游提交保存失败而
// 变成失败或取消。
func assertUpstreamUnchanged(t *testing.T, dir string, s *Store, up *Job, diskBefore []byte) {
	t.Helper()
	g, err := s.Get(up.ID)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusSucceeded || g.BlockerID != 0 || g.Archive == nil {
		t.Fatalf("upstream disturbed: status=%s blocker=%d archive=%v",
			g.Status, g.BlockerID, g.Archive)
	}
	if g.Archive.Sum != 5 || g.Archive.SumOfSquares != 25 ||
		g.Archive.Checksum != up.Archive.Checksum ||
		len(g.Archive.EffectiveValues) != 1 || g.Archive.EffectiveValues[0] != 5 {
		t.Fatalf("upstream archive altered: %+v", g)
	}
	diskAfter, err := os.ReadFile(filepath.Join(dir, jobFileName(up.ID)))
	if err != nil {
		t.Fatal(err)
	}
	if string(diskAfter) != string(diskBefore) {
		t.Fatalf("upstream on-disk record changed despite rejected submit:\nbefore=%s\nafter=%s",
			diskBefore, diskAfter)
	}
}

// 目录暂时不可写、临时记录无法创建时，合法的带依赖提交必须返回实际保存
// 错误而非已接受作业；写入恢复后用同一提交人+请求号原样重试仍被接受，
// 并按现有规则把上游总和追加到最初指定的整数次序之后计算。
func TestSubmitTempRecordCannotCreateRejectsAndAllowsIdenticalRetry(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, WithClock(newManualClock()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	up := submitSumFiveUpstream(t, s)
	upDisk, err := os.ReadFile(filepath.Join(dir, jobFileName(up.ID)))
	if err != nil {
		t.Fatal(err)
	}

	req := SubmitRequest{
		Submitter: "alice", RequestID: "retry-k",
		Values: []int64{2, -3}, Seed: 7,
		Dependencies: []uint64{up.ID},
	}
	rejectedID := up.ID + 1 // 若被接受，它应取得的作业号

	// 目录不可写：临时记录无法创建。
	makeArchiveReadOnly(t, dir)
	j, err := s.Submit(req)
	if !errors.Is(err, fs.ErrPermission) || j != nil {
		t.Fatalf("submit while temp cannot be created: job=%+v err=%v, want permission error and nil job", j, err)
	}
	// 第一次未被接受：紧接着的原样重试同样必须返回实际保存错误，而不是命中
	// “已登记请求号”的幂等返回。
	if j2, err2 := s.Submit(req); !errors.Is(err2, fs.ErrPermission) || j2 != nil {
		t.Fatalf("repeat submit while unwritable: job=%+v err=%v, want permission error and nil job", j2, err2)
	}
	makeArchiveWritable(t, dir)

	// 未接受请求在任何视角都不可见：按提交人列举只有上游，按作业号查不到，
	// 目录里没有对应正式记录，也没有残留临时文件。
	list, err := s.List("alice", time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != up.ID {
		t.Fatalf("rejected submit visible via List: %v", ids(list))
	}
	if _, err := s.Get(rejectedID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected submit visible via Get: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, jobFileName(rejectedID))); !os.IsNotExist(err) {
		t.Fatalf("formal record of rejected submit exists: %v", err)
	}
	if countJobFiles(t, dir) != 1 || countTmpFiles(t, dir) != 0 {
		t.Fatalf("directory must contain only the upstream formal record, files=%d tmps=%d",
			countJobFiles(t, dir), countTmpFiles(t, dir))
	}
	// 内存中的幂等索引同样不能登记该请求号。
	s.mu.Lock()
	_, claimed := s.idem[idemIdentity{"alice", "retry-k"}]
	nextID := s.nextID
	s.mu.Unlock()
	if claimed {
		t.Fatal("request id registered despite save failure")
	}
	if nextID != rejectedID {
		t.Fatalf("nextID=%d, rejected submit must not consume job id %d", nextID, rejectedID)
	}
	assertUpstreamUnchanged(t, dir, s, up, upDisk)

	// 写入恢复后原样重试：同一提交人+请求号被当作全新请求接受，取得未被
	// 占用的下一个作业号，仍使用最初指定的整数次序、种子与依赖。
	again := mustSubmit(t, s, req)
	if again.ID != rejectedID {
		t.Fatalf("identical retry got id=%d, want unconsumed %d", again.ID, rejectedID)
	}
	if again.RequestID != "retry-k" || again.Seed != 7 ||
		len(again.Values) != 2 || again.Values[0] != 2 || again.Values[1] != -3 ||
		len(again.Dependencies) != 1 || again.Dependencies[0] != up.ID {
		t.Fatalf("identical retry must keep original params/deps/order: %+v", again)
	}
	done := waitStatus(t, s, again.ID, StatusSucceeded)
	// 实际输入 [2,-3] 后按现有规则追加上游总和 5：[2,-3,5]，总和 4、平方和 38。
	wantEffective := []int64{2, -3, 5}
	if len(done.Archive.EffectiveValues) != 3 {
		t.Fatalf("effective=%v want %v", done.Archive.EffectiveValues, wantEffective)
	}
	for i, v := range wantEffective {
		if done.Archive.EffectiveValues[i] != v {
			t.Fatalf("effective=%v want %v", done.Archive.EffectiveValues, wantEffective)
		}
	}
	if done.Archive.Sum != 4 || done.Archive.SumOfSquares != 38 {
		t.Fatalf("retry results sum=%d sq=%d, want 4,38",
			done.Archive.Sum, done.Archive.SumOfSquares)
	}
	// 上游结果保持不变。
	assertUpstreamUnchanged(t, dir, s, up, upDisk)
}

// 临时记录已写好却无法保存为正式记录（rename 被挡住）时，提交同样必须返回
// 实际保存错误。按题述序列：首次提交 [2,-3]、种子 7 并依赖总和为 5 的上游，
// 保存失败；写入条件恢复（含重开归档）后用同号改交 [4]、种子 9 且不带依赖，
// 必须按新内容接受——不报请求号冲突、不返回失败作业冒充已接受请求、不沿用
// 被拒绝请求的输入或依赖，最终归档总和 4、平方和 16，实际输入只有 [4]，
// 原上游结果保持不变。
func TestSubmitFormalRecordRenameFailsThenChangedContentAccepted(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	up := submitSumFiveUpstream(t, s)
	upDisk, err := os.ReadFile(filepath.Join(dir, jobFileName(up.ID)))
	if err != nil {
		t.Fatal(err)
	}
	rejectedID := up.ID + 1

	first := SubmitRequest{
		Submitter: "alice", RequestID: "change-k",
		Values: []int64{2, -3}, Seed: 7,
		Dependencies: []uint64{up.ID},
	}
	// 目标正式记录路径被非空目录挡住：临时文件可创建可写盘，但 rename 失败。
	unblock := blockRenameWithDir(t, dir, rejectedID)
	j, err := s.Submit(first)
	if !isRenameBlockedError(err) || j != nil {
		t.Fatalf("submit while formal record cannot be saved: job=%+v err=%v, want actual save error and nil job", j, err)
	}
	unblock()

	// 失败提交不留任何“已接受作业”痕迹。
	list, _ := s.List("alice", time.Time{}, time.Time{})
	if len(list) != 1 || list[0].ID != up.ID {
		t.Fatalf("rejected submit visible via List: %v", ids(list))
	}
	if _, err := s.Get(rejectedID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected submit visible via Get: %v", err)
	}
	if countJobFiles(t, dir) != 1 || countTmpFiles(t, dir) != 0 {
		t.Fatalf("directory must keep only upstream formal record, files=%d tmps=%d",
			countJobFiles(t, dir), countTmpFiles(t, dir))
	}
	s.mu.Lock()
	_, claimed := s.idem[idemIdentity{"alice", "change-k"}]
	nextID := s.nextID
	s.mu.Unlock()
	if claimed || nextID != rejectedID {
		t.Fatalf("rejected submit consumed registration: claimed=%v nextID=%d want %d",
			claimed, nextID, rejectedID)
	}
	assertUpstreamUnchanged(t, dir, s, up, upDisk)

	// “写入条件恢复”也包括关闭后重新打开同一目录：被拒绝请求没有留下任何
	// 会被恢复成已接受作业的记录，幂等号也未跨重开生效。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	if _, err := s2.Get(rejectedID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected submit resurfaced after reopen: %v", err)
	}
	assertUpstreamUnchanged(t, dir, s2, up, upDisk)

	// 同号改交另一份合法内容：作为新作业接受，不能报请求号冲突。
	changed := SubmitRequest{
		Submitter: "alice", RequestID: "change-k",
		Values: []int64{4}, Seed: 9,
	}
	got, err := s2.Submit(changed)
	if err != nil {
		t.Fatalf("changed-content retry after rejected submit: %v", err)
	}
	if got == nil || got.ID != rejectedID {
		t.Fatalf("changed-content retry job=%+v, want newly accepted id %d", got, rejectedID)
	}
	if got.Status == StatusFailed {
		t.Fatalf("changed-content retry must not return a failed job posing as accepted: %+v", got)
	}
	done := waitStatus(t, s2, got.ID, StatusSucceeded)
	// 必须按新内容接受：种子 9、无依赖、实际输入只有 [4]，绝不沿用被拒绝
	// 请求的 [2,-3] 或对上游总和 5 的依赖。
	if done.Seed != 9 || done.RequestID != "change-k" {
		t.Fatalf("accepted retry params wrong: %+v", done)
	}
	if len(done.Dependencies) != 0 || done.HasDependency || done.DependencyID != 0 {
		t.Fatalf("accepted retry must carry no dependency from rejected request: %+v", done)
	}
	if len(done.Values) != 1 || done.Values[0] != 4 {
		t.Fatalf("accepted retry original values=%v want [4]", done.Values)
	}
	if len(done.Archive.EffectiveValues) != 1 || done.Archive.EffectiveValues[0] != 4 {
		t.Fatalf("accepted retry effective=%v want [4] (must not append upstream sum 5)",
			done.Archive.EffectiveValues)
	}
	if done.Archive.Sum != 4 || done.Archive.SumOfSquares != 16 {
		t.Fatalf("accepted retry results sum=%d sq=%d, want 4,16",
			done.Archive.Sum, done.Archive.SumOfSquares)
	}
	assertUpstreamUnchanged(t, dir, s2, up, upDisk)

	// 同号请求真正被接受以后，现有幂等规则才生效：
	// 相同内容再次提交返回该作业当前详情；改变内容返回冲突并保留原记录。
	replay, err := s2.Submit(changed)
	if err != nil || replay == nil || replay.ID != got.ID || replay.Status != StatusSucceeded {
		t.Fatalf("idempotent replay after acceptance: job=%+v err=%v", replay, err)
	}
	conflictJob, err := s2.Submit(SubmitRequest{
		Submitter: "alice", RequestID: "change-k",
		Values: []int64{2, -3}, Seed: 7,
		Dependencies: []uint64{up.ID},
	})
	if !errors.Is(err, ErrIdempotencyConflict) || conflictJob == nil || conflictJob.ID != got.ID {
		t.Fatalf("changed content after acceptance must conflict: job=%+v err=%v", conflictJob, err)
	}
	kept := waitStatus(t, s2, got.ID, StatusSucceeded)
	if kept.Archive.Sum != 4 || kept.Archive.SumOfSquares != 16 || kept.Seed != 9 ||
		len(kept.Dependencies) != 0 || len(kept.Archive.EffectiveValues) != 1 {
		t.Fatalf("accepted record must be preserved by conflict: %+v", kept)
	}
	assertUpstreamUnchanged(t, dir, s2, up, upDisk)
}

// 已接受作业后来计算失败与“首份记录保存失败、请求未被接受”是两回事：
// 终态为失败的作业仍保留自己的作业号与请求号，相同内容重放返回它本身，
// 改变内容按冲突处理并保留失败记录——不能把失败终态当作允许重新创建作业
// 的理由（重开归档后同样如此）。
func TestAcceptedThenFailedJobKeepsIDAndRequestIDAcrossReplay(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	big := int64(3037000500)
	req := SubmitRequest{Submitter: "a", RequestID: "k", Values: []int64{big}}
	j := mustSubmit(t, s, req)
	failed := waitStatus(t, s, j.ID, StatusFailed)
	if !strings.Contains(failed.FailureReason, "平方和") || failed.Archive != nil {
		t.Fatalf("accepted job should fail by overflow with no archive: %+v", failed)
	}
	if failed.ID != j.ID || failed.RequestID != "k" {
		t.Fatalf("failed terminal job must keep own id and request id: %+v", failed)
	}

	// 相同内容再次提交：返回该失败作业本身，无错误，不重建作业、不重新排队。
	replay, err := s.Submit(req)
	if err != nil || replay == nil || replay.ID != j.ID || replay.Status != StatusFailed {
		t.Fatalf("replay of accepted-then-failed job must return the same job: %+v err=%v", replay, err)
	}
	// 改变内容：幂等冲突，同时返回原失败作业，原因保持不变。
	cj, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "k", Values: []int64{1, 2}, Seed: 0,
	})
	if !errors.Is(err, ErrIdempotencyConflict) || cj == nil || cj.ID != j.ID {
		t.Fatalf("changed content for failed accepted job must conflict: %+v err=%v", cj, err)
	}
	if cj.Status != StatusFailed || cj.FailureReason != failed.FailureReason || cj.Archive != nil {
		t.Fatalf("failed record altered by conflict attempt: %+v", cj)
	}
	if countJobFiles(t, dir) != 1 {
		t.Fatalf("replay/conflict must not create records: %d files", countJobFiles(t, dir))
	}

	// 重开归档后仍保留自己的作业号与请求号，重放依旧指向它，不允许重建。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	g, _ := s2.Get(j.ID)
	if g.Status != StatusFailed || g.RequestID != "k" || g.FailureReason != failed.FailureReason {
		t.Fatalf("failed accepted job after reopen: %+v", g)
	}
	r2, err := s2.Submit(req)
	if err != nil || r2 == nil || r2.ID != j.ID || r2.Status != StatusFailed {
		t.Fatalf("replay after reopen must keep the failed job, no recreation: %+v err=%v", r2, err)
	}
}
