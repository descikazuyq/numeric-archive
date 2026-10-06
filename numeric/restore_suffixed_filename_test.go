package numeric

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 本文件覆盖“读取规则接受的合法记录使用带额外后缀的文件名”这一形态：
// 作业号由记录内容识别，单份记录的名称不同于默认命名并不等于存在两个作业。
// 恢复改写与后续状态更新（运行、完成、失败、取消）都必须写回原来那一份
// 正式文件，不能按作业号另写默认命名的记录而同号旧记录原样保留——否则
// 下一次打开会因重复作业号拒绝打开。

// suffixedRecordName 给出作业 id 的一份合法但非默认的记录文件名。
func suffixedRecordName(id uint64) string {
	return fmt.Sprintf("job-%d-saved.json", id)
}

// assertNoDefaultRecord 断言默认命名的记录文件不存在（没有制造同号副本）。
func assertNoDefaultRecord(t *testing.T, dir string, id uint64) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, jobFileName(id))); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("默认命名的同号记录不应出现（作业 %d）：stat err=%v", id, err)
	}
}

// 排队记录以非默认名称恢复后继续计算：进入运行、完成计算都写回原文件，
// 目录中始终只有那一份正式记录；关闭再开读取到更新后的成功状态与完整归档，
// 其他作业的记录不被移动或重写。
func TestRestoredQueuedJobWithSuffixedFileNameUpdatesSameFile(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, WithClock(newManualClock()))
	if err != nil {
		t.Fatal(err)
	}
	started, block, _, _, _ := gateCompute(t, s)
	block(1)
	head := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	waitStarted(t, started, head.ID)
	q := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "q", Values: []int64{5}, Seed: 2})
	if err := s.Close(); err != nil { // head 被标记为中断失败，q 仍是排队记录
		t.Fatal(err)
	}

	// 把 q 的正式记录换成带额外后缀的合法名称：读取规则接受它，
	// 作业号由记录内容识别，这不是第二份作业。
	custom := suffixedRecordName(q.ID)
	if err := os.Rename(filepath.Join(dir, jobFileName(q.ID)), filepath.Join(dir, custom)); err != nil {
		t.Fatal(err)
	}
	headBefore, err := os.ReadFile(filepath.Join(dir, jobFileName(head.ID)))
	if err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir) // 单份记录名称不标准，不等于存在两个作业，不能拒绝打开
	if err != nil {
		t.Fatalf("open with suffixed record name: %v", err)
	}
	done := waitStatus(t, s2, q.ID, StatusSucceeded)
	if done.Archive == nil || done.Archive.Sum != 5 {
		t.Fatalf("restored queued job must run to success: %+v", done)
	}

	// 运行与完成都写回原文件：目录中只有 custom 这一份作业 q 的记录。
	assertNoDefaultRecord(t, dir, q.ID)
	data, err := os.ReadFile(filepath.Join(dir, custom))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"status": "succeeded"`) {
		t.Fatalf("成功状态必须写回原记录文件：\n%s", data)
	}
	// 其他作业的记录不被这次更新移动或重写。
	headAfter, err := os.ReadFile(filepath.Join(dir, jobFileName(head.ID)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(headBefore, headAfter) {
		t.Fatal("其他作业的记录不应被重写")
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	// 再次打开：系统没有制造同号副本，打开成功并读到更新后的完整归档。
	s3, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after in-place update: %v", err)
	}
	defer s3.Close()
	g, err := s3.Get(q.ID)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusSucceeded || g.Archive == nil || g.Archive.Sum != 5 {
		t.Fatalf("更新后的成功状态与归档应在重开后可读：%+v", g)
	}
}

// 上次关闭时仍在运行的记录以非默认名称恢复：改判失败只更新原记录文件，
// 重开后读到失败状态，不返回成功归档或实际输入。
func TestRestoredRunningRecordWithSuffixedFileNameFailedInPlace(t *testing.T) {
	dir := t.TempDir()
	when := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	j := &storedJob{
		id: 7, submitter: "a", requestID: "r7", seed: 3,
		values: []int64{1, 2},
		queuedAt: when, startedAt: when,
		status: StatusRunning,
	}
	data, err := encodeRecord(j)
	if err != nil {
		t.Fatal(err)
	}
	custom := suffixedRecordName(j.id)
	if err := writeFileAtomic(dir, custom, data, 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	g, err := s.Get(j.id)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusFailed || !strings.Contains(g.FailureReason, "计算被中断") ||
		g.Archive != nil || len(g.EffectiveValues) != 0 {
		t.Fatalf("中断的运行记录应改判失败：%+v", g)
	}
	// 失败改判写回原文件，不产生默认命名的同号副本。
	out, err := os.ReadFile(filepath.Join(dir, custom))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"status": "failed"`) {
		t.Fatalf("失败改判必须写回原记录文件：\n%s", out)
	}
	assertNoDefaultRecord(t, dir, j.id)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 再次打开：读到更新后的失败状态，不返回成功归档或实际输入。
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after in-place failure update: %v", err)
	}
	defer s2.Close()
	g2, err := s2.Get(j.id)
	if err != nil {
		t.Fatal(err)
	}
	if g2.Status != StatusFailed || g2.Archive != nil || len(g2.EffectiveValues) != 0 {
		t.Fatalf("重开后应读到失败状态且无归档与实际输入：%+v", g2)
	}
}

// 取消已恢复的排队作业同样以原记录替换成功为生效条件：原文件无法替换时
// （即使默认命名对应的位置可写）返回实际保存错误，作业保持取消前状态，
// 不中止计算、不级联下游、不转到另一文件；写入条件恢复后重新取消生效，
// 取消记录仍写回原文件。
func TestCancelRestoredQueuedJobMustReplaceOriginalFile(t *testing.T) {
	dir := t.TempDir()
	when := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 一批各自合法的排队上游记录：本作业等待它们全部成功，worker 逐个处理
	// 期间本作业保持排队。每条记录都很小，worker 单次落盘的持锁时间极短，
	// 取消与状态查询不会被长时间挡在锁外；上游数量保证两次取消请求都
	// 发生在本作业变为可运行之前。
	const upstreams = 400
	depIDs := make([]uint64, 0, upstreams)
	for i := 1; i <= upstreams; i++ {
		u := &storedJob{
			id: uint64(i), submitter: "b",
			values: []int64{1}, queuedAt: when, status: StatusQueued,
		}
		data, err := encodeRecord(u)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, jobFileName(u.id)), data, 0o600); err != nil {
			t.Fatal(err)
		}
		depIDs = append(depIDs, u.id)
	}
	q := &storedJob{
		id: upstreams + 1, submitter: "a", requestID: "q", seed: 7,
		values: []int64{5}, dependencies: depIDs,
		queuedAt: when,
		status:   StatusQueued,
	}
	qData, err := encodeRecord(q)
	if err != nil {
		t.Fatal(err)
	}
	custom := suffixedRecordName(q.id)
	customPath := filepath.Join(dir, custom)
	if err := os.WriteFile(customPath, qData, 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() // 失败路径上也要停掉 worker，避免干扰临时目录清理
	// 等待本作业的下游：取消保存失败时不能仅因这次请求被级联标记。
	child := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{9},
		HasDependency: true, DependencyID: q.id,
	})

	// 原记录文件暂时无法被原子替换（目标路径被非空目录挡住，即便 root 运行
	// rename 也失败），但默认命名对应的位置可写。
	orig, err := os.ReadFile(customPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(customPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(customPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(customPath, "keep"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	cj, err := s.Cancel(q.id)
	if err == nil || cj != nil {
		t.Fatalf("原文件无法替换时取消不得成功：view=%+v err=%v", cj, err)
	}
	if !isRenameBlockedError(err) {
		t.Fatalf("应返回实际保存错误（rename 被非空目录挡住），got %v", err)
	}
	// 作业保持取消前状态：仍排队、无完成时间、参数原样。
	g, _ := s.Get(q.id)
	if g.Status != StatusQueued || !g.FinishedAt.IsZero() || g.WaitReason != WaitDependency ||
		g.RequestID != "q" || g.Seed != 7 || len(g.Values) != 1 || g.Values[0] != 5 {
		t.Fatalf("取消保存失败后作业必须保持原状：%+v", g)
	}
	// 不中止正在运行的上游计算，不级联等待中的下游。
	if u, _ := s.Get(1); u.Status == StatusCanceled || u.Status == StatusFailed {
		t.Fatalf("失败的取消不应中止正在运行的计算：%+v", u)
	}
	if c, _ := s.Get(child.ID); c.Status != StatusQueued || c.WaitReason != WaitDependency {
		t.Fatalf("失败的取消不应级联下游：%+v", c)
	}
	// 不转到默认命名的另一文件假装取消成功，本作业的临时记录也已清理
	// （目录中可能存在的其他 .tmp- 文件是 worker 落盘上游记录的瞬时产物）。
	assertNoDefaultRecord(t, dir, q.id)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-") && strings.Contains(e.Name(), custom) {
			t.Fatalf("取消保存失败不应留下本作业的临时记录：%s", e.Name())
		}
	}

	// 写入条件恢复：还原原记录文件后重新取消，按现有规则生效。
	if err := os.RemoveAll(customPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(customPath, orig, 0o600); err != nil {
		t.Fatal(err)
	}
	cj, err = s.Cancel(q.id)
	if err != nil || cj.Status != StatusCanceled || cj.FinishedAt.IsZero() {
		t.Fatalf("写入恢复后重新取消应生效：view=%+v err=%v", cj, err)
	}
	// 取消记录写回原文件，仍不产生默认命名的同号副本。
	out, err := os.ReadFile(customPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"status": "canceled"`) {
		t.Fatalf("取消状态必须写回原记录文件：\n%s", out)
	}
	assertNoDefaultRecord(t, dir, q.id)
	// 取消确认后，等待它的下游才按现有规则失败并指出阻断作业。
	fc := waitStatus(t, s, child.ID, StatusFailed)
	if fc.BlockerID != q.id {
		t.Fatalf("下游失败的根因应为被取消的作业 %d：%+v", q.id, fc)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 再次打开：已确认的取消与下游失败保留，且仍只有原文件一份记录。
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after in-place cancel: %v", err)
	}
	defer s2.Close()
	rq, _ := s2.Get(q.id)
	if rq.Status != StatusCanceled || rq.FinishedAt.IsZero() || rq.Archive != nil {
		t.Fatalf("重开后取消状态应保留：%+v", rq)
	}
	rc, _ := s2.Get(child.ID)
	if rc.Status != StatusFailed || rc.BlockerID != q.id {
		t.Fatalf("重开后下游失败应保留：%+v", rc)
	}
	assertNoDefaultRecord(t, dir, q.id)
}
