package numeric

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// makeArchiveReadOnly 把归档目录临时变为目录本身不可写（无法创建临时文件、
// 无法 rename 替换记录），并在测试结束时自动恢复，避免污染 t.TempDir 的清理。
// 若当前权限模型下只读目录仍可写（例如以 root 运行），故障注入无法成立，
// 跳过测试而不是误报失败。
func makeArchiveReadOnly(t *testing.T, dir string) {
	t.Helper()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod 0500: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	probe := filepath.Join(dir, ".tmp-write-probe")
	if err := os.WriteFile(probe, []byte("x"), 0o600); err == nil {
		_ = os.Remove(probe)
		_ = os.Chmod(dir, 0o700)
		t.Skip("只读目录在当前环境下仍可写，无法注入保存失败（通常为 root 运行）")
	}
}

func makeArchiveWritable(t *testing.T, dir string) {
	t.Helper()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("chmod 0700: %v", err)
	}
}

// 排队作业取消时无法保存：返回实际保存错误，作业在内存与磁盘上都保持排队，
// 不新增完成时间、不清空信息、不级联下游；写入恢复后重新取消才生效，
// 重新打开归档仍保留这次已确认的取消，且作业号与提交参数不变。
func TestCancelQueuedPersistFailureKeepsStateAndRetries(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, WithClock(newManualClock()))
	if err != nil {
		t.Fatal(err)
	}
	started, block, release, _, _ := gateCompute(t, s)
	block(1)

	head := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	waitStarted(t, started, head.ID)
	q := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "cq", Values: []int64{5}, Seed: 7,
	})
	child := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{9},
		HasDependency: true, DependencyID: q.ID,
	})

	onDisk := func() string {
		data, err := os.ReadFile(filepath.Join(dir, jobFileName(q.ID)))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	if !strings.Contains(onDisk(), `"status": "queued"`) {
		t.Fatal("queued job not persisted as queued before cancel")
	}

	// 目录暂时不可写：取消必须返回底层错误，不能返回任何成功视图。
	makeArchiveReadOnly(t, dir)
	if cj, err := s.Cancel(q.ID); err == nil {
		t.Fatalf("cancel with unwritable dir returned success: %+v", cj)
	} else if !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("cancel err=%v, want the actual save error (permission denied)", err)
	}
	// 首次未保存的取消不能被当成已完成操作：紧接着的重复请求同样失败。
	if _, err := s.Cancel(q.ID); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("repeat cancel while unwritable err=%v, want permission error", err)
	}

	// 内存可查询状态保持原状态：仍排队、无完成时间、保留参数、等待计算位置。
	g, _ := s.Get(q.ID)
	if g.Status != StatusQueued || !g.FinishedAt.IsZero() || g.WaitReason != WaitSlot {
		t.Fatalf("job changed after failed cancel: status=%s finished=%s reason=%q",
			g.Status, g.FinishedAt, g.WaitReason)
	}
	if g.ID != q.ID || g.Submitter != "a" || g.RequestID != "cq" ||
		g.Seed != 7 || len(g.Values) != 1 || g.Values[0] != 5 {
		t.Fatalf("submit params altered after failed cancel: %+v", g)
	}
	// 运行中的作业未被提前中止，等待依赖的下游也没有仅因这次请求被标记失败。
	if h, _ := s.Get(head.ID); h.Status != StatusRunning || !h.FinishedAt.IsZero() {
		t.Fatalf("running job disturbed by failed cancel: %+v", h)
	}
	c, _ := s.Get(child.ID)
	if c.Status != StatusQueued || c.WaitReason != WaitDependency || !c.FinishedAt.IsZero() {
		t.Fatalf("downstream disturbed by failed cancel: %+v", c)
	}
	// 磁盘上仍是原来的排队记录。
	if !strings.Contains(onDisk(), `"status": "queued"`) ||
		strings.Contains(onDisk(), `"status": "canceled"`) {
		t.Fatalf("disk record changed despite failed cancel:\n%s", onDisk())
	}

	// 写入恢复：对仍排队的同一作业重新取消，成功后才返回取消状态。
	makeArchiveWritable(t, dir)
	cj, err := s.Cancel(q.ID)
	if err != nil || cj.Status != StatusCanceled || cj.FinishedAt.IsZero() {
		t.Fatalf("retry cancel: view=%+v err=%v", cj, err)
	}
	if cj.ID != q.ID || cj.RequestID != "cq" || cj.Seed != 7 ||
		len(cj.Values) != 1 || cj.Values[0] != 5 {
		t.Fatalf("confirmed cancel must keep job id and submit params: %+v", cj)
	}
	// 已成功保存的取消允许重复请求返回成功。
	if again, err := s.Cancel(q.ID); err != nil || again.Status != StatusCanceled {
		t.Fatalf("repeat confirmed cancel: %+v %v", again, err)
	}
	// 直到取消确认后，依赖它的下游才按现有规则失败并指出阻断作业。
	fc := waitStatus(t, s, child.ID, StatusFailed)
	if fc.BlockerID != q.ID || !strings.Contains(fc.FailureReason, "取消") {
		t.Fatalf("downstream failure after confirmed cancel: %+v", fc)
	}

	release(head.ID)
	waitStatus(t, s, head.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开同一归档：已确认的取消与下游失败都保留。
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	rq, _ := s2.Get(q.ID)
	if rq.Status != StatusCanceled || rq.FinishedAt.IsZero() || rq.Archive != nil {
		t.Fatalf("canceled job after reopen: %+v", rq)
	}
	if rq.ID != q.ID || rq.RequestID != "cq" || rq.Seed != 7 ||
		len(rq.Values) != 1 || rq.Values[0] != 5 {
		t.Fatalf("job params changed across reopen: %+v", rq)
	}
	rc, _ := s2.Get(child.ID)
	if rc.Status != StatusFailed || rc.BlockerID != q.ID {
		t.Fatalf("downstream after reopen: %+v", rc)
	}
}

// 运行中的作业取消保存失败时，不中止计算：计算随后成功，第二次取消返回
// 已有的不能取消错误并保留成功结果；溢出场景下计算仍正常失败，同样不可再取消。
func TestCancelRunningPersistFailureLetsComputeFinish(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)
	run := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	waitStarted(t, started, run.ID)

	makeArchiveReadOnly(t, s.Dir())
	cj, err := s.Cancel(run.ID)
	if !errors.Is(err, fs.ErrPermission) || cj != nil {
		t.Fatalf("cancel running while unwritable: view=%+v err=%v", cj, err)
	}
	g, _ := s.Get(run.ID)
	if g.Status != StatusRunning || g.StartedAt.IsZero() || !g.FinishedAt.IsZero() {
		t.Fatalf("running job changed after failed cancel: %+v", g)
	}
	data, err := os.ReadFile(filepath.Join(s.Dir(), jobFileName(run.ID)))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"status": "running"`) {
		t.Fatalf("disk record must stay running after failed cancel:\n%s", data)
	}

	// 计算在两次取消请求之间成功完成。
	makeArchiveWritable(t, s.Dir())
	release(run.ID)
	done := waitStatus(t, s, run.ID, StatusSucceeded)
	if done.Archive == nil || done.Archive.Sum != 1 {
		t.Fatalf("compute must still be allowed to succeed: %+v", done)
	}
	if _, err := s.Cancel(run.ID); !errors.Is(err, ErrNotCancellable) {
		t.Fatalf("cancel after success: %v, want ErrNotCancellable", err)
	}
	again, _ := s.Get(run.ID)
	if again.Status != StatusSucceeded || again.Archive == nil ||
		again.Archive.Checksum != done.Archive.Checksum {
		t.Fatalf("succeeded result must be preserved, got %+v", again)
	}

	// 同样的保存失败不应妨碍计算正常失败（输入溢出）。
	block(2)
	big := int64(3037000500)
	bad := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{big}})
	waitStarted(t, started, bad.ID)
	makeArchiveReadOnly(t, s.Dir())
	if _, err := s.Cancel(bad.ID); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("cancel overflow job while unwritable: %v", err)
	}
	makeArchiveWritable(t, s.Dir())
	release(bad.ID)
	failed := waitStatus(t, s, bad.ID, StatusFailed)
	if !strings.Contains(failed.FailureReason, "平方和") || failed.Archive != nil {
		t.Fatalf("overflow job must still fail normally: %+v", failed)
	}
	if _, err := s.Cancel(bad.ID); !errors.Is(err, ErrNotCancellable) {
		t.Fatalf("cancel failed job: %v, want ErrNotCancellable", err)
	}
	if g, _ := s.Get(bad.ID); g.Status != StatusFailed ||
		g.FailureReason != failed.FailureReason || g.Archive != nil {
		t.Fatalf("failed result changed after rejected cancel: %+v", g)
	}
}

// 排队作业取消保存失败本身不改变调度：恢复写入但不重新取消、放行前面的作业后，
// 该作业仍按原有排队规则正常计算并成功。
func TestFailedQueuedCancelDoesNotChangeScheduling(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1)
	head := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}})
	waitStarted(t, started, head.ID)
	q := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{5}})

	makeArchiveReadOnly(t, s.Dir())
	if _, err := s.Cancel(q.ID); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("cancel: %v", err)
	}
	makeArchiveWritable(t, s.Dir())
	if g, _ := s.Get(q.ID); g.Status != StatusQueued {
		t.Fatalf("job must remain queued after failed cancel: %s", g.Status)
	}

	release(head.ID)
	done := waitStatus(t, s, q.ID, StatusSucceeded)
	if done.Archive.Sum != 5 {
		t.Fatalf("queued job must still run normally, sum=%d", done.Archive.Sum)
	}
}

// 取消成功保存后，正在运行的计算即使随后“成功完成”也不能留下成功归档；
// 关闭重开归档后，已确认的取消仍然成立。
func TestConfirmedRunningCancelDiscardsLateSuccess(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// 自定义钩子：放行后无视取消信号、照常返回成功结果，模拟计算在取消确认
	// 之后才结束的竞态。
	started := make(chan uint64, 1)
	gate := make(chan struct{})
	s.compute = func(_ uint64, _ []int64, _ int64, _ func() bool) (int64, int64, string, bool) {
		started <- 1
		<-gate
		return 3, 5, "", true
	}
	run := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1, 2}})
	waitStarted(t, started, run.ID)

	cj, err := s.Cancel(run.ID)
	if err != nil || cj.Status != StatusCanceled {
		t.Fatalf("cancel running: %+v %v", cj, err)
	}
	if g, _ := s.Get(run.ID); g.Status != StatusCanceled || g.FinishedAt.IsZero() {
		t.Fatalf("query right after confirmed cancel: %+v", g)
	}
	close(gate) // 计算随即以“成功”返回；其产物必须被丢弃。

	// Close 会等待 worker 走完计算后的裁决分支，返回即说明晚到的成功结果
	// 已被处理，磁盘上必须仍是取消记录且不含归档。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, jobFileName(run.ID)))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"status": "canceled"`) ||
		strings.Contains(string(data), `"archive"`) {
		t.Fatalf("late compute success must not be archived:\n%s", data)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	g, _ := s2.Get(run.ID)
	if g.Status != StatusCanceled || g.Archive != nil {
		t.Fatalf("confirmed running cancel after reopen: %+v", g)
	}
}
