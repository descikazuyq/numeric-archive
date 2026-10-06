package numeric

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 本文件覆盖“以非默认名保存的合法记录”在重开归档后的更新行为：
// 读取规则接受任何 job-*.json 记录文件并以记录内容中的作业号识别作业，
// 因此状态更新（恢复改判、运行、完成、取消）都必须写回原来那份文件，
// 不能按默认命名另写一份造成同号副本。

// listJobFiles 返回目录中全部作业记录文件名（排除临时文件）。
func listJobFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".tmp-") {
			continue
		}
		if strings.HasPrefix(e.Name(), "job-") && strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	return names
}

// assertOnlyRecordFile 断言目录中唯一的作业记录就是 name（不存在按默认名
// 另写的同号副本），并返回该文件内容。
func assertOnlyRecordFile(t *testing.T, dir, name string) string {
	t.Helper()
	names := listJobFiles(t, dir)
	if len(names) != 1 || names[0] != name {
		t.Fatalf("目录中的记录文件=%v，应只有原文件 %s（不允许另写默认名副本）", names, name)
	}
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// 排队记录以带额外后缀的文件名保存时，恢复后继续计算，运行与成功状态都
// 写回原文件；重开不再出现重复作业号，成功归档完整可见。
func TestRestoredQueuedJobCustomFileNameUpdatesOriginalFile(t *testing.T) {
	dir := t.TempDir()
	const name = "job-7-saved.json"
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	j := queuedStoredJob(7, "a", []int64{1, 2, 3}, base)
	j.requestID = "req-7"
	j.seed = 5
	writeNamedSyntheticRecord(t, dir, name, j)

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("单份非默认名记录必须能打开: %v", err)
	}
	done := waitStatus(t, s, 7, StatusSucceeded)
	if done.Archive == nil || done.Archive.Sum != 6 || done.Archive.SumOfSquares != 14 {
		t.Fatalf("恢复后继续计算的结果不对: %+v", done.Archive)
	}
	wantChecksum := done.Archive.Checksum

	// 运行与成功状态都写回原文件，目录中没有第二份作业 7。
	content := assertOnlyRecordFile(t, dir, name)
	if !strings.Contains(content, `"status": "succeeded"`) || !strings.Contains(content, `"checksum"`) {
		t.Fatalf("原文件未更新为带完整归档的成功记录:\n%s", content)
	}
	if _, err := os.Stat(filepath.Join(dir, jobFileName(7))); !os.IsNotExist(err) {
		t.Fatalf("不允许按默认名另写同号记录: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 再次打开：不能因系统自己写出的副本报重复作业号；成功归档完整返回。
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("更新后重开必须成功: %v", err)
	}
	defer s2.Close()
	g, err := s2.Get(7)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusSucceeded || g.Archive == nil || g.Archive.Checksum != wantChecksum {
		t.Fatalf("重开后成功归档不完整或被改写: %+v", g)
	}
	// 幂等请求号继续生效：同人同号同内容返回原作业，不新建。
	replay, err := s2.Submit(SubmitRequest{Submitter: "a", RequestID: "req-7", Values: []int64{1, 2, 3}, Seed: 5})
	if err != nil || replay.ID != 7 {
		t.Fatalf("幂等重放: id=%d err=%v, want 7", replayID(replay), err)
	}
}

// 上次关闭时仍在运行的记录（非默认名）重开后改判失败，也只更新原文件。
func TestRestoredRunningJobCustomFileNameFailsInPlace(t *testing.T) {
	dir := t.TempDir()
	const name = "job-3-run.json"
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	j := queuedStoredJob(3, "a", []int64{1, 2}, base)
	j.status = StatusRunning
	j.startedAt = base.Add(time.Second)
	writeNamedSyntheticRecord(t, dir, name, j)

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	g, err := s.Get(3)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusFailed || !strings.Contains(g.FailureReason, "中断") {
		t.Fatalf("中断的运行记录应改判失败: status=%s reason=%q", g.Status, g.FailureReason)
	}
	content := assertOnlyRecordFile(t, dir, name)
	if !strings.Contains(content, `"status": "failed"`) {
		t.Fatalf("失败状态必须写回原文件:\n%s", content)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("改判后重开必须成功: %v", err)
	}
	defer s2.Close()
	g2, _ := s2.Get(3)
	if g2.Status != StatusFailed || g2.Archive != nil {
		t.Fatalf("重开后失败状态未保留: %+v", g2)
	}
}

// 取消恢复后仍在排队的作业：取消状态必须原子替换原记录才生效。
// 原记录保存失败时返回实际错误、保持取消前状态；写入恢复后重新取消成功，
// 且取消记录仍写回原文件，不转到默认名文件。
func TestCancelRestoredJobCustomFileName(t *testing.T) {
	dir := t.TempDir()
	const name = "job-7-saved.json"
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	// 作业 7 依赖仍在排队的作业 1：恢复后它保持排队（等待依赖结果），
	// 取消请求到达时不会被 worker 抢先完成。
	writeSyntheticRecord(t, dir, queuedStoredJob(1, "a", []int64{1}, base))
	j7 := queuedStoredJob(7, "a", []int64{4, 5}, base.Add(time.Second), 1)
	j7.requestID = "req-7"
	writeNamedSyntheticRecord(t, dir, name, j7)

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	assertJobFiles := func() {
		t.Helper()
		for _, n := range listJobFiles(t, dir) {
			if n != name && n != jobFileName(1) {
				t.Fatalf("不允许为作业 7 另写记录文件 %s", n)
			}
		}
	}

	// 注入“原记录无法替换”的保存失败（持锁安装，与 worker 的落盘同步）：
	// 取消必须返回实际保存错误，作业保持取消前状态，不级联下游，
	// 也不转到另一文件假装取消成功。
	injected := errors.New("injected: 原记录无法替换")
	s.mu.Lock()
	s.persistFault = func(j *storedJob) error {
		if j.id == 7 && j.status == StatusCanceled {
			return injected
		}
		return nil
	}
	s.mu.Unlock()
	if cj, err := s.Cancel(7); !errors.Is(err, injected) || cj != nil {
		t.Fatalf("保存失败的取消必须返回实际错误: view=%+v err=%v", cj, err)
	}
	g, _ := s.Get(7)
	if g.Status != StatusQueued || !g.FinishedAt.IsZero() || g.WaitReason != WaitDependency {
		t.Fatalf("取消保存失败后作业必须保持取消前状态: %+v", g)
	}
	if c, _ := s.Get(1); c.Status == StatusFailed || c.Status == StatusCanceled {
		t.Fatalf("取消保存失败不得影响上游: %+v", c)
	}
	assertJobFiles()
	if _, err := os.Stat(filepath.Join(dir, jobFileName(7))); !os.IsNotExist(err) {
		t.Fatalf("取消保存失败不得另写默认名记录: %v", err)
	}

	// 写入条件恢复：按现有取消规则重新请求，成功并写回原文件。
	s.mu.Lock()
	s.persistFault = nil
	s.mu.Unlock()
	cj, err := s.Cancel(7)
	if err != nil || cj.Status != StatusCanceled || cj.FinishedAt.IsZero() {
		t.Fatalf("重新取消: view=%+v err=%v", cj, err)
	}
	waitStatus(t, s, 7, StatusCanceled)
	waitStatus(t, s, 1, StatusSucceeded) // 上游照常完成，不受取消影响
	content, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), `"status": "canceled"`) {
		t.Fatalf("取消状态必须写回原文件:\n%s", content)
	}
	assertJobFiles()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重开：取消结果保留，不出现同号副本错误。
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("取消后重开必须成功: %v", err)
	}
	defer s2.Close()
	g2, _ := s2.Get(7)
	if g2.Status != StatusCanceled {
		t.Fatalf("重开后取消状态未保留: %s", g2.Status)
	}
}
