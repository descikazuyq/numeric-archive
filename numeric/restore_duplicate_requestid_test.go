package numeric

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// queuedStoredJobWithRequest 构造一条带幂等请求号的排队记录，用于合成正常
// 提交不会产生的形态：不同作业号保存了同一提交人的同一非空请求号。
func queuedStoredJobWithRequest(id uint64, submitter, requestID string, values []int64, queuedAt time.Time, deps ...uint64) *storedJob {
	j := queuedStoredJob(id, submitter, values, queuedAt, deps...)
	j.requestID = requestID
	return j
}

// TestOpenRejectsDuplicateRequestID 两份可正常解析、版本与状态合法的记录以
// 不同作业号保存了同一提交人的同一非空请求号时，Open 必须失败、返回非空
// 错误且归档对象为 nil；错误须说明请求号重复归属于不同作业，并指出提交人、
// 请求号、两个冲突作业号及对应记录文件名。
func TestOpenRejectsDuplicateRequestID(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	name1 := jobFileName(3)
	name2 := jobFileName(5)
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(3, "alice", "req-1", []int64{1, 2}, base))
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(5, "alice", "req-1", []int64{3, 4}, base.Add(time.Second)))

	s, err := Open(dir)
	if err == nil {
		if s != nil {
			_ = s.Close()
		}
		t.Fatal("同一提交人的同一非空请求号归属两个作业号时必须打开失败")
	}
	if s != nil {
		t.Fatalf("冲突时归档对象必须为 nil，得到 %#v", s)
	}
	for _, want := range []string{"重复", "请求号", `"alice"`, `"req-1"`, "3", "5", name1, name2} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("错误必须包含 %s（提交人、请求号、两个作业号与两份记录文件名）: %v", want, err)
		}
	}
}

// TestOpenRejectsDuplicateRequestIDIdenticalContent 两份记录的整数序列、种子、
// 依赖与计算结果完全相同也不能合并成一次请求：请求号唯一性冲突仍然成立。
func TestOpenRejectsDuplicateRequestIDIdenticalContent(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 5, 2, 8, 0, 0, 0, time.UTC)
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(2, "alice", "req-same", []int64{1, 2, 3}, base))
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(4, "alice", "req-same", []int64{1, 2, 3}, base))

	s, err := Open(dir)
	if err == nil {
		if s != nil {
			_ = s.Close()
		}
		t.Fatal("内容完全相同的两份记录也不能合并，必须因请求号冲突失败")
	}
	if s != nil {
		t.Fatalf("冲突时归档对象必须为 nil: %#v", s)
	}
	if !strings.Contains(err.Error(), jobFileName(2)) || !strings.Contains(err.Error(), jobFileName(4)) {
		t.Fatalf("错误必须包含两份冲突记录的文件名: %v", err)
	}
}

// TestOpenRejectsDuplicateRequestIDTerminalRecords 冲突双方即使已经失败或
// 取消，请求号也不会被释放：终态记录同样占用请求号，打开必须失败。
func TestOpenRejectsDuplicateRequestIDTerminalRecords(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 5, 3, 8, 0, 0, 0, time.UTC)
	failed := queuedStoredJobWithRequest(1, "alice", "req-term", []int64{1}, base)
	failed.status = StatusFailed
	failed.finishedAt = base.Add(time.Second)
	failed.failureReason = "平方和超出 int64 范围"
	writeSyntheticRecord(t, dir, failed)
	canceled := queuedStoredJobWithRequest(2, "alice", "req-term", []int64{2}, base.Add(2*time.Second))
	canceled.status = StatusCanceled
	canceled.finishedAt = base.Add(3 * time.Second)
	writeSyntheticRecord(t, dir, canceled)

	s, err := Open(dir)
	if err == nil {
		if s != nil {
			_ = s.Close()
		}
		t.Fatal("已失败/已取消的记录同样占用请求号，冲突必须打开失败")
	}
	if s != nil {
		t.Fatalf("冲突时归档对象必须为 nil: %#v", s)
	}
}

// TestOpenDuplicateRequestIDDoesNotTouchAnyRecord 请求号冲突导致打开失败时，
// 本次打开不能启动任何计算，也不能按正常恢复规则改判或补写任何原有记录：
// 中断的运行记录、排队下游、成功归档（含完成时间与校验值）与冲突记录自身
// 的文件字节必须全部保持原样。
func TestOpenDuplicateRequestIDDoesNotTouchAnyRecord(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 5, 4, 8, 0, 0, 0, time.UTC)
	// 作业 1：上次中断的运行记录，正常恢复规则会改判失败并补完成时间。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", values: []int64{1, 2},
		queuedAt: base, startedAt: base.Add(time.Second), status: StatusRunning,
	})
	// 作业 2：排队等待作业 1，正常恢复会级联失败。
	writeSyntheticRecord(t, dir,
		queuedStoredJob(2, "a", []int64{3}, base.Add(2*time.Second), 1))
	// 作业 3：合法成功归档，正常情况下摘要与校验值原样保留。
	writeSyntheticRecord(t, dir,
		succeededStoredJob(t, 3, []int64{4}, base.Add(3*time.Second), base.Add(4*time.Second)))
	// 作业 4 与 5：不同作业号保存了同一提交人的同一非空请求号。
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(4, "alice", "req-dup", []int64{9}, base.Add(5*time.Second)))
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(5, "alice", "req-dup", []int64{10}, base.Add(6*time.Second)))

	before := snapshotDir(t, dir)
	s, err := Open(dir)
	if err == nil {
		if s != nil {
			_ = s.Close()
		}
		t.Fatal("存在重复请求号时必须打开失败")
	}
	if s != nil {
		t.Fatalf("冲突时归档对象必须为 nil: %#v", s)
	}
	assertDirUnchanged(t, dir, before)

	// 没有任何延迟补写：稍等后再次比对，所有记录仍是打开前的字节。
	time.Sleep(50 * time.Millisecond)
	assertDirUnchanged(t, dir, before)
}

// TestOpenEmptyRequestIDNotConflicting 空请求号不启用幂等：同一提交人可以
// 保存多条空请求号作业，不同提交人使用相同的非空请求号也正常打开。
func TestOpenEmptyRequestIDNotConflicting(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 5, 5, 8, 0, 0, 0, time.UTC)
	// 同一提交人的多条空请求号作业。
	writeSyntheticRecord(t, dir, queuedStoredJob(1, "alice", []int64{1}, base))
	writeSyntheticRecord(t, dir, queuedStoredJob(2, "alice", []int64{2}, base.Add(time.Second)))
	// 空提交人也是有效标识，其空请求号同样不启用幂等。
	writeSyntheticRecord(t, dir, queuedStoredJob(3, "", []int64{3}, base.Add(2*time.Second)))
	// 不同提交人使用相同的非空请求号。
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(4, "alice", "req-shared", []int64{4}, base.Add(3*time.Second)))
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(5, "bob", "req-shared", []int64{5}, base.Add(4*time.Second)))

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("空请求号与跨提交人的相同请求号不应构成冲突: %v", err)
	}
	defer s.Close()
	for _, id := range []uint64{1, 2, 3, 4, 5} {
		waitStatus(t, s, id, StatusSucceeded)
	}
}

// TestOpenEmptySubmitterDuplicateRequestID 空提交人是有效标识：两条空提交人
// 记录使用同一非空请求号时同样构成冲突。
func TestOpenEmptySubmitterDuplicateRequestID(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 5, 6, 8, 0, 0, 0, time.UTC)
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(1, "", "req-empty-sub", []int64{1}, base))
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(2, "", "req-empty-sub", []int64{2}, base.Add(time.Second)))

	s, err := Open(dir)
	if err == nil {
		if s != nil {
			_ = s.Close()
		}
		t.Fatal("空提交人的同一非空请求号归属两个作业号时必须打开失败")
	}
	if s != nil {
		t.Fatalf("冲突时归档对象必须为 nil: %#v", s)
	}
	if !strings.Contains(err.Error(), `"req-empty-sub"`) {
		t.Fatalf("错误必须包含冲突的请求号: %v", err)
	}
}

// TestOpenRequestIDConflictEscapesInvisibleBytes 错误中的标识必须能区分
// 不可见或非法字节：含 U+0000 与非法 UTF-8 字节的请求号以转义形式呈现，
// 不会显示成相同的替换字符而无法定位。
func TestOpenRequestIDConflictEscapesInvisibleBytes(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 5, 7, 8, 0, 0, 0, time.UTC)
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(1, "alice", "req\x00\xfff", []int64{1}, base))
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(2, "alice", "req\x00\xfff", []int64{2}, base.Add(time.Second)))

	s, err := Open(dir)
	if err == nil {
		if s != nil {
			_ = s.Close()
		}
		t.Fatal("含不可见字节的相同请求号归属两个作业号时必须打开失败")
	}
	if s != nil {
		t.Fatalf("冲突时归档对象必须为 nil: %#v", s)
	}
	// %q 引用下 U+0000 呈现为 \x00、非法字节 0xff 呈现为 \xff，而非替换字符。
	if !strings.Contains(err.Error(), `\x00`) || !strings.Contains(err.Error(), `\xff`) {
		t.Fatalf("错误中的请求号必须以转义形式呈现不可见与非法字节: %v", err)
	}
	if strings.Contains(err.Error(), "�") {
		t.Fatalf("错误中不应出现替换字符，否则不同非法字节无法区分: %v", err)
	}
}

// TestOpenDistinctInvalidByteIdentitiesNotConfused 按恢复后的完整字节比较：
// 不同的非法字节序列（落盘后显示成相同替换字符）是不同标识，含 U+0000 的
// 标识也不能被截短，都不构成冲突。
func TestOpenDistinctInvalidByteIdentitiesNotConfused(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 5, 8, 8, 0, 0, 0, time.UTC)
	// 请求号 0xff 与 0xfe 是不同标识，尽管都可能显示为 "�"。
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(1, "alice", "req\xff", []int64{1}, base))
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(2, "alice", "req\xfe", []int64{2}, base.Add(time.Second)))
	// 含 U+0000 的请求号不能被截短："req\x00x" 与 "req" 是不同标识。
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(3, "alice", "req\x00x", []int64{3}, base.Add(2*time.Second)))
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(4, "alice", "req", []int64{4}, base.Add(3*time.Second)))
	// 提交人一侧同样按完整字节比较："al\x00ice" 与 "al" 是不同提交人。
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(5, "al\x00ice", "req", []int64{5}, base.Add(4*time.Second)))
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(6, "al", "req", []int64{6}, base.Add(5*time.Second)))

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("完整字节不同的标识不应被误报为重复: %v", err)
	}
	defer s.Close()
	for _, id := range []uint64{1, 2, 3, 4, 5, 6} {
		waitStatus(t, s, id, StatusSucceeded)
	}
	// 重开后标识逐字节保真，幂等重放仍命中各自的原作业。
	replay, err := s.Submit(SubmitRequest{
		Submitter: "alice", RequestID: "req\xff", Values: []int64{1},
	})
	if err != nil {
		t.Fatalf("非法字节请求号的幂等重放必须命中原作业: %v", err)
	}
	if replay.ID != 1 {
		t.Fatalf("重放应返回作业 1，得到 %d", replay.ID)
	}
}

// TestOpenAfterRequestIDConflictResolvedRecoversNormally 调用方处理冲突
// （删除一份冲突记录）后再次打开，既有恢复规则照常生效，留下的记录正常
// 参与调度，幂等请求号继续生效。
func TestOpenAfterRequestIDConflictResolvedRecoversNormally(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 5, 9, 8, 0, 0, 0, time.UTC)
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(1, "alice", "req-fix", []int64{7}, base))
	writeSyntheticRecord(t, dir,
		queuedStoredJobWithRequest(2, "alice", "req-fix", []int64{8}, base.Add(time.Second)))

	if s, err := Open(dir); err == nil {
		_ = s.Close()
		t.Fatal("处理冲突前打开必须失败")
	}
	// 调用方自行删除一份冲突记录。
	if err := os.Remove(filepath.Join(dir, jobFileName(2))); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("冲突解决后再次打开应按既有规则恢复: %v", err)
	}
	defer s.Close()
	j1 := waitStatus(t, s, 1, StatusSucceeded)
	if j1.Archive == nil || j1.Archive.Sum != 7 {
		t.Fatalf("保留的作业 1 应正常计算完成: %+v", j1)
	}
	// 同人同号同内容提交仍返回原作业。
	replay, err := s.Submit(SubmitRequest{
		Submitter: "alice", RequestID: "req-fix", Values: []int64{7},
	})
	if err != nil {
		t.Fatalf("冲突解决后的幂等重放必须可用: %v", err)
	}
	if replay.ID != 1 {
		t.Fatalf("重放应返回原作业 1，得到 %d", replay.ID)
	}
}
