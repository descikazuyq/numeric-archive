package numeric

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// requestJob 在内存中构造一条带提交人/请求号的合成记录，可指定任意状态；
// 成功状态会按给定参数生成全套自洽归档，失败/取消状态带上完成时间与原因，
// 用于直接写盘构造正常提交不会产生的形态。
func requestJob(t *testing.T, id uint64, submitter, requestID string, status Status, base time.Time) *storedJob {
	t.Helper()
	j := &storedJob{
		id:        id,
		submitter: submitter,
		requestID: requestID,
		values:    []int64{1, 2},
		queuedAt:  base.Add(time.Duration(id) * time.Second),
		status:    status,
	}
	switch status {
	case StatusSucceeded:
		sum, sumSq, _, ok := computeResult(j.values, 0, nil)
		if !ok {
			t.Fatalf("test setup: values %v overflow", j.values)
		}
		j.effectiveValues = append([]int64(nil), j.values...)
		j.finishedAt = base.Add(time.Duration(id+1) * time.Second)
		j.archive = newArchive(j, j.values, sum, sumSq, j.finishedAt)
	case StatusFailed:
		j.finishedAt = base.Add(time.Duration(id+1) * time.Second)
		j.failureReason = "既有的失败原因"
	case StatusCanceled:
		j.finishedAt = base.Add(time.Duration(id+1) * time.Second)
	}
	return j
}

// assertRequestIDConflict 校验请求号重复冲突错误的统一形态：非空、归档为 nil
// 由调用方另行断言；错误必须说明请求号重复归属于不同作业，指出提交人、
// 请求号、两个冲突作业号与两份记录文件名。
func assertRequestIDConflict(t *testing.T, err error, submitter, requestID string,
	idA uint64, nameA string, idB uint64, nameB string) {
	t.Helper()
	if err == nil {
		t.Fatal("Open 必须在请求号重复归属不同作业时失败")
	}
	msg := err.Error()
	if !strings.Contains(msg, "重复") || !strings.Contains(msg, "请求号") {
		t.Fatalf("错误必须说明请求号重复归属不同作业: %v", err)
	}
	// 标识在错误中按 %q 呈现：含 U+0000 与非法 UTF-8 字节时以 \x00、\xff
	// 这类字节转义给出，因此这里同样按 %q 形态比对。
	if !strings.Contains(msg, fmt.Sprintf("%q", submitter)) {
		t.Fatalf("错误必须指出提交人 %q: %v", submitter, err)
	}
	if !strings.Contains(msg, fmt.Sprintf("%q", requestID)) {
		t.Fatalf("错误必须指出请求号 %q: %v", requestID, err)
	}
	for _, want := range []string{itoa(idA), itoa(idB), nameA, nameB} {
		if !strings.Contains(msg, want) {
			t.Fatalf("错误必须包含冲突作业号/文件名 %q: %v", want, err)
		}
	}
}

// TestOpenRejectsDuplicateRequestIDDifferentJobs 两份可正常解析的记录保存了
// 不同作业号、相同提交人与同一个非空请求号时，Open 必须失败、归档对象为
// nil；错误说明请求号重复归属不同作业，指出提交人、请求号、两个冲突作业号
// 与各自记录文件名。
func TestOpenRejectsDuplicateRequestIDDifferentJobs(t *testing.T) {
	base := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)

	cases := []struct {
		name    string
		nameB   string // 作业 5 的文件名（可排在作业 3 标准名之前）
		statusA Status
		statusB Status
	}{
		{
			name:    "标准文件名_两条排队",
			nameB:   jobFileName(5),
			statusA: StatusQueued,
			statusB: StatusQueued,
		},
		{
			name:    "排队与成功",
			nameB:   jobFileName(5),
			statusA: StatusQueued,
			statusB: StatusSucceeded,
		},
		{
			name:    "冲突文件名排在标准文件名之前",
			nameB:   "job--0000000000000000005-copy.json",
			statusA: StatusQueued,
			statusB: StatusQueued,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeSyntheticRecord(t, dir,
				requestJob(t, 3, "alice", "req-7", tc.statusA, base))
			writeNamedSyntheticRecord(t, dir, tc.nameB,
				requestJob(t, 5, "alice", "req-7", tc.statusB, base.Add(time.Hour)))

			s, err := Open(dir)
			if s != nil {
				_ = s.Close()
				t.Fatalf("请求号冲突时归档对象必须为 nil，得到 %#v", s)
			}
			assertRequestIDConflict(t, err, "alice", "req-7", 3, jobFileName(3), 5, tc.nameB)
		})
	}
}

// TestOpenDuplicateRequestIDIdenticalContentNotMerged 即使两份记录的整数序列、
// 种子、依赖与计算结果完全相同，也不能合并成一次请求：不同作业号仍是冲突。
func TestOpenDuplicateRequestIDIdenticalContentNotMerged(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 5, 2, 8, 0, 0, 0, time.UTC)
	// 除作业号外字段完全一致的两条成功记录（结果摘要、校验值也相同）。
	j3 := requestJob(t, 3, "alice", "same", StatusSucceeded, base)
	j5 := requestJob(t, 5, "alice", "same", StatusSucceeded, base)
	writeSyntheticRecord(t, dir, j3)
	writeSyntheticRecord(t, dir, j5)

	s, err := Open(dir)
	if s != nil {
		_ = s.Close()
		t.Fatalf("内容完全相同也不能合并，归档对象必须为 nil: %#v", s)
	}
	assertRequestIDConflict(t, err, "alice", "same",
		3, jobFileName(3), 5, jobFileName(5))
}

// TestOpenDuplicateRequestIDTerminalStatusesDoNotRelease 一份记录已经失败或
// 取消也不释放请求号：任意终态组合下同一提交人的同一非空请求号跨作业出现
// 都必须拒绝打开。
func TestOpenDuplicateRequestIDTerminalStatusesDoNotRelease(t *testing.T) {
	base := time.Date(2026, 5, 3, 8, 0, 0, 0, time.UTC)
	statuses := []Status{StatusQueued, StatusRunning, StatusSucceeded, StatusFailed, StatusCanceled}
	for _, sa := range statuses {
		for _, sb := range statuses {
			t.Run(string(sa)+"+"+string(sb), func(t *testing.T) {
				dir := t.TempDir()
				writeSyntheticRecord(t, dir, requestJob(t, 2, "u", "held", sa, base))
				writeSyntheticRecord(t, dir, requestJob(t, 4, "u", "held", sb, base.Add(time.Hour)))
				s, err := Open(dir)
				if s != nil {
					_ = s.Close()
					t.Fatalf("状态组合 %s/%s 也必须拒绝打开", sa, sb)
				}
				assertRequestIDConflict(t, err, "u", "held",
					2, jobFileName(2), 4, jobFileName(4))
			})
		}
	}
}

// TestOpenDuplicateRequestIDDoesNotTouchAnyRecord 请求号冲突导致的失败与重复
// 作业号失败遵守同一项纪律：不启动计算，也不改写任何原有记录——本来会被
// 标记为中断失败的运行记录、需要复核的成功归档、排队与已失败记录全部保持
// 原状态、完成时间与校验值；不自动换号、删除或修补冲突记录。
func TestOpenDuplicateRequestIDDoesNotTouchAnyRecord(t *testing.T) {
	base := time.Date(2026, 5, 4, 8, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	// 作业 1：上次中断的运行记录，正常恢复会改判失败并补完成时间。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", values: []int64{1, 2},
		queuedAt: base, startedAt: base.Add(time.Second), status: StatusRunning,
	})
	// 作业 2：排队等待作业 1，正常恢复会级联失败。
	writeSyntheticRecord(t, dir,
		queuedStoredJob(2, "a", []int64{3}, base.Add(2*time.Second), 1))
	// 作业 3：合法成功归档，摘要与校验值应原样保留。
	writeSyntheticRecord(t, dir,
		succeededStoredJob(t, 3, []int64{4}, base.Add(3*time.Second), base.Add(4*time.Second)))
	// 作业 4：已失败终态记录，正常恢复不改动。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 4, submitter: "a", values: []int64{5},
		queuedAt: base.Add(5 * time.Second), finishedAt: base.Add(6 * time.Second),
		status: StatusFailed, failureReason: "平方和超出 int64 范围",
	})
	// 作业 5 与作业 8：不同作业号保存了相同提交人与非空请求号。
	writeSyntheticRecord(t, dir,
		requestJob(t, 5, "a", "clash", StatusQueued, base))
	writeSyntheticRecord(t, dir,
		requestJob(t, 8, "a", "clash", StatusSucceeded, base.Add(time.Hour)))

	before := snapshotDir(t, dir)
	s, err := Open(dir)
	if s != nil {
		_ = s.Close()
		t.Fatalf("冲突时归档对象必须为 nil: %#v", s)
	}
	assertRequestIDConflict(t, err, "a", "clash",
		5, jobFileName(5), 8, jobFileName(8))
	assertDirUnchanged(t, dir, before)

	// 没有任何延迟补写或延迟计算：稍等后所有记录字节仍与打开前一致。
	time.Sleep(50 * time.Millisecond)
	assertDirUnchanged(t, dir, before)
}

// TestOpenEmptyRequestIDExemptFromConflict 空请求号不启用幂等：同一提交人可以
// 保存多条空请求号作业，重开后照常恢复，新的空请求号提交也继续创建新作业。
func TestOpenEmptyRequestIDExemptFromConflict(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 5, 5, 8, 0, 0, 0, time.UTC)
	writeSyntheticRecord(t, dir, requestJob(t, 1, "alice", "", StatusQueued, base))
	writeSyntheticRecord(t, dir, requestJob(t, 2, "alice", "", StatusQueued, base))
	writeSyntheticRecord(t, dir, requestJob(t, 3, "", "", StatusQueued, base)) // 空提交人同样有效

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("空请求号记录不应被判冲突: %v", err)
	}
	defer s.Close()
	for _, id := range []uint64{1, 2, 3} {
		waitStatus(t, s, id, StatusSucceeded)
	}
	next, err := s.Submit(SubmitRequest{Submitter: "alice", RequestID: "", Values: []int64{1, 2}})
	if err != nil {
		t.Fatalf("空请求号新提交应被接受: %v", err)
	}
	if next.ID <= 3 {
		t.Fatalf("空请求号不占用幂等号，新作业应续号分配，得到 %d", next.ID)
	}
	waitStatus(t, s, next.ID, StatusSucceeded)
}

// TestOpenDifferentSubmittersSameRequestIDAllowed 不同提交人使用相同请求号是
// 各自独立的幂等作用域，必须正常打开并重放命中各自的原作业。
func TestOpenDifferentSubmittersSameRequestIDAllowed(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 5, 6, 8, 0, 0, 0, time.UTC)
	writeSyntheticRecord(t, dir, requestJob(t, 1, "alice", "shared", StatusQueued, base))
	writeSyntheticRecord(t, dir, requestJob(t, 2, "bob", "shared", StatusQueued, base))

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("不同提交人同请求号不应被拒绝: %v", err)
	}
	defer s.Close()
	waitStatus(t, s, 1, StatusSucceeded)
	waitStatus(t, s, 2, StatusSucceeded)

	for _, c := range []struct {
		submitter string
		wantID    uint64
	}{{"alice", 1}, {"bob", 2}} {
		j, err := s.Submit(SubmitRequest{
			Submitter: c.submitter, RequestID: "shared", Values: []int64{1, 2},
		})
		if err != nil || j.ID != c.wantID {
			t.Fatalf("提交人 %s 重放应命中作业 %d，得到 id=%d err=%v",
				c.submitter, c.wantID, replayID(j), err)
		}
	}
}

// TestOpenEmptySubmitterRequestIDConflict 空提交人依旧是有效标识：空提交人
// 的同一非空请求号跨作业出现同样必须拒绝，错误中要能看出提交人为空。
func TestOpenEmptySubmitterRequestIDConflict(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 5, 7, 8, 0, 0, 0, time.UTC)
	writeSyntheticRecord(t, dir, requestJob(t, 1, "", "r-x", StatusQueued, base))
	writeSyntheticRecord(t, dir, requestJob(t, 2, "", "r-x", StatusQueued, base))

	s, err := Open(dir)
	if s != nil {
		_ = s.Close()
		t.Fatalf("空提交人的请求号冲突也必须拒绝打开: %#v", s)
	}
	if err == nil || !strings.Contains(err.Error(), `提交人 ""`) {
		t.Fatalf("错误须明确指出提交人为空: %v", err)
	}
	assertRequestIDConflict(t, err, "", "r-x", 1, jobFileName(1), 2, jobFileName(2))
}

// TestOpenNULIdentityRequestIDConflict 含 U+0000 的标识按完整字节比较：
//   - 同一完整（提交人, 请求号）跨作业出现必须拒绝，错误中的标识不能在
//     U+0000 处被截短，须以转义与十六进制给出完整字节；
//   - ("a","b\x00c") 与 ("a\x00b","c") 是两个不同键，绝不能误报重复。
func TestOpenNULIdentityRequestIDConflict(t *testing.T) {
	base := time.Date(2026, 5, 8, 8, 0, 0, 0, time.UTC)

	t.Run("完整字节相同的含零请求号冲突", func(t *testing.T) {
		dir := t.TempDir()
		writeSyntheticRecord(t, dir, requestJob(t, 1, "a", "b\x00c", StatusQueued, base))
		writeSyntheticRecord(t, dir, requestJob(t, 3, "a", "b\x00c", StatusQueued, base))

		s, err := Open(dir)
		if s != nil {
			_ = s.Close()
			t.Fatalf("含零请求号冲突必须拒绝打开: %#v", s)
		}
		msg := err.Error()
		if !strings.Contains(msg, `\x00`) {
			t.Fatalf("错误中的请求号不能在 U+0000 处被截短，须含转义 \\x00: %v", err)
		}
		if !strings.Contains(msg, "620063") { // "b\x00c" 的完整字节
			t.Fatalf("错误须给出请求号完整字节十六进制 620063: %v", err)
		}
		assertRequestIDConflict(t, err, "a", "b\x00c", 1, jobFileName(1), 3, jobFileName(3))
	})

	t.Run("零字符位置不同的两对标识不冲突", func(t *testing.T) {
		dir := t.TempDir()
		writeSyntheticRecord(t, dir, requestJob(t, 1, "a", "b\x00c", StatusQueued, base))
		writeSyntheticRecord(t, dir, requestJob(t, 2, "a\x00b", "c", StatusQueued, base))

		s, err := Open(dir)
		if err != nil {
			t.Fatalf("零字符摆在分隔符两侧的两对标识不能误报重复: %v", err)
		}
		defer s.Close()
		r1, err := s.Submit(SubmitRequest{Submitter: "a", RequestID: "b\x00c", Values: []int64{1, 2}})
		if err != nil || r1.ID != 1 {
			t.Fatalf("键1重放应命中作业1: id=%d err=%v", replayID(r1), err)
		}
		r2, err := s.Submit(SubmitRequest{Submitter: "a\x00b", RequestID: "c", Values: []int64{1, 2}})
		if err != nil || r2.ID != 2 {
			t.Fatalf("键2重放应命中作业2: id=%d err=%v", replayID(r2), err)
		}
	})
}

// TestOpenInvalidUTF8RequestIDConflictIsByteBased 非法 UTF-8 字节沿用保存格式
// 的恢复含义按完整字节比较：显示成相同替换字符的不同非法字节不能误报重复；
// 完整字节相同的非法请求号跨作业出现必须拒绝，错误以字节转义与十六进制
// 区分，避免冲突信息看起来相同却无法定位。
func TestOpenInvalidUTF8RequestIDConflictIsByteBased(t *testing.T) {
	base := time.Date(2026, 5, 9, 8, 0, 0, 0, time.UTC)

	t.Run("不同非法字节不误报重复", func(t *testing.T) {
		dir := t.TempDir()
		writeSyntheticRecord(t, dir, requestJob(t, 1, "alice", "r\xff", StatusQueued, base))
		writeSyntheticRecord(t, dir, requestJob(t, 2, "alice", "r\xfe", StatusQueued, base))

		s, err := Open(dir)
		if err != nil {
			t.Fatalf("0xFF 与 0xFE 是不同请求号，不应误报重复: %v", err)
		}
		defer s.Close()
		waitStatus(t, s, 1, StatusSucceeded)
		waitStatus(t, s, 2, StatusSucceeded)
		ff, err := s.Submit(SubmitRequest{Submitter: "alice", RequestID: "r\xff", Values: []int64{1, 2}})
		if err != nil || ff.ID != 1 {
			t.Fatalf("0xFF 重放应命中作业1: id=%d err=%v", replayID(ff), err)
		}
		fe, err := s.Submit(SubmitRequest{Submitter: "alice", RequestID: "r\xfe", Values: []int64{1, 2}})
		if err != nil || fe.ID != 2 {
			t.Fatalf("0xFE 重放应命中作业2: id=%d err=%v", replayID(fe), err)
		}
	})

	t.Run("相同非法字节跨作业冲突且错误可定位", func(t *testing.T) {
		dir := t.TempDir()
		writeSyntheticRecord(t, dir, requestJob(t, 1, "alice", "r\xff", StatusQueued, base))
		writeSyntheticRecord(t, dir, requestJob(t, 2, "alice", "r\xfe", StatusQueued, base))
		writeSyntheticRecord(t, dir, requestJob(t, 3, "alice", "r\xff", StatusQueued, base))

		s, err := Open(dir)
		if s != nil {
			_ = s.Close()
			t.Fatalf("相同非法字节请求号跨作业必须拒绝: %#v", s)
		}
		msg := err.Error()
		if !strings.Contains(msg, `\xff`) {
			t.Fatalf("错误须以字节转义给出非法请求号: %v", err)
		}
		if !strings.Contains(msg, "72ff") { // "r\xff"
			t.Fatalf("错误须给出请求号完整字节十六进制 72ff: %v", err)
		}
		if strings.Contains(msg, `\xfe`) || strings.Contains(msg, "72fe") {
			t.Fatalf("冲突信息不能混入无关的 0xFE 请求号: %v", err)
		}
		assertRequestIDConflict(t, err, "alice", "r\xff", 1, jobFileName(1), 3, jobFileName(3))

		// 调用方处理冲突（删掉重复的一份）后再次打开，既有恢复规则照常生效，
		// 0xFF 请求号指向保留的作业 1。
		if err := os.Remove(filepath.Join(dir, jobFileName(3))); err != nil {
			t.Fatal(err)
		}
		s2, err := Open(dir)
		if err != nil {
			t.Fatalf("冲突解决后应正常打开: %v", err)
		}
		defer s2.Close()
		waitStatus(t, s2, 1, StatusSucceeded)
		waitStatus(t, s2, 2, StatusSucceeded)
		ff, err := s2.Submit(SubmitRequest{Submitter: "alice", RequestID: "r\xff", Values: []int64{1, 2}})
		if err != nil || ff.ID != 1 {
			t.Fatalf("冲突解决后 0xFF 应命中保留的作业1: id=%d err=%v", replayID(ff), err)
		}
	})

	t.Run("非法字节提交人按字节区分作用域", func(t *testing.T) {
		// 不同非法字节的提交人各自独立：即使请求号相同也不误报。
		dir := t.TempDir()
		writeSyntheticRecord(t, dir, requestJob(t, 4, "s\xff", "shared", StatusQueued, base))
		writeSyntheticRecord(t, dir, requestJob(t, 5, "s\xfe", "shared", StatusQueued, base))
		s, err := Open(dir)
		if err != nil {
			t.Fatalf("不同非法字节提交人不应共用作用域: %v", err)
		}
		_ = s.Close()

		// 再换成两份提交人完整字节相同的记录，即构成冲突，错误中的提交人也
		// 须以字节转义与十六进制区分。
		dir2 := t.TempDir()
		writeSyntheticRecord(t, dir2, requestJob(t, 4, "s\xff", "shared", StatusQueued, base))
		writeSyntheticRecord(t, dir2, requestJob(t, 6, "s\xff", "shared", StatusQueued, base))
		s2, err := Open(dir2)
		if s2 != nil {
			_ = s2.Close()
			t.Fatalf("相同非法字节提交人跨作业必须拒绝: %#v", s2)
		}
		msg := err.Error()
		if !strings.Contains(msg, `\xff`) || !strings.Contains(msg, "73ff") {
			t.Fatalf("错误中的提交人须以字节转义与十六进制给出: %v", err)
		}
		assertRequestIDConflict(t, err, "s\xff", "shared", 4, jobFileName(4), 6, jobFileName(6))
	})
}

// TestOpenAfterRequestIDConflictResolvedRecoversNormally 调用方删除一份冲突
// 记录后再次打开，既有恢复规则照常生效：中断运行改判失败、排队下游级联，
// 保留的冲突记录正常参与调度并继续持有该请求号。
func TestOpenAfterRequestIDConflictResolvedRecoversNormally(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 5, 10, 8, 0, 0, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", values: []int64{1, 2},
		queuedAt: base, startedAt: base.Add(time.Second), status: StatusRunning,
	})
	writeSyntheticRecord(t, dir,
		queuedStoredJob(2, "a", []int64{3}, base.Add(2*time.Second), 1))
	writeSyntheticRecord(t, dir, requestJob(t, 5, "a", "clash", StatusQueued, base))
	writeSyntheticRecord(t, dir, requestJob(t, 8, "a", "clash", StatusQueued, base))

	if s, err := Open(dir); err == nil {
		_ = s.Close()
		t.Fatal("处理冲突前打开必须失败")
	}
	if err := os.Remove(filepath.Join(dir, jobFileName(8))); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("冲突解决后再次打开应按既有规则恢复: %v", err)
	}
	defer s.Close()

	j1, _ := s.Get(1)
	if j1.Status != StatusFailed || !strings.Contains(j1.FailureReason, "中断") {
		t.Fatalf("作业 1 应按既有规则改判中断失败: %s %q", j1.Status, j1.FailureReason)
	}
	j2 := waitStatus(t, s, 2, StatusFailed)
	if j2.BlockerID != 1 {
		t.Fatalf("作业 2 应沿链条保留根因 1，得到 %d", j2.BlockerID)
	}
	j5 := waitStatus(t, s, 5, StatusSucceeded)
	if j5.RequestID != "clash" {
		t.Fatalf("保留的作业 5 应继续持有请求号，得到 %q", j5.RequestID)
	}
	replay, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "clash", Values: []int64{1, 2},
	})
	if err != nil || replay.ID != 5 {
		t.Fatalf("冲突解决后请求号应指向保留的作业 5，得到 id=%d err=%v", replayID(replay), err)
	}
}

// TestOpenRequestIDConflictDoesNotMaskOrBeMaskedByPreexistingErrors 原有的
// 记录解析失败与重复作业号错误继续有效：
//   - 损坏记录按目录读取顺序先被解析，保留原有的“记录损坏”错误；
//   - 重复作业号与请求号冲突并存时，先做的重复作业号检查先报错；
//   - 请求号冲突本身不允许改写目录内任何记录。
func TestOpenRequestIDConflictDoesNotMaskOrBeMaskedByPreexistingErrors(t *testing.T) {
	base := time.Date(2026, 5, 11, 8, 0, 0, 0, time.UTC)

	t.Run("损坏记录先被解析时先报损坏", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, jobFileName(1)), []byte("{not json"), 0o600); err != nil {
			t.Fatal(err)
		}
		writeSyntheticRecord(t, dir, requestJob(t, 3, "a", "clash", StatusQueued, base))
		writeSyntheticRecord(t, dir, requestJob(t, 5, "a", "clash", StatusQueued, base))
		before := snapshotDir(t, dir)
		s, err := Open(dir)
		if s != nil {
			_ = s.Close()
			t.Fatalf("必须打开失败: %#v", s)
		}
		if !strings.Contains(err.Error(), "损坏") {
			t.Fatalf("应保留原有的记录损坏错误，而非请求号冲突: %v", err)
		}
		assertDirUnchanged(t, dir, before)
	})

	t.Run("重复作业号与请求号冲突并存时先报重复作业号", func(t *testing.T) {
		dir := t.TempDir()
		// 请求号冲突对：作业 3、5。
		writeSyntheticRecord(t, dir, requestJob(t, 3, "a", "clash", StatusQueued, base))
		writeSyntheticRecord(t, dir, requestJob(t, 5, "a", "clash", StatusQueued, base))
		// 重复作业号对：作业 7 两份文件。
		writeSyntheticRecord(t, dir, queuedStoredJob(7, "b", []int64{1}, base))
		writeNamedSyntheticRecord(t, dir, "job-00000000000000000007-dup.json",
			queuedStoredJob(7, "c", []int64{2}, base.Add(time.Second)))
		s, err := Open(dir)
		if s != nil {
			_ = s.Close()
			t.Fatalf("必须打开失败: %#v", s)
		}
		if !strings.Contains(err.Error(), "重复作业号") {
			t.Fatalf("重复作业号检查先行，应先报重复作业号: %v", err)
		}
	})
}
