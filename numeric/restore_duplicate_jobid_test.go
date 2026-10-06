package numeric

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeNamedSyntheticRecord 用任意文件名写一条作业记录，用于构造正常运行
// 不会产生的形态：多份记录共享同一作业号但文件名不同。
func writeNamedSyntheticRecord(t *testing.T, dir, name string, j *storedJob) {
	t.Helper()
	data, err := encodeRecord(j)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(dir, name, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func snapshotDir(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string][]byte)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = b
	}
	return out
}

func assertDirUnchanged(t *testing.T, dir string, before map[string][]byte) {
	t.Helper()
	after := snapshotDir(t, dir)
	if len(after) != len(before) {
		t.Fatalf("目录条目变化: before=%d after=%d", len(before), len(after))
	}
	for name, want := range before {
		got, ok := after[name]
		if !ok {
			t.Fatalf("原有记录文件 %s 在打开失败后消失", name)
		}
		if string(got) != string(want) {
			t.Fatalf("记录文件 %s 在重复作业号打开失败后被改写", name)
		}
	}
	for name := range after {
		if _, ok := before[name]; !ok {
			t.Fatalf("打开失败后目录中多出文件 %s", name)
		}
	}
}

func queuedStoredJob(id uint64, submitter string, values []int64, queuedAt time.Time, deps ...uint64) *storedJob {
	return &storedJob{
		id:           id,
		submitter:    submitter,
		values:       append([]int64(nil), values...),
		dependencies: append([]uint64(nil), deps...),
		queuedAt:     queuedAt,
		status:       StatusQueued,
	}
}

func succeededStoredJob(t *testing.T, id uint64, values []int64, queuedAt, finishedAt time.Time) *storedJob {
	t.Helper()
	j := &storedJob{
		id:              id,
		submitter:       "a",
		values:          append([]int64(nil), values...),
		queuedAt:        queuedAt,
		finishedAt:      finishedAt,
		status:          StatusSucceeded,
		effectiveValues: append([]int64(nil), values...),
	}
	sum, sumSq, _, ok := computeResult(values, 0, nil)
	if !ok {
		t.Fatalf("test setup: values %v overflow", values)
	}
	j.archive = newArchive(j, values, sum, sumSq, finishedAt)
	return j
}

// TestOpenRejectsDuplicateJobID 两份可正常解析、版本与状态合法的记录顶层
// 保存同一作业号时，Open 必须失败、返回非空错误且归档对象为 nil；错误须
// 说明重复作业号、包含该作业号与两份冲突记录的文件名。
func TestOpenRejectsDuplicateJobID(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 4, 1, 8, 0, 0, 0, time.UTC)
	name1 := jobFileName(7)
	name2 := "job-00000000000000000007-copy.json"
	writeNamedSyntheticRecord(t, dir, name1,
		queuedStoredJob(7, "alice", []int64{1, 2}, base))
	writeNamedSyntheticRecord(t, dir, name2,
		queuedStoredJob(7, "bob", []int64{3, 4}, base.Add(time.Second)))

	s, err := Open(dir)
	if err == nil {
		if s != nil {
			_ = s.Close()
		}
		t.Fatal("Open 必须在重复作业号时失败")
	}
	if s != nil {
		t.Fatalf("重复作业号冲突时归档对象必须为 nil，得到 %#v", s)
	}
	if !strings.Contains(err.Error(), "重复作业号") {
		t.Fatalf("错误必须明确说明存在重复作业号: %v", err)
	}
	if !strings.Contains(err.Error(), "7") {
		t.Fatalf("错误必须包含重复的作业号 7: %v", err)
	}
	if !strings.Contains(err.Error(), name1) || !strings.Contains(err.Error(), name2) {
		t.Fatalf("错误必须包含两份冲突记录的文件名 %s、%s: %v", name1, name2, err)
	}
}

// TestOpenRejectsDuplicateJobIDIdenticalContent 两份内容完全相同的记录也必须
// 拒绝，不能去重后当作一个作业继续使用。
func TestOpenRejectsDuplicateJobIDIdenticalContent(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 4, 2, 8, 0, 0, 0, time.UTC)
	j := queuedStoredJob(9, "alice", []int64{1, 2, 3}, base)
	name1 := jobFileName(9)
	name2 := "job-00000000000000000009-dup.json"
	writeNamedSyntheticRecord(t, dir, name1, j)
	// 重新构造一份字段完全一致的记录并写到另一个文件名。
	writeNamedSyntheticRecord(t, dir, name2,
		queuedStoredJob(9, "alice", []int64{1, 2, 3}, base))

	s, err := Open(dir)
	if err == nil {
		if s != nil {
			_ = s.Close()
		}
		t.Fatal("即使两份记录内容完全相同，Open 也必须因重复作业号失败")
	}
	if s != nil {
		t.Fatalf("冲突时归档对象必须为 nil: %#v", s)
	}
	if !strings.Contains(err.Error(), name1) || !strings.Contains(err.Error(), name2) {
		t.Fatalf("错误必须包含两份同内容冲突记录的文件名: %v", err)
	}
}

// TestOpenRejectsDuplicateJobIDThreeRecords 三份同号记录同样拒绝，错误至少
// 指出其中两份冲突文件，且两个文件名不同。
func TestOpenRejectsDuplicateJobIDThreeRecords(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 4, 3, 8, 0, 0, 0, time.UTC)
	names := []string{
		jobFileName(11),
		"job-00000000000000000011-a.json",
		"job-00000000000000000011-b.json",
	}
	for i, name := range names {
		writeNamedSyntheticRecord(t, dir, name,
			queuedStoredJob(11, "u", []int64{int64(i + 1)}, base.Add(time.Duration(i)*time.Second)))
	}
	s, err := Open(dir)
	if err == nil {
		if s != nil {
			_ = s.Close()
		}
		t.Fatal("三份同号记录必须拒绝打开")
	}
	if s != nil {
		t.Fatalf("冲突时归档对象必须为 nil: %#v", s)
	}
	named := 0
	for _, name := range names {
		if strings.Contains(err.Error(), name) {
			named++
		}
	}
	if named < 2 {
		t.Fatalf("错误至少要包含两份冲突记录的文件名，实际包含 %d 份: %v", named, err)
	}
}

// TestOpenDuplicateJobIDDoesNotTouchAnyRecord 存在冲突时，本次打开不能启动
// 任何计算，也不能按正常恢复规则把其他作业改判失败或补写结果：中断的运行
// 记录、排队下游、成功归档（含完成时间与校验值）、已失败记录以及冲突记录
// 自身的文件字节必须全部保持原样，且与它们是否比冲突记录更早被读取无关。
func TestOpenDuplicateJobIDDoesNotTouchAnyRecord(t *testing.T) {
	base := time.Date(2026, 4, 4, 8, 0, 0, 0, time.UTC)

	cases := []struct {
		name    string
		dupName string // 冲突额外文件的命名位置：排在标准文件名之前或之后
		setup   func(t *testing.T, dir string)
	}{
		{
			name:    "冲突记录排在其他记录之后",
			dupName: "job-00000000000000000005-dup.json",
		},
		{
			name:    "冲突记录排在其他记录之前",
			dupName: "job--0000000000000000005-dup.json",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
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
			// 作业 4：已失败终态记录，正常恢复不改动。
			writeSyntheticRecord(t, dir, &storedJob{
				id: 4, submitter: "a", values: []int64{5},
				queuedAt: base.Add(5 * time.Second), finishedAt: base.Add(6 * time.Second),
				status: StatusFailed, failureReason: "平方和超出 int64 范围",
			})
			// 作业 5 同号两份：一份排队，一份带完整成功归档。文件名不同不能
			// 让它们成为两个作业，也不能挑带完整结果的那份继续。
			writeSyntheticRecord(t, dir,
				queuedStoredJob(5, "a", []int64{9}, base.Add(7*time.Second)))
			writeNamedSyntheticRecord(t, dir, tc.dupName,
				succeededStoredJob(t, 5, []int64{9}, base.Add(7*time.Second), base.Add(8*time.Second)))

			before := snapshotDir(t, dir)
			s, err := Open(dir)
			if err == nil {
				if s != nil {
					_ = s.Close()
				}
				t.Fatal("存在重复作业号时必须打开失败")
			}
			if s != nil {
				t.Fatalf("冲突时归档对象必须为 nil: %#v", s)
			}
			assertDirUnchanged(t, dir, before)

			// 没有任何延迟补写：稍等后再次比对，所有记录（含状态、完成时间、
			// 归档与校验值）仍是打开前的字节。
			time.Sleep(50 * time.Millisecond)
			assertDirUnchanged(t, dir, before)
		})
	}
}

// TestOpenDuplicateJobIDRunningPairNotFailed 重复作业号的两份记录中一份是
// running 时，也不能先按“计算被中断”改判它再返回重复错误。
func TestOpenDuplicateJobIDRunningPairNotFailed(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 4, 5, 8, 0, 0, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 6, submitter: "a", values: []int64{1},
		queuedAt: base, startedAt: base.Add(time.Second), status: StatusRunning,
	})
	writeNamedSyntheticRecord(t, dir, "job-00000000000000000006-dup.json",
		queuedStoredJob(6, "b", []int64{2}, base.Add(2*time.Second)))
	before := snapshotDir(t, dir)

	s, err := Open(dir)
	if err == nil {
		if s != nil {
			_ = s.Close()
		}
		t.Fatal("running 与排队记录同号也必须拒绝打开")
	}
	if s != nil {
		t.Fatalf("冲突时归档对象必须为 nil: %#v", s)
	}
	assertDirUnchanged(t, dir, before)
}

// TestOpenDistinctJobIDsWithSameContentAccepted 不同作业号即使整数序列、种子
// 或结果完全相同，仍是不同作业，不因这项检查被拒绝；已保存作业号保持不变，
// 正常查询与幂等重放继续可用。
func TestOpenDistinctJobIDsWithSameContentAccepted(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 4, 6, 8, 0, 0, 0, time.UTC)
	// 同提交人、同请求号、同内容但作业号不同在正常提交中不会发生，因此这里
	// 请求号留空；两份记录除作业号与提交时间外完全一致。
	writeSyntheticRecord(t, dir,
		queuedStoredJob(1, "alice", []int64{1, 2, 3}, base))
	writeSyntheticRecord(t, dir,
		queuedStoredJob(2, "alice", []int64{1, 2, 3}, base.Add(time.Second)))

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("不同作业号同内容不应被拒绝: %v", err)
	}
	defer s.Close()
	j1 := waitStatus(t, s, 1, StatusSucceeded)
	j2 := waitStatus(t, s, 2, StatusSucceeded)
	if j1.Archive.Sum != j2.Archive.Sum || j1.Archive.Checksum != j2.Archive.Checksum {
		t.Fatal("同输入同种子的结果摘要与校验值本应一致")
	}
	replay, err := s.Submit(SubmitRequest{
		Submitter: "alice", RequestID: "later", Values: []int64{7},
	})
	if err != nil {
		t.Fatalf("冲突解决后的新提交必须可用: %v", err)
	}
	waitStatus(t, s, replay.ID, StatusSucceeded)
	if replay.ID <= 2 {
		t.Fatalf("新作业号必须续号分配，得到 %d", replay.ID)
	}
}

// TestOpenAfterDuplicateResolvedRecoversNormally 调用方处理冲突（删除一份）
// 后再次打开，既有恢复规则照常生效：running 改判失败并级联排队下游，
// 成功归档保留，留下的那份同号记录正常参与调度。
func TestOpenAfterDuplicateResolvedRecoversNormally(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 4, 7, 8, 0, 0, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "a", values: []int64{1, 2},
		queuedAt: base, startedAt: base.Add(time.Second), status: StatusRunning,
	})
	writeSyntheticRecord(t, dir,
		queuedStoredJob(2, "a", []int64{3}, base.Add(2*time.Second), 1))
	succeeded := succeededStoredJob(t, 3, []int64{4}, base.Add(3*time.Second), base.Add(4*time.Second))
	wantChecksum := succeeded.archive.Checksum
	writeSyntheticRecord(t, dir, succeeded)
	writeSyntheticRecord(t, dir,
		queuedStoredJob(5, "a", []int64{9}, base.Add(7*time.Second)))
	dupName := "job-00000000000000000005-dup.json"
	writeNamedSyntheticRecord(t, dir, dupName,
		queuedStoredJob(5, "b", []int64{10}, base.Add(8*time.Second)))

	if s, err := Open(dir); err == nil {
		_ = s.Close()
		t.Fatal("处理冲突前打开必须失败")
	}
	// 调用方自行删除一份冲突记录。
	if err := os.Remove(filepath.Join(dir, dupName)); err != nil {
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
	j3, _ := s.Get(3)
	if j3.Status != StatusSucceeded || j3.Archive == nil || j3.Archive.Checksum != wantChecksum {
		t.Fatalf("合法成功归档应原样保留: %+v", j3)
	}
	j5 := waitStatus(t, s, 5, StatusSucceeded)
	if j5.Archive == nil || j5.Archive.Sum != 9 {
		t.Fatalf("保留的作业 5 应正常计算完成: %+v", j5)
	}
}

// TestOpenDuplicateDoesNotMaskPreexistingRecordErrors 原有的解析失败、版本
// 不支持与非法状态错误继续有效；与重复作业号并存时，按目录读取顺序先遇到
// 的记录返回对应的原有错误。
func TestOpenDuplicateDoesNotMaskPreexistingRecordErrors(t *testing.T) {
	base := time.Date(2026, 4, 8, 8, 0, 0, 0, time.UTC)

	t.Run("损坏记录单独存在", func(t *testing.T) {
		dir := t.TempDir()
		writeNamedSyntheticRecord(t, dir, jobFileName(1),
			queuedStoredJob(1, "a", []int64{1}, base))
		if err := os.WriteFile(filepath.Join(dir, "job-00000000000000000002.json"),
			[]byte("{not json"), 0o600); err != nil {
			t.Fatal(err)
		}
		s, err := Open(dir)
		if err == nil {
			_ = s.Close()
			t.Fatal("损坏记录必须导致打开失败")
		}
		if s != nil || !strings.Contains(err.Error(), "损坏") {
			t.Fatalf("应返回原有的记录损坏错误且归档为 nil: %v", err)
		}
	})

	t.Run("版本不支持", func(t *testing.T) {
		dir := t.TempDir()
		writeNamedSyntheticRecord(t, dir, jobFileName(1),
			queuedStoredJob(1, "a", []int64{1}, base))
		writeRawJSONRecord(t, dir, 2, `{
  "version": "numeric-job-v9",
  "id": 2,
  "submitter": "a",
  "request_id": "",
  "seed": 0,
  "values": [1],
  "has_dependency": false,
  "dependency_id": 0,
  "queued_at": "2026-04-08T08:00:00Z",
  "status": "queued"
}`)
		s, err := Open(dir)
		if err == nil {
			_ = s.Close()
			t.Fatal("版本不支持必须导致打开失败")
		}
		if s != nil || !strings.Contains(err.Error(), "版本不受支持") {
			t.Fatalf("应返回原有的版本不支持错误且归档为 nil: %v", err)
		}
	})

	t.Run("非法状态", func(t *testing.T) {
		dir := t.TempDir()
		writeNamedSyntheticRecord(t, dir, jobFileName(1),
			queuedStoredJob(1, "a", []int64{1}, base))
		writeRawJSONRecord(t, dir, 2, `{
  "version": "numeric-job-v1",
  "id": 2,
  "submitter": "a",
  "request_id": "",
  "seed": 0,
  "values": [1],
  "has_dependency": false,
  "dependency_id": 0,
  "queued_at": "2026-04-08T08:00:00Z",
  "status": "paused"
}`)
		s, err := Open(dir)
		if err == nil {
			_ = s.Close()
			t.Fatal("非法状态必须导致打开失败")
		}
		if s != nil || !strings.Contains(err.Error(), "状态非法") {
			t.Fatalf("应返回原有的非法状态错误且归档为 nil: %v", err)
		}
	})

	t.Run("损坏记录排在冲突之前时先报损坏", func(t *testing.T) {
		dir := t.TempDir()
		// job-...01.json 损坏，文件名排序早于作业 3 的冲突对。
		if err := os.WriteFile(filepath.Join(dir, jobFileName(1)),
			[]byte("{not json"), 0o600); err != nil {
			t.Fatal(err)
		}
		writeNamedSyntheticRecord(t, dir, jobFileName(3),
			queuedStoredJob(3, "a", []int64{1}, base))
		writeNamedSyntheticRecord(t, dir, "job-00000000000000000003-dup.json",
			queuedStoredJob(3, "b", []int64{2}, base))
		s, err := Open(dir)
		if err == nil {
			_ = s.Close()
			t.Fatal("必须打开失败")
		}
		if s != nil || !strings.Contains(err.Error(), "损坏") {
			t.Fatalf("先读到损坏记录时应保留原有损坏错误: %v", err)
		}
	})

	t.Run("同目录另有损坏记录时保留原有损坏错误", func(t *testing.T) {
		// 重复作业号检查只在全部记录都可正常解析、版本及状态合法时生效；
		// 损坏记录即使文件名排在冲突对之后，其原有打开错误仍然有效。
		dir := t.TempDir()
		writeNamedSyntheticRecord(t, dir, "job-00000000000000000001-a.json",
			queuedStoredJob(1, "a", []int64{1}, base))
		writeNamedSyntheticRecord(t, dir, jobFileName(1),
			queuedStoredJob(1, "b", []int64{2}, base))
		if err := os.WriteFile(filepath.Join(dir, jobFileName(2)),
			[]byte("{not json"), 0o600); err != nil {
			t.Fatal(err)
		}
		before := snapshotDir(t, dir)
		s, err := Open(dir)
		if err == nil {
			_ = s.Close()
			t.Fatal("必须打开失败")
		}
		if s != nil || !strings.Contains(err.Error(), "损坏") {
			t.Fatalf("存在损坏记录时应保留原有的损坏错误，而非重复作业号错误: %v", err)
		}
		// 解析阶段失败同样不允许改写目录内任何记录。
		assertDirUnchanged(t, dir, before)
	})
}
