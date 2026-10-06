package numeric

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// jsonMarshalRecord 序列化一条作业记录，供构造畸形记录文件使用。
func jsonMarshalRecord(r *jobRecord) ([]byte, error) {
	return json.Marshal(r)
}

// writeRecordAs 把内存作业记录按指定文件名（不必是 jobFileName(id)）原子写入
// 目录，用于人为构造“两份文件保存同一顶层作业号”的冲突场景。
func writeRecordAs(t *testing.T, dir, name string, j *storedJob) {
	t.Helper()
	data, err := encodeRecord(j)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(dir, name, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// syntheticSucceededJob 在内存中构造一条自洽的成功作业（摘要、日志与校验值
// 经 newArchive 全套生成），不落盘，由调用方决定写到什么文件名。
func syntheticSucceededJob(t *testing.T, id uint64, values, eff []int64, deps []uint64, base time.Time) *storedJob {
	t.Helper()
	sum, sumSq, _, ok := computeResult(eff, 0, nil)
	if !ok {
		t.Fatalf("synthetic eff %v overflows", eff)
	}
	j := &storedJob{
		id: id, submitter: "a", requestID: "j" + itoa(id),
		values: values, dependencies: deps,
		queuedAt:   base.Add(time.Duration(id) * time.Second),
		startedAt:  base.Add(time.Duration(id)*time.Second + time.Millisecond),
		finishedAt: base.Add(time.Duration(id+1) * time.Second),
		status:     StatusSucceeded,
	}
	j.effectiveValues = append([]int64(nil), eff...)
	j.archive = newArchive(j, eff, sum, sumSq, j.finishedAt)
	return j
}

// leadingDupName / trailingDupName 给出同一作业号的额外冲突文件名：
// 按 ReadDir 的文件名排序，前者排在规范文件名之前，后者排在其后，
// 用于证明冲突检查与文件读取先后无关。
func leadingDupName(id uint64) string  { return fmt.Sprintf("job-%020d-dup.json", id) }
func trailingDupName(id uint64) string { return fmt.Sprintf("job-%020dz.json", id) }

// snapshotJobFiles 读取目录内全部 job-*.json 的原始字节，用于断言失败的打开
// 没有改写任何记录。
func snapshotJobFiles(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string][]byte)
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "job-") || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = data
	}
	return out
}

func assertJobFilesUntouched(t *testing.T, dir string, want map[string][]byte) {
	t.Helper()
	got := snapshotJobFiles(t, dir)
	if len(got) != len(want) {
		t.Fatalf("job 文件集合发生变化: before=%d after=%d", len(want), len(got))
	}
	for name, wb := range want {
		gb, ok := got[name]
		if !ok {
			t.Fatalf("原有记录文件 %s 在打开后消失", name)
		}
		if string(gb) != string(wb) {
			t.Fatalf("记录文件 %s 在重复作业号打开失败后被改写", name)
		}
	}
}

// 两份不同文件名的记录在顶层保存同一作业号、且内容不同（一份成功 [7]、
// 一份排队 [8]）时，Open 必须失败：返回非 nil 错误（可 errors.Is 到
// ErrDuplicateJobRecord）、归档对象为 nil；错误明确说明重复作业号，包含
// 作业号与两份冲突记录的文件名。额外文件按文件名排序在前、在后两种情况
// 都不影响结果。
func TestOpenDuplicateJobIDRejects(t *testing.T) {
	base := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	for _, extra := range []func(uint64) string{leadingDupName, trailingDupName} {
		dir := t.TempDir()
		canonical := jobFileName(5)
		dupName := extra(5)
		writeRecordAs(t, dir, canonical,
			syntheticSucceededJob(t, 5, []int64{7}, []int64{7}, nil, base))
		writeRecordAs(t, dir, dupName, &storedJob{
			id: 5, submitter: "a", requestID: "j5-queued",
			values:   []int64{8},
			queuedAt: base.Add(5 * time.Second),
			status:   StatusQueued,
		})

		s, err := Open(dir)
		if err == nil {
			if s != nil {
				_ = s.Close()
			}
			t.Fatalf("重复作业号必须使 Open 失败（额外文件排序位置=%s）", dupName)
		}
		if s != nil {
			_ = s.Close()
			t.Fatalf("重复作业号冲突时归档对象必须为 nil，got %v", s)
		}
		if !errors.Is(err, ErrDuplicateJobRecord) {
			t.Fatalf("错误必须包装 ErrDuplicateJobRecord, got %v", err)
		}
		msg := err.Error()
		if !strings.Contains(msg, "重复作业号") {
			t.Fatalf("错误必须明确说明存在重复作业号: %q", msg)
		}
		if !strings.Contains(msg, "作业号 5") {
			t.Fatalf("错误必须包含冲突作业号 5: %q", msg)
		}
		if !strings.Contains(msg, canonical) || !strings.Contains(msg, dupName) {
			t.Fatalf("错误必须包含至少两份冲突记录的文件名（%s、%s）: %q",
				canonical, dupName, msg)
		}
	}
}

// 两份内容完全相同的记录只是文件名不同，也必须拒绝，不能去重后继续使用。
func TestOpenDuplicateJobIDIdenticalContentsRejected(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	j := syntheticSucceededJob(t, 9, []int64{1, 2}, []int64{1, 2}, nil, base)
	writeRecordAs(t, dir, jobFileName(9), j)
	writeRecordAs(t, dir, leadingDupName(9), j)

	s, err := Open(dir)
	if err == nil {
		if s != nil {
			_ = s.Close()
		}
		t.Fatal("字节完全相同的两份同号记录也必须拒绝")
	}
	if s != nil {
		_ = s.Close()
		t.Fatalf("冲突时归档对象必须为 nil, got %v", s)
	}
	if !errors.Is(err, ErrDuplicateJobRecord) {
		t.Fatalf("want ErrDuplicateJobRecord, got %v", err)
	}
}

// 三份记录同号时，错误列出全部冲突文件名与份数，调用方不会漏掉第三份。
func TestOpenDuplicateJobIDThreeFilesReportsAll(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	j := syntheticSucceededJob(t, 7, []int64{3}, []int64{3}, nil, base)
	n1, n2, n3 := jobFileName(7), leadingDupName(7), fmt.Sprintf("job-%020d-dup2.json", 7)
	writeRecordAs(t, dir, n1, j)
	writeRecordAs(t, dir, n2, j)
	writeRecordAs(t, dir, n3, j)

	s, err := Open(dir)
	if err == nil {
		if s != nil {
			_ = s.Close()
		}
		t.Fatal("三份同号记录必须拒绝打开")
	}
	if s != nil {
		_ = s.Close()
		t.Fatalf("冲突时归档对象必须为 nil, got %v", s)
	}
	msg := err.Error()
	for _, n := range []string{n1, n2, n3} {
		if !strings.Contains(msg, n) {
			t.Fatalf("错误必须列出全部三份冲突文件名，缺 %s: %q", n, msg)
		}
	}
	if !strings.Contains(msg, "3 份") || !strings.Contains(msg, "作业号 7") {
		t.Fatalf("错误必须说明作业号 7 有 3 份记录: %q", msg)
	}
}

// 多组冲突时报告作业号最小的那组，选择与文件排列无关。
func TestOpenDuplicateJobIDSmallestGroupReported(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	writeRecordAs(t, dir, jobFileName(3),
		syntheticSucceededJob(t, 3, []int64{3}, []int64{3}, nil, base))
	writeRecordAs(t, dir, leadingDupName(3),
		syntheticSucceededJob(t, 3, []int64{33}, []int64{33}, nil, base))
	writeRecordAs(t, dir, jobFileName(9),
		syntheticSucceededJob(t, 9, []int64{9}, []int64{9}, nil, base))
	writeRecordAs(t, dir, leadingDupName(9),
		syntheticSucceededJob(t, 9, []int64{99}, []int64{99}, nil, base))

	_, err := Open(dir)
	if err == nil || !errors.Is(err, ErrDuplicateJobRecord) {
		t.Fatalf("want ErrDuplicateJobRecord, got %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "作业号 3") || strings.Contains(msg, "作业号 9") {
		t.Fatalf("应报告作业号最小的冲突组 3，而非 9: %q", msg)
	}
}

// 冲突存在时本次打开不能改写目录内任何记录，也不能启动计算——即使同目录还
// 放有上次中断的运行记录、需要复查的损坏成功记录、待补判的空序列排队记录、
// 等待它们的下游和无关的待运行排队记录；冲突文件按文件名排序在前、在后都
// 一样。调用方删除多出的冲突文件后再次打开，既有恢复规则才生效。
func TestOpenDuplicateJobIDLeavesAllRecordsUntouchedThenRestore(t *testing.T) {
	base := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	for _, extra := range []func(uint64) string{leadingDupName, trailingDupName} {
		dir := t.TempDir()

		// 作业 1：上次关闭时仍在运行——正常恢复会改判“计算被中断”并回写。
		writeRecordAs(t, dir, jobFileName(1), &storedJob{
			id: 1, submitter: "a", requestID: "running",
			values:    []int64{100},
			queuedAt:  base,
			startedAt: base.Add(time.Second),
			status:    StatusRunning,
		})

		// 作业 2：成功但校验值被破坏——正常恢复会复查改判失败并回写。
		j2 := syntheticSucceededJob(t, 2, []int64{1, 2}, []int64{1, 2}, nil, base)
		j2.archive.Checksum = "deadbeef"
		writeRecordAs(t, dir, jobFileName(2), j2)

		// 作业 3：排队但原始整数序列为空——正常恢复会判失败并回写。
		writeRecordAs(t, dir, jobFileName(3), &storedJob{
			id: 3, submitter: "a", requestID: "empty",
			queuedAt: base.Add(3 * time.Second),
			status:   StatusQueued,
		})

		// 作业 4：排队等待作业 3——正常恢复会随作业 3 级联失败并回写。
		writeRecordAs(t, dir, jobFileName(4), &storedJob{
			id: 4, submitter: "a", requestID: "child",
			values:       []int64{9},
			dependencies: []uint64{3},
			queuedAt:     base.Add(4 * time.Second),
			status:       StatusQueued,
		})

		// 作业 5：同号冲突，两份都是合法记录（成功 [7] 与排队 [8]）。
		dupName := extra(5)
		writeRecordAs(t, dir, jobFileName(5),
			syntheticSucceededJob(t, 5, []int64{7}, []int64{7}, nil, base))
		writeRecordAs(t, dir, dupName, &storedJob{
			id: 5, submitter: "a", requestID: "j5-queued",
			values:   []int64{8},
			queuedAt: base.Add(5 * time.Second),
			status:   StatusQueued,
		})

		// 作业 6：无关排队作业——正常恢复后 worker 会立即计算并改写记录。
		writeRecordAs(t, dir, jobFileName(6), &storedJob{
			id: 6, submitter: "b", requestID: "indep",
			values:   []int64{6},
			queuedAt: base.Add(6 * time.Second),
			status:   StatusQueued,
		})

		before := snapshotJobFiles(t, dir)

		s, err := Open(dir)
		if err == nil || !errors.Is(err, ErrDuplicateJobRecord) {
			if s != nil {
				_ = s.Close()
			}
			t.Fatalf("extra=%s: want ErrDuplicateJobRecord, got %v", dupName, err)
		}
		if s != nil {
			_ = s.Close()
			t.Fatalf("extra=%s: 冲突时归档对象必须为 nil", dupName)
		}
		// 冲突打开失败后：状态、完成时间、归档与校验值、排队内容全部原样，
		// 没有任何恢复改判或计算发生。
		assertJobFilesUntouched(t, dir, before)

		// 调用方处理冲突（移除多出的文件）后再次打开，既有恢复规则照常生效。
		if err := os.Remove(filepath.Join(dir, dupName)); err != nil {
			t.Fatal(err)
		}
		s2, err := Open(dir)
		if err != nil {
			t.Fatalf("冲突处理后应能正常打开: %v", err)
		}
		g1 := waitStatus(t, s2, 1, StatusFailed)
		if !strings.Contains(g1.FailureReason, "计算被中断") {
			t.Fatalf("job 1 reason=%q want 计算被中断", g1.FailureReason)
		}
		g2 := waitStatus(t, s2, 2, StatusFailed)
		if !strings.Contains(g2.FailureReason, "校验值不一致") {
			t.Fatalf("job 2 reason=%q want 校验值不一致的复查失败", g2.FailureReason)
		}
		g3 := waitStatus(t, s2, 3, StatusFailed)
		if !strings.Contains(g3.FailureReason, "原始整数序列为空") {
			t.Fatalf("job 3 reason=%q want 原始整数序列为空", g3.FailureReason)
		}
		g4 := waitStatus(t, s2, 4, StatusFailed)
		if g4.BlockerID != 3 || !strings.Contains(g4.FailureReason, "直接上游作业 3") {
			t.Fatalf("job 4 blocker=%d reason=%q want root/direct 3", g4.BlockerID, g4.FailureReason)
		}
		g5 := waitStatus(t, s2, 5, StatusSucceeded)
		if g5.Archive == nil || g5.Archive.Sum != 7 || g5.Archive.SumOfSquares != 49 {
			t.Fatalf("保留的 job 5 应按既有成功记录恢复: %+v", g5.Archive)
		}
		g6 := waitStatus(t, s2, 6, StatusSucceeded)
		if g6.Archive.Sum != 6 || g6.Archive.SumOfSquares != 36 {
			t.Fatalf("无关排队 job 6 应在冲突解除后正常计算: %+v", g6.Archive)
		}
		_ = s2.Close()
	}
}

// 不同作业号即使整数序列、种子与结果完全相同，仍是两个作业：正常打开、
// 作业号保持不变，幂等重放各自返回原作业，依赖恢复与查询照常。
func TestOpenDistinctJobIDsIdenticalContentRemainSeparate(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	// 作业 1 与作业 2 的序列、种子、结果摘要与校验值全部相同，只是作业号与
	// 提交人不同。
	j1 := syntheticSucceededJob(t, 1, []int64{2, 3}, []int64{2, 3}, nil, base)
	j1.submitter, j1.requestID = "alice", "same"
	j1.archive = newArchive(j1, []int64{2, 3}, 5, 13, j1.finishedAt)
	j2 := syntheticSucceededJob(t, 2, []int64{2, 3}, []int64{2, 3}, nil, base)
	j2.submitter, j2.requestID = "bob", "same"
	j2.archive = newArchive(j2, []int64{2, 3}, 5, 13, j2.finishedAt)
	writeRecordAs(t, dir, jobFileName(1), j1)
	writeRecordAs(t, dir, jobFileName(2), j2)
	// 作业 3：依赖两个“同结果但不同作业号”的上游，实际输入 [9,5,5]。
	writeRecordAs(t, dir, jobFileName(3), &storedJob{
		id: 3, submitter: "carol", requestID: "join",
		values:       []int64{9},
		dependencies: []uint64{1, 2},
		queuedAt:     base.Add(3 * time.Second),
		status:       StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("不同作业号内容相同不应被拒绝: %v", err)
	}
	defer s.Close()

	g1, err := s.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	g2, err := s.Get(2)
	if err != nil {
		t.Fatal(err)
	}
	if g1.Status != StatusSucceeded || g2.Status != StatusSucceeded ||
		g1.Archive == nil || g2.Archive == nil ||
		g1.Archive.Checksum != g2.Archive.Checksum {
		t.Fatalf("两份同内容的不同作业都应保持成功且校验值相同: %+v %+v", g1, g2)
	}

	// 幂等重放：同一请求号在不同提交人名下分别返回原作业。
	r1, err := s.Submit(SubmitRequest{Submitter: "alice", RequestID: "same", Values: []int64{2, 3}})
	if err != nil || r1.ID != 1 {
		t.Fatalf("alice 幂等重放应返回作业 1, got id=%d err=%v", r1.ID, err)
	}
	r2, err := s.Submit(SubmitRequest{Submitter: "bob", RequestID: "same", Values: []int64{2, 3}})
	if err != nil || r2.ID != 2 {
		t.Fatalf("bob 幂等重放应返回作业 2, got id=%d err=%v", r2.ID, err)
	}

	// 依赖恢复：两个上游都成功后作业 3 按 [9,5,5] 计算（总和 19）。
	g3 := waitStatus(t, s, 3, StatusSucceeded)
	if g3.Archive.Sum != 19 {
		t.Fatalf("job 3 sum=%d want 19 (eff=%v)", g3.Archive.Sum, g3.Archive.EffectiveValues)
	}

	// 按提交人列举仍可分别查到两份不同作业号的记录。
	la, err := s.List("alice", time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	lb, err := s.List("bob", time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(la) != 1 || la[0].ID != 1 || len(lb) != 1 || lb[0].ID != 2 {
		t.Fatalf("按提交人列举结果异常: alice=%v bob=%v", listIDs(la), listIDs(lb))
	}
}

// 冲突组中任一份记录无法解析、版本不受支持或状态非法时，沿用既有的打开
// 错误（重复作业号检查只适用于全部合法的记录），且同样不发生任何回写。
func TestOpenDuplicateJobIDWithInvalidRecordKeepsExistingError(t *testing.T) {
	base := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	cases := []struct {
		name    string
		want    string
		payload func(id uint64) []byte
	}{
		{
			name: "corrupt-json",
			want: "损坏",
			payload: func(uint64) []byte {
				return []byte("{not json")
			},
		},
		{
			name: "unsupported-version",
			want: "版本不受支持",
			payload: func(id uint64) []byte {
				r := jobRecord{Version: "numeric-job-v0", ID: id, Status: StatusQueued}
				data, err := jsonMarshalRecord(&r)
				if err != nil {
					t.Fatal(err)
				}
				return data
			},
		},
		{
			name: "invalid-status",
			want: "状态非法",
			payload: func(id uint64) []byte {
				r := jobRecord{Version: recordVersion, ID: id, Status: Status("bogus")}
				data, err := jsonMarshalRecord(&r)
				if err != nil {
					t.Fatal(err)
				}
				return data
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			// 作业 1：运行中记录，正常恢复会回写——验证任一既有解析错误同样
			// 先于一切回写返回。
			writeRecordAs(t, dir, jobFileName(1), &storedJob{
				id: 1, submitter: "a", requestID: "running",
				values:    []int64{100},
				queuedAt:  base,
				startedAt: base.Add(time.Second),
				status:    StatusRunning,
			})
			writeRecordAs(t, dir, jobFileName(5),
				syntheticSucceededJob(t, 5, []int64{7}, []int64{7}, nil, base))
			badName := leadingDupName(5)
			if err := os.WriteFile(filepath.Join(dir, badName), tc.payload(5), 0o600); err != nil {
				t.Fatal(err)
			}
			before := snapshotJobFiles(t, dir)

			s, err := Open(dir)
			if err == nil {
				if s != nil {
					_ = s.Close()
				}
				t.Fatal("非法记录必须使 Open 失败")
			}
			if s != nil {
				_ = s.Close()
				t.Fatalf("打开失败时归档对象必须为 nil, got %v", s)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("错误应沿用既有%q判定: %v", tc.want, err)
			}
			if errors.Is(err, ErrDuplicateJobRecord) {
				t.Fatalf("存在非法记录时不应判定为重复作业号: %v", err)
			}
			assertJobFilesUntouched(t, dir, before)
		})
	}
}
