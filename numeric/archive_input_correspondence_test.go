package numeric

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// rewriteRecordEffectiveInputs 把磁盘上成功记录的实际输入替换为另一份序列，
// 并按新序列重算总和、平方和、输入/结果摘要、日志与校验值，使归档全套字段
// 与这份伪造的实际输入完全自洽；顶层记录与归档中的原始参数（values、依赖
// 列表）保持原样。它模拟“把另一份输入的计算结果挂在原始参数下”的记录：
// 总和/平方和可能恰好不变，但逐项次序与追加数量对不上。
func rewriteRecordEffectiveInputs(t *testing.T, dir string, id uint64, effective []int64) {
	t.Helper()
	p := filepath.Join(dir, jobFileName(id))
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var r jobRecord
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	if r.Archive == nil {
		t.Fatal("record has no archive")
	}
	sum, sumSq, _, ok := computeResult(effective, r.Seed, nil)
	if !ok {
		t.Fatalf("forged effective inputs %v overflow; choose a non-overflowing sequence", effective)
	}
	r.EffectiveValues = append([]int64(nil), effective...)
	r.Archive.EffectiveValues = append([]int64(nil), effective...)
	r.Archive.Sum = sum
	r.Archive.SumOfSquares = sumSq
	r.Archive.InputsDigest = inputsDigestHex(effective, r.Seed)
	r.Archive.ResultDigest = resultDigestHex(effective, r.Seed, sum, sumSq)
	r.Archive.Log = buildLog(effective, r.Seed, sum, sumSq)
	r.Archive.Checksum = checksumHex(effective, r.Seed, sum, sumSq, r.Archive.Log, r.Archive.ResultDigest)
	out, err := json.MarshalIndent(&r, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, out, 0o600); err != nil {
		t.Fatal(err)
	}
}

// assertCorrespondenceFailure 重开归档后作业必须呈现为输入对应关系失败：
// 状态失败、查询不再返回成功归档与实际输入、原作业号与原始参数保留、
// 失败原因明确，落盘记录同样已改写为失败。
func assertCorrespondenceFailure(t *testing.T, s *Store, id uint64, wantValues []int64) {
	t.Helper()
	g, err := s.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusFailed {
		t.Fatalf("job %d must fail closed, got %s", id, g.Status)
	}
	if g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("job %d must not carry archive/effective inputs: archive=%v effective=%v",
			id, g.Archive, g.EffectiveValues)
	}
	if g.ID != id {
		t.Fatalf("original job id changed: got %d, want %d", g.ID, id)
	}
	if len(g.Values) != len(wantValues) {
		t.Fatalf("original values changed: %v, want %v", g.Values, wantValues)
	}
	for i := range wantValues {
		if g.Values[i] != wantValues[i] {
			t.Fatalf("original values changed: %v, want %v", g.Values, wantValues)
		}
	}
	if !strings.Contains(g.FailureReason, "实际输入与原始参数或依赖数量不符") ||
		!strings.Contains(g.FailureReason, "成功结果不可用") {
		t.Fatalf("job %d reason=%q must state effective inputs mismatch and success results unusable",
			id, g.FailureReason)
	}
}

// 原始参数 [2,-3] 的实际输入被换成另一份序列 [-3,2]：总和与平方和不变，
// 摘要、日志与校验值也按这份实际输入保持自洽，重开后仍必须判定失败。
func TestReopenRejectsSwappedEffectiveInputs(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	j := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{2, -3}})
	done := waitStatus(t, s, j.ID, StatusSucceeded)
	if done.Archive.Sum != -1 || done.Archive.SumOfSquares != 13 {
		t.Fatalf("real result=%d,%d", done.Archive.Sum, done.Archive.SumOfSquares)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 换序：[-3,2] 与 [2,-3] 的总和、平方和完全相同。
	rewriteRecordEffectiveInputs(t, dir, j.ID, []int64{-3, 2})

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	assertCorrespondenceFailure(t, s2, j.ID, []int64{2, -3})

	data, err := os.ReadFile(filepath.Join(dir, jobFileName(j.ID)))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"status": "failed"`) {
		t.Fatal("on-disk record must be rewritten as failed")
	}
}

// 原始部分被替换、截短或多出未记录输入时，即使全套摘要自洽也必须失败；
// 负数、零与重复整数按位置判断，不能只比较总和或忽略次序。
func TestReopenRejectsMismatchedEffectiveInputs(t *testing.T) {
	cases := []struct {
		name   string
		values []int64
		eff    []int64
	}{
		// 次序调换、含负数与重复值：[0,-2,-2,3] 与 [0,-2,3,-2] 总和相同。
		{"reordered with negatives zero and dups",
			[]int64{0, -2, -2, 3}, []int64{0, -2, 3, -2}},
		// 同长度但替换为另一份序列。
		{"replaced same length", []int64{2, -3}, []int64{1, -2}},
		// 无依赖却多出一个未记录的输入。
		{"extra unrecorded input", []int64{2, -3}, []int64{2, -3, 99}},
		// 无依赖却截短原始序列。
		{"truncated original", []int64{2, -3}, []int64{2}},
		// 原始序列开头被塞入一个值，原始参数整体后移。
		{"prepended value", []int64{2, -3}, []int64{99, 2, -3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			j := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: tc.values})
			waitStatus(t, s, j.ID, StatusSucceeded)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			rewriteRecordEffectiveInputs(t, dir, j.ID, tc.eff)

			s2, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s2.Close()
			assertCorrespondenceFailure(t, s2, j.ID, tc.values)
		})
	}
}

// 有依赖时：完整保留原始序列、追加数量恰好等于直接依赖数的记录重开后
// 保持成功；借依赖之名替换/截短原始部分、漏掉或多出追加输入的记录失败。
func TestReopenDependencyEffectiveInputCorrespondence(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{2, -3}})
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{10}})
	waitStatus(t, s, u1.ID, StatusSucceeded) // sum=-1
	waitStatus(t, s, u2.ID, StatusSucceeded) // sum=10
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{5},
		Dependencies: []uint64{u1.ID, u2.ID},
	})
	done := waitStatus(t, s, down.ID, StatusSucceeded)
	if done.Archive.Sum != 14 || done.Archive.SumOfSquares != 126 {
		t.Fatalf("real result=%d,%d", done.Archive.Sum, done.Archive.SumOfSquares)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 非法的实际输入：长度不对、原始部分被替换或追加部分缺失/多出。
	bad := []struct {
		name string
		eff  []int64
	}{
		{"missing one appended upstream sum", []int64{5, -1}},
		{"extra unrecorded appended input", []int64{5, -1, 10, 10}},
		{"original replaced by first upstream sum", []int64{-1, -1, 10}},
		{"original dropped, only appended sums", []int64{-1, 10}},
		{"reordered across the boundary", []int64{-1, 5, 10}},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			d := t.TempDir()
			// 每个子用例独立复制一份原始归档再篡改。
			copyArchive(t, dir, d)
			rewriteRecordEffectiveInputs(t, d, down.ID, tc.eff)
			s2, err := Open(d)
			if err != nil {
				t.Fatal(err)
			}
			defer s2.Close()
			assertCorrespondenceFailure(t, s2, down.ID, []int64{5})
		})
	}

	// 合法的追加输入：原始序列完整保留、追加数量正确，重开后保持原结果。
	s3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	g, err := s3.Get(down.ID)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusSucceeded || g.Archive == nil {
		t.Fatalf("valid dependent archive must survive reopen: %s", g.Status)
	}
	if g.Archive.Sum != 14 || g.Archive.SumOfSquares != 126 ||
		g.Archive.Checksum != done.Archive.Checksum {
		t.Fatalf("archive changed across reopen: %+v", g.Archive)
	}
	if len(g.EffectiveValues) != 3 || g.EffectiveValues[0] != 5 ||
		g.EffectiveValues[1] != -1 || g.EffectiveValues[2] != 10 {
		t.Fatalf("effective values=%v, want [5 -1 10]", g.EffectiveValues)
	}
}

// copyArchive 把一个归档目录中的记录文件复制到另一个（已存在的）目录。
func copyArchive(t *testing.T, src, dst string) {
	t.Helper()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "job-") {
			continue
		}
		in, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), in, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// 旧的单依赖记录（只有 has_dependency/dependency_id，没有有序依赖列表，
// 归档中同样没有 dependencies 字段）按同一含义判断：实际输入为原始序列
// 加唯一上游的总和时恢复为成功，追加数量不对时失败。
func TestReopenLegacySingleDependencyCorrespondence(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	// 上游作业 1：[2,-3]，成功，总和 -1。
	up := &storedJob{
		id: 1, submitter: "a", requestID: "up",
		values:          []int64{2, -3},
		effectiveValues: []int64{2, -3},
		queuedAt:        base, finishedAt: base.Add(time.Second),
		status: StatusSucceeded,
	}
	up.archive = newArchive(up, []int64{2, -3}, -1, 13, up.finishedAt)
	writeSyntheticRecord(t, dir, up)

	// 下游作业 2：旧格式单依赖记录——顶层与归档都不写 dependencies 字段。
	eff := []int64{5, -1}
	down := &storedJob{
		id: 2, submitter: "a", requestID: "down",
		values:          []int64{5},
		dependencies:    []uint64{1},
		effectiveValues: eff,
		queuedAt:        base.Add(2 * time.Second), finishedAt: base.Add(3 * time.Second),
		status: StatusSucceeded,
	}
	arc := newArchive(down, eff, 4, 26, down.finishedAt) // sum=5-1=4, sq=25+1=26
	arc.Dependencies = nil                               // 旧归档无此字段
	r := buildLegacyRecord(t, down, arc, eff)
	if err := os.WriteFile(filepath.Join(dir, jobFileName(2)), r, 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	g, err := s.Get(2)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusSucceeded || g.Archive == nil {
		t.Fatalf("legacy single-dependency record must stay succeeded: %s", g.Status)
	}
	if g.Archive.Sum != 4 || g.Archive.SumOfSquares != 26 {
		t.Fatalf("legacy result=%d,%d, want 4,26", g.Archive.Sum, g.Archive.SumOfSquares)
	}
	if len(g.Dependencies) != 1 || g.Dependencies[0] != 1 {
		t.Fatalf("legacy dependencies=%v, want [1]", g.Dependencies)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 同样的旧格式记录若漏掉唯一追加输入，仍按对应关系失败。
	dir2 := t.TempDir()
	writeSyntheticRecord(t, dir2, up)
	badEff := []int64{5} // 应有两个输入，只有原始序列
	arc2 := newArchive(down, badEff, 5, 25, down.finishedAt)
	arc2.Dependencies = nil
	r2 := buildLegacyRecord(t, down, arc2, badEff)
	if err := os.WriteFile(filepath.Join(dir2, jobFileName(2)), r2, 0o600); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir2)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	assertCorrespondenceFailure(t, s2, 2, []int64{5})
}

// buildLegacyRecord 构造旧格式单依赖成功记录：序列化后移除顶层与归档中的
// dependencies 字段，只保留 has_dependency/dependency_id。
func buildLegacyRecord(t *testing.T, j *storedJob, a *Archive, eff []int64) []byte {
	t.Helper()
	r := jobRecord{
		Version: recordVersion,
		ID:      j.id, Submitter: j.submitter, RequestID: j.requestID,
		Seed: j.seed, Values: append([]int64(nil), j.values...),
		HasDependency: true, DependencyID: 1,
		// Dependencies 刻意留空（omitempty 后落盘无此字段）。
		QueuedAt: j.queuedAt, StartedAt: j.startedAt, FinishedAt: j.finishedAt,
		Status:          StatusSucceeded,
		EffectiveValues: append([]int64(nil), eff...),
		Archive:         a,
	}
	out, err := json.MarshalIndent(&r, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// 上游的成功记录因输入对应关系错误被改判失败后，仍在排队等待它的下游
// 沿用既有依赖失败行为级联失败，不能继续用它的结果计算。
func TestReopenMismatchedUpstreamCascadesToQueuedDownstream(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	up := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{2, -3}})
	waitStatus(t, s, up.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	rewriteRecordEffectiveInputs(t, dir, up.ID, []int64{-3, 2})
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "down",
		values: []int64{5}, dependencies: []uint64{up.ID},
		queuedAt: base,
		status:   StatusQueued,
	})

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	g1, _ := s2.Get(up.ID)
	if g1.Status != StatusFailed {
		t.Fatalf("mismatched upstream: %s, want failed", g1.Status)
	}
	g2 := waitStatus(t, s2, 2, StatusFailed)
	if g2.BlockerID != up.ID || !strings.Contains(g2.FailureReason, "作业 1") {
		t.Fatalf("downstream must cascade from upstream failure: blocker=%d reason=%q",
			g2.BlockerID, g2.FailureReason)
	}
	if g2.Archive != nil {
		t.Fatal("downstream must not produce a success archive")
	}

	// 提交、查询入口继续可用；新作业正常处理。
	next := mustSubmit(t, s2, SubmitRequest{Submitter: "b", Values: []int64{1, 2, 3}})
	done := waitStatus(t, s2, next.ID, StatusSucceeded)
	if done.Archive.Sum != 6 || done.Archive.SumOfSquares != 14 {
		t.Fatalf("new job result=%d,%d", done.Archive.Sum, done.Archive.SumOfSquares)
	}
}

// 对应关系错误只把成功改判为失败；记录无法读取、格式非法或版本不受支持时
// 仍保留已有的打开失败行为，不静默当作失败作业继续服务。
func TestReopenUnreadableRecordsStillFailToOpen(t *testing.T) {
	t.Run("illegal json", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, jobFileName(7)), []byte("{not json"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(dir); err == nil || !strings.Contains(err.Error(), "损坏") {
			t.Fatalf("illegal json must fail Open with corruption error, got %v", err)
		}
	})
	t.Run("unsupported version", func(t *testing.T) {
		dir := t.TempDir()
		r := jobRecord{Version: "numeric-job-v999", ID: 7, Status: StatusSucceeded}
		out, err := json.Marshal(&r)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, jobFileName(7)), out, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(dir); err == nil || !strings.Contains(err.Error(), "版本不受支持") {
			t.Fatalf("unsupported version must fail Open, got %v", err)
		}
	})
	t.Run("unreadable file", func(t *testing.T) {
		// root 仍可读 0000 权限文件，无法模拟不可读，跳过。
		if os.Geteuid() == 0 {
			t.Skip("running as root")
		}
		dir := t.TempDir()
		p := filepath.Join(dir, jobFileName(7))
		if err := os.WriteFile(p, []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, 0o000); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(dir); err == nil {
			t.Fatal("unreadable record must fail Open")
		}
		_ = os.Chmod(p, 0o600) // 便于 TempDir 清理
	})
}
