package numeric

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeForgedInputRecord 落一条成功记录：原始序列为 values、直接上游为 deps，
// 但实际输入换成 effective，并按这份实际输入重算总和、平方和、日志、摘要与
// 校验值，使归档全套字段与实际输入自洽——模拟“输入被替换但内容一致”的记录。
func writeForgedInputRecord(t *testing.T, dir string, id uint64, values, effective []int64, deps []uint64) {
	t.Helper()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	j := &storedJob{
		id:              id,
		submitter:       "a",
		requestID:       "r1",
		values:          values,
		dependencies:    deps,
		effectiveValues: effective,
		queuedAt:        base,
		startedAt:       base.Add(time.Second),
		finishedAt:      base.Add(2 * time.Second),
		status:          StatusSucceeded,
	}
	sum, sumSquares, _, ok := computeResult(effective, j.seed, nil)
	if !ok {
		t.Fatalf("forged effective input %v overflows", effective)
	}
	j.archive = newArchive(j, effective, sum, sumSquares, j.finishedAt)
	writeSyntheticRecord(t, dir, j)
}

// 实际输入与原始参数或依赖数量不符的成功记录，即使总和、平方和、日志、摘要与
// 校验值全部与这份实际输入自洽，重开后也必须按损坏归档处理：状态失败、
// 不带归档与实际输入，原作业号与原始提交参数保留，落盘记录同样改写为失败。
func TestReopenRejectsMismatchedEffectiveInputs(t *testing.T) {
	cases := []struct {
		name      string
		values    []int64
		effective []int64
		deps      []uint64
	}{
		// 次序被调换：总和与平方和都与原序列相同，只有逐项按次序比较才能识别。
		{"swapped order", []int64{2, -3}, []int64{-3, 2}, nil},
		// 无依赖却多出未记录的追加输入。
		{"extra input without dependency", []int64{5}, []int64{5, 7}, nil},
		// 原始部分被截短。
		{"truncated original", []int64{2, -3}, []int64{2}, nil},
		// 原始部分被同长度替换（含零与重复整数，按位置判断）。
		{"replaced position", []int64{0, -4, -4}, []int64{0, -4, 0}, nil},
		// 有一个直接上游，实际输入却没有追加上游总和。
		{"missing dependency append", []int64{5}, []int64{5}, []uint64{1}},
		// 有一个直接上游，实际输入却追加了两份。
		{"extra dependency append", []int64{5}, []int64{5, 7, 7}, []uint64{1}},
		// 依赖追加位置顶替了原始序列的末位。
		{"dependency replaces original tail", []int64{2, -3}, []int64{2, 7}, []uint64{1}},
		// 原始序列为空的成功记录不合法。
		{"empty original", nil, []int64{1}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeForgedInputRecord(t, dir, 1, tc.values, tc.effective, tc.deps)

			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			g, err := s.Get(1)
			if err != nil {
				t.Fatal(err)
			}
			if g.Status != StatusFailed {
				t.Fatalf("mismatched effective input must fail closed, got %s", g.Status)
			}
			if g.Archive != nil || g.EffectiveValues != nil {
				t.Fatalf("failed record must not carry archive/effective inputs: archive=%v effective=%v",
					g.Archive, g.EffectiveValues)
			}
			if !strings.Contains(g.FailureReason, "实际输入与原始参数或依赖数量不符") ||
				!strings.Contains(g.FailureReason, "成功结果不可用") {
				t.Fatalf("reason=%q must state input mismatch and success results unusable", g.FailureReason)
			}
			// 原作业号与原始提交参数保留。
			if g.ID != 1 || g.Submitter != "a" || g.RequestID != "r1" {
				t.Fatalf("identity changed: id=%d submitter=%q request=%q", g.ID, g.Submitter, g.RequestID)
			}
			if len(g.Values) != len(tc.values) {
				t.Fatalf("original values=%v, want %v", g.Values, tc.values)
			}
			for i := range tc.values {
				if g.Values[i] != tc.values[i] {
					t.Fatalf("original values=%v, want %v", g.Values, tc.values)
				}
			}
			// 落盘记录同样已被改写为失败，不能继续声称成功。
			data, err := os.ReadFile(filepath.Join(dir, jobFileName(1)))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(data), `"status": "failed"`) {
				t.Fatal("on-disk record must be rewritten as failed")
			}
		})
	}
}

// 输入对应关系错误的成功记录重开失败后，仍在排队的下游沿用既有依赖失败行为
// 级联失败，不能继续用它的结果计算。
func TestReopenMismatchedInputFailsQueuedDownstream(t *testing.T) {
	dir := t.TempDir()
	// 上游：原始 [2,-3]，实际输入被换成 [-3,2]（总和、平方和均不变）。
	writeForgedInputRecord(t, dir, 1, []int64{2, -3}, []int64{-3, 2}, nil)
	// 仍在排队等待它的下游。
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "r2",
		values: []int64{5}, dependencies: []uint64{1},
		queuedAt: base,
		status:   StatusQueued,
	})

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	g1, err := s.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if g1.Status != StatusFailed {
		t.Fatalf("mismatched upstream: %s, want failed", g1.Status)
	}
	g2 := waitStatus(t, s, 2, StatusFailed)
	if g2.BlockerID != 1 || !strings.Contains(g2.FailureReason, "作业 1") {
		t.Fatalf("downstream must cascade from upstream failure: blocker=%d reason=%q",
			g2.BlockerID, g2.FailureReason)
	}
	if g2.Archive != nil {
		t.Fatal("downstream must not produce a success archive")
	}
}

// 合法的输入对应关系不被误判：无依赖时实际输入与原始序列完全相同，
// 有依赖时原始部分完整保留、每个直接上游恰好追加一个位置——
// 含负数、零与重复整数，重开后保持原结果、摘要、日志与校验值。
func TestReopenKeepsMatchingEffectiveInputs(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{2, -3}})
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{0, -4, -4}})
	waitStatus(t, s, u1.ID, StatusSucceeded) // sum=-1
	waitStatus(t, s, u2.ID, StatusSucceeded) // sum=-8
	plain := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "plain", Values: []int64{0, -4, -4, 0}})
	dep := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "dep", Values: []int64{2, -3},
		Dependencies: []uint64{u1.ID, u2.ID},
	})
	plainDone := waitStatus(t, s, plain.ID, StatusSucceeded)
	depDone := waitStatus(t, s, dep.ID, StatusSucceeded)
	// 无依赖：实际输入与原始序列完全相同。
	if len(plainDone.EffectiveValues) != 4 ||
		plainDone.EffectiveValues[0] != 0 || plainDone.EffectiveValues[1] != -4 ||
		plainDone.EffectiveValues[2] != -4 || plainDone.EffectiveValues[3] != 0 {
		t.Fatalf("plain effective=%v, want [0 -4 -4 0]", plainDone.EffectiveValues)
	}
	// 多依赖：原始部分完整保留，末尾按列表顺序各追加一个上游总和。
	wantEffective := []int64{2, -3, -1, -8}
	if len(depDone.EffectiveValues) != len(wantEffective) {
		t.Fatalf("dep effective=%v, want %v", depDone.EffectiveValues, wantEffective)
	}
	for i := range wantEffective {
		if depDone.EffectiveValues[i] != wantEffective[i] {
			t.Fatalf("dep effective=%v, want %v", depDone.EffectiveValues, wantEffective)
		}
	}
	plainChecksum := plainDone.Archive.Checksum
	depChecksum := depDone.Archive.Checksum
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for _, tc := range []struct {
		id       uint64
		checksum string
	}{
		{plain.ID, plainChecksum},
		{dep.ID, depChecksum},
	} {
		g, err := s2.Get(tc.id)
		if err != nil {
			t.Fatal(err)
		}
		if g.Status != StatusSucceeded || g.Archive == nil {
			t.Fatalf("job %d: valid archive must survive reopen: %s", tc.id, g.Status)
		}
		if g.Archive.Checksum != tc.checksum {
			t.Fatalf("job %d: archive changed across reopen: %+v", tc.id, g.Archive)
		}
	}
}

// 旧格式单依赖记录（只有 has_dependency/dependency_id，没有 dependencies 字段）
// 按同一含义判断：原始部分完整且恰好追加一个上游总和时保持成功，
// 对应关系错误时同样失败。
func TestReopenLegacySingleDependencyInputMatch(t *testing.T) {
	writeLegacyRecord := func(t *testing.T, dir string, id uint64, values, effective []int64, depID uint64) {
		t.Helper()
		base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
		j := &storedJob{
			id:              id,
			submitter:       "a",
			requestID:       "r1",
			values:          values,
			dependencies:    []uint64{depID},
			effectiveValues: effective,
			queuedAt:        base,
			startedAt:       base.Add(time.Second),
			finishedAt:      base.Add(2 * time.Second),
			status:          StatusSucceeded,
		}
		sum, sumSquares, _, ok := computeResult(effective, j.seed, nil)
		if !ok {
			t.Fatalf("effective input %v overflows", effective)
		}
		j.archive = newArchive(j, effective, sum, sumSquares, j.finishedAt)
		data, err := encodeRecord(j)
		if err != nil {
			t.Fatal(err)
		}
		// 抹掉新格式字段，模拟旧版写出的单依赖记录。
		var r map[string]any
		if err := json.Unmarshal(data, &r); err != nil {
			t.Fatal(err)
		}
		delete(r, "dependencies")
		if a, ok := r["archive"].(map[string]any); ok {
			delete(a, "dependencies")
		}
		out, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, jobFileName(id)), out, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("valid legacy record survives", func(t *testing.T) {
		dir := t.TempDir()
		// 上游作业 1：sum=-1。
		writeForgedInputRecord(t, dir, 1, []int64{2, -3}, []int64{2, -3}, nil)
		// 旧格式单依赖记录：原始 [5]，实际输入 [5,-1]。
		writeLegacyRecord(t, dir, 2, []int64{5}, []int64{5, -1}, 1)

		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		g, err := s.Get(2)
		if err != nil {
			t.Fatal(err)
		}
		if g.Status != StatusSucceeded || g.Archive == nil {
			t.Fatalf("valid legacy dependent record must survive reopen: %s", g.Status)
		}
		if g.Archive.Sum != 4 || g.Archive.SumOfSquares != 26 {
			t.Fatalf("legacy result=%d,%d, want 4,26", g.Archive.Sum, g.Archive.SumOfSquares)
		}
	})

	t.Run("mismatched legacy record fails", func(t *testing.T) {
		dir := t.TempDir()
		writeForgedInputRecord(t, dir, 1, []int64{2, -3}, []int64{2, -3}, nil)
		// 旧格式单依赖记录：实际输入缺少追加的上游总和。
		writeLegacyRecord(t, dir, 2, []int64{5, -1}, []int64{5, -1}, 1)

		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		g, err := s.Get(2)
		if err != nil {
			t.Fatal(err)
		}
		if g.Status != StatusFailed || g.Archive != nil || g.EffectiveValues != nil {
			t.Fatalf("mismatched legacy record must fail closed: %s archive=%v", g.Status, g.Archive)
		}
		if !strings.Contains(g.FailureReason, "成功结果不可用") {
			t.Fatalf("reason=%q must state success results unusable", g.FailureReason)
		}
	})
}
