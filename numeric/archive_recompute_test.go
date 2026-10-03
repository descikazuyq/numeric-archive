package numeric

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// rewriteRecordNumbers 把磁盘上成功记录的数值结果改为给定值，并按这些错误
// 数字重算结果摘要、计算日志与校验值，使归档全套字段与所写数字自洽——
// 模拟“数字写错但日志/摘要/校验值一致”的记录。
func rewriteRecordNumbers(t *testing.T, dir string, id uint64, sum, sumSquares int64) {
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
	r.Archive.Sum = sum
	r.Archive.SumOfSquares = sumSquares
	r.Archive.ResultDigest = resultDigestHex(r.EffectiveValues, r.Seed, sum, sumSquares)
	r.Archive.Log = buildLog(r.EffectiveValues, r.Seed, sum, sumSquares)
	r.Archive.Checksum = checksumHex(r.EffectiveValues, r.Seed, sum, sumSquares, r.Archive.Log, r.Archive.ResultDigest)
	out, err := json.MarshalIndent(&r, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, out, 0o600); err != nil {
		t.Fatal(err)
	}
}

// 成功归档的总和或平方和写错时，即使日志、结果摘要与校验值都按错误数字
// 保持一致，重开后也必须按损坏归档处理：状态失败、不带归档与实际输入。
func TestReopenRejectsForgedConsistentNumbers(t *testing.T) {
	cases := []struct {
		name            string
		sum, sumSquares int64
	}{
		{"wrong sum", 8, 13},             // 实际输入 [2,-3]：总和应为 -1
		{"wrong sum of squares", -1, 12}, // 平方和应为 13
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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

			rewriteRecordNumbers(t, dir, j.ID, tc.sum, tc.sumSquares)

			s2, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s2.Close()
			g, err := s2.Get(j.ID)
			if err != nil {
				t.Fatal(err)
			}
			if g.Status != StatusFailed {
				t.Fatalf("forged numbers must fail closed, got %s", g.Status)
			}
			if g.Archive != nil || g.EffectiveValues != nil {
				t.Fatalf("failed record must not carry archive/effective inputs: archive=%v effective=%v",
					g.Archive, g.EffectiveValues)
			}
			if !strings.Contains(g.FailureReason, "成功结果不可用") {
				t.Fatalf("reason=%q must state success results unusable", g.FailureReason)
			}
			// 落盘记录同样已被改写为失败，不能继续声称成功。
			data, err := os.ReadFile(filepath.Join(dir, jobFileName(j.ID)))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(data), `"status": "failed"`) {
				t.Fatal("on-disk record must be rewritten as failed")
			}
		})
	}
}

// 实际输入按既有规则会造成 int64 溢出的记录，即使归档写着回绕后的数字且
// 全套字段自洽，也不能保留成功状态。
func TestReopenRejectsOverflowWrappedNumbers(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	eff := []int64{math.MaxInt64, 1} // 总和溢出；回绕值为 math.MinInt64
	j := &storedJob{
		id: 1, submitter: "a", requestID: "r1",
		values:          eff,
		effectiveValues: eff,
		queuedAt:        base,
		startedAt:       base.Add(time.Second),
		finishedAt:      base.Add(2 * time.Second),
		status:          StatusSucceeded,
	}
	j.archive = newArchive(j, eff, math.MinInt64, 1, j.finishedAt)
	writeSyntheticRecord(t, dir, j)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	g, err := s.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusFailed || g.Archive != nil || g.EffectiveValues != nil {
		t.Fatalf("overflow record must fail closed: %s archive=%v", g.Status, g.Archive)
	}
	if !strings.Contains(g.FailureReason, "成功结果不可用") {
		t.Fatalf("reason=%q must state success results unusable", g.FailureReason)
	}
}

// 依赖作业的成功归档数值被篡改时，重开后该作业失败，仍在排队的下游
// 沿用既有上游失败处理级联失败，不能继续使用错误总和计算。
func TestReopenForgedUpstreamFailsQueuedDownstream(t *testing.T) {
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

	rewriteRecordNumbers(t, dir, up.ID, 8, 13)
	// 仍在排队的下游（多依赖，含一个无关上游的位置）：直接落一条排队记录。
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 2, submitter: "a", requestID: "r2",
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
		t.Fatalf("forged upstream: %s, want failed", g1.Status)
	}
	g2 := waitStatus(t, s2, 2, StatusFailed)
	if g2.BlockerID != up.ID || !strings.Contains(g2.FailureReason, "作业 1") {
		t.Fatalf("downstream must cascade from upstream failure: blocker=%d reason=%q",
			g2.BlockerID, g2.FailureReason)
	}
	if g2.Archive != nil {
		t.Fatal("downstream must not produce a success archive")
	}
}

// 有效归档（含依赖追加后的实际输入）重开后原样可用：数值检查以保存的
// 实际输入为准，不多算也不少算。
func TestReopenKeepsValidDependentArchive(t *testing.T) {
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
	// 实际输入 [5,-1,10]：sum=14，sq=25+1+100=126。
	if done.Archive.Sum != 14 || done.Archive.SumOfSquares != 126 {
		t.Fatalf("dependent result=%d,%d", done.Archive.Sum, done.Archive.SumOfSquares)
	}
	wantChecksum := done.Archive.Checksum
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	g, err := s2.Get(down.ID)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusSucceeded || g.Archive == nil {
		t.Fatalf("valid dependent archive must survive reopen: %s", g.Status)
	}
	if g.Archive.Sum != 14 || g.Archive.SumOfSquares != 126 || g.Archive.Checksum != wantChecksum {
		t.Fatalf("archive changed across reopen: %+v", g.Archive)
	}
	if len(g.EffectiveValues) != 3 || g.EffectiveValues[0] != 5 ||
		g.EffectiveValues[1] != -1 || g.EffectiveValues[2] != 10 {
		t.Fatalf("effective values=%v, want [5 -1 10]", g.EffectiveValues)
	}
}
