package numeric

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 读取落盘记录中的 blocker_id，验证保存的失败记录与查询/列举保留相同根因。
func onDiskBlocker(t *testing.T, dir string, id uint64) uint64 {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, jobFileName(id)))
	if err != nil {
		t.Fatal(err)
	}
	var r jobRecord
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	return r.BlockerID
}

// 规格示例：作业 1、2、3 都曾成功归档，作业 4 排队等待作业 3。重新打开时
// 作业 1 因自身归档校验不通过变成失败，作业 2、3 的成功结果按既有规则不可用，
// 作业 4 也必须被阻断。作业 2、3、4 的 BlockerID 都为 1；失败原因分别指出
// 直接上游 1、2、3，对直接上游与根因不同的 3、4 同时说明根因是作业 1；
// 作业 1 自身仍属归档校验失败，BlockerID 保持 0 且保留其具体校验原因。
func TestReopenArchivedChainKeepsOriginalRootBlocker(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	j1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "j1", Values: []int64{2, -3}})
	waitStatus(t, s, j1.ID, StatusSucceeded) // 和 -1
	j2 := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "j2", Values: []int64{5},
		Dependencies: []uint64{j1.ID},
	})
	waitStatus(t, s, j2.ID, StatusSucceeded) // 实际输入 [5,-1]
	j3 := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "j3", Values: []int64{1},
		Dependencies: []uint64{j2.ID},
	})
	waitStatus(t, s, j3.ID, StatusSucceeded) // 实际输入 [1,4]
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 作业 1 的数值结果被篡改（全套字段自洽）：重开后它因自身复算校验失败。
	rewriteRecordNumbers(t, dir, j1.ID, 8, 13)

	// 作业 4 在上次关闭前仍排队等待作业 3（合成排队记录）。
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 4, submitter: "a", requestID: "j4",
		values: []int64{9}, dependencies: []uint64{j3.ID},
		queuedAt: base,
		status:   StatusQueued,
	})

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	g1, _ := s2.Get(j1.ID)
	if g1.Status != StatusFailed || g1.BlockerID != 0 {
		t.Fatalf("j1 自身归档校验失败且无阻断根因: status=%s blocker=%d", g1.Status, g1.BlockerID)
	}
	if !strings.Contains(g1.FailureReason, "归档数值结果") {
		t.Fatalf("j1 应保留具体校验失败原因: %q", g1.FailureReason)
	}

	g2, _ := s2.Get(j2.ID)
	g3, _ := s2.Get(j3.ID)
	g4 := waitStatus(t, s2, 4, StatusFailed)

	// 2、3、4 的 BlockerID 都沿链条保留为最初的根因作业 1。
	for _, g := range []*Job{g2, g3, g4} {
		if g.BlockerID != j1.ID {
			t.Fatalf("job %d blocker=%d want root %d (reason=%q)",
				g.ID, g.BlockerID, j1.ID, g.FailureReason)
		}
		if g.Archive != nil || g.EffectiveValues != nil {
			t.Fatalf("job %d 不得返回成功归档与实际输入", g.ID)
		}
	}
	// 失败原因分别指出直接上游 1、2、3。
	if !strings.Contains(g2.FailureReason, "直接上游作业 1") {
		t.Fatalf("j2 原因须指出直接上游 1: %q", g2.FailureReason)
	}
	if !strings.Contains(g3.FailureReason, "直接上游作业 2") ||
		!strings.Contains(g3.FailureReason, "阻断根因为作业 1") {
		t.Fatalf("j3 须指出直接上游 2 并说明根因 1: %q", g3.FailureReason)
	}
	if !strings.Contains(g4.FailureReason, "直接上游作业 3") ||
		!strings.Contains(g4.FailureReason, "阻断根因为作业 1") {
		t.Fatalf("j4 须指出直接上游 3 并说明根因 1: %q", g4.FailureReason)
	}

	// 按提交人列举与按作业号读取返回一致的归因。
	listed, err := s2.List("a", time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	wantBlocker := map[uint64]uint64{1: 0, 2: 1, 3: 1, 4: 1}
	got := 0
	for _, lj := range listed {
		want, ok := wantBlocker[lj.ID]
		if !ok {
			continue
		}
		got++
		if lj.Status != StatusFailed || lj.BlockerID != want {
			t.Fatalf("listed job %d status=%s blocker=%d want %d", lj.ID, lj.Status, lj.BlockerID, want)
		}
		if lj.Archive != nil || lj.EffectiveValues != nil {
			t.Fatalf("listed job %d 不得携带归档", lj.ID)
		}
	}
	if got != 4 {
		t.Fatalf("列举缺少作业: got %d of 4", got)
	}

	// 保存的失败记录保留相同根因；原作业号、提交参数及依赖次序不变。
	for id, want := range wantBlocker {
		if b := onDiskBlocker(t, dir, id); b != want {
			t.Fatalf("on-disk job %d blocker=%d want %d", id, b, want)
		}
	}
	g4v, _ := s2.Get(4)
	if g4v.ID != 4 || len(g4v.Values) != 1 || g4v.Values[0] != 9 ||
		len(g4v.Dependencies) != 1 || g4v.Dependencies[0] != 3 {
		t.Fatalf("j4 原作业号、提交参数或依赖次序被改动: %+v", g4v)
	}
}

// 多依赖作业因上游不可用而失败时，按保存的依赖顺序选取最靠前的不可用上游，
// 再沿该上游已有的阻断信息确定根因；其他上游作业号更小或失败原因不同都不能
// 改变这个选择。
func TestReopenMultiDependencyPicksEarliestUnavailableAndKeepsItsRoot(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// root → bad：bad 已成功归档但重开时因 root 失效而改判，blocker=root。
	root := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "root", Values: []int64{2, -3}})
	waitStatus(t, s, root.ID, StatusSucceeded)
	bad := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "bad", Values: []int64{5},
		Dependencies: []uint64{root.ID},
	})
	waitStatus(t, s, bad.ID, StatusSucceeded)
	// other 独立成功，作业号比 root 大，但位于依赖列表更靠后，不影响选择。
	other := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "other", Values: []int64{7}})
	waitStatus(t, s, other.ID, StatusSucceeded)
	// down 依赖顺序为 [other, bad]：最靠前的不可用上游是 bad（other 仍有效），
	// 根因须沿 bad 已有的阻断信息保留为 root，而不是 bad 本身。
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{1},
		Dependencies: []uint64{other.ID, bad.ID},
	})
	waitStatus(t, s, down.ID, StatusSucceeded)
	// child 排队等待 down，进一步验证根因跨已归档/排队边界传播。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	rewriteRecordNumbers(t, dir, root.ID, 8, 13)
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: down.ID + 100, submitter: "a", requestID: "child",
		values: []int64{2}, dependencies: []uint64{down.ID},
		queuedAt: base,
		status:   StatusQueued,
	})

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	gOther, _ := s2.Get(other.ID)
	if gOther.Status != StatusSucceeded {
		t.Fatalf("有效上游 other 不应被改判: %s", gOther.Status)
	}
	gBad, _ := s2.Get(bad.ID)
	if gBad.Status != StatusFailed || gBad.BlockerID != root.ID {
		t.Fatalf("bad blocker=%d want root %d", gBad.BlockerID, root.ID)
	}
	gDown, _ := s2.Get(down.ID)
	if gDown.Status != StatusFailed || gDown.BlockerID != root.ID {
		t.Fatalf("down 应选列表中最靠前的不可用上游 bad 并保留根因 root: blocker=%d want %d",
			gDown.BlockerID, root.ID)
	}
	if !strings.Contains(gDown.FailureReason, "直接上游作业 "+itoa(bad.ID)) {
		t.Fatalf("down 原因须指出直接上游 bad %d: %q", bad.ID, gDown.FailureReason)
	}
	gChild := waitStatus(t, s2, down.ID+100, StatusFailed)
	if gChild.BlockerID != root.ID {
		t.Fatalf("child blocker=%d want root %d", gChild.BlockerID, root.ID)
	}
	if !strings.Contains(gChild.FailureReason, "直接上游作业 "+itoa(down.ID)) ||
		!strings.Contains(gChild.FailureReason, "阻断根因为作业 "+itoa(root.ID)) {
		t.Fatalf("child 须指出直接上游 down 与根因 root: %q", gChild.FailureReason)
	}
}

// 多依赖记录中既保存了错误的追加值（对应位置上游仍有效）、又有更靠后的
// 上游不可用时：归因仍按“最靠前的不可用上游”确定根因，有效上游处的追加
// 值错误属于本作业归档问题，但不改变对不可用上游及其根因的选择。
func TestReopenUnavailableUpstreamOutranksValidPositionMismatch(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	root := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "root", Values: []int64{2, -3}})
	waitStatus(t, s, root.ID, StatusSucceeded)
	good := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "good", Values: []int64{10}})
	waitStatus(t, s, good.ID, StatusSucceeded) // 和 10
	bad := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "bad", Values: []int64{5},
		Dependencies: []uint64{root.ID},
	})
	waitStatus(t, s, bad.ID, StatusSucceeded)
	// down 的依赖顺序 [good, bad]：真实追加应为 [10, badSum]。
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{1},
		Dependencies: []uint64{good.ID, bad.ID},
	})
	done := waitStatus(t, s, down.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// root 失效：bad 重开后改判（blocker=root）。同时把 down 在 good 位置的
	// 追加值改错（good 本身仍有效）。
	rewriteRecordNumbers(t, dir, root.ID, 8, 13)
	eff := append([]int64(nil), done.Archive.EffectiveValues...)
	eff[1] = 999 // good 位置（原始 [1] 之后第一个追加位）
	rewriteRecordEffectiveInputs(t, dir, down.ID, eff)

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	gDown, _ := s2.Get(down.ID)
	if gDown.Status != StatusFailed || gDown.BlockerID != root.ID {
		t.Fatalf("down 须选最靠前的不可用上游 bad 并保留根因 root: status=%s blocker=%d",
			gDown.Status, gDown.BlockerID)
	}
	if !strings.Contains(gDown.FailureReason, "作业 "+itoa(bad.ID)) {
		t.Fatalf("down 原因须指出不可用上游 bad: %q", gDown.FailureReason)
	}
	gGood, _ := s2.Get(good.ID)
	if gGood.Status != StatusSucceeded {
		t.Fatalf("有效上游 good 不得被改判: %s", gGood.Status)
	}
}

// 已成功归档的作业，其直接上游记录缺失时该作业以缺失的作业号为根因，并明确
// 说明结果无法使用；排队等待它的下游继续把同一缺失作业号作为根因。
func TestReopenMissingUpstreamRecordBecomesRoot(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	up := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "up", Values: []int64{4}})
	waitStatus(t, s, up.ID, StatusSucceeded)
	mid := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "mid", Values: []int64{1},
		Dependencies: []uint64{up.ID},
	})
	waitStatus(t, s, mid.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(filepath.Join(dir, jobFileName(up.ID))); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: mid.ID + 100, submitter: "a", requestID: "down",
		values: []int64{9}, dependencies: []uint64{mid.ID},
		queuedAt: base,
		status:   StatusQueued,
	})

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	gMid, _ := s2.Get(mid.ID)
	if gMid.Status != StatusFailed || gMid.BlockerID != up.ID {
		t.Fatalf("mid blocker=%d want missing upstream %d", gMid.BlockerID, up.ID)
	}
	if !strings.Contains(gMid.FailureReason, "作业 "+itoa(up.ID)) ||
		!strings.Contains(gMid.FailureReason, "成功结果不可用") {
		t.Fatalf("mid 须说明缺失上游的结果无法使用: %q", gMid.FailureReason)
	}
	gDown := waitStatus(t, s2, mid.ID+100, StatusFailed)
	if gDown.BlockerID != up.ID {
		t.Fatalf("down blocker=%d want missing root %d", gDown.BlockerID, up.ID)
	}
	if !strings.Contains(gDown.FailureReason, "直接上游作业 "+itoa(mid.ID)) ||
		!strings.Contains(gDown.FailureReason, "阻断根因为作业 "+itoa(up.ID)) {
		t.Fatalf("down 须指出直接上游 mid 与缺失根因 up: %q", gDown.FailureReason)
	}
}
