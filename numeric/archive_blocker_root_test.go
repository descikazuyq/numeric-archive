package numeric

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 重开归档时的根因归因：作业 1、依赖它的作业 2、依赖作业 2 的作业 3 都曾
// 成功归档，作业 4 仍排队等待作业 3。重开目录时作业 1 因自身归档校验不通过
// 变成失败，作业 2、3 的成功结果按既有规则不可用，作业 4 也被阻断。
//
// 此时作业 2、3、4 的 BlockerID 都必须是最初的根因作业 1；失败原因分别指出
// 直接上游 1、2、3，对直接上游与根因不同的情况（作业 3、4）同时说明根因是
// 作业 1。作业 1 自身属于归档校验失败，BlockerID 保持 0，其具体校验失败
// 原因保留。按作业号读取与按提交人列举、以及落盘记录都必须给出一致的根因。
func TestReopenDependencyFailureKeepsOriginalRootAcrossArchivedChain(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	j1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "j1", Values: []int64{2, -3}}) // 和 -1
	waitStatus(t, s, j1.ID, StatusSucceeded)
	j2 := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "j2", Values: []int64{5},
		Dependencies: []uint64{j1.ID},
	}) // 实际输入 [5,-1]
	waitStatus(t, s, j2.ID, StatusSucceeded)
	j3 := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "j3", Values: []int64{1},
		Dependencies: []uint64{j2.ID},
	}) // 实际输入 [1,4]
	waitStatus(t, s, j3.ID, StatusSucceeded)
	b2, _ := s.Get(j2.ID)
	b3, _ := s.Get(j3.ID)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 作业 1 的数值结果被改成另一套自洽数字：重开后它因自身复算校验失败。
	rewriteRecordNumbers(t, dir, j1.ID, 8, 13)
	// 作业 4：关闭后仍是排队等待作业 3 的状态。
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 4, submitter: "a", requestID: "j4",
		values: []int64{9}, dependencies: []uint64{j3.ID},
		queuedAt: base.Add(time.Minute),
		status:   StatusQueued,
	})

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	// 作业 1：自身归档校验失败，BlockerID 保持 0，具体校验原因保留。
	g1, _ := s2.Get(j1.ID)
	if g1.Status != StatusFailed {
		t.Fatalf("j1 must fail its own archive validation, got %s", g1.Status)
	}
	if g1.BlockerID != 0 {
		t.Fatalf("j1 blocker=%d want 0 (self archive error)", g1.BlockerID)
	}
	if !strings.Contains(g1.FailureReason, "归档数值结果") ||
		!strings.Contains(g1.FailureReason, "成功结果不可用") {
		t.Fatalf("j1 must keep its specific validation reason, got %q", g1.FailureReason)
	}
	if g1.Archive != nil || g1.EffectiveValues != nil {
		t.Fatalf("j1 must carry no archive/effective inputs")
	}

	// 作业 2：直接上游就是根因作业 1，BlockerID=1，原因不再单列根因。
	g2, _ := s2.Get(j2.ID)
	if g2.Status != StatusFailed || g2.BlockerID != 1 {
		t.Fatalf("j2 status=%s blocker=%d want failed/1", g2.Status, g2.BlockerID)
	}
	if !strings.Contains(g2.FailureReason, "直接上游作业 1") ||
		!strings.Contains(g2.FailureReason, "无法使用") {
		t.Fatalf("j2 reason must name direct upstream 1 unusable, got %q", g2.FailureReason)
	}
	if strings.Contains(g2.FailureReason, "根因") {
		t.Fatalf("j2 direct upstream is the root; reason must not add a distinct root: %q", g2.FailureReason)
	}
	assertOriginalParamsPreserved(t, b2, g2)

	// 作业 3：直接上游是作业 2，根因仍是作业 1。
	g3, _ := s2.Get(j3.ID)
	if g3.Status != StatusFailed || g3.BlockerID != 1 {
		t.Fatalf("j3 status=%s blocker=%d want failed/1", g3.Status, g3.BlockerID)
	}
	if !strings.Contains(g3.FailureReason, "直接上游作业 2") ||
		!strings.Contains(g3.FailureReason, "根因为作业 1") {
		t.Fatalf("j3 reason must name direct upstream 2 and root 1, got %q", g3.FailureReason)
	}
	assertOriginalParamsPreserved(t, b3, g3)

	// 作业 4：排队等待作业 3，级联失败；直接上游是作业 3，根因仍是作业 1。
	g4 := waitStatus(t, s2, 4, StatusFailed)
	if g4.BlockerID != 1 {
		t.Fatalf("j4 blocker=%d want root 1", g4.BlockerID)
	}
	if !strings.Contains(g4.FailureReason, "直接上游作业 3") ||
		!strings.Contains(g4.FailureReason, "根因为作业 1") {
		t.Fatalf("j4 reason must name direct upstream 3 and root 1, got %q", g4.FailureReason)
	}
	if g4.Archive != nil || g4.EffectiveValues != nil {
		t.Fatal("blocked queued job must never carry an archive or effective inputs")
	}
	if len(g4.Dependencies) != 1 || g4.Dependencies[0] != 3 || len(g4.Values) != 1 || g4.Values[0] != 9 {
		t.Fatalf("j4 original params/deps altered: %+v", g4)
	}

	// 按提交人列举必须返回与按作业号读取一致的状态与根因，且按提交先后排列。
	listed, err := s2.List("a", time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 4 || listed[0].ID != 1 || listed[1].ID != 2 || listed[2].ID != 3 || listed[3].ID != 4 {
		t.Fatalf("listed order/ids wrong: %v", listIDs(listed))
	}
	wantBlocker := map[uint64]uint64{1: 0, 2: 1, 3: 1, 4: 1}
	for _, lj := range listed {
		if lj.Status != StatusFailed || lj.BlockerID != wantBlocker[lj.ID] || lj.Archive != nil {
			t.Fatalf("listed job %d status=%s blocker=%d archive=%v",
				lj.ID, lj.Status, lj.BlockerID, lj.Archive)
		}
	}

	// 落盘记录同样保留相同根因，且不再含成功归档。
	for _, id := range []uint64{2, 3, 4} {
		data, derr := os.ReadFile(filepath.Join(dir, jobFileName(id)))
		if derr != nil {
			t.Fatal(derr)
		}
		text := string(data)
		if !strings.Contains(text, `"status": "failed"`) ||
			!strings.Contains(text, `"blocker_id": 1`) ||
			strings.Contains(text, `"archive"`) {
			t.Fatalf("on-disk job %d must be failed with blocker 1 and no archive:\n%s", id, text)
		}
	}
}

// assertOriginalParamsPreserved 校验改判失败后作业号、提交人、请求号、种子、
// 原始序列与依赖次序均未改变，且不返回成功归档与实际输入。
func assertOriginalParamsPreserved(t *testing.T, before, after *Job) {
	t.Helper()
	if after.Archive != nil || after.EffectiveValues != nil {
		t.Fatalf("job %d must carry no archive/effective inputs", after.ID)
	}
	if after.ID != before.ID || after.Submitter != before.Submitter ||
		after.RequestID != before.RequestID || after.Seed != before.Seed {
		t.Fatalf("job identity changed: before=%+v after=%+v", before, after)
	}
	if len(after.Values) != len(before.Values) {
		t.Fatalf("job %d original values changed: %v, want %v", after.ID, after.Values, before.Values)
	}
	for i := range before.Values {
		if after.Values[i] != before.Values[i] {
			t.Fatalf("job %d original values changed: %v, want %v", after.ID, after.Values, before.Values)
		}
	}
	if len(after.Dependencies) != len(before.Dependencies) {
		t.Fatalf("job %d dependency order changed: %v, want %v", after.ID, after.Dependencies, before.Dependencies)
	}
	for i := range before.Dependencies {
		if after.Dependencies[i] != before.Dependencies[i] {
			t.Fatalf("job %d dependency order changed: %v, want %v", after.ID, after.Dependencies, before.Dependencies)
		}
	}
}

// 多依赖作业因上游不可用而失败时，仍按保存的依赖顺序选取最靠前的不可用
// 上游，再沿该上游已有的阻断信息确定根因；其他上游作业号更小或失败原因
// 不同都不能改变这个选择。
func TestReopenMultiDependencyPicksEarliestUnusableAndKeepsItsRoot(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// root（作业 1）→ mid（作业 2，成功归档）；另有一个独立的、作业号更小
	// 维度上不会出现——改用两个独立上游 uA/uB，其中 uB 沿链条带有根因。
	root := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "root", Values: []int64{2, -3}}) // 和 -1
	waitStatus(t, s, root.ID, StatusSucceeded)
	uA := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "ua", Values: []int64{10}}) // 和 10，独立有效
	waitStatus(t, s, uA.ID, StatusSucceeded)
	// mid 依赖 root，成功归档；重开时随 root 失效，根因为 root。
	mid := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "mid", Values: []int64{5},
		Dependencies: []uint64{root.ID},
	})
	waitStatus(t, s, mid.ID, StatusSucceeded)
	// down 的依赖列表把 uA 放在 mid 前面，但 uA 重开后仍有效；不可用的是
	// 列表中的 mid，其根因是 root。直接阻断者按列表顺序应为 mid，根因 root。
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{7},
		Dependencies: []uint64{uA.ID, mid.ID},
	})
	waitStatus(t, s, down.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	rewriteRecordNumbers(t, dir, root.ID, 8, 13)

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	gA, _ := s2.Get(uA.ID)
	if gA.Status != StatusSucceeded || gA.Archive == nil {
		t.Fatalf("valid upstream uA must stay succeeded, got %s", gA.Status)
	}
	gMid, _ := s2.Get(mid.ID)
	if gMid.Status != StatusFailed || gMid.BlockerID != root.ID {
		t.Fatalf("mid status=%s blocker=%d want failed/root %d", gMid.Status, gMid.BlockerID, root.ID)
	}
	gDown, _ := s2.Get(down.ID)
	if gDown.Status != StatusFailed {
		t.Fatalf("down must fail, got %s", gDown.Status)
	}
	if gDown.BlockerID != root.ID {
		t.Fatalf("down blocker=%d want root %d carried via mid", gDown.BlockerID, root.ID)
	}
	if !strings.Contains(gDown.FailureReason, "直接上游作业 "+itoa(mid.ID)) ||
		!strings.Contains(gDown.FailureReason, "根因为作业 "+itoa(root.ID)) {
		t.Fatalf("down reason must name earliest unusable mid and root, got %q", gDown.FailureReason)
	}
}

// 直接上游的记录缺失时，以这个缺失的作业号作为根因，并明确说明结果无法使用；
// 已归档的下游与排队的下游都遵守同一规则。
func TestReopenMissingUpstreamRecordIsItsOwnRoot(t *testing.T) {
	t.Run("archived dependent", func(t *testing.T) {
		dir := t.TempDir()
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		up := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "up", Values: []int64{4}})
		waitStatus(t, s, up.ID, StatusSucceeded)
		down := mustSubmit(t, s, SubmitRequest{
			Submitter: "a", RequestID: "down", Values: []int64{1},
			Dependencies: []uint64{up.ID},
		})
		waitStatus(t, s, down.ID, StatusSucceeded)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(dir, jobFileName(up.ID))); err != nil {
			t.Fatal(err)
		}
		s2, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s2.Close()
		g, _ := s2.Get(down.ID)
		if g.Status != StatusFailed || g.BlockerID != up.ID {
			t.Fatalf("down status=%s blocker=%d want failed/missing %d", g.Status, g.BlockerID, up.ID)
		}
		if !strings.Contains(g.FailureReason, "作业 "+itoa(up.ID)) ||
			!strings.Contains(g.FailureReason, "成功结果不可用") {
			t.Fatalf("reason must name missing upstream and state unusable, got %q", g.FailureReason)
		}
	})

	t.Run("queued dependent", func(t *testing.T) {
		dir := t.TempDir()
		base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
		// 只写一条排队记录，引用缺失的作业 7。
		writeSyntheticRecord(t, dir, &storedJob{
			id: 8, submitter: "a", requestID: "q",
			values: []int64{1}, dependencies: []uint64{7},
			queuedAt: base,
			status:   StatusQueued,
		})
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		g := waitStatus(t, s, 8, StatusFailed)
		if g.BlockerID != 7 {
			t.Fatalf("blocker=%d want missing upstream 7", g.BlockerID)
		}
		if !strings.Contains(g.FailureReason, "作业 7") ||
			!strings.Contains(g.FailureReason, "成功结果不可用") {
			t.Fatalf("reason must name missing upstream 7 as root, got %q", g.FailureReason)
		}
	})
}

// 上游归档全部有效，只是本作业保存的追加输入与上游总和不符：属于本作业自身
// 归档有误，BlockerID 保持 0，不能把有效上游判成失败或归因给它。
func TestReopenAppendedValueMismatchWithValidUpstreamsKeepsBlockerZero(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{1, 2}})  // 和 3
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{1, -3}}) // 和 -2
	waitStatus(t, s, u1.ID, StatusSucceeded)
	waitStatus(t, s, u2.ID, StatusSucceeded)
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{10}, Seed: 9,
		Dependencies: []uint64{u2.ID, u1.ID}, // 正确实际输入 [10,-2,3]
	})
	waitStatus(t, s, down.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 追加部分调换：上游都有效，只是本作业保存的追加输入对不上。
	rewriteRecordEffectiveInputs(t, dir, down.ID, []int64{10, 3, -2})

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	g, _ := s2.Get(down.ID)
	if g.Status != StatusFailed {
		t.Fatalf("down must fail, got %s", g.Status)
	}
	if g.BlockerID != 0 {
		t.Fatalf("own archive mismatch blocker=%d want 0 (must not blame valid upstream)", g.BlockerID)
	}
	if !strings.Contains(g.FailureReason, "无法对应") ||
		!strings.Contains(g.FailureReason, "成功结果不可用") {
		t.Fatalf("reason must state appended values cannot correspond, got %q", g.FailureReason)
	}
	// 有效上游不受下游归档错误影响。
	for _, id := range []uint64{u1.ID, u2.ID} {
		gu, _ := s2.Get(id)
		if gu.Status != StatusSucceeded || gu.Archive == nil {
			t.Fatalf("valid upstream %d must stay succeeded, got %s", id, gu.Status)
		}
	}
}
