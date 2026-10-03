package numeric

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// failArchiveWrite 注入一个只让指定作业“成功归档”那一次落盘失败的故障：
// 该作业以 succeeded 状态（携带归档）保存时返回错误，模拟无法创建临时记录或
// 无法替换原记录；其随后的失败记录以及所有其他作业、其他状态的写入照常成功。
func failArchiveWrite(s *Store, id uint64) {
	s.persistFault = func(j *storedJob) error {
		if j.id == id && j.status == StatusSucceeded && j.archive != nil {
			return errors.New("numeric(测试故障): 模拟结果归档写入失败")
		}
		return nil
	}
}

// 上游正常算出总和与平方和，但成功归档无法原子保存而最终失败时，所有仍在
// 排队、直接或间接等待它的作业必须立刻随之失败——无需任何新的提交、取消或
// 其他用户操作。失败信息遵循现有依赖规则，根因作业号指向最初归档写入失败的
// 作业；无关作业仍按原调度规则正常成功。
func TestArchiveWriteFailureCascadesWithoutFurtherAction(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, WithClock(newManualClock()))
	if err != nil {
		t.Fatal(err)
	}
	started, block, release, _, _ := gateCompute(t, s)
	block(1)

	up := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "up", Values: []int64{1, 2, 3}, Seed: 9,
	}) // 总和 6
	waitStarted(t, started, up.ID)
	failArchiveWrite(s, up.ID)

	child := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "child", Values: []int64{7},
		Dependencies: []uint64{up.ID},
	})
	grand := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "grand", Values: []int64{8},
		Dependencies: []uint64{child.ID},
	})
	// 与失败链无关、可独立运行的作业。
	indep := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "indep", Values: []int64{5},
	})

	// 放行后上游计算成功，但归档保存失败；此后不做任何提交/取消操作，
	// 仅靠 Get 轮询（只读，不触发级联）就应观察到失败沿链条传播。
	release(up.ID)

	u := waitStatus(t, s, up.ID, StatusFailed)
	if !strings.Contains(u.FailureReason, "结果归档写入失败") {
		t.Fatalf("upstream reason=%q, want 结果归档写入失败", u.FailureReason)
	}
	if u.Archive != nil || u.EffectiveValues != nil {
		t.Fatalf("upstream must keep no usable success archive: %+v", u)
	}
	if u.FinishedAt.IsZero() || u.BlockerID != 0 {
		t.Fatalf("upstream terminal fields wrong: finished=%s blocker=%d",
			u.FinishedAt, u.BlockerID)
	}
	// 作业号、提交参数与依赖关系原样保留。
	if u.ID != up.ID || u.RequestID != "up" || u.Seed != 9 ||
		len(u.Values) != 3 || u.Values[2] != 3 || len(u.Dependencies) != 0 {
		t.Fatalf("upstream params altered: %+v", u)
	}

	// 直接下游：指出直接阻断者就是上游，根因也是上游。
	c := waitStatus(t, s, child.ID, StatusFailed)
	if c.BlockerID != up.ID {
		t.Fatalf("child blocker=%d want upstream %d", c.BlockerID, up.ID)
	}
	if !strings.Contains(c.FailureReason, "直接上游作业 "+itoa(up.ID)) ||
		!strings.Contains(c.FailureReason, "阻断") {
		t.Fatalf("child must name direct upstream blocker: %q", c.FailureReason)
	}
	if strings.Contains(c.FailureReason, "阻断根因") {
		t.Fatalf("direct downstream must not add a distinct root: %q", c.FailureReason)
	}
	if c.Archive != nil || c.FinishedAt.IsZero() || !c.StartedAt.IsZero() {
		t.Fatalf("blocked child must never compute: started=%s finished=%s archive=%v",
			c.StartedAt, c.FinishedAt, c.Archive)
	}
	if c.ID != child.ID || c.RequestID != "child" || len(c.Dependencies) != 1 ||
		c.Dependencies[0] != up.ID || len(c.Values) != 1 || c.Values[0] != 7 {
		t.Fatalf("child params/deps altered: %+v", c)
	}

	// 间接下游：说明自己的直接上游（child），并通过根因作业号指回 up。
	g := waitStatus(t, s, grand.ID, StatusFailed)
	if g.BlockerID != up.ID {
		t.Fatalf("grand blocker=%d want root %d", g.BlockerID, up.ID)
	}
	if !strings.Contains(g.FailureReason, "直接上游作业 "+itoa(child.ID)) ||
		!strings.Contains(g.FailureReason, "阻断根因为作业 "+itoa(up.ID)) {
		t.Fatalf("grand must name direct upstream %d and root %d: %q",
			child.ID, up.ID, g.FailureReason)
	}
	if g.Archive != nil || g.FinishedAt.IsZero() {
		t.Fatalf("grand terminal state wrong: finished=%s archive=%v",
			g.FinishedAt, g.Archive)
	}

	// 不在失败链上的作业照常计算并成功归档。
	ind := waitStatus(t, s, indep.ID, StatusSucceeded)
	if ind.Archive == nil || ind.Archive.Sum != 5 {
		t.Fatalf("unrelated job must still succeed: %+v", ind)
	}

	// 按提交人列举也应看到传播后的同一状态，顺序仍为提交先后。
	list, err := s.List("a", time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	wantStatus := map[uint64]Status{
		up.ID: StatusFailed, child.ID: StatusFailed,
		grand.ID: StatusFailed, indep.ID: StatusSucceeded,
	}
	if len(list) != 4 {
		t.Fatalf("list len=%d want 4", len(list))
	}
	for i, j := range list {
		if j.ID != uint64(i+1) {
			t.Fatalf("list not in submission order: pos %d id %d", i, j.ID)
		}
		if j.Status != wantStatus[j.ID] {
			t.Fatalf("listed job %d status=%s want %s", j.ID, j.Status, wantStatus[j.ID])
		}
	}

	// 磁盘上：上游不得保留成功归档，失败链条均为 failed 记录。
	for _, id := range []uint64{up.ID, child.ID, grand.ID} {
		data, derr := os.ReadFile(filepath.Join(dir, jobFileName(id)))
		if derr != nil {
			t.Fatal(derr)
		}
		if !strings.Contains(string(data), `"status": "failed"`) ||
			strings.Contains(string(data), `"archive"`) {
			t.Fatalf("job %d disk record must be failed without archive:\n%s", id, data)
		}
	}

	// 重新打开同一归档：失败传播结果不丢失，上游依旧没有可用成功归档。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for _, want := range []struct {
		id      uint64
		blocker uint64
	}{
		{up.ID, 0}, {child.ID, up.ID}, {grand.ID, up.ID},
	} {
		j, _ := s2.Get(want.id)
		if j.Status != StatusFailed || j.BlockerID != want.blocker || j.Archive != nil {
			t.Fatalf("after reopen job %d: status=%s blocker=%d archive=%v",
				want.id, j.Status, j.BlockerID, j.Archive)
		}
	}
}

// 一个作业等待多个上游时，只要其中一个归档写入失败，就不能因为其他上游仍在
// 排队或运行而继续等待；阻断者按已提交依赖列表顺序选取当时已失败的上游，
// 并作为根因向更下游传播。
func TestArchiveWriteFailureOneOfMultipleUpstreamsBlocks(t *testing.T) {
	s, _ := openTestStore(t)
	started, block, release, _, _ := gateCompute(t, s)
	block(1, 2)

	upA := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "a", Values: []int64{1}})
	waitStarted(t, started, upA.ID)
	upB := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "b", Values: []int64{2}})
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{3},
		Dependencies: []uint64{upA.ID, upB.ID},
	})
	child := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "child", Values: []int64{4},
		Dependencies: []uint64{down.ID},
	})

	// upA 计算成功并归档成功；upB 计算成功但归档写入失败。
	failArchiveWrite(s, upB.ID)
	release(upA.ID)
	waitStatus(t, s, upA.ID, StatusSucceeded)
	release(upB.ID)
	waitStatus(t, s, upB.ID, StatusFailed)

	// down 不能因 upA 已成功、或“还在等 upB”而继续排队：立即失败，
	// 阻断者为列表中当时已失败的 upB。
	d := waitStatus(t, s, down.ID, StatusFailed)
	if d.BlockerID != upB.ID {
		t.Fatalf("down blocker=%d want failed-in-list-order upstream %d",
			d.BlockerID, upB.ID)
	}
	if !strings.Contains(d.FailureReason, "直接上游作业 "+itoa(upB.ID)) {
		t.Fatalf("down reason must name upB: %q", d.FailureReason)
	}
	// 更下游通过根因作业号指向 upB。
	c2 := waitStatus(t, s, child.ID, StatusFailed)
	if c2.BlockerID != upB.ID {
		t.Fatalf("child blocker=%d want root %d", c2.BlockerID, upB.ID)
	}
}
