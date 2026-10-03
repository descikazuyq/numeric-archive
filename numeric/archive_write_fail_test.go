package numeric

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// 上游计算成功但成功归档无法落盘（目录暂时不可写）时，上游以“结果归档写入失败”
// 进入失败终态；仍在排队、直接依赖它的作业以及更下游的作业必须随之失败——
// 不需要任何新的提交、取消或其他操作，作业详情与按提交人列举都要能看到
// 这次失败传播后的状态。上游不得留下可用成功归档，被阻断的作业不产生归档、
// 结束时间有值，作业号、提交参数与依赖关系保留。
func TestArchiveWriteFailureCascadesToWaitingDependents(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, WithClock(newManualClock()))
	if err != nil {
		t.Fatal(err)
	}
	started, block, release, _, _ := gateCompute(t, s)
	block(1)

	up := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "up", Values: []int64{1, 2}, Seed: 3,
	})
	waitStarted(t, started, up.ID)
	mid := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{5}, Dependencies: []uint64{up.ID},
	})
	leaf := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", Values: []int64{6}, Dependencies: []uint64{mid.ID},
	})
	if j, _ := s.Get(mid.ID); j.Status != StatusQueued || j.WaitReason != WaitDependency {
		t.Fatalf("mid must be waiting for dependency before failure: %+v", j)
	}

	// 归档目录暂时不可写：上游的计算本身成功，但成功记录无法创建/替换。
	makeArchiveReadOnly(t, dir)
	release(up.ID)

	// 上游：归档写入失败，与计算溢出失败一样进入失败终态。
	upView := waitStatus(t, s, up.ID, StatusFailed)
	if !strings.Contains(upView.FailureReason, "结果归档写入失败") {
		t.Fatalf("upstream reason=%q, want 结果归档写入失败", upView.FailureReason)
	}
	if upView.Archive != nil || len(upView.EffectiveValues) != 0 {
		t.Fatalf("upstream must not retain a usable success archive: %+v", upView)
	}
	if upView.FinishedAt.IsZero() {
		t.Fatal("upstream finished time must be set")
	}
	if upView.ID != up.ID || upView.Submitter != "a" || upView.RequestID != "up" ||
		upView.Seed != 3 || len(upView.Values) != 2 || upView.Values[0] != 1 || upView.Values[1] != 2 {
		t.Fatalf("upstream submit params altered: %+v", upView)
	}

	// 不做任何新操作：直接等待者与更下游都必须已经结束排队。
	midView := waitStatus(t, s, mid.ID, StatusFailed)
	leafView := waitStatus(t, s, leaf.ID, StatusFailed)

	// 直接等待者指出阻断它的上游；更下游说明自己的直接上游并指向根因作业号。
	if midView.BlockerID != up.ID {
		t.Fatalf("mid blocker=%d, want root %d", midView.BlockerID, up.ID)
	}
	if !strings.Contains(midView.FailureReason, fmt.Sprintf("作业 %d", up.ID)) {
		t.Fatalf("mid reason=%q must name its direct upstream %d", midView.FailureReason, up.ID)
	}
	if leafView.BlockerID != up.ID {
		t.Fatalf("leaf blocker=%d, want root %d", leafView.BlockerID, up.ID)
	}
	if !strings.Contains(leafView.FailureReason, fmt.Sprintf("作业 %d", mid.ID)) {
		t.Fatalf("leaf reason=%q must name its direct upstream %d", leafView.FailureReason, mid.ID)
	}
	if !strings.Contains(leafView.FailureReason, fmt.Sprintf("作业 %d", up.ID)) {
		t.Fatalf("leaf reason=%q must identify root cause job %d", leafView.FailureReason, up.ID)
	}

	// 被阻断的作业不开始计算、不产生成功归档，结束时间有值，依赖关系保留。
	for _, v := range []*Job{midView, leafView} {
		if v.Archive != nil || len(v.EffectiveValues) != 0 {
			t.Fatalf("blocked job %d must not carry a success archive: %+v", v.ID, v)
		}
		if !v.StartedAt.IsZero() || v.FinishedAt.IsZero() {
			t.Fatalf("blocked job %d times: started=%s finished=%s", v.ID, v.StartedAt, v.FinishedAt)
		}
	}
	if len(midView.Dependencies) != 1 || midView.Dependencies[0] != up.ID ||
		len(leafView.Dependencies) != 1 || leafView.Dependencies[0] != mid.ID {
		t.Fatalf("dependency links must be preserved: mid=%v leaf=%v",
			midView.Dependencies, leafView.Dependencies)
	}

	// 按提交人列举同样呈现传播后的状态，且仍按提交先后排列。
	listed, err := s.List("a", time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 3 || listed[0].ID != up.ID || listed[1].ID != mid.ID || listed[2].ID != leaf.ID {
		t.Fatalf("list order: %v", ids(listed))
	}
	for _, j := range listed {
		if j.Status != StatusFailed {
			t.Fatalf("listed job %d status=%s, want failed", j.ID, j.Status)
		}
	}

	// 写入恢复后，不依赖失败链的作业仍按原有规则正常计算与归档。
	makeArchiveWritable(t, dir)
	free := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{7}})
	done := waitStatus(t, s, free.ID, StatusSucceeded)
	if done.Archive == nil || done.Archive.Sum != 7 {
		t.Fatalf("independent job must still compute and archive: %+v", done)
	}
	// 已进入终态的作业不被后来的变化改写。
	again, _ := s.Get(up.ID)
	if again.Status != StatusFailed || again.FailureReason != upView.FailureReason || again.Archive != nil {
		t.Fatalf("terminal upstream record rewritten: %+v", again)
	}
}

// 一个作业等待多个上游时，只要其中一个发生归档写入失败，就不能因为其他上游
// 仍在排队或运行而继续等待；阻断者按已提交的依赖列表顺序选取当时已失败/取消的
// 上游。其他上游后来正常结束不改写已进入终态的作业。
func TestArchiveWriteFailureUnblocksMultiDependencyWaiter(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, WithClock(newManualClock()))
	if err != nil {
		t.Fatal(err)
	}
	started, block, release, _, _ := gateCompute(t, s)
	block(1)

	up := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1, 2}}) // id 1，归档将写失败
	waitStarted(t, started, up.ID)
	slow := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{4}}) // id 2，仍排队
	multi := mustSubmit(t, s, SubmitRequest{                                    // id 3，等待 [up, slow]
		Submitter: "a", Values: []int64{8}, Dependencies: []uint64{up.ID, slow.ID},
	})
	if j, _ := s.Get(multi.ID); j.WaitReason != WaitDependency {
		t.Fatalf("multi must be waiting for dependencies: %+v", j)
	}

	makeArchiveReadOnly(t, dir)
	release(up.ID)
	upView := waitStatus(t, s, up.ID, StatusFailed)
	if !strings.Contains(upView.FailureReason, "结果归档写入失败") {
		t.Fatalf("upstream reason=%q, want 结果归档写入失败", upView.FailureReason)
	}

	// slow 仍在排队，但 multi 不能继续等待：立即失败，阻断者为列表中
	// 最靠前的已失败上游 up。
	multiView := waitStatus(t, s, multi.ID, StatusFailed)
	if multiView.BlockerID != up.ID {
		t.Fatalf("multi blocker=%d, want %d", multiView.BlockerID, up.ID)
	}
	if !strings.Contains(multiView.FailureReason, fmt.Sprintf("作业 %d", up.ID)) {
		t.Fatalf("multi reason=%q must name blocker %d", multiView.FailureReason, up.ID)
	}
	if len(multiView.Dependencies) != 2 || multiView.Dependencies[0] != up.ID ||
		multiView.Dependencies[1] != slow.ID {
		t.Fatalf("multi dependencies must be preserved: %v", multiView.Dependencies)
	}

	// slow 不依赖失败链，但只读窗口内它自己的运行状态同样无法落盘，
	// 按既有规则以保存失败而终；这不改写 multi 已进入的终态。
	waitStatus(t, s, slow.ID, StatusFailed)
	makeArchiveWritable(t, dir)
	after, _ := s.Get(multi.ID)
	if after.Status != StatusFailed || after.FailureReason != multiView.FailureReason ||
		after.BlockerID != up.ID || after.Archive != nil {
		t.Fatalf("terminal multi record rewritten after other upstream finished: %+v", after)
	}

	// 写入恢复后，不依赖失败链的新作业仍按原有规则正常计算与归档。
	free := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{4}})
	done := waitStatus(t, s, free.ID, StatusSucceeded)
	if done.Archive == nil || done.Archive.Sum != 4 {
		t.Fatalf("independent job must still compute and archive: %+v", done)
	}
	final, _ := s.Get(multi.ID)
	if final.Status != StatusFailed || final.FailureReason != multiView.FailureReason ||
		final.BlockerID != up.ID {
		t.Fatalf("terminal multi record rewritten by later scheduling: %+v", final)
	}
}
