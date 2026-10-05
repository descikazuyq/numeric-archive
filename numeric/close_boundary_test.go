package numeric

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// TestCloseBoundaryNoLateSuccessAndQueuedPreserved 固定关闭边界上的同一条规则：
// 一旦归档开始以 ErrStoreClosed 拒绝提交（关闭生效），此后——
//   - 不得从队列启动新的计算：无依赖的排队作业保持排队，不新增开始/完成时间；
//   - 不得把当时尚未确认完成的计算补成成功：运行中的作业即使在关闭生效的瞬间
//     已经得出总和与平方和并无视中止返回这些数值，也只能以“计算被中断”失败，
//     不返回成功归档或实际计算输入；
//   - 等待该中断作业的下游继续按既有依赖失败规则级联，根因指向中断作业本身，
//     不会因为计算曾返回数值就拿到可用的上游结果；
//   - 关闭生效前已完整保存的成功归档（摘要、日志、校验值）原样保留；
//   - 已确认取消的排队作业保持取消终态，不被关闭改写成中断失败。
//
// Close 成功返回即意味着 worker 已退出、计算与记录写入都已结束，调用方可立即
// 重开同一目录：排队作业保留原作业号、参数、种子、提交时间，按原提交先后继续。
func TestCloseBoundaryNoLateSuccessAndQueuedPreserved(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// 先让关闭前的成功作业在未安装钩子时正常完成并完整归档。
	done := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "before", Values: []int64{7}, Seed: 11})
	doneView := waitStatus(t, s, done.ID, StatusSucceeded)

	// 此后唯一会进入计算的是运行中的作业（唯一 worker、它占住位置），
	// 因此钩子可以无条件阻塞到关闭生效。
	started := make(chan struct{})
	s.compute = func(_ uint64, in []int64, seed int64, canceled func() bool) (int64, int64, string, bool) {
		close(started)
		// 一直占着计算位置，直到关闭生效（stopCh 与 closed 在 Close 的同一
		// 临界区内关闭，关闭 stopCh 即代表提交已开始被拒绝）。
		<-s.stopCh
		// 关闭生效瞬间已得出总和与平方和，却无视中止信号照常返回“成功数值”：
		// 若关闭与停止信号是两个独立状态，旧实现可能据此补成成功归档；
		// 新实现必须判为中断失败。
		return 3, 5, "", true
	}

	// 运行中的作业：关闭时仍在计算。
	run := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "run", Values: []int64{1, 2}, Seed: 0})
	<-started

	// 与中断作业无依赖、关闭生效时仍排队的作业：必须原样保留排队状态。
	independent := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "queued", Values: []int64{10, 20}, Seed: 3})
	// 等待中断作业结果的下游：关闭后按既有依赖失败规则被阻断。
	downstream := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{5},
		Dependencies: []uint64{run.ID},
	})
	// 已确认取消的排队作业：取消终态已落盘，关闭不能把它改写成中断失败。
	canceledJob := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "canceled", Values: []int64{8}})
	cj, err := s.Cancel(canceledJob.ID)
	if err != nil || cj.Status != StatusCanceled || cj.FinishedAt.IsZero() {
		t.Fatalf("cancel queued before close: %+v err=%v", cj, err)
	}

	// 关闭前排队作业的完整身份快照，关闭后与重开后逐项比对。
	queuedBefore, _ := s.Get(independent.ID)

	// Close 关闭 stopCh 后计算才返回；Close 必须等到 worker 裁决落盘、退出才返回。
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 关闭已完成：运行作业为中断失败，没有成功归档或实际计算输入，
	// 保留开始时间并补上完成时间。
	gRun, _ := s.Get(run.ID)
	if gRun.Status != StatusFailed {
		t.Fatalf("运行作业关闭后状态=%s，want failed", gRun.Status)
	}
	if !strings.Contains(gRun.FailureReason, "计算被中断") {
		t.Fatalf("中断失败原因=%q，应说明计算因归档关闭而中断", gRun.FailureReason)
	}
	if gRun.Archive != nil || len(gRun.EffectiveValues) != 0 {
		t.Fatalf("中断作业不应留下成功归档或实际输入：archive=%v effective=%v",
			gRun.Archive, gRun.EffectiveValues)
	}
	if gRun.StartedAt.IsZero() || gRun.FinishedAt.IsZero() {
		t.Fatalf("中断作业应保留开始时间并补上完成时间：started=%v finished=%v",
			gRun.StartedAt, gRun.FinishedAt)
	}

	// 排队的无依赖作业：作业号、参数、种子、提交时间与排队状态全部保留，
	// 关闭没有借机执行它，也没有新增开始/完成时间。
	gQ, _ := s.Get(independent.ID)
	if gQ.Status != StatusQueued || gQ.ID != queuedBefore.ID ||
		gQ.Seed != queuedBefore.Seed || !gQ.QueuedAt.Equal(queuedBefore.QueuedAt) {
		t.Fatalf("排队作业身份被关闭改动: got=%+v want=%+v", gQ, queuedBefore)
	}
	if len(gQ.Values) != len(queuedBefore.Values) || gQ.Values[0] != 10 || gQ.Values[1] != 20 {
		t.Fatalf("排队作业参数被改动: %v", gQ.Values)
	}
	if !gQ.StartedAt.IsZero() || !gQ.FinishedAt.IsZero() || gQ.Archive != nil {
		t.Fatalf("排队作业不应因关闭产生开始/完成时间或归档：started=%v finished=%v archive=%v",
			gQ.StartedAt, gQ.FinishedAt, gQ.Archive)
	}

	// 等待中断作业的下游：遵守正常依赖阻断，根因是中断作业，且无可用上游结果。
	gD, _ := s.Get(downstream.ID)
	if gD.Status != StatusFailed || gD.BlockerID != run.ID || gD.Archive != nil {
		t.Fatalf("下游应被中断作业阻断（root=%d），got status=%s blocker=%d archive=%v reason=%q",
			run.ID, gD.Status, gD.BlockerID, gD.Archive, gD.FailureReason)
	}
	if !strings.Contains(gD.FailureReason, "作业") {
		t.Fatalf("下游失败原因应指出阻断它的作业：%q", gD.FailureReason)
	}

	// 关闭前已确认的成功归档原样保留：摘要、日志、校验值、结果均不变。
	gDone, _ := s.Get(done.ID)
	if gDone.Status != StatusSucceeded || gDone.Archive == nil {
		t.Fatalf("关闭前成功作业被改动: status=%s", gDone.Status)
	}
	if gDone.Archive.Sum != doneView.Archive.Sum ||
		gDone.Archive.SumOfSquares != doneView.Archive.SumOfSquares ||
		gDone.Archive.ResultDigest != doneView.Archive.ResultDigest ||
		gDone.Archive.InputsDigest != doneView.Archive.InputsDigest ||
		gDone.Archive.Checksum != doneView.Archive.Checksum ||
		gDone.Archive.Log != doneView.Archive.Log {
		t.Fatal("关闭前成功归档的结果、摘要、日志或校验值被关闭改动")
	}

	// 已确认取消的作业保持取消，不被关闭改判为中断失败。
	gC, _ := s.Get(canceledJob.ID)
	if gC.Status != StatusCanceled || gC.BlockerID != 0 || gC.Archive != nil ||
		!gC.FinishedAt.Equal(cj.FinishedAt) {
		t.Fatalf("已确认取消作业被关闭改写: status=%s blocker=%d finished=%v reason=%q",
			gC.Status, gC.BlockerID, gC.FinishedAt, gC.FailureReason)
	}

	// Close 一返回即可立即重开同一目录。
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen right after close: %v", err)
	}
	defer func() { _ = s2.Close() }()

	// 重开后终态与关闭返回时一致。
	if r, _ := s2.Get(run.ID); r.Status != StatusFailed || r.Archive != nil ||
		!strings.Contains(r.FailureReason, "计算被中断") {
		t.Fatalf("重开后中断作业状态错误: %+v", r)
	}
	if r, _ := s2.Get(downstream.ID); r.Status != StatusFailed || r.BlockerID != run.ID {
		t.Fatalf("重开后下游阻断错误: status=%s blocker=%d", r.Status, r.BlockerID)
	}
	if r, _ := s2.Get(canceledJob.ID); r.Status != StatusCanceled {
		t.Fatalf("重开后取消作业被改写: %s", r.Status)
	}
	if r, _ := s2.Get(done.ID); r.Status != StatusSucceeded ||
		r.Archive.Checksum != doneView.Archive.Checksum {
		t.Fatalf("重开后关闭前成功归档被改动: status=%s", r.Status)
	}

	// 排队作业保留原提交时间，按原提交先后继续处理。
	resumed := waitStatus(t, s2, independent.ID, StatusSucceeded)
	if !resumed.QueuedAt.Equal(queuedBefore.QueuedAt) {
		t.Fatalf("重开后排队作业提交时间变化: got=%v want=%v",
			resumed.QueuedAt, queuedBefore.QueuedAt)
	}
	if resumed.Archive == nil || resumed.Archive.Sum != 30 || resumed.Archive.SumOfSquares != 500 {
		t.Fatalf("排队作业重开后应按原参数成功（30,500），got %+v", resumed.Archive)
	}

	// 关闭后的旧对象继续拒绝提交。
	if _, err := s.Submit(SubmitRequest{Submitter: "a", Values: []int64{1}}); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("submit on closed store: %v", err)
	}
}

// TestCloseSingleGateWorkerHonorsClosed 复现历史上的分裂状态窗口：旧实现里
// Close 先在锁内置位 closed（提交据此被拒绝），解锁后才单独置位 worker 使用的
// 第二个停止信号。若 worker 在这两步之间完成计算，旧代码会把未确认的成功补写
// 落盘，并继续从队列启动下一个作业。
//
// 新实现只有同一条关闭状态：worker 的启动判断与成功裁决都在同一把锁下读取
// closed 本身，不依赖任何第二个信号。本测试手工进入“提交已被拒绝（closed=true）
// 但第二个停止信号尚未置位”的历史瞬间，并让计算在此刻返回成功数值：worker 仍
// 必须把运行作业判为中断失败，且下一轮不再启动后面的排队作业而直接退出。
// （真实 Close 永远在同一临界区同时置位 closed 并关闭 stopCh，生产中不存在
// 该中间态；这里只用于白盒验证单状态门控。）
func TestCloseSingleGateWorkerHonorsClosed(t *testing.T) {
	s, _ := openTestStore(t)

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	s.compute = func(_ uint64, in []int64, seed int64, canceled func() bool) (int64, int64, string, bool) {
		entered <- struct{}{}
		<-release
		return 3, 5, "", true // 无视中止，返回一份本可成功的数值
	}

	run := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1, 2}})
	queued := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{9}})
	<-entered

	// 仅进入“关闭已开始拒绝提交”这一半：closed 置位，但刻意不关闭 stopCh，
	// 忠实复现旧实现解锁后、第二个停止信号置位前的窗口。
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	close(release)
	// worker 裁决完运行作业后，下一轮循环在同一 closed 位前退出；它既不应补写
	// 成功，也不应启动排队作业。这里给退出加上时限：旧实现的 worker 只认第二个
	// 停止信号，closed 单独置位时它会补写成功、继续启动排队作业并永久阻塞，
	// 因而会在该时限处被明确判失败，而不是拖到整体测试超时。
	exited := make(chan struct{})
	go func() { s.wg.Wait(); close(exited) }()
	select {
	case <-exited:
	case <-time.After(2 * time.Second):
		t.Fatal("closed 生效后 worker 未退出：它仍在等待第二个停止信号，关闭门控不是同一条状态")
	}

	g, _ := s.Get(run.ID)
	if g.Status != StatusFailed || !strings.Contains(g.FailureReason, "计算被中断") {
		t.Fatalf("closed 生效后返回的计算不得补成成功：status=%s reason=%q", g.Status, g.FailureReason)
	}
	if g.Archive != nil || len(g.EffectiveValues) != 0 {
		t.Fatalf("中断失败不得携带成功归档或实际输入：archive=%v effective=%v",
			g.Archive, g.EffectiveValues)
	}
	gq, _ := s.Get(queued.ID)
	if gq.Status != StatusQueued || !gq.StartedAt.IsZero() || gq.Archive != nil {
		t.Fatalf("关闭生效后不得从队列启动新作业：status=%s started=%v archive=%v",
			gq.Status, gq.StartedAt, gq.Archive)
	}
}
