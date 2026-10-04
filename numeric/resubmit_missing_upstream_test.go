package numeric

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 本文件覆盖“带依赖的作业被接受后，重开归档时上游记录缺失”场景下的重复提交：
// 原请求重放必须继续指向恢复后已被改判为失败的原作业，而不是以“依赖作业不
// 存在”拒绝；内容变化仍按幂等冲突处理；与已接受请求无关的新请求维持原有的
// 拒绝语义。

// reopenMissing 删除指定作业号的落盘记录后重新打开目录，模拟重开归档时部分
// 直接上游记录缺失。重开后的 Store 注册到测试清理。
func reopenMissing(t *testing.T, dir string, missing ...uint64) *Store {
	t.Helper()
	for _, id := range missing {
		if err := os.Remove(filepath.Join(dir, jobFileName(id))); err != nil {
			t.Fatalf("remove job %d: %v", id, err)
		}
	}
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func countJobFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "job-") && strings.HasSuffix(e.Name(), ".json") {
			n++
		}
	}
	return n
}

func readOnDiskJobRecord(t *testing.T, dir string, id uint64) jobRecord {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, jobFileName(id)))
	if err != nil {
		t.Fatal(err)
	}
	var r jobRecord
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

// assertFailedViewEqual 断言两次查询/重放返回的失败视图逐字段一致：失败状态、
// 原因、根因、全部已有时间、原始参数与有序依赖，且成功归档与实际输入均为空。
func assertFailedViewEqual(t *testing.T, label string, got, want *Job) {
	t.Helper()
	if got.ID != want.ID {
		t.Fatalf("%s: ID=%d want %d", label, got.ID, want.ID)
	}
	if got.Status != StatusFailed || want.Status != StatusFailed {
		t.Fatalf("%s: status=%s want %s", label, got.Status, want.Status)
	}
	if got.FailureReason != want.FailureReason {
		t.Fatalf("%s: reason=%q want %q", label, got.FailureReason, want.FailureReason)
	}
	if got.BlockerID != want.BlockerID {
		t.Fatalf("%s: blocker=%d want %d", label, got.BlockerID, want.BlockerID)
	}
	if !got.QueuedAt.Equal(want.QueuedAt) || !got.StartedAt.Equal(want.StartedAt) ||
		!got.FinishedAt.Equal(want.FinishedAt) {
		t.Fatalf("%s: times got (%s,%s,%s) want (%s,%s,%s)", label,
			got.QueuedAt, got.StartedAt, got.FinishedAt,
			want.QueuedAt, want.StartedAt, want.FinishedAt)
	}
	if got.Archive != nil || got.EffectiveValues != nil {
		t.Fatalf("%s: failed view must carry no archive/effective values, got archive=%v eff=%v",
			label, got.Archive != nil, got.EffectiveValues)
	}
	if len(got.Values) != len(want.Values) {
		t.Fatalf("%s: values=%v want %v", label, got.Values, want.Values)
	}
	for i := range want.Values {
		if got.Values[i] != want.Values[i] {
			t.Fatalf("%s: values=%v want %v", label, got.Values, want.Values)
		}
	}
	if len(got.Dependencies) != len(want.Dependencies) {
		t.Fatalf("%s: deps=%v want %v", label, got.Dependencies, want.Dependencies)
	}
	for i := range want.Dependencies {
		if got.Dependencies[i] != want.Dependencies[i] {
			t.Fatalf("%s: deps=%v want %v", label, got.Dependencies, want.Dependencies)
		}
	}
}

// 单直接上游的作业曾成功归档；删除上游记录重开后，原作业因“成功结果不可用”
// 被改判为失败。用与已接受请求完全相同的请求（列表写法与单依赖写法互换都算
// 相同内容）再次提交：返回原作业的当前失败详情、无错误，不建作业、不补建
// 上游、不重新排队、不改写记录。
func TestReplayIdenticalRequestAfterUpstreamRecordMissing(t *testing.T) {
	listReq := func(up uint64) SubmitRequest {
		return SubmitRequest{
			Submitter: "a", RequestID: "r", Values: []int64{5},
			Dependencies: []uint64{up},
		}
	}
	singleReq := func(up uint64) SubmitRequest {
		return SubmitRequest{
			Submitter: "a", RequestID: "r", Values: []int64{5},
			HasDependency: true, DependencyID: up,
		}
	}
	cases := []struct {
		name         string
		orig, replay func(up uint64) SubmitRequest
	}{
		{"list then list", listReq, listReq},
		{"single then list", singleReq, listReq},
		{"list then single", listReq, singleReq},
		{"single then single", singleReq, singleReq},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			up := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "up", Values: []int64{4}})
			waitStatus(t, s, up.ID, StatusSucceeded)
			origReq := c.orig(up.ID)
			orig := mustSubmit(t, s, origReq)
			done := waitStatus(t, s, orig.ID, StatusSucceeded) // 实际输入 [5,4]
			if len(done.Archive.EffectiveValues) != 2 || done.Archive.EffectiveValues[1] != 4 {
				t.Fatalf("effective=%v want [5 4]", done.Archive.EffectiveValues)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			s2 := reopenMissing(t, dir, up.ID)

			g0, err := s2.Get(orig.ID)
			if err != nil {
				t.Fatal(err)
			}
			if g0.Status != StatusFailed || g0.BlockerID != up.ID {
				t.Fatalf("reopen status=%s blocker=%d want failed/root %d (reason=%q)",
					g0.Status, g0.BlockerID, up.ID, g0.FailureReason)
			}
			if !strings.Contains(g0.FailureReason, "作业 "+itoa(up.ID)) ||
				!strings.Contains(g0.FailureReason, "成功结果不可用") {
				t.Fatalf("failure reason must name missing upstream: %q", g0.FailureReason)
			}
			if g0.Archive != nil || g0.EffectiveValues != nil || g0.FinishedAt.IsZero() || g0.StartedAt.IsZero() {
				t.Fatalf("failed restore must clear archive/inputs and keep timestamps: %+v", g0)
			}

			before := countJobFiles(t, dir)

			// 完全相同的原请求再次提交：返回原作业当前详情，无错误。
			got, err := s2.Submit(c.replay(up.ID))
			if err != nil {
				t.Fatalf("identical replay after upstream missing: %v", err)
			}
			if got == nil {
				t.Fatal("replay returned nil job")
			}
			assertFailedViewEqual(t, "replay", got, g0)

			// 不创建新作业，不补建缺失上游。
			if countJobFiles(t, dir) != before {
				t.Fatalf("replay changed on-disk record count: before=%d after=%d",
					before, countJobFiles(t, dir))
			}
			if _, err := s2.Get(up.ID); !errors.Is(err, ErrNotFound) {
				t.Fatalf("missing upstream recreated? err=%v want ErrNotFound", err)
			}
			if _, err := os.Stat(filepath.Join(dir, jobFileName(up.ID))); !os.IsNotExist(err) {
				t.Fatalf("missing upstream file reappeared: %v", err)
			}

			// 不把失败作业重新排队、不改写失败记录：稍等后状态/时间/原因不变。
			time.Sleep(20 * time.Millisecond)
			g1, _ := s2.Get(orig.ID)
			assertFailedViewEqual(t, "after wait", g1, g0)
			rec := readOnDiskJobRecord(t, dir, orig.ID)
			if rec.Status != StatusFailed || rec.BlockerID != up.ID ||
				rec.FailureReason != g0.FailureReason ||
				!rec.FinishedAt.Equal(g0.FinishedAt) || !rec.QueuedAt.Equal(g0.QueuedAt) ||
				len(rec.Dependencies) != 1 || rec.Dependencies[0] != up.ID ||
				len(rec.Values) != 1 || rec.Values[0] != 5 {
				t.Fatalf("on-disk original record altered by replay: %+v", rec)
			}

			// 后续真正的新提交取得新作业号，缺失作业号也未被占用。
			fresh := mustSubmit(t, s2, SubmitRequest{Submitter: "a", RequestID: "fresh", Values: []int64{1}})
			if fresh.ID != orig.ID+1 {
				t.Fatalf("new job id=%d want %d", fresh.ID, orig.ID+1)
			}
			waitStatus(t, s2, fresh.ID, StatusSucceeded)
		})
	}
}

// 多个直接上游中只缺失其中一个时同样适用：按保存的依赖顺序最靠前的缺失上游
// 决定失败归因；原请求重放仍返回原作业。依赖次序变化或改成单依赖写法则属于
// 内容不一致，即使请求仍引用缺失上游也返回幂等冲突并指出缺失作业号。
func TestReplayMultiDependencyWithOneUpstreamMissing(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	up1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{4}})
	up2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{10}})
	waitStatus(t, s, up1.ID, StatusSucceeded)
	waitStatus(t, s, up2.ID, StatusSucceeded)
	orig := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "r", Values: []int64{1},
		Dependencies: []uint64{up1.ID, up2.ID},
	})
	waitStatus(t, s, orig.ID, StatusSucceeded) // 实际输入 [1,4,10]
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 只缺失依赖列表中更靠前的 up1。
	s2 := reopenMissing(t, dir, up1.ID)

	g0, _ := s2.Get(orig.ID)
	if g0.Status != StatusFailed || g0.BlockerID != up1.ID {
		t.Fatalf("status=%s blocker=%d want failed/root %d (reason=%q)",
			g0.Status, g0.BlockerID, up1.ID, g0.FailureReason)
	}
	if !strings.Contains(g0.FailureReason, "作业 "+itoa(up1.ID)) {
		t.Fatalf("reason must name earliest missing upstream %d: %q", up1.ID, g0.FailureReason)
	}
	// 另一个上游保持自己的成功结果。
	gUp2, _ := s2.Get(up2.ID)
	if gUp2.Status != StatusSucceeded {
		t.Fatalf("surviving upstream status=%s want succeeded", gUp2.Status)
	}

	// 完全相同（多依赖列表）→ 原作业，无错误。
	got, err := s2.Submit(SubmitRequest{
		Submitter: "a", RequestID: "r", Values: []int64{1},
		Dependencies: []uint64{up1.ID, up2.ID},
	})
	if err != nil {
		t.Fatalf("identical multi-dep replay: %v", err)
	}
	assertFailedViewEqual(t, "multi-dep replay", got, g0)

	// 依赖次序变化 → 幂等冲突（不是“依赖不存在”），视图仍指原作业，
	// 且明确说明内容不一致并指出仍被引用的缺失上游。
	swapped, err := s2.Submit(SubmitRequest{
		Submitter: "a", RequestID: "r", Values: []int64{1},
		Dependencies: []uint64{up2.ID, up1.ID},
	})
	if !errors.Is(err, ErrIdempotencyConflict) || swapped == nil || swapped.ID != orig.ID {
		t.Fatalf("order change: job=%v err=%v want ErrIdempotencyConflict + original job", swapped, err)
	}
	if !strings.Contains(err.Error(), "不一致") ||
		!strings.Contains(err.Error(), "作业 "+itoa(up1.ID)) ||
		!strings.Contains(err.Error(), "记录已缺失") {
		t.Fatalf("conflict error must state mismatch and missing upstream %d: %q", up1.ID, err.Error())
	}

	// 改成只含一个上游的单依赖写法 → 依赖内容不同，同样冲突。
	single, err := s2.Submit(SubmitRequest{
		Submitter: "a", RequestID: "r", Values: []int64{1},
		HasDependency: true, DependencyID: up1.ID,
	})
	if !errors.Is(err, ErrIdempotencyConflict) || single == nil || single.ID != orig.ID {
		t.Fatalf("single-dep rewrite vs 2-dep list must conflict: job=%v err=%v", single, err)
	}
	if !strings.Contains(err.Error(), "作业 "+itoa(up1.ID)) {
		t.Fatalf("conflict error must still name missing upstream: %q", err.Error())
	}

	// 冲突不改写原记录。
	g1, _ := s2.Get(orig.ID)
	assertFailedViewEqual(t, "after conflicts", g1, g0)
}

// 同人同号下改变整数内容/次序或种子，即使请求仍引用已经缺失的上游，也返回
// 幂等冲突与原作业详情，明确告知内容不一致；原记录保持原样。
func TestReplayContentConflictAfterUpstreamRecordMissing(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	up := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "up", Values: []int64{4}})
	waitStatus(t, s, up.ID, StatusSucceeded)
	orig := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "r", Values: []int64{1, 2}, Seed: 7,
		Dependencies: []uint64{up.ID},
	})
	waitStatus(t, s, orig.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := reopenMissing(t, dir, up.ID)
	g0, _ := s2.Get(orig.ID)
	if g0.Status != StatusFailed {
		t.Fatalf("status=%s want failed", g0.Status)
	}

	conflicts := []struct {
		name string
		req  SubmitRequest
		// noteMissing 表示这次冲突请求仍引用缺失上游，错误中必须指出它。
		noteMissing bool
	}{
		{
			name:        "values content changed",
			req:         SubmitRequest{Submitter: "a", RequestID: "r", Values: []int64{1, 3}, Seed: 7, Dependencies: []uint64{up.ID}},
			noteMissing: true,
		},
		{
			name:        "values order changed",
			req:         SubmitRequest{Submitter: "a", RequestID: "r", Values: []int64{2, 1}, Seed: 7, Dependencies: []uint64{up.ID}},
			noteMissing: true,
		},
		{
			name:        "seed changed",
			req:         SubmitRequest{Submitter: "a", RequestID: "r", Values: []int64{1, 2}, Seed: 8, Dependencies: []uint64{up.ID}},
			noteMissing: true,
		},
		{
			name:        "dependency dropped",
			req:         SubmitRequest{Submitter: "a", RequestID: "r", Values: []int64{1, 2}, Seed: 7},
			noteMissing: false,
		},
	}
	for _, c := range conflicts {
		t.Run(c.name, func(t *testing.T) {
			j, err := s2.Submit(c.req)
			if !errors.Is(err, ErrIdempotencyConflict) {
				t.Fatalf("err=%v want ErrIdempotencyConflict", err)
			}
			if j == nil || j.ID != orig.ID {
				t.Fatalf("conflict must return original job view: %+v", j)
			}
			assertFailedViewEqual(t, "conflict view", j, g0)
			if !strings.Contains(err.Error(), "不一致") {
				t.Fatalf("conflict error must state content mismatch: %q", err.Error())
			}
			if c.noteMissing {
				if !strings.Contains(err.Error(), "作业 "+itoa(up.ID)) ||
					!strings.Contains(err.Error(), "记录已缺失") {
					t.Fatalf("conflict error must name still-referenced missing upstream %d: %q",
						up.ID, err.Error())
				}
			} else if strings.Contains(err.Error(), "记录已缺失") {
				t.Fatalf("request without missing upstream must not carry missing note: %q", err.Error())
			}
		})
	}

	// 依赖改挂到另一个仍存在的作业：同样只是冲突，且不产生缺失提示、不补建上游。
	other := mustSubmit(t, s2, SubmitRequest{Submitter: "a", RequestID: "other", Values: []int64{9}})
	waitStatus(t, s2, other.ID, StatusSucceeded)
	j, err := s2.Submit(SubmitRequest{
		Submitter: "a", RequestID: "r", Values: []int64{1, 2}, Seed: 7,
		Dependencies: []uint64{other.ID},
	})
	if !errors.Is(err, ErrIdempotencyConflict) || j == nil || j.ID != orig.ID {
		t.Fatalf("dep swap to existing job must still conflict: job=%v err=%v", j, err)
	}
	if strings.Contains(err.Error(), "记录已缺失") {
		t.Fatalf("rewrite referencing only existing jobs must not carry missing note: %q", err.Error())
	}

	// 全部冲突之后，失败记录的状态、原因、根因、时间、原始参数与依赖原样保留。
	g1, _ := s2.Get(orig.ID)
	assertFailedViewEqual(t, "final", g1, g0)
	if _, err := s2.Get(up.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing upstream must stay missing: %v", err)
	}
}

// 提交人不同、请求号不同或请求号为空的提交不命中幂等记录：引用缺失上游时
// 仍按新请求拒绝（ErrDependencyNotFound），不产生记录，也不占用请求号。
func TestNewRequestReferencingMissingUpstreamStillRejected(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	up := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "up", Values: []int64{4}})
	waitStatus(t, s, up.ID, StatusSucceeded)
	orig := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "r", Values: []int64{5},
		Dependencies: []uint64{up.ID},
	})
	waitStatus(t, s, orig.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := reopenMissing(t, dir, up.ID)
	before := countJobFiles(t, dir) // 只剩原作业一条记录

	newRequests := []struct {
		name string
		req  SubmitRequest
	}{
		{"different submitter same request id", SubmitRequest{
			Submitter: "b", RequestID: "r", Values: []int64{5},
			Dependencies: []uint64{up.ID},
		}},
		{"same submitter different request id", SubmitRequest{
			Submitter: "a", RequestID: "r2", Values: []int64{5},
			Dependencies: []uint64{up.ID},
		}},
		{"empty request id", SubmitRequest{
			Submitter: "a", Values: []int64{5},
			Dependencies: []uint64{up.ID},
		}},
		{"single-dep form", SubmitRequest{
			Submitter: "a", RequestID: "r3", Values: []int64{5},
			HasDependency: true, DependencyID: up.ID,
		}},
	}
	for _, c := range newRequests {
		t.Run(c.name, func(t *testing.T) {
			j, err := s2.Submit(c.req)
			if !errors.Is(err, ErrDependencyNotFound) {
				t.Fatalf("err=%v want ErrDependencyNotFound", err)
			}
			if j != nil {
				t.Fatalf("rejected submit must not return a job: %+v", j)
			}
			if countJobFiles(t, dir) != before {
				t.Fatalf("rejected submit created a record (files %d != %d)",
					countJobFiles(t, dir), before)
			}
		})
	}

	// 不同提交人被拒绝后不产生其名下记录。
	if listed, err := s2.List("b", time.Time{}, time.Time{}); err != nil || len(listed) != 0 {
		t.Fatalf("rejected different-submitter submit left records: %v err=%v", listed, err)
	}
	// 被拒绝的请求号未被占用：同号提交改为引用合法内容（无依赖）应作为新作业接受。
	reuse := mustSubmit(t, s2, SubmitRequest{Submitter: "a", RequestID: "r2", Values: []int64{3}})
	if reuse.ID != orig.ID+1 {
		t.Fatalf("reused request id created job %d, want fresh id %d", reuse.ID, orig.ID+1)
	}
	waitStatus(t, s2, reuse.ID, StatusSucceeded)
	// 缺失上游始终未被补建，原失败作业也未被改动。
	if _, err := s2.Get(up.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing upstream recreated: %v", err)
	}
	g, _ := s2.Get(orig.ID)
	if g.Status != StatusFailed || g.BlockerID != up.ID {
		t.Fatalf("original job altered: status=%s blocker=%d", g.Status, g.BlockerID)
	}
}

// 请求自身的格式错误先于幂等判定：即使提交人+请求号正好命中已接受请求，
// 空整数序列、依赖号为零、重复依赖、两种写法同时启用仍返回原有对应拒绝，
// 不返回原作业，也不改写原记录。
func TestReplayStructuralValidationPrecedesIdempotency(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	up := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "up", Values: []int64{4}})
	waitStatus(t, s, up.ID, StatusSucceeded)
	orig := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "r", Values: []int64{5},
		Dependencies: []uint64{up.ID},
	})
	waitStatus(t, s, orig.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// 上游缺失下重放：结构校验必须仍然先于“上游不存在”与幂等判定。
	s2 := reopenMissing(t, dir, up.ID)
	g0, _ := s2.Get(orig.ID)

	cases := []struct {
		name string
		req  SubmitRequest
		want error
	}{
		{"empty sequence", SubmitRequest{
			Submitter: "a", RequestID: "r", Dependencies: []uint64{up.ID},
		}, ErrEmptySequence},
		{"zero dependency", SubmitRequest{
			Submitter: "a", RequestID: "r", Values: []int64{5},
			HasDependency: true, DependencyID: 0,
		}, ErrInvalidDependency},
		{"duplicate dependency", SubmitRequest{
			Submitter: "a", RequestID: "r", Values: []int64{5},
			Dependencies: []uint64{up.ID, up.ID},
		}, ErrInvalidDependency},
		{"both dependency forms", SubmitRequest{
			Submitter: "a", RequestID: "r", Values: []int64{5},
			HasDependency: true, DependencyID: up.ID,
			Dependencies: []uint64{up.ID},
		}, ErrInvalidDependency},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			j, err := s2.Submit(c.req)
			if !errors.Is(err, c.want) {
				t.Fatalf("err=%v want %v", err, c.want)
			}
			if j != nil {
				t.Fatalf("structural rejection must not return a job: %+v", j)
			}
		})
	}
	g1, _ := s2.Get(orig.ID)
	assertFailedViewEqual(t, "original after structural rejections", g1, g0)
}

// 归档关闭后，原请求重放同样返回归档已关闭。
func TestReplayAfterArchiveClosed(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	up := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "up", Values: []int64{4}})
	waitStatus(t, s, up.ID, StatusSucceeded)
	orig := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "r", Values: []int64{5},
		Dependencies: []uint64{up.ID},
	})
	waitStatus(t, s, orig.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// 上游记录同时缺失：关闭检查最先生效，无论内容如何都返回 ErrStoreClosed。
	if err := os.Remove(filepath.Join(dir, jobFileName(up.ID))); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Submit(SubmitRequest{
		Submitter: "a", RequestID: "r", Values: []int64{5},
		Dependencies: []uint64{up.ID},
	}); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed-store replay err=%v want ErrStoreClosed", err)
	}
}
