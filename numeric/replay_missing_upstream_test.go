package numeric

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 带依赖的作业被接受后，某个直接上游的记录可能在重新打开归档时缺失：
// 恢复会把该作业改判失败（保留失败状态、原因、BlockerID 与已有时间，清空
// 成功归档与实际输入），但原请求号与原始参数仍保存在记录里。用原请求再次
// 提交必须按幂等返回原作业的当前详情，不能以“依赖作业不存在”拒绝。
//
// 本用例起源是已成功归档的多依赖作业，三个直接上游中只缺失中间一个。
func TestReplayOriginalRequestAfterUpstreamRecordMissing(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{1, 2}})  // 和 3
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{1, -3}}) // 和 -2
	u3 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u3", Values: []int64{5}})     // 和 5
	waitStatus(t, s, u1.ID, StatusSucceeded)
	waitStatus(t, s, u2.ID, StatusSucceeded)
	waitStatus(t, s, u3.ID, StatusSucceeded)
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{10},
		Dependencies: []uint64{u1.ID, u2.ID, u3.ID},
	})
	d := waitStatus(t, s, down.ID, StatusSucceeded)
	wantEffective := []int64{10, 3, -2, 5}
	if len(d.Archive.EffectiveValues) != len(wantEffective) {
		t.Fatalf("effective=%v want %v", d.Archive.EffectiveValues, wantEffective)
	}
	for i := range wantEffective {
		if d.Archive.EffectiveValues[i] != wantEffective[i] {
			t.Fatalf("effective=%v want %v", d.Archive.EffectiveValues, wantEffective)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 多个直接上游中只缺失中间的 u2：重开后 down 改判失败，根因即缺失作业号。
	if err := os.Remove(filepath.Join(dir, jobFileName(u2.ID))); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	g := waitStatus(t, s2, down.ID, StatusFailed)
	if g.BlockerID != u2.ID {
		t.Fatalf("blocker=%d want missing upstream %d", g.BlockerID, u2.ID)
	}
	if !strings.Contains(g.FailureReason, "作业 "+itoa(u2.ID)) ||
		!strings.Contains(g.FailureReason, "成功结果不可用") {
		t.Fatalf("失败原因须指出缺失上游且说明成功结果不可用: %q", g.FailureReason)
	}
	if g.Archive != nil || len(g.EffectiveValues) != 0 {
		t.Fatalf("失败记录不得带成功归档/实际输入: archive=%v effective=%v",
			g.Archive, g.EffectiveValues)
	}

	// 用原请求（整数序列及次序、种子、有序依赖内容完全一致）再次提交：
	// 返回原作业的当前详情且不返回错误；不创建新作业、不补建缺失上游、
	// 不重新排队、不改写失败状态/原因/BlockerID/已有时间。
	exact := SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{10},
		Dependencies: []uint64{u1.ID, u2.ID, u3.ID},
	}
	for i := 0; i < 2; i++ {
		r, err := s2.Submit(exact)
		if err != nil {
			t.Fatalf("第 %d 次原请求重放 err=%v，要返回原作业且无错误", i+1, err)
		}
		if r.ID != down.ID {
			t.Fatalf("第 %d 次重放 id=%d want %d", i+1, r.ID, down.ID)
		}
		if r.Status != StatusFailed || r.FailureReason != g.FailureReason ||
			r.BlockerID != g.BlockerID || !r.QueuedAt.Equal(g.QueuedAt) ||
			!r.StartedAt.Equal(g.StartedAt) || !r.FinishedAt.Equal(g.FinishedAt) {
			t.Fatalf("第 %d 次重放改写了恢复得到的失败记录: %+v vs %+v", i+1, r, g)
		}
		if r.Archive != nil || len(r.EffectiveValues) != 0 {
			t.Fatalf("第 %d 次重放带回了成功归档/实际输入", i+1)
		}
		// 原始参数与依赖列表原样保留。
		if r.Seed != 0 || len(r.Values) != 1 || r.Values[0] != 10 ||
			len(r.Dependencies) != 3 || r.Dependencies[0] != u1.ID ||
			r.Dependencies[1] != u2.ID || r.Dependencies[2] != u3.ID {
			t.Fatalf("第 %d 次重放视图的原始参数被改写: %+v", i+1, r)
		}
	}
	// 缺失上游没有被补建，记录数量不变，作业号不复用、不跳号。
	if _, err := s2.Get(u2.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("缺失上游记录被补建: err=%v", err)
	}
	if list, _ := s2.List("a", time.Time{}, time.Time{}); len(list) != 3 {
		t.Fatalf("重放产生/删除了记录: %d 条，要 3（u1、u3、down）", len(list))
	}
}

// 排队中的多依赖作业在重开时遇到缺失的直接上游，同样在恢复阶段被判失败；
// 原请求重放返回该失败作业，且不开始计算、不补时间。
func TestReplayOriginalRequestAfterMissingUpstreamFromQueued(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	started, block, _, _, _ := gateCompute(t, s)
	block(1)
	head := mustSubmit(t, s, SubmitRequest{Submitter: "a", Values: []int64{1}}) // id1
	waitStarted(t, started, head.ID)
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{1, 2}})  // id2
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{3}})     // id3
	down := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "down", Values: []int64{10}, // id4
		Dependencies: []uint64{u2.ID, u1.ID}})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 关闭时 down 仍排队、u2 仍排队；删除 u2 记录后重开。
	if err := os.Remove(filepath.Join(dir, jobFileName(u2.ID))); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	waitStatus(t, s2, head.ID, StatusFailed)
	g := waitStatus(t, s2, down.ID, StatusFailed)
	if g.BlockerID != u2.ID {
		t.Fatalf("blocker=%d want missing %d", g.BlockerID, u2.ID)
	}
	if !strings.Contains(g.FailureReason, "作业 "+itoa(u2.ID)) ||
		!strings.Contains(g.FailureReason, "缺少该作业的记录") ||
		!strings.Contains(g.FailureReason, "结果无法使用") {
		t.Fatalf("缺失上游原因不对: %q", g.FailureReason)
	}
	if !g.StartedAt.IsZero() {
		t.Fatalf("排队改判失败的作业不应有开始时间: %s", g.StartedAt)
	}
	if g.FinishedAt.IsZero() {
		t.Fatal("排队改判失败时应补上完成时间")
	}
	waitStatus(t, s2, u1.ID, StatusSucceeded)

	r, err := s2.Submit(SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{10},
		Dependencies: []uint64{u2.ID, u1.ID},
	})
	if err != nil || r.ID != down.ID {
		t.Fatalf("排队起源重放: id=%d err=%v, want %d/nil", replayID(r), err, down.ID)
	}
	if r.Status != StatusFailed || r.BlockerID != g.BlockerID ||
		r.FailureReason != g.FailureReason || !r.FinishedAt.Equal(g.FinishedAt) ||
		r.Archive != nil || len(r.EffectiveValues) != 0 {
		t.Fatalf("重放改写了失败记录: %+v vs %+v", r, g)
	}
	// 重放既不新建作业也不占用作业号：下一个合法新作业仍是 down.ID+1。
	next := mustSubmit(t, s2, SubmitRequest{Submitter: "a", Values: []int64{1}})
	if next.ID != down.ID+1 {
		t.Fatalf("重放占用了作业号: next=%d want %d", next.ID, down.ID+1)
	}
	waitStatus(t, s2, next.ID, StatusSucceeded)
}

// 单依赖写法与只含同一作业号的依赖列表是同一份内容：上游记录缺失导致恢复
// 失败后，换等价写法重放仍必须返回原作业，不能当成内容变化而冲突。
func TestReplaySingleDependencyEquivalentFormAfterMissingUpstream(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	u := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u", Values: []int64{1, 2}})
	waitStatus(t, s, u.ID, StatusSucceeded)
	// 作业 s 以单依赖方式接受，作业 l 以只含同一作业号的列表接受。
	single := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "s", Values: []int64{10},
		HasDependency: true, DependencyID: u.ID,
	})
	list := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "l", Values: []int64{20},
		Dependencies: []uint64{u.ID},
	})
	waitStatus(t, s, single.ID, StatusSucceeded)
	waitStatus(t, s, list.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, jobFileName(u.ID))); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	gs := waitStatus(t, s2, single.ID, StatusFailed)
	gl := waitStatus(t, s2, list.ID, StatusFailed)

	// 单依赖 → 列表写法。
	r1, err := s2.Submit(SubmitRequest{
		Submitter: "a", RequestID: "s", Values: []int64{10},
		Dependencies: []uint64{u.ID},
	})
	if err != nil || r1.ID != single.ID {
		t.Fatalf("单依赖原请求换列表写法: id=%d err=%v, want %d/nil",
			replayID(r1), err, single.ID)
	}
	if r1.Status != StatusFailed || r1.BlockerID != gs.BlockerID ||
		r1.FailureReason != gs.FailureReason {
		t.Fatalf("等价写法重放改写了失败记录: %+v vs %+v", r1, gs)
	}
	// 列表 → 单依赖写法。
	r2, err := s2.Submit(SubmitRequest{
		Submitter: "a", RequestID: "l", Values: []int64{20},
		HasDependency: true, DependencyID: u.ID,
	})
	if err != nil || r2.ID != list.ID {
		t.Fatalf("列表原请求换单依赖写法: id=%d err=%v, want %d/nil",
			replayID(r2), err, list.ID)
	}
	if r2.Status != StatusFailed || r2.BlockerID != gl.BlockerID ||
		r2.FailureReason != gl.FailureReason {
		t.Fatalf("等价写法重放改写了失败记录: %+v vs %+v", r2, gl)
	}
}

// 同人同号但内容相对原请求发生变化（整数内容或次序、种子、依赖内容或次序），
// 即使请求仍引用已经缺失的上游，也要返回幂等冲突并附原作业详情，
// 原记录保持原样；不能误判成“依赖不存在”，也不能返回原作业当成功重放。
func TestConflictUnderSameRequestIDDespiteMissingUpstream(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{1, 2}})
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{3}})
	waitStatus(t, s, u1.ID, StatusSucceeded)
	waitStatus(t, s, u2.ID, StatusSucceeded)
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{10}, Seed: 7,
		Dependencies: []uint64{u1.ID, u2.ID},
	})
	waitStatus(t, s, down.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, jobFileName(u2.ID))); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	before := waitStatus(t, s2, down.ID, StatusFailed)

	variants := []SubmitRequest{
		// 整数内容变化（仍引用缺失上游 u2）。
		{Submitter: "a", RequestID: "down", Values: []int64{11}, Seed: 7,
			Dependencies: []uint64{u1.ID, u2.ID}},
		// 整数次序变化。
		{Submitter: "a", RequestID: "down", Values: []int64{10, 0}, Seed: 7,
			Dependencies: []uint64{u1.ID, u2.ID}},
		// 种子变化。
		{Submitter: "a", RequestID: "down", Values: []int64{10}, Seed: 8,
			Dependencies: []uint64{u1.ID, u2.ID}},
		// 依赖次序变化（缺失上游换到最前）。
		{Submitter: "a", RequestID: "down", Values: []int64{10}, Seed: 7,
			Dependencies: []uint64{u2.ID, u1.ID}},
		// 依赖内容变化：只保留一个上游。
		{Submitter: "a", RequestID: "down", Values: []int64{10}, Seed: 7,
			Dependencies: []uint64{u1.ID}},
		// 依赖内容变化：引用一个从不存在的作业。
		{Submitter: "a", RequestID: "down", Values: []int64{10}, Seed: 7,
			Dependencies: []uint64{999}},
	}
	for i, v := range variants {
		j, err := s2.Submit(v)
		if !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("变体 %d: err=%v, want ErrIdempotencyConflict（即使引用缺失上游）", i, err)
		}
		if j == nil || j.ID != down.ID {
			t.Fatalf("变体 %d: 冲突须附原作业 %d，got %+v", i, down.ID, j)
		}
		if j.Status != StatusFailed || j.FailureReason != before.FailureReason ||
			j.BlockerID != before.BlockerID || !j.FinishedAt.Equal(before.FinishedAt) {
			t.Fatalf("变体 %d 改写了原失败记录: %+v vs %+v", i, j, before)
		}
	}
	// 原记录的参数与依赖列表保持原样，冲突不产生新记录。
	g, _ := s2.Get(down.ID)
	if g.Seed != 7 || len(g.Values) != 1 || g.Values[0] != 10 ||
		len(g.Dependencies) != 2 || g.Dependencies[0] != u1.ID || g.Dependencies[1] != u2.ID {
		t.Fatalf("冲突后原记录参数被改写: %+v", g)
	}
	if list, _ := s2.List("a", time.Time{}, time.Time{}); len(list) != 2 {
		var ids []uint64
		for _, j := range list {
			ids = append(ids, j.ID)
		}
		t.Fatalf("冲突产生了记录: %v", ids)
	}
	// 冲突错误中明确告知内容与原请求不一致。
	if _, err := s2.Submit(variants[0]); err != nil &&
		!strings.Contains(err.Error(), "与原请求不一致") {
		t.Fatalf("冲突错误未明确说明内容不一致: %v", err)
	}
}

// 提交人不同、请求号不同或请求号为空时按新请求处理：引用缺失上游一律拒绝，
// 不产生记录、不占用请求号；被拒后同一请求号改引仍存在的上游可以正常提交。
func TestNewRequestsReferencingMissingUpstreamRejected(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{1, 2}})
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{3}})
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{10},
		Dependencies: []uint64{u1.ID, u2.ID},
	})
	waitStatus(t, s, u1.ID, StatusSucceeded)
	waitStatus(t, s, u2.ID, StatusSucceeded)
	waitStatus(t, s, down.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, jobFileName(u2.ID))); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	waitStatus(t, s2, down.ID, StatusFailed)

	newRequests := []SubmitRequest{
		// 不同提交人、同请求号。
		{Submitter: "b", RequestID: "down", Values: []int64{10},
			Dependencies: []uint64{u1.ID, u2.ID}},
		// 同提交人、不同请求号。
		{Submitter: "a", RequestID: "fresh", Values: []int64{10},
			Dependencies: []uint64{u2.ID}},
		// 空请求号。
		{Submitter: "a", RequestID: "", Values: []int64{10},
			Dependencies: []uint64{u2.ID}},
	}
	for i, req := range newRequests {
		j, err := s2.Submit(req)
		if !errors.Is(err, ErrDependencyNotFound) {
			t.Fatalf("新请求 %d: err=%v, want ErrDependencyNotFound", i, err)
		}
		if j != nil {
			t.Fatalf("新请求 %d 被拒却返回了作业视图: %+v", i, j)
		}
	}
	// 拒绝不产生记录（现存只有 u1 与 down）。
	if list, _ := s2.List("a", time.Time{}, time.Time{}); len(list) != 2 {
		var ids []uint64
		for _, j := range list {
			ids = append(ids, j.ID)
		}
		t.Fatalf("被拒新请求产生了记录: %v", ids)
	}
	// "fresh" 未被占用：改引仍存在的 u1 后，同号提交作为新请求成功。
	reused := mustSubmit(t, s2, SubmitRequest{
		Submitter: "a", RequestID: "fresh", Values: []int64{10},
		Dependencies: []uint64{u1.ID},
	})
	if reused.ID == down.ID {
		t.Fatal("被拒请求号被占用，同号合法提交没有创建新作业")
	}
	waitStatus(t, s2, reused.ID, StatusSucceeded)
}

// 原请求号下的重放仍先做输入格式校验：空整数序列、依赖号为零、重复依赖、
// 单依赖与非空列表同时填写，继续返回原有对应拒绝错误，而不是按幂等返回。
func TestReplayShapeValidationStillRejects(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	u1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u1", Values: []int64{1, 2}})
	u2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "u2", Values: []int64{3}})
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: "down", Values: []int64{10},
		Dependencies: []uint64{u1.ID, u2.ID},
	})
	waitStatus(t, s, u1.ID, StatusSucceeded)
	waitStatus(t, s, u2.ID, StatusSucceeded)
	waitStatus(t, s, down.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, jobFileName(u2.ID))); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	before := waitStatus(t, s2, down.ID, StatusFailed)

	cases := []struct {
		name string
		req  SubmitRequest
		want error
	}{
		{"empty", SubmitRequest{Submitter: "a", RequestID: "down",
			Dependencies: []uint64{u1.ID, u2.ID}}, ErrEmptySequence},
		{"zero dep", SubmitRequest{Submitter: "a", RequestID: "down", Values: []int64{10},
			Dependencies: []uint64{0}}, ErrInvalidDependency},
		{"duplicate dep", SubmitRequest{Submitter: "a", RequestID: "down", Values: []int64{10},
			Dependencies: []uint64{u1.ID, u1.ID}}, ErrInvalidDependency},
		{"both modes", SubmitRequest{Submitter: "a", RequestID: "down", Values: []int64{10},
			HasDependency: true, DependencyID: u1.ID, Dependencies: []uint64{u1.ID}}, ErrInvalidDependency},
	}
	for _, c := range cases {
		if _, err := s2.Submit(c.req); !errors.Is(err, c.want) {
			t.Fatalf("%s: err=%v want %v", c.name, err, c.want)
		}
	}
	// 格式拒绝不产生记录，也不改写原失败记录。
	g, _ := s2.Get(down.ID)
	if g.Status != StatusFailed || g.FailureReason != before.FailureReason ||
		g.BlockerID != before.BlockerID || !g.FinishedAt.Equal(before.FinishedAt) {
		t.Fatalf("格式拒绝改写了原记录: %+v vs %+v", g, before)
	}
	if list, _ := s2.List("a", time.Time{}, time.Time{}); len(list) != 2 {
		t.Fatal("格式拒绝产生了记录")
	}
}
