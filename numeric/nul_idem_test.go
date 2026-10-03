package numeric

import (
	"errors"
	"testing"
	"time"
)

// 含零字符（U+0000）的提交人与请求号必须按完整字符串区分：
// ("a", "b\x00c") 与 ("a\x00b", "c") 是两组不同的幂等键，
// 即使内容完全一致也必须分别接受为两个作业。
func TestSubmit_NULIdentifiersDistinct(t *testing.T) {
	s, _ := openTestStore(t)

	req1 := SubmitRequest{Submitter: "a", RequestID: "b\x00c", Values: []int64{1, 2, 3}, Seed: 7}
	req2 := SubmitRequest{Submitter: "a\x00b", RequestID: "c", Values: []int64{1, 2, 3}, Seed: 7}

	j1 := mustSubmit(t, s, req1)
	j2, err := s.Submit(req2)
	if err != nil {
		t.Fatalf("第二组提交被误拒: %v", err)
	}
	if j2.ID == j1.ID {
		t.Fatalf("两组不同标识被当成同一次请求，同得作业号 %d", j1.ID)
	}

	// 各自按完整标识重复提交（同内容）应返回原作业，不新增记录。
	again1 := mustSubmit(t, s, req1)
	if again1.ID != j1.ID {
		t.Fatalf("第一组重复提交得到作业号 %d，期望原作业 %d", again1.ID, j1.ID)
	}
	again2 := mustSubmit(t, s, req2)
	if again2.ID != j2.ID {
		t.Fatalf("第二组重复提交得到作业号 %d，期望原作业 %d", again2.ID, j2.ID)
	}
	if jobs, err := s.List("", time.Time{}, time.Time{}); err != nil || len(jobs) != 0 {
		t.Fatalf("空提交人不应列出记录: jobs=%v err=%v", jobs, err)
	}
	l1, err := s.List("a", time.Time{}, time.Time{})
	if err != nil || len(l1) != 1 || l1[0].ID != j1.ID {
		t.Fatalf("按提交人 a 列举应只看到作业 %d: jobs=%v err=%v", j1.ID, l1, err)
	}
	if l1[0].Submitter != "a" || l1[0].RequestID != "b\x00c" {
		t.Fatalf("作业 %d 标识被改写: submitter=%q requestID=%q", j1.ID, l1[0].Submitter, l1[0].RequestID)
	}
	l2, err := s.List("a\x00b", time.Time{}, time.Time{})
	if err != nil || len(l2) != 1 || l2[0].ID != j2.ID {
		t.Fatalf("按提交人 a\\x00b 列举应只看到作业 %d: jobs=%v err=%v", j2.ID, l2, err)
	}
	if l2[0].Submitter != "a\x00b" || l2[0].RequestID != "c" {
		t.Fatalf("作业 %d 标识被改写: submitter=%q requestID=%q", j2.ID, l2[0].Submitter, l2[0].RequestID)
	}

	// 两组都成功归档，归档中原样保留各自的提交信息。
	d1 := waitStatus(t, s, j1.ID, StatusSucceeded)
	d2 := waitStatus(t, s, j2.ID, StatusSucceeded)
	if d1.Archive.Submitter != "a" || d1.Archive.RequestID != "b\x00c" {
		t.Fatalf("作业 %d 归档标识被改写: submitter=%q requestID=%q",
			j1.ID, d1.Archive.Submitter, d1.Archive.RequestID)
	}
	if d2.Archive.Submitter != "a\x00b" || d2.Archive.RequestID != "c" {
		t.Fatalf("作业 %d 归档标识被改写: submitter=%q requestID=%q",
			j2.ID, d2.Archive.Submitter, d2.Archive.RequestID)
	}
}

// 两组标识的计算内容不同时，第二组仍应作为新作业接受，
// 不得返回另一提交人的作业视图或冲突错误。
func TestSubmit_NULIdentifiersDifferentContent(t *testing.T) {
	s, _ := openTestStore(t)

	j1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "b\x00c", Values: []int64{1}, Seed: 1})
	j2, err := s.Submit(SubmitRequest{Submitter: "a\x00b", RequestID: "c", Values: []int64{9, 9}, Seed: 2})
	if err != nil {
		t.Fatalf("不同内容的第二组提交被误拒: %v", err)
	}
	if j2.ID == j1.ID {
		t.Fatalf("不同内容的第二组提交复用了作业号 %d", j1.ID)
	}
	got, err := s.Get(j2.ID)
	if err != nil {
		t.Fatalf("get %d: %v", j2.ID, err)
	}
	if got.Submitter != "a\x00b" || got.RequestID != "c" || got.Seed != 2 ||
		len(got.Values) != 2 || got.Values[0] != 9 || got.Values[1] != 9 {
		t.Fatalf("作业 %d 视图串用了另一组参数: %+v", j2.ID, got)
	}
}

// 同一完整提交人与同一非空请求号下，内容变化仍返回幂等冲突与原作业视图。
func TestSubmit_NULIdentifierConflict(t *testing.T) {
	s, _ := openTestStore(t)

	orig := mustSubmit(t, s, SubmitRequest{Submitter: "a\x00b", RequestID: "c", Values: []int64{1, 2}, Seed: 3})
	view, err := s.Submit(SubmitRequest{Submitter: "a\x00b", RequestID: "c", Values: []int64{2, 1}, Seed: 3})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("内容变化应返回幂等冲突，实际: %v", err)
	}
	if view == nil || view.ID != orig.ID {
		t.Fatalf("冲突应返回原作业 %d 的视图，实际: %+v", orig.ID, view)
	}
	if len(view.Values) != 2 || view.Values[0] != 1 || view.Values[1] != 2 {
		t.Fatalf("原作业视图内容被改写: %+v", view.Values)
	}
}

// 只含零字符的请求号属于非空请求号：启用幂等，重复提交找到自己的原作业。
func TestSubmit_NULOnlyRequestID(t *testing.T) {
	s, _ := openTestStore(t)

	req := SubmitRequest{Submitter: "bob", RequestID: "\x00", Values: []int64{4, 5}, Seed: 0}
	j1 := mustSubmit(t, s, req)
	j2 := mustSubmit(t, s, req)
	if j2.ID != j1.ID {
		t.Fatalf("只含零字符的非空请求号重复提交应返回原作业 %d，实际 %d", j1.ID, j2.ID)
	}
	// 空请求号不启用幂等，每次都应创建新作业。
	e1 := mustSubmit(t, s, SubmitRequest{Submitter: "bob", RequestID: "", Values: []int64{4, 5}})
	e2 := mustSubmit(t, s, SubmitRequest{Submitter: "bob", RequestID: "", Values: []int64{4, 5}})
	if e1.ID == e2.ID {
		t.Fatalf("空请求号不应启用幂等，两次提交同得作业号 %d", e1.ID)
	}
}

// 关闭并重新打开同一目录后，两组含零字符的标识仍分别对应各自原来的作业号，
// 重放不创建额外记录。
func TestSubmit_NULIdentifiersPersistAcrossReopen(t *testing.T) {
	s, clock := openTestStore(t)
	dir := s.Dir()

	req1 := SubmitRequest{Submitter: "a", RequestID: "b\x00c", Values: []int64{1, 2, 3}, Seed: 7}
	req2 := SubmitRequest{Submitter: "a\x00b", RequestID: "c", Values: []int64{1, 2, 3}, Seed: 7}
	j1 := mustSubmit(t, s, req1)
	j2 := mustSubmit(t, s, req2)
	waitStatus(t, s, j1.ID, StatusSucceeded)
	waitStatus(t, s, j2.ID, StatusSucceeded)
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := Open(dir, WithClock(clock))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	r1 := mustSubmit(t, s2, req1)
	if r1.ID != j1.ID {
		t.Fatalf("重开后第一组重放得到作业号 %d，期望原作业 %d", r1.ID, j1.ID)
	}
	r2 := mustSubmit(t, s2, req2)
	if r2.ID != j2.ID {
		t.Fatalf("重开后第二组重放得到作业号 %d，期望原作业 %d", r2.ID, j2.ID)
	}
	// 重放不得产生新记录：目录中仍只有原来的两个作业。
	all, err := s2.List("a", time.Time{}, time.Time{})
	if err != nil || len(all) != 1 {
		t.Fatalf("重开后提交人 a 应只有 1 条记录: jobs=%v err=%v", all, err)
	}
	all2, err := s2.List("a\x00b", time.Time{}, time.Time{})
	if err != nil || len(all2) != 1 {
		t.Fatalf("重开后提交人 a\\x00b 应只有 1 条记录: jobs=%v err=%v", all2, err)
	}
	// 归档在重开后仍完整可读且标识原样保留。
	d1, err := s2.Get(j1.ID)
	if err != nil || d1.Status != StatusSucceeded || d1.Archive == nil {
		t.Fatalf("重开后作业 %d 归档不可读: %+v err=%v", j1.ID, d1, err)
	}
	if d1.Archive.Submitter != "a" || d1.Archive.RequestID != "b\x00c" {
		t.Fatalf("重开后作业 %d 归档标识被改写: submitter=%q requestID=%q",
			j1.ID, d1.Archive.Submitter, d1.Archive.RequestID)
	}
	d2, err := s2.Get(j2.ID)
	if err != nil || d2.Status != StatusSucceeded || d2.Archive == nil {
		t.Fatalf("重开后作业 %d 归档不可读: %+v err=%v", j2.ID, d2, err)
	}
	if d2.Archive.Submitter != "a\x00b" || d2.Archive.RequestID != "c" {
		t.Fatalf("重开后作业 %d 归档标识被改写: submitter=%q requestID=%q",
			j2.ID, d2.Archive.Submitter, d2.Archive.RequestID)
	}
}
