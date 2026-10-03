package numeric

import (
	"errors"
	"testing"
	"time"
)

// nul 是实际的 U+0000 字符；提交人与请求号都允许包含它，两个标识必须按
// 调用方给出的完整字符串区分，不能用分隔符拼接（否则下列两对
// (提交人, 请求号) 会塌缩成同一个键）：
//
//	("a",      "b\x00c")
//	("a\x00b", "c")
const nul = "\x00"

// TestNULSubmitterRequestIDAreDistinctKeys 覆盖核心串用缺陷：两对标识只是
// 把同一个零字符摆在了分隔符的两侧，必须被视为互不相关的提交。
func TestNULSubmitterRequestIDAreDistinctKeys(t *testing.T) {
	s, _ := openTestStore(t)

	// 内容完全相同（整数序列、种子、依赖都一致）也不能当成同一次请求。
	req1 := SubmitRequest{
		Submitter: "a", RequestID: "b" + nul + "c",
		Values: []int64{1, 2, 3}, Seed: 7,
	}
	req2 := SubmitRequest{
		Submitter: "a" + nul + "b", RequestID: "c",
		Values: []int64{1, 2, 3}, Seed: 7,
	}
	j1 := mustSubmit(t, s, req1)
	j2 := mustSubmit(t, s, req2) // 旧实现会在此处串用 j1 或误报幂等冲突
	if j1.ID == j2.ID {
		t.Fatalf("两组标识串用：都得到作业号 %d", j1.ID)
	}
	d1 := waitStatus(t, s, j1.ID, StatusSucceeded)
	d2 := waitStatus(t, s, j2.ID, StatusSucceeded)
	if d1.Submitter != req1.Submitter || d1.RequestID != req1.RequestID {
		t.Fatalf("作业 %d 标识错误：%q %q", j1.ID, d1.Submitter, d1.RequestID)
	}
	if d2.Submitter != req2.Submitter || d2.RequestID != req2.RequestID {
		t.Fatalf("作业 %d 标识错误：%q %q", j2.ID, d2.Submitter, d2.RequestID)
	}

	// 计算内容不同：第二组仍必须作为新作业接受，不能返回另一提交人的作业
	// 视图，也不能报幂等冲突。
	j3, err := s.Submit(SubmitRequest{
		Submitter: "a" + nul + "b", RequestID: "c2",
		Values: []int64{9}, Seed: 7,
	})
	if err != nil {
		t.Fatalf("不同内容的新标识组合被拒绝：%v", err)
	}
	if j3.ID == j1.ID || j3.ID == j2.ID {
		t.Fatalf("新提交串用了已有作业号：%d", j3.ID)
	}
	waitStatus(t, s, j3.ID, StatusSucceeded)

	// 按作业号读取：标识与参数各自准确。
	g1, _ := s.Get(j1.ID)
	g2, _ := s.Get(j2.ID)
	if g1.Submitter != "a" || g1.RequestID != "b"+nul+"c" {
		t.Fatalf("Get(%d)=%q,%q", j1.ID, g1.Submitter, g1.RequestID)
	}
	if g2.Submitter != "a"+nul+"b" || g2.RequestID != "c" {
		t.Fatalf("Get(%d)=%q,%q", j2.ID, g2.Submitter, g2.RequestID)
	}
	if g1.Archive == nil || g1.Archive.Submitter != "a" || g1.Archive.RequestID != "b"+nul+"c" {
		t.Fatalf("作业 %d 归档未原样保留提交信息：%+v", j1.ID, g1.Archive)
	}
	if g2.Archive == nil || g2.Archive.Submitter != "a"+nul+"b" || g2.Archive.RequestID != "c" {
		t.Fatalf("作业 %d 归档未原样保留提交信息：%+v", j2.ID, g2.Archive)
	}

	// 按提交人列举：含零字符的提交人也是完整匹配，互不可见。
	l1, _ := s.List("a", time.Time{}, time.Time{})
	l2, _ := s.List("a"+nul+"b", time.Time{}, time.Time{})
	if len(l1) != 1 || l1[0].ID != j1.ID {
		var ids []uint64
		for _, j := range l1 {
			ids = append(ids, j.ID)
		}
		t.Fatalf("List(\"a\")=%v，应只含作业 %d", ids, j1.ID)
	}
	if len(l2) != 2 || l2[0].ID != j2.ID || l2[1].ID != j3.ID {
		var ids []uint64
		for _, j := range l2 {
			ids = append(ids, j.ID)
		}
		t.Fatalf("List(\"a\\x00b\")=%v，应含作业 %d,%d", ids, j2.ID, j3.ID)
	}
}

// TestNULIdempotencyWithinSameIdentity ：同一完整（提交人, 请求号）的幂等
// 语义在标识含零字符时保持不变：同内容重放返回原作业；整数序列内容/次序、
// 种子、依赖内容/次序变化报冲突并返回原作业视图。
func TestNULIdempotencyWithinSameIdentity(t *testing.T) {
	s, _ := openTestStore(t)
	sub := "x" + nul + "y"
	req := "r" + nul + "q"
	base := mustSubmit(t, s, SubmitRequest{
		Submitter: sub, RequestID: req, Values: []int64{1, 2}, Seed: 4,
	})
	waitStatus(t, s, base.ID, StatusSucceeded)

	// 完全相同 → 原作业、当前状态，不增加记录。
	again, err := s.Submit(SubmitRequest{
		Submitter: sub, RequestID: req, Values: []int64{1, 2}, Seed: 4,
	})
	if err != nil || again.ID != base.ID || again.Status != StatusSucceeded {
		t.Fatalf("同内容重放：id=%d status=%s err=%v", replayID(again), again.Status, err)
	}
	if got, _ := s.List(sub, time.Time{}, time.Time{}); len(got) != 1 {
		t.Fatalf("重放增加了记录：%d", len(got))
	}

	variants := []SubmitRequest{
		{Submitter: sub, RequestID: req, Values: []int64{2, 1}, Seed: 4}, // 次序变化
		{Submitter: sub, RequestID: req, Values: []int64{1, 3}, Seed: 4}, // 内容变化
		{Submitter: sub, RequestID: req, Values: []int64{1, 2}, Seed: 5}, // 种子变化
	}
	for i, v := range variants {
		j, err := s.Submit(v)
		if !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("变体 %d：err=%v，要幂等冲突", i, err)
		}
		if j == nil || j.ID != base.ID || j.Submitter != sub || j.RequestID != req {
			t.Fatalf("变体 %d 的冲突视图不是原作业：%+v", i, j)
		}
	}

	// 依赖内容/次序变化也是冲突；单依赖方式与只含同一上游的列表是相同内容。
	dep1 := mustSubmit(t, s, SubmitRequest{Submitter: sub, Values: []int64{10}})
	dep2 := mustSubmit(t, s, SubmitRequest{Submitter: sub, Values: []int64{20}})
	waitStatus(t, s, dep1.ID, StatusSucceeded)
	waitStatus(t, s, dep2.ID, StatusSucceeded)

	withList := SubmitRequest{
		Submitter: sub, RequestID: "dep" + nul, Values: []int64{1, 2}, Seed: 4,
		Dependencies: []uint64{dep1.ID},
	}
	jl := mustSubmit(t, s, withList)
	waitStatus(t, s, jl.ID, StatusSucceeded)
	// 单依赖方式重放同一上游：相同内容，返回原作业。
	single := withList
	single.Dependencies = nil
	single.HasDependency = true
	single.DependencyID = dep1.ID
	if replay, err := s.Submit(single); err != nil || replay.ID != jl.ID {
		t.Fatalf("单依赖与单元素列表应等价：id=%d err=%v，要 %d", replayID(replay), err, jl.ID)
	}
	// 依赖次序变化 → 冲突：先以两上游列表建立基线，再调换次序提交。
	two := withList
	two.RequestID = "two" + nul
	two.Dependencies = []uint64{dep1.ID, dep2.ID}
	jtwo := mustSubmit(t, s, two)
	waitStatus(t, s, jtwo.ID, StatusSucceeded)
	swapped := two
	swapped.Dependencies = []uint64{dep2.ID, dep1.ID}
	if j, err := s.Submit(swapped); !errors.Is(err, ErrIdempotencyConflict) ||
		j == nil || j.ID != jtwo.ID {
		t.Fatalf("依赖次序变化应冲突并返回原作业 %d：j=%+v err=%v", jtwo.ID, j, err)
	}
}

// TestNULRequestIDNonEmptyEnablesIdempotency ：只含零字符的请求号属于非空
// 请求号，重复提交必须找到自己的原作业；空请求号仍不启用幂等。
func TestNULRequestIDNonEmptyEnablesIdempotency(t *testing.T) {
	s, _ := openTestStore(t)
	withNUL := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: nul, Values: []int64{1, 2},
	})
	waitStatus(t, s, withNUL.ID, StatusSucceeded)
	again, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: nul, Values: []int64{1, 2},
	})
	if err != nil || again.ID != withNUL.ID {
		t.Fatalf("零字符请求号重放：id=%d err=%v，要 %d", replayID(again), err, withNUL.ID)
	}
	// 两个零字符与一个零字符是不同的非空请求号。
	twoNULs := mustSubmit(t, s, SubmitRequest{
		Submitter: "a", RequestID: nul + nul, Values: []int64{1, 2},
	})
	if twoNULs.ID == withNUL.ID {
		t.Fatal("\"\\x00\" 与 \"\\x00\\x00\" 被当成同一请求号")
	}
	waitStatus(t, s, twoNULs.ID, StatusSucceeded)

	// 空请求号不启用幂等：每次合法提交都创建新作业。
	e1 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "", Values: []int64{5}})
	e2 := mustSubmit(t, s, SubmitRequest{Submitter: "a", RequestID: "", Values: []int64{5}})
	if e1.ID == e2.ID {
		t.Fatal("空请求号的重复提交不应命中幂等")
	}
	waitStatus(t, s, e1.ID, StatusSucceeded)
	waitStatus(t, s, e2.ID, StatusSucceeded)
}

// TestNULIdempotencyKeysSurviveReopen ：关闭并重新打开同一归档目录后，两对
// 含零字符的标识仍分别指向各自原来的作业号；重放不创建额外记录，且记录
// 加载次序不影响索引指向。已成功归档的参数、摘要与校验值不得被改写。
func TestNULIdempotencyKeysSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	req1 := SubmitRequest{
		Submitter: "a", RequestID: "b" + nul + "c",
		Values: []int64{1, 2, 3}, Seed: 7,
	}
	req2 := SubmitRequest{
		Submitter: "a" + nul + "b", RequestID: "c",
		Values: []int64{1, 2, 3}, Seed: 7,
	}
	j1 := mustSubmit(t, s, req1)
	j2 := mustSubmit(t, s, req2)
	d1 := waitStatus(t, s, j1.ID, StatusSucceeded)
	d2 := waitStatus(t, s, j2.ID, StatusSucceeded)
	sum1, sum2 := d1.Archive.Sum, d2.Archive.Sum
	digest1, digest2 := d1.Archive.ResultDigest, d2.Archive.ResultDigest
	checksum1, checksum2 := d1.Archive.Checksum, d2.Archive.Checksum
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	g1, _ := s2.Get(j1.ID)
	g2, _ := s2.Get(j2.ID)
	if g1.Submitter != "a" || g1.RequestID != "b"+nul+"c" ||
		g2.Submitter != "a"+nul+"b" || g2.RequestID != "c" {
		t.Fatalf("重开后标识错位：%q,%q / %q,%q",
			g1.Submitter, g1.RequestID, g2.Submitter, g2.RequestID)
	}
	if g1.Archive == nil || g2.Archive == nil ||
		g1.Archive.Sum != sum1 || g2.Archive.Sum != sum2 ||
		g1.Archive.ResultDigest != digest1 || g2.Archive.ResultDigest != digest2 ||
		g1.Archive.Checksum != checksum1 || g2.Archive.Checksum != checksum2 {
		t.Fatal("重开后归档参数/摘要/校验值被改写")
	}

	// 各自重放必须命中各自的原作业。
	r1, err := s2.Submit(req1)
	if err != nil || r1.ID != j1.ID {
		t.Fatalf("重放组1：id=%d err=%v，要 %d", replayID(r1), err, j1.ID)
	}
	r2, err := s2.Submit(req2)
	if err != nil || r2.ID != j2.ID {
		t.Fatalf("重放组2：id=%d err=%v，要 %d", replayID(r2), err, j2.ID)
	}

	// 用组1的请求号但组2的完整提交人提交——不同键，必须建新作业，
	// 既不串用 j1/j2，也不误报冲突。
	fresh, err := s2.Submit(SubmitRequest{
		Submitter: "a" + nul + "b", RequestID: "b" + nul + "c",
		Values: []int64{1, 2, 3}, Seed: 7,
	})
	if err != nil {
		t.Fatalf("跨组组合被误判：%v", err)
	}
	if fresh.ID == j1.ID || fresh.ID == j2.ID {
		t.Fatalf("跨组组合串用作业号：%d", fresh.ID)
	}
	waitStatus(t, s2, fresh.ID, StatusSucceeded)

	if got, _ := s2.List("a", time.Time{}, time.Time{}); len(got) != 1 || got[0].ID != j1.ID {
		t.Fatal("重开后重放产生了额外记录或列举错位")
	}
}
