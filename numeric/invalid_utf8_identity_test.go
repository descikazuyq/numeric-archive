package numeric

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 本组测试覆盖修复目标：提交人、请求号含不能组成合法 UTF-8 的字节时，
// 关闭并重新打开同一归档后，按作业号查到的标识必须与提交时逐字节一致，
// 幂等作用域、作业号、状态与原始数值参数均不得因标识的保存方式改变。

// reopenStore 关闭 s 并以同一目录重新打开，返回新的归档。
func reopenStore(t *testing.T, s *Store) *Store {
	t.Helper()
	dir := s.Dir()
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	return s2
}

// TestInvalidUTF8RequestIDsAreDistinctJobs ：单字节 0xFF、单字节 0xFE 与
// 字符 U+FFFD 的 UTF-8 编码是三个不同的非空请求号；即使整数序列与种子
// 完全相同，也分别对应自己的作业，成功归档的参数逐字节一致。
func TestInvalidUTF8RequestIDsAreDistinctJobs(t *testing.T) {
	s, _ := openTestStore(t)
	const submitter = "user-中文"
	reqs := []string{"\xff", "\xfe", "�"}
	var ids []uint64
	for _, req := range reqs {
		j := mustSubmit(t, s, SubmitRequest{
			Submitter: submitter, RequestID: req,
			Values: []int64{1, 2, 3}, Seed: 9,
		})
		ids = append(ids, j.ID)
		d := waitStatus(t, s, j.ID, StatusSucceeded)
		if d.Submitter != submitter || d.RequestID != req {
			t.Fatalf("提交后标识不一致：%q,%q，要 %q,%q",
				d.Submitter, d.RequestID, submitter, req)
		}
		if a := d.Archive; a == nil || a.Submitter != submitter || a.RequestID != req {
			t.Fatalf("作业 %d 归档标识错误：%+v", j.ID, a)
		}
	}
	if ids[0] == ids[1] || ids[0] == ids[2] || ids[1] == ids[2] {
		t.Fatalf("三个请求号串用同一条记录：%v", ids)
	}

	s = reopenStore(t, s)

	// 重开后按作业号查询：提交人、请求号与提交时逐字节一致；数值参数不变。
	for i, req := range reqs {
		g, err := s.Get(ids[i])
		if err != nil {
			t.Fatalf("get %d: %v", ids[i], err)
		}
		if g.Submitter != submitter || g.RequestID != req {
			t.Fatalf("重开后作业 %d 标识变形：% x,% x，要 % x,% x",
				ids[i], g.Submitter, g.RequestID, submitter, req)
		}
		if g.Status != StatusSucceeded || g.Seed != 9 {
			t.Fatalf("作业 %d 状态/种子错误：%s seed=%d", ids[i], g.Status, g.Seed)
		}
		if len(g.Values) != 3 || g.Values[0] != 1 || g.Values[1] != 2 || g.Values[2] != 3 {
			t.Fatalf("作业 %d 原始数值参数被改：%v", ids[i], g.Values)
		}
		a := g.Archive
		if a == nil || a.Submitter != submitter || a.RequestID != req {
			t.Fatalf("重开后作业 %d 归档标识错误：%+v", ids[i], a)
		}
		if a.Sum != 6 || a.SumOfSquares != 14 {
			t.Fatalf("作业 %d 结果摘要改变：%d,%d", ids[i], a.Sum, a.SumOfSquares)
		}
		if a.InputsDigest != inputsDigestHex([]int64{1, 2, 3}, 9) {
			t.Fatalf("作业 %d 输入摘要改变", ids[i])
		}
		if a.ResultDigest != resultDigestHex([]int64{1, 2, 3}, 9, 6, 14) ||
			a.Checksum != checksumHex([]int64{1, 2, 3}, 9, 6, 14, a.Log, a.ResultDigest) {
			t.Fatalf("作业 %d 结果摘要/校验值改变", ids[i])
		}
	}

	// 用原请求再次提交：各自返回原作业号与当前状态，不增加记录。
	for i, req := range reqs {
		again, err := s.Submit(SubmitRequest{
			Submitter: submitter, RequestID: req,
			Values: []int64{1, 2, 3}, Seed: 9,
		})
		if err != nil || again.ID != ids[i] || again.Status != StatusSucceeded {
			t.Fatalf("重放请求 % x：id=%d status=%s err=%v，要 %d succeeded",
				req, replayID(again), again.Status, err, ids[i])
		}
	}
	if got, _ := s.List(submitter, time.Time{}, time.Time{}); len(got) != 3 {
		t.Fatalf("重放后记录数=%d，要 3", len(got))
	}

	// 用其中一个已用请求号提交不同整数序列：仍是幂等冲突，返回原作业视图，
	// 保留原记录。
	conflict, err := s.Submit(SubmitRequest{
		Submitter: submitter, RequestID: reqs[0],
		Values: []int64{1, 2, 4}, Seed: 9,
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("不同内容应冲突：%v", err)
	}
	if conflict == nil || conflict.ID != ids[0] ||
		conflict.RequestID != reqs[0] || conflict.Submitter != submitter {
		t.Fatalf("冲突视图不是原作业：%+v", conflict)
	}
	// 种子变化同样冲突。
	if _, err := s.Submit(SubmitRequest{
		Submitter: submitter, RequestID: reqs[1],
		Values: []int64{1, 2, 3}, Seed: 10,
	}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("种子变化应冲突：%v", err)
	}
	// 原记录的内容保持不变。
	g, _ := s.Get(ids[0])
	if g.Seed != 9 || len(g.Values) != 3 || g.Values[2] != 3 {
		t.Fatalf("冲突提交改写了原记录：seed=%d values=%v", g.Seed, g.Values)
	}
}

// TestInvalidUTF8SubmitterScopesRequestIDs ：提交人包含非法 UTF-8 字节时，
// 与其他字节不同的提交人之间不能共用请求号作用域，即使二者看起来都是
// 替换字符。
func TestInvalidUTF8SubmitterScopesRequestIDs(t *testing.T) {
	s, _ := openTestStore(t)
	submitters := []string{"\xff", "\xfe", "�"}
	const req = "same-request"
	var ids []uint64
	for _, sub := range submitters {
		j := mustSubmit(t, s, SubmitRequest{
			Submitter: sub, RequestID: req, Values: []int64{7},
		})
		ids = append(ids, j.ID)
		waitStatus(t, s, j.ID, StatusSucceeded)
	}
	if ids[0] == ids[1] || ids[0] == ids[2] || ids[1] == ids[2] {
		t.Fatalf("不同提交人共用了请求号作用域：%v", ids)
	}

	s = reopenStore(t, s)

	// 各提交人重放各自的请求号，命中各自的原作业。
	for i, sub := range submitters {
		again, err := s.Submit(SubmitRequest{
			Submitter: sub, RequestID: req, Values: []int64{7},
		})
		if err != nil || again.ID != ids[i] {
			t.Fatalf("提交人 % x 重放：id=%d err=%v，要 %d",
				sub, replayID(again), err, ids[i])
		}
	}
	// 按提交人列举：完整字符串匹配，显示相似也不能混入他人的作业。
	for i, sub := range submitters {
		got, err := s.List(sub, time.Time{}, time.Time{})
		if err != nil || len(got) != 1 || got[0].ID != ids[i] {
			t.Fatalf("List(% x)=%v err=%v，应只含作业 %d",
				sub, idsOf(got), err, ids[i])
		}
	}
}

// TestInvalidUTF8IdentitiesMixedWithNULAndText ：普通中文、零字符与非法
// UTF-8 字节混用的标识各自独立，空请求号不启用幂等、只含零字符的请求号
// 启用幂等的既有区别保持不变。
func TestInvalidUTF8IdentitiesMixedWithNULAndText(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	type spec struct {
		sub string
		req string
	}
	specs := []spec{
		{"中文提交人", "请求-001"},
		{"a\x00b", "r\x00"},
		{"s\xff", "\xfe"},
		{"\x00", "\x00"},
	}
	want := make(map[spec]uint64)
	for _, sp := range specs {
		j := mustSubmit(t, s, SubmitRequest{
			Submitter: sp.sub, RequestID: sp.req, Values: []int64{2, 4}, Seed: 1,
		})
		want[sp] = j.ID
		waitStatus(t, s, j.ID, StatusSucceeded)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for sp, id := range want {
		g, _ := s2.Get(id)
		if g.Submitter != sp.sub || g.RequestID != sp.req {
			t.Fatalf("重开后 %v 标识变形：% x,% x", id, g.Submitter, g.RequestID)
		}
		again, err := s2.Submit(SubmitRequest{
			Submitter: sp.sub, RequestID: sp.req, Values: []int64{2, 4}, Seed: 1,
		})
		if err != nil || again.ID != id {
			t.Fatalf("%v 重放：id=%d err=%v，要 %d", sp, replayID(again), err, id)
		}
	}

	// 只含零字符的请求号启用幂等；空请求号不启用。
	nulReq, err := s2.Submit(SubmitRequest{
		Submitter: "z", RequestID: "\x00", Values: []int64{1},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, s2, nulReq.ID, StatusSucceeded)
	again, err := s2.Submit(SubmitRequest{
		Submitter: "z", RequestID: "\x00", Values: []int64{1},
	})
	if err != nil || again.ID != nulReq.ID {
		t.Fatalf("零字符请求号重放：id=%d err=%v，要 %d", replayID(again), err, nulReq.ID)
	}
	e1 := mustSubmit(t, s2, SubmitRequest{Submitter: "z", RequestID: "", Values: []int64{1}})
	e2 := mustSubmit(t, s2, SubmitRequest{Submitter: "z", RequestID: "", Values: []int64{1}})
	if e1.ID == e2.ID {
		t.Fatal("空请求号不应启用幂等")
	}
	waitStatus(t, s2, e1.ID, StatusSucceeded)
	waitStatus(t, s2, e2.ID, StatusSucceeded)
}

// TestOldPlainTextArchiveOpensWithoutConversion ：既有普通文本与含零字符的
// 归档（旧编码器写出的 JSON）无需任何转换即可继续打开，标识原样恢复。
func TestOldPlainTextArchiveOpensWithoutConversion(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	writeSyntheticRecord(t, dir, &storedJob{
		id: 1, submitter: "alice", requestID: "r\x001",
		values:   []int64{1, 2},
		queuedAt: base,
		status:   StatusFailed,
	})
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("旧归档应可直接打开：%v", err)
	}
	defer s.Close()
	g, err := s.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if g.Submitter != "alice" || g.RequestID != "r\x001" {
		t.Fatalf("旧记录标识错误：%q,%q", g.Submitter, g.RequestID)
	}
	// 旧幂等号继续生效。
	again, err := s.Submit(SubmitRequest{
		Submitter: "alice", RequestID: "r\x001", Values: []int64{1, 2},
	})
	if err != nil || again.ID != 1 {
		t.Fatalf("旧幂等号重放：id=%d err=%v，要 1", replayID(again), err)
	}
}

// TestOldStyleEscapedJSONOpens ：旧版本经标准 encoding/json 写出的记录会
// 使用 \u0000、< 等转义；新解码器必须照常读出原始字节。
func TestOldStyleEscapedJSONOpens(t *testing.T) {
	dir := t.TempDir()
	written := `{
  "version": "numeric-job-v1",
  "id": 1,
  "submitter": "a<>b",
  "request_id": "q\u00001",
  "seed": 0,
  "values": [1],
  "has_dependency": false,
  "dependency_id": 0,
  "queued_at": "2026-03-04T05:06:07Z",
  "status": "failed"
}`
	if err := os.WriteFile(filepath.Join(dir, jobFileName(1)), []byte(written), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("旧转义记录应可打开：%v", err)
	}
	defer s.Close()
	g, _ := s.Get(1)
	if g.Submitter != "a<>b" || g.RequestID != "q\x001" {
		t.Fatalf("旧转义解码错误：%q,%q", g.Submitter, g.RequestID)
	}
	again, err := s.Submit(SubmitRequest{
		Submitter: "a<>b", RequestID: "q\x001", Values: []int64{1},
	})
	if err != nil || again.ID != 1 {
		t.Fatalf("旧转义记录幂等号重放：id=%d err=%v，要 1", replayID(again), err)
	}
}

func idsOf(js []*Job) []uint64 {
	out := make([]uint64, len(js))
	for i, j := range js {
		out[i] = j.ID
	}
	return out
}
