package numeric

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// 三个“看起来可能一样”、但字节互不相同的非空请求号：
// 单字节 0xFF、单字节 0xFE（二者都不是合法 UTF-8），以及替换字符
// U+FFFD 的合法 UTF-8 编码 EF BF BD。旧实现落盘后前两者都会被
// encoding/json 改写成 U+FFFD，三个标识在重新打开归档后塌缩成同一个。
var invalidUTF8Requests = []string{"\xff", "\xfe", "\uFFFD"}

// 中文标识、零字符与非法 UTF-8 字节混排，确认保真规则不是只对单字节生效。
var mixedIdentitySub = "提交人\xff\x00尾"
var mixedIdentityReq = "请求\xfe号\x00\xff"

func TestInvalidUTF8RequestsAreDistinctBeforeReopen(t *testing.T) {
	s, _ := openTestStore(t)
	sub := "alice"
	var ids []uint64
	for _, req := range invalidUTF8Requests {
		j := mustSubmit(t, s, SubmitRequest{
			Submitter: sub, RequestID: req,
			Values: []int64{1, 2, 3}, Seed: 9,
		})
		ids = append(ids, j.ID)
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] == ids[0] {
			t.Fatalf("请求号 %x 与 %x 串用同一作业号 %d",
				invalidUTF8Requests[i], invalidUTF8Requests[0], ids[0])
		}
	}
	for i, req := range invalidUTF8Requests {
		d := waitStatus(t, s, ids[i], StatusSucceeded)
		if d.Submitter != sub || d.RequestID != req {
			t.Fatalf("作业 %d 标识失真：%x / %x，要 %x / %x",
				ids[i], d.Submitter, d.RequestID, sub, req)
		}
	}
}

// TestInvalidUTF8IdentitySurvivesReopen 是核心回归：关闭再打开同一归档后，
// 按作业号查到的提交人、请求号与提交时逐字节一致，归档中的参数同样一致，
// 作业号/状态/原始数值参数/结果摘要/日志/校验值都不因保存方式改变。
func TestInvalidUTF8IdentitySurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	type submitted struct {
		req SubmitRequest
		id  uint64
	}
	var jobs []submitted
	for _, req := range invalidUTF8Requests {
		r := SubmitRequest{
			Submitter: "bob", RequestID: req,
			Values: []int64{2, 4, -1, 7}, Seed: 99,
		}
		j := mustSubmit(t, s, r)
		jobs = append(jobs, submitted{req: r, id: j.ID})
	}
	// 再提交一个混合标识（中文 + 非法字节 + 零字符）的作业。
	mixed := SubmitRequest{
		Submitter: mixedIdentitySub, RequestID: mixedIdentityReq,
		Values: []int64{5, -5}, Seed: 13,
	}
	mj := mustSubmit(t, s, mixed)

	snapshot := map[uint64]*Job{}
	for _, p := range jobs {
		snapshot[p.id] = waitStatus(t, s, p.id, StatusSucceeded)
	}
	snapshot[mj.ID] = waitStatus(t, s, mj.ID, StatusSucceeded)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	for _, p := range jobs {
		before := snapshot[p.id]
		g, err := s2.Get(p.id)
		if err != nil {
			t.Fatal(err)
		}
		if g.Submitter != p.req.Submitter || g.RequestID != p.req.RequestID {
			t.Fatalf("作业 %d 重开后标识失真：%x / %x，要 %x / %x",
				p.id, g.Submitter, g.RequestID, p.req.Submitter, p.req.RequestID)
		}
		if g.Status != before.Status || g.Seed != before.Seed ||
			len(g.Values) != len(before.Values) {
			t.Fatalf("作业 %d 状态/原始参数被改写：%+v", p.id, g)
		}
		for i := range before.Values {
			if g.Values[i] != before.Values[i] {
				t.Fatalf("作业 %d 原始数值参数被改写", p.id)
			}
		}
		if g.Archive == nil {
			t.Fatalf("作业 %d 重开后丢失成功归档", p.id)
		}
		if g.Archive.Submitter != p.req.Submitter || g.Archive.RequestID != p.req.RequestID {
			t.Fatalf("作业 %d 归档标识失真：%x / %x",
				p.id, g.Archive.Submitter, g.Archive.RequestID)
		}
		if g.Archive.Sum != before.Archive.Sum ||
			g.Archive.SumOfSquares != before.Archive.SumOfSquares ||
			g.Archive.ResultDigest != before.Archive.ResultDigest ||
			g.Archive.InputsDigest != before.Archive.InputsDigest ||
			g.Archive.Log != before.Archive.Log ||
			g.Archive.Checksum != before.Archive.Checksum {
			t.Fatalf("作业 %d 的结果摘要/日志/校验值被改写", p.id)
		}
	}
	mg, err := s2.Get(mj.ID)
	if err != nil {
		t.Fatal(err)
	}
	if mg.Submitter != mixed.Submitter || mg.RequestID != mixed.RequestID {
		t.Fatalf("混合标识作业重开后失真：%x / %x，要 %x / %x",
			mg.Submitter, mg.RequestID, mixed.Submitter, mixed.RequestID)
	}
	if mg.Archive == nil ||
		mg.Archive.Submitter != mixed.Submitter || mg.Archive.RequestID != mixed.RequestID {
		t.Fatalf("混合标识作业归档失真：%+v", mg.Archive)
	}
}

// TestInvalidUTF8IdempotencyAfterReopen ：重开后用原请求重放必须返回各自
// 原作业号且不新增记录；用已用请求号提交不同整数序列或种子，仍是既有的
// 幂等冲突错误，视图指向该请求自己的原作业，原记录保留。
func TestInvalidUTF8IdempotencyAfterReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	sub := "carol"
	baseReq := []SubmitRequest{
		{Submitter: sub, RequestID: invalidUTF8Requests[0], Values: []int64{1, 2}, Seed: 4},
		{Submitter: sub, RequestID: invalidUTF8Requests[1], Values: []int64{1, 2}, Seed: 4},
		{Submitter: sub, RequestID: invalidUTF8Requests[2], Values: []int64{1, 2}, Seed: 4},
	}
	var baseIDs []uint64
	for _, r := range baseReq {
		baseIDs = append(baseIDs, mustSubmit(t, s, r).ID)
	}
	for _, id := range baseIDs {
		waitStatus(t, s, id, StatusSucceeded)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	for i, r := range baseReq {
		replay, err := s2.Submit(r)
		if err != nil || replay.ID != baseIDs[i] || replay.Status != StatusSucceeded {
			t.Fatalf("请求 %x 重放：id=%d status=%s err=%v，要原作业 %d",
				r.RequestID, replayID(replay), replay.Status, err, baseIDs[i])
		}
	}

	// 用 0xFF 请求号提交不同内容 → 冲突，且视图是 0xFF 自己的原作业，
	// 不能返回 0xFE 或 U+FFFD 的作业（旧实现会在这里串用）。
	conflictOwner := baseIDs[0]
	variants := []SubmitRequest{
		{Submitter: sub, RequestID: invalidUTF8Requests[0], Values: []int64{2, 1}, Seed: 4},
		{Submitter: sub, RequestID: invalidUTF8Requests[0], Values: []int64{1, 2}, Seed: 5},
	}
	for i, v := range variants {
		j, err := s2.Submit(v)
		if !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("变体 %d：err=%v，要幂等冲突", i, err)
		}
		if j == nil || j.ID != conflictOwner {
			owned := uint64(0)
			if j != nil {
				owned = j.ID
			}
			t.Fatalf("变体 %d 冲突视图指向作业 %d，要 %d", i, owned, conflictOwner)
		}
		if j.RequestID != invalidUTF8Requests[0] || j.Submitter != sub {
			t.Fatalf("变体 %d 冲突视图标失真：%x / %x",
				i, j.Submitter, j.RequestID)
		}
	}
	// 原记录保留：三个请求各自仍只对应一个作业。
	if got, _ := s2.List(sub, time.Time{}, time.Time{}); len(got) != len(baseReq) {
		var ids []uint64
		for _, j := range got {
			ids = append(ids, j.ID)
		}
		t.Fatalf("冲突/重放不应改变记录数，List=%v，要 %d 条", ids, len(baseReq))
	}

	// 0xFE 的原请求号正常重放仍命中它自己的作业，与上面的冲突互不相干。
	rep, err := s2.Submit(baseReq[1])
	if err != nil || rep.ID != baseIDs[1] {
		t.Fatalf("0xFE 重放被 0xFF 的冲突污染：id=%d err=%v，要 %d",
			replayID(rep), err, baseIDs[1])
	}
}

// TestInvalidUTF8SubmitterScopesRequestIDs ：提交人含非法 UTF-8 字节时，
// 字节不同的提交人不得共用请求号作用域；重开后仍然隔离。按提交人列举也
// 必须完整字符串匹配，显示相似不能混入别人的作业。
func TestInvalidUTF8SubmitterScopesRequestIDs(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	submitters := []string{"\xff", "\xfe", "\uFFFD", "中文\xff人"}
	commonReq := "shared\x00id"
	owner := map[string]uint64{}
	for _, sub := range submitters {
		j := mustSubmit(t, s, SubmitRequest{
			Submitter: sub, RequestID: commonReq,
			Values: []int64{3, 4}, Seed: 1,
		})
		owner[sub] = j.ID
	}
	seen := map[uint64]struct{}{}
	for sub, id := range owner {
		if _, dup := seen[id]; dup {
			t.Fatalf("不同提交人串用作业号 %d（提交人 %x）", id, sub)
		}
		seen[id] = struct{}{}
		waitStatus(t, s, id, StatusSucceeded)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	for _, sub := range submitters {
		// 同提交人同请求号 → 自己的原作业。
		j, err := s2.Submit(SubmitRequest{
			Submitter: sub, RequestID: commonReq,
			Values: []int64{3, 4}, Seed: 1,
		})
		if err != nil || j.ID != owner[sub] {
			t.Fatalf("提交人 %x 重放：id=%d err=%v，要 %d",
				sub, replayID(j), err, owner[sub])
		}
		// 列举只按完整字符串匹配：每个提交人只能看到自己那一条。
		list, err := s2.List(sub, time.Time{}, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 1 || list[0].ID != owner[sub] {
			var ids []uint64
			for _, got := range list {
				ids = append(ids, got.ID)
			}
			t.Fatalf("List(%x)=%v，要只含作业 %d", sub, ids, owner[sub])
		}
		if list[0].Submitter != sub || list[0].RequestID != commonReq {
			t.Fatalf("List(%x) 视图标失真：%x / %x",
				sub, list[0].Submitter, list[0].RequestID)
		}
	}
}

// TestInvalidUTF8EmptyAndNULOnlyRequestIDDistinction ：空请求号不启用幂等、
// 只含零字符的请求号启用幂等——这一区别在提交人含非法字节时保持不变。
func TestInvalidUTF8EmptyAndNULOnlyRequestIDDistinction(t *testing.T) {
	s, _ := openTestStore(t)
	sub := "dave\xff"

	nulOnly := mustSubmit(t, s, SubmitRequest{
		Submitter: sub, RequestID: "\x00", Values: []int64{1},
	})
	waitStatus(t, s, nulOnly.ID, StatusSucceeded)
	again, err := s.Submit(SubmitRequest{
		Submitter: sub, RequestID: "\x00", Values: []int64{1},
	})
	if err != nil || again.ID != nulOnly.ID {
		t.Fatalf("零字符请求号重放：id=%d err=%v，要 %d",
			replayID(again), err, nulOnly.ID)
	}

	e1 := mustSubmit(t, s, SubmitRequest{Submitter: sub, RequestID: "", Values: []int64{1}})
	e2 := mustSubmit(t, s, SubmitRequest{Submitter: sub, RequestID: "", Values: []int64{1}})
	if e1.ID == e2.ID {
		t.Fatal("空请求号即使提交人含非法字节也不应启用幂等")
	}
	waitStatus(t, s, e1.ID, StatusSucceeded)
	waitStatus(t, s, e2.ID, StatusSucceeded)
}

// TestIdentityRawEncodingRoundTrip 直接覆盖编码兜底规则：
//   - 合法 UTF-8（含零字符与普通中文）不产生 identity_raw，落盘格式与旧版一致；
//   - 含非法字节时写入 identity_raw，且可逐字节恢复；
//   - 看起来像 base64 的普通文本（"abcd"）不会被误判为兜底编码。
func TestIdentityRawEncodingRoundTrip(t *testing.T) {
	cases := []struct {
		submitter string
		requestID string
	}{
		{"alice", "r-1"},
		{"张三", "请求号\x00续"},
		{"a", "b\x00c"},
		{"a\x00b", "c"},
		{"\xff", "abcd"}, // "abcd" 恰为合法 base64，验证前缀无歧义
		{mixedIdentitySub, mixedIdentityReq},
		{"", "\xff"}, // 空提交人 + 非法字节请求号
	}
	for i, c := range cases {
		j := &storedJob{
			id:        uint64(i + 1),
			submitter: c.submitter,
			requestID: c.requestID,
			seed:      1,
			values:    []int64{1},
			status:    StatusQueued,
		}
		data, err := encodeRecord(j)
		if err != nil {
			t.Fatalf("case %d encode: %v", i, err)
		}
		wantRaw := !utf8.ValidString(c.submitter) || !utf8.ValidString(c.requestID)
		if strings.Contains(string(data), "identity_raw") != wantRaw {
			t.Fatalf("case %d identity_raw 存在性=%v，要 %v", i, !wantRaw, wantRaw)
		}
		var r jobRecord
		if err := json.Unmarshal(data, &r); err != nil {
			t.Fatalf("case %d decode: %v", i, err)
		}
		gotSub, gotReq, err := resolveIdentity(r.Submitter, r.RequestID, r.IdentityRaw)
		if err != nil {
			t.Fatalf("case %d resolve: %v", i, err)
		}
		if gotSub != c.submitter || gotReq != c.requestID {
			t.Fatalf("case %d 往返失真：%x / %x，要 %x / %x",
				i, gotSub, gotReq, c.submitter, c.requestID)
		}
	}
}

// TestLegacyPlainAndNULArchivesOpenWithoutConversion ：旧格式记录
// （没有 identity_raw 字段，标识含零字符）无需转换即可继续打开并生效。
func TestLegacyPlainAndNULArchivesOpenWithoutConversion(t *testing.T) {
	dir := t.TempDir()
	queuedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	data, err := encodeRecord(&storedJob{
		id: 1, submitter: "a", requestID: "b\x00c",
		seed: 2, values: []int64{8}, queuedAt: queuedAt, status: StatusQueued,
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "identity_raw") {
		t.Fatal("含零字符的合法 UTF-8 标识不应写出 identity_raw")
	}
	if err := writeFileAtomic(dir, jobFileName(1), data, 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	g, err := s.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if g.Submitter != "a" || g.RequestID != "b\x00c" {
		t.Fatalf("旧记录标识失真：%x / %x", g.Submitter, g.RequestID)
	}
	// 旧归档中的幂等号继续生效。
	replay, err := s.Submit(SubmitRequest{
		Submitter: "a", RequestID: "b\x00c", Values: []int64{8}, Seed: 2,
	})
	if err != nil || replay.ID != 1 {
		t.Fatalf("旧记录幂等号未恢复：id=%d err=%v，要 1", replayID(replay), err)
	}
}
