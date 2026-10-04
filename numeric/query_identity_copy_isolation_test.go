package numeric

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// 本文件覆盖带特殊字节标识的成功归档在“返回数据是独立副本”约定下的表现。
//
// 提交人与请求号继续按完整字节保留。这里的作业标识含不能组成合法 UTF-8 的
// 字节，并混有中文与 U+0000（mixedIdentitySub / mixedIdentityReq 定义在
// invalid_utf8_identity_test.go）；归档因此带有 IdentityRaw 兜底保存信息。
// 修改任何一份返回值中的提交人、请求号或 IdentityRaw，只能影响当前返回值：
// 再次按原提交人列举仍能找到原作业，详情与归档中的标识逐字节保持一致，
// 也不能因副本里的改动而把作业改归其他提交人。
//
// 普通中文标识、以及包含 U+0000 但字节整体仍是合法 UTF-8 的标识保持现有
// 查询结果，不要求它们额外提供原始字节信息（归档中没有 IdentityRaw）。

// corruptIdentityView 改写一份返回值中的全部标识字段：作业视图与归档中的
// 提交人、请求号，以及归档的 IdentityRaw 兜底保存信息。
func corruptIdentityView(v *Job, otherSub, otherReq string) {
	v.Submitter = otherSub
	v.RequestID = otherReq
	a := v.Archive
	a.Submitter = otherSub
	a.RequestID = otherReq
	if a.IdentityRaw != nil {
		// 用另一份（同样含非法字节的）标识的完整字节兜底表示替换，
		// 模拟调用方把保存信息改成别的字节。
		if raw := rawIdentity(otherSub, otherReq); raw != nil {
			a.IdentityRaw.Submitter = raw.Submitter
			a.IdentityRaw.RequestID = raw.RequestID
		} else {
			a.IdentityRaw.Submitter = rawBase64Prefix + "AAAA"
			a.IdentityRaw.RequestID = rawBase64Prefix + "BBBB"
		}
	}
}

// assertIdentityBytes 逐字节核对作业视图与归档中的提交人、请求号。
func assertIdentityBytes(t *testing.T, where, sub, req string, v *Job) {
	t.Helper()
	if v.Submitter != sub {
		t.Fatalf("%s: 作业提交人失真：%x，要 %x", where, v.Submitter, sub)
	}
	if v.RequestID != req {
		t.Fatalf("%s: 作业请求号失真：%x，要 %x", where, v.RequestID, req)
	}
	if v.Archive == nil {
		t.Fatalf("%s: 成功作业缺少归档", where)
	}
	if v.Archive.Submitter != sub {
		t.Fatalf("%s: 归档提交人失真：%x，要 %x", where, v.Archive.Submitter, sub)
	}
	if v.Archive.RequestID != req {
		t.Fatalf("%s: 归档请求号失真：%x，要 %x", where, v.Archive.RequestID, req)
	}
}

// assertRawIdentityPresent 核对归档带有 IdentityRaw，且其兜底字段解码后与
// 提交时的完整字节一致。
func assertRawIdentityPresent(t *testing.T, where string, v *Job, sub, req string) {
	t.Helper()
	raw := v.Archive.IdentityRaw
	if raw == nil {
		t.Fatalf("%s: 含非法 UTF-8 字节的标识必须带 IdentityRaw", where)
	}
	gotSub, err := unmarshalRaw(raw.Submitter)
	if err != nil {
		t.Fatalf("%s: Submitter 兜底字段无法解码: %v", where, err)
	}
	gotReq, err := unmarshalRaw(raw.RequestID)
	if err != nil {
		t.Fatalf("%s: RequestID 兜底字段无法解码: %v", where, err)
	}
	if gotSub != sub {
		t.Fatalf("%s: Submitter 兜底字节=%x，要 %x", where, gotSub, sub)
	}
	if gotReq != req {
		t.Fatalf("%s: RequestID 兜底字节=%x，要 %x", where, gotReq, req)
	}
}

// setupMixedIdentitySuccess 建立带直接上游、已成功归档且标识含非法 UTF-8 字节
// （混有中文与 U+0000）的场景：上游 [5] 总和 5，下游 [2,-3] 实际输入
// [2,-3,5]，总和 4、平方和 38。上下游属于同一提交人。
func setupMixedIdentitySuccess(t *testing.T) (*Store, uint64, uint64) {
	t.Helper()
	s, _ := openTestStore(t)
	up := mustSubmit(t, s, SubmitRequest{
		Submitter: mixedIdentitySub, RequestID: "up\xff",
		Values: queryIsolationUpValues,
	})
	if got := waitStatus(t, s, up.ID, StatusSucceeded); got.Archive.Sum != 5 {
		t.Fatalf("上游总和=%d，要 5", got.Archive.Sum)
	}
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: mixedIdentitySub, RequestID: mixedIdentityReq,
		Values: queryIsolationDownValues, Seed: queryIsolationSeed,
		Dependencies: []uint64{up.ID},
	})
	waitStatus(t, s, down.ID, StatusSucceeded)
	return s, up.ID, down.ID
}

// 按作业号读取与按提交人列举，对带特殊字节标识的同一份成功归档必须给出逐字节
// 一致的标识与数值结果，归档中存在 IdentityRaw 且解码回提交时的完整字节。
func TestInvalidUTF8IdentityConsistentAcrossGetAndList(t *testing.T) {
	s, upID, downID := setupMixedIdentitySuccess(t)

	byGet := mustGet(t, s, downID)
	list, err := s.List(mixedIdentitySub, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	// 同提交人的上下游都应被完整字节匹配列举到，按提交先后排列。
	if len(list) != 2 || list[0].ID != upID || list[1].ID != downID {
		t.Fatalf("List(%x)=%v，要 [%d %d]", mixedIdentitySub, ids(list), upID, downID)
	}
	byList := list[1]

	for name, v := range map[string]*Job{"Get": byGet, "List": byList} {
		assertIdentityBytes(t, name, mixedIdentitySub, mixedIdentityReq, v)
		assertRawIdentityPresent(t, name, v, mixedIdentitySub, mixedIdentityReq)
		// 数值结果仍是原先成功的计算。
		assertInt64s(t, name+": effective", v.Archive.EffectiveValues, queryIsolationEffective)
		if v.Archive.Sum != queryIsolationSum || v.Archive.SumOfSquares != queryIsolationSumSq {
			t.Fatalf("%s: sum=%d sq=%d，要 %d,%d",
				name, v.Archive.Sum, v.Archive.SumOfSquares, queryIsolationSum, queryIsolationSumSq)
		}
		assertUint64s(t, name+": dependencies", v.Dependencies, []uint64{upID})
	}
}

// 修改返回值中的提交人、请求号与 IdentityRaw 保存信息后：
//   - 再次按作业号读取、按原提交人列举，标识仍与提交时逐字节一致，
//     IdentityRaw 仍解码为原字节，作业仍归原提交人；
//   - 按被改成的其他提交人（含“非法字节被替换成 U+FFFD”的塌缩形态）列举
//     找不到该作业，不会因副本改动而改归别人；
//   - 此前通过另一种查询方式取得并持有的副本保持原样。
func TestMutatingIdentityViewDoesNotChangeStoredIdentity(t *testing.T) {
	s, upID, downID := setupMixedIdentitySuccess(t)

	heldList := mustListJob(t, s, mixedIdentitySub, downID)
	getView := mustGet(t, s, downID)
	otherSub := "其他\xff提交人"
	otherReq := "别的\xfe请求\x00号"
	corruptIdentityView(getView, otherSub, otherReq)

	// 再次读取与列举：仍是原字节、原归档。
	fresh := mustGet(t, s, downID)
	assertIdentityBytes(t, "re-Get", mixedIdentitySub, mixedIdentityReq, fresh)
	assertRawIdentityPresent(t, "re-Get", fresh, mixedIdentitySub, mixedIdentityReq)
	reListed := mustListJob(t, s, mixedIdentitySub, downID)
	assertIdentityBytes(t, "re-List", mixedIdentitySub, mixedIdentityReq, reListed)
	assertRawIdentityPresent(t, "re-List", reListed, mixedIdentitySub, mixedIdentityReq)

	// 副本改动不能把作业改归其他提交人：按改动后的标识、以及非法字节被替换
	// 成 U+FFFD 的塌缩形态列举，都不能找到这份作业。
	if got, _ := s.List(otherSub, time.Time{}, time.Time{}); len(got) != 0 {
		t.Fatalf("按副本改出的提交人列举竟有记录：%v", ids(got))
	}
	mangledSub := strings.ToValidUTF8(mixedIdentitySub, "�")
	mangledReq := strings.ToValidUTF8(mixedIdentityReq, "�")
	if mangledSub == mixedIdentitySub || mangledReq == mixedIdentityReq {
		t.Fatal("测试前置不成立：塌缩形态必须与原字节不同")
	}
	if got, _ := s.List(mangledSub, time.Time{}, time.Time{}); len(got) != 0 {
		t.Fatalf("按 U+FFFD 塌缩提交人列举竟有记录：%v", ids(got))
	}

	// 此前持有的列举副本不受后取得副本上的修改影响。
	assertIdentityBytes(t, "held List copy", mixedIdentitySub, mixedIdentityReq, heldList)
	assertRawIdentityPresent(t, "held List copy", heldList, mixedIdentitySub, mixedIdentityReq)

	// 另一方向：持有 Get 副本，修改后来的 List 副本（含其归档与 IdentityRaw），
	// 已持有的 Get 副本仍保持原样。
	heldGet := mustGet(t, s, downID)
	laterList := mustListJob(t, s, mixedIdentitySub, downID)
	corruptIdentityView(laterList, "\xfe", "x\xff")
	assertIdentityBytes(t, "held Get copy", mixedIdentitySub, mixedIdentityReq, heldGet)
	assertRawIdentityPresent(t, "held Get copy", heldGet, mixedIdentitySub, mixedIdentityReq)

	// 幂等作用域仍是原字节：原标识同内容重放命中原作业；用副本改出的提交人
	// 提交是互不相关的新作业，不会串用原作业号。
	replay := mustSubmit(t, s, SubmitRequest{
		Submitter: mixedIdentitySub, RequestID: mixedIdentityReq,
		Values: queryIsolationDownValues, Seed: queryIsolationSeed,
		Dependencies: []uint64{upID},
	})
	if replay.ID != downID {
		t.Fatalf("原标识重放 id=%d，要原作业 %d", replay.ID, downID)
	}
	other := mustSubmit(t, s, SubmitRequest{
		Submitter: otherSub, RequestID: otherReq, Values: []int64{1},
	})
	if other.ID == downID || other.ID == upID {
		t.Fatalf("改出的标识串用了原作业号：%d", other.ID)
	}
	waitStatus(t, s, other.ID, StatusSucceeded)
	// 原提交人列举仍只含自己的上下游，不含新提交人的作业。
	if got, _ := s.List(mixedIdentitySub, time.Time{}, time.Time{}); len(got) != 2 {
		t.Fatalf("原提交人列举被污染：%v，要只含 %d,%d", ids(got), upID, downID)
	}
}

// 普通中文标识、以及包含 U+0000 但整体仍是合法 UTF-8 的标识：归档不提供
// IdentityRaw 兜底信息，查询继续按完整字符串工作；修改返回值中的标识同样
// 只影响当前返回值。
func TestValidUTF8ChineseAndNULIdentitiesStayStable(t *testing.T) {
	s, _ := openTestStore(t)
	cases := []struct {
		name string
		sub  string
		req  string
	}{
		{"plain chinese", "张三", "请求号-1"},
		{"nul in submitter and request", "a\x00提交人b", "r\x00q"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !utf8.ValidString(tc.sub) || !utf8.ValidString(tc.req) {
				t.Fatal("测试前置不成立：本用例标识必须是合法 UTF-8")
			}
			j := mustSubmit(t, s, SubmitRequest{
				Submitter: tc.sub, RequestID: tc.req,
				Values: []int64{2, -3}, Seed: 3,
			})
			done := waitStatus(t, s, j.ID, StatusSucceeded)
			if done.Archive.IdentityRaw != nil {
				t.Fatalf("合法 UTF-8（含 U+0000）标识不应带 IdentityRaw：%+v",
					done.Archive.IdentityRaw)
			}

			v := mustListJob(t, s, tc.sub, j.ID)
			corruptIdentityView(v, "别的\xfe人", "q\xff") // 改成含非法字节的标识
			// 该副本归档原本没有 IdentityRaw；corruptIdentityView 不会凭空造出它，
			// 兜底字段缺省也不能影响记录。
			if v.Archive.IdentityRaw != nil {
				t.Fatal("调用方修改不应让合法标识副本长出 IdentityRaw 并影响判断")
			}

			fresh := mustGet(t, s, j.ID)
			assertIdentityBytes(t, "re-Get", tc.sub, tc.req, fresh)
			if fresh.Archive.IdentityRaw != nil {
				t.Fatal("重新读取后合法标识不应出现 IdentityRaw")
			}
			again := mustListJob(t, s, tc.sub, j.ID)
			assertIdentityBytes(t, "re-List", tc.sub, tc.req, again)
			if got, _ := s.List("别的提交人", time.Time{}, time.Time{}); len(got) != 0 {
				t.Fatalf("作业被副本改动改归其他提交人：%v", ids(got))
			}
		})
	}
}

// 关闭再重开归档后，带特殊字节标识的成功作业仍按原字节可查、可列举；
// 重开前对返回副本的任何修改都不落盘、不串用记录。
func TestInvalidUTF8IdentitySurvivesReopenAfterViewMutations(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	up := mustSubmit(t, s, SubmitRequest{
		Submitter: mixedIdentitySub, RequestID: "up\xff",
		Values: queryIsolationUpValues,
	})
	waitStatus(t, s, up.ID, StatusSucceeded)
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: mixedIdentitySub, RequestID: mixedIdentityReq,
		Values: queryIsolationDownValues, Seed: queryIsolationSeed,
		Dependencies: []uint64{up.ID},
	})
	waitStatus(t, s, down.ID, StatusSucceeded)

	// 重开前对多份返回副本做全套标识与数值修改，这些修改没有任何落盘途径。
	v1 := mustGet(t, s, down.ID)
	v2 := mustListJob(t, s, mixedIdentitySub, down.ID)
	corruptIdentityView(v1, "x\xfe", "y\xff")
	corruptIdentityView(v2, "z\xff", "w\xfe")
	v1.Status = StatusFailed
	v2.Archive.Sum = -1
	v2.Archive.IdentityRaw.Submitter = rawBase64Prefix + "////"

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	g := mustGet(t, s2, down.ID)
	if g.Status != StatusSucceeded {
		t.Fatalf("重开后状态=%s，要 succeeded", g.Status)
	}
	assertIdentityBytes(t, "reopen Get", mixedIdentitySub, mixedIdentityReq, g)
	assertRawIdentityPresent(t, "reopen Get", g, mixedIdentitySub, mixedIdentityReq)
	assertInt64s(t, "reopen effective", g.Archive.EffectiveValues, queryIsolationEffective)
	if g.Archive.Sum != queryIsolationSum || g.Archive.SumOfSquares != queryIsolationSumSq {
		t.Fatalf("重开后数值=%d,%d，要 %d,%d",
			g.Archive.Sum, g.Archive.SumOfSquares, queryIsolationSum, queryIsolationSumSq)
	}
	assertUint64s(t, "reopen dependencies", g.Dependencies, []uint64{up.ID})

	list, err := s2.List(mixedIdentitySub, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].ID != up.ID || list[1].ID != down.ID {
		t.Fatalf("重开后 List(%x)=%v，要 [%d %d]", mixedIdentitySub, ids(list), up.ID, down.ID)
	}
	assertIdentityBytes(t, "reopen List", mixedIdentitySub, mixedIdentityReq, list[1])
	assertRawIdentityPresent(t, "reopen List", list[1], mixedIdentitySub, mixedIdentityReq)

	// 重开后按原标识的幂等号继续生效，仍命中原作业。
	replay, err := s2.Submit(SubmitRequest{
		Submitter: mixedIdentitySub, RequestID: mixedIdentityReq,
		Values: queryIsolationDownValues, Seed: queryIsolationSeed,
		Dependencies: []uint64{up.ID},
	})
	if err != nil || replay.ID != down.ID {
		t.Fatalf("重开后原标识重放 id=%d err=%v，要原作业 %d", replayID(replay), err, down.ID)
	}
}
