package numeric

import (
	"testing"
	"time"
)

// 本文件为查询能力补充自动化回归保障，保护“返回数据是独立副本”的既有约定
// （README “查询”一节：返回数据为拷贝，外部修改不影响记录）。
//
// 场景围绕一个带直接上游且已成功归档的作业展开：
//
//	上游 up：序列 [5]，成功后总和为 5；
//	当前作业 down（直接上游为 up）：原始序列 [2,-3]，
//	实际参与计算的输入为 [2,-3,5]（原始序列后按依赖顺序追加上游总和 5），
//	总和 2-3+5 = 4，平方和 4+9+25 = 38。
//
// 调用方修改自己持有的返回值（作业状态、种子、原始整数序列、依赖列表，以及
// 归档中的参数、实际输入、总和、平方和、日志与校验值）只能影响当前返回值：
// 不能改变再次按作业号读取或按提交人列举得到的内容，也不能污染此前通过另一种
// 查询方式取得的详情。
var (
	queryIsolationUpValues   = []int64{5}
	queryIsolationDownValues = []int64{2, -3}
	queryIsolationEffective  = []int64{2, -3, 5}
	queryIsolationSum        = int64(4)
	queryIsolationSumSq      = int64(38)
	queryIsolationSeed       = int64(17)
	queryIsolationSubmitter  = "isolation-user"
)

// setupDependentSuccess 建立“一个直接上游已成功归档、下游也已成功归档”的场景，
// 返回归档与上下游作业号。
func setupDependentSuccess(t *testing.T) (*Store, uint64, uint64) {
	t.Helper()
	s, _ := openTestStore(t)
	up := mustSubmit(t, s, SubmitRequest{
		Submitter: queryIsolationSubmitter, RequestID: "up",
		Values: queryIsolationUpValues,
	})
	if got := waitStatus(t, s, up.ID, StatusSucceeded); got.Archive.Sum != 5 {
		t.Fatalf("upstream sum=%d, want 5", got.Archive.Sum)
	}
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: queryIsolationSubmitter, RequestID: "down",
		Values: queryIsolationDownValues, Seed: queryIsolationSeed,
		Dependencies: []uint64{up.ID},
	})
	waitStatus(t, s, down.ID, StatusSucceeded)
	return s, up.ID, down.ID
}

func mustGet(t *testing.T, s *Store, id uint64) *Job {
	t.Helper()
	j, err := s.Get(id)
	if err != nil {
		t.Fatalf("Get(%d): %v", id, err)
	}
	return j
}

// mustListJob 按提交人列举并返回其中指定作业号的详情；要求该作业恰好出现一次，
// 且列举结果按提交先后排列。
func mustListJob(t *testing.T, s *Store, submitter string, id uint64) *Job {
	t.Helper()
	list, err := s.List(submitter, time.Time{}, time.Time{})
	if err != nil {
		t.Fatalf("List(%q): %v", submitter, err)
	}
	var found *Job
	for _, j := range list {
		if j.ID == id {
			if found != nil {
				t.Fatalf("job %d listed more than once for submitter %q", id, submitter)
			}
			found = j
		}
	}
	if found == nil {
		t.Fatalf("job %d not found in List(%q): %v", id, submitter, ids(list))
	}
	return found
}

// assertExpectedView 逐字段核对一份成功归档视图仍是原始的成功计算：
// 状态、种子、原始序列、（单）依赖列表、实际输入、总和、平方和、输入/结果
// 摘要、日志、校验值、归档内保存的原始参数与依赖，全部对应 [2,-3] + 上游 5。
func assertExpectedView(t *testing.T, where string, v *Job, upID, downID uint64) {
	t.Helper()
	if v.ID != downID {
		t.Fatalf("%s: ID=%d, want %d", where, v.ID, downID)
	}
	if v.Status != StatusSucceeded {
		t.Fatalf("%s: status=%s, want succeeded", where, v.Status)
	}
	if v.Submitter != queryIsolationSubmitter || v.RequestID != "down" {
		t.Fatalf("%s: identity=%q/%q", where, v.Submitter, v.RequestID)
	}
	if v.Seed != queryIsolationSeed {
		t.Fatalf("%s: seed=%d, want %d", where, v.Seed, queryIsolationSeed)
	}
	assertInt64s(t, where+": values", v.Values, queryIsolationDownValues)
	assertUint64s(t, where+": dependencies", v.Dependencies, []uint64{upID})
	if !v.HasDependency || v.DependencyID != upID {
		t.Fatalf("%s: single dependency fields: has=%v id=%d, want true,%d",
			where, v.HasDependency, v.DependencyID, upID)
	}
	assertInt64s(t, where+": effective", v.EffectiveValues, queryIsolationEffective)
	if v.FailureReason != "" || v.BlockerID != 0 {
		t.Fatalf("%s: failure=%q blocker=%d, want none", where, v.FailureReason, v.BlockerID)
	}

	a := v.Archive
	if a == nil {
		t.Fatalf("%s: succeeded view has no archive", where)
	}
	if a.JobID != downID || a.Submitter != queryIsolationSubmitter || a.RequestID != "down" {
		t.Fatalf("%s: archive identity=%+v", where, a)
	}
	if a.Seed != queryIsolationSeed {
		t.Fatalf("%s: archive seed=%d", where, a.Seed)
	}
	assertInt64s(t, where+": archive values", a.Values, queryIsolationDownValues)
	assertUint64s(t, where+": archive dependencies", a.Dependencies, []uint64{upID})
	if !a.HasDependency || a.DependencyID != upID {
		t.Fatalf("%s: archive single dependency fields wrong", where)
	}
	assertInt64s(t, where+": archive effective", a.EffectiveValues, queryIsolationEffective)
	if a.Sum != queryIsolationSum || a.SumOfSquares != queryIsolationSumSq {
		t.Fatalf("%s: results sum=%d sq=%d, want %d,%d",
			where, a.Sum, a.SumOfSquares, queryIsolationSum, queryIsolationSumSq)
	}
	if a.InputsDigest != inputsDigestHex(queryIsolationEffective, queryIsolationSeed) {
		t.Fatalf("%s: inputs digest no longer belongs to the original successful computation", where)
	}
	if a.ResultDigest != resultDigestHex(queryIsolationEffective, queryIsolationSeed,
		queryIsolationSum, queryIsolationSumSq) {
		t.Fatalf("%s: result digest no longer belongs to the original successful computation", where)
	}
	wantLog := buildLog(queryIsolationEffective, queryIsolationSeed,
		queryIsolationSum, queryIsolationSumSq)
	if a.Log != wantLog {
		t.Fatalf("%s: log=%q, want original computation log", where, a.Log)
	}
	if a.Checksum != checksumHex(queryIsolationEffective, queryIsolationSeed,
		queryIsolationSum, queryIsolationSumSq, wantLog, a.ResultDigest) {
		t.Fatalf("%s: checksum no longer belongs to the original successful computation", where)
	}
	if a.CompletedAt.IsZero() {
		t.Fatalf("%s: completed time missing", where)
	}
}

// corruptView 对一份调用方持有的返回值做“全套改写”：作业状态、种子、原始序列、
// 依赖列表，以及归档中的保存参数、实际输入、总和、平方和、摘要、日志与校验值。
// 若某切片容量足够，还会在末尾追加元素，验证底层数组没有与其他视图共享。
func corruptView(v *Job, upID, downID uint64) {
	v.Status = StatusFailed
	v.Seed = 987654321
	v.Values[0] = 777
	v.Values = append(v.Values, 888)
	v.Dependencies[0] = downID + 4096
	v.Dependencies = append(v.Dependencies, downID+4097)
	v.HasDependency = false
	v.DependencyID = 0
	v.EffectiveValues[0] = -777
	v.EffectiveValues = append(v.EffectiveValues, -888)
	v.FailureReason = "caller-local mutation"
	v.BlockerID = downID + 4096
	v.Submitter = "mutated-submitter"
	v.RequestID = "mutated-request"

	a := v.Archive
	a.JobID = downID + 4096
	a.Submitter = "mutated-archive-submitter"
	a.RequestID = "mutated-archive-request"
	if a.IdentityRaw != nil {
		a.IdentityRaw.Submitter = rawBase64Prefix + "AAAA"
		a.IdentityRaw.RequestID = rawBase64Prefix + "BBBB"
	}
	a.Seed = 111
	a.Values[0] = 111
	a.Values = append(a.Values, 222)
	a.Dependencies[0] = downID + 4098
	a.Dependencies = append(a.Dependencies, downID+4099)
	a.HasDependency = false
	a.DependencyID = 0
	a.EffectiveValues[0] = 333
	a.EffectiveValues = append(a.EffectiveValues, 444)
	a.Sum = -999999
	a.SumOfSquares = -999998
	a.InputsDigest = "mutated-inputs-digest"
	a.ResultDigest = "mutated-result-digest"
	a.Log = "mutated log"
	a.Checksum = "mutated-checksum"
	a.CompletedAt = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
}

// 按作业号读取与按提交人列举，对同一份已归档成功结果必须给出一致内容。
func TestGetAndListReturnConsistentArchivedSuccess(t *testing.T) {
	s, upID, downID := setupDependentSuccess(t)

	byGet := mustGet(t, s, downID)
	list, err := s.List(queryIsolationSubmitter, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	// 上游与下游都属于该提交人，按提交先后（作业号升序）排列。
	if len(list) != 2 || list[0].ID != upID || list[1].ID != downID {
		t.Fatalf("list=%v, want [%d %d] in submission order", ids(list), upID, downID)
	}
	byList := list[1]
	assertExpectedView(t, "Get view", byGet, upID, downID)
	assertExpectedView(t, "List view", byList, upID, downID)
}

// 修改按作业号读取得到的返回值：再次读取与按提交人列举都仍是原成功归档；
// 此前通过列举取得的副本也不被污染。
func TestMutatingGetViewDoesNotAffectStoreOrEarlierListView(t *testing.T) {
	s, upID, downID := setupDependentSuccess(t)

	listBefore := mustListJob(t, s, queryIsolationSubmitter, downID)
	getView := mustGet(t, s, downID)
	corruptView(getView, upID, downID)

	assertExpectedView(t, "re-Get after mutating Get", mustGet(t, s, downID), upID, downID)
	assertExpectedView(t, "List after mutating Get",
		mustListJob(t, s, queryIsolationSubmitter, downID), upID, downID)
	// 修改不能带回记录：依赖作业号及其次序仍是原先的单依赖 [upID]，
	// 原来的成功状态和完整归档仍能正常查询（上面已逐项核对）。
	assertExpectedView(t, "earlier List view", listBefore, upID, downID)
}

// 修改按提交人列举得到的返回值：再次列举与按作业号读取都仍是原成功归档；
// 此前通过读取取得的副本也不被污染。
func TestMutatingListViewDoesNotAffectStoreOrEarlierGetView(t *testing.T) {
	s, upID, downID := setupDependentSuccess(t)

	getBefore := mustGet(t, s, downID)
	listView := mustListJob(t, s, queryIsolationSubmitter, downID)
	corruptView(listView, upID, downID)

	assertExpectedView(t, "re-List after mutating List",
		mustListJob(t, s, queryIsolationSubmitter, downID), upID, downID)
	assertExpectedView(t, "Get after mutating List", mustGet(t, s, downID), upID, downID)
	assertExpectedView(t, "earlier Get view", getBefore, upID, downID)
}

// 作业本身的原始输入与归档里保存的原始输入各自独立：同一次查询中先改作业的
// Values，归档 Values 不变；换一份副本再改归档 Values，作业 Values 不变。
// 实际输入（EffectiveValues）在作业视图与归档之间同样各自独立。
func TestJobValuesAndArchiveValuesAreIndependentCopies(t *testing.T) {
	s, upID, downID := setupDependentSuccess(t)

	v := mustGet(t, s, downID)
	v.Values[0] = 777
	v.Values[1] = -777
	assertInt64s(t, "archive values after mutating job values", v.Archive.Values, queryIsolationDownValues)
	v.EffectiveValues[0] = 777
	assertInt64s(t, "archive effective after mutating job effective",
		v.Archive.EffectiveValues, queryIsolationEffective)
	// 作业视图的修改只影响当前返回值：重新读取仍是原内容。
	assertExpectedView(t, "re-Get after within-view mutation", mustGet(t, s, downID), upID, downID)

	w := mustGet(t, s, downID)
	w.Archive.Values[0] = 666
	w.Archive.Values[1] = -666
	assertInt64s(t, "job values after mutating archive values", w.Values, queryIsolationDownValues)
	w.Archive.EffectiveValues[2] = 666
	assertInt64s(t, "job effective after mutating archive effective",
		w.EffectiveValues, queryIsolationEffective)
	assertExpectedView(t, "re-Get after archive-only mutation", mustGet(t, s, downID), upID, downID)
}

// 后来重新取得的详情被修改时，已经持有的其他副本仍保持原样：
// 先持有 List 副本，再取一份 Get 副本并全套改写，List 副本不变；反之亦然。
func TestLaterViewMutationDoesNotReachEarlierHeldCopies(t *testing.T) {
	s, upID, downID := setupDependentSuccess(t)

	held := mustListJob(t, s, queryIsolationSubmitter, downID)
	later := mustGet(t, s, downID)
	corruptView(later, upID, downID)
	assertExpectedView(t, "held List copy after later Get mutated", held, upID, downID)

	heldGet := mustGet(t, s, downID)
	laterList := mustListJob(t, s, queryIsolationSubmitter, downID)
	corruptView(laterList, upID, downID)
	assertExpectedView(t, "held Get copy after later List mutated", heldGet, upID, downID)

	// 连续取得多份副本，对最后一份做切片底层数组层面的追加改写，
	// 前序副本与记录都不受影响。
	copies := []*Job{
		mustGet(t, s, downID),
		mustListJob(t, s, queryIsolationSubmitter, downID),
		mustGet(t, s, downID),
	}
	corruptView(copies[2], upID, downID)
	for _, c := range copies[:2] {
		assertExpectedView(t, "earlier independent copy", c, upID, downID)
	}
	assertExpectedView(t, "store after mutating third copy", mustGet(t, s, downID), upID, downID)
}

// 把某份查询结果的原始序列、追加值或数值结果改成其他内容后，重新读取及列举
// 仍应得到原始内容，输入摘要、结果摘要、日志与校验值仍属于原先成功的计算；
// 依赖作业号及其次序不能被返回值中的修改带回记录。
func TestMutatedNumbersDigestsAndDependenciesNeverReturnToStore(t *testing.T) {
	s, upID, downID := setupDependentSuccess(t)

	v := mustListJob(t, s, queryIsolationSubmitter, downID)
	// 原始序列改成“另一份输入”，追加的上游总和也改掉，数值结果随之改写。
	v.Values[0], v.Values[1] = -3, 2
	v.EffectiveValues[0], v.EffectiveValues[1], v.EffectiveValues[2] = 9, 9, 9
	v.Dependencies[0] = downID // 试图把依赖改成不存在/非法的作业号
	v.Archive.Values[0], v.Archive.Values[1] = -3, 2
	v.Archive.EffectiveValues = []int64{1, 1, 1}
	v.Archive.Dependencies[0] = 0
	v.Archive.Sum, v.Archive.SumOfSquares = 123, 456
	v.Archive.InputsDigest = inputsDigestHex([]int64{1, 1, 1}, queryIsolationSeed)
	v.Archive.ResultDigest = resultDigestHex([]int64{1, 1, 1}, queryIsolationSeed, 123, 456)
	v.Archive.Log = buildLog([]int64{1, 1, 1}, queryIsolationSeed, 123, 456)
	v.Archive.Checksum = checksumHex([]int64{1, 1, 1}, queryIsolationSeed,
		123, 456, v.Archive.Log, v.Archive.ResultDigest)

	assertExpectedView(t, "Get after number/digest tampering", mustGet(t, s, downID), upID, downID)
	assertExpectedView(t, "List after number/digest tampering",
		mustListJob(t, s, queryIsolationSubmitter, downID), upID, downID)

	// 原提交内容的幂等重放仍命中原作业（记录参数没被带回），冲突内容仍冲突，
	// 从侧面确认记录中的原始序列、种子、依赖次序未被修改。
	replay := mustSubmit(t, s, SubmitRequest{
		Submitter: queryIsolationSubmitter, RequestID: "down",
		Values: queryIsolationDownValues, Seed: queryIsolationSeed,
		Dependencies: []uint64{upID},
	})
	if replay.ID != downID || replay.Status != StatusSucceeded {
		t.Fatalf("equal-content replay: id=%d status=%s, want %d succeeded",
			replay.ID, replay.Status, downID)
	}
	assertExpectedView(t, "idempotent replay view", replay, upID, downID)
}
