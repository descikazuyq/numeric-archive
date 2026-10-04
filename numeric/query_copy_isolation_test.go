package numeric

import (
	"testing"
	"time"
	"unicode/utf8"
)

// 本文件为“查询返回数据是独立副本”这一既有约定补充自动化回归保障。
//
// 重点场景围绕一个带直接上游且已成功归档的作业展开（见 submitArchivedDown）：
//   - 上游总和为 5，本作业原始序列 [2,-3]，实际输入 [2,-3,5]，总和 4、平方和 38；
//   - 按作业号读取（Get）与按提交人列举（List）应对同一份已归档结果给出一致内容。
//
// 调用方修改自己持有的返回值时——作业状态、种子、原始整数序列、依赖列表，
// 以及归档中的参数、实际输入、总和、平方和、日志和校验值——这些修改只能影响
// 当前返回值：既不能改变再次查询得到的内容，也不能污染此前通过另一种查询方式
// 取得的详情；作业本身的原始输入与归档里保存的原始输入也各自独立。后来重新
// 取得的详情被修改时，已经持有的其他副本仍应保持原样。
//
// 另外覆盖提交人、请求号按完整字节保留：标识含不能组成合法 UTF-8 的字节、
// 并混有中文或 U+0000 的成功作业，其归档中存在 IdentityRaw 兜底表示；
// 修改其中保存的提交人或请求号同样只能影响当前返回值，再次按原提交人列举
// 仍能找到原作业，详情与归档中的标识逐字节保持一致，也不能因副本里的改动
// 而改归其他提交人。普通中文或只含 U+0000（字节合法）的标识不额外提供
// 原始字节信息。按提交人和时间范围列举的公开用法保持不变。

// archivedFixture 持有一次“上游已成功、下游也成功归档”的查询回归基线。
type archivedFixture struct {
	s          *Store
	upID       uint64
	downID     uint64
	submitter  string
	requestID  string
	seed       int64
	values     []int64 // 下游原始序列
	depIDs     []uint64
	effective  []int64 // 实际输入
	sum        int64
	sumSquares int64
}

// submitArchivedDown 构造规范场景：上游 [1,4] 成功归档（总和 5），下游以
// [2,-3] 与种子 7 引用该直接上游；实际输入为 [2,-3,5]，总和 4、平方和 38。
// 返回时两个作业均为成功归档状态。
func submitArchivedDown(t *testing.T, submitter, requestID string, seed int64) archivedFixture {
	t.Helper()
	s, _ := openTestStore(t)
	up := mustSubmit(t, s, SubmitRequest{
		Submitter: submitter, RequestID: requestID + "-up",
		Values: []int64{1, 4},
	})
	waitStatus(t, s, up.ID, StatusSucceeded)
	down := mustSubmit(t, s, SubmitRequest{
		Submitter: submitter, RequestID: requestID,
		Values: []int64{2, -3}, Seed: seed,
		Dependencies: []uint64{up.ID},
	})
	waitStatus(t, s, down.ID, StatusSucceeded)
	return archivedFixture{
		s:          s,
		upID:       up.ID,
		downID:     down.ID,
		submitter:  submitter,
		requestID:  requestID,
		seed:       seed,
		values:     []int64{2, -3},
		depIDs:     []uint64{up.ID},
		effective:  []int64{2, -3, 5},
		sum:        4,
		sumSquares: 38,
	}
}

// listOne 按提交人列举全部时间范围的记录，并返回其中作业号为 id 的那一条；
// 找不到或列举结果不符合预期即终止。
func listOne(t *testing.T, s *Store, submitter string, id uint64) *Job {
	t.Helper()
	got, err := s.List(submitter, time.Time{}, time.Time{})
	if err != nil {
		t.Fatalf("list %x: %v", submitter, err)
	}
	for _, j := range got {
		if j.ID == id {
			return j
		}
	}
	var ids []uint64
	for _, j := range got {
		ids = append(ids, j.ID)
	}
	t.Fatalf("List(%x)=%v 中找不到作业 %d", submitter, ids, id)
	return nil
}

// assertArchivedView 校验一份查询视图与规范成功归档完全一致：状态、种子、
// 原始整数次序、依赖次序、实际输入、数值结果、输入/结果摘要、日志、校验值，
// 以及归档内嵌的参数与成功状态。任何字段被此前对其他副本的改写污染都会在此暴露。
func assertArchivedView(t *testing.T, f archivedFixture, j *Job, where string) {
	t.Helper()
	if j == nil {
		t.Fatalf("%s: 视图为空", where)
	}
	if j.ID != f.downID {
		t.Fatalf("%s: ID=%d, want %d", where, j.ID, f.downID)
	}
	if j.Status != StatusSucceeded {
		t.Fatalf("%s: Status=%s, want succeeded", where, j.Status)
	}
	if j.Seed != f.seed {
		t.Fatalf("%s: Seed=%d, want %d", where, j.Seed, f.seed)
	}
	assertInt64s(t, where+" Values", j.Values, f.values)
	assertUint64s(t, where+" Dependencies", j.Dependencies, f.depIDs)
	if !j.HasDependency || j.DependencyID != f.upID {
		t.Fatalf("%s: HasDependency=%v DependencyID=%d, want true/%d",
			where, j.HasDependency, j.DependencyID, f.upID)
	}
	assertInt64s(t, where+" EffectiveValues", j.EffectiveValues, f.effective)
	if j.Submitter != f.submitter || j.RequestID != f.requestID {
		t.Fatalf("%s: 标识失真 %x/%x, want %x/%x",
			where, j.Submitter, j.RequestID, f.submitter, f.requestID)
	}

	a := j.Archive
	if a == nil {
		t.Fatalf("%s: 成功作业缺少归档", where)
	}
	if a.JobID != f.downID {
		t.Fatalf("%s: Archive.JobID=%d, want %d", where, a.JobID, f.downID)
	}
	if a.Seed != f.seed {
		t.Fatalf("%s: Archive.Seed=%d, want %d", where, a.Seed, f.seed)
	}
	if a.Submitter != f.submitter || a.RequestID != f.requestID {
		t.Fatalf("%s: 归档标识失真 %x/%x, want %x/%x",
			where, a.Submitter, a.RequestID, f.submitter, f.requestID)
	}
	assertInt64s(t, where+" Archive.Values", a.Values, f.values)
	assertUint64s(t, where+" Archive.Dependencies", a.Dependencies, f.depIDs)
	assertInt64s(t, where+" Archive.EffectiveValues", a.EffectiveValues, f.effective)
	if a.Sum != f.sum || a.SumOfSquares != f.sumSquares {
		t.Fatalf("%s: 结果=%d,%d, want %d,%d",
			where, a.Sum, a.SumOfSquares, f.sum, f.sumSquares)
	}
	if a.InputsDigest != inputsDigestHex(f.effective, f.seed) {
		t.Fatalf("%s: InputsDigest 不属于原成功计算", where)
	}
	wantResultDigest := resultDigestHex(f.effective, f.seed, f.sum, f.sumSquares)
	if a.ResultDigest != wantResultDigest {
		t.Fatalf("%s: ResultDigest 不属于原成功计算", where)
	}
	wantLog := buildLog(f.effective, f.seed, f.sum, f.sumSquares)
	if a.Log != wantLog {
		t.Fatalf("%s: Log 不属于原成功计算", where)
	}
	if a.Checksum != checksumHex(f.effective, f.seed, f.sum, f.sumSquares, wantLog, wantResultDigest) {
		t.Fatalf("%s: Checksum 不属于原成功计算", where)
	}
	if a.CompletedAt.IsZero() {
		t.Fatalf("%s: CompletedAt 缺失", where)
	}
}

// mutateReturnedView 把调用方持有的一份查询视图改得面目全非：状态、种子、
// 原始序列、依赖列表、作业级实际输入，以及归档中的参数、实际输入、总和、
// 平方和、日志、校验值与摘要。全部改动必须只影响这一个返回值。
func mutateReturnedView(j *Job) {
	j.Status = StatusFailed
	j.WaitReason = WaitDependency
	j.FailureReason = "caller scribble"
	j.BlockerID = 999
	j.Seed = -j.Seed - 1
	for i := range j.Values {
		j.Values[i] = 1000 + int64(i)
	}
	for i := range j.Dependencies {
		j.Dependencies[i] = 900 + uint64(i)
	}
	j.HasDependency = false
	j.DependencyID = 0
	for i := range j.PendingDependencies {
		j.PendingDependencies[i] = 800 + uint64(i)
	}
	for i := range j.EffectiveValues {
		j.EffectiveValues[i] = -1000 - int64(i)
	}
	j.Submitter = "其他提交人"
	j.RequestID = "other-request"

	a := j.Archive
	if a == nil {
		return
	}
	a.JobID = 424242
	a.Submitter = "归档里的其他提交人"
	a.RequestID = "archive-other-request"
	if a.IdentityRaw != nil {
		a.IdentityRaw.Submitter = "base64:" + "AAAA"
		a.IdentityRaw.RequestID = "base64:" + "BBBB"
	}
	a.Seed = a.Seed + 777
	for i := range a.Values {
		a.Values[i] = 2000 + int64(i)
	}
	a.HasDependency = false
	a.DependencyID = 0
	for i := range a.Dependencies {
		a.Dependencies[i] = 700 + uint64(i)
	}
	for i := range a.EffectiveValues {
		a.EffectiveValues[i] = -2000 - int64(i)
	}
	a.InputsDigest = "inputs-tampered"
	a.Sum = -4242
	a.SumOfSquares = -99
	a.ResultDigest = "result-tampered"
	a.Log = "tampered log"
	a.Checksum = "checksum-tampered"
}

// TestGetAndListReturnConsistentArchivedResult 是核心一致性保障：围绕一个带
// 直接上游且已成功归档的作业，先按提交人列举取得详情，再按作业号读取，
// 两条查询路径对同一份归档给出逐项一致的内容。
func TestGetAndListReturnConsistentArchivedResult(t *testing.T) {
	f := submitArchivedDown(t, "alice", "dep-job", 7)

	byList := listOne(t, f.s, f.submitter, f.downID)
	byGet, err := f.s.Get(f.downID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	assertArchivedView(t, f, byGet, "Get 视图")
	assertArchivedView(t, f, byList, "List 视图")

	// 列举结果按提交先后排列，且上游也在同一提交人名下。
	all, err := f.s.List(f.submitter, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0].ID != f.upID || all[1].ID != f.downID {
		t.Fatalf("List 次序/内容=%v, want [%d %d]", ids(all), f.upID, f.downID)
	}
}

// TestMutatingGotViewDoesNotChangeStoreOrListView 覆盖：按作业号取得的详情被
// 任意改写后，重新按作业号读取、按提交人列举都仍得到原成功归档，落盘记录
// 同样不变（重开归档后仍是同一成功结果）；此前先通过列举取得的副本也不被污染。
func TestMutatingGotViewDoesNotChangeStoreOrListView(t *testing.T) {
	f := submitArchivedDown(t, "alice", "dep-job", 7)

	// 先通过“另一种查询方式”（列举）持有一份详情。
	listBefore := listOne(t, f.s, f.submitter, f.downID)
	listDigest := listBefore.Archive.ResultDigest

	// 再按作业号读取，并把这份返回值彻底改写。
	got, _ := f.s.Get(f.downID)
	mutateReturnedView(got)

	// 重新按作业号读取：仍是原成功状态与完整归档。
	assertArchivedView(t, f, mustGet(t, f.s, f.downID), "改写后重新 Get")
	// 按提交人列举：仍能找到，内容一致。
	assertArchivedView(t, f, listOne(t, f.s, f.submitter, f.downID), "改写后重新 List")
	// 此前持有的列举副本未被污染。
	assertArchivedView(t, f, listBefore, "改写前持有的 List 副本")
	if listBefore.Archive.ResultDigest != listDigest {
		t.Fatal("此前持有的列举副本被 Get 副本的改写污染")
	}

	// 落盘记录也未被改写：重开归档后仍是同一成功结果。
	dir := f.s.Dir()
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	assertArchivedView(t, f, mustGet(t, s2, f.downID), "重开后 Get")
	assertArchivedView(t, f, listOne(t, s2, f.submitter, f.downID), "重开后 List")
}

// TestMutatingListViewDoesNotChangeStoreOrGetView 是对称保障：改写按提交人
// 列举得到的详情，不影响重新列举/读取，也不污染此前按作业号取得的副本。
func TestMutatingListViewDoesNotChangeStoreOrGetView(t *testing.T) {
	f := submitArchivedDown(t, "alice", "dep-job", 7)

	getBefore := mustGet(t, f.s, f.downID)
	getDigest := getBefore.Archive.ResultDigest

	listed := listOne(t, f.s, f.submitter, f.downID)
	mutateReturnedView(listed)

	assertArchivedView(t, f, mustGet(t, f.s, f.downID), "改写后重新 Get")
	assertArchivedView(t, f, listOne(t, f.s, f.submitter, f.downID), "改写后重新 List")
	assertArchivedView(t, f, getBefore, "改写前持有的 Get 副本")
	if getBefore.Archive.ResultDigest != getDigest {
		t.Fatal("此前持有的 Get 副本被 List 副本的改写污染")
	}

	// 列举出的多条记录各自独立：改写下游视图不能波及同批返回的上游视图。
	all, _ := f.s.List(f.submitter, time.Time{}, time.Time{})
	up := listOne(t, f.s, f.submitter, f.upID)
	if all[0].ID != f.upID {
		t.Fatalf("列举次序被副本改写影响: %v", ids(all))
	}
	if up.Archive == nil || up.Archive.Sum != 5 {
		t.Fatalf("上游归档被下游副本改写污染: %+v", up.Archive)
	}
}

// TestSeparateViewsAndSliceBackingAreIndependent 覆盖副本之间的底层数组隔离：
// 后来取得的详情被改写时，已经持有的其他副本（以及同一视图内作业级与归档级
// 的同值切片）保持原样。特别地，作业本身的原始输入 Values 与归档中保存的
// Archive.Values 各自独立，修改其中一份不能连带改变另一份；实际输入的两份
// 副本（job.EffectiveValues 与 Archive.EffectiveValues）亦然。
func TestSeparateViewsAndSliceBackingAreIndependent(t *testing.T) {
	f := submitArchivedDown(t, "alice", "dep-job", 7)

	first := mustGet(t, f.s, f.downID)
	second := mustGet(t, f.s, f.downID)
	third := listOne(t, f.s, f.submitter, f.downID)

	// 同一份视图内：作业级原始序列与归档内原始序列是两块独立内存。
	first.Values[0] = 333
	if first.Archive.Values[0] != f.values[0] {
		t.Fatalf("修改 job.Values 连带改变了 Archive.Values: %v", first.Archive.Values)
	}
	first.Archive.Values[1] = 444
	if first.Values[1] != f.values[1] {
		t.Fatalf("修改 Archive.Values 连带改变了 job.Values: %v", first.Values)
	}
	// 实际输入的两份副本同样独立。
	first.EffectiveValues[2] = 555
	if first.Archive.EffectiveValues[2] != f.effective[2] {
		t.Fatalf("修改 job.EffectiveValues 连带改变了归档实际输入: %v",
			first.Archive.EffectiveValues)
	}
	first.Archive.EffectiveValues[0] = 666
	if first.EffectiveValues[0] != f.effective[0] {
		t.Fatalf("修改归档实际输入连带改变了 job.EffectiveValues: %v", first.EffectiveValues)
	}
	// 依赖列表的作业级与归档级副本也各自独立。
	first.Dependencies[0] = 121
	if first.Archive.Dependencies[0] != f.depIDs[0] {
		t.Fatalf("修改 job.Dependencies 连带改变了归档依赖: %v", first.Archive.Dependencies)
	}

	// 后来取得的两份副本被改写，已经持有的 first 之外的视图互不影响，
	// 重新查询仍是原内容。
	mutateReturnedView(second)
	mutateReturnedView(third)

	// first 在本次测试中已被局部改写，校验其未被 second/third 的改写触碰：
	// 它保留自己的改动，其余字段仍是原值。
	if first.Values[0] != 333 || first.Values[1] != f.values[1] {
		t.Fatalf("first 副本被后来的副本改写污染: %v", first.Values)
	}
	if first.Archive.Values[0] != f.values[0] || first.Archive.Values[1] != 444 {
		t.Fatalf("first 的归档副本被后来的副本改写污染: %v", first.Archive.Values)
	}
	if first.EffectiveValues[2] != 555 || first.Archive.EffectiveValues[0] != 666 {
		t.Fatal("first 的实际输入副本被后来的副本改写污染")
	}
	if first.Dependencies[0] != 121 || first.Archive.Dependencies[0] != f.depIDs[0] {
		t.Fatal("first 的依赖副本被后来的副本改写污染")
	}
	if first.Status != StatusSucceeded || first.Seed != f.seed ||
		first.Archive.Sum != f.sum || first.Archive.SumOfSquares != f.sumSquares ||
		first.Archive.Checksum != checksumHex(f.effective, f.seed, f.sum, f.sumSquares,
			first.Archive.Log, first.Archive.ResultDigest) {
		t.Fatal("first 的其余字段被后来的副本改写污染")
	}

	// 重新查询得到的全新视图仍是完整原归档。
	assertArchivedView(t, f, mustGet(t, f.s, f.downID), "再次 Get")
	assertArchivedView(t, f, listOne(t, f.s, f.submitter, f.downID), "再次 List")
}

// TestMutationCannotBringDependenciesBackIntoRecord 覆盖：依赖作业号及其次序
// 不能被返回值中的修改带回记录。改写副本里的依赖列表后，原来的成功状态与
// 完整归档（含原依赖次序）仍能正常查询，单依赖视图字段也仍是原上游。
func TestMutationCannotBringDependenciesBackIntoRecord(t *testing.T) {
	f := submitArchivedDown(t, "alice", "dep-job", 7)

	got, _ := f.s.Get(f.downID)
	got.Dependencies[0] = 424242 // 不存在的作业号
	got.HasDependency = false
	got.DependencyID = 0
	got.Archive.Dependencies[0] = 424242
	got.Archive.HasDependency = false
	got.Archive.DependencyID = 0

	fresh := mustGet(t, f.s, f.downID)
	assertArchivedView(t, f, fresh, "依赖被改写后重新 Get")
	if !fresh.HasDependency || fresh.DependencyID != f.upID ||
		!fresh.Archive.HasDependency || fresh.Archive.DependencyID != f.upID {
		t.Fatalf("副本对依赖的修改被带回记录: %+v", fresh)
	}
	// 列举路径同样保留原依赖。
	assertArchivedView(t, f, listOne(t, f.s, f.submitter, f.downID), "依赖被改写后重新 List")
}

// TestInvalidUTF8IdentityArchiveIsReturnedByteExactAndCopyIsolated 覆盖带特殊
// 字节标识的成功作业：提交人含非法 UTF-8 字节并混有中文与 U+0000，请求号
// 同样如此；归档中必须存在 IdentityRaw 兜底表示，且按作业号读取与按提交人
// 列举得到的标识逐字节一致。修改某份返回值（含归档 IdentityRaw 中保存的
// 提交人/请求号）只能影响该返回值：再次按原提交人列举仍能找到原作业，
// 详情与归档标识逐字节保持原样，也不会因副本改动而改归其他提交人。
func TestInvalidUTF8IdentityArchiveIsReturnedByteExactAndCopyIsolated(t *testing.T) {
	sub := "提交人\xff\x00尾"
	req := "请求\xfe号\x00\xff"
	if utf8.ValidString(sub) || utf8.ValidString(req) {
		t.Fatal("测试标识必须含非法 UTF-8 字节")
	}
	f := submitArchivedDown(t, sub, req, 11)

	got := mustGet(t, f.s, f.downID)
	if got.Submitter != sub || got.RequestID != req {
		t.Fatalf("Get 顶层标识失真: %x/%x, want %x/%x",
			got.Submitter, got.RequestID, sub, req)
	}
	if got.Archive.IdentityRaw == nil {
		t.Fatal("含非法 UTF-8 字节的成功归档必须带 IdentityRaw 兜底表示")
	}
	if got.Archive.Submitter != sub || got.Archive.RequestID != req {
		t.Fatalf("Get 归档标识失真: %x/%x, want %x/%x",
			got.Archive.Submitter, got.Archive.RequestID, sub, req)
	}
	listed := listOne(t, f.s, sub, f.downID)
	if listed.Submitter != sub || listed.RequestID != req ||
		listed.Archive.Submitter != sub || listed.Archive.RequestID != req {
		t.Fatalf("List 标识未逐字节保持: 顶层 %x/%x 归档 %x/%x",
			listed.Submitter, listed.RequestID,
			listed.Archive.Submitter, listed.Archive.RequestID)
	}
	if listed.Archive.IdentityRaw == nil {
		t.Fatal("List 视图的归档也必须带 IdentityRaw")
	}

	// 改写 Get 副本的顶层与归档（含 IdentityRaw）标识保存信息。
	got.Submitter = "别人"
	got.RequestID = "别的请求"
	got.Archive.Submitter = "归档别人"
	got.Archive.RequestID = "归档别的请求"
	got.Archive.IdentityRaw.Submitter = "base64:" + "AQID"
	got.Archive.IdentityRaw.RequestID = "base64:" + "BAYG"

	// 再次按原提交人列举：仍只找到原作业（还有同提交人的上游，共两条），
	// 不能因副本改动改归其他提交人；原提交人列举数量不变。
	all, err := f.s.List(sub, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("List(原提交人)=%d 条, want 2（上游+下游）", len(all))
	}
	freshList := listOne(t, f.s, sub, f.downID)
	if freshList.Submitter != sub || freshList.RequestID != req ||
		freshList.Archive.Submitter != sub || freshList.Archive.RequestID != req {
		t.Fatalf("副本标识改动污染了重新列举的结果: %x/%x",
			freshList.Archive.Submitter, freshList.Archive.RequestID)
	}
	if freshList.Archive.IdentityRaw == nil {
		t.Fatal("重新列举后 IdentityRaw 丢失")
	}
	freshGet := mustGet(t, f.s, f.downID)
	if freshGet.Submitter != sub || freshGet.RequestID != req ||
		freshGet.Archive.Submitter != sub || freshGet.Archive.RequestID != req ||
		freshGet.Archive.IdentityRaw == nil {
		t.Fatalf("副本标识改动污染了重新读取的结果: 顶层 %x/%x",
			freshGet.Submitter, freshGet.RequestID)
	}
	assertArchivedView(t, f, freshGet, "特殊标识作业改写后重新 Get")
	// 按被改写后的“其他提交人”列举，绝不能找到这份作业。
	if stolen, _ := f.s.List("别人", time.Time{}, time.Time{}); len(stolen) != 0 {
		t.Fatalf("副本改动把作业改归其他提交人: %v", ids(stolen))
	}
	if stolen, _ := f.s.List("归档别人", time.Time{}, time.Time{}); len(stolen) != 0 {
		t.Fatalf("归档副本改动把作业改归其他提交人: %v", ids(stolen))
	}
	// 此前持有的 List 副本保持原样。
	if listed.Submitter != sub || listed.RequestID != req ||
		listed.Archive.Submitter != sub || listed.Archive.RequestID != req {
		t.Fatal("此前持有的 List 副本被 Get 副本的标识改写污染")
	}
}

// TestValidUTF8IdentitiesStayByteExactWithoutRawField 覆盖：普通中文或包含
// U+0000 但字节合法的标识继续按完整字节保留查询结果，但不额外提供原始字节
// 信息（归档中没有 IdentityRaw）。改写副本标识同样不能改变记录归属。
func TestValidUTF8IdentitiesStayByteExactWithoutRawField(t *testing.T) {
	cases := []struct {
		name      string
		submitter string
		requestID string
	}{
		{"plain chinese", "提交人甲", "请求号-子"},
		{"nul but valid utf8", "a\x00甲", "r\x00q"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !utf8.ValidString(tc.submitter) || !utf8.ValidString(tc.requestID) {
				t.Fatal("该用例标识必须是合法 UTF-8")
			}
			f := submitArchivedDown(t, tc.submitter, tc.requestID, 3)
			got := mustGet(t, f.s, f.downID)
			if got.Submitter != tc.submitter || got.RequestID != tc.requestID ||
				got.Archive.Submitter != tc.submitter || got.Archive.RequestID != tc.requestID {
				t.Fatalf("合法 UTF-8 标识失真: %x/%x", got.Submitter, got.RequestID)
			}
			if got.Archive.IdentityRaw != nil {
				t.Fatalf("合法 UTF-8（含 U+0000）标识不应带 IdentityRaw: %+v",
					got.Archive.IdentityRaw)
			}
			assertArchivedView(t, f, listOne(t, f.s, tc.submitter, f.downID),
				"合法标识 List 视图")

			got.Submitter = "改了"
			got.RequestID = "也改了"
			got.Archive.Submitter = "归档改了"
			got.Archive.RequestID = "归档也改"
			assertArchivedView(t, f, mustGet(t, f.s, f.downID), "改写后重新 Get")
			if rows, _ := f.s.List(tc.submitter, time.Time{}, time.Time{}); len(rows) != 2 {
				t.Fatalf("原提交人列举数量变化: %d, want 2", len(rows))
			}
			if rows, _ := f.s.List("改了", time.Time{}, time.Time{}); len(rows) != 0 {
				t.Fatalf("副本标识改动改变了记录归属: %v", ids(rows))
			}
		})
	}
}

// TestListTimeRangePublicUsageUnaffected 确认按提交人与时间范围列举的公开用法
// 在副本隔离保障下保持不变：两端包含、起始晚于结束被拒绝、结果按提交先后
// 排列；改写返回的列举元素也不影响后续列举。
func TestListTimeRangePublicUsageUnaffected(t *testing.T) {
	f := submitArchivedDown(t, "alice", "dep-job", 7)

	upView := listOne(t, f.s, f.submitter, f.upID)
	downView := listOne(t, f.s, f.submitter, f.downID)

	// 两端包含：以上游提交时间为起点、下游提交时间为终点，两条都在范围内。
	ranged, err := f.s.List(f.submitter, upView.QueuedAt, downView.QueuedAt)
	if err != nil || len(ranged) != 2 || ranged[0].ID != f.upID || ranged[1].ID != f.downID {
		t.Fatalf("包含端点的时间范围=%v err=%v", ids(ranged), err)
	}
	// 单端点范围只命中下游。
	single, err := f.s.List(f.submitter, downView.QueuedAt, downView.QueuedAt)
	if err != nil || len(single) != 1 || single[0].ID != f.downID {
		t.Fatalf("单点时间范围=%v err=%v", ids(single), err)
	}
	// 起始晚于结束仍被拒绝。
	if _, err := f.s.List(f.submitter, downView.QueuedAt, upView.QueuedAt); err == nil {
		t.Fatal("起始晚于结束必须返回错误")
	}

	// 改写列举元素及其归档，随后再次列举：公开用法与内容不变。
	for _, j := range ranged {
		mutateReturnedView(j)
	}
	again, err := f.s.List(f.submitter, time.Time{}, time.Time{})
	if err != nil || len(again) != 2 || again[0].ID != f.upID || again[1].ID != f.downID {
		t.Fatalf("改写列举元素后公开列举行为变化: %v err=%v", ids(again), err)
	}
	assertArchivedView(t, f, again[1], "改写后时间范围列举内容")
}

// mustGet 按作业号读取，失败即终止。
func mustGet(t *testing.T, s *Store, id uint64) *Job {
	t.Helper()
	j, err := s.Get(id)
	if err != nil {
		t.Fatalf("get %d: %v", id, err)
	}
	return j
}
