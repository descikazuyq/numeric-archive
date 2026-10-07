package numeric

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 本文件锁定“重新打开归档时两种依赖表示的兼容规则”：
//
// 对作业记录及其中成功归档的同一规则是——Dependencies 非空时，它就是实际的
// 有序上游列表；HasDependency 与 DependencyID 的缺省、零值或残留值不能覆盖
// 它，也不能单独成为归档失效的理由。两处（顶层记录与内嵌归档）表达的实际
// 依赖必须逐项、按次序一致，不能只比较集合。列表为空、缺省或为 null 时仍
// 沿用旧方式（启用单依赖便使用该作业号，否则无依赖）。
//
// 规格示例：作业 1 总和 2、作业 2 总和 -3；成功作业 3 原始输入 [4]、
// 依赖列表 [2,1]，实际输入 [4,-3,2]，总和 3、平方和 29。

// legacyDep 表示旧单依赖字段的一种落盘写法；nil 表示两个字段缺省（删除）。
type legacyDep struct {
	has bool
	id  uint64
}

// applyLegacyFields 在原始 JSON 映射上设置或删除旧单依赖字段。
func applyLegacyFields(m map[string]any, v *legacyDep) {
	if v == nil {
		delete(m, "has_dependency")
		delete(m, "dependency_id")
		return
	}
	m["has_dependency"] = v.has
	m["dependency_id"] = v.id
}

// rewriteRecordLegacyDependencyFields 只改写落盘记录顶层（top）与内嵌归档
// （arc）的旧单依赖字段，有序依赖列表、实际输入、结果、日志、摘要与校验值
// 全部保持原样（上游作业号不参与摘要与校验值）。top/arc 为 nil 表示删除该
// 层的两个旧字段以模拟缺省。
func rewriteRecordLegacyDependencyFields(t *testing.T, dir string, id uint64, top, arc *legacyDep) {
	t.Helper()
	p := filepath.Join(dir, jobFileName(id))
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	applyLegacyFields(m, top)
	if am, ok := m["archive"].(map[string]any); ok {
		applyLegacyFields(am, arc)
	}
	out, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, out, 0o600); err != nil {
		t.Fatal(err)
	}
}

// rewriteRecordDependencyLists 只改写顶层与内嵌归档的有序依赖列表；nil 表示
// 删除该层列表（模拟缺省），其余字段保持原样。
func rewriteRecordDependencyLists(t *testing.T, dir string, id uint64, topList, arcList []uint64) {
	t.Helper()
	p := filepath.Join(dir, jobFileName(id))
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	applyList := func(scope map[string]any, list []uint64) {
		if list == nil {
			delete(scope, "dependencies")
			return
		}
		vals := make([]any, len(list))
		for i, id := range list {
			vals[i] = float64(id)
		}
		scope["dependencies"] = vals
	}
	applyList(m, topList)
	if am, ok := m["archive"].(map[string]any); ok {
		applyList(am, arcList)
	}
	out, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, out, 0o600); err != nil {
		t.Fatal(err)
	}
}

// buildSpecExampleArchive 构造规格示例的三条成功记录：
// 作业 1 [2]（sum=2）、作业 2 [-3]（sum=-3）、作业 3 [4]+[2,1] →
// 实际输入 [4,-3,2]（sum=3、sq=29）。
func buildSpecExampleArchive(t *testing.T, dir string, base time.Time) *storedJob {
	t.Helper()
	j1 := syntheticSucceededRecord(t, dir, 1, []int64{2}, []int64{2}, nil, base)
	j2 := syntheticSucceededRecord(t, dir, 2, []int64{-3}, []int64{-3}, nil, base)
	if j1.archive.Sum != 2 || j2.archive.Sum != -3 {
		t.Fatalf("premise upstream sums: %d,%d want 2,-3", j1.archive.Sum, j2.archive.Sum)
	}
	j3 := syntheticSucceededRecord(t, dir, 3, []int64{4}, []int64{4, -3, 2}, []uint64{2, 1}, base)
	if j3.archive.Sum != 3 || j3.archive.SumOfSquares != 29 {
		t.Fatalf("premise job3 result=%d,%d want 3,29", j3.archive.Sum, j3.archive.SumOfSquares)
	}
	return j3
}

// assertSpecExampleSuccess 校验作业 3 重开后仍是完整成功：原始输入、依赖列表、
// 实际输入与结果不变；旧单依赖字段（详情与内嵌归档一致）只表示“有依赖 +
// 列表首项 2”，即使落盘时缺省或残留成其他作业号。
func assertSpecExampleSuccess(t *testing.T, s *Store, j3 *storedJob) {
	t.Helper()
	g, err := s.Get(3)
	if err != nil {
		t.Fatal(err)
	}
	if g.Status != StatusSucceeded || g.Archive == nil {
		t.Fatalf("job 3 must stay succeeded with a complete archive: %s", g.Status)
	}
	if g.BlockerID != 0 || g.FailureReason != "" {
		t.Fatalf("job 3 must carry no failure: blocker=%d reason=%q", g.BlockerID, g.FailureReason)
	}
	if len(g.Values) != 1 || g.Values[0] != 4 {
		t.Fatalf("job 3 original values=%v want [4]", g.Values)
	}
	if len(g.Dependencies) != 2 || g.Dependencies[0] != 2 || g.Dependencies[1] != 1 {
		t.Fatalf("job 3 dependency order=%v want [2 1]", g.Dependencies)
	}
	// 旧字段始终表示“是否有依赖以及列表首项”，与落盘残留无关。
	if !g.HasDependency || g.DependencyID != 2 {
		t.Fatalf("job 3 legacy fields=(%v,%d) want (true,2)", g.HasDependency, g.DependencyID)
	}
	wantEff := []int64{4, -3, 2}
	if len(g.EffectiveValues) != 3 {
		t.Fatalf("job 3 effective=%v want %v", g.EffectiveValues, wantEff)
	}
	for i := range wantEff {
		if g.EffectiveValues[i] != wantEff[i] {
			t.Fatalf("job 3 effective=%v want %v（追加次序不变）", g.EffectiveValues, wantEff)
		}
	}
	a := g.Archive
	if a.Sum != 3 || a.SumOfSquares != 29 {
		t.Fatalf("job 3 result=%d,%d want 3,29", a.Sum, a.SumOfSquares)
	}
	if len(a.Dependencies) != 2 || a.Dependencies[0] != 2 || a.Dependencies[1] != 1 {
		t.Fatalf("archive dependency order=%v want [2 1]", a.Dependencies)
	}
	if !a.HasDependency || a.DependencyID != 2 {
		t.Fatalf("archive legacy fields=(%v,%d) want (true,2)", a.HasDependency, a.DependencyID)
	}
	if a.Checksum != j3.archive.Checksum || a.InputsDigest != j3.archive.InputsDigest ||
		a.ResultDigest != j3.archive.ResultDigest || a.Log != j3.archive.Log {
		t.Fatalf("job 3 digest/checksum/log changed:\n got %+v\nwant %+v", a, j3.archive)
	}
	if !a.CompletedAt.Equal(j3.finishedAt) {
		t.Fatalf("job 3 completed=%s want %s", a.CompletedAt, j3.finishedAt)
	}
}

// 非空 Dependencies 是实际的有序上游：无论顶层还是内嵌归档的旧单依赖字段
// 缺省、为零、残留成列表中的另一个作业号（作业 1），还是残留成不存在的
// 作业号 9，记录与归档保存相同列表且其余校验全部成立时，重开都必须保留
// 成功状态与完整结果，不因旧字段改判失败。
func TestReopenNonEmptyDependencyListTakesPrecedenceOverLegacyFields(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	j3 := buildSpecExampleArchive(t, dir, base)

	variants := []struct {
		name string
		dep  *legacyDep
	}{
		{"absent", nil},
		{"zero", &legacyDep{false, 0}},
		{"stale other listed job 1", &legacyDep{true, 1}},
		{"stale nonexistent job 9", &legacyDep{true, 9}},
	}
	for _, top := range variants {
		for _, arc := range variants {
			name := "top=" + top.name + "/archive=" + arc.name
			t.Run(name, func(t *testing.T) {
				d := t.TempDir()
				copyArchive(t, dir, d)
				rewriteRecordLegacyDependencyFields(t, d, 3, top.dep, arc.dep)
				s, err := Open(d)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				assertSpecExampleSuccess(t, s, j3)

				// 有效上游不受任何旧字段写法影响。
				for _, want := range []struct {
					id  uint64
					sum int64
				}{
					{1, 2}, {2, -3},
				} {
					u, err := s.Get(want.id)
					if err != nil {
						t.Fatal(err)
					}
					if u.Status != StatusSucceeded || u.Archive == nil || u.Archive.Sum != want.sum {
						t.Fatalf("upstream %d must stay succeeded with sum %d: %s",
							want.id, want.sum, u.Status)
					}
				}
			})
		}
	}
}

// 两处实际列表的内容或次序不同（即使作为集合相同）仍是本作业自身归档有误：
// 改判失败、BlockerID 为 0、不返回成功归档与实际输入；有效上游保留自己的
// 结果，不被改判。
func TestReopenRejectsDisagreementBetweenRecordAndArchiveDependencyLists(t *testing.T) {
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	cases := []struct {
		name    string
		topList []uint64
		arcList []uint64
		// 两处列表一致、但与按列表追加的实际输入不符时，原因应指出第一个
		// 对不上的直接上游；其余情形只要求自身失败原因。
		wantMismatchDep uint64
	}{
		// 同一集合、次序相反：不能只比较集合。
		{"archive list reversed", []uint64{2, 1}, []uint64{1, 2}, 0},
		{"record list reversed", []uint64{1, 2}, []uint64{2, 1}, 0},
		// 长度不同。
		{"archive list shorter", []uint64{2, 1}, []uint64{2}, 0},
		{"record list shorter", []uint64{2}, []uint64{2, 1}, 0},
		// 两处列表一致但次序为 [1,2]，实际输入仍按 [2,1] 追加：
		// 第一个对不上的位置是直接上游作业 1（其总和 2，保存为 -3）。
		{"both lists disagree with appended order", []uint64{1, 2}, []uint64{1, 2}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			buildSpecExampleArchive(t, dir, base)
			rewriteRecordDependencyLists(t, dir, 3, tc.topList, tc.arcList)

			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			g, err := s.Get(3)
			if err != nil {
				t.Fatal(err)
			}
			if g.Status != StatusFailed {
				t.Fatalf("job 3 with disagreeing dependency lists must fail, got %s", g.Status)
			}
			if g.BlockerID != 0 {
				t.Fatalf("own archive error must keep blocker 0, got %d", g.BlockerID)
			}
			if g.Archive != nil || g.EffectiveValues != nil {
				t.Fatalf("failed job 3 must not return archive/effective inputs: %v %v",
					g.Archive, g.EffectiveValues)
			}
			if !strings.Contains(g.FailureReason, "成功结果不可用") {
				t.Fatalf("reason=%q must state success results unusable", g.FailureReason)
			}
			if tc.wantMismatchDep != 0 &&
				!strings.Contains(g.FailureReason, "作业 "+itoa(tc.wantMismatchDep)) {
				t.Fatalf("reason=%q must name first mismatched upstream %d",
					g.FailureReason, tc.wantMismatchDep)
			}

			// 有效上游不受影响。
			for _, want := range []struct {
				id  uint64
				sum int64
			}{
				{1, 2}, {2, -3},
			} {
				u, _ := s.Get(want.id)
				if u.Status != StatusSucceeded || u.Archive == nil || u.Archive.Sum != want.sum {
					t.Fatalf("valid upstream %d must keep result %d: %s", want.id, want.sum, u.Status)
				}
			}

			// 落盘记录已改写为失败且不再含归档。
			data, err := os.ReadFile(filepath.Join(dir, jobFileName(3)))
			if err != nil {
				t.Fatal(err)
			}
			text := string(data)
			if !strings.Contains(text, `"status": "failed"`) || strings.Contains(text, `"archive"`) {
				t.Fatalf("job 3 on-disk record must be failed without archive:\n%s", text)
			}
		})
	}
}

// 端到端规格示例：作业 3 顶层旧字段残留为作业 1、归档旧字段缺省，列表与
// 实际输入正确——重开后作业 3 保持成功；等待它的作业 4 按既有规则使用其
// 结果（实际输入 [10,3]，总和 13、平方和 109）。按作业号读取与按提交人
// 列举返回一致的依赖信息；原有结果、日志、摘要、校验值与完成时间不变；
// 再次重开保持稳定。
func TestReopenSpecExampleKeepsSuccessAndServesWaitingDownstream(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	j3 := buildSpecExampleArchive(t, dir, base)

	// 作业 4：排队等待作业 3，重开后应能用作业 3 的成功结果计算。
	writeSyntheticRecord(t, dir, &storedJob{
		id: 4, submitter: "a", requestID: "j4",
		values: []int64{10}, dependencies: []uint64{3},
		queuedAt: base.Add(4 * time.Second),
		status:   StatusQueued,
	})

	// 顶层旧字段残留为列表第二项作业 1；归档旧字段直接缺省。
	rewriteRecordLegacyDependencyFields(t, dir, 3, &legacyDep{true, 1}, nil)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	assertSpecExampleSuccess(t, s, j3)

	// 等待它的作业按现有规则使用结果：实际输入 [10,3]，sum=13、sq=109。
	child := waitStatus(t, s, 4, StatusSucceeded)
	if child.Archive.Sum != 13 || child.Archive.SumOfSquares != 109 {
		t.Fatalf("job 4 result=%d,%d want 13,109", child.Archive.Sum, child.Archive.SumOfSquares)
	}
	wantChildEff := []int64{10, 3}
	for i := range wantChildEff {
		if child.EffectiveValues[i] != wantChildEff[i] {
			t.Fatalf("job 4 effective=%v want %v", child.EffectiveValues, wantChildEff)
		}
	}
	if !child.HasDependency || child.DependencyID != 3 ||
		len(child.Dependencies) != 1 || child.Dependencies[0] != 3 {
		t.Fatalf("job 4 dependency view=%+v want single dep 3", child)
	}

	// 按作业号读取与按提交人列举返回一致的依赖信息（含内嵌归档旧字段）。
	listed, err := s.List("a", time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	var l3 *Job
	for _, lj := range listed {
		if lj.ID == 3 {
			l3 = lj
		}
	}
	if l3 == nil {
		t.Fatal("job 3 missing from submitter listing")
	}
	g3, _ := s.Get(3)
	if l3.Status != g3.Status || l3.HasDependency != g3.HasDependency ||
		l3.DependencyID != g3.DependencyID || len(l3.Dependencies) != len(g3.Dependencies) {
		t.Fatalf("listed job 3 dependency info disagrees with Get: %+v vs %+v", l3, g3)
	}
	for i := range g3.Dependencies {
		if l3.Dependencies[i] != g3.Dependencies[i] {
			t.Fatalf("listed job 3 dependency order=%v want %v", l3.Dependencies, g3.Dependencies)
		}
	}
	if l3.Archive == nil || l3.Archive.HasDependency != true || l3.Archive.DependencyID != 2 {
		t.Fatalf("listed archive legacy fields wrong: %+v", l3.Archive)
	}
	if len(l3.Archive.Dependencies) != 2 || l3.Archive.Dependencies[0] != 2 || l3.Archive.Dependencies[1] != 1 {
		t.Fatalf("listed archive dependencies=%v want [2 1]", l3.Archive.Dependencies)
	}

	// 无依赖的作业 1：详情与归档旧字段都表示无依赖，列表为空。
	g1, _ := s.Get(1)
	if g1.HasDependency || g1.DependencyID != 0 || len(g1.Dependencies) != 0 {
		t.Fatalf("job 1 legacy fields must indicate no dependency: has=%v id=%d deps=%v",
			g1.HasDependency, g1.DependencyID, g1.Dependencies)
	}
	if g1.Archive.HasDependency || g1.Archive.DependencyID != 0 || len(g1.Archive.Dependencies) != 0 {
		t.Fatalf("job 1 archive legacy fields must indicate no dependency: %+v", g1.Archive)
	}

	// 成功记录重开时不被改写：磁盘上仍是 succeeded，归档保留，顶层旧字段的
	// 残留原样留存；输入、结果、校验值不因打开而变化。
	data, err := os.ReadFile(filepath.Join(dir, jobFileName(3)))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, `"status": "succeeded"`) {
		t.Fatalf("job 3 on-disk record must stay succeeded:\n%s", text)
	}
	if !strings.Contains(text, `"checksum": `) || !strings.Contains(text, `"dependency_id": 1`) {
		t.Fatalf("job 3 on-disk record must keep its archive and untouched stale legacy field:\n%s", text)
	}

	// 再次重开：成功状态与规范化后的依赖视图保持稳定。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	assertSpecExampleSuccess(t, s2, j3)
	g4, _ := s2.Get(4)
	if g4.Status != StatusSucceeded || g4.Archive.Sum != 13 || g4.Archive.SumOfSquares != 109 {
		t.Fatalf("after second reopen job 4 must keep result 13,109: %s", g4.Status)
	}
}
