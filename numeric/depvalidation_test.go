package numeric

import (
	"strings"
	"testing"
)

// dependencyListFault 是排队记录与成功记录重开复查共用的唯一依赖列表合法性
// 规则：先查重复（指出保存顺序中首次再次出现的作业号），没有重复时才查先后
// 关系（指出第一个不小于本作业号的作业）。这里直接锁定该共享规则本身，与
// record.go 中两类状态各自的集成测试相互印证。
func TestDependencyListFaultSharedRule(t *testing.T) {
	cases := []struct {
		name     string
		id       uint64
		deps     []uint64
		illegal  bool
		contains string
	}{
		{name: "无依赖", id: 3, deps: nil, illegal: false},
		{name: "合法乱序上游", id: 5, deps: []uint64{3, 1, 2}, illegal: false},
		{name: "不同上游同和不判重", id: 3, deps: []uint64{1, 2}, illegal: false},
		{
			name: "相邻重复", id: 2, deps: []uint64{1, 1},
			illegal: true, contains: "依赖列表重复",
		},
		{
			name: "隔项重复指出首次再次出现者", id: 3, deps: []uint64{1, 2, 1},
			illegal: true, contains: "作业 1",
		},
		{
			name: "引用自己", id: 7, deps: []uint64{7},
			illegal: true, contains: "依赖先后关系不合法",
		},
		{
			name: "引用后来作业", id: 3, deps: []uint64{5},
			illegal: true, contains: "作业 5",
		},
		{
			name: "乱序但只认第一个不小于自己者", id: 7, deps: []uint64{1, 9, 5, 8},
			illegal: true, contains: "作业 9",
		},
		{
			name: "重复与先后同时存在保留重复原因", id: 3, deps: []uint64{9, 9},
			illegal: true, contains: "依赖列表重复",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			j := &storedJob{id: tc.id, dependencies: append([]uint64(nil), tc.deps...)}
			reason, ok := dependencyListFault(j)
			if ok != tc.illegal {
				t.Fatalf("deps=%v illegal=%v want %v (reason=%q)", tc.deps, ok, tc.illegal, reason)
			}
			if tc.illegal {
				if reason == "" {
					t.Fatalf("illegal deps=%v must carry a reason", tc.deps)
				}
				if !strings.Contains(reason, tc.contains) {
					t.Fatalf("reason=%q must contain %q", reason, tc.contains)
				}
			} else if reason != "" {
				t.Fatalf("legal deps=%v must not carry a reason, got %q", tc.deps, reason)
			}
			// 检查不得改动或排重依赖列表：合法列表保持保存次序，非法列表也不被整理。
			if len(j.dependencies) != len(tc.deps) {
				t.Fatalf("dependencies mutated: got %v want %v", j.dependencies, tc.deps)
			}
			for i := range tc.deps {
				if j.dependencies[i] != tc.deps[i] {
					t.Fatalf("dependencies order mutated at %d: got %v want %v", i, j.dependencies, tc.deps)
				}
			}
		})
	}
}

// 重复优先于先后关系时，原因中不得出现先后关系的措辞——锁定“两种问题同时
// 存在保留重复依赖失败原因”的要求。
func TestDependencyListFaultDuplicatePrecedenceReasonWording(t *testing.T) {
	j := &storedJob{id: 3, dependencies: []uint64{9, 9}}
	reason, ok := dependencyListFault(j)
	if !ok {
		t.Fatal("deps [9,9] of job 3 must be illegal")
	}
	if !strings.Contains(reason, "依赖列表重复") {
		t.Fatalf("reason=%q must be the duplicate reason", reason)
	}
	if strings.Contains(reason, "依赖先后关系不合法") {
		t.Fatalf("duplicate reason must not be replaced by ordering reason: %q", reason)
	}
}
