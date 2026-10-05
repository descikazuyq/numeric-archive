package numeric

// 本文件是“单依赖字段（HasDependency + DependencyID）”与“有序依赖列表
// （Dependencies）”两种写法的唯一兼容解释处。提交内容比较、重新打开归档
// （顶层记录与成功归档各一处）以及成功归档核对都通过本文件的同一组规则理解
// 同一份依赖内容，避免在多处重复推导而产生分歧：
//
//  1. requestDeps：提交请求中的两种写法规范化为同一份有序上游作业号列表；
//  2. recordDeps：已保存记录（顶层 jobRecord 或成功 Archive）中的两种字段
//     组合恢复为同一份有序上游作业号——列表优先于单依赖字段；
//  3. depsEqual：两份有序依赖内容逐项、按原次序比较，不按作业号排序；
//  4. firstDep：无歧义的“首个直接上游”。
//
// 已保存记录与提交请求的兼容含义不同，必须分开处理：
//   - 提交请求受 validateDependencyShapeLocked 的互斥/零/重复格式约束，
//     requestDeps 只在该校验通过后参与幂等内容比较与上游存在性校验；
//   - 已保存记录不套用提交请求的互斥要求——已有的新格式记录可能同时保存
//     非空列表与单依赖字段，此时仍以列表为实际依赖；旧记录只有单依赖字段
//     时恢复为只含该作业号的列表。成功归档与作业记录的依赖内容按相同含义
//     核对，旧的单依赖归档仍可使用。
//
// 实际依赖永远以规范化/恢复出的列表为准：返回详情、成功归档中的依赖列表继续
// 按原次序表达实际依赖，不按作业号排序。

// requestDeps 规范化一次提交请求的直接上游作业号：
//   - 非空 Dependencies 优先（保持原次序）；
//   - 否则 HasDependency 时退化为只含 DependencyID 的单元素列表；
//   - 两者皆无（含“未启用单依赖时单独填写 DependencyID”）返回 nil，作业不会
//     凭空等待上游；空列表也不会覆盖已启用的单依赖（HasDependency 为 true
//     且列表为空时仍取单依赖）。
//
// 调用前须先经 Store.validateDependencyShapeLocked（结构/零/重复/互斥校验）
// 与（新请求路径上的）Store.validateDependenciesExistLocked 校验。
func requestDeps(req *SubmitRequest) []uint64 {
	if len(req.Dependencies) > 0 {
		return append([]uint64(nil), req.Dependencies...)
	}
	if req.HasDependency {
		return []uint64{req.DependencyID}
	}
	return nil
}

// recordDeps 把一份已保存记录的依赖字段恢复为有序直接上游作业号：
//   - 非空 Dependencies 优先（保持原次序），即使同时保存了单依赖字段也以
//     列表为实际依赖——已保存记录不套用提交请求的互斥要求；
//   - 否则 HasDependency 时恢复为只含 DependencyID 的单元素列表（旧记录
//     只有单依赖字段时恢复同一个上游）；
//   - 两者皆无返回 nil（无依赖）。
//
// 顶层作业记录与成功归档中的字段组合各调用一次，按相同含义恢复后再做内容核对。
func recordDeps(hasDep bool, depID uint64, list []uint64) []uint64 {
	if len(list) > 0 {
		return append([]uint64(nil), list...)
	}
	if hasDep {
		return []uint64{depID}
	}
	return nil
}

// depsEqual 判断两份有序依赖内容是否一致：长度相同且逐项、按原次序相等。
// 单依赖字段与只含同一作业号的列表视为相同内容；调换列表次序、更换上游都
// 属于内容不一致。比较结果只取决于规范化/恢复后的有序内容，与写法
// （单依赖/列表）无关，也不按作业号排序。
func depsEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// firstDep 返回首个直接上游作业号；无依赖时返回 0。依赖含义（是否有依赖、
// 首个上游是谁）一律以恢复/规范化出的实际依赖列表为准，供写出记录与对外
// 视图统一派生 has_dependency/dependency_id 字段。
func firstDep(deps []uint64) uint64 {
	if len(deps) == 0 {
		return 0
	}
	return deps[0]
}
