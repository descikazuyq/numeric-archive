package numeric

// 本文件收拢作业依赖两种写法（旧式单依赖字段 HasDependency/DependencyID 与
// 有序依赖列表 Dependencies）之间的相互换算，让提交内容比较、重新打开归档
// 与成功归档核对对同一份依赖内容保持同一种理解：
//
//   - 读方向（normalizeDependencies）：把两种写法统一为有序依赖列表——非空
//     列表优先并保持原次序；列表为空（含缺省）且单依赖启用时，退化为只含该
//     作业号的列表；两者皆无视为无依赖。未启用单依赖时单独填写的
//     DependencyID 不产生任何依赖，空列表也不会覆盖已启用的单依赖。
//   - 写方向（legacyDependencyFields）：由规范化后的有序列表推导记录与归档
//     中保留的旧式单依赖字段——有无依赖由列表是否为空决定，单依赖作业号取
//     列表首项（即最早追加其总和的上游）；列表为空时两个字段都是零值。
//
// 提交请求的格式校验（两种写法互斥、零与重复作业号）仍在提交路径上先行完成，
// 不属于这里的换算；已保存的记录与归档则允许两类字段同时出现，一律以非空
// 列表为实际依赖，不套用提交请求的互斥要求。

// normalizeDependencies 把“单依赖字段 + 有序依赖列表”统一为有序依赖列表。
// 返回值永远是新切片，调用方可安全持有；无依赖时返回 nil。
func normalizeDependencies(hasDependency bool, dependencyID uint64, dependencies []uint64) []uint64 {
	if len(dependencies) > 0 {
		return append([]uint64(nil), dependencies...)
	}
	if hasDependency {
		return []uint64{dependencyID}
	}
	return nil
}

// legacyDependencyFields 由规范化后的有序依赖列表推导旧式单依赖字段：
// 列表非空时 hasDependency 为 true、dependencyID 为列表首项；空列表对应
// 两个零值。
func legacyDependencyFields(dependencies []uint64) (hasDependency bool, dependencyID uint64) {
	if len(dependencies) == 0 {
		return false, 0
	}
	return true, dependencies[0]
}
