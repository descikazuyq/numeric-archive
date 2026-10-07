package numeric

import "fmt"

// 本文件收拢“重新打开归档时，已保存的依赖列表自身是否合法”的唯一判断规则。
// 排队记录（record.go 第二趟升序扫描中的复查）与成功记录（第一趟成功归档复核）
// 共用这里的同一判断与同一份失败原因，避免以后调整规则时只改到一种状态、
// 让另一类记录漏判或给出不同说法。
//
// 规则严格按以下顺序执行，调用方不得自行调换或只取其中一项：
//
//  1. 重复性：同一作业号在依赖列表中再次出现即不合法，相邻重复与隔着其他
//     上游的重复同样识别；唯一性按作业号判断，两个不同上游即使总和相同也
//     不是重复。原因指出保存顺序中“首次再次出现”的作业号。
//  2. 先后关系：没有重复时，才按保存的原顺序找第一个作业号不小于本作业号
//     的直接上游（引用自己或后来接受的作业）。正常提交只能引用已存在的作业，
//     作业号又按接受先后递增，所以每个直接上游都必须严格小于本作业号；不
//     比较提交时间，也不要求列表按作业号排序。
//
// 两类问题都属于记录自身有误，与上游是否存在、是否成功无关，也不因本作业的
// 数值、日志、摘要与校验值全部自洽而放行，因此 BlockerID 恒为 0，被引用且
// 原本有效的上游保留自己的结果。两类问题同时存在时保留重复依赖的失败原因
// （重复性检查先行），不能因整理逻辑改成先后关系的说明。
//
// 运行期间提交时这两项要求分别由 validateDependencyShapeLocked（唯一性）与
// validateDependenciesExistLocked（只能引用已存在作业，作业号又按接受先后
// 递增）强制；本文件只服务于重新打开归档：已保存记录可能绕过提交校验（旧
// 版本写入或记录被改动），恢复时必须按与提交一致的规则改判失败。依赖两种
// 写法（旧式单依赖字段与有序列表）的换算仍在 deps.go，进入本规则前已经统一
// 为有序列表，因此旧式单依赖记录与有序列表遵守同一项判断。

// dependencyListFault 按上述唯一规则检查恢复作业保存的依赖列表自身是否合法，
// 返回对应失败原因（非法时 ok 为 true）。排队记录与成功记录都只通过本函数
// 取得这项结论：先查重复，再查先后关系；无依赖或列表合法时 ok 为 false。
func dependencyListFault(j *storedJob) (reason string, ok bool) {
	if id, dup := firstDuplicateDependencyID(j.dependencies); dup {
		return duplicateDependencyReason(id), true
	}
	if id, bad := firstDependencyIDNotBeforeSelf(j.id, j.dependencies); bad {
		return illegalOrderingDependencyReason(id, j.id), true
	}
	return "", false
}

// firstDuplicateDependencyID 按依赖列表的保存顺序找出第一个再次出现的上游
// 作业号（即该作业号第二次出现的位置；相邻重复与隔着其他上游的重复同样
// 识别）。唯一性按作业号判断，与上游总和是否相同无关：两个不同上游都得到
// 3 时各引用一次并不重复。没有重复时 ok=false。
func firstDuplicateDependencyID(dependencies []uint64) (id uint64, ok bool) {
	seen := make(map[uint64]struct{}, len(dependencies))
	for _, id := range dependencies {
		if _, dup := seen[id]; dup {
			return id, true
		}
		seen[id] = struct{}{}
	}
	return 0, false
}

// firstDependencyIDNotBeforeSelf 按依赖列表的保存顺序找出第一个作业号不小于
// selfID 的直接上游：等于 selfID 即引用自己，大于即引用后来接受的作业，两者
// 都违反先后关系；只比较作业号，不比较提交时间，也不要求列表排序。没有
// 违反时 ok=false。
func firstDependencyIDNotBeforeSelf(selfID uint64, dependencies []uint64) (id uint64, ok bool) {
	for _, id := range dependencies {
		if id >= selfID {
			return id, true
		}
	}
	return 0, false
}

// duplicateDependencyReason 构造“依赖列表重复”的失败原因：明确说明依赖列表
// 重复引用，并指出首次再次出现的上游作业号。这属于本作业记录自身有误，
// 与上游是否有效无关——即使被重复引用的上游恢复后仍成功，也不把它判成
// 失败或归因给它，因此 BlockerID 保持 0。
func duplicateDependencyReason(id uint64) string {
	return fmt.Sprintf(
		"依赖列表重复：直接上游作业 %d 在依赖列表中再次出现，同一作业不能重复引用，成功结果不可用",
		id)
}

// illegalOrderingDependencyReason 构造“依赖先后关系不合法”的失败原因：指出
// 保存的依赖顺序中第一个不合法的上游作业号，并说明它不能作为本作业的上游。
// BlockerID 保持 0——这是本作业记录自身有误，即使被引用的作业存在且成功也不
// 把它判成失败或归因给它。
func illegalOrderingDependencyReason(id, selfID uint64) string {
	return fmt.Sprintf(
		"依赖先后关系不合法：保存的依赖顺序中作业 %d 是第一个作业号不小于本作业 %d 的直接上游，作业号按接受先后递增，它不能作为本作业的上游，成功结果不可用",
		id, selfID)
}
