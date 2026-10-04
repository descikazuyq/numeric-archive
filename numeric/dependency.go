package numeric

import "fmt"

// 本文件集中维护“依赖阻断”的判定与归因规则，运行期间的级联失败
// （[Store.cascadeLocked]）与重新打开归档时的依赖检查（loadDir 第二趟
// 扫描、dependencyInputsCheck）共用同一套实现，避免两处各自表述漂移。
//
// 统一的归因规则：
//   - 直接上游的选择：作业等待多个直接上游时，按提交时保存的依赖顺序选取
//     最靠前的不可用上游；不按作业号大小、完成先后或失败原因重新挑选。
//   - 根因的确定：失败原因指出当前作业的直接上游，BlockerID 沿该上游已有
//     的阻断信息保留最初的根因；上游没有根因记录时，以该上游自身的作业号
//     为根因。直接上游与根因不是同一作业时，原因中同时说明两者。
//   - 直接上游记录缺失（仅重开归档时可能出现）：以缺失的作业号为根因，
//     并明确说明结果无法使用。
//   - 作业一旦因依赖阻断进入失败状态，后续其他上游再失败或取消，不改写
//     已经确定的原因和根因（各调用方只对排队/成功记录应用本规则，失败
//     终态不会被再次归因）。

// firstUnavailableDep 按保存的依赖顺序选取最靠前的不可用直接上游，
// 返回其作业号、记录以及记录是否缺失；全部可用时返回 depID == 0。
//
// “不可用”由谓词 unavailable 按调用场景定义（运行期/重开归档的排队记录
// 用 depBlocked，重开归档的成功记录用 depResultUnavailable）；直接上游
// 记录缺失在任何场景下一律视为不可用。运行期间直接上游记录必然存在
// （提交时校验存在性且记录从不删除），缺失分支仅在重开归档时可达。
func firstUnavailableDep(deps []uint64, byID map[uint64]*storedJob, unavailable func(dep *storedJob) bool) (depID uint64, dep *storedJob, missing bool) {
	for _, id := range deps {
		d := byID[id]
		if d == nil {
			return id, nil, true
		}
		if unavailable(d) {
			return id, d, false
		}
	}
	return 0, nil, false
}

// depBlocked 判定直接上游对排队作业不可用：已失败或已取消。
// 仍在排队或运行的上游只是尚未就绪，不属于不可用。
func depBlocked(dep *storedJob) bool {
	return dep.status == StatusFailed || dep.status == StatusCanceled
}

// depResultUnavailable 判定直接上游对重开归档时的成功记录不可用：
// 恢复后未保持带完整归档的成功状态。
func depResultUnavailable(dep *storedJob) bool {
	return dep.status != StatusSucceeded || dep.archive == nil
}

// blockerRootOf 沿直接上游已有的阻断信息确定根因作业号：上游自身携带
// 根因时沿用，否则以上游自身的作业号为根因。
func blockerRootOf(dep *storedJob) uint64 {
	if dep.blockerID != 0 {
		return dep.blockerID
	}
	return dep.id
}

// blockedReason 构造“因直接上游失败/取消而被阻断”的失败原因：指出当前
// 作业的直接上游及其状态，附上该上游自身的失败原因；直接上游与根因
// 不是同一作业时，再说明沿链条保留的根因作业号。
func blockedReason(dep *storedJob, root uint64) string {
	state := "失败"
	if dep.status == StatusCanceled {
		state = "被取消"
	}
	reason := fmt.Sprintf("直接上游作业 %d 已%s，阻断本作业继续计算", dep.id, state)
	if dep.status == StatusFailed && dep.failureReason != "" {
		reason += "（其失败原因：" + dep.failureReason + "）"
	}
	if root != dep.id {
		reason += fmt.Sprintf("；阻断根因为作业 %d", root)
	}
	return reason
}

// missingDepBlockedReason 构造重开归档时“直接上游记录缺失”的失败原因：
// 以缺失的作业号为根因，并明确说明其结果无法使用。
func missingDepBlockedReason(depID uint64) string {
	return fmt.Sprintf(
		"直接上游作业 %d 无法使用：归档目录中缺少该作业的记录，其结果无法使用，阻断本作业继续计算",
		depID)
}

// restoredBlockedReason 构造重开归档时“因直接上游结果失效而失败”的原因：
// detail 说明直接上游 depID 为何无法使用及其成功结果不可用；随后附上该
// 直接上游自身的失败原因，直接上游与根因 root 不同或直接上游记录缺失时，
// 再说明沿链条保留的根因作业号。
func restoredBlockedReason(dep *storedJob, depID, root uint64, detail string) string {
	reason := detail + "，阻断本作业继续计算"
	if dep != nil && dep.status == StatusFailed && dep.failureReason != "" {
		reason += "（其失败原因：" + dep.failureReason + "）"
	}
	if root != depID {
		reason += fmt.Sprintf("；阻断根因为作业 %d", root)
	}
	return reason
}
