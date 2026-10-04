package numeric

import "fmt"

// 本文件收拢“作业因直接上游结果不可用而失败”的判断与归因规则。运行期间
// （提交后级联、取消/失败传播）与重新打开归档（排队记录复查、已成功归档
// 复核）两处共用同一套规则，避免后续调整时两处说法不一致：
//
//  1. 选择直接阻断者：作业等待多个直接上游时，一律按提交时保存的依赖顺序
//     （storedJob.dependencies 的次序）选取最靠前的不可用上游，绝不按作业号
//     大小、完成先后或失败原因重新挑选。
//  2. 确定根因 BlockerID：沿选中的直接上游已有的阻断信息（blockerID）确定
//     根因——上游自身带根因时沿用，根因不因经过一个已归档的中间作业而重新
//     起算；上游没有根因记录时，以该上游自身作业号为根因；直接上游记录缺失
//     时以缺失的作业号为根因。
//  3. 构造失败原因：始终指出当前作业的直接上游；能取得上游自身失败原因时一并
//     附上；直接上游与根因不是同一作业时，再说明沿链条保留的根因作业号。
//
// 作业一旦据此进入失败状态，后续另一上游再失败或取消都不会重选直接阻断者、
// 改写原因或根因（调用方只对仍在排队/待复核的作业调用本文件的规则）。
//
// 三种场景对“不可用”的界定与措辞有必须保留的区别：
//   - 运行期间（含重开后排队记录遇到仍存在的失败/取消上游）：仅已失败或已
//     取消的上游阻断排队作业，原因表述为该上游“已失败/已被取消”；
//   - 重开归档时的排队记录：上游记录在目录中缺失也算不可用，以缺失作业号为
//     根因，并明确说明其结果无法使用；
//   - 重开归档时的成功记录：上游记录缺失、或恢复后未保持带完整归档的成功
//     状态都使成功结果不可用，原因表述为“成功结果不可用”。
//
// 若全部直接上游都存在且有效，仅本作业保存的追加输入与上游总和对不上，则属于
// 本作业自身归档错误（BlockerID 保持 0），由 firstAppendedMismatch 单独处理；
// 该核对只在没有不可用上游时进行，因此“追加输入不符”与“上游不可用”同时出现
// 时，仍归因给依赖顺序中最靠前的不可用上游。保存的依赖列表本身重复引用同一
// 作业号同样属于本作业记录自身有误（BlockerID 保持 0），由 duplicateDependency
// 识别，优先于上述一切与上游状态、追加数值相关的核对。

// upstreamBlocked 判断某直接上游在当前场景下是否阻断本作业。dep 为 nil 表示
// 归档目录中缺少该作业号的记录（仅重开归档可能出现）；返回 false 表示暂不能
// 据此判定失败——排队作业继续等待，成功记录则可继续核对追加输入。
type upstreamBlocked func(id uint64, dep *storedJob) bool

// runtimeUpstreamBlocked 是运行期间的判定：仅已失败/取消的上游阻断仍在排队的
// 下游；排队、运行中与已成功的上游都不阻断（继续等待）。运行期内存索引与已
// 接受记录一致，已接受的依赖不可能缺失，nil 视为不阻断。
func runtimeUpstreamBlocked(_ uint64, dep *storedJob) bool {
	return dep != nil && (dep.status == StatusFailed || dep.status == StatusCanceled)
}

// restoredQueuedUpstreamBlocked 是重开归档时对排队记录的判定：除已失败/取消
// 外，直接上游记录缺失同样使其不可能再变为可用，立即级联失败。
func restoredQueuedUpstreamBlocked(_ uint64, dep *storedJob) bool {
	return dep == nil || dep.status == StatusFailed || dep.status == StatusCanceled
}

// restoredArchiveUpstreamBlocked 是重开归档时对已成功记录的判定：直接上游记录
// 缺失，或恢复后未保持“带完整归档的成功状态”，其成功结果都不可继续使用。
func restoredArchiveUpstreamBlocked(_ uint64, dep *storedJob) bool {
	return dep == nil || dep.status != StatusSucceeded || dep.archive == nil
}

// pickBlockingUpstream 按作业保存的依赖顺序返回第一个不可用的直接上游。
// dep 为 nil 表示该作业号的记录缺失；没有不可用上游时 ok=false。选择只取决于
// 依赖列表中的先后，与作业号大小、完成先后、失败原因无关。
func pickBlockingUpstream(j *storedJob, byID map[uint64]*storedJob, blocked upstreamBlocked) (directID uint64, dep *storedJob, ok bool) {
	for _, id := range j.dependencies {
		d := byID[id]
		if blocked(id, d) {
			return id, d, true
		}
	}
	return 0, nil, false
}

// blockerRoot 沿选中的直接上游已有的阻断信息确定根因作业号：上游自身带有根因
// 时沿用（根因不随中间已归档作业重新起算）；上游没有根因记录、或直接上游记录
// 缺失时，以该直接上游作业号本身为根因。
func blockerRoot(dep *storedJob, directID uint64) uint64 {
	if dep != nil && dep.blockerID != 0 {
		return dep.blockerID
	}
	return directID
}

// runtimeBlockedHead 给出运行期间（以及重开排队记录遇到仍存在的失败/取消上游
// 时）原因的首句：直接上游已失败或已被取消。
func runtimeBlockedHead(dep *storedJob) string {
	state := "失败"
	if dep.status == StatusCanceled {
		state = "被取消"
	}
	return fmt.Sprintf("直接上游作业 %d 已%s，阻断本作业继续计算", dep.id, state)
}

// restoredMissingHead 给出重开排队记录遇到直接上游记录缺失时原因的首句：以缺失
// 的作业号为根因，并明确说明其结果无法使用。
func restoredMissingHead(directID uint64) string {
	return fmt.Sprintf(
		"直接上游作业 %d 无法使用：归档目录中缺少该作业的记录，其结果无法使用，阻断本作业继续计算",
		directID)
}

// restoredArchiveHead 给出重开已成功记录因直接上游结果失效而改判时原因的首句：
// 上游记录缺失或恢复后未保持带完整归档的成功状态，成功结果不可用。
func restoredArchiveHead(directID uint64) string {
	return fmt.Sprintf(
		"直接上游作业 %d 无法使用：不存在或恢复后未保持带完整归档的成功状态，成功结果不可用，阻断本作业继续计算",
		directID)
}

// blockedFailureReason 在首句 head 之后统一补齐归因尾部：附上直接上游自身的
// 失败原因（若有），并在直接上游与根因不同一时说明沿链条保留的根因作业号。
func blockedFailureReason(head string, dep *storedJob, directID, root uint64) string {
	reason := head
	if dep != nil && dep.status == StatusFailed && dep.failureReason != "" {
		reason += "（其失败原因：" + dep.failureReason + "）"
	}
	if root != directID {
		reason += fmt.Sprintf("；阻断根因为作业 %d", root)
	}
	return reason
}

// duplicateDependency 返回保存的依赖列表中首次再次出现的作业号——按列表顺序
// 扫描时第一个已经在前面出现过的作业号（不论相邻还是隔着其他上游）；没有重复
// 时 ok=false。唯一性按作业号判断，与上游总和无关：两个不同上游恰好同和时
// 按保存次序各占一个位置，不构成重复。
func duplicateDependency(deps []uint64) (dupID uint64, ok bool) {
	seen := make(map[uint64]struct{}, len(deps))
	for _, id := range deps {
		if _, dup := seen[id]; dup {
			return id, true
		}
		seen[id] = struct{}{}
	}
	return 0, false
}

// duplicateDependencyReason 给出依赖列表重复引用的失败原因：提交时不允许同一
// 份作业重复引用同一个直接上游，恢复时同样不接受——即使实际输入、数值结果、
// 日志、摘要与校验值全套自洽。这属于本作业记录自身有误（BlockerID 保持 0），
// 原因指出首次再次出现的上游作业号；被重复引用的上游保留自己的结果。
func duplicateDependencyReason(dupID uint64) string {
	return fmt.Sprintf(
		"保存的依赖列表重复：直接上游作业 %d 在依赖列表中多次出现，本作业记录有误，结果不可用",
		dupID)
}

// firstAppendedMismatch 在全部直接上游都存在且有效时，按保存的依赖顺序逐项核对
// 实际输入的追加部分（原始序列之后的 len(dependencies) 个位置），返回第一个对
// 不上的位置对应的上游作业号与失败原因；逐项相等时 ok=false。负数、零以及不同
// 上游恰好同和都按位置判断，不要求上游总和互不相同。
//
// 这属于本作业自身归档有误：有效上游不受影响，BlockerID 保持 0。调用前须确认
// 没有不可用上游（否则应优先归因给最靠前的不可用上游），且作业已通过
// effectiveInputsCorrespond（追加部分长度恰为依赖数量）。
func firstAppendedMismatch(j *storedJob, byID map[uint64]*storedJob) (directID uint64, reason string, ok bool) {
	n := len(j.values)
	for _, id := range j.dependencies {
		if j.effectiveValues[n] != byID[id].archive.Sum {
			return id, fmt.Sprintf(
				"实际输入的追加部分与直接上游作业 %d 的总和无法对应（追加值须按保存的依赖顺序逐项等于各上游总和），成功结果不可用",
				id), true
		}
		n++
	}
	return 0, "", false
}
