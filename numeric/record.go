package numeric

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// rawBase64Prefix 标记 _raw 字段中的值是原始字节的 base64 编码。
// ':' 不在 base64 字母表内，普通文本（即使本身像 base64，如 "abcd"）
// 不会以该前缀开头；而 _raw 字段由本版本写入，旧归档不含此字段，
// 因此标记无歧义。
const rawBase64Prefix = "base64:"

// unmarshalRaw 恢复 rawIdentity 写出的字段；带 base64 前缀的兜底值解码回
// 原始字节，普通 JSON 字符串原样返回。
func unmarshalRaw(s string) (string, error) {
	if strings.HasPrefix(s, rawBase64Prefix) {
		b, err := base64.StdEncoding.DecodeString(s[len(rawBase64Prefix):])
		if err != nil {
			return "", fmt.Errorf("numeric: 标识原始字节解码失败: %w", err)
		}
		return string(b), nil
	}
	return s, nil
}

// identityRaw 携带提交人、请求号的完整字节兜底表示。只有当对应字段含
// 非法 UTF-8 字节时相应成员才非空；普通文本记录不写这两个字段，
// 保持旧格式归档可直接打开。
type identityRaw struct {
	Submitter string `json:"submitter_raw,omitempty"`
	RequestID string `json:"request_id_raw,omitempty"`
}

// rawIdentity 在标识含非法 UTF-8 字节时构造完整字节兜底字段；
// 两个标识都是合法 UTF-8 时返回 nil，记录与旧格式完全一致。
func rawIdentity(submitter, requestID string) *identityRaw {
	if utf8.ValidString(submitter) && utf8.ValidString(requestID) {
		return nil
	}
	raw := &identityRaw{}
	if !utf8.ValidString(submitter) {
		raw.Submitter = rawBase64Prefix + base64.StdEncoding.EncodeToString([]byte(submitter))
	}
	if !utf8.ValidString(requestID) {
		raw.RequestID = rawBase64Prefix + base64.StdEncoding.EncodeToString([]byte(requestID))
	}
	return raw
}

// resolveIdentity 恢复提交人、请求号：_raw 兜底字段存在且非空时以其
// 解码出的完整字节为准，否则使用 JSON 字符串字段（旧归档与普通文本）。
func resolveIdentity(plainSub, plainReq string, raw *identityRaw) (string, string, error) {
	var err error
	sub, req := plainSub, plainReq
	if raw != nil {
		if raw.Submitter != "" {
			if sub, err = unmarshalRaw(raw.Submitter); err != nil {
				return "", "", err
			}
		}
		if raw.RequestID != "" {
			if req, err = unmarshalRaw(raw.RequestID); err != nil {
				return "", "", err
			}
		}
	}
	return sub, req, nil
}

// recordVersion 是落盘记录格式的版本标记。
const recordVersion = "numeric-job-v1"

// storedJob 是作业在内存中的完整状态，所有字段均由 Store.mu 保护。
type storedJob struct {
	id           uint64
	submitter    string
	requestID    string
	seed         int64
	values       []int64
	dependencies []uint64 // 有序直接上游作业号；空表示无依赖

	queuedAt   time.Time
	startedAt  time.Time
	finishedAt time.Time

	status        Status
	failureReason string
	blockerID     uint64

	// 成功后保存的有效输入与归档（与 succeeded 状态同一次原子写入）。
	effectiveValues []int64
	archive         *Archive

	// canceled 仅存于内存：运行中的作业收到取消/关闭请求后置位，
	// 计算循环据此尽快中止；终态不会依赖它落盘（终态本身会持久化）。
	canceled atomic.Bool
}

// jobRecord 是 storedJob 的落盘 JSON 表示。成功状态与归档在同一记录内，
// 一次 temp-file + rename 原子写入，因此对外不会出现“成功却没有结果”。
type jobRecord struct {
	Version string `json:"version"`

	ID        uint64 `json:"id"`
	Submitter string `json:"submitter"`
	RequestID string `json:"request_id"`
	// IdentityRaw 仅在提交人或请求号含非法 UTF-8 字节时出现，按完整字节
	// 保真恢复标识；合法 UTF-8（含 U+0000）时缺省，记录与旧格式一致。
	IdentityRaw   *identityRaw `json:"identity_raw,omitempty"`
	Seed          int64        `json:"seed"`
	Values        []int64      `json:"values"`
	HasDependency bool         `json:"has_dependency"`
	DependencyID  uint64       `json:"dependency_id"`
	// Dependencies 为有序直接上游作业号；旧格式记录无此字段，
	// 恢复时由 HasDependency/DependencyID 推导。
	Dependencies []uint64 `json:"dependencies,omitempty"`

	QueuedAt   time.Time `json:"queued_at"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`

	Status        Status `json:"status"`
	FailureReason string `json:"failure_reason,omitempty"`
	BlockerID     uint64 `json:"blocker_id,omitempty"`

	EffectiveValues []int64  `json:"effective_values,omitempty"`
	Archive         *Archive `json:"archive,omitempty"`
}

func jobFileName(id uint64) string {
	return fmt.Sprintf("job-%020d.json", id)
}

func (s *Store) jobPath(j *storedJob) string {
	return filepath.Join(s.dir, jobFileName(j.id))
}

func encodeRecord(j *storedJob) ([]byte, error) {
	r := jobRecord{
		Version: recordVersion,

		ID:            j.id,
		Submitter:     j.submitter,
		RequestID:     j.requestID,
		IdentityRaw:   rawIdentity(j.submitter, j.requestID),
		Seed:          j.seed,
		Values:        append([]int64(nil), j.values...),
		HasDependency: len(j.dependencies) > 0,
		DependencyID:  depID(j),
		Dependencies:  append([]uint64(nil), j.dependencies...),

		QueuedAt:      j.queuedAt,
		StartedAt:     j.startedAt,
		FinishedAt:    j.finishedAt,
		Status:        j.status,
		FailureReason: j.failureReason,
		BlockerID:     j.blockerID,

		EffectiveValues: append([]int64(nil), j.effectiveValues...),
		Archive:         j.archive,
	}
	return json.MarshalIndent(&r, "", "  ")
}

// depID 返回首个直接上游作业号（无依赖时为 0）。
func depID(j *storedJob) uint64 {
	if len(j.dependencies) == 0 {
		return 0
	}
	return j.dependencies[0]
}

// persist 在已持锁的情况下原子写一条作业记录。
func (s *Store) persist(j *storedJob) error {
	// 测试用故障注入点（生产代码中 persistFault 恒为 nil）。
	if s.persistFault != nil {
		if err := s.persistFault(j); err != nil {
			return err
		}
	}
	data, err := encodeRecord(j)
	if err != nil {
		return err
	}
	return writeFileAtomic(s.dir, jobFileName(j.id), data, 0o600)
}

// writeFileAtomic 将 data 写入 dir/name：同目录临时文件 → fsync → rename
// → 目录 fsync，保证记录要么是旧内容要么是新内容，不会出现半条记录。
func writeFileAtomic(dir, name string, data []byte, perm os.FileMode) error {
	tmp, err := os.CreateTemp(dir, ".tmp-"+name+".*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		return err
	}
	if err := os.Rename(tmpName, filepath.Join(dir, name)); err != nil {
		return err
	}
	cleanup = false
	syncDir(dir)
	return nil
}

// syncDir 对目录本身做一次 fsync，让新建项与 rename 在崩溃后仍然成立。
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}

// loadDir 扫描目录中的全部作业记录并重建内存索引。
//
// 恢复规则：
//   - 上次关闭时仍为 running 的作业标记为失败，原因是计算被中断；
//   - 成功记录缺少归档、校验值对不上，实际输入与原始参数或直接依赖数量
//     不对应，或归档数值结果与有效输入按计算规则应得的结果不符时，
//     fail-closed 地标记为失败，维持“成功必有完整归档”的不变量；
//   - 有依赖的成功记录还要求每个直接上游都存在、恢复后仍是带完整归档的
//     成功状态，且实际输入的追加部分逐项等于这些上游按保存的依赖顺序
//     排列的总和；上游不可用的同样改判为失败，其失败原因指出直接上游、
//     BlockerID 沿链条保留最初的根因（与运行中依赖失败同一规则）；
//     仅追加输入对不上而上游全部有效的属于本作业自身归档有误，BlockerID
//     保持 0。仍在排队的作业在同一趟升序扫描中按既有依赖规则级联失败。
func loadDir(dir string) ([]*storedJob, uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}
	var jobs []*storedJob
	var maxID uint64
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		// 清理上一进程崩溃时可能残留的原子写临时文件。
		if strings.HasPrefix(name, ".tmp-") {
			_ = os.Remove(filepath.Join(dir, name))
			continue
		}
		if !strings.HasPrefix(name, "job-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, 0, err
		}
		var r jobRecord
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, 0, fmt.Errorf("numeric: 记录 %s 损坏: %w", name, err)
		}
		if r.Version != recordVersion {
			return nil, 0, fmt.Errorf("numeric: 记录 %s 版本不受支持: %s", name, r.Version)
		}
		// 提交人、请求号按完整字节恢复：含非法 UTF-8 字节时以
		// identity_raw 兜底字段为准，普通文本（含 U+0000）与旧归档原样使用。
		submitter, requestID, err := resolveIdentity(r.Submitter, r.RequestID, r.IdentityRaw)
		if err != nil {
			return nil, 0, fmt.Errorf("numeric: 记录 %s 标识损坏: %w", name, err)
		}
		// 成功归档内嵌同一份标识，同样按其自身兜底字段恢复，保证顶层记录
		// 与归档中的参数逐字节一致。
		if r.Archive != nil {
			aSub, aReq, aErr := resolveIdentity(r.Archive.Submitter, r.Archive.RequestID, r.Archive.IdentityRaw)
			if aErr != nil {
				return nil, 0, fmt.Errorf("numeric: 记录 %s 归档标识损坏: %w", name, aErr)
			}
			r.Archive.Submitter = aSub
			r.Archive.RequestID = aReq
		}
		switch r.Status {
		case StatusQueued, StatusRunning, StatusSucceeded, StatusFailed, StatusCanceled:
		default:
			return nil, 0, fmt.Errorf("numeric: 记录 %s 状态非法: %q", name, r.Status)
		}
		// 新格式记录直接给出有序依赖列表；旧格式记录只有单依赖字段，
		// 恢复为只含一个作业号的列表（语义与提交时一致）。
		deps := append([]uint64(nil), r.Dependencies...)
		if len(deps) == 0 && r.HasDependency {
			deps = []uint64{r.DependencyID}
		}
		j := &storedJob{
			id:           r.ID,
			submitter:    submitter,
			requestID:    requestID,
			seed:         r.Seed,
			values:       append([]int64(nil), r.Values...),
			dependencies: deps,

			queuedAt:   r.QueuedAt,
			startedAt:  r.StartedAt,
			finishedAt: r.FinishedAt,

			status:        r.Status,
			failureReason: r.FailureReason,
			blockerID:     r.BlockerID,

			effectiveValues: append([]int64(nil), r.EffectiveValues...),
			archive:         r.Archive,
		}
		if j.status == StatusRunning {
			j.status = StatusFailed
			j.failureReason = "计算被中断：归档上次关闭时作业仍在运行"
			j.blockerID = 0
			if j.finishedAt.IsZero() {
				j.finishedAt = time.Now().UTC()
			}
			j.effectiveValues = nil
			j.archive = nil
			// 就地重写恢复后的终态。
			if data, err := encodeRecord(j); err == nil {
				_ = writeFileAtomic(dir, jobFileName(j.id), data, 0o600)
			}
		}
		if j.status == StatusSucceeded {
			// 除完整性外，还必须满足两条独立的对应/复算规则：
			//  1. 实际输入与原始参数、直接依赖数量逐项对应——原始序列非空且
			//     按原次序完整出现在实际输入开头，总长度恰为原始长度加上直接
			//     依赖数；摘要、日志、校验值只与“实际输入”绑定，换成另一份
			//     序列（如 [-3,2] 冒充 [2,-3]）仍能全套自洽，只有按位置比对
			//     原始参数与依赖数量才能识别。
			//  2. 归档中的数值结果必须与实际输入按既有计算规则应得的结果一致：
			//     日志、摘要与校验值都可能与错误数字自洽，只有按保存的实际
			//     输入复算才能识别。
			// 任一不符合都按损坏归档处理。第三条规则——追加部分逐项等于各
			// 直接上游恢复后的总和——需要全部记录恢复完毕才能核对，
			// 在排序后的第二趟扫描中执行。
			var invalidReason string
			switch {
			case !archiveIntact(j):
				invalidReason = "归档不完整或校验值不一致，成功结果不可用"
			case !effectiveInputsCorrespond(j):
				invalidReason = "实际输入与原始参数或依赖数量不符，成功结果不可用"
			case !archiveResultsValid(j):
				invalidReason = "归档数值结果与有效输入按计算规则应得的结果不符，成功结果不可用"
			}
			if invalidReason != "" {
				j.status = StatusFailed
				j.failureReason = invalidReason
				// 归档自身校验不通过：属于本作业自身失败，不携带阻断根因。
				j.blockerID = 0
				if j.finishedAt.IsZero() {
					j.finishedAt = time.Now().UTC()
				}
				j.effectiveValues = nil
				j.archive = nil
				if data, err := encodeRecord(j); err == nil {
					_ = writeFileAtomic(dir, jobFileName(j.id), data, 0o600)
				}
			}
		}
		jobs = append(jobs, j)
		if j.id > maxID {
			maxID = j.id
		}
	}
	// 恢复后的排列必须与关闭前的接受先后一致：作业号在接受时单调分配，
	// 按作业号升序即提交先后。记录上的提交时间只用于时间范围过滤，
	// 不参与排序——本机时钟回拨会让后接受的作业带有更早的时间，
	// 按时间重排会改变已确定的先后关系（也会让上游作业号更大的
	// 依赖链在单趟级联扫描中漏掉失败传播）。
	sort.Slice(jobs, func(a, b int) bool {
		return jobs[a].id < jobs[b].id
	})
	// 第二趟：按作业号升序单趟处理依赖失效。直接上游在提交时必须已存在，
	// 上游作业号必然更小，因此每个作业被处理时其全部直接上游都已是本次恢复
	// 确定的最终状态——上游被改判失败时携带的根因作业号可直接沿链条继承，
	// 不会在每经过一个已归档的中间作业时重新起算。
	//
	// 两类作业与正常运行中的依赖失败遵守同一项归因规则（实现集中在
	// dependency.go）：失败原因指出当前作业的直接上游，BlockerID 沿链条
	// 保留最初使结果不可用的根因；只有本作业自身归档有误（上游全部有效、
	// 仅保存的追加输入不符）时 BlockerID 才保持 0。
	//   - 带依赖的成功记录：追加部分与上游恢复后的结果逐项核对；
	//   - 排队记录：按保存的依赖顺序选取最靠前的不可用上游（已失败/取消，
	//     或记录缺失），立即级联失败，不再等待。
	byID := make(map[uint64]*storedJob, len(jobs))
	for _, j := range jobs {
		byID[j.id] = j
	}
	// failRestored 把恢复阶段判定失败的作业就地改判并原子改写落盘记录，
	// 使按作业号读取、按提交人列举与磁盘记录保留相同的根因。
	failRestored := func(j *storedJob, reason string, root uint64) {
		j.status = StatusFailed
		j.failureReason = reason
		j.blockerID = root
		if j.finishedAt.IsZero() {
			j.finishedAt = time.Now().UTC()
		}
		j.effectiveValues = nil
		j.archive = nil
		if data, err := encodeRecord(j); err == nil {
			_ = writeFileAtomic(dir, jobFileName(j.id), data, 0o600)
		}
	}
	for _, j := range jobs {
		if len(j.dependencies) == 0 {
			continue
		}
		switch j.status {
		case StatusSucceeded:
			depID, unavailable, detail := dependencyInputsCheck(j, byID)
			if detail == "" {
				continue
			}
			if !unavailable {
				// 直接上游全部存在且有效，只是本作业保存的追加输入与上游
				// 总和不符：属于本作业自身归档有误，不把有效上游判成失败
				// 或归因给它。
				failRestored(j, detail, 0)
				continue
			}
			// 因直接上游结果失效而失败：沿该上游已有的阻断信息保留根因；
			// 直接上游记录缺失时以缺失的作业号本身作为根因。
			dep := byID[depID]
			root := depID
			if dep != nil {
				root = blockerRootOf(dep)
			}
			failRestored(j, restoredBlockedReason(dep, depID, root, detail), root)
		case StatusQueued:
			// 与运行期间的级联失败共用同一套选取与归因规则（见
			// dependency.go）：按保存的依赖顺序选取最靠前的不可用上游，
			// 其他上游作业号更小或失败原因不同都不能改变这个选择。
			directID, direct, missing := firstUnavailableDep(j.dependencies, byID, depBlocked)
			if directID == 0 {
				continue // 上游仍在排队等情况：继续等待，不挡住其他作业。
			}
			if missing {
				// 直接上游的记录在归档目录中缺失，其结果不可能再变为可用：
				// 以缺失的作业号作为根因，并明确说明结果无法使用。
				failRestored(j, missingDepBlockedReason(directID), directID)
				continue
			}
			root := blockerRootOf(direct)
			failRestored(j, blockedReason(direct, root), root)
		}
	}
	return jobs, maxID, nil
}

// dependencyInputsCheck 核对有依赖的成功记录：实际输入的追加部分（原始
// 序列之后的 len(dependencies) 个位置）必须与直接上游恢复后的结果对应——
//   - 每个直接上游都必须存在、恢复后仍是成功状态且带有通过全部校验的完整
//     归档；上游在本次打开中被其他校验改判失败的，本作业也不能继续使用它；
//   - 追加部分逐项等于这些上游按保存的依赖顺序排列的总和。负数、零以及
//     不同上游恰好同和都按位置判断，不要求上游总和互不相同。
//
// 多依赖作业按保存的依赖顺序选取最靠前的不可用上游，其他上游的作业号更小
// 或失败原因不同都不能改变这个选择；注意“不可用”与“追加值不符”分两趟
// 判断：只要依赖列表中存在不可用上游，就归因到其中最靠前的一个，即使更
// 靠前的有效上游处恰好保存了错误的追加值——只有全部上游都有效时，追加值
// 按位置对不上才属于本作业自身的归档错误。
//
// 返回：
//   - upstreamUnavailable=true：选中的直接上游 depID 无法使用（记录缺失或
//     恢复后未保持带完整归档的成功状态）。本作业的失败属于上游结果失效，
//     root 沿该上游已有的阻断信息确定（直接上游记录缺失时以缺失的作业号
//     本身为根因），与运行中发生的依赖级联遵守同一项规则。
//   - upstreamUnavailable=false 且 reason != ""：全部直接上游都有效，只是
//     本作业保存的追加输入与上游总和按位置对不上。这属于本作业自身归档
//     有误，不能把有效上游判成失败或归因给它，BlockerID 保持 0。
//   - reason == ""：对应关系成立，记录保持成功。
//
// 只读上游状态，绝不因下游追加值错误改动上游。调用前须已通过
// effectiveInputsCorrespond（保证追加部分长度恰为依赖数量）。
func dependencyInputsCheck(j *storedJob, byID map[uint64]*storedJob) (depID uint64, upstreamUnavailable bool, reason string) {
	// 第一趟：按保存的依赖顺序找出最靠前的不可用上游（与排队记录的
	// 选取共用同一实现，只是“不可用”的判定不同）。
	if id, _, _ := firstUnavailableDep(j.dependencies, byID, depResultUnavailable); id != 0 {
		return id, true,
			fmt.Sprintf("直接上游作业 %d 无法使用：不存在或恢复后未保持带完整归档的成功状态，成功结果不可用", id)
	}
	// 第二趟：全部直接上游都有效，追加值按位置逐项核对，选出最靠前的不符。
	n := len(j.values)
	for i, id := range j.dependencies {
		dep := byID[id]
		if j.effectiveValues[n+i] != dep.archive.Sum {
			return id, false,
				fmt.Sprintf("实际输入的追加部分与直接上游作业 %d 的总和无法对应（追加值须按保存的依赖顺序逐项等于各上游总和），成功结果不可用", id)
		}
	}
	return 0, false, ""
}

// archiveIntact 复算成功归档的全部确定性字段，任何不一致都视为不可用。
func archiveIntact(j *storedJob) bool {
	a := j.archive
	if a == nil {
		return false
	}
	// 归档中的原始参数必须与记录顶层字段一致。
	if a.JobID != j.id || a.Submitter != j.submitter || a.RequestID != j.requestID ||
		a.Seed != j.seed || a.HasDependency != (len(j.dependencies) > 0) {
		return false
	}
	if a.HasDependency && a.DependencyID != depID(j) {
		return false
	}
	// 依赖列表（内容与次序）必须一致。旧格式归档没有 dependencies 字段，
	// 由单依赖字段推导后再比较。
	aDeps := append([]uint64(nil), a.Dependencies...)
	if len(aDeps) == 0 && a.HasDependency {
		aDeps = []uint64{a.DependencyID}
	}
	if len(aDeps) != len(j.dependencies) {
		return false
	}
	for i := range j.dependencies {
		if aDeps[i] != j.dependencies[i] {
			return false
		}
	}
	if len(a.Values) != len(j.values) {
		return false
	}
	for i := range j.values {
		if a.Values[i] != j.values[i] {
			return false
		}
	}
	if len(j.effectiveValues) == 0 || len(a.EffectiveValues) != len(j.effectiveValues) {
		return false
	}
	for i := range j.effectiveValues {
		if j.effectiveValues[i] != a.EffectiveValues[i] {
			return false
		}
	}
	if a.InputsDigest != inputsDigestHex(j.effectiveValues, j.seed) {
		return false
	}
	if a.ResultDigest != resultDigestHex(j.effectiveValues, j.seed, a.Sum, a.SumOfSquares) {
		return false
	}
	if a.Log != buildLog(j.effectiveValues, j.seed, a.Sum, a.SumOfSquares) {
		return false
	}
	if a.Checksum != checksumHex(j.effectiveValues, j.seed, a.Sum, a.SumOfSquares, a.Log, a.ResultDigest) {
		return false
	}
	return true
}

// archiveResultsValid 按既有计算规则对归档保存的实际输入（effectiveValues，
// 已含按顺序追加的上游总和，不再追加）复算总和与平方和，归档中的数值结果
// 必须与复算结果完全一致。完整性检查只验证日志、摘要与校验值和所写数字
// 自洽，无法识别“数字本身写错但全套字段一致”的记录，因此这里以计算规则
// 为准。实际输入按规则会造成 int64 溢出时 ok 为 false，任何数值结果
// （包括回绕后的值）都不能作为有效成功结果保留。
//
// 调用前须已通过 archiveIntact（保证 archive 非空且有效输入非空）。
func archiveResultsValid(j *storedJob) bool {
	a := j.archive
	sum, sumSquares, _, ok := computeResult(j.effectiveValues, j.seed, nil)
	if !ok {
		return false
	}
	return a.Sum == sum && a.SumOfSquares == sumSquares
}

// effectiveInputsCorrespond 校验成功记录中实际参与计算的输入与原始提交
// 参数、直接依赖数量之间的对应关系：
//   - 原始序列必须非空；
//   - 原始序列逐项、按原次序完整出现在实际输入的开头——负数、零与重复
//     整数一律按位置判断，不比较总和或平方和，也不忽略次序；
//   - 实际输入总长度必须恰好等于原始序列长度加直接依赖数量。无依赖时两份
//     序列必须完全相同；有单个或多个依赖时，原始部分之后只能保留每个直接
//     上游各占一个位置的追加部分，不能借依赖之名替换、截短原始部分，也不
//     能多出未记录的输入。
//
// 该规则防止把另一份输入的自洽结果挂在原始参数下：例如原始参数为 [2,-3]
// 而实际输入被换成 [-3,2] 时，总和与平方和不变，摘要、日志与校验值又只
// 绑定实际输入，全套字段仍会自洽，只有按位置比对原始参数并核对追加数量
// 才能识别。旧的单依赖记录恢复后依赖列表恰含一个作业号，按同一含义判断。
//
// 调用前须已通过 archiveIntact（保证 archive 非空、实际输入非空且归档内
// 副本与顶层一致）。
func effectiveInputsCorrespond(j *storedJob) bool {
	n := len(j.values)
	if n == 0 {
		return false
	}
	if len(j.effectiveValues) != n+len(j.dependencies) {
		return false
	}
	for i := 0; i < n; i++ {
		if j.effectiveValues[i] != j.values[i] {
			return false
		}
	}
	return true
}
