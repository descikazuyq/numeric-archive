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

	// fileName 是该作业记录在归档目录中的正式文件名（不含目录）。
	// 恢复出的记录保留读入时的文件名——读取规则接受任何 job- 开头、
	// .json 结尾的合法记录，作业号由记录内容而非文件名识别，因此状态
	// 更新必须写回同一份文件，不能按默认命名另写一份而同号旧记录
	// 原样保留（下次打开会因重复作业号拒绝打开）。新提交的作业此字段
	// 为空，落盘时使用默认命名 jobFileName(id)。
	fileName string
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

// recordFileName 返回作业记录落盘使用的正式文件名：恢复出的记录写回其
// 原有文件（文件名不必是默认命名），新提交的记录使用默认命名。
func recordFileName(j *storedJob) string {
	if j.fileName != "" {
		return j.fileName
	}
	return jobFileName(j.id)
}

func (s *Store) jobPath(j *storedJob) string {
	return filepath.Join(s.dir, recordFileName(j))
}

func encodeRecord(j *storedJob) ([]byte, error) {
	hasDependency, dependencyID := legacyDependencyFields(j.dependencies)
	r := jobRecord{
		Version: recordVersion,

		ID:            j.id,
		Submitter:     j.submitter,
		RequestID:     j.requestID,
		IdentityRaw:   rawIdentity(j.submitter, j.requestID),
		Seed:          j.seed,
		Values:        append([]int64(nil), j.values...),
		HasDependency: hasDependency,
		DependencyID:  dependencyID,
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

// persist 在已持锁的情况下原子写一条作业记录。恢复出的记录写回其原有
// 文件：原文件无法替换时返回实际保存错误，不转到默认命名的另一份文件。
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
	return writeFileAtomic(s.dir, recordFileName(j), data, 0o600)
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
//   - 两份或更多记录文件顶层保存相同作业号时（即使文件名不同或内容完全
//     相同），本次打开直接失败并返回说明该作业号与冲突文件名的非空错误：
//     一个稳定作业号只能对应一份已保存作业。该检查在所有记录都可正常解析、
//     版本及状态合法之后、任何恢复改写之前完成，因此冲突时不会启动任何
//     计算，也不会改判或补写目录内的任何原有记录（中断的运行记录、待复查
//     的成功记录、排队记录等全部保持原样）；不按时间、状态、排列或完整度
//     挑选一份，也不自动换号、合并或删除，调用方处理冲突后再次打开才按
//     既有规则恢复；
//   - 两份作业号不同的记录保存了相同提交人与同一个非空请求号时（提交功能
//     保证不会出现的形态），同样在恢复改写之前直接失败，返回说明提交人、
//     请求号、两个冲突作业号与冲突文件名的非空错误；标识按恢复后的完整
//     字节比较（含 U+0000 与非法 UTF-8），空请求号不启用幂等因此豁免，
//     不同提交人使用相同请求号也合法。冲突时同样不启动计算、不改写任何
//     原有记录，不因记录状态（含已失败/取消）、提交时间或文件排列挑选一条；
//   - 上次关闭时仍为 running 的作业标记为失败，原因是计算被中断；
//   - 成功记录缺少归档、校验值对不上，依赖列表重复引用同一直接上游作业号，
//     或直接上游作业号不小于本作业号（引用自己或后来作业，违反提交时的
//     先后关系规则），实际输入与原始参数或直接依赖数量不对应，或归档数值
//     结果与有效输入按计算规则应得的结果不符时，fail-closed 地标记为失败，
//     维持“成功必有完整归档”的不变量；
//   - 有依赖的成功记录还要求每个直接上游都存在、恢复后仍是带完整归档的
//     成功状态，且实际输入的追加部分逐项等于这些上游按保存的依赖顺序
//     排列的总和；上游不可用的同样改判为失败，其失败原因指出直接上游、
//     BlockerID 沿链条保留最初的根因（与运行中依赖失败同一规则）；
//     仅追加输入对不上而上游全部有效的属于本作业自身归档有误，BlockerID
//     保持 0。排队记录的依赖列表重复引用同一作业号，或引用作业号不小于
//     自己的作业（自己/后来作业），同样在恢复时直接判失败（BlockerID 为
//     0），不再等待或开始计算；排队记录的原始整数序列为空（缺省、null 或
//     空数组）也直接判失败（BlockerID 为 0）——提交时即拒绝空序列，恢复时
//     不能凭已有实际输入或成功上游的总和补出原始参数后继续计算。仍在排队
//     的其他作业在同一趟升序扫描中按既有依赖规则级联失败。
func loadDir(dir string) ([]*storedJob, uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}
	// 第一阶段：只读解析全部记录文件，不进行任何恢复改写。这样重复作业号与
	// 请求号唯一性检查可以在第一条记录被改判失败或补写结果之前作出判断：一个
	// 稳定作业号只能对应一份已保存作业，同一提交人的非空请求号也只能对应一个
	// 作业；两份顶层作业号相同的记录（文件名不同、内容完全相同也不例外），或
	// 两份不同作业号却保存相同提交人+非空请求号的记录，必须让本次打开整体
	// 失败，而不是按文件排列、时间或状态挑一份继续，也不能先按正常恢复规则把
	// 其他作业改判失败/补写结果再报错——冲突时本次打开不启动任何计算，也不
	// 改写目录内任何原有记录。
	type parsedRecord struct {
		name string
		job  *storedJob
	}
	var parsed []parsedRecord
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
		j, err := parseJobRecord(dir, name)
		if err != nil {
			return nil, 0, err
		}
		parsed = append(parsed, parsedRecord{name: name, job: j})
		if j.id > maxID {
			maxID = j.id
		}
	}

	// 重复作业号检查：所有记录都可正常解析、版本与状态合法之后，在任何恢复
	// 改写与第二趟依赖失效处理之前执行。检查按目录列举顺序进行，但结果不
	// 依赖文件排列——只要同一作业号出现至少两次即构成冲突；错误中给出该
	// 作业号以及全部冲突记录的文件名（至少两份），便于调用方定位。不按
	// 提交时间、状态、排列或哪份带完整结果挑选，也不自动换号、合并参数或
	// 删除冲突文件；调用方处理完冲突后再次打开才按既有规则恢复。
	// 紧随其后的请求号唯一性检查遵守同一条“失败即整次打开失败、不挑选、不
	// 改写”的纪律，只是冲突键从作业号换成（提交人, 非空请求号）。
	seen := make(map[uint64]*parsedRecord, len(parsed))
	for i := range parsed {
		rec := &parsed[i]
		if prev, dup := seen[rec.job.id]; dup {
			return nil, 0, duplicateJobIDError(rec.job.id, prev.name, rec.name)
		}
		seen[rec.job.id] = rec
	}

	// 请求号唯一性检查：所有记录都可正常解析、版本与状态合法、且每个作业号只
	// 对应一份记录之后，在任何恢复改写之前执行。提交功能已保证“同一提交人的
	// 非空请求号只能对应一个作业”，读取保存记录时必须成立同样的唯一性：两份
	// 记录作业号不同却保存了相同提交人和同一个非空请求号（按恢复后的完整字节
	// 逐字节比较，含 U+0000 与非法 UTF-8，后者由 identity_raw 兜底恢复）时，
	// 本次打开整体失败，而不是让索引静默指向作业号较大的一条。空请求号不启用
	// 幂等，同一提交人可以保存多条空请求号作业；不同提交人使用相同请求号也
	// 合法。即使两份记录的整数序列、种子、依赖与计算结果完全相同也不能合并
	// 成一次请求；其中一份已失败或取消也不释放请求号——检查只看恢复出的标识
	// 与作业号，不看状态、提交时间或文件排列，也不挑一条继续使用。
	type idemSeenEntry struct {
		id   uint64
		name string
	}
	idemSeen := make(map[idemIdentity]*idemSeenEntry, len(parsed))
	for i := range parsed {
		rec := &parsed[i]
		if rec.job.requestID == "" {
			continue
		}
		key := idemIdentity{rec.job.submitter, rec.job.requestID}
		if prev, dup := idemSeen[key]; dup {
			return nil, 0, duplicateRequestIDError(rec.job.submitter, rec.job.requestID,
				prev.id, prev.name, rec.job.id, rec.name)
		}
		idemSeen[key] = &idemSeenEntry{id: rec.job.id, name: rec.name}
	}

	jobs := make([]*storedJob, 0, len(parsed))
	for _, rec := range parsed {
		jobs = append(jobs, rec.job)
	}

	// 第二阶段：至此目录中每个作业号只对应一份记录、且每个（提交人, 非空
	// 请求号）也只对应一个作业号，下面的既有恢复规则
	// （中断改判、成功归档复核、排队记录自身有误与依赖失效级联）才允许
	// 改写记录。第一趟状态修正只依赖单条记录自身，直接逐个执行即可。
	for _, j := range jobs {
		if j.status == StatusRunning {
			failRestoredJob(dir, j, "计算被中断：归档上次关闭时作业仍在运行", 0)
		}
		if j.status == StatusSucceeded {
			// 除完整性外，还必须满足三条独立的对应/复算规则：
			//  0. 依赖列表自身合法，按以下顺序检查：
			//     a. 同一直接上游作业号不能出现两次（相邻或隔着其他上游都算）。
			//        这与提交时 validateDependencyShapeLocked 的唯一性要求一致；
			//        旧版本或被改动的记录可能保存了重复引用，此时即使追加值、
			//        总和、平方和、摘要、日志与校验值全部自洽，也不能当作合法
			//        成功归档（重复引用会把同一上游总和追加两次）。
			//     b. 每个直接上游作业号都必须严格小于本作业号。正常提交只能引用
			//        已经存在的作业，作业号又按接受先后递增，所以引用自己或引用
			//        后来作业都不可能通过提交校验；旧版本或被改动的记录可能保存
			//        这种依赖，即使被引用作业确实存在且成功、本作业的追加值、总和、
			//        平方和、摘要、日志与校验值全部自洽，也不能接受。已因重复
			//        依赖判失败的记录保留重复失败说明，不再改写。
			//     两项都属于本作业记录自身有误，在任何上游可用性核对之前判定，
			//     BlockerID 为 0，不把被引用的有效上游判成失败。
			//  1. 实际输入与原始参数、直接依赖数量逐项对应——原始序列非空且
			//     按原次序完整出现在实际输入开头，总长度恰为原始长度加上直接
			//     依赖数；摘要、日志、校验值只与“实际输入”绑定，换成另一份
			//     序列（如 [-3,2] 冒充 [2,-3]）仍能全套自洽，只有按位置比对
			//     原始参数与依赖数量才能识别。
			//  2. 归档中的数值结果必须与实际输入按既有计算规则应得的结果一致：
			//     日志、摘要与校验值都可能与错误数字自洽，只有按保存的实际
			//     输入复算才能识别。
			// 任一不符合都按损坏归档处理。第四条规则——追加部分逐项等于各
			// 直接上游恢复后的总和——需要全部记录恢复完毕才能核对，
			// 在排序后的第二趟扫描中执行。
			var invalidReason string
			if dupID, dup := duplicateDependency(j); dup {
				invalidReason = duplicateDependencyReason(dupID)
			} else if badID, bad := firstIllegalOrderingDependency(j); bad {
				invalidReason = illegalOrderingDependencyReason(badID, j.id)
			} else if !archiveIntact(j) {
				invalidReason = "归档不完整或校验值不一致，成功结果不可用"
			} else if !effectiveInputsCorrespond(j) {
				invalidReason = "实际输入与原始参数或依赖数量不符，成功结果不可用"
			} else if !archiveResultsValid(j) {
				invalidReason = "归档数值结果与有效输入按计算规则应得的结果不符，成功结果不可用"
			}
			if invalidReason != "" {
				// 归档自身校验不通过：属于本作业自身失败，不携带阻断根因。
				failRestoredJob(dir, j, invalidReason, 0)
			}
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
	// 两类作业遵守同一项归因规则（与正常运行中的依赖失败一致）：失败原因
	// 指出当前作业的直接上游，BlockerID 沿链条保留最初使结果不可用的根因；
	// 只有本作业自身归档有误（上游全部有效、仅保存的追加输入不符）时
	// BlockerID 才保持 0。
	//   - 带依赖的成功记录：追加部分与上游恢复后的结果逐项核对；
	//   - 排队记录：先检查记录自身是否合法——依赖列表重复引用同一作业号、
	//     引用作业号不小于自己的作业（自己或后来作业），或原始整数序列为空，
	//     都属本作业自身有误，BlockerID 为 0，立即失败（重复检查最前，空序列
	//     检查最后）；再按保存的依赖顺序选取最靠前的不可用上游（已失败/取消，
	//     或记录缺失），立即级联失败，不再等待。
	//
	// 依赖列表自身不合法属于记录自身问题，先于一切上游可用性判断：即使被引用
	// 的上游仍在排队或确实存在且成功，不合法记录也要立即失败；而等待该记录
	// 结果的下游则按既有规则把它当作失败上游级联，根因就是这份记录自身。
	byID := make(map[uint64]*storedJob, len(jobs))
	for _, j := range jobs {
		byID[j.id] = j
	}
	for _, j := range jobs {
		if j.status == StatusQueued {
			// 排队记录自身有误的三项检查先于一切上游可用性判断，对有无依赖的
			// 记录同样适用（无依赖时前两项自然不命中）：
			//  1. 依赖列表重复引用同一作业号：不能开始计算，也不能把同一上游
			//     总和追加两次后继续。BlockerID 保持 0，不把被重复引用且原本
			//     有效的上游改成失败。
			//  2. 依赖先后关系不合法：直接上游作业号必须严格小于本作业号，
			//     引用自己或后来作业即使被引用者存在且成功也不能接受，不能开始
			//     计算或继续等待。排在重复检查之后，已因重复依赖判失败的记录
			//     保留现有说明。
			//  3. 原始整数序列为空（缺省、null 或空数组）：提交时即拒绝空序列，
			//     恢复时遵守同一项输入要求——不能开始运行，也不能凭已有实际输入
			//     或成功上游的总和补出原始参数。BlockerID 保持 0。空序列指没有
			//     任何原始整数；[0]、[0,0] 等非空序列仍是合法输入，不按总和
			//     是否为 0 判断。
			if dupID, dup := duplicateDependency(j); dup {
				failRestoredJob(dir, j, duplicateDependencyReason(dupID), 0)
				continue
			}
			if badID, bad := firstIllegalOrderingDependency(j); bad {
				failRestoredJob(dir, j, illegalOrderingDependencyReason(badID, j.id), 0)
				continue
			}
			if len(j.values) == 0 {
				failRestoredJob(dir, j, emptyOriginalValuesReason(), 0)
				continue
			}
		}
		if len(j.dependencies) == 0 {
			continue
		}
		switch j.status {
		case StatusSucceeded:
			// 第一趟：按保存的依赖顺序找最靠前的不可用上游（记录缺失或恢复后
			// 未保持带完整归档的成功状态）。
			directID, dep, unavailable := pickBlockingUpstream(j, byID, restoredArchiveUpstreamBlocked)
			if unavailable {
				// 因直接上游结果失效而失败：根因沿该上游已有的阻断信息保留，
				// 直接上游记录缺失时以缺失的作业号本身作为根因。
				root := blockerRoot(dep, directID)
				failRestoredJob(dir, j, blockedFailureReason(restoredArchiveHead(directID), dep, directID, root), root)
				continue
			}
			// 第二趟：直接上游全部存在且有效，只是本作业保存的追加输入与上游
			// 总和按位置对不上——属于本作业自身归档有误，不把有效上游判成
			// 失败或归因给它，BlockerID 保持 0。
			if _, detail, mismatch := firstAppendedMismatch(j, byID); mismatch {
				failRestoredJob(dir, j, detail, 0)
			}
		case StatusQueued:
			// 记录自身的三项检查已在上面完成；这里按保存的依赖顺序选取最靠前
			// 的不可用上游，其他上游作业号更小或失败原因不同都不能改变这个选择。
			directID, direct, blocked := pickBlockingUpstream(j, byID, restoredQueuedUpstreamBlocked)
			if !blocked {
				continue // 上游仍在排队等情况：继续等待，不挡住其他作业。
			}
			root := blockerRoot(direct, directID)
			if direct == nil {
				// 直接上游的记录在归档目录中缺失，其结果不可能再变为可用：
				// 以缺失的作业号作为根因，并明确说明结果无法使用。
				failRestoredJob(dir, j, blockedFailureReason(restoredMissingHead(directID), nil, directID, root), root)
				continue
			}
			failRestoredJob(dir, j, blockedFailureReason(runtimeBlockedHead(direct), direct, directID, root), root)
		}
	}
	return jobs, maxID, nil
}

// parseJobRecord 只读解析单个作业记录文件并构造内存中的恢复作业，不执行任何
// 依赖其他记录的核对，也绝不写目录。原有的打开错误在此阶段继续有效并保持
// 原有信息：JSON 无法解析（记录损坏）、版本不受支持、标识兜底字节损坏或
// 状态非法都直接返回非空错误——这类记录不会因为同目录里恰好存在重复作业号
// 而改变错误类型。
//
// running 改判与成功归档复核等会改写记录的恢复步骤不在此处进行：调用方先
// 解析完全部记录并确认作业号无重复、同一提交人的非空请求号也没有跨不同
// 作业号重复之后，才允许进入恢复改写阶段。
//
// 恢复出的作业记住读入时的文件名（name）：文件名不必是默认命名，后续任何
// 状态更新都写回这同一份正式文件，而不是按作业号另写默认命名的记录。
func parseJobRecord(dir, name string) (*storedJob, error) {
	path := filepath.Join(dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r jobRecord
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("numeric: 记录 %s 损坏: %w", name, err)
	}
	if r.Version != recordVersion {
		return nil, fmt.Errorf("numeric: 记录 %s 版本不受支持: %s", name, r.Version)
	}
	// 提交人、请求号按完整字节恢复：含非法 UTF-8 字节时以
	// identity_raw 兜底字段为准，普通文本（含 U+0000）与旧归档原样使用。
	submitter, requestID, err := resolveIdentity(r.Submitter, r.RequestID, r.IdentityRaw)
	if err != nil {
		return nil, fmt.Errorf("numeric: 记录 %s 标识损坏: %w", name, err)
	}
	// 成功归档内嵌同一份标识，同样按其自身兜底字段恢复，保证顶层记录
	// 与归档中的参数逐字节一致。
	if r.Archive != nil {
		aSub, aReq, aErr := resolveIdentity(r.Archive.Submitter, r.Archive.RequestID, r.Archive.IdentityRaw)
		if aErr != nil {
			return nil, fmt.Errorf("numeric: 记录 %s 归档标识损坏: %w", name, aErr)
		}
		r.Archive.Submitter = aSub
		r.Archive.RequestID = aReq
	}
	switch r.Status {
	case StatusQueued, StatusRunning, StatusSucceeded, StatusFailed, StatusCanceled:
	default:
		return nil, fmt.Errorf("numeric: 记录 %s 状态非法: %q", name, r.Status)
	}
	// 新格式记录直接给出有序依赖列表；旧格式记录只有单依赖字段，
	// 恢复为只含一个作业号的列表（语义与提交时一致）。两类字段同时
	// 出现时以非空列表为实际依赖，与提交、归档核对共用同一换算。
	deps := normalizeDependencies(r.HasDependency, r.DependencyID, r.Dependencies)
	return &storedJob{
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

		// 记录读入时的正式文件名，后续状态更新写回同一份文件。
		fileName: name,
	}, nil
}

// duplicateJobIDError 构造重复作业号冲突的打开错误：明确说明存在重复作业号，
// 包含该作业号以及至少两份冲突记录的文件名。更多冲突文件不逐个展开，但
// first 与 second 已足以让调用方定位冲突；列出哪些文件不取决于提交时间、
// 状态是否成功、文件排列或哪份带有完整结果。
func duplicateJobIDError(id uint64, first, second string) error {
	return fmt.Errorf("numeric: 归档目录存在重复作业号 %d：一个稳定作业号只能对应一份已保存作业，"+
		"但记录文件 %s 与 %s 顶层保存了相同作业号；拒绝打开且不改动任何记录，请处理冲突文件后重试",
		id, first, second)
}

// recordFileNameOccupiedError 构造提交时的默认文件名占用错误：拟分配作业号
// newID 的新作业按默认命名应写入 fileName，但该文件已是另一份已恢复作业
// ownerID 的正式文件——恢复规则接受 job- 开头、.json 结尾的合法记录，作业
// 号由记录内容识别，文件名中的数字不能替代真实作业号。若按默认命名落盘，
// 原子替换会覆盖 ownerID 的已保存记录，因此拒绝提交。错误明确给出拟分配的
// 新作业号、被占用的文件名与文件的原归属作业号，让调用方能够定位冲突；
// 旧记录处于失败或取消状态也同样占用该文件名。
func recordFileNameOccupiedError(newID uint64, fileName string, ownerID uint64) error {
	return fmt.Errorf("%w：拟分配的新作业号为 %d，其默认记录文件 %s 已属于另一作业 %d；"+
		"为避免覆盖作业 %d 的已保存记录，本次提交被拒绝且不占用作业号，"+
		"请处理（重命名）占用该文件名的记录后重试",
		ErrRecordFileNameOccupied, newID, fileName, ownerID, ownerID)
}

// jobIDsExhaustedError 构造作业号耗尽时的提交拒绝错误：已接受作业的最大
// 作业号已达到 uint64 上限 math.MaxUint64，下一个编号会回绕为 0。作业号
// 按接受先后严格递增且 0 不能成为新作业编号，因此不能把回绕后的 0 或任何
// 没有记录的小编号当作可用编号；这次拒绝不产生记录、不进入计算队列、不
// 登记请求号。已有作业的查询、列举与处理以及已有请求号的幂等语义不受影响。
func jobIDsExhaustedError() error {
	return fmt.Errorf("%w：已接受作业的最大作业号已达到 uint64 上限 %d，"+
		"没有剩余编号可分配；编号不会回绕，较小的未使用编号（含作业号 0）也不会回头补号，"+
		"本次提交不产生记录",
		ErrJobIDsExhausted, ^uint64(0))
}

// duplicateRequestIDError 构造请求号重复归属冲突的打开错误：两份不同作业号的
// 记录保存了相同提交人与同一个非空请求号——这是提交功能本就保证不会出现的
// 形态，读取保存记录时同样必须拒绝，而不能让恢复出的幂等索引静默指向作业号
// 较大的一条。错误指出提交人、请求号以及至少两个冲突作业号与各自的记录文件名。
//
// 标识按恢复后的完整字节给出：%q 会把 U+0000 转义成 \x00、把非法 UTF-8
// 字节转义成 \xff 这类字节转义（不会像直接显示那样都塌缩成替换字符 U+FFFD），
// 再附上完整字节的十六进制，使不可见或非法字节也能唯一定位——两个显示相同
// 但字节不同的标识不会给出无法区分的冲突信息。
func duplicateRequestIDError(submitter, requestID string, firstID uint64, firstName string, secondID uint64, secondName string) error {
	return fmt.Errorf("numeric: 归档目录存在重复的幂等请求号：提交人 %q（完整字节 %x）的非空请求号 %q（完整字节 %x）"+
		"重复归属于不同作业 %d（记录文件 %s）与 %d（记录文件 %s）；同一提交人的非空请求号只能对应一个作业，"+
		"拒绝打开且不改动任何记录，请处理冲突文件后重试",
		submitter, []byte(submitter), requestID, []byte(requestID),
		firstID, firstName, secondID, secondName)
}

// failRestoredJob 收拢重新打开归档时把恢复出的作业改判为失败的统一处理，
// 供六类恢复失败共用（调用顺序即各类判定的既有先后）：
//   - 上次关闭时仍在运行的作业：计算被中断（第一趟记录扫描）；
//   - 原标为成功、但归档完整性、依赖列表唯一性或先后关系、实际输入对应
//     关系或数值结果不符规则的作业：本作业自身归档校验失败（第一趟记录
//     扫描，BlockerID 恒 0；唯一性检查先于先后关系检查）；
//   - 排队记录的依赖列表重复引用同一作业号：本作业记录自身有误（第二趟升序
//     扫描，BlockerID 恒 0，先于上游可用性判断）；
//   - 排队记录引用作业号不小于自己的作业（自己/后来作业）：本作业记录自身
//     有误（第二趟升序扫描，BlockerID 恒 0，在重复检查之后、空序列检查与
//     上游可用性判断之前）；
//   - 排队记录的原始整数序列为空（缺省、null 或空数组）：本作业记录自身
//     有误（第二趟升序扫描，BlockerID 恒 0，在依赖列表自身合法性检查之后、
//     上游可用性判断之前）；
//   - 被不可用直接上游阻断的作业：成功记录复核与排队记录复查（第二趟升序扫描），
//     BlockerID 由调用方按 blocker.go 的归因规则给出。
//
// 处理内容与各处原先内联的步骤完全一致：置失败状态、失败原因与阻断根因；
// 已经有完成时间的记录保留该时间，只有没有完成时间的才补上当前时间；
// 清除成功归档与实际参与计算的输入，使按作业号读取与按提交人列举都不再返回
// 成功归档或实际输入；作业号、提交人、请求号、原始整数次序、种子、依赖次序
// 与已有开始时间均不在此改动。归档可正常写入时，变化经该作业原有记录文件的
// 一次原子替换写回磁盘（恢复出的记录保持读入时的文件名，不按默认命名另写
// 一份而同号旧记录原样保留），而不只停留在内存查询结果中；写入失败维持
// 既有尽力而为语义（不因恢复阶段的一次落盘失败让整个归档无法打开）。
func failRestoredJob(dir string, j *storedJob, reason string, blocker uint64) {
	j.status = StatusFailed
	j.failureReason = reason
	j.blockerID = blocker
	if j.finishedAt.IsZero() {
		j.finishedAt = time.Now().UTC()
	}
	j.effectiveValues = nil
	j.archive = nil
	if data, err := encodeRecord(j); err == nil {
		_ = writeFileAtomic(dir, recordFileName(j), data, 0o600)
	}
}

// archiveIntact 复算成功归档的全部确定性字段，任何不一致都视为不可用。
func archiveIntact(j *storedJob) bool {
	a := j.archive
	if a == nil {
		return false
	}
	// 归档中的原始参数必须与记录顶层字段一致。
	wantHasDependency, wantDependencyID := legacyDependencyFields(j.dependencies)
	if a.JobID != j.id || a.Submitter != j.submitter || a.RequestID != j.requestID ||
		a.Seed != j.seed || a.HasDependency != wantHasDependency {
		return false
	}
	if a.HasDependency && a.DependencyID != wantDependencyID {
		return false
	}
	// 依赖列表（内容与次序）必须一致。旧格式归档没有 dependencies 字段，
	// 由单依赖字段推导后再比较，与记录恢复、提交内容比较共用同一换算。
	aDeps := normalizeDependencies(a.HasDependency, a.DependencyID, a.Dependencies)
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
