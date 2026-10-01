package numeric

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

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

	ID            uint64  `json:"id"`
	Submitter     string  `json:"submitter"`
	RequestID     string  `json:"request_id"`
	Seed          int64   `json:"seed"`
	Values        []int64 `json:"values"`
	HasDependency bool    `json:"has_dependency"`
	DependencyID  uint64  `json:"dependency_id"`
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
//   - 成功记录缺少归档或校验值对不上时 fail-closed 地标记为失败，
//     维持“成功必有完整归档”的不变量。
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
			submitter:    r.Submitter,
			requestID:    r.RequestID,
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
		if j.status == StatusSucceeded && !archiveIntact(j) {
			j.status = StatusFailed
			j.failureReason = "归档不完整或校验值不一致，成功结果不可用"
			if j.finishedAt.IsZero() {
				j.finishedAt = time.Now().UTC()
			}
			j.effectiveValues = nil
			j.archive = nil
			if data, err := encodeRecord(j); err == nil {
				_ = writeFileAtomic(dir, jobFileName(j.id), data, 0o600)
			}
		}
		jobs = append(jobs, j)
		if j.id > maxID {
			maxID = j.id
		}
	}
	sort.Slice(jobs, func(a, b int) bool {
		if !jobs[a].queuedAt.Equal(jobs[b].queuedAt) {
			return jobs[a].queuedAt.Before(jobs[b].queuedAt)
		}
		return jobs[a].id < jobs[b].id
	})
	return jobs, maxID, nil
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
