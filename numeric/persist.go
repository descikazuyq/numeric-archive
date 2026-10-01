package numeric

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// 事件类型。事件以 JSON 行追加到 events.log，重放即可恢复全部状态。
const (
	evSubmitted = "submitted"
	evStarted   = "started"
	evCancelled = "cancelled"
	evFinished  = "finished"
)

// event 是追加写入的一条不可变事件。
type event struct {
	Type   string     `json:"type"`
	Job    *jobRecord `json:"job,omitempty"`
	ID     int64      `json:"id,omitempty"`
	Status string     `json:"status,omitempty"`
	Reason string     `json:"reason,omitempty"`
	// Archive 仅成功结束事件携带；成功状态与完整归档在同一条事件中原子可见。
	Archive *Archive `json:"archive,omitempty"`
}

// jobRecord 是作业的内部状态，随事件日志持久化。
type jobRecord struct {
	ID            int64     `json:"id"`
	Submitter     string    `json:"submitter"`
	ReqNo         string    `json:"req_no"`
	Sequence      []int64   `json:"sequence"`
	Seed          int64     `json:"seed"`
	DependencyID  *int64    `json:"dependency_id,omitempty"`
	SubmittedAt   time.Time `json:"submitted_at"`
	Status        JobStatus `json:"status"`
	Archive       *Archive  `json:"archive,omitempty"`
	FailureReason string    `json:"failure_reason,omitempty"`
}

// idemKey 构造幂等键：提交人 + 请求号。
func idemKey(submitter, reqNo string) string {
	return submitter + "\x00" + reqNo
}

// appendLocked 追加一条事件。调用方必须持有 s.mu。
//
// 每条事件是一次 O_APPEND 单行写入；成功结束事件同时包含状态与归档，
// 因此崩溃恢复时二者要么都可见，要么都不可见，不会出现成功却无结果的记录。
func (s *Store) appendLocked(e event) {
	b, err := json.Marshal(e)
	if err != nil { // 记录只含基础类型与时间，不会失败
		panic(err)
	}
	if _, err := fmt.Fprintf(s.logFile, "%s\n", b); err != nil {
		panic(err)
	}
}

// replay 从重放事件日志恢复内存状态。
func (s *Store) replay() error {
	path := filepath.Join(s.dir, "events.log")
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// 单行可能较长（大序列 + 日志），放宽缓冲。
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var e event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			return fmt.Errorf("numeric: 事件日志第 %d 行解析失败: %w", s.replayLines+1, err)
		}
		s.applyEvent(e)
		s.replayLines++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

// applyEvent 把一条事件应用到内存状态。
func (s *Store) applyEvent(e event) {
	switch e.Type {
	case evSubmitted:
		rec := e.Job
		if rec == nil {
			return
		}
		s.jobs[rec.ID] = rec
		s.idem[idemKey(rec.Submitter, rec.ReqNo)] = rec.ID
		if rec.ID >= s.nextID {
			s.nextID = rec.ID + 1
		}
	case evStarted:
		if rec, ok := s.jobs[e.ID]; ok && rec.Status == StatusQueued {
			rec.Status = StatusRunning
		}
	case evCancelled:
		if rec, ok := s.jobs[e.ID]; ok {
			rec.Status = StatusCancelled
		}
	case evFinished:
		rec, ok := s.jobs[e.ID]
		if !ok {
			return
		}
		rec.Status = JobStatus(e.Status)
		rec.FailureReason = e.Reason
		if e.Status == string(StatusSucceeded) {
			rec.Archive = e.Archive
		}
	}
}

// sortedIDs 返回当前全部作业号（升序，即提交先后顺序）。
func (s *Store) sortedIDs() []int64 {
	ids := make([]int64, 0, len(s.jobs))
	for id := range s.jobs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
