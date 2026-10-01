package numeric

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Store 是本地数值作业与结果归档。
//
// 通过 Open 打开一个目录（不存在则创建），目录下 events.log 是唯一的
// 持久化事实来源。Store 内建单作业调度器：一次只运行一个作业，其余
// 可运行作业按提交顺序等待；等待依赖的作业不会挡住后面可运行的作业。
type Store struct {
	mu     sync.Mutex
	dir    string
	jobs   map[int64]*jobRecord
	idem   map[string]int64
	nextID int64

	logFile *os.File

	wake chan struct{}
	stop chan struct{}
	done chan struct{}

	// 测试钩子：paused 时调度器不领取新作业；computeGate 非空时，
	// 作业置为运行状态后在锁外等待该通道，用于观测运行态。
	paused      bool
	resume      chan struct{}
	computeGate chan struct{}

	replayLines int
	closed      bool
}

// Open 打开（或创建）目录并恢复作业状态。
//
// 重新打开后：已确认的提交、取消和完成结果不丢失，幂等请求号继续生效；
// 排队作业继续等待或处理；上次关闭时仍在运行的作业标为失败（计算被中断），
// 并阻止其下游继续计算。
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		dir:    dir,
		jobs:   map[int64]*jobRecord{},
		idem:   map[string]int64{},
		nextID: 1,
		wake:   make(chan struct{}, 1),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		resume: make(chan struct{}),
	}
	f, err := os.OpenFile(filepath.Join(dir, "events.log"),
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.logFile = f
	if err := s.replay(); err != nil {
		f.Close()
		return nil, err
	}

	s.mu.Lock()
	// 恢复：上次关闭时仍在运行的作业标为失败，原因说明计算被中断。
	var interrupted []int64
	for id, rec := range s.jobs {
		if rec.Status == StatusRunning {
			interrupted = append(interrupted, id)
		}
	}
	sort.Slice(interrupted, func(i, j int) bool { return interrupted[i] < interrupted[j] })
	for _, id := range interrupted {
		rec := s.jobs[id]
		rec.Status = StatusFailed
		rec.FailureReason = "计算被中断：存储关闭时作业仍在运行"
		s.appendLocked(event{
			Type: evFinished, ID: id,
			Status: string(StatusFailed), Reason: rec.FailureReason,
		})
	}
	// 中断作业可能还有排队的下游，一并级联失败。
	s.cascadeLocked()
	s.mu.Unlock()

	go s.schedulerLoop()
	return s, nil
}

// Close 停止调度器并落盘关闭。
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.stop)
	s.signalWakeLocked()
	f := s.logFile
	s.mu.Unlock()

	<-s.done
	return f.Close()
}

// signalWakeLocked 非阻塞地唤醒调度器。
func (s *Store) signalWakeLocked() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Submit 提交一个作业。
//
// 空序列拒绝提交，不产生记录。每个作业可指定一个已存在的作业作为依赖；
// 引用不存在的作业拒绝提交，不产生记录。
//
// 幂等请求号在同一提交人内有效：相同请求号且内容相同（整数次序、种子、
// 依赖均一致）时返回原作业及当前状态；内容冲突时返回 ErrConflict 并保留
// 原记录。并发重复提交只会创建一个作业。
func (s *Store) Submit(p SubmitParams) (*Job, error) {
	if len(p.Sequence) == 0 {
		return nil, ErrEmptySequence
	}
	seq := append([]int64(nil), p.Sequence...)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}

	key := idemKey(p.Submitter, p.ReqNo)
	if id, ok := s.idem[key]; ok {
		rec := s.jobs[id]
		if !sameContent(rec, p) {
			return nil, fmt.Errorf(
				"%w：提交人 %q 的请求号 %q 已被内容不同的作业 %d 使用",
				ErrConflict, p.Submitter, p.ReqNo, id)
		}
		return s.viewLocked(rec), nil
	}

	var depID *int64
	if p.DependencyID != nil {
		d, ok := s.jobs[*p.DependencyID]
		if !ok {
			return nil, fmt.Errorf("%w：%d", ErrDepNotFound, *p.DependencyID)
		}
		id := d.ID
		depID = &id
	}

	id := s.nextID
	s.nextID++
	rec := &jobRecord{
		ID:           id,
		Submitter:    p.Submitter,
		ReqNo:        p.ReqNo,
		Sequence:     seq,
		Seed:         p.Seed,
		DependencyID: depID,
		SubmittedAt:  time.Now(),
		Status:       StatusQueued,
	}
	s.jobs[id] = rec
	s.idem[key] = id
	s.appendLocked(event{Type: evSubmitted, Job: rec})
	s.signalWakeLocked()
	return s.viewLocked(rec), nil
}

// sameContent 判别幂等重提内容是否一致：整数次序、种子、依赖均需相同。
func sameContent(rec *jobRecord, p SubmitParams) bool {
	if rec.Seed != p.Seed {
		return false
	}
	if len(rec.Sequence) != len(p.Sequence) {
		return false
	}
	for i := range rec.Sequence {
		if rec.Sequence[i] != p.Sequence[i] {
			return false
		}
	}
	if (rec.DependencyID == nil) != (p.DependencyID == nil) {
		return false
	}
	if rec.DependencyID != nil && *rec.DependencyID != *p.DependencyID {
		return false
	}
	return true
}

// Cancel 取消排队或运行中的作业。
//
// 取消返回成功后，该作业及其依赖它的作业都不能再产生成功归档。重复取消
// 已取消的作业仍返回成功；取消已成功或已失败的作业返回 ErrCannotCancel，
// 并保持原结果不变。
func (s *Store) Cancel(id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	rec, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("%w：%d", ErrNotFound, id)
	}
	switch rec.Status {
	case StatusQueued, StatusRunning:
		rec.Status = StatusCancelled
		rec.FailureReason = ""
		s.appendLocked(event{Type: evCancelled, ID: id})
		// 级联失败所有排队中的下游。
		s.cascadeLocked()
		s.signalWakeLocked()
		return nil
	case StatusCancelled:
		return nil
	default:
		return fmt.Errorf("%w：作业 %d 当前状态为 %s", ErrCannotCancel, id, rec.Status)
	}
}

// Get 按作业号读取详情，返回深拷贝。
func (s *Store) Get(id int64) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	rec, ok := s.jobs[id]
	if !ok {
		return nil, fmt.Errorf("%w：%d", ErrNotFound, id)
	}
	return s.viewLocked(rec), nil
}

// List 按提交人列出提交时间范围内的记录，结果按提交先后（作业号升序）排列。
//
// 时间范围包含两个端点；起始时间晚于结束时间时拒绝查询。
func (s *Store) List(submitter string, from, to time.Time) ([]*Job, error) {
	if from.After(to) {
		return nil, ErrInvalidRange
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	var ids []int64
	for id, rec := range s.jobs {
		if rec.Submitter != submitter {
			continue
		}
		if rec.SubmittedAt.Before(from) || rec.SubmittedAt.After(to) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]*Job, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.viewLocked(s.jobs[id]))
	}
	return out, nil
}

// viewLocked 构造作业视图（深拷贝），并计算排队位置信息。
func (s *Store) viewLocked(rec *jobRecord) *Job {
	j := &Job{
		ID:            rec.ID,
		Submitter:     rec.Submitter,
		ReqNo:         rec.ReqNo,
		Sequence:      append([]int64(nil), rec.Sequence...),
		Seed:          rec.Seed,
		SubmittedAt:   rec.SubmittedAt,
		Status:        rec.Status,
		FailureReason: rec.FailureReason,
	}
	if rec.DependencyID != nil {
		d := *rec.DependencyID
		j.DependencyID = &d
	}
	if rec.Archive != nil {
		a := *rec.Archive
		a.Sequence = append([]int64(nil), rec.Archive.Sequence...)
		a.Inputs = append([]int64(nil), rec.Archive.Inputs...)
		a.ComputationLog = append([]string(nil), rec.Archive.ComputationLog...)
		if rec.Archive.DependencyID != nil {
			d := *rec.Archive.DependencyID
			a.DependencyID = &d
		}
		j.Archive = &a
	}
	if rec.Status == StatusQueued {
		if rec.DependencyID != nil {
			dep := s.jobs[*rec.DependencyID]
			if dep != nil && dep.Status != StatusSucceeded {
				j.QueueState = "waiting_dependency"
				d := *rec.DependencyID
				j.WaitingFor = &d
			}
		}
		if j.QueueState == "" {
			j.QueueState = "waiting_slot"
			// 可运行队列中排在前面的作业数（依赖已成功或无依赖）。
			pos := 1
			for id, q := range s.jobs {
				if id >= rec.ID || q.Status != StatusQueued {
					continue
				}
				if q.DependencyID != nil {
					d := s.jobs[*q.DependencyID]
					if d == nil || d.Status != StatusSucceeded {
						continue
					}
				}
				pos++
			}
			j.QueuePosition = pos
		}
	}
	return j
}

// cascadeLocked 把依赖已失败或被取消的排队作业级联置为失败。
//
// 依赖失败或被取消时，下游失败并指出阻断它的作业；级联反复进行，
// 直到没有新的受影响者，从而覆盖更下游的等待者。
func (s *Store) cascadeLocked() {
	for {
		changed := false
		for _, id := range s.sortedIDs() {
			rec := s.jobs[id]
			if rec.Status != StatusQueued || rec.DependencyID == nil {
				continue
			}
			dep := s.jobs[*rec.DependencyID]
			if dep == nil {
				continue
			}
			if dep.Status == StatusFailed || dep.Status == StatusCancelled {
				rec.Status = StatusFailed
				rec.FailureReason = fmt.Sprintf(
					"依赖作业 %d 未成功（%s），下游计算被阻断",
					dep.ID, dep.Status)
				s.appendLocked(event{
					Type: evFinished, ID: id,
					Status: string(StatusFailed), Reason: rec.FailureReason,
				})
				changed = true
			}
		}
		if !changed {
			return
		}
	}
}

// schedulerLoop 单作业调度器主循环。
func (s *Store) schedulerLoop() {
	defer close(s.done)
	for {
		// 没有可运行作业时等待唤醒信号。
		select {
		case <-s.stop:
			return
		case <-s.wake:
		}

		// 只要还有可运行的排队作业就持续处理，处理完一个立即重新扫描，
		// 不依赖新的唤醒信号。
	processLoop:
		for {
			s.mu.Lock()
			if s.paused {
				s.mu.Unlock()
				select {
				case <-s.resume:
				case <-s.stop:
					return
				}
				continue
			}

			s.cascadeLocked()

			// 按提交顺序领取第一个可运行的排队作业；等待依赖的作业被跳过，
			// 不会挡住后面可运行的作业。
			var rec *jobRecord
			for _, id := range s.sortedIDs() {
				j := s.jobs[id]
				if j.Status != StatusQueued {
					continue
				}
				if j.DependencyID != nil {
					dep := s.jobs[*j.DependencyID]
					if dep == nil || dep.Status != StatusSucceeded || dep.Archive == nil {
						continue
					}
				}
				rec = j
				break
			}
			if rec == nil {
				s.mu.Unlock()
				break processLoop // 回到外层循环等待唤醒
			}

			rec.Status = StatusRunning
			s.appendLocked(event{Type: evStarted, ID: rec.ID})

			// 组装实际输入：原序列 + 依赖成功归档的总和。
			inputs := append([]int64(nil), rec.Sequence...)
			if rec.DependencyID != nil {
				dep := s.jobs[*rec.DependencyID]
				if dep != nil && dep.Status == StatusSucceeded && dep.Archive != nil {
					inputs = append(inputs, dep.Archive.Sum)
				}
			}
			seed := rec.Seed
			gate := s.computeGate
			s.mu.Unlock()

			// 测试钩子：在运行态等待，便于观测。
			if gate != nil {
				select {
				case <-gate:
				case <-s.stop:
					return
				}
			}

			res, err := compute(inputs, seed)

			s.mu.Lock()
			// 计算期间可能被取消：取消已先行落盘，此处丢弃结果，绝不覆盖。
			if rec.Status != StatusRunning {
				s.mu.Unlock()
				continue
			}
			if err != nil {
				rec.Status = StatusFailed
				rec.FailureReason = err.Error()
				s.appendLocked(event{
					Type: evFinished, ID: rec.ID,
					Status: string(StatusFailed), Reason: err.Error(),
				})
				s.cascadeLocked()
				s.mu.Unlock()
				continue
			}
			arch := buildArchive(rec, res)
			rec.Status = StatusSucceeded
			rec.Archive = arch
			// 成功状态与完整归档在同一条事件中原子落盘。
			s.appendLocked(event{
				Type: evFinished, ID: rec.ID,
				Status: string(StatusSucceeded), Archive: arch,
			})
			s.mu.Unlock()
		}
	}
}
