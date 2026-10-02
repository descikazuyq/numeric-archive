package numeric

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Clock 用于注入时间来源，主要服务于确定性测试；生产代码使用墙钟。
type Clock interface {
	Now() time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now().UTC() }

// Option 配置 [Open] 的行为。
type Option func(*Store)

// WithClock 注入自定义时钟。
func WithClock(c Clock) Option {
	return func(s *Store) { s.now = c.Now }
}

// Store 是本地数值作业归档：负责提交、调度、取消、查询与持久化。
//
// 一次只运行一个作业；所有方法可被多个 goroutine 并发调用。
type Store struct {
	dir string

	mu     sync.Mutex
	jobs   []*storedJob // 严格按作业号（即提交先后）排列
	byID   map[uint64]*storedJob
	idem   map[string]uint64 // submitter\x00requestID -> 作业号
	nextID uint64
	closed bool

	now func() time.Time

	runningID uint64
	waitCh    chan struct{}
	stopCh    chan struct{}
	stopping  atomic.Bool
	wg        sync.WaitGroup

	// compute 仅用于包内确定性测试：若为空则使用真实的 [computeResult]。
	// 必须在任何 Submit 之前（Open 之后立即）设置；worker 只会在收到
	// Submit 的唤醒之后读取它，因此与设置之间存在 happens-before 关系。
	compute func(id uint64, inputs []int64, seed int64, canceled func() bool) (sum, sumSquares int64, reason string, ok bool)
}

// Open 打开（必要时创建）目录 dir 并恢复其中的全部作业记录。
//
// 恢复语义：
//   - 已确认的提交、取消与完成结果原样保留，幂等请求号继续生效；
//   - 上次关闭时仍在运行的作业标记为失败（计算被中断）并阻止其下游；
//   - 排队作业继续排队并由本地唯一 worker 按提交先后处理；
//   - 成功状态始终与完整归档同时可见。
func Open(dir string, opts ...Option) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	jobs, maxID, err := loadDir(dir)
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir:    dir,
		jobs:   jobs,
		byID:   make(map[uint64]*storedJob),
		idem:   make(map[string]uint64),
		nextID: maxID + 1,
		now:    wallClock{}.Now,
		waitCh: make(chan struct{}, 1),
		stopCh: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	for _, j := range jobs {
		s.byID[j.id] = j
		if j.requestID != "" {
			s.idem[s.idemKey(j.submitter, j.requestID)] = j.id
		}
	}
	// 重建失败/取消作业对排队下游的阻断。
	s.cascadeLocked()
	s.wg.Add(1)
	go s.worker()
	return s, nil
}

func (s *Store) idemKey(submitter, requestID string) string {
	return submitter + "\x00" + requestID
}

// Close 关闭归档。正在运行的作业会尽快中止并标记为失败（计算被中断），
// 排队作业原样保留在磁盘上，下次 [Open] 继续。
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	var running *storedJob
	if s.runningID != 0 {
		running = s.byID[s.runningID]
	}
	s.mu.Unlock()

	if s.stopping.CompareAndSwap(false, true) {
		close(s.stopCh)
	}
	// 中止正在运行的计算。
	if running != nil {
		running.canceled.Store(true)
	}
	s.notify()
	s.wg.Wait()
	return nil
}

// Submit 接受一次作业提交。
//
// 空序列返回 [ErrEmptySequence]；依赖列表含零或重复作业号、或同时使用单依赖
// 方式与非空列表返回 [ErrInvalidDependencyList]；引用不存在的依赖返回
// [ErrDependencyNotFound]；同一提交人复用请求号但内容变化（整数次序、种子或
// 依赖列表的内容/次序）返回 [ErrIdempotencyConflict]（同时返回原作业视图）
// ——这些拒绝都不产生记录，也不占用幂等请求号。
// 引用已失败或已取消的上游仍会被接受，但新作业立即失败。
// 相同提交人+请求号+内容的重复提交（含并发重复）返回原作业及其当前状态；
// 单依赖方式与只含同一个作业号的列表视为相同内容。
func (s *Store) Submit(req SubmitRequest) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.stopping.Load() {
		return nil, ErrStoreClosed
	}
	if len(req.Values) == 0 {
		return nil, ErrEmptySequence
	}
	// 两种依赖方式不能混用；多依赖列表本身必须非零且无重复。
	if req.HasDependency && len(req.Dependencies) > 0 {
		return nil, fmt.Errorf("%w：同时填写了单依赖 %d 与非空依赖列表 %v",
			ErrInvalidDependencyList, req.DependencyID, req.Dependencies)
	}
	deps := dependencyList(&req)
	if len(req.Dependencies) > 0 {
		var zeros, dups []uint64
		seen := make(map[uint64]struct{}, len(req.Dependencies))
		for _, id := range req.Dependencies {
			if id == 0 {
				if !containsUint64(zeros, 0) {
					zeros = append(zeros, 0)
				}
				continue
			}
			if _, ok := seen[id]; ok {
				if !containsUint64(dups, id) {
					dups = append(dups, id)
				}
				continue
			}
			seen[id] = struct{}{}
		}
		if len(zeros) > 0 || len(dups) > 0 {
			return nil, fmt.Errorf("%w：无效作业号 %v，重复作业号 %v",
				ErrInvalidDependencyList, zeros, dups)
		}
		var missing []uint64
		for _, id := range deps { // deps 保持提交次序
			if s.byID[id] == nil {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 {
			return nil, fmt.Errorf("%w：依赖作业号 %v 不存在", ErrDependencyNotFound, missing)
		}
	} else if req.HasDependency {
		if s.byID[req.DependencyID] == nil {
			return nil, fmt.Errorf("%w：作业号 %d", ErrDependencyNotFound, req.DependencyID)
		}
	}
	if req.RequestID != "" {
		key := s.idemKey(req.Submitter, req.RequestID)
		if origID, ok := s.idem[key]; ok {
			orig := s.byID[origID]
			if sameContent(orig, &req) {
				return s.viewLocked(orig), nil
			}
			return s.viewLocked(orig),
				fmt.Errorf("%w：提交人 %q 的请求号 %q 已用于作业 %d",
					ErrIdempotencyConflict, req.Submitter, req.RequestID, origID)
		}
	}

	now := s.now().UTC()
	j := &storedJob{
		id:            s.nextID,
		submitter:     req.Submitter,
		requestID:     req.RequestID,
		seed:          req.Seed,
		values:        append([]int64(nil), req.Values...),
		hasDependency: req.HasDependency,
		dependencyID:  req.DependencyID,
		dependencies:  deps,
		queuedAt:      now,
		status:        StatusQueued,
	}
	// 先持久化，接受之后该提交即不可丢失。
	if err := s.persist(j); err != nil {
		return nil, err
	}
	s.nextID++
	s.jobs = append(s.jobs, j)
	s.byID[j.id] = j
	if req.RequestID != "" {
		s.idem[s.idemKey(req.Submitter, req.RequestID)] = j.id
	}
	// 依赖已失败/取消时，新接受的作业立刻进入失败终态。
	s.cascadeLocked()
	s.notifyLocked()
	return s.viewLocked(j), nil
}

// dependencyList 返回提交的归一化直接上游列表（拷贝，保持提交次序）：
// 优先使用多依赖列表；只启用单依赖方式时为单元素列表；否则为空。
func dependencyList(req *SubmitRequest) []uint64 {
	if len(req.Dependencies) > 0 {
		return append([]uint64(nil), req.Dependencies...)
	}
	if req.HasDependency {
		return []uint64{req.DependencyID}
	}
	return nil
}

func containsUint64(xs []uint64, x uint64) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// sameContent 判断两次提交的内容是否一致；整数次序或直接上游列表的
// 内容/次序不同都视为不同内容。单依赖方式与只含同一作业号的列表等价。
func sameContent(j *storedJob, req *SubmitRequest) bool {
	if j.seed != req.Seed {
		return false
	}
	if len(j.values) != len(req.Values) {
		return false
	}
	for i := range j.values {
		if j.values[i] != req.Values[i] {
			return false
		}
	}
	deps := dependencyList(req)
	if len(j.dependencies) != len(deps) {
		return false
	}
	for i := range j.dependencies {
		if j.dependencies[i] != deps[i] {
			return false
		}
	}
	return true
}

// Get 按作业号读取作业详情（含成功归档）；不存在返回 [ErrNotFound]。
func (s *Store) Get(id uint64) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.byID[id]
	if j == nil {
		return nil, fmt.Errorf("%w：作业号 %d", ErrNotFound, id)
	}
	return s.viewLocked(j), nil
}

// Cancel 请求取消作业。
//
// 排队或运行中的作业都可以取消；取消一旦返回成功，该作业及依赖它的作业
// 都不会再产生成功归档。重复取消已取消的作业仍返回成功；对已成功或已
// 失败的作业取消返回 [ErrNotCancellable]，原结果保持不变。
func (s *Store) Cancel(id uint64) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrStoreClosed
	}
	j := s.byID[id]
	if j == nil {
		return nil, fmt.Errorf("%w：作业号 %d", ErrNotFound, id)
	}
	switch j.status {
	case StatusCanceled:
		return s.viewLocked(j), nil
	case StatusSucceeded, StatusFailed:
		return s.viewLocked(j),
			fmt.Errorf("%w：作业 %d 当前状态为 %s", ErrNotCancellable, id, j.status)
	}

	// queued / running：立刻把终态落盘，使“已确认的取消”不会因崩溃丢失。
	now := s.now().UTC()
	j.status = StatusCanceled
	j.failureReason = ""
	j.blockerID = 0
	if j.finishedAt.IsZero() {
		j.finishedAt = now
	}
	j.effectiveValues = nil
	j.archive = nil
	j.canceled.Store(true)
	if err := s.persist(j); err != nil {
		return nil, err
	}
	// 依赖它的等待者一并失败，且继续向更下游传播。
	s.cascadeLocked()
	s.notifyLocked()
	return s.viewLocked(j), nil
}

// List 按提交人和提交时间范围（两端均包含）列出记录，结果按提交先后排列。
// start 晚于 end 返回 [ErrInvalidTimeRange]。零值时间表示该端不限。
func (s *Store) List(submitter string, start, end time.Time) ([]*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !start.IsZero() && !end.IsZero() && start.After(end) {
		return nil, fmt.Errorf("%w：起始时间 %s 晚于结束时间 %s",
			ErrInvalidTimeRange, start.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano))
	}
	out := make([]*Job, 0)
	for _, j := range s.jobs { // jobs 已按 id 升序，即提交先后
		if j.submitter != submitter {
			continue
		}
		if !start.IsZero() && j.queuedAt.Before(start) {
			continue
		}
		if !end.IsZero() && j.queuedAt.After(end) {
			continue
		}
		out = append(out, s.viewLocked(j))
	}
	return out, nil
}

// cascadeLocked 把“任一直接上游已失败/取消”的排队作业立即标记为失败并递归
// 传播，不必等待其余上游结束。
//
// 选择阻断者时严格按提交时的依赖列表次序，取第一个已失败/取消的直接上游；
// 失败一旦确定即为终态，后来其余上游的状态变化不会改写原因。
// 依赖在提交时必须已存在，因此直接上游作业号必然更小，按作业号升序单趟
// 扫描即可让阻断沿链条传播到最下游；阻断链条上的根因作业号沿 blockerID 追溯。
func (s *Store) cascadeLocked() {
	for _, j := range s.jobs {
		if j.status != StatusQueued || len(j.dependencies) == 0 {
			continue
		}
		for idx, depID := range j.dependencies {
			dep := s.byID[depID]
			if dep == nil {
				continue
			}
			if dep.status != StatusFailed && dep.status != StatusCanceled {
				continue
			}
			root := dep.blockerID
			if root == 0 {
				root = dep.id
			}
			s.markFailedLocked(j, blockedReason(dep, root, idx, len(j.dependencies)), root)
			break // 本作业的失败原因已确定，忽略其余上游。
		}
	}
}

func blockedReason(dep *storedJob, root uint64, idx, total int) string {
	state := "失败"
	if dep.status == StatusCanceled {
		state = "被取消"
	}
	var reason string
	if total > 1 {
		reason = fmt.Sprintf("多个直接上游中，作业 %d（依赖列表第 %d/%d 项）已%s，阻断本作业继续计算",
			dep.id, idx+1, total, state)
	} else {
		reason = fmt.Sprintf("依赖的作业 %d 已%s，阻断本作业继续计算", dep.id, state)
	}
	if dep.status == StatusFailed && dep.failureReason != "" {
		reason += "（其失败原因：" + dep.failureReason + "）"
	}
	if root != dep.id {
		reason += fmt.Sprintf("；阻断根因为作业 %d", root)
	}
	return reason
}

// pendingDependenciesLocked 按依赖列表次序返回尚未成功归档的直接上游作业号。
func (s *Store) pendingDependenciesLocked(j *storedJob) []uint64 {
	var pending []uint64
	for _, depID := range j.dependencies {
		dep := s.byID[depID]
		if dep == nil || dep.status != StatusSucceeded {
			pending = append(pending, depID)
		}
	}
	return pending
}

// pickRunnableLocked 返回提交顺序上第一个可运行的排队作业。
// 等待任一上游（上游排队/运行中）的作业会被跳过，不挡住后面的作业；
// 可运行要求全部直接上游都已成功且归档完整。
func (s *Store) pickRunnableLocked() *storedJob {
	for _, j := range s.jobs {
		if j.status != StatusQueued {
			continue
		}
		if len(j.dependencies) == 0 {
			return j
		}
		ready := true
		for _, depID := range j.dependencies {
			dep := s.byID[depID]
			if dep == nil || dep.status != StatusSucceeded {
				ready = false
				break
			}
		}
		if ready {
			return j
		}
	}
	return nil
}

// notifyLocked 在持锁状态下非阻塞唤醒 worker。
func (s *Store) notifyLocked() {
	select {
	case s.waitCh <- struct{}{}:
	default:
	}
}

// notify 在不持锁时唤醒 worker。
func (s *Store) notify() {
	s.mu.Lock()
	s.notifyLocked()
	s.mu.Unlock()
}

// persistRetry 对落盘做有限次重试，尽量避免内存状态与磁盘记录分叉。
func (s *Store) persistRetry(j *storedJob, attempts int) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = s.persist(j); err == nil {
			return nil
		}
	}
	return err
}

// worker 是本地唯一的计算执行体。
func (s *Store) worker() {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		if s.stopping.Load() {
			s.mu.Unlock()
			return
		}
		j := s.pickRunnableLocked()
		if j == nil {
			ch := s.waitCh
			s.mu.Unlock()
			select {
			case <-ch:
			case <-s.stopCh:
			}
			continue
		}

		// 进入运行并原子持久化该状态。
		j.status = StatusRunning
		j.startedAt = s.now().UTC()
		// 可运行意味着全部直接上游均已成功归档；严格按依赖列表次序把
		// 各上游总和逐个追加到原始序列末尾，与上游完成先后无关。
		inputs := append([]int64(nil), j.values...)
		for _, depID := range j.dependencies {
			if dep := s.byID[depID]; dep != nil && dep.archive != nil {
				inputs = append(inputs, dep.archive.Sum)
			}
		}
		seed := j.seed
		s.runningID = j.id
		if err := s.persistRetry(j, 3); err != nil {
			// 连运行状态都无法落盘：保守地失败，不进行计算。
			s.markFailedLocked(j, "无法持久化作业状态："+err.Error(), 0)
			s.runningID = 0
			s.cascadeLocked()
			s.notifyLocked()
			s.mu.Unlock()
			continue
		}
		s.mu.Unlock()

		abort := func() bool { return j.canceled.Load() || s.stopping.Load() }
		doCompute := s.compute
		if doCompute == nil {
			doCompute = func(_ uint64, in []int64, sd int64, c func() bool) (int64, int64, string, bool) {
				return computeResult(in, sd, c)
			}
		}
		sum, sumSq, reason, ok := doCompute(j.id, inputs, seed, abort)

		s.mu.Lock()
		s.runningID = 0
		switch {
		case j.status == StatusCanceled:
			// 用户取消已在 Cancel 中落盘；此处确保丢弃任何计算产物。
			j.archive = nil
			j.effectiveValues = nil
			_ = s.persistRetry(j, 3)
			s.cascadeLocked()
		case s.stopping.Load():
			s.markFailedLocked(j, "计算被中断：归档在计算过程中关闭", 0)
			s.cascadeLocked()
		case !ok:
			s.markFailedLocked(j, reason, 0)
			s.cascadeLocked()
		default:
			// 成功：状态与完整归档在同一条记录的同一次原子写入中可见。
			completed := s.now().UTC()
			a := newArchive(j, inputs, sum, sumSq, completed)
			j.effectiveValues = append([]int64(nil), inputs...)
			j.status = StatusSucceeded
			j.finishedAt = completed
			j.failureReason = ""
			j.blockerID = 0
			j.archive = a
			if err := s.persistRetry(j, 3); err != nil {
				// 落盘失败时绝不让对外可见“成功却没有归档”：回退为失败。
				j.archive = nil
				j.effectiveValues = nil
				s.markFailedLocked(j, "结果归档写入失败："+err.Error(), 0)
			}
		}
		s.notifyLocked()
		s.mu.Unlock()
	}
}

// markFailedLocked 在持锁状态下把作业置为失败终态并尝试落盘。
func (s *Store) markFailedLocked(j *storedJob, reason string, blocker uint64) {
	j.status = StatusFailed
	j.failureReason = reason
	j.blockerID = blocker
	if j.finishedAt.IsZero() {
		j.finishedAt = s.now().UTC()
	}
	j.archive = nil
	j.effectiveValues = nil
	_ = s.persistRetry(j, 3)
}

// newArchive 构造成功归档；CompletedAt 之外的全部字段都是确定性的。
func newArchive(j *storedJob, effective []int64, sum, sumSquares int64, completedAt time.Time) *Archive {
	inDigest := inputsDigestHex(effective, j.seed)
	resDigest := resultDigestHex(effective, j.seed, sum, sumSquares)
	log := buildLog(effective, j.seed, sum, sumSquares)
	checksum := checksumHex(effective, j.seed, sum, sumSquares, log, resDigest)
	return &Archive{
		JobID:           j.id,
		Submitter:       j.submitter,
		RequestID:       j.requestID,
		Seed:            j.seed,
		Values:          append([]int64(nil), j.values...),
		HasDependency:   j.hasDependency,
		DependencyID:    j.dependencyID,
		Dependencies:    append([]uint64(nil), j.dependencies...),
		EffectiveValues: append([]int64(nil), effective...),
		InputsDigest:    inDigest,
		Sum:             sum,
		SumOfSquares:    sumSquares,
		ResultDigest:    resDigest,
		Log:             log,
		Checksum:        checksum,
		CompletedAt:     completedAt,
	}
}

// viewLocked 在持锁状态下生成对外只读视图，全部切片深拷贝。
func (s *Store) viewLocked(j *storedJob) *Job {
	v := &Job{
		ID:              j.id,
		Submitter:       j.submitter,
		RequestID:       j.requestID,
		Seed:            j.seed,
		Values:          append([]int64(nil), j.values...),
		HasDependency:   j.hasDependency,
		DependencyID:    j.dependencyID,
		Dependencies:    append([]uint64(nil), j.dependencies...),
		QueuedAt:        j.queuedAt,
		StartedAt:       j.startedAt,
		FinishedAt:      j.finishedAt,
		Status:          j.status,
		FailureReason:   j.failureReason,
		BlockerID:       j.blockerID,
		EffectiveValues: append([]int64(nil), j.effectiveValues...),
	}
	if j.status == StatusQueued {
		v.WaitReason = WaitSlot
		if pending := s.pendingDependenciesLocked(j); len(pending) > 0 {
			v.WaitReason = WaitDependency
			v.PendingDependencies = pending // 已是新分配的有序拷贝
		}
	}
	if j.archive != nil {
		a := *j.archive
		a.Values = append([]int64(nil), j.archive.Values...)
		a.EffectiveValues = append([]int64(nil), j.archive.EffectiveValues...)
		a.Dependencies = append([]uint64(nil), j.archive.Dependencies...)
		v.Archive = &a
	}
	return v
}

// Dir 返回归档目录路径。
func (s *Store) Dir() string { return s.dir }
