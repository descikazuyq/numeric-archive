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
// idemIdentity 是幂等作用域（提交人, 请求号）的复合键。两个字段都按调用方
// 给出的完整字符串逐字节参与比较，允许包含 U+0000 与非法 UTF-8 字节
// （后者靠记录中的 identity_raw 兜底字段跨重开保真）。不能用分隔符把两个
// 字段拼成单个字符串：提交人 "a"、请求号 "b\x00c" 与提交人 "a\x00b"、
// 请求号 "c" 在任何分隔符方案下都会产生歧义（分隔符本身可以出现在字段内部）。
type idemIdentity struct {
	submitter string
	requestID string
}

type Store struct {
	dir string

	mu     sync.Mutex
	jobs   []*storedJob // 严格按作业号（即提交先后）排列
	byID   map[uint64]*storedJob
	idem   map[idemIdentity]uint64 // (提交人, 请求号) -> 作业号
	nextID uint64
	closed bool

	// occupiedNames 记录归档目录中每份正式记录文件实际占用的文件名（不含目录）
	// 及其归属作业号。恢复出的作业保留读入时的文件名——它可能恰好是另一个
	// （尚未分配的）作业号的默认命名；新提交按默认命名写入前必须据此确认目标
	// 文件不属于另一作业，否则拒绝提交而不是覆盖旧记录。新接受的作业写入默认
	// 命名后同样登记进来，使该映射始终等于目录内正式记录文件的真实占用情况。
	occupiedNames map[string]uint64

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

	// persistFault 仅用于包内确定性测试：若非 nil，每次 persist 都会先调用它，
	// 返回非 nil 即模拟该次记录保存失败（如同临时记录无法创建或原记录无法替换）。
	// 生产代码恒为 nil。钩子可按作业号与“待保存的当前状态”只让某一次写入失败
	// （例如仅让成功归档写入失败），而让其余记录照常落盘。
	persistFault func(j *storedJob) error
}

// Open 打开（必要时创建）目录 dir 并恢复其中的全部作业记录。
//
// 恢复语义：
//   - 已确认的提交、取消与完成结果原样保留，幂等请求号继续生效；
//   - 目录内两份或更多记录文件顶层保存相同作业号（文件名不同、乃至内容
//     完全相同）时打开失败，返回非空错误且归档对象为 nil；错误说明重复的
//     作业号与冲突文件名。此时不启动任何计算，也不改写目录内任何原有
//     记录，调用方处理冲突后再次打开才按下列规则恢复；
//   - 两份作业号不同的记录保存了相同提交人与同一个非空请求号时同样打开
//     失败，返回非空错误且归档对象为 nil；错误说明重复归属的请求号、提交
//     人、两个冲突作业号与各自文件名。标识按恢复后的完整字节比较（含
//     U+0000 与非法 UTF-8）；空请求号不启用幂等故不参与，不同提交人使用
//     相同请求号合法。即使两份记录内容完全相同、或其中一份已失败/取消也
//     不合并、不释放请求号；同样不启动计算、不改写任何原有记录；
//   - 上次关闭时仍在运行的作业标记为失败（计算被中断）并阻止其下游；
//   - 排队作业继续排队并由本地唯一 worker 按提交先后处理；
//   - 成功状态始终与完整归档同时可见。
//
// 恢复出的记录保持读入时的正式文件名：读取规则接受任何 job- 开头、.json
// 结尾的合法记录，作业号由记录内容识别，因此恢复改写与后续状态更新（运行、
// 完成、失败、取消）都写回同一份文件，不按默认命名另写一份而同号旧记录
// 原样保留；原文件无法替换时返回实际保存错误，不转到另一文件。若某份恢复
// 记录的文件名恰好等于下一个新作业将使用的默认文件名，新提交会被拒绝
// （[ErrFileNameOccupied]）而不是覆盖该记录，直到调用方改名解除冲突。
func Open(dir string, opts ...Option) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	jobs, maxID, occupiedNames, err := loadDir(dir)
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir:           dir,
		jobs:          jobs,
		byID:          make(map[uint64]*storedJob),
		idem:          make(map[idemIdentity]uint64),
		nextID:        maxID + 1,
		occupiedNames: occupiedNames,
		now:           wallClock{}.Now,
		waitCh:        make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	for _, j := range jobs {
		s.byID[j.id] = j
		if j.requestID != "" {
			// loadDir 已在任何恢复改写之前确认同一提交人的非空请求号不会跨
			// 不同作业号重复，因此这里的索引恢复与提交路径遵守同一项唯一性。
			s.idem[idemIdentity{j.submitter, j.requestID}] = j.id
		}
	}
	// 重建失败/取消作业对排队下游的阻断。
	s.cascadeLocked()
	s.wg.Add(1)
	go s.worker()
	return s, nil
}

// Close 关闭归档。正在运行的作业会尽快中止并标记为失败（计算被中断），
// 排队作业原样保留在磁盘上，下次 [Open] 继续。
//
// 多次 Close（含并发调用）共同完成对同一个归档对象的一次关闭：每个调用都
// 等到 worker 退出、运行中作业的中断处理已经落盘后才成功返回。后发调用
// 不能只因为前一次调用已把关闭标记置位就提前返回——否则调用方可能据此立即
// 重新打开目录，而旧 worker 仍在补写运行作业的结果。归档完全关闭后的再次
// Close 立即成功返回；空闲归档（无运行中作业）也直接完成关闭。
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		// 关闭可能正由另一次并发 Close 进行。wg 在 Open 时 +1、worker 退出
		// 时归零；Wait 允许被多个 goroutine 同时等待，所有调用都在同一次
		// 关闭真正完成（worker 已退出、再无计算与补写）时才返回。
		// 已完全关闭时计数早已归零，这里立即成功返回。
		s.mu.Unlock()
		s.wg.Wait()
		return nil
	}
	s.closed = true
	// 停止标记必须与 closed 在同一次持锁中生效：Submit 的拒绝、worker 的
	// “是否从队列启动新计算”与“是否把刚结束的计算确认为成功”都观察同一把
	// 锁。若先放开锁再置 stopping，worker 可能在这个窗口内通过循环开头的
	// 检查并启动排队作业，或把尚未确认保存的计算按成功落盘——调用方已经在
	// 被 ErrStoreClosed 拒绝，却仍能拿到关闭过程新产生的成功结果。
	if s.stopping.CompareAndSwap(false, true) {
		close(s.stopCh)
	}
	var running *storedJob
	if s.runningID != 0 {
		running = s.byID[s.runningID]
	}
	s.mu.Unlock()

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
// 空序列返回 [ErrEmptySequence]；依赖列表结构不合法（单依赖与列表同时启用、
// 含零或重复作业号）返回 [ErrInvalidDependency]——这些请求本身的格式错误先于
// 幂等判定拒绝，即使复用已接受的请求号也不例外。
//
// 非空请求号命中同一提交人的已接受请求时，幂等判定先于“引用的上游必须存在”
// 校验：内容（整数序列及次序、种子、有序依赖内容）完全一致时返回原作业的
// 当前详情且不返回错误，即使其直接上游记录在重开归档后已缺失、原作业已因此
// 被改判为失败；内容不一致返回 [ErrIdempotencyConflict]（同时返回原作业视图，
// 仍引用缺失上游时在原因中一并指出），原记录保持原样。重复提交不创建新作业、
// 不补建缺失上游、不重新排队失败作业、也不改写原始参数或失败记录。
//
// 未命中已接受请求的新请求（提交人不同、请求号不同或请求号为空）引用不存在
// 的依赖时返回 [ErrDependencyNotFound]；上述拒绝都不产生记录，也不占用请求号。
// 新作业按默认命名 job-<作业号>.json 落盘，而恢复出的旧记录允许使用合法的
// 非默认文件名（作业号由记录内容识别）并继续写回原文件：当某份旧记录的
// 文件名恰好等于拟分配作业号的默认文件名时，返回 [ErrFileNameOccupied]——
// 错误给出拟分配的新作业号、被占用的文件名与原归属作业号。该拒绝不产生新
// 作业、不进入计算队列、不消耗作业号、不登记请求号，也不改动占用该名称的
// 旧记录（其即使已失败或取消仍占用文件名；只按记录内容识别作业号，不看
// 文件名中的数字）。这项文件名检查只作用于需要创建记录的新请求，位于幂等
// 判定与上游存在性校验之后：命中已接受请求号时，相同内容仍返回原作业当前
// 详情、内容不同仍返回原有的 [ErrIdempotencyConflict]，空请求号仍按新请求处理。
// 相同提交人+请求号+内容的重复提交（含并发重复）返回原作业及其当前状态。
func (s *Store) Submit(req SubmitRequest) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.stopping.Load() {
		return nil, ErrStoreClosed
	}
	if len(req.Values) == 0 {
		return nil, ErrEmptySequence
	}
	// 先校验与上游存在性无关的列表结构（单依赖与列表互斥、零与重复作业号），
	// 这类拒绝不依赖内存索引，对新请求与幂等重放一视同仁。
	if err := s.validateDependencyShapeLocked(&req); err != nil {
		return nil, err
	}
	deps := requestDeps(&req)
	if req.RequestID != "" {
		key := idemIdentity{req.Submitter, req.RequestID}
		if origID, ok := s.idem[key]; ok {
			orig := s.byID[origID]
			// 幂等重放先于“引用的上游必须存在”的校验：重新打开归档后原作业
			// 直接上游的记录可能已经缺失，原作业也因此被改判为失败并落盘。
			// 只要本次提交的整数序列及次序、种子与有序依赖内容和已接受请求
			// 完全一致（单依赖写法与只含同一作业号的列表等价），就返回原作业
			// 的当前详情——不重建缺失上游、不重新排队、不补算、不改写记录。
			if sameContent(orig, &req) {
				return s.viewLocked(orig), nil
			}
			return s.viewLocked(orig),
				fmt.Errorf("%w：提交人 %q 的请求号 %q 已用于作业 %d，且本次内容与原请求不一致%s，原记录保持原样",
					ErrIdempotencyConflict, req.Submitter, req.RequestID, origID,
					s.missingDependencyNoteLocked(deps))
		}
	}
	// 新请求（请求号为空，或该提交人首次使用此请求号）仍要求每个直接上游
	// 都存在；引用不存在上游的提交被拒绝，不产生记录，也不占用请求号。
	// 此检查必须位于幂等判定之后：上面的重放/冲突分支可能引用缺失的上游。
	if err := s.validateDependenciesExistLocked(deps); err != nil {
		return nil, err
	}
	// 新作业按默认命名写入，但目录中可能有一份恢复出的旧记录（作业号由记录
	// 内容识别，文件名是合法的非默认命名）恰好占用了这个文件名。直接写入会
	// 覆盖属于另一作业的已保存记录，因此拒绝本次请求。该校验只作用于需要
	// 创建记录的新请求：上面的幂等重放/冲突不写文件，早已返回；旧记录即使
	// 已失败或取消仍占用其文件名，文件名里的数字也不能替代记录中的真实作业号。
	if err := s.validateDefaultFileNameFreeLocked(); err != nil {
		return nil, err
	}

	now := s.now().UTC()
	j := &storedJob{
		id:           s.nextID,
		submitter:    req.Submitter,
		requestID:    req.RequestID,
		seed:         req.Seed,
		values:       append([]int64(nil), req.Values...),
		dependencies: append([]uint64(nil), deps...),
		queuedAt:     now,
		status:       StatusQueued,
	}
	// 先持久化，接受之后该提交即不可丢失。
	if err := s.persist(j); err != nil {
		return nil, err
	}
	s.nextID++
	s.jobs = append(s.jobs, j)
	s.byID[j.id] = j
	// 新作业按默认命名落盘，登记该文件的归属；恢复出的旧记录不会使用默认
	// 命名（否则上面的校验已拒绝本次提交），故这里登记的键此前不存在。
	s.occupiedNames[recordFileName(j)] = j.id
	if req.RequestID != "" {
		s.idem[idemIdentity{req.Submitter, req.RequestID}] = j.id
	}
	// 依赖已失败/取消时，新接受的作业立刻进入失败终态。
	s.cascadeLocked()
	s.notifyLocked()
	return s.viewLocked(j), nil
}

// requestDeps 规范化一次提交的直接上游作业号列表，与记录、归档共用 deps.go
// 中的同一换算：非空 Dependencies 优先（保持次序）；否则单依赖方式退化为只含
// 一个作业号的列表；两者皆无则为 nil。调用前须先经
// [Store.validateDependencyShapeLocked] 与（新请求路径上的）
// [Store.validateDependenciesExistLocked] 校验。
func requestDeps(req *SubmitRequest) []uint64 {
	return normalizeDependencies(req.HasDependency, req.DependencyID, req.Dependencies)
}

// validateDependencyShapeLocked 在持锁状态下校验提交依赖列表中与上游存在性
// 无关的结构问题：
//   - 单依赖方式与非空列表同时启用 → 拒绝；
//   - 列表含零或重复作业号 → 拒绝（错误指出具体作业号）。
//
// 该校验先于幂等判定执行：空整数序列、依赖号为零、重复依赖与两种写法
// 同时启用都属于请求本身的格式错误，即使提交人+请求号与已接受请求相同
// 也仍返回原有的对应拒绝。
func (s *Store) validateDependencyShapeLocked(req *SubmitRequest) error {
	if req.HasDependency && len(req.Dependencies) > 0 {
		return fmt.Errorf("%w：单依赖方式与依赖列表不能同时启用", ErrInvalidDependency)
	}
	deps := requestDeps(req)
	seen := make(map[uint64]struct{}, len(deps))
	for _, id := range deps {
		if id == 0 {
			return fmt.Errorf("%w：依赖列表包含无效作业号 0", ErrInvalidDependency)
		}
		if _, dup := seen[id]; dup {
			return fmt.Errorf("%w：依赖列表包含重复作业号 %d", ErrInvalidDependency, id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

// validateDependenciesExistLocked 校验规范化后的依赖列表只引用当前归档中
// 存在的作业。仅适用于幂等判定之后的新请求：已接受请求的重放（或内容
// 冲突）即使引用了重开后缺失的上游，也要返回原作业而不是被这里拒绝。
//
// 任一拒绝都不产生记录，也不占用幂等请求号。
func (s *Store) validateDependenciesExistLocked(deps []uint64) error {
	for _, id := range deps {
		if s.byID[id] == nil {
			return fmt.Errorf("%w：作业号 %d", ErrDependencyNotFound, id)
		}
	}
	return nil
}

// validateDefaultFileNameFreeLocked 校验拟分配作业号 s.nextID 的默认记录
// 文件没有被一份恢复出的旧记录占用。读取功能按记录内容识别作业号，允许
// 合法记录使用不同于默认命名的文件名并在状态更新时写回原文件；若这份文件
// 名恰好等于新作业将使用的默认命名，直接写入会覆盖属于另一作业的已保存
// 记录，因此拒绝新请求。
//
// 拒绝不产生新作业、不进入计算队列、不消耗作业号、不登记提交人的请求号，
// 也不移动、删除、重命名或改写占用该名称的旧记录：旧作业的参数、状态与
// 已有结果继续按原有文件名读取与保存。旧记录即使已失败或取消也不释放其
// 文件名；只按实际文件名判断，不拿文件名中的数字充当记录中的真实作业号。
// 错误明确给出拟分配的新作业号、被占用的文件名与该文件原归属的作业号。
func (s *Store) validateDefaultFileNameFreeLocked() error {
	name := jobFileName(s.nextID)
	if owner, occupied := s.occupiedNames[name]; occupied {
		return fmt.Errorf("%w：拟分配的新作业号为 %d，其默认记录文件 %s 已属于另一作业 %d；"+
			"拒绝提交以避免覆盖该作业的已保存记录，未产生新作业、未消耗作业号、未登记请求号，"+
			"也不改动该文件；请先处理占用文件（如改名）后重试",
			ErrFileNameOccupied, s.nextID, name, owner)
	}
	return nil
}

// missingDependencyNoteLocked 在幂等冲突时补充说明：若本次（与原请求内容
// 不一致的）提交仍引用已经缺失的上游，明确指出缺失的作业号，使冲突原因
// 不会被误读成仅仅是上游不存在。
func (s *Store) missingDependencyNoteLocked(deps []uint64) string {
	for _, id := range deps {
		if s.byID[id] == nil {
			return fmt.Sprintf("；本次引用的直接上游作业 %d 记录已缺失", id)
		}
	}
	return ""
}

// sameContent 判断两次提交的内容是否一致；整数次序或依赖列表（内容与次序）
// 不同即视为不同内容。单依赖方式与只含同一作业号的列表视为相同内容。
func sameContent(j *storedJob, req *SubmitRequest) bool {
	if j.seed != req.Seed {
		return false
	}
	rd := requestDeps(req)
	if len(j.dependencies) != len(rd) {
		return false
	}
	for i := range j.dependencies {
		if j.dependencies[i] != rd[i] {
			return false
		}
	}
	if len(j.values) != len(req.Values) {
		return false
	}
	for i := range j.values {
		if j.values[i] != req.Values[i] {
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
// 排队或运行中的作业都可以取消；取消以“取消状态已成功保存进归档记录”为生效
// 条件：只有原子替换原记录成功后，返回结果、可查询状态与磁盘记录才一致地呈现
// 为取消，该作业及依赖它的作业才不会再产生成功归档。若取消记录无法创建临时
// 文件或无法替换原记录，返回的是实际保存错误，作业保持原状——不新增完成时间、
// 不清空原有信息、不中止正在运行的计算、不级联标记下游，排队与调度规则不变；
// 写入条件恢复后对仍处于排队或运行状态的同一作业再次取消会重新尝试保存。
// 重复取消已取消的作业仍返回成功；对已成功或已失败的作业取消返回
// [ErrNotCancellable]，原结果保持不变（计算在两次取消请求之间完成时亦然）。
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

	// queued / running：先把取消终态写入原记录，确认原子替换成功后才让取消
	// 生效。持锁期间外界观察不到中间态；若创建临时记录或替换原记录失败，
	// 必须逐字段回滚，内存与磁盘都保持取消前的原状并返回实际保存错误，
	// 不能把这次取消当作已完成。
	now := s.now().UTC()
	prevStatus := j.status
	prevFinishedAt := j.finishedAt
	prevReason := j.failureReason
	prevBlocker := j.blockerID
	prevEffective := j.effectiveValues
	prevArchive := j.archive
	j.status = StatusCanceled
	j.failureReason = ""
	j.blockerID = 0
	if j.finishedAt.IsZero() {
		j.finishedAt = now
	}
	j.effectiveValues = nil
	j.archive = nil
	if err := s.persist(j); err != nil {
		j.status = prevStatus
		j.finishedAt = prevFinishedAt
		j.failureReason = prevReason
		j.blockerID = prevBlocker
		j.effectiveValues = prevEffective
		j.archive = prevArchive
		return nil, err
	}

	// 取消已确认保存：提交终态，让正在运行的计算尽快中止，并级联标记下游。
	j.canceled.Store(true)
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

// cascadeLocked 把“直接上游已失败/取消”的排队作业标记为失败并递归传播。
//
// 上游在提交时必须已存在，因此上游作业号必然更小，按作业号升序单趟
// 扫描即可让阻断沿链条传播到最下游。选择直接阻断者、确定根因与构造失败
// 原因统一走 blocker.go 中的规则（与重开归档同一套）：作业有多个直接上游时
// 按保存的依赖顺序选取最靠前的已失败/取消者，根因沿该上游已有的阻断信息
// 保留；作业一旦失败，其原因不再被后来发生的上游状态变化改写。
func (s *Store) cascadeLocked() {
	for _, j := range s.jobs {
		if j.status != StatusQueued || len(j.dependencies) == 0 {
			continue
		}
		directID, direct, blocked := pickBlockingUpstream(j, s.byID, runtimeUpstreamBlocked)
		if !blocked {
			continue
		}
		root := blockerRoot(direct, directID)
		s.markFailedLocked(j, blockedFailureReason(runtimeBlockedHead(direct), direct, directID, root), root)
	}
}

// pickRunnableLocked 返回提交顺序上第一个可运行的排队作业。
// 可运行要求全部直接上游都已成功并归档；只要还有一个上游在排队或运行，
// 该作业就被跳过，不挡住后面可运行的作业。
func (s *Store) pickRunnableLocked() *storedJob {
	for _, j := range s.jobs {
		if j.status != StatusQueued {
			continue
		}
		if len(j.dependencies) == 0 {
			return j
		}
		ready := true
		for _, id := range j.dependencies {
			dep := s.byID[id]
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
		// 实际输入 = 原始序列 + 各直接上游的总和（严格按列表顺序追加）。
		inputs := append([]int64(nil), j.values...)
		for _, id := range j.dependencies {
			if dep := s.byID[id]; dep != nil && dep.archive != nil {
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
				// 归档写入失败与计算溢出等失败同等对待：立即沿依赖链阻断所有仍在
				// 排队等待它的作业，使等待者无需等到下一次提交/取消即进入失败终态。
				s.cascadeLocked()
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
// 上游作业号只作记录，不参与任何摘要与校验值。
func newArchive(j *storedJob, effective []int64, sum, sumSquares int64, completedAt time.Time) *Archive {
	inDigest := inputsDigestHex(effective, j.seed)
	resDigest := resultDigestHex(effective, j.seed, sum, sumSquares)
	log := buildLog(effective, j.seed, sum, sumSquares)
	checksum := checksumHex(effective, j.seed, sum, sumSquares, log, resDigest)
	hasDependency, dependencyID := legacyDependencyFields(j.dependencies)
	return &Archive{
		JobID:           j.id,
		Submitter:       j.submitter,
		RequestID:       j.requestID,
		IdentityRaw:     rawIdentity(j.submitter, j.requestID),
		Seed:            j.seed,
		Values:          append([]int64(nil), j.values...),
		HasDependency:   hasDependency,
		DependencyID:    dependencyID,
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
		Dependencies:    append([]uint64(nil), j.dependencies...),
		QueuedAt:        j.queuedAt,
		StartedAt:       j.startedAt,
		FinishedAt:      j.finishedAt,
		Status:          j.status,
		FailureReason:   j.failureReason,
		BlockerID:       j.blockerID,
		EffectiveValues: append([]int64(nil), j.effectiveValues...),
	}
	v.HasDependency, v.DependencyID = legacyDependencyFields(j.dependencies)
	if j.status == StatusQueued {
		v.WaitReason = WaitSlot
		// 列出尚未成功归档的直接上游，保持提交时的顺序。
		for _, id := range j.dependencies {
			if dep := s.byID[id]; dep == nil || dep.status != StatusSucceeded {
				v.PendingDependencies = append(v.PendingDependencies, id)
			}
		}
		if len(v.PendingDependencies) > 0 {
			v.WaitReason = WaitDependency
		}
	}
	if j.archive != nil {
		a := *j.archive
		a.Values = append([]int64(nil), j.archive.Values...)
		a.EffectiveValues = append([]int64(nil), j.archive.EffectiveValues...)
		a.Dependencies = append([]uint64(nil), j.archive.Dependencies...)
		if j.archive.IdentityRaw != nil {
			raw := *j.archive.IdentityRaw
			a.IdentityRaw = &raw
		}
		v.Archive = &a
	}
	return v
}

// Dir 返回归档目录路径。
func (s *Store) Dir() string { return s.dir }
