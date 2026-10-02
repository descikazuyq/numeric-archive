// Package numeric 实现本地数值作业的提交、调度、取消与结果归档。
//
// 作业对有符号 64 位整数序列计算总和 (sum) 与平方和 (sum of squares)。
// 本地一次只运行一个作业，其余可运行作业严格按提交先后依次执行；
// 正在等待依赖结果的作业不会挡住后面可运行的作业。
//
// 所有已确认的状态（提交、取消、成功/失败结果）都持久化到目录中，
// 重新 [Store.Open] 同一目录后状态不丢失，幂等请求号继续生效，
// 上次关闭时仍在运行的作业会被标记为失败并阻止其下游继续计算。
package numeric

import (
	"errors"
	"time"
)

// Status 表示作业的状态。
type Status string

const (
	// StatusQueued 表示作业已接受，正在排队（等待计算位置或依赖结果）。
	StatusQueued Status = "queued"
	// StatusRunning 表示作业正在本地计算。
	StatusRunning Status = "running"
	// StatusSucceeded 表示计算成功，且结果已与成功状态一同归档落盘。
	StatusSucceeded Status = "succeeded"
	// StatusFailed 表示作业失败（输入溢出、依赖未通过、重启被中断等）。
	StatusFailed Status = "failed"
	// StatusCanceled 表示作业被提交人取消。
	StatusCanceled Status = "canceled"
)

// WaitReason 描述排队作业当前在等待什么。
type WaitReason string

const (
	// WaitSlot 表示依赖（若有）已全部产出可使用的结果，作业在等待本地唯一的计算位置。
	WaitSlot WaitReason = "waiting for computation slot"
	// WaitDependency 表示作业在等待其（一个或多个）依赖作业产出结果。
	WaitDependency WaitReason = "waiting for dependency result"
)

// 包级哨兵错误，调用方使用 errors.Is 判断。
var (
	// ErrEmptySequence 表示提交的整数序列为空，该提交被拒绝且不产生记录。
	ErrEmptySequence = errors.New("numeric: 整数序列为空，拒绝提交")
	// ErrNotFound 表示按作业号查不到作业。
	ErrNotFound = errors.New("numeric: 作业不存在")
	// ErrDependencyNotFound 表示提交引用了不存在的依赖作业，提交被拒绝且不产生记录。
	ErrDependencyNotFound = errors.New("numeric: 依赖作业不存在，拒绝提交")
	// ErrInvalidDependencyList 表示多依赖列表非法（含零或重复作业号），
	// 或同时使用单依赖方式与非空列表；提交被整体拒绝，不留下记录，
	// 也不占用幂等请求号。
	ErrInvalidDependencyList = errors.New("numeric: 依赖列表无效（含零或重复作业号，或混用两种依赖方式），拒绝提交")
	// ErrIdempotencyConflict 表示同一提交人使用了已存在的幂等请求号，
	// 但提交内容（整数次序、种子或依赖）与原提交不一致，保留原记录。
	ErrIdempotencyConflict = errors.New("numeric: 幂等请求号冲突，提交内容与原提交不一致")
	// ErrNotCancellable 表示作业已处于成功或失败终态，不能取消，原结果保持不变。
	ErrNotCancellable = errors.New("numeric: 作业已结束，不能取消")
	// ErrInvalidTimeRange 表示列举查询的起始时间晚于结束时间。
	ErrInvalidTimeRange = errors.New("numeric: 查询时间范围无效，起始时间晚于结束时间")
	// ErrStoreClosed 表示归档已关闭，不再接受操作。
	ErrStoreClosed = errors.New("numeric: 归档已关闭")
)

// SubmitRequest 是一次作业提交的原始参数。
type SubmitRequest struct {
	// Submitter 为提交人标识，与 RequestID 共同构成幂等键。
	Submitter string
	// RequestID 为幂等请求号，在同一提交人内有效。
	RequestID string
	// Values 为有符号 64 位整数序列，次序属于提交内容的一部分；为空则拒绝提交。
	Values []int64
	// Seed 为种子，属于提交内容的一部分，并参与结果摘要与校验值计算。
	Seed int64
	// HasDependency 为 true 时 DependencyID 指定本作业依赖的单个已存在作业。
	// 这是较早的单依赖写法，继续受支持；它与只含同一个作业号的 Dependencies
	// 单元素列表视为完全相同的提交内容。
	HasDependency bool
	// DependencyID 是单依赖写法下的依赖作业号。该依赖成功且结果归档后，
	// 其总和会作为一个额外整数（追加在序列末尾）参与本次计算。
	DependencyID uint64
	// Dependencies 为按顺序排列的上游作业号列表：只有列表中的作业全部成功
	// 且归档完整后，本作业才可运行；各上游的总和严格按列表次序逐个追加到
	// 原始序列末尾，与上游完成先后无关。列表只能引用提交时已存在的作业，
	// 不能含零或重复作业号。空列表（或 nil）表示不使用多依赖方式；
	// 同时启用单依赖方式（HasDependency）并填写非空列表会被拒绝。
	// 传入的切片会被复制，调用方事后修改不影响已保存的记录。
	Dependencies []uint64
}

// Job 是作业元数据与结果的只读视图。
//
// 所有切片都是内部数据的拷贝，调用方修改返回的 [Job]（含其 [Archive]）
// 不会影响归档内部状态，也不会改变已保存的记录。
type Job struct {
	// ID 为稳定作业号，接受提交后分配，终身不变。
	ID uint64
	// Submitter 为提交人。
	Submitter string
	// RequestID 为幂等请求号。
	RequestID string
	// Seed 为提交时记录的种子。
	Seed int64
	// Values 为提交时记录的原始整数序列（保持原次序）。
	Values []int64
	// HasDependency / DependencyID 记录提交时使用的单依赖写法；
	// 直接依赖的完整且唯一权威表示为 Dependencies（保持提交次序）。
	HasDependency bool
	DependencyID  uint64
	// Dependencies 为提交时给出的有顺序直接上游作业号列表；
	// 使用单依赖写法时它是只含 DependencyID 的单元素列表，无依赖时为空。
	// 此处为内部记录的拷贝，调用方修改不会影响归档。
	Dependencies []uint64
	// PendingDependencies 仅在 Status == StatusQueued 且 WaitReason ==
	// WaitDependency 时有意义：列出尚未成功归档的直接上游作业号，
	// 保持提交时的次序。全部上游可用后，排队原因转为 WaitSlot。
	PendingDependencies []uint64

	// QueuedAt 为接受提交的时间，列举排序与时间范围过滤均以它为准。
	QueuedAt time.Time
	// StartedAt 为进入运行状态的时间；未运行过为零值。
	StartedAt time.Time
	// FinishedAt 为进入终态（成功/失败/取消）的时间。
	FinishedAt time.Time

	// Status 为当前状态。
	Status Status
	// WaitReason 仅在 Status == StatusQueued 时有意义，说明排队原因。
	WaitReason WaitReason
	// FailureReason 仅在失败时有意义，说明失败原因（含阻断它的上游作业号）。
	FailureReason string
	// BlockerID 在因上游失败/取消而失败时，指出阻断链条的根因作业号；0 表示无。
	BlockerID uint64

	// EffectiveValues 为实际参与计算的输入（成功时）：
	// 原始序列末尾追加依赖作业的总和（若有依赖且其结果已归档）。
	EffectiveValues []int64

	// Archive 仅在 Status == StatusSucceeded 时非空，为完整成功归档。
	Archive *Archive
}

// Archive 是成功作业的完整归档记录：原始参数、实际参与计算的输入摘要、
// 结果摘要、计算日志与校验值。成功状态与归档在同一次原子落盘中可见。
type Archive struct {
	// JobID 为所属作业号。
	JobID uint64 `json:"job_id"`

	// 原始提交参数。
	Submitter     string  `json:"submitter"`
	RequestID     string  `json:"request_id"`
	Seed          int64   `json:"seed"`
	Values        []int64 `json:"values"`
	HasDependency bool    `json:"has_dependency"`
	DependencyID  uint64  `json:"dependency_id"`
	// Dependencies 保存提交时的有顺序直接上游列表（单依赖写法下为单元素列表）。
	// 仅用于归档溯源，不参与任何摘要与校验值。
	Dependencies []uint64 `json:"dependencies,omitempty"`

	// 实际参与计算的输入摘要。
	// EffectiveValues 为实际输入序列（原始序列 + 按依赖次序逐个追加的各上游总和）。
	// InputsDigest 为其与种子的确定性摘要，与作业号、时间无关。
	EffectiveValues []int64 `json:"effective_values"`
	InputsDigest    string  `json:"inputs_digest"`

	// 结果摘要。
	Sum          int64 `json:"sum"`            // 总和
	SumOfSquares int64 `json:"sum_of_squares"` // 平方和
	// ResultDigest 为有效输入、种子与结果的确定性摘要，与作业号、时间无关。
	ResultDigest string `json:"result_digest"`

	// Log 为人类可读的确定性计算日志（不含作业号与时间）。
	Log string `json:"log"`

	// Checksum 为归档校验值，只由有效输入、种子、结果与计算日志决定，
	// 相同有效输入与种子必然得到相同校验值，作业号及时间不影响它。
	Checksum string `json:"checksum"`

	// CompletedAt 为归档完成时间，不参与任何摘要与校验值。
	CompletedAt time.Time `json:"completed_at"`
}
