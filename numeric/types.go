// Package numeric 是本地数值作业与结果归档。
//
// 作业接收有符号 64 位整数序列，输出总和与平方和；一次只运行一个作业，
// 其余可运行作业按提交顺序等待。成功结果与完整归档一并落盘，重新打开
// 目录后状态延续。
package numeric

import (
	"errors"
	"time"
)

// JobStatus 描述作业生命周期状态。
type JobStatus string

const (
	// StatusQueued 已接受，等待计算位置或依赖结果。
	StatusQueued JobStatus = "queued"
	// StatusRunning 正在本地计算。
	StatusRunning JobStatus = "running"
	// StatusSucceeded 计算完成，结果与归档已落盘。
	StatusSucceeded JobStatus = "succeeded"
	// StatusFailed 计算失败（溢出、依赖失败、中断等），无成功归档。
	StatusFailed JobStatus = "failed"
	// StatusCancelled 排队或运行中被取消，无成功归档。
	StatusCancelled JobStatus = "cancelled"
)

// 拒绝与冲突类错误，调用方可用 errors.Is 判别。
var (
	// ErrEmptySequence 空整数序列被拒绝提交，不产生记录。
	ErrEmptySequence = errors.New("numeric: 拒绝提交空整数序列")
	// ErrNotFound 作业不存在。
	ErrNotFound = errors.New("numeric: 作业不存在")
	// ErrConflict 幂等请求号与已有作业内容冲突。
	ErrConflict = errors.New("numeric: 幂等请求号与已有作业内容冲突")
	// ErrCannotCancel 作业已结束，不能取消。
	ErrCannotCancel = errors.New("numeric: 作业已结束，不能取消")
	// ErrInvalidRange 查询时间范围无效（起始晚于结束）。
	ErrInvalidRange = errors.New("numeric: 查询时间范围无效，起始时间晚于结束时间")
	// ErrDepNotFound 依赖的作业不存在。
	ErrDepNotFound = errors.New("numeric: 依赖的作业不存在")
	// ErrClosed 存储已关闭。
	ErrClosed = errors.New("numeric: 存储已关闭")
)

// SubmitParams 是提交作业的参数。
type SubmitParams struct {
	// Submitter 提交人，幂等请求号在提交人内有效。
	Submitter string
	// ReqNo 幂等请求号，同一提交人内唯一。
	ReqNo string
	// Sequence 有符号 64 位整数序列，不能为空，次序参与幂等判别。
	Sequence []int64
	// Seed 种子，参与计算日志与校验值。
	Seed int64
	// DependencyID 可选依赖的作业号；依赖成功且归档后，其总和作为额外整数加入计算。
	DependencyID *int64
}

// Archive 是作业成功后关闭的结果归档。
//
// 归档随作业完成一次性写入，普通调用不能覆盖；查询返回的是深拷贝，
// 修改返回值不会改变已保存记录。
type Archive struct {
	// 原始参数
	Submitter    string
	ReqNo        string
	Sequence     []int64
	Seed         int64
	DependencyID *int64
	SubmittedAt  time.Time

	// 实际参与计算的输入（原序列 + 依赖总和）
	Inputs []int64
	Count  int

	// 结果摘要
	Sum          int64
	SumOfSquares int64

	// 计算日志（确定性，仅依赖输入与种子）
	ComputationLog []string

	// 校验值：对输入、种子与结果的确定性哈希，与作业号、时间无关
	Checksum string
}

// Job 是查询返回的作业视图，字段均为副本。
type Job struct {
	ID           int64
	Submitter    string
	ReqNo        string
	Sequence     []int64
	Seed         int64
	DependencyID *int64
	SubmittedAt  time.Time
	Status       JobStatus

	// 排队说明：waiting_slot 表示等待计算位置；waiting_dependency 表示等待依赖结果。
	QueueState string
	// QueuePosition 在可运行队列中的位置（1 起）；等待依赖时为 0。
	QueuePosition int
	// WaitingFor 等待依赖时给出依赖作业号。
	WaitingFor *int64

	// Archive 成功归档；仅 StatusSucceeded 时有值。
	Archive *Archive
	// FailureReason 失败原因；仅 StatusFailed 时有值。
	FailureReason string
}
