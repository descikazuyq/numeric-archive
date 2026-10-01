package numeric

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
)

// computeResult 是一次成功计算的内部结果。
type computeResult struct {
	sum      int64
	sumSq    int64
	inputs   []int64
	logLines []string
}

// compute 对实际参与计算的输入求和与平方和。
//
// 使用任意精度整数累加，结果超出 int64 范围时返回错误，不产生归档。
// 计算日志仅依赖输入与种子，保证确定性。
func compute(inputs []int64, seed int64) (*computeResult, error) {
	sumB := new(big.Int)
	sqB := new(big.Int)
	logLines := make([]string, 0, len(inputs))
	for i, v := range inputs {
		vB := big.NewInt(v)
		sumB.Add(sumB, vB)
		sqB.Add(sqB, new(big.Int).Mul(vB, vB))
		logLines = append(logLines, fmt.Sprintf(
			"seed=%d i=%d x=%d sum=%s sumsq=%s",
			seed, i, v, sumB.String(), sqB.String()))
	}
	if !sumB.IsInt64() || !sqB.IsInt64() {
		return nil, fmt.Errorf(
			"计算结果超出有符号 64 位整数范围：sum=%s, sumsq=%s",
			sumB.String(), sqB.String())
	}
	return &computeResult{
		sum:      sumB.Int64(),
		sumSq:    sqB.Int64(),
		inputs:   append([]int64(nil), inputs...),
		logLines: logLines,
	}, nil
}

// checksumPayload 只包含决定校验值的字段，刻意排除作业号与时间。
type checksumPayload struct {
	Inputs       []int64 `json:"inputs"`
	Seed         int64   `json:"seed"`
	Sum          int64   `json:"sum"`
	SumOfSquares int64   `json:"sum_of_squares"`
}

// checksum 计算归档校验值：对输入、种子与结果的确定性 JSON 求 SHA-256。
//
// 相同有效输入与种子得到相同校验值，与作业号、提交时间无关。
func checksum(inputs []int64, seed, sum, sumSq int64) string {
	b, err := json.Marshal(checksumPayload{
		Inputs:       inputs,
		Seed:         seed,
		Sum:          sum,
		SumOfSquares: sumSq,
	})
	if err != nil { // 仅含基础类型，不会失败
		panic(err)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// buildArchive 由计算结果构造归档。
func buildArchive(rec *jobRecord, res *computeResult) *Archive {
	a := &Archive{
		Submitter:      rec.Submitter,
		ReqNo:          rec.ReqNo,
		Sequence:       append([]int64(nil), rec.Sequence...),
		Seed:           rec.Seed,
		SubmittedAt:    rec.SubmittedAt,
		Inputs:         append([]int64(nil), res.inputs...),
		Count:          len(res.inputs),
		Sum:            res.sum,
		SumOfSquares:   res.sumSq,
		ComputationLog: res.logLines,
	}
	if rec.DependencyID != nil {
		d := *rec.DependencyID
		a.DependencyID = &d
	}
	a.Checksum = checksum(a.Inputs, a.Seed, a.Sum, a.SumOfSquares)
	return a
}
