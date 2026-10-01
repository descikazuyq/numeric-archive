package numeric

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/bits"
	"strings"
)

// addInt64 返回 a+b；结果超出 int64 时 ok 为 false。
func addInt64(a, b int64) (r int64, ok bool) {
	r = a + b
	// 同号相加却变号即溢出。
	if (a >= 0 && b >= 0 && r < 0) || (a < 0 && b < 0 && r >= 0) {
		return 0, false
	}
	return r, true
}

// squareUint 返回 |x|^2 的 128 位无符号表示 (hi, lo)。
// x 为 math.MinInt64 时 uint64(-x) 恰为 2^63，绝对值仍然正确。
func squareUint(x int64) (hi, lo uint64) {
	var m uint64
	if x >= 0 {
		m = uint64(x)
	} else {
		m = uint64(-x)
	}
	return bits.Mul64(m, m)
}

// computeResult 对有效输入（原始序列，末尾可能已追加依赖总和）做求和与平方和。
//
// canceled 在计算过程中被置位时（例如运行中被取消、归档关闭）尽快中止，
// 此时不应产生任何成功归档。任一结果超出有符号 64 位整数范围时以失败返回，
// 失败原因明确指出是哪一个结果溢出。
func computeResult(inputs []int64, seed int64, canceled func() bool) (sum, sumSquares int64, reason string, ok bool) {
	sum = 0
	var sqLo, sqHi uint64 // 平方和的 128 位累加器
	for i, v := range inputs {
		if canceled != nil && i&0x3f == 0 && canceled() {
			return 0, 0, "", false
		}

		s, sumOK := addInt64(sum, v)
		if !sumOK {
			return 0, 0, "总和超出有符号 64 位整数范围", false
		}
		sum = s

		shi, slo := squareUint(v)
		lo, carry := bits.Add64(sqLo, slo, 0)
		hi := sqHi + shi + carry
		sqLo, sqHi = lo, hi
		// 平方和非负，一旦超过 MaxInt64 就不可能再回到范围内。
		if sqHi != 0 || sqLo > 1<<63-1 {
			return 0, 0, "平方和超出有符号 64 位整数范围", false
		}
	}
	sumSquares = int64(sqLo)
	return sum, sumSquares, "", true
}

// digestWriter 按固定大端编码写入摘要输入，避免任何长度/符号歧义。
type digestWriter struct {
	buf []byte
}

func (w *digestWriter) writeBytes(b []byte) { w.buf = append(w.buf, b...) }

func (w *digestWriter) writeTag(tag string) {
	w.buf = append(w.buf, byte(len(tag)))
	w.buf = append(w.buf, tag...)
}

func (w *digestWriter) writeInt64(v int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	w.buf = append(w.buf, b[:]...)
}

func (w *digestWriter) writeUint64(v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	w.buf = append(w.buf, b[:]...)
}

func sumHex(data []byte) string {
	s := sha256.Sum256(data)
	return hex.EncodeToString(s[:])
}

// inputsDigestHex 只由有效输入序列与种子决定，与作业号、时间无关。
func inputsDigestHex(effective []int64, seed int64) string {
	w := digestWriter{}
	w.writeTag("numeric/inputs/v1")
	w.writeInt64(seed)
	w.writeUint64(uint64(len(effective)))
	for _, v := range effective {
		w.writeInt64(v)
	}
	return sumHex(w.buf)
}

// resultDigestHex 只由有效输入、种子与结果决定。
func resultDigestHex(effective []int64, seed, sum, sumSquares int64) string {
	in := inputsDigestHex(effective, seed)
	raw, _ := hex.DecodeString(in)
	w := digestWriter{}
	w.writeTag("numeric/result/v1")
	w.writeBytes(raw)
	w.writeInt64(sum)
	w.writeInt64(sumSquares)
	return sumHex(w.buf)
}

// buildLog 生成确定性的人类可读计算日志：不含作业号与时间，
// 因此相同有效输入与种子产生完全相同的日志。
func buildLog(effective []int64, seed, sum, sumSquares int64) string {
	var b strings.Builder
	fmt.Fprintf(&b, "numeric computation log\n")
	fmt.Fprintf(&b, "seed=%d\n", seed)
	fmt.Fprintf(&b, "input_count=%d\n", len(effective))
	for i, v := range effective {
		fmt.Fprintf(&b, "input[%d]=%d\n", i, v)
	}
	fmt.Fprintf(&b, "sum=%d\n", sum)
	fmt.Fprintf(&b, "sum_of_squares=%d\n", sumSquares)
	return b.String()
}

// checksumHex 只由有效输入、种子、结果与计算日志决定。
func checksumHex(effective []int64, seed, sum, sumSquares int64, log, resultDigest string) string {
	raw, _ := hex.DecodeString(resultDigest)
	w := digestWriter{}
	w.writeTag("numeric/checksum/v1")
	w.writeBytes(raw)
	w.writeInt64(seed)
	w.writeUint64(uint64(len(effective)))
	for _, v := range effective {
		w.writeInt64(v)
	}
	w.writeInt64(sum)
	w.writeInt64(sumSquares)
	w.writeBytes([]byte(log))
	return sumHex(w.buf)
}
