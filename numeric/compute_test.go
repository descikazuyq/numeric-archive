package numeric

import (
	"math"
	"strings"
	"testing"
)

func TestAddInt64(t *testing.T) {
	cases := []struct {
		a, b int64
		ok   bool
		want int64
	}{
		{1, 2, true, 3},
		{-1, -2, true, -3},
		{math.MaxInt64, 1, false, 0},
		{math.MinInt64, -1, false, 0},
		{math.MaxInt64, 0, true, math.MaxInt64},
		{math.MinInt64, 0, true, math.MinInt64},
		{math.MaxInt64, math.MinInt64, true, -1},
	}
	for _, c := range cases {
		r, ok := addInt64(c.a, c.b)
		if ok != c.ok || (ok && r != c.want) {
			t.Fatalf("addInt64(%d,%d)=(%d,%v), want (%d,%v)", c.a, c.b, r, ok, c.want, c.ok)
		}
	}
}

func TestComputeResultBasic(t *testing.T) {
	sum, sq, reason, ok := computeResult([]int64{1, 2, 3, -4}, 7, nil)
	if !ok {
		t.Fatalf("expected success, reason=%q", reason)
	}
	if sum != 2 || sq != 1+4+9+16 {
		t.Fatalf("got sum=%d sq=%d, want 2,30", sum, sq)
	}
}

func TestComputeResultEmpty(t *testing.T) {
	sum, sq, _, ok := computeResult(nil, 0, nil)
	if !ok || sum != 0 || sq != 0 {
		t.Fatalf("empty inputs = (%d,%d,%v), want 0,0,true", sum, sq, ok)
	}
}

func TestComputeResultSumOverflowUnit(t *testing.T) {
	// 总和溢出检测本身由 addInt64 覆盖：任何真实输入若能让总和溢出，
	// 其平方和（平方非负）也必然先越界，因此端到端只能观察到平方和失败；
	// Store 层“总和溢出”原因通过计算钩子单独测试。
	if _, ok := addInt64(math.MaxInt64, 1); ok {
		t.Fatal("MaxInt64+1 must overflow")
	}
	if _, ok := addInt64(math.MinInt64, -1); ok {
		t.Fatal("MinInt64-1 must overflow")
	}
}

func TestComputeResultSquareOverflow(t *testing.T) {
	// 3037000500^2 ≈ 9.223372037e18 > MaxInt64(9.223372036854775807e18)；
	// 总和 3037000500 远在范围内，因此必须报告“平方和”溢出且无成功结果。
	big := int64(3037000500)
	_, _, reason, ok := computeResult([]int64{big}, 0, nil)
	if ok {
		t.Fatal("expected square overflow")
	}
	if !strings.Contains(reason, "平方和") {
		t.Fatalf("reason=%q, want 平方和溢出", reason)
	}
}

func TestComputeResultNegativeSquare(t *testing.T) {
	// 负数平方按绝对值计算；MinInt64 的绝对值（2^63）平方超过范围。
	if _, _, _, ok := computeResult([]int64{-3}, 0, nil); !ok {
		t.Fatal("-3 should succeed")
	}
	if sum, sq, _, ok := computeResult([]int64{-3, -4}, 0, nil); !ok || sum != -7 || sq != 25 {
		t.Fatalf("negatives: (%d,%d,%v) want -7,25,true", sum, sq, ok)
	}
	if _, _, reason, ok := computeResult([]int64{math.MinInt64}, 0, nil); ok {
		t.Fatal("MinInt64 square must overflow")
	} else if !strings.Contains(reason, "平方和") {
		t.Fatalf("reason=%q want 平方和", reason)
	}
}

func TestComputeResultOverflowNoLookahead(t *testing.T) {
	// 即使后面的值会让总和回落，平方一旦越界立即失败（不做前瞻）。
	big := int64(3037000500)
	if _, _, reason, ok := computeResult([]int64{1, 2, big, -big}, 0, nil); ok {
		t.Fatal("expected overflow even if later negatives would bring the sum back")
	} else if !strings.Contains(reason, "平方和") {
		t.Fatalf("reason=%q want 平方和", reason)
	}
}

func TestComputeResultCanceled(t *testing.T) {
	// 取消信号在计算前置位时不得产出成功结果。
	_, _, _, ok := computeResult([]int64{1, 2, 3}, 0, func() bool { return true })
	if ok {
		t.Fatal("canceled compute must not succeed")
	}
}

func TestDeterministicDigests(t *testing.T) {
	in := []int64{1, 2, -3, 4}
	d1 := inputsDigestHex(in, 9)
	d2 := inputsDigestHex(append([]int64(nil), in...), 9)
	if d1 != d2 {
		t.Fatal("inputs digest not stable")
	}
	if inputsDigestHex(in, 10) == d1 {
		t.Fatal("seed must affect inputs digest")
	}
	if inputsDigestHex([]int64{1, -3, 2, 4}, 9) == d1 {
		t.Fatal("order must affect inputs digest")
	}
	r1 := resultDigestHex(in, 9, 4, 30)
	r2 := resultDigestHex(append([]int64(nil), in...), 9, 4, 30)
	if r1 != r2 {
		t.Fatal("result digest not stable")
	}
	log := buildLog(in, 9, 4, 30)
	c1 := checksumHex(in, 9, 4, 30, log, r1)
	c2 := checksumHex(append([]int64(nil), in...), 9, 4, 30, log, r1)
	if c1 != c2 {
		t.Fatal("checksum not stable")
	}
	if c1 == r1 {
		t.Fatal("checksum should differ from result digest")
	}
	// 日志必须确定性且不含时间/作业号。
	if buildLog(in, 9, 4, 30) != log {
		t.Fatal("log not deterministic")
	}
	if strings.Contains(log, "time") || strings.Contains(log, "job-") {
		t.Fatalf("log must not contain time/job id: %q", log)
	}
}
