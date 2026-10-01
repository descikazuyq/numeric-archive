package numeric_test

import (
	"errors"
	"fmt"
	"os"
	"time"

	numeric "github.com/descikazuyq/numeric-archive/numeric"
)

// Example 展示一次完整的提交、幂等重放、查询与归档读取流程。
func Example() {
	dir, err := os.MkdirTemp("", "numeric-example-*")
	if err != nil {
		fmt.Println("mkdir error")
		return
	}
	defer os.RemoveAll(dir)

	store, err := numeric.Open(dir)
	if err != nil {
		fmt.Println("open error")
		return
	}
	defer func() { _ = store.Close() }()

	// 提交一个作业：记录提交人、幂等请求号、整数序列与种子。
	first, err := store.Submit(numeric.SubmitRequest{
		Submitter: "alice",
		RequestID: "req-001",
		Values:    []int64{1, 2, 3, 4},
		Seed:      42,
	})
	if err != nil {
		fmt.Println("submit error")
		return
	}

	// 相同提交人 + 幂等请求号 + 相同内容再次提交，返回同一个作业。
	replay, err := store.Submit(numeric.SubmitRequest{
		Submitter: "alice",
		RequestID: "req-001",
		Values:    []int64{1, 2, 3, 4},
		Seed:      42,
	})
	if err != nil || replay.ID != first.ID {
		fmt.Println("idempotent replay failed")
		return
	}

	// 轮询直到终态。
	var done *numeric.Job
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		done, err = store.Get(first.ID)
		if err != nil {
			fmt.Println("get error")
			return
		}
		if done.Status == numeric.StatusSucceeded || done.Status == numeric.StatusFailed {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if done.Status == numeric.StatusSucceeded {
		a := done.Archive
		fmt.Printf("sum=%d sum_of_squares=%d\n", a.Sum, a.SumOfSquares)
		fmt.Printf("digest=%t checksum=%t\n", a.ResultDigest != "", a.Checksum != "")
	}

	// 空序列被拒绝且不产生记录。
	if _, err := store.Submit(numeric.SubmitRequest{Submitter: "alice", Values: nil}); errors.Is(err, numeric.ErrEmptySequence) {
		fmt.Println("empty sequence rejected")
	}

	// 时间范围查询（两端包含；起始晚于结束会被拒绝）。
	list, err := store.List("alice", time.Time{}, time.Time{})
	if err == nil {
		fmt.Printf("listed=%d\n", len(list))
	}

	// Output:
	// sum=10 sum_of_squares=30
	// digest=true checksum=true
	// empty sequence rejected
	// listed=1
}
