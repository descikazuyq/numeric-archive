package numeric

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// 本文件覆盖“已恢复作业的原文件名恰好等于新作业将使用的默认文件名”这一
// 形态：读取规则接受任何 job- 开头、.json 结尾的合法记录，作业号由记录内容
// 识别，允许合法记录使用非默认文件名（文件名中的数字可以与真实作业号不同）。
// 例如目录中只有作业 1，它却保存在 job-...0002.json 中：归档正常打开，但
// 新请求拟分配的作业号为 2，若按默认命名落盘会原子替换掉作业 1 的已保存
// 记录。提交功能必须在产生任何记录之前拒绝这类新请求。

// restoredOccupiedBase 是构造恢复记录时使用的固定时间。
var restoredOccupiedBase = time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)

// succeededRecordInFile 构造一份内容合法的成功作业记录（不写盘）。
func succeededRecordInFile(t *testing.T, id uint64, submitter, requestID string, values []int64, at time.Time) *storedJob {
	t.Helper()
	j := &storedJob{
		id:              id,
		submitter:       submitter,
		requestID:       requestID,
		values:          append([]int64(nil), values...),
		queuedAt:        at,
		finishedAt:      at.Add(time.Second),
		status:          StatusSucceeded,
		effectiveValues: append([]int64(nil), values...),
	}
	sum, sumSq, _, ok := computeResult(values, 0, nil)
	if !ok {
		t.Fatalf("test setup: values %v overflow", values)
	}
	j.archive = newArchive(j, values, sum, sumSq, j.finishedAt)
	return j
}

// assertSubmitBlockedByOccupiedFile 提交一次新请求并断言它被文件名占用错误
// 拒绝：返回空作业与非空错误，错误包装 ErrRecordFileNameOccupied，并明确
// 给出拟分配的新作业号、被占用的文件名与原归属作业号。
func assertSubmitBlockedByOccupiedFile(t *testing.T, s *Store, req SubmitRequest, newID, ownerID uint64, occupiedName string) {
	t.Helper()
	j, err := s.Submit(req)
	if j != nil || err == nil {
		t.Fatalf("文件名被另一作业占用时必须拒绝新请求：job=%+v err=%v", j, err)
	}
	if !errors.Is(err, ErrRecordFileNameOccupied) {
		t.Fatalf("错误必须包装 ErrRecordFileNameOccupied，got %v", err)
	}
	msg := err.Error()
	for _, want := range []string{
		jobFileName(newID), occupiedName, "另一作业 " + strconv.FormatUint(ownerID, 10),
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("错误必须说明拟分配作业号、被占用文件名与原归属作业号（缺少 %q）：%v", want, err)
		}
	}
}

// assertNewRequestLeftNoTrace 断言被拒绝的新请求没有留下任何痕迹：作业号未
// 消耗、请求号未登记、按作业号查询与按提交人列举都看不到、目录里没有新增
// 正式记录或临时文件，占用默认文件名的旧记录内容仍归属 ownerID。
func assertNewRequestLeftNoTrace(t *testing.T, s *Store, dir string, req SubmitRequest, rejectedID, ownerID uint64, wantFiles int) {
	t.Helper()
	if _, err := s.Get(rejectedID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("被拒绝请求不应能按作业号查询：%v", err)
	}
	list, err := s.List(req.Submitter, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range list {
		if j.ID == rejectedID {
			t.Fatalf("被拒绝请求不应出现在按提交人列举中：%v", ids(list))
		}
	}
	// 默认命名文件作为占用者存在，但其中的记录必须仍归属 ownerID：拒绝不得
	// 用新作业的记录替换它。
	data, err := os.ReadFile(filepath.Join(dir, jobFileName(rejectedID)))
	if err != nil {
		t.Fatal(err)
	}
	var onDisk jobRecord
	if err := json.Unmarshal(data, &onDisk); err != nil {
		t.Fatalf("占用文件记录损坏: %v", err)
	}
	if onDisk.ID != ownerID {
		t.Fatalf("占用文件应仍归属作业 %d，却保存了作业 %d", ownerID, onDisk.ID)
	}
	if got := countJobFiles(t, dir); got != wantFiles {
		t.Fatalf("目录中的正式记录数变化：got %d want %d", got, wantFiles)
	}
	if got := countTmpFiles(t, dir); got != 0 {
		t.Fatalf("被拒绝请求不得残留临时文件：%d", got)
	}
	s.mu.Lock()
	nextID := s.nextID
	_, claimed := s.idem[idemIdentity{req.Submitter, req.RequestID}]
	running := s.runningID
	s.mu.Unlock()
	if nextID != rejectedID {
		t.Fatalf("拒绝不得消耗作业号：nextID=%d want %d", nextID, rejectedID)
	}
	if claimed {
		t.Fatalf("拒绝不得登记提交人的请求号 %q", req.RequestID)
	}
	if running == rejectedID {
		t.Fatal("被拒绝请求不得进入计算队列")
	}
}

// TestSubmitRejectsWhenDefaultRecordFileNameOccupied 核心场景：目录中只有
// 作业 1（成功归档），却保存在作业 2 的默认命名文件中。归档正常打开，但新
// 请求拟分配作业号 2，默认记录文件已属于作业 1——提交必须返回空作业与
// ErrRecordFileNameOccupied，不产生任何副作用，也不改动占用文件。
func TestSubmitRejectsWhenDefaultRecordFileNameOccupied(t *testing.T) {
	dir := t.TempDir()
	occupiedName := jobFileName(2)
	old := succeededRecordInFile(t, 1, "alice", "old-req", []int64{6}, restoredOccupiedBase)
	writeNamedSyntheticRecord(t, dir, occupiedName, old)

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("目录本身没有重复作业号，单份非默认命名记录必须正常打开：%v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	g1, err := s.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if g1.Status != StatusSucceeded || g1.Archive == nil || g1.Archive.Sum != 6 {
		t.Fatalf("作业 1 应按记录内容正常恢复：%+v", g1)
	}
	// 作业号由记录内容识别，文件名中的 2 不代表作业 2。
	if _, err := s.Get(2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("文件名中的数字不能充当作业号：Get(2)=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, jobFileName(1))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("打开归档不得为作业 1 补写默认命名副本：%v", err)
	}

	before, err := os.ReadFile(filepath.Join(dir, occupiedName))
	if err != nil {
		t.Fatal(err)
	}

	req := SubmitRequest{Submitter: "bob", RequestID: "new-req", Values: []int64{7}}
	assertSubmitBlockedByOccupiedFile(t, s, req, 2, 1, occupiedName)
	assertNewRequestLeftNoTrace(t, s, dir, req, 2, 1, 1)

	// 占用名称的旧记录逐字节不变，旧作业的成功归档继续可读。
	after, err := os.ReadFile(filepath.Join(dir, occupiedName))
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("被拒绝提交不得改写占用名称的旧记录：\nbefore=%s\nafter=%s", before, after)
	}
	g1Again, _ := s.Get(1)
	if g1Again.Status != StatusSucceeded || g1Again.Archive == nil ||
		g1Again.Archive.Sum != 6 || g1Again.Archive.SumOfSquares != 36 ||
		g1Again.Archive.Checksum != g1.Archive.Checksum {
		t.Fatalf("旧作业的状态、结果与校验值不得因拒绝而改变：%+v", g1Again)
	}

	// 占用未解除前，同样的新请求（以及空请求号的新请求）继续被同一错误拒绝，
	// 作业号始终未被消耗。
	assertSubmitBlockedByOccupiedFile(t, s, req, 2, 1, occupiedName)
	emptyReq := SubmitRequest{Submitter: "bob", Values: []int64{8}}
	assertSubmitBlockedByOccupiedFile(t, s, emptyReq, 2, 1, occupiedName)
	assertNewRequestLeftNoTrace(t, s, dir, req, 2, 1, 1)
}

// TestSubmitFileNameOccupiedEvenWhenOccupierFailedOrCanceled 占用文件的旧记录
// 即使处于失败或取消终态，也不能被当作可覆盖的空位。
func TestSubmitFileNameOccupiedEvenWhenOccupierFailedOrCanceled(t *testing.T) {
	cases := []struct {
		name   string
		status Status
		reason string
	}{
		{name: "占用记录已失败", status: StatusFailed, reason: "平方和超出有符号 64 位整数范围"},
		{name: "占用记录已取消", status: StatusCanceled, reason: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			occupiedName := jobFileName(2)
			j := &storedJob{
				id: 1, submitter: "a", values: []int64{6},
				queuedAt: restoredOccupiedBase, finishedAt: restoredOccupiedBase.Add(time.Second),
				status: tc.status, failureReason: tc.reason,
			}
			writeNamedSyntheticRecord(t, dir, occupiedName, j)

			s, err := Open(dir)
			if err != nil {
				t.Fatalf("单份非默认命名记录必须正常打开：%v", err)
			}
			t.Cleanup(func() { _ = s.Close() })

			before, err := os.ReadFile(filepath.Join(dir, occupiedName))
			if err != nil {
				t.Fatal(err)
			}
			req := SubmitRequest{Submitter: "b", RequestID: "r", Values: []int64{1}}
			assertSubmitBlockedByOccupiedFile(t, s, req, 2, 1, occupiedName)
			assertNewRequestLeftNoTrace(t, s, dir, req, 2, 1, 1)
			after, _ := os.ReadFile(filepath.Join(dir, occupiedName))
			if string(after) != string(before) {
				t.Fatalf("终态占用记录不得被被拒绝提交改写：\nbefore=%s\nafter=%s", before, after)
			}
			g, _ := s.Get(1)
			if g.Status != tc.status {
				t.Fatalf("旧作业状态必须保持 %s，got %s", tc.status, g.Status)
			}
		})
	}
}

// TestSubmitFileNameOccupiedDoesNotOverrideIdempotency 文件名冲突只影响需要
// 创建记录的新请求：命中已接受作业的幂等重放与幂等冲突优先，不被文件名占用
// 错误替代。
func TestSubmitFileNameOccupiedDoesNotOverrideIdempotency(t *testing.T) {
	dir := t.TempDir()
	occupiedName := jobFileName(2)
	old := succeededRecordInFile(t, 1, "alice", "r1", []int64{6}, restoredOccupiedBase)
	writeNamedSyntheticRecord(t, dir, occupiedName, old)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	before, _ := os.ReadFile(filepath.Join(dir, occupiedName))

	// 同一提交人+非空请求号+相同内容：返回原作业当前详情，无错误。
	replay := SubmitRequest{Submitter: "alice", RequestID: "r1", Values: []int64{6}}
	j, err := s.Submit(replay)
	if err != nil || j == nil || j.ID != 1 || j.Status != StatusSucceeded {
		t.Fatalf("幂等重放必须优先于文件名占用检查：job=%+v err=%v", j, err)
	}
	if j.Archive == nil || j.Archive.Sum != 6 {
		t.Fatalf("幂等重放应返回原作业当前详情：%+v", j)
	}

	// 同一提交人+请求号但内容不同：仍是原有的幂等冲突（同时返回原作业），
	// 不能被文件名占用错误替代。
	conflictReq := SubmitRequest{Submitter: "alice", RequestID: "r1", Values: []int64{6, 7}}
	cj, err := s.Submit(conflictReq)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("内容不同应返回幂等冲突，got %v", err)
	}
	if errors.Is(err, ErrRecordFileNameOccupied) {
		t.Fatalf("幂等冲突不得被文件名占用错误替代：%v", err)
	}
	if cj == nil || cj.ID != 1 {
		t.Fatalf("幂等冲突仍须返回原作业视图：%+v", cj)
	}

	// 原记录字节不变；新作业号仍未消耗。
	after, _ := os.ReadFile(filepath.Join(dir, occupiedName))
	if string(after) != string(before) {
		t.Fatal("幂等重放/冲突不得改写占用文件")
	}
	s.mu.Lock()
	nextID := s.nextID
	s.mu.Unlock()
	if nextID != 2 {
		t.Fatalf("幂等路径不得消耗作业号：nextID=%d", nextID)
	}

	// 同一提交人的另一请求号（新请求）仍被占用错误拦截；不同提交人的新请求
	// 同样被拦截——占用判断与提交人无关。
	assertSubmitBlockedByOccupiedFile(t, s,
		SubmitRequest{Submitter: "alice", RequestID: "r2", Values: []int64{1}}, 2, 1, occupiedName)
	assertSubmitBlockedByOccupiedFile(t, s,
		SubmitRequest{Submitter: "carol", RequestID: "r3", Values: []int64{1}}, 2, 1, occupiedName)
}

// TestSubmitFileNameCollisionResolvedByRenamingAfterClose 关闭归档后把占用
// 名称的旧记录改名为另一个合法且不冲突的文件名，再打开归档：此前被拒绝的
// 请求作为新请求接受，取得原本未消耗的下一个作业号（2），按正常规则计算与
// 归档；旧作业仍使用它被读取时的文件名，新作业使用默认命名。
func TestSubmitFileNameCollisionResolvedByRenamingAfterClose(t *testing.T) {
	dir := t.TempDir()
	occupiedName := jobFileName(2)
	// 作业 1 是排队记录：首次打开后会在占用文件中原地完成，验证恢复作业的
	// 状态更新确实写回读入时的文件。
	writeNamedSyntheticRecord(t, dir, occupiedName,
		queuedStoredJob(1, "alice", []int64{6}, restoredOccupiedBase))

	s1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	old := waitStatus(t, s1, 1, StatusSucceeded)
	if old.Archive == nil || old.Archive.Sum != 6 {
		t.Fatalf("作业 1 应在占用文件中正常完成：%+v", old)
	}
	assertNoDefaultRecord(t, dir, 1)
	before, _ := os.ReadFile(filepath.Join(dir, occupiedName))

	req := SubmitRequest{Submitter: "carol", RequestID: "new-req", Values: []int64{7}}
	assertSubmitBlockedByOccupiedFile(t, s1, req, 2, 1, occupiedName)
	assertNewRequestLeftNoTrace(t, s1, dir, req, 2, 1, 1)
	after, _ := os.ReadFile(filepath.Join(dir, occupiedName))
	if string(after) != string(before) {
		t.Fatal("被拒绝的提交不得改写占用名称的旧记录")
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	// 关闭后由调用方把占用记录改名为另一个合法（job- 开头、.json 结尾）且
	// 不与下一个默认文件名冲突的名称。
	relocated := suffixedRecordName(1) // job-1-saved.json
	if err := os.Rename(filepath.Join(dir, occupiedName), filepath.Join(dir, relocated)); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("改名后归档应正常打开：%v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	// 旧作业从新读入的文件恢复，参数、状态与已有结果不变。
	g1, err := s2.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if g1.Status != StatusSucceeded || g1.Archive == nil || g1.Archive.Sum != 6 ||
		g1.Archive.Checksum != old.Archive.Checksum {
		t.Fatalf("改名恢复后旧作业结果应原样可读：%+v", g1)
	}

	// 此前被拒绝的请求现在作为新请求接受，取得原本未消耗的作业号 2。
	j2, err := s2.Submit(req)
	if err != nil {
		t.Fatalf("冲突解除后被拒绝的请求应能作为新请求接受：%v", err)
	}
	if j2 == nil || j2.ID != 2 {
		t.Fatalf("新作业应取得未消耗的下一个作业号 2：%+v", j2)
	}
	done := waitStatus(t, s2, 2, StatusSucceeded)
	if done.Archive == nil || done.Archive.Sum != 7 || done.Archive.SumOfSquares != 49 {
		t.Fatalf("新作业应按正常规则计算归档：%+v", done)
	}

	// 新作业使用默认命名且记录内容中的作业号为 2；旧作业保持改名后的文件，
	// 系统没有补出任何默认命名的作业 1 副本，也没有重复记录。
	newData, err := os.ReadFile(filepath.Join(dir, jobFileName(2)))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(newData), `"id": 2`) ||
		!strings.Contains(string(newData), `"status": "succeeded"`) {
		t.Fatalf("默认命名文件应保存新作业 2：\n%s", newData)
	}
	oldData, err := os.ReadFile(filepath.Join(dir, relocated))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(oldData), `"id": 1`) ||
		!strings.Contains(string(oldData), `"status": "succeeded"`) {
		t.Fatalf("旧作业应仍在它被读取时的文件中：\n%s", oldData)
	}
	assertNoDefaultRecord(t, dir, 1)
	if countJobFiles(t, dir) != 2 || countTmpFiles(t, dir) != 0 {
		t.Fatalf("目录应恰好包含两份正式记录：files=%d tmps=%d",
			countJobFiles(t, dir), countTmpFiles(t, dir))
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	// 再次打开：没有重复作业号，两个作业都按记录内容正确恢复。
	s3, err := Open(dir)
	if err != nil {
		t.Fatalf("改名并接受新作业后应正常重开：%v", err)
	}
	defer s3.Close()
	r1, _ := s3.Get(1)
	r2, _ := s3.Get(2)
	if r1.Archive == nil || r1.Archive.Sum != 6 {
		t.Fatalf("重开后作业 1 结果应保留：%+v", r1)
	}
	if r2.Archive == nil || r2.Archive.Sum != 7 {
		t.Fatalf("重开后作业 2 结果应保留：%+v", r2)
	}
	l1, _ := s3.List("alice", time.Time{}, time.Time{})
	l2, _ := s3.List("carol", time.Time{}, time.Time{})
	if len(l1) != 1 || l1[0].ID != 1 || len(l2) != 1 || l2[0].ID != 2 {
		t.Fatalf("按提交人列举应只看到各自的作业：alice=%v carol=%v", ids(l1), ids(l2))
	}
}

// TestNonCollidingNonDefaultFileNameDoesNotAffectSubmit 合法的非默认文件名
// 只要不与新作业的默认文件名冲突，就不影响打开与提交；作业号始终由记录内容
// 决定，而不是文件名中的数字（作业 1 保存在 job-...0007.json 中，下一个作业
// 仍是 2 而不是 8）。
func TestNonCollidingNonDefaultFileNameDoesNotAffectSubmit(t *testing.T) {
	dir := t.TempDir()
	nonDefault := jobFileName(7) // 文件名带 7，内容是作业 1
	old := succeededRecordInFile(t, 1, "alice", "", []int64{6}, restoredOccupiedBase)
	writeNamedSyntheticRecord(t, dir, nonDefault, old)

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("合法非默认文件名不得阻止打开：%v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if _, err := s.Get(7); !errors.Is(err, ErrNotFound) {
		t.Fatalf("文件名中的 7 不能充当作业号：%v", err)
	}
	g1, _ := s.Get(1)
	if g1.Status != StatusSucceeded || g1.Archive == nil || g1.Archive.Sum != 6 {
		t.Fatalf("作业 1 应按记录内容恢复：%+v", g1)
	}

	j2, err := s.Submit(SubmitRequest{Submitter: "bob", RequestID: "r", Values: []int64{2}})
	if err != nil {
		t.Fatalf("不冲突的非默认文件名不得影响新提交：%v", err)
	}
	if j2.ID != 2 {
		t.Fatalf("作业号必须按记录内容续号为 2，got %d", j2.ID)
	}
	done := waitStatus(t, s, 2, StatusSucceeded)
	if done.Archive.Sum != 2 {
		t.Fatalf("新作业应正常计算：%+v", done)
	}

	// 旧作业仍只在非默认文件中，新作业在默认命名文件中，各一份。
	if countJobFiles(t, dir) != 2 {
		t.Fatalf("目录应恰好有两份正式记录：%d", countJobFiles(t, dir))
	}
	assertNoDefaultRecord(t, dir, 1)
	oldData, err := os.ReadFile(filepath.Join(dir, nonDefault))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(oldData), `"id": 1`) {
		t.Fatalf("非默认文件应继续只保存作业 1：\n%s", oldData)
	}
	newData, err := os.ReadFile(filepath.Join(dir, jobFileName(2)))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(newData), `"id": 2`) {
		t.Fatalf("默认命名文件应保存新作业 2：\n%s", newData)
	}
}
